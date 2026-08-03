// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build dingo_extra_plugins

package aws

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

// BlobStoreS3 stores data in an AWS S3 bucket
type BlobStoreS3 struct {
	promRegistry  prometheus.Registerer
	startupCtx    context.Context
	logger        *S3Logger
	client        *s3.Client
	startupCancel context.CancelFunc
	endpoint      string
	bucket        string
	prefix        string
	region        string
	timeout       time.Duration
}

const maxBlobReadBytes int64 = 256 << 20

func readBlobBody(r io.Reader) ([]byte, error) {
	return readBlobBodyWithLimit(r, maxBlobReadBytes)
}

func readBlobBodyWithLimit(r io.Reader, maxBytes int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf(
			"blob object exceeds maximum size of %d bytes",
			maxBytes,
		)
	}
	return data, nil
}

// s3Txn stages S3 operations until commit. Commit applies the staged object
// changes in a deterministic order and compensates already-applied changes if
// a later cloud operation fails.
type s3Txn struct {
	store     *BlobStoreS3
	finished  bool
	readWrite bool
	pending   map[string]s3PendingChange
}

type s3PendingChange struct {
	value   []byte
	deleted bool
}

// New creates a new S3-backed blob store and dataDir must be "s3://bucket" or "s3://bucket/prefix"
func New(
	dataDir string,
	logger *slog.Logger,
	promRegistry prometheus.Registerer,
) (*BlobStoreS3, error) {
	const prefix = "s3://"
	if !strings.HasPrefix(dataDir, prefix) {
		return nil, errors.New(
			"s3 blob: expected dataDir='s3://<bucket>[/prefix]'",
		)
	}

	path := strings.TrimPrefix(dataDir, prefix)
	if path == "" {
		return nil, errors.New("s3 blob: bucket not set")
	}

	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		return nil, errors.New("s3 blob: invalid S3 path (missing bucket)")
	}

	bucket := parts[0]
	keyPrefix := ""
	if len(parts) > 1 && parts[1] != "" {
		keyPrefix = strings.TrimSuffix(parts[1], "/")
		if keyPrefix != "" {
			keyPrefix += "/"
		}
	}

	return NewWithOptions(
		WithBucket(bucket),
		WithPrefix(keyPrefix),
		WithLogger(logger),
		WithPromRegistry(promRegistry),
	)
}

// NewWithOptions creates a new S3-backed blob store using options.
func NewWithOptions(opts ...BlobStoreS3OptionFunc) (*BlobStoreS3, error) {
	db := &BlobStoreS3{}

	// Apply options
	for _, opt := range opts {
		opt(db)
	}

	// Set defaults (no side effects)
	if db.logger == nil {
		db.logger = NewS3Logger(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	}

	// Note: AWS config loading and validation moved to Start()
	return db, nil
}

func (d *BlobStoreS3) opContext() (context.Context, context.CancelFunc) {
	timeout := d.timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout) //nolint:gosec // G118: cancel func is returned to caller
}

// Close implements the BlobStore interface.
func (d *BlobStoreS3) Close() error {
	return d.Stop()
}

// DiskSize returns 0 for cloud-backed stores.
func (d *BlobStoreS3) DiskSize() (int64, error) {
	return 0, nil
}

// Sync is a no-op: an S3 object is durable once its PutObject has been
// acknowledged, so a committed write needs no additional flush.
func (d *BlobStoreS3) Sync() error {
	return nil
}

// NewTransaction returns a lightweight transaction wrapper.
func (d *BlobStoreS3) NewTransaction(readWrite bool) types.Txn {
	return &s3Txn{
		store:     d,
		readWrite: readWrite,
		pending:   make(map[string]s3PendingChange),
	}
}

func (t *s3Txn) assertWritable() error {
	if !t.readWrite {
		return errors.New("transaction is read-only")
	}
	return nil
}

func (d *BlobStoreS3) validateTxn(txn types.Txn) (*s3Txn, error) {
	if txn == nil {
		return nil, types.ErrNilTxn
	}
	t, ok := txn.(*s3Txn)
	if !ok || t.store != d {
		return nil, types.ErrTxnWrongType
	}
	if t.finished {
		return nil, errors.New("transaction already finished")
	}
	if d.client == nil {
		return nil, types.ErrBlobStoreUnavailable
	}
	return t, nil
}

func (t *s3Txn) stageSet(key, value []byte) {
	t.pending[string(key)] = s3PendingChange{
		value: append([]byte(nil), value...),
	}
}

func (t *s3Txn) stageDelete(key []byte) {
	t.pending[string(key)] = s3PendingChange{deleted: true}
}

func (t *s3Txn) stagedValue(key []byte) ([]byte, bool) {
	change, ok := t.pending[string(key)]
	if !ok {
		return nil, false
	}
	if change.deleted {
		return nil, true
	}
	return append([]byte(nil), change.value...), true
}

func (d *BlobStoreS3) deleteObject(ctx context.Context, key string) error {
	_, err := d.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    new(d.fullKey(key)),
	})
	if isS3NotFound(err) {
		return types.ErrBlobKeyNotFound
	}
	return err
}

// Get retrieves a value from S3 within a transaction
func (d *BlobStoreS3) Get(txn types.Txn, key []byte) ([]byte, error) {
	t, err := d.validateTxn(txn)
	if err != nil {
		return nil, err
	}
	if value, staged := t.stagedValue(key); staged {
		if value == nil {
			return nil, types.ErrBlobKeyNotFound
		}
		return value, nil
	}
	ctx, cancel := d.opContext()
	defer cancel()
	data, err := d.getInternal(ctx, string(key))
	if err != nil {
		if isS3NotFound(err) {
			return nil, types.ErrBlobKeyNotFound
		}
		return nil, err
	}
	return data, nil
}

// Set stores a key-value pair in S3 within a transaction
func (d *BlobStoreS3) Set(txn types.Txn, key, val []byte) error {
	t, err := d.validateTxn(txn)
	if err != nil {
		return err
	}
	if err := t.assertWritable(); err != nil {
		return err
	}
	t.stageSet(key, val)
	return nil
}

// Delete removes a key from S3 within a transaction
func (d *BlobStoreS3) Delete(txn types.Txn, key []byte) error {
	t, err := d.validateTxn(txn)
	if err != nil {
		return err
	}
	if err := t.assertWritable(); err != nil {
		return err
	}
	if value, staged := t.stagedValue(key); staged {
		if value == nil {
			return types.ErrBlobKeyNotFound
		}
		t.stageDelete(key)
		return nil
	}
	ctx, cancel := d.opContext()
	defer cancel()
	if _, err := d.getInternal(ctx, string(key)); err != nil {
		if isS3NotFound(err) {
			return types.ErrBlobKeyNotFound
		}
		return err
	}
	t.stageDelete(key)
	return nil
}

// NewIterator creates an iterator for S3 within a transaction.
//
// Important: items returned by the iterator's `Item()` must only be
// accessed while the transaction used to create the iterator is still
// active. Implementations may validate transaction state at access time
// (for example `ValueCopy` may fail if the transaction has been committed
// or rolled back). Typical usage iterates and accesses item values within
// the same transaction scope.
func (d *BlobStoreS3) NewIterator(
	txn types.Txn,
	opts types.BlobIteratorOptions,
) types.BlobIterator {
	if _, err := d.validateTxn(txn); err != nil {
		return &s3ErrorIterator{err: err}
	}
	if !opts.Reverse {
		iterator := &s3StreamIterator{
			store:  d,
			txn:    txn,
			prefix: opts.Prefix,
		}
		iterator.Rewind()
		return iterator
	}
	reverseKeys, err := d.listKeysToFile(opts)
	if err != nil {
		d.logger.Errorf("s3 list failed: %v", err)
		return &s3Iterator{
			store:   d,
			keys:    []string{},
			reverse: opts.Reverse,
			err:     err,
			txn:     txn,
		}
	}
	iterator := &s3ReverseIterator{store: d, txn: txn, keys: reverseKeys}
	iterator.Rewind()
	return iterator
}

type s3StreamIterator struct {
	store     *BlobStoreS3
	txn       types.Txn
	prefix    []byte
	paginator *s3.ListObjectsV2Paginator
	page      []s3types.Object
	pageIdx   int
	seek      string
	key       string
	valid     bool
	err       error
	cancel    context.CancelFunc
}

func (it *s3StreamIterator) reset(seek []byte) {
	if it.cancel != nil {
		it.cancel()
	}
	ctx, cancel := it.store.opContext()
	it.cancel = cancel
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(it.store.bucket),
	}
	if prefix := it.store.fullKey(string(it.prefix)); prefix != "" {
		input.Prefix = aws.String(prefix)
	} else if it.store.prefix != "" {
		input.Prefix = aws.String(it.store.prefix)
	}
	it.paginator = s3.NewListObjectsV2Paginator(it.store.client, input)
	it.page = nil
	it.pageIdx = 0
	it.seek = string(seek)
	it.key = ""
	it.valid = false
	it.err = nil
	it.advance(ctx)
}

func (it *s3StreamIterator) advance(ctx context.Context) {
	if it.err != nil || it.paginator == nil {
		return
	}
	for {
		if it.pageIdx >= len(it.page) {
			if !it.paginator.HasMorePages() {
				it.valid = false
				return
			}
			page, err := it.paginator.NextPage(ctx)
			if err != nil {
				it.err = err
				it.valid = false
				return
			}
			it.page = page.Contents
			it.pageIdx = 0
		}
		objectKey := strings.TrimPrefix(
			aws.ToString(it.page[it.pageIdx].Key),
			it.store.prefix,
		)
		it.pageIdx++
		externalKey, err := hex.DecodeString(objectKey)
		if err != nil {
			it.err = fmt.Errorf("error decoding s3 key: %w", err)
			it.valid = false
			return
		}
		it.key = string(externalKey)
		if it.seek != "" && it.key < it.seek {
			continue
		}
		it.valid = true
		return
	}
}

func (it *s3StreamIterator) Rewind() { it.reset(nil) }

func (it *s3StreamIterator) Seek(prefix []byte) { it.reset(prefix) }

func (it *s3StreamIterator) Valid() bool {
	return it.err == nil && it.valid
}

func (it *s3StreamIterator) ValidForPrefix(prefix []byte) bool {
	return it.Valid() && strings.HasPrefix(it.key, string(prefix))
}

func (it *s3StreamIterator) Next() {
	if it.cancel == nil {
		return
	}
	ctx, cancel := it.store.opContext()
	defer cancel()
	it.advance(ctx)
}

func (it *s3StreamIterator) Item() types.BlobItem {
	if !it.Valid() {
		return nil
	}
	return &s3Item{store: it.store, txn: it.txn, key: it.key}
}

func (it *s3StreamIterator) Close() {
	if it.cancel != nil {
		it.cancel()
		it.cancel = nil
	}
	it.paginator = nil
	it.valid = false
}

func (it *s3StreamIterator) Err() error { return it.err }

type s3ReverseIterator struct {
	store *BlobStoreS3
	txn   types.Txn
	keys  *reverseKeyFile
	key   string
	valid bool
	err   error
}

func (it *s3ReverseIterator) advance() {
	key, valid, err := it.keys.nextReverse()
	if err != nil {
		it.err = err
		it.valid = false
		return
	}
	it.key = key
	it.valid = valid
}

func (it *s3ReverseIterator) Rewind() {
	it.keys.pos = 0
	it.keys.initialized = false
	it.valid = false
	it.advance()
}

func (it *s3ReverseIterator) Seek(prefix []byte) {
	it.Rewind()
	for it.Valid() && it.key > string(prefix) {
		it.advance()
	}
}

func (it *s3ReverseIterator) Valid() bool {
	return it.err == nil && it.valid
}

func (it *s3ReverseIterator) ValidForPrefix(prefix []byte) bool {
	return it.Valid() && strings.HasPrefix(it.key, string(prefix))
}

func (it *s3ReverseIterator) Next() { it.advance() }

func (it *s3ReverseIterator) Item() types.BlobItem {
	if !it.Valid() {
		return nil
	}
	return &s3Item{store: it.store, txn: it.txn, key: it.key}
}

func (it *s3ReverseIterator) Close() {
	if it.keys != nil && it.keys.file != nil {
		name := it.keys.file.Name()
		_ = it.keys.file.Close()
		_ = os.Remove(name)
		it.keys.file = nil
	}
	it.valid = false
}

func (it *s3ReverseIterator) Err() error { return it.err }

// SetBlock stores a block with its metadata and index
func (d *BlobStoreS3) SetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
	cborData []byte,
	id uint64,
	blockType uint,
	height uint64,
	prevHash []byte,
) error {
	t, err := d.validateTxn(txn)
	if err != nil {
		return err
	}
	if err := t.assertWritable(); err != nil {
		return err
	}
	// Block content by point
	key := types.BlockBlobKey(slot, hash)
	t.stageSet(key, cborData)
	// Block index to point key
	indexKey := types.BlockBlobIndexKey(id)
	t.stageSet(indexKey, key)
	// Hash-to-block-key index for O(1) BlockByHash lookups
	hashIndexKey := types.BlockHashIndexKey(hash)
	t.stageSet(hashIndexKey, key)
	// Block metadata by point
	metadataKey := types.BlockBlobMetadataKey(key)
	tmpMetadata := types.BlockMetadata{
		ID:       id,
		Type:     blockType,
		Height:   height,
		PrevHash: prevHash,
	}
	tmpMetadataBytes, err := cbor.Encode(tmpMetadata)
	if err != nil {
		return err
	}
	t.stageSet(metadataKey, tmpMetadataBytes)
	return nil
}

// GetBlock retrieves a block's CBOR data and metadata
func (d *BlobStoreS3) GetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	if _, err := d.validateTxn(txn); err != nil {
		return nil, types.BlockMetadata{}, err
	}
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.BlockBlobKey(slot, hash)
	cborData, err := d.getInternal(ctx, string(key))
	if err != nil {
		if isS3NotFound(err) {
			return nil, types.BlockMetadata{}, types.ErrBlobKeyNotFound
		}
		return nil, types.BlockMetadata{}, err
	}
	if types.IsBlockTombstone(cborData) {
		return nil, types.BlockMetadata{},
			&types.HistoryExpiredError{Slot: slot, Hash: hash}
	}
	metadataKey := types.BlockBlobMetadataKey(key)
	metadataBytes, err := d.getInternal(ctx, string(metadataKey))
	if err != nil {
		if isS3NotFound(err) {
			return nil, types.BlockMetadata{}, types.ErrBlobKeyNotFound
		}
		return nil, types.BlockMetadata{}, err
	}
	var tmpMetadata types.BlockMetadata
	if _, err := cbor.Decode(metadataBytes, &tmpMetadata); err != nil {
		return nil, types.BlockMetadata{}, err
	}
	return cborData, tmpMetadata, nil
}

// DeleteBlock removes a block and its associated data
func (d *BlobStoreS3) DeleteBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
	id uint64,
) error {
	t, err := d.validateTxn(txn)
	if err != nil {
		return err
	}
	if err := t.assertWritable(); err != nil {
		return err
	}
	key := types.BlockBlobKey(slot, hash)
	indexKey := types.BlockBlobIndexKey(id)
	metadataKey := types.BlockBlobMetadataKey(key)
	hashIndexKey := types.BlockHashIndexKey(hash)
	t.stageDelete(key)
	t.stageDelete(indexKey)
	t.stageDelete(metadataKey)
	t.stageDelete(hashIndexKey)
	return nil
}

// TombstoneBlock replaces a block's CBOR with an expired-history marker.
// GetBlock reads the bp object, sees the marker, and returns
// types.ErrHistoryExpired so an archive proxy can intercept it.
//
// What stays:
//   - bi<id>: required by BlockByIndex (the chain iterator translates
//     id→key here; no equivalent index exists in metadata).
//   - bh<hash>: BlockByHash resolves only through this index and treats
//     a missing entry as a hard miss (ErrBlockNotFound), so the entry
//     must survive tombstoning to keep the block reachable by hash.
//
// What goes:
//   - bp_metadata: GetBlock short-circuits on the expiry marker before
//     reading metadata, and no other caller asks for local metadata of an
//     expired block — bark's archive response carries its own.
func (d *BlobStoreS3) TombstoneBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) error {
	t, err := d.validateTxn(txn)
	if err != nil {
		return err
	}
	if err := t.assertWritable(); err != nil {
		return err
	}
	key := types.BlockBlobKey(slot, hash)
	metadataKey := types.BlockBlobMetadataKey(key)
	t.stageSet(key, types.BlockTombstone())
	t.stageDelete(metadataKey)
	return nil
}

// SetUtxo stores a UTxO's CBOR data
func (d *BlobStoreS3) SetUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
	cborData []byte,
) error {
	t, err := d.validateTxn(txn)
	if err != nil {
		return err
	}
	if err := t.assertWritable(); err != nil {
		return err
	}
	key := types.UtxoBlobKey(txId, outputIdx)
	t.stageSet(key, cborData)
	return nil
}

// GetUtxo retrieves a UTxO's CBOR data
func (d *BlobStoreS3) GetUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) ([]byte, error) {
	if _, err := d.validateTxn(txn); err != nil {
		return nil, err
	}
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.UtxoBlobKey(txId, outputIdx)
	data, err := d.getInternal(ctx, string(key))
	if err != nil {
		if isS3NotFound(err) {
			return nil, types.ErrBlobKeyNotFound
		}
		return nil, err
	}
	return data, nil
}

// DeleteUtxo removes a UTxO's data
func (d *BlobStoreS3) DeleteUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) error {
	t, err := d.validateTxn(txn)
	if err != nil {
		return err
	}
	if err := t.assertWritable(); err != nil {
		return err
	}
	key := types.UtxoBlobKey(txId, outputIdx)
	t.stageDelete(key)
	return nil
}

// SetTx stores a transaction's offset data
func (d *BlobStoreS3) SetTx(
	txn types.Txn,
	txHash []byte,
	offsetData []byte,
) error {
	t, err := d.validateTxn(txn)
	if err != nil {
		return fmt.Errorf("SetTx: validate txn: %w", err)
	}
	if err := t.assertWritable(); err != nil {
		return fmt.Errorf("SetTx: assert writable: %w", err)
	}
	key := types.TxBlobKey(txHash)
	t.stageSet(key, offsetData)
	return nil
}

// GetTx retrieves a transaction's offset data
func (d *BlobStoreS3) GetTx(
	txn types.Txn,
	txHash []byte,
) ([]byte, error) {
	if _, err := d.validateTxn(txn); err != nil {
		return nil, fmt.Errorf("GetTx: validate txn: %w", err)
	}
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.TxBlobKey(txHash)
	data, err := d.getInternal(ctx, string(key))
	if err != nil {
		if isS3NotFound(err) {
			return nil, fmt.Errorf("GetTx: tx blob %s: %w", key, types.ErrBlobKeyNotFound)
		}
		return nil, fmt.Errorf("GetTx: get tx blob %s: %w", key, err)
	}
	return data, nil
}

// DeleteTx removes a transaction's offset data
func (d *BlobStoreS3) DeleteTx(
	txn types.Txn,
	txHash []byte,
) error {
	t, err := d.validateTxn(txn)
	if err != nil {
		return fmt.Errorf("DeleteTx: validate txn: %w", err)
	}
	if err := t.assertWritable(); err != nil {
		return fmt.Errorf("DeleteTx: assert writable: %w", err)
	}
	key := types.TxBlobKey(txHash)
	t.stageDelete(key)
	return nil
}

func (t *s3Txn) Commit() error {
	if t.finished {
		return nil
	}
	ctx, cancel := t.store.opContext()
	defer cancel()
	keys := make([]string, 0, len(t.pending))
	for key := range t.pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	type original struct {
		key     string
		value   []byte
		existed bool
	}
	originals := make([]original, 0, len(keys))
	for _, key := range keys {
		value, err := t.store.getInternal(ctx, key)
		if err != nil && !isS3NotFound(err) {
			t.finished = true
			return err
		}
		originals = append(originals, original{
			key:     key,
			value:   value,
			existed: err == nil,
		})
	}
	restore := func(items []original) {
		for i := len(items) - 1; i >= 0; i-- {
			item := items[i]
			var err error
			if item.existed {
				err = t.store.Put(ctx, item.key, item.value)
			} else {
				err = t.store.deleteObject(ctx, item.key)
				if errors.Is(err, types.ErrBlobKeyNotFound) {
					err = nil
				}
			}
			if err != nil {
				t.store.logger.Errorf(
					"failed to restore S3 transaction key %q: %v",
					item.key,
					err,
				)
			}
		}
	}
	for i, key := range keys {
		change := t.pending[key]
		var err error
		if change.deleted {
			err = t.store.deleteObject(ctx, key)
			if errors.Is(err, types.ErrBlobKeyNotFound) {
				err = nil
			}
		} else {
			err = t.store.Put(ctx, key, change.value)
		}
		if err != nil {
			restore(originals[:i])
			t.finished = true
			return fmt.Errorf("commit S3 blob transaction: %w", err)
		}
	}
	t.finished = true
	return nil
}

func (t *s3Txn) Rollback() error {
	if t.finished {
		return nil
	}
	t.finished = true
	t.pending = nil
	return nil
}

func (t *s3Txn) RollbackIsNoop() bool {
	return true
}

type s3Iterator struct {
	store   *BlobStoreS3
	keys    []string
	idx     int
	reverse bool
	err     error
	txn     types.Txn
}

func (it *s3Iterator) Rewind() {
	it.idx = 0
}

func (it *s3Iterator) Seek(prefix []byte) {
	target := string(prefix)
	it.idx = len(it.keys)
	if it.reverse {
		for i, key := range it.keys {
			if key <= target {
				it.idx = i
				break
			}
		}
		return
	}
	for i, key := range it.keys {
		if key >= target {
			it.idx = i
			break
		}
	}
}

func (it *s3Iterator) Valid() bool {
	return it.err == nil && it.idx < len(it.keys)
}

func (it *s3Iterator) ValidForPrefix(prefix []byte) bool {
	if !it.Valid() {
		return false
	}
	return strings.HasPrefix(it.keys[it.idx], string(prefix))
}

func (it *s3Iterator) Next() {
	if it.idx < len(it.keys) {
		it.idx++
	}
}

func (it *s3Iterator) Item() types.BlobItem {
	if !it.Valid() {
		return nil
	}
	return &s3Item{store: it.store, key: it.keys[it.idx], txn: it.txn}
}

// Err surfaces any iterator initialization error (e.g. listKeys failures).
func (it *s3Iterator) Err() error {
	return it.err
}

func (it *s3Iterator) Close() {}

type s3ErrorIterator struct {
	err error
}

func (it *s3ErrorIterator) Rewind()                      {}
func (it *s3ErrorIterator) Seek(prefix []byte)           {}
func (it *s3ErrorIterator) Valid() bool                  { return false }
func (it *s3ErrorIterator) ValidForPrefix(p []byte) bool { return false }
func (it *s3ErrorIterator) Next()                        {}
func (it *s3ErrorIterator) Item() types.BlobItem         { return nil }
func (it *s3ErrorIterator) Close()                       {}
func (it *s3ErrorIterator) Err() error                   { return it.err }

type s3Item struct {
	store *BlobStoreS3
	key   string
	txn   types.Txn
}

func (i *s3Item) Key() []byte {
	return []byte(i.key)
}

func (i *s3Item) ValueCopy(dst []byte) ([]byte, error) {
	data, err := i.store.Get(i.txn, []byte(i.key))
	if err != nil {
		return nil, err
	}
	if types.IsBlockTombstone(data) {
		// Tombstones live at fully-formed bp keys; parse this item's
		// own key (the plugin produced it) to attach (slot, hash) to
		// the typed error.
		slot, hash, parseErr := types.ParseBlockBlobKey([]byte(i.key))
		if parseErr != nil {
			return nil, fmt.Errorf(
				"history expiry marker at unexpected key shape: %w",
				parseErr,
			)
		}
		return nil, &types.HistoryExpiredError{Slot: slot, Hash: hash}
	}
	if dst != nil {
		return append(dst[:0], data...), nil
	}
	return data, nil
}

func isS3NotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey" {
		return true
	}
	var noSuchKey *s3types.NoSuchKey
	return errors.As(err, &noSuchKey)
}

func (d *BlobStoreS3) listKeysToFile(
	opts types.BlobIteratorOptions,
) (*reverseKeyFile, error) {
	file, err := os.CreateTemp("", "dingo-s3-iterator-")
	if err != nil {
		return nil, err
	}
	cleanup := func(err error) error {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return err
	}
	ctx, cancel := d.opContext()
	defer cancel()
	prefix := d.fullKey(string(opts.Prefix))
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(d.bucket),
	}
	if prefix != "" {
		input.Prefix = aws.String(prefix)
	} else if d.prefix != "" {
		input.Prefix = aws.String(d.prefix)
	}
	paginator := s3.NewListObjectsV2Paginator(d.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, cleanup(err)
		}
		for _, obj := range page.Contents {
			key := strings.TrimPrefix(aws.ToString(obj.Key), d.prefix)
			externalKey, err := hex.DecodeString(key)
			if err != nil {
				return nil, cleanup(fmt.Errorf("error decoding s3 key: %w", err))
			}
			if err := writeReverseKey(file, string(externalKey)); err != nil {
				return nil, cleanup(err)
			}
		}
	}
	return &reverseKeyFile{file: file}, nil
}

type reverseKeyFile struct {
	file        *os.File
	pos         int64
	initialized bool
}

func writeReverseKey(file *os.File, key string) error {
	if len(key) > math.MaxUint32 {
		return fmt.Errorf("key length %d exceeds uint32 maximum", len(key))
	}
	length := make([]byte, 4)
	// #nosec G115 -- length is bounds-checked against math.MaxUint32 above.
	binary.BigEndian.PutUint32(length, uint32(len(key)))
	if _, err := file.Write(length); err != nil {
		return err
	}
	if _, err := file.WriteString(key); err != nil {
		return err
	}
	_, err := file.Write(length)
	return err
}

func (f *reverseKeyFile) nextReverse() (string, bool, error) {
	if !f.initialized {
		info, err := f.file.Stat()
		if err != nil {
			return "", false, err
		}
		f.pos = info.Size()
		f.initialized = true
	}
	if f.pos == 0 {
		return "", false, nil
	}
	trailer := make([]byte, 4)
	if _, err := f.file.ReadAt(trailer, f.pos-4); err != nil {
		return "", false, err
	}
	length := int64(binary.BigEndian.Uint32(trailer))
	start := f.pos - 4 - length - 4
	key := make([]byte, length)
	if _, err := f.file.ReadAt(key, start+4); err != nil {
		return "", false, err
	}
	f.pos = start
	return string(key), true, nil
}

func (d *BlobStoreS3) init() error {
	// Configure metrics
	if d.promRegistry != nil {
		d.registerBlobMetrics()
	}

	// Close the startup context so that initialization will succeed.
	if d.startupCancel != nil {
		d.startupCancel()
		d.startupCancel = nil
	}
	return nil
}

// Returns the S3 client.
func (d *BlobStoreS3) Client() *s3.Client {
	return d.client
}

// Returns the bucket handle.
func (d *BlobStoreS3) Bucket() string {
	return d.bucket
}

// Returns the S3 key with an optional prefix.
func (d *BlobStoreS3) fullKey(key string) string {
	return d.prefix + hex.EncodeToString([]byte(key))
}

// getInternal reads the value at key.
func (d *BlobStoreS3) getInternal(
	ctx context.Context,
	key string,
) ([]byte, error) {
	out, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &d.bucket,
		Key:    new(d.fullKey(key)),
	})
	if err != nil {
		if !isS3NotFound(err) {
			d.logger.Errorf("s3 get %q failed: %v", key, err)
		}
		return nil, err
	}
	defer out.Body.Close()

	data, err := readBlobBody(out.Body)
	if err != nil {
		d.logger.Errorf("s3 read %q failed: %v", key, err)
		return nil, err
	}
	d.logger.Debugf("s3 get %q ok (%d bytes)", key, len(data))
	return data, nil
}

// Put writes a value to key.
func (d *BlobStoreS3) Put(ctx context.Context, key string, value []byte) error {
	_, err := d.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &d.bucket,
		Key:    new(d.fullKey(key)),
		Body:   bytes.NewReader(value),
	})
	if err != nil {
		d.logger.Errorf("s3 put %q failed: %v", key, err)
		return err
	}
	d.logger.Debugf("s3 put %q ok (%d bytes)", key, len(value))
	return nil
}

// Start implements the plugin.Plugin interface.
func (d *BlobStoreS3) Start() error {
	// Validate required fields
	if d.bucket == "" {
		return errors.New("s3 blob: bucket not set")
	}

	// Use configured timeout or default to 60 seconds for better reliability
	timeout := d.timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	// Load AWS config
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		cancel()
		return fmt.Errorf("s3 blob: load default AWS config: %w", err)
	}

	// Override region if specified
	if d.region != "" {
		awsCfg.Region = d.region
	}

	if d.endpoint != "" {
		awsCfg.BaseEndpoint = &d.endpoint
	}

	// When pointing at a custom endpoint (typically Minio or another
	// S3-compatible target), force path-style addressing. Virtual-hosted
	// style requires DNS for "<bucket>.<endpoint>", which custom
	// endpoints cannot satisfy.
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if d.endpoint != "" {
			o.UsePathStyle = true
		}
	})

	d.client = client
	d.startupCtx = ctx
	d.startupCancel = cancel

	if err := d.init(); err != nil {
		cancel()
		d.startupCancel = nil
		return err
	}
	return nil
}

// Stop implements the plugin.Plugin interface.
func (d *BlobStoreS3) Stop() error {
	// S3 client doesn't need explicit closing
	return nil
}

func (d *BlobStoreS3) GetBlockURL(
	ctx context.Context,
	txn types.Txn,
	point ocommon.Point,
) (types.SignedURL, types.BlockMetadata, error) {
	if _, err := d.validateTxn(txn); err != nil {
		return types.SignedURL{}, types.BlockMetadata{},
			fmt.Errorf("s3: invalid transaction: %w", err)
	}

	key := types.BlockBlobKey(point.Slot, point.Hash)

	metadataKey := types.BlockBlobMetadataKey(key)
	metadataBytes, err := d.getInternal(ctx, string(metadataKey))
	if err != nil {
		if isS3NotFound(err) {
			return types.SignedURL{}, types.BlockMetadata{},
				fmt.Errorf("s3: block metadata not found for key: %w",
					errors.Join(err, types.ErrBlobKeyNotFound))
		}
		return types.SignedURL{}, types.BlockMetadata{},
			fmt.Errorf("s3: failed getting block metadata: %w", err)
	}
	var tmpMetadata types.BlockMetadata
	if _, err := cbor.Decode(metadataBytes, &tmpMetadata); err != nil {
		return types.SignedURL{}, types.BlockMetadata{},
			fmt.Errorf("s3: failed decoding block metadata: %w", err)
	}

	_, err = d.client.HeadObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket: &d.bucket,
			Key:    new(d.fullKey(string(key))),
		})
	if isS3NotFound(err) {
		return types.SignedURL{}, types.BlockMetadata{},
			fmt.Errorf("s3 blob: object %q not found: %w",
				d.fullKey(string(key)),
				errors.Join(err, types.ErrBlobKeyNotFound))
	}
	if err != nil {
		return types.SignedURL{}, types.BlockMetadata{},
			fmt.Errorf("s3 blob: head object %q failed: %w",
				d.fullKey(string(key)), err)
	}

	presignClient := s3.NewPresignClient(d.client)
	presignedURL, err := presignClient.PresignGetObject(
		ctx,
		&s3.GetObjectInput{
			Bucket: &d.bucket,
			Key:    new(d.fullKey(string(key))),
		},
		s3.WithPresignExpires(time.Hour))
	if err != nil {
		return types.SignedURL{}, types.BlockMetadata{},
			fmt.Errorf("s3: failed to generate presigned url: %w", err)
	}

	u, err := url.Parse(presignedURL.URL)
	if err != nil {
		return types.SignedURL{}, types.BlockMetadata{},
			fmt.Errorf("s3: failed to parse presigned url: %w", err)
	}

	signedURL := types.SignedURL{
		URL:     *u,
		Expires: time.Now().Add(time.Hour),
	}

	metadata := types.BlockMetadata{
		Type:     tmpMetadata.Type,
		Height:   tmpMetadata.Height,
		PrevHash: tmpMetadata.PrevHash,
	}

	return signedURL, metadata, nil
}
