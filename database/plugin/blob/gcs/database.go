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

package gcs

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/blinklabs-io/dingo/database/plugin/blob/internal/compensate"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/blockverify"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// BlobStoreGCS stores data in a Google Cloud Storage bucket.
type BlobStoreGCS struct {
	promRegistry  prometheus.Registerer
	startupCtx    context.Context
	logger        *GcsLogger
	client        *storage.Client
	bucket        *storage.BucketHandle
	startupCancel context.CancelFunc
	bucketName    string
	timeout       time.Duration
}

const maxBlobReadBytes int64 = 256 << 20

func readBlobObject(r io.Reader) ([]byte, error) {
	return readBlobObjectWithLimit(r, maxBlobReadBytes)
}

func readBlobObjectWithLimit(r io.Reader, maxBytes int64) ([]byte, error) {
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

// gcsTxn stages GCS operations until commit. Commit applies the staged object
// changes in a deterministic order and compensates already-applied changes if
// a later cloud operation fails.
type gcsTxn struct {
	store     *BlobStoreGCS
	finished  bool
	readWrite bool
	pending   map[string]gcsPendingChange
}

type gcsPendingChange struct {
	value   []byte
	deleted bool
}

// New creates a new GCS-backed blob store.
func New(
	dataDir string,
	logger *slog.Logger,
	promRegistry prometheus.Registerer,
) (*BlobStoreGCS, error) {
	const prefix = "gcs://"
	var bucketName string
	if after, ok := strings.CutPrefix(dataDir, prefix); ok {
		bucketName = after
	}
	if bucketName == "" {
		return nil, errors.New(
			"gcs blob: bucket not set (expected dataDir='gcs://<bucket>')",
		)
	}

	return NewWithOptions(
		WithBucket(bucketName),
		WithLogger(logger),
		WithPromRegistry(promRegistry),
	)
}

// NewWithOptions creates a new GCS-backed blob store using options.
func NewWithOptions(opts ...BlobStoreGCSOptionFunc) (*BlobStoreGCS, error) {
	db := &BlobStoreGCS{}

	// Apply options
	for _, opt := range opts {
		opt(db)
	}

	// Set defaults
	if db.logger == nil {
		db.logger = NewGcsLogger(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	}

	return db, nil
}

func (d *BlobStoreGCS) fullKey(key string) string {
	return hex.EncodeToString([]byte(key))
}

func (d *BlobStoreGCS) externalKey(key string) (string, error) {
	b, err := hex.DecodeString(key)
	if err != nil {
		return "", fmt.Errorf("failed decoding hex key %q: %w", key, err)
	}
	return string(b), nil
}

func (d *BlobStoreGCS) object(key []byte) *storage.ObjectHandle {
	return d.bucket.Object(d.fullKey(string(key)))
}

func (d *BlobStoreGCS) objects(
	ctx context.Context,
	prefix []byte,
) *storage.ObjectIterator {
	return d.bucket.Objects(
		ctx,
		&storage.Query{Prefix: d.fullKey(string(prefix))},
	)
}

func (d *BlobStoreGCS) opContext() (context.Context, context.CancelFunc) {
	timeout := d.timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	return context.WithTimeout(
		context.Background(),
		timeout,
	) //nolint:gosec // G118: cancel func is returned to caller
}

// Close closes the GCS client.
func (d *BlobStoreGCS) Close() error {
	if d.client == nil {
		return nil
	}
	err := d.client.Close()
	d.client = nil
	return err
}

// DiskSize returns 0 for cloud-backed stores.
func (d *BlobStoreGCS) DiskSize() (int64, error) {
	return 0, nil
}

// Sync is a no-op: a GCS object is durable once its writer has been closed
// successfully, so a committed write needs no additional flush.
func (d *BlobStoreGCS) Sync() error {
	return nil
}

// NewTransaction returns a lightweight transaction wrapper.
func (d *BlobStoreGCS) NewTransaction(readWrite bool) types.Txn {
	return &gcsTxn{
		store:     d,
		readWrite: readWrite,
		pending:   make(map[string]gcsPendingChange),
	}
}

func (t *gcsTxn) assertWritable() error {
	if !t.readWrite {
		return errors.New("transaction is read-only")
	}
	return nil
}

func (d *BlobStoreGCS) validateTxn(txn types.Txn) error {
	if txn == nil {
		return types.ErrNilTxn
	}
	t, ok := txn.(*gcsTxn)
	if !ok || t.store != d {
		return types.ErrTxnWrongType
	}
	if t.finished {
		return errors.New("transaction already finished")
	}
	if d.bucket == nil || d.client == nil {
		return types.ErrBlobStoreUnavailable
	}
	return nil
}

func (t *gcsTxn) stageSet(key, value []byte) {
	t.pending[string(key)] = gcsPendingChange{
		value: append([]byte(nil), value...),
	}
}

func (t *gcsTxn) stageDelete(key []byte) {
	t.pending[string(key)] = gcsPendingChange{deleted: true}
}

// stagedDeleted reports whether the transaction has staged a delete for key.
// Iterators skip such keys: listing one would surface a key whose value reads
// back as missing, since the value path resolves staged changes.
func stagedDeleted(txn types.Txn, key string) bool {
	t, ok := txn.(*gcsTxn)
	if !ok || t.finished || t.pending == nil {
		return false
	}
	change, staged := t.pending[key]
	return staged && change.deleted
}

// stagedValue reports this transaction's staged state for key. deleted is
// reported separately from value because a staged zero-length write is a real
// value, not a deletion: collapsing the two on a nil value would make Set of an
// empty blob read back as missing until commit.
func (t *gcsTxn) stagedValue(key []byte) (value []byte, deleted, staged bool) {
	change, ok := t.pending[string(key)]
	if !ok {
		return nil, false, false
	}
	if change.deleted {
		return nil, true, true
	}
	return append([]byte{}, change.value...), false, true
}

func (d *BlobStoreGCS) readObject(
	ctx context.Context,
	key []byte,
) ([]byte, error) {
	r, err := d.object(key).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, types.ErrBlobKeyNotFound
		}
		return nil, err
	}
	defer r.Close()
	return readBlobObject(r)
}

// spoolPriorValue streams key's current object into the compensation log. The
// reader is copied straight to disk rather than through readBlobObject: that cap
// bounds memory for ordinary reads, and applying it here would make an object
// larger than the cap impossible to overwrite or delete inside a transaction.
func (d *BlobStoreGCS) spoolPriorValue(
	ctx context.Context,
	comp *compensate.Log,
	key []byte,
) error {
	r, err := d.object(key).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return types.ErrBlobKeyNotFound
		}
		return err
	}
	defer r.Close()
	return comp.RecordValueFrom(string(key), r)
}

func (d *BlobStoreGCS) writeObject(
	ctx context.Context,
	key, value []byte,
) error {
	w := d.object(key).NewWriter(ctx)
	if _, err := w.Write(value); err != nil {
		return joinCloseErr(err, w)
	}
	return w.Close()
}

// writeObjectStream writes key from a reader. Compensation uses this so
// restoring a large prior value streams out of the spool file instead of being
// materialized in memory.
func (d *BlobStoreGCS) writeObjectStream(
	ctx context.Context,
	key []byte,
	body io.Reader,
) error {
	w := d.object(key).NewWriter(ctx)
	if _, err := io.Copy(w, body); err != nil {
		return joinCloseErr(err, w)
	}
	return w.Close()
}

// joinCloseErr closes closer and folds any close error into err, so a
// writer that fails to close on an already-failing write path isn't
// silently lost.
func joinCloseErr(err error, closer io.Closer) error {
	if closeErr := closer.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}

func (d *BlobStoreGCS) deleteObject(
	ctx context.Context,
	key []byte,
) error {
	err := d.object(key).Delete(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return types.ErrBlobKeyNotFound
	}
	return err
}

// objectExists reports whether key has an object, without downloading it. Used
// where only presence matters, so object size never determines memory use and a
// blob larger than the read cap stays deletable.
func (d *BlobStoreGCS) objectExists(
	ctx context.Context,
	key []byte,
) (bool, error) {
	if _, err := d.object(key).Attrs(ctx); err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// resolveKey reads key through the transaction: a value staged by this
// transaction wins over the bucket, and a staged delete reads as missing. Every
// typed getter goes through this so a read-after-write inside one transaction
// behaves like the badger plugin instead of returning pre-transaction state.
func (d *BlobStoreGCS) resolveKey(
	ctx context.Context,
	t *gcsTxn,
	key []byte,
) ([]byte, error) {
	if value, deleted, staged := t.stagedValue(key); staged {
		if deleted {
			return nil, types.ErrBlobKeyNotFound
		}
		return value, nil
	}
	value, err := d.readObject(ctx, key)
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return nil, types.ErrBlobKeyNotFound
		}
		wrappedErr := fmt.Errorf(
			"read object %q from bucket %q: %w",
			string(key),
			d.bucketName,
			err,
		)
		d.logger.Errorf("%v", wrappedErr)
		return nil, wrappedErr
	}
	return value, nil
}

// Get retrieves a value from GCS within a transaction
func (d *BlobStoreGCS) Get(txn types.Txn, key []byte) ([]byte, error) {
	if err := d.validateTxn(txn); err != nil {
		return nil, err
	}
	t := txn.(*gcsTxn) // safe after validateTxn
	ctx, cancel := d.opContext()
	defer cancel()
	return d.resolveKey(ctx, t, key)
}

// Set stores a key-value pair in GCS within a transaction
func (d *BlobStoreGCS) Set(txn types.Txn, key, val []byte) error {
	if err := d.validateTxn(txn); err != nil {
		return err
	}
	t := txn.(*gcsTxn) // safe after validateTxn
	if err := t.assertWritable(); err != nil {
		return err
	}
	t.stageSet(key, val)
	return nil
}

// Delete removes a key from GCS within a transaction
func (d *BlobStoreGCS) Delete(txn types.Txn, key []byte) error {
	if err := d.validateTxn(txn); err != nil {
		return err
	}
	t := txn.(*gcsTxn) // safe after validateTxn
	if err := t.assertWritable(); err != nil {
		return err
	}
	if _, deleted, staged := t.stagedValue(key); staged {
		if deleted {
			return types.ErrBlobKeyNotFound
		}
		t.stageDelete(key)
		return nil
	}
	ctx, cancel := d.opContext()
	defer cancel()
	// Probe with object metadata rather than a bounded read: a blob larger
	// than the read cap must still be deletable.
	exists, err := d.objectExists(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return types.ErrBlobKeyNotFound
	}
	t.stageDelete(key)
	return nil
}

// NewIterator creates an iterator for GCS within a transaction.
//
// Important: items returned by the iterator's `Item()` must only be
// accessed while the transaction used to create the iterator is still
// active. Implementations may validate transaction state at access time
// (for example `ValueCopy` may fail if the transaction has been committed
// or rolled back). Typical usage iterates and accesses item values within
// the same transaction scope.
func (d *BlobStoreGCS) NewIterator(
	txn types.Txn,
	opts types.BlobIteratorOptions,
) types.BlobIterator {
	if err := d.validateTxn(txn); err != nil {
		return &gcsErrorIterator{err: err}
	}
	if !opts.Reverse {
		iterator := &gcsStreamIterator{
			store:  d,
			txn:    txn,
			prefix: opts.Prefix,
		}
		iterator.Rewind()
		return iterator
	}
	reverseKeys, err := d.listKeysToFile(opts)
	if err != nil {
		d.logger.Errorf("gcs list failed: %v", err)
		return &gcsIterator{
			store:   d,
			txn:     txn,
			keys:    []string{},
			reverse: opts.Reverse,
			err:     err,
		}
	}
	iterator := &gcsReverseIterator{
		store: d,
		txn:   txn,
		keys:  reverseKeys,
	}
	iterator.Rewind()
	return iterator
}

type gcsStreamIterator struct {
	store  *BlobStoreGCS
	txn    types.Txn
	prefix []byte
	iter   *storage.ObjectIterator
	cancel context.CancelFunc
	key    string
	valid  bool
	err    error
}

func (it *gcsStreamIterator) reset(start []byte) {
	if it.cancel != nil {
		it.cancel()
	}
	ctx, cancel := it.store.opContext()
	it.cancel = cancel
	query := &storage.Query{Prefix: it.store.fullKey(string(it.prefix))}
	if start != nil {
		query.StartOffset = it.store.fullKey(string(start))
	}
	it.iter = it.store.bucket.Objects(ctx, query)
	it.key = ""
	it.valid = false
	it.err = nil
	it.advance()
}

func (it *gcsStreamIterator) advance() {
	if it.err != nil || it.iter == nil {
		return
	}
	for {
		objAttrs, err := it.iter.Next()
		if errors.Is(err, iterator.Done) {
			it.valid = false
			return
		}
		if err != nil {
			it.err = err
			it.valid = false
			return
		}
		key, err := it.store.externalKey(objAttrs.Name)
		if err != nil {
			it.err = err
			it.valid = false
			return
		}
		if stagedDeleted(it.txn, key) {
			continue
		}
		it.key = key
		it.valid = true
		return
	}
}

func (it *gcsStreamIterator) Rewind() { it.reset(nil) }

func (it *gcsStreamIterator) Seek(prefix []byte) { it.reset(prefix) }

func (it *gcsStreamIterator) Valid() bool {
	return it.err == nil && it.valid
}

func (it *gcsStreamIterator) ValidForPrefix(prefix []byte) bool {
	return it.Valid() && strings.HasPrefix(it.key, string(prefix))
}

func (it *gcsStreamIterator) Next() { it.advance() }

func (it *gcsStreamIterator) Item() types.BlobItem {
	if !it.Valid() {
		return nil
	}
	return &gcsItem{store: it.store, txn: it.txn, key: it.key}
}

func (it *gcsStreamIterator) Close() {
	if it.cancel != nil {
		it.cancel()
		it.cancel = nil
	}
	it.iter = nil
	it.valid = false
}

func (it *gcsStreamIterator) Err() error { return it.err }

type gcsReverseIterator struct {
	store *BlobStoreGCS
	txn   types.Txn
	keys  *reverseKeyFile
	key   string
	valid bool
	err   error
}

func (it *gcsReverseIterator) advance() {
	for {
		key, valid, err := it.keys.nextReverse()
		if err != nil {
			it.err = err
			it.valid = false
			return
		}
		if valid && stagedDeleted(it.txn, key) {
			continue
		}
		it.key = key
		it.valid = valid
		return
	}
}

func (it *gcsReverseIterator) Rewind() {
	it.keys.pos = 0
	it.keys.initialized = false
	it.valid = false
	it.advance()
}

func (it *gcsReverseIterator) Seek(prefix []byte) {
	it.Rewind()
	for it.Valid() && it.key > string(prefix) {
		it.advance()
	}
}

func (it *gcsReverseIterator) Valid() bool {
	return it.err == nil && it.valid
}

func (it *gcsReverseIterator) ValidForPrefix(prefix []byte) bool {
	return it.Valid() && strings.HasPrefix(it.key, string(prefix))
}

func (it *gcsReverseIterator) Next() { it.advance() }

func (it *gcsReverseIterator) Item() types.BlobItem {
	if !it.Valid() {
		return nil
	}
	return &gcsItem{store: it.store, txn: it.txn, key: it.key}
}

func (it *gcsReverseIterator) Close() {
	if it.keys != nil && it.keys.file != nil {
		name := it.keys.file.Name()
		_ = it.keys.file.Close()
		_ = os.Remove(name)
		it.keys.file = nil
	}
	it.valid = false
}

func (it *gcsReverseIterator) Err() error { return it.err }

// SetBlock stores a block with its metadata and index
func (d *BlobStoreGCS) SetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
	cborData []byte,
	id uint64,
	blockType uint,
	height uint64,
	prevHash []byte,
) error {
	if err := d.validateTxn(txn); err != nil {
		return err
	}
	t := txn.(*gcsTxn) // safe after validateTxn
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
func (d *BlobStoreGCS) GetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	if err := d.validateTxn(txn); err != nil {
		return nil, types.BlockMetadata{}, err
	}
	t := txn.(*gcsTxn) // safe after validateTxn
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.BlockBlobKey(slot, hash)
	cborData, err := d.resolveKey(ctx, t, key)
	if err != nil {
		return nil, types.BlockMetadata{}, err
	}
	isTombstone := types.IsBlockTombstone(cborData)
	metadataKey := types.BlockBlobMetadataKey(key)
	metadataBytes, err := d.resolveKey(ctx, t, metadataKey)
	if err != nil {
		if isTombstone && errors.Is(err, types.ErrBlobKeyNotFound) {
			return nil, types.BlockMetadata{},
				&types.HistoryExpiredError{Slot: slot, Hash: hash}
		}
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			// Block content exists but metadata is missing - this indicates a partial write
			d.logger.Warningf(
				"block content exists but metadata is missing, possible partial write: key=%s metadataKey=%s",
				string(key),
				string(metadataKey),
			)
		}
		return nil, types.BlockMetadata{}, err
	}
	var tmpMetadata types.BlockMetadata
	if _, err := cbor.Decode(metadataBytes, &tmpMetadata); err != nil {
		return nil, types.BlockMetadata{}, err
	}
	if isTombstone {
		return nil, tmpMetadata,
			&types.HistoryExpiredError{Slot: slot, Hash: hash}
	}
	// GCS offers no content-addressing guarantee: re-derive the block's
	// identity from its bytes rather than trusting the (slot, hash) key
	// used to fetch it. (ID, Type) == (0, 0) marks a synthetic, non-block
	// entry sharing this same bp/bp..._metadata key layout
	// (SetGenesisCbor: genesis UTxO CBOR or a Leios endorser-block
	// manifest, neither of which is a decodable ledger block); a real
	// chain block never has both zero -- BlockCreate always assigns
	// ID >= 1 regardless of type. Checking both, not ID alone, keeps a
	// real block that somehow ended up with ID == 0 from silently
	// skipping verification instead of failing it.
	if tmpMetadata.ID != 0 || tmpMetadata.Type != 0 {
		if _, err := blockverify.Hash(
			tmpMetadata.Type,
			slot,
			cborData,
			hash,
		); err != nil {
			return nil, types.BlockMetadata{}, fmt.Errorf(
				"get block: slot %d: %w",
				slot,
				err,
			)
		}
	}
	return cborData, tmpMetadata, nil
}

// DeleteBlock removes a block and its associated data
func (d *BlobStoreGCS) DeleteBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
	id uint64,
) error {
	if err := d.validateTxn(txn); err != nil {
		return err
	}
	t := txn.(*gcsTxn) // safe after validateTxn
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
//   - bp_metadata: carries the local block ID, which Bark's archive does not
//     know and primary-chain membership checks require.
func (d *BlobStoreGCS) TombstoneBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) error {
	if err := d.validateTxn(txn); err != nil {
		return err
	}
	t := txn.(*gcsTxn) // safe after validateTxn
	if err := t.assertWritable(); err != nil {
		return err
	}
	key := types.BlockBlobKey(slot, hash)
	t.stageSet(key, types.BlockTombstone())
	return nil
}

// SetUtxo stores a UTxO's CBOR data
func (d *BlobStoreGCS) SetUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
	cborData []byte,
) error {
	if err := d.validateTxn(txn); err != nil {
		return err
	}
	t := txn.(*gcsTxn) // safe after validateTxn
	if err := t.assertWritable(); err != nil {
		return err
	}
	key := types.UtxoBlobKey(txId, outputIdx)
	t.stageSet(key, cborData)
	return nil
}

// GetUtxo retrieves a UTxO's CBOR data
func (d *BlobStoreGCS) GetUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) ([]byte, error) {
	if err := d.validateTxn(txn); err != nil {
		return nil, err
	}
	t := txn.(*gcsTxn) // safe after validateTxn
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.UtxoBlobKey(txId, outputIdx)
	return d.resolveKey(ctx, t, key)
}

// DeleteUtxo removes a UTxO's data
func (d *BlobStoreGCS) DeleteUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) error {
	if err := d.validateTxn(txn); err != nil {
		return err
	}
	t := txn.(*gcsTxn) // safe after validateTxn
	if err := t.assertWritable(); err != nil {
		return err
	}
	key := types.UtxoBlobKey(txId, outputIdx)
	t.stageDelete(key)
	return nil
}

// SetTx stores a transaction's offset data
func (d *BlobStoreGCS) SetTx(
	txn types.Txn,
	txHash []byte,
	offsetData []byte,
) error {
	if err := d.validateTxn(txn); err != nil {
		return fmt.Errorf("SetTx validateTxn failed: %w", err)
	}
	t := txn.(*gcsTxn) // safe after validateTxn
	if err := t.assertWritable(); err != nil {
		return fmt.Errorf("SetTx assertWritable failed: %w", err)
	}
	key := types.TxBlobKey(txHash)
	t.stageSet(key, offsetData)
	return nil
}

// GetTx retrieves a transaction's offset data
func (d *BlobStoreGCS) GetTx(
	txn types.Txn,
	txHash []byte,
) ([]byte, error) {
	if err := d.validateTxn(txn); err != nil {
		return nil, fmt.Errorf("GetTx validateTxn failed: %w", err)
	}
	t := txn.(*gcsTxn) // safe after validateTxn
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.TxBlobKey(txHash)
	return d.resolveKey(ctx, t, key)
}

// DeleteTx removes a transaction's offset data
func (d *BlobStoreGCS) DeleteTx(
	txn types.Txn,
	txHash []byte,
) error {
	if err := d.validateTxn(txn); err != nil {
		return fmt.Errorf("DeleteTx validateTxn failed: %w", err)
	}
	t := txn.(*gcsTxn) // safe after validateTxn
	if err := t.assertWritable(); err != nil {
		return fmt.Errorf("DeleteTx assertWritable failed: %w", err)
	}
	key := types.TxBlobKey(txHash)
	t.stageDelete(key)
	return nil
}

func (t *gcsTxn) Commit() error {
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

	// Build the compensation log before applying anything. Existence is probed
	// with object metadata rather than a full read, and a prior value is
	// downloaded only for the keys this commit overwrites or deletes -- spooled
	// to disk, so a multi-key block commit does not retain object payloads in
	// memory.
	comp, err := compensate.NewLog("dingo-gcs-commit-")
	if err != nil {
		t.finished = true
		return err
	}
	defer func() {
		if closeErr := comp.Close(); closeErr != nil {
			t.store.logger.Errorf(
				"failed to release GCS commit compensation log: %v",
				closeErr,
			)
		}
	}()
	for _, key := range keys {
		exists, err := t.store.objectExists(ctx, []byte(key))
		if err != nil {
			t.finished = true
			return fmt.Errorf(
				"commit GCS blob transaction: probe %q: %w",
				key,
				err,
			)
		}
		if !exists {
			comp.RecordMissing(key)
			continue
		}
		if err := t.store.spoolPriorValue(ctx, comp, []byte(key)); err != nil {
			if errors.Is(err, types.ErrBlobKeyNotFound) {
				// Raced with an external delete between probe and spool; the
				// undo for this key is a delete either way.
				comp.RecordMissing(key)
				continue
			}
			t.finished = true
			return fmt.Errorf(
				"commit GCS blob transaction: read prior value of %q: %w",
				key,
				err,
			)
		}
	}

	for i, key := range keys {
		change := t.pending[key]
		var err error
		if change.deleted {
			err = t.store.deleteObject(ctx, []byte(key))
			if errors.Is(err, types.ErrBlobKeyNotFound) {
				err = nil
			}
		} else {
			err = t.store.writeObject(ctx, []byte(key), change.value)
		}
		if err != nil {
			t.finished = true
			commitErr := fmt.Errorf("commit GCS blob transaction: %w", err)
			// Compensate on a fresh context. Reusing the commit context would
			// make every restore fail instantly when the commit failed because
			// that context expired, which is exactly when compensation matters.
			undoCtx, undoCancel := t.store.opContext()
			defer undoCancel()
			undoErr := comp.Undo(
				i,
				func(key string, value *io.SectionReader, _ int64) error {
					return t.store.writeObjectStream(
						undoCtx,
						[]byte(key),
						value,
					)
				},
				func(key string) error {
					delErr := t.store.deleteObject(undoCtx, []byte(key))
					if errors.Is(delErr, types.ErrBlobKeyNotFound) {
						return nil
					}
					return delErr
				},
			)
			if undoErr == nil {
				return commitErr
			}
			// The bucket is left partially applied. Report it as such instead
			// of only logging, so callers do not treat this as a clean abort.
			t.store.logger.Errorf(
				"failed to compensate GCS blob transaction: %v",
				undoErr,
			)
			return fmt.Errorf(
				"%w: %w (compensation failed: %w)",
				types.ErrPartialCommit,
				commitErr,
				undoErr,
			)
		}
	}
	t.finished = true
	return nil
}

func (t *gcsTxn) Rollback() error {
	if t.finished {
		return nil
	}
	t.finished = true
	t.pending = nil
	return nil
}

// RollbackIsNoop reports false: this transaction stages mutations and applies
// them only in Commit, so Rollback discards the staged work without issuing any
// GCS request. It reported true when Set/Delete wrote through immediately.
func (t *gcsTxn) RollbackIsNoop() bool {
	return false
}

type gcsIterator struct {
	store   *BlobStoreGCS
	txn     types.Txn
	keys    []string
	idx     int
	reverse bool
	err     error
}

// Note: Iterator items (gcsItem) must only be accessed while the transaction (txn)
// is still active. ValueCopy validates the transaction state at access time, so if
// the transaction is committed or rolled back before accessing an item, it will fail.
// This is a minor edge case since iterators are typically used within a single
// transaction scope, but callers should be aware of this constraint.

func (it *gcsIterator) Rewind() {
	it.idx = 0
}

func (it *gcsIterator) Seek(prefix []byte) {
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

func (it *gcsIterator) Valid() bool {
	return it.err == nil && it.idx < len(it.keys)
}

func (it *gcsIterator) ValidForPrefix(prefix []byte) bool {
	if !it.Valid() {
		return false
	}
	return strings.HasPrefix(it.keys[it.idx], string(prefix))
}

func (it *gcsIterator) Next() {
	if it.idx < len(it.keys) {
		it.idx++
	}
}

func (it *gcsIterator) Item() types.BlobItem {
	if !it.Valid() {
		return nil
	}
	return &gcsItem{store: it.store, txn: it.txn, key: it.keys[it.idx]}
}

// Err surfaces any iterator initialization error (e.g. listKeys failures).
func (it *gcsIterator) Err() error {
	return it.err
}

func (it *gcsIterator) Close() {}

type gcsErrorIterator struct {
	err error
}

func (it *gcsErrorIterator) Rewind()                      {}
func (it *gcsErrorIterator) Seek(prefix []byte)           {}
func (it *gcsErrorIterator) Valid() bool                  { return false }
func (it *gcsErrorIterator) ValidForPrefix(p []byte) bool { return false }
func (it *gcsErrorIterator) Next()                        {}
func (it *gcsErrorIterator) Item() types.BlobItem         { return nil }
func (it *gcsErrorIterator) Close()                       {}
func (it *gcsErrorIterator) Err() error                   { return it.err }

type gcsItem struct {
	store *BlobStoreGCS
	txn   types.Txn
	key   string
}

func (i *gcsItem) Key() []byte {
	return []byte(i.key)
}

func (i *gcsItem) ValueCopy(dst []byte) ([]byte, error) {
	// Note: this will fail if the transaction has been committed or rolled back
	// between Item() and ValueCopy(), because Get validates the transaction
	// state at call time.
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

func (d *BlobStoreGCS) listKeysToFile(
	opts types.BlobIteratorOptions,
) (*reverseKeyFile, error) {
	file, err := os.CreateTemp("", "dingo-gcs-iterator-")
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
	iter := d.objects(ctx, opts.Prefix)
	for {
		objAttrs, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, cleanup(err)
		}

		externalKey, err := d.externalKey(objAttrs.Name)
		if err != nil {
			return nil, cleanup(err)
		}
		if err := writeReverseKey(file, externalKey); err != nil {
			return nil, cleanup(err)
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
	// len(key) is compared as int64 rather than directly against the
	// untyped constant math.MaxUint32: on a 32-bit platform int is 32
	// bits wide and that constant does not fit in it, so the naive
	// comparison fails to compile.
	if int64(len(key)) > math.MaxUint32 {
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

func (d *BlobStoreGCS) init() error {
	// Configure metrics
	if d.promRegistry != nil {
		d.registerBlobMetrics()
	}

	// Close the startup context so that initialization will succeed.
	if d.startupCancel != nil {
		d.startupCancel()
		d.startupCancel = nil
	}
	d.startupCtx = context.Background()
	return nil
}

// Returns the GCS client.
func (d *BlobStoreGCS) Client() *storage.Client {
	return d.client
}

// Returns the bucket handle.
func (d *BlobStoreGCS) Bucket() *storage.BucketHandle {
	return d.bucket
}

// Start implements the plugin.Plugin interface.
func (d *BlobStoreGCS) Start() error {
	// Validate required fields
	if d.bucketName == "" {
		return errors.New("gcs blob: bucket not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	clientOpts := make([]option.ClientOption, 0, 1)
	clientOpts = append(clientOpts, storage.WithDisabledClientMetrics())

	client, err := storage.NewGRPCClient(
		ctx,
		clientOpts...,
	)
	if err != nil {
		cancel()
		return fmt.Errorf(
			"gcs blob: failed in creating storage client: %w",
			err,
		)
	}

	d.client = client
	d.bucket = client.Bucket(d.bucketName)
	d.startupCtx = ctx
	d.startupCancel = cancel

	if err := d.init(); err != nil {
		// Clean up resources on init failure
		d.Close()
		return err
	}
	return nil
}

// Stop implements the plugin.Plugin interface.
func (d *BlobStoreGCS) Stop() error {
	return d.Close()
}

func (d *BlobStoreGCS) GetBlockURL(
	ctx context.Context,
	txn types.Txn,
	point ocommon.Point,
) (types.SignedURL, types.BlockMetadata, error) {
	if err := d.validateTxn(txn); err != nil {
		return types.SignedURL{}, types.BlockMetadata{},
			fmt.Errorf("gcs: invalid transaction: %w", err)
	}
	t := txn.(*gcsTxn) // safe after validateTxn

	key := types.BlockBlobKey(point.Slot, point.Hash)

	// A signed URL points at a bucket object, so it cannot represent a write
	// this transaction has staged but not committed. Reject that explicitly
	// rather than handing back a URL that would 404.
	if _, deleted, staged := t.stagedValue(key); staged {
		if deleted {
			return types.SignedURL{}, types.BlockMetadata{},
				fmt.Errorf("gcs: block deleted in this transaction: %w",
					types.ErrBlobKeyNotFound)
		}
		return types.SignedURL{}, types.BlockMetadata{}, fmt.Errorf(
			"gcs: block staged in this transaction is not signable until commit: %w",
			types.ErrBlobKeyNotFound,
		)
	}

	exists, err := d.objectExists(ctx, key)
	if err != nil {
		return types.SignedURL{}, types.BlockMetadata{},
			fmt.Errorf("gcs: failed getting object attributes: %w", err)
	}
	if !exists {
		return types.SignedURL{}, types.BlockMetadata{},
			fmt.Errorf("gcs: block not found: %w", types.ErrBlobKeyNotFound)
	}

	metadataKey := types.BlockBlobMetadataKey(key)
	metadataBytes, err := d.resolveKey(ctx, t, metadataKey)
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			// Block content exists but metadata is missing - this indicates a partial write
			d.logger.Warningf(
				"block content exists but metadata is missing, possible partial write: key=%s metadataKey=%s",
				string(key),
				string(metadataKey),
			)
		}
		wrappedErr := fmt.Errorf(
			"read object %q from bucket %q: %w",
			string(metadataKey),
			d.bucketName,
			err,
		)
		d.logger.Errorf("%v", wrappedErr)
		return types.SignedURL{}, types.BlockMetadata{}, wrappedErr
	}
	var tmpMetadata types.BlockMetadata
	if _, err := cbor.Decode(metadataBytes, &tmpMetadata); err != nil {
		return types.SignedURL{}, types.BlockMetadata{},
			fmt.Errorf("gcs: failed decoding metadata: %w", err)
	}

	expires := time.Now().Add(time.Hour)
	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  http.MethodGet,
		Expires: expires,
	}

	presignedURL, err := d.bucket.SignedURL(
		d.fullKey(string(key)),
		opts,
	)
	if err != nil {
		return types.SignedURL{}, types.BlockMetadata{},
			fmt.Errorf("gcs: failed to sign URL: %w", err)
	}

	u, err := url.Parse(presignedURL)
	if err != nil {
		return types.SignedURL{}, types.BlockMetadata{},
			fmt.Errorf("gcs: failed to parse URL: %w", err)
	}

	signedURL := types.SignedURL{
		URL:     *u,
		Expires: expires,
	}

	metadata := types.BlockMetadata{
		Type:     tmpMetadata.Type,
		Height:   tmpMetadata.Height,
		PrevHash: tmpMetadata.PrevHash,
	}

	return signedURL, metadata, nil
}
