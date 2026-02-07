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

package gcs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/blinklabs-io/dingo/database/types"
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

// gcsTxn wraps GCS operations to satisfy types.Txn and types.BlobTx
// Operations are not atomic but respect the transaction interface used by the
// database layer.
type gcsTxn struct {
	store     *BlobStoreGCS
	finished  bool
	readWrite bool
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

func (d *BlobStoreGCS) objects(ctx context.Context, prefix []byte) *storage.ObjectIterator {
	return d.bucket.Objects(ctx, &storage.Query{Prefix: d.fullKey(string(prefix))})
}

func (d *BlobStoreGCS) opContext() (context.Context, context.CancelFunc) {
	timeout := d.timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
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

// NewTransaction returns a lightweight transaction wrapper.
func (d *BlobStoreGCS) NewTransaction(readWrite bool) types.Txn {
	return &gcsTxn{store: d, readWrite: readWrite}
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

// Get retrieves a value from GCS within a transaction
func (d *BlobStoreGCS) Get(txn types.Txn, key []byte) ([]byte, error) {
	if err := d.validateTxn(txn); err != nil {
		return nil, err
	}
	ctx, cancel := d.opContext()
	defer cancel()
	r, err := d.object(key).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
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
	defer r.Close()

	ciphertext, err := io.ReadAll(r)
	if err != nil {
		wrappedErr := fmt.Errorf(
			"read object %q from bucket %q: %w",
			string(key),
			d.bucketName,
			err,
		)
		d.logger.Errorf("%v", wrappedErr)
		return nil, wrappedErr
	}
	return ciphertext, nil
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
	ctx, cancel := d.opContext()
	defer cancel()
	w := d.object(key).NewWriter(ctx)
	if _, err := w.Write(val); err != nil {
		_ = w.Close()
		d.logger.Errorf("failed to write object %q: %v", string(key), err)
		return err
	}
	if err := w.Close(); err != nil {
		d.logger.Errorf(
			"failed to close writer for %q: %v",
			string(key),
			err,
		)
		return err
	}
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
	ctx, cancel := d.opContext()
	defer cancel()
	if err := d.object(key).Delete(ctx); err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return types.ErrBlobKeyNotFound
		}
		d.logger.Errorf("gcs delete %q failed: %v", string(key), err)
		return err
	}
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
	keys, err := d.listKeys(opts)
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
	return &gcsIterator{store: d, txn: txn, keys: keys, reverse: opts.Reverse}
}

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
	ctx, cancel := d.opContext()
	defer cancel()

	// Track written objects for cleanup on failure
	var writtenObjects []string

	// Block content by point
	key := types.BlockBlobKey(slot, hash)
	w := d.object(key).NewWriter(ctx)
	if _, err := w.Write(cborData); err != nil {
		_ = w.Close()
		d.logger.Errorf("failed to write object %q: %v", string(key), err)
		return err
	}
	if err := w.Close(); err != nil {
		d.logger.Errorf("failed to close writer for %q: %v", string(key), err)
		return err
	}
	writtenObjects = append(writtenObjects, string(key))

	// Block index to point key
	indexKey := types.BlockBlobIndexKey(id)
	w = d.object(indexKey).NewWriter(ctx)
	if _, err := w.Write(key); err != nil {
		_ = w.Close()
		d.logger.Errorf("failed to write object %q: %v", string(indexKey), err)
		d.cleanupObjects(ctx, writtenObjects)
		return err
	}
	if err := w.Close(); err != nil {
		d.logger.Errorf(
			"failed to close writer for %q: %v",
			string(indexKey),
			err,
		)
		d.cleanupObjects(ctx, writtenObjects)
		return err
	}
	writtenObjects = append(writtenObjects, string(indexKey))

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
		d.cleanupObjects(ctx, writtenObjects)
		return err
	}
	w = d.object(metadataKey).NewWriter(ctx)
	if _, err := w.Write(tmpMetadataBytes); err != nil {
		_ = w.Close()
		d.logger.Errorf(
			"failed to write object %q: %v",
			string(metadataKey),
			err,
		)
		d.cleanupObjects(ctx, writtenObjects)
		return err
	}
	if err := w.Close(); err != nil {
		d.logger.Errorf(
			"failed to close writer for %q: %v",
			string(metadataKey),
			err,
		)
		d.cleanupObjects(ctx, writtenObjects)
		return err
	}

	return nil
}

// cleanupObjects deletes the specified objects from GCS, logging any failures
// but not treating them as fatal since cleanup is best-effort
func (d *BlobStoreGCS) cleanupObjects(
	ctx context.Context,
	objectNames []string,
) {
	for _, objName := range objectNames {
		if err := d.object([]byte(objName)).Delete(ctx); err != nil {
			if !errors.Is(err, storage.ErrObjectNotExist) {
				d.logger.Warningf(
					"failed to cleanup orphaned object during SetBlock failure: object=%s error=%v",
					objName,
					err,
				)
			}
			// Ignore ErrObjectNotExist as the object may have been deleted already
		} else {
			d.logger.Debugf(
				"cleaned up orphaned object during SetBlock failure: object=%s",
				objName,
			)
		}
	}
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
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.BlockBlobKey(slot, hash)
	r, err := d.object(key).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, types.BlockMetadata{}, types.ErrBlobKeyNotFound
		}
		wrappedErr := fmt.Errorf(
			"read object %q from bucket %q: %w",
			string(key),
			d.bucketName,
			err,
		)
		d.logger.Errorf("%v", wrappedErr)
		return nil, types.BlockMetadata{}, wrappedErr
	}
	defer r.Close()
	cborData, err := io.ReadAll(r)
	if err != nil {
		wrappedErr := fmt.Errorf(
			"read object %q from bucket %q: %w",
			string(key),
			d.bucketName,
			err,
		)
		d.logger.Errorf("%v", wrappedErr)
		return nil, types.BlockMetadata{}, wrappedErr
	}
	metadataKey := types.BlockBlobMetadataKey(key)
	r, err = d.object(metadataKey).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			// Block content exists but metadata is missing - this indicates a partial write
			d.logger.Warningf(
				"block content exists but metadata is missing, possible partial write: key=%s metadataKey=%s",
				string(key),
				string(metadataKey),
			)
			return nil, types.BlockMetadata{}, types.ErrBlobKeyNotFound
		}
		wrappedErr := fmt.Errorf(
			"read object %q from bucket %q: %w",
			string(metadataKey),
			d.bucketName,
			err,
		)
		d.logger.Errorf("%v", wrappedErr)
		return nil, types.BlockMetadata{}, wrappedErr
	}
	defer r.Close()
	metadataBytes, err := io.ReadAll(r)
	if err != nil {
		wrappedErr := fmt.Errorf(
			"read object %q from bucket %q: %w",
			string(metadataKey),
			d.bucketName,
			err,
		)
		d.logger.Errorf("%v", wrappedErr)
		return nil, types.BlockMetadata{}, wrappedErr
	}
	var tmpMetadata types.BlockMetadata
	if _, err := cbor.Decode(metadataBytes, &tmpMetadata); err != nil {
		return nil, types.BlockMetadata{}, err
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
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.BlockBlobKey(slot, hash)
	if err := d.object(key).Delete(ctx); err != nil &&
		!errors.Is(err, storage.ErrObjectNotExist) {
		d.logger.Errorf("gcs delete %q failed: %v", string(key), err)
		return err
	}
	indexKey := types.BlockBlobIndexKey(id)
	if err := d.object(indexKey).Delete(ctx); err != nil &&
		!errors.Is(err, storage.ErrObjectNotExist) {
		d.logger.Errorf("gcs delete %q failed: %v", string(indexKey), err)
		return err
	}
	metadataKey := types.BlockBlobMetadataKey(key)
	if err := d.object(metadataKey).Delete(ctx); err != nil &&
		!errors.Is(err, storage.ErrObjectNotExist) {
		d.logger.Errorf("gcs delete %q failed: %v", string(metadataKey), err)
		return err
	}
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
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.UtxoBlobKey(txId, outputIdx)
	w := d.object(key).NewWriter(ctx)
	if _, err := w.Write(cborData); err != nil {
		_ = w.Close()
		d.logger.Errorf("failed to write object %q: %v", string(key), err)
		return err
	}
	if err := w.Close(); err != nil {
		d.logger.Errorf("failed to close writer for %q: %v", string(key), err)
		return err
	}
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
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.UtxoBlobKey(txId, outputIdx)
	r, err := d.object(key).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
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
	defer r.Close()
	ciphertext, err := io.ReadAll(r)
	if err != nil {
		wrappedErr := fmt.Errorf(
			"read object %q from bucket %q: %w",
			string(key),
			d.bucketName,
			err,
		)
		d.logger.Errorf("%v", wrappedErr)
		return nil, wrappedErr
	}
	return ciphertext, nil
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
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.UtxoBlobKey(txId, outputIdx)
	if err := d.object(key).Delete(ctx); err != nil &&
		!errors.Is(err, storage.ErrObjectNotExist) {
		d.logger.Errorf("gcs delete %q failed: %v", string(key), err)
		return err
	}
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
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.TxBlobKey(txHash)
	w := d.object(key).NewWriter(ctx)
	if _, err := w.Write(offsetData); err != nil {
		_ = w.Close()
		wrappedErr := fmt.Errorf("gcs write %q failed: %w", string(key), err)
		d.logger.Errorf("%v", wrappedErr)
		return wrappedErr
	}
	if err := w.Close(); err != nil {
		wrappedErr := fmt.Errorf("gcs write close %q failed: %w", string(key), err)
		d.logger.Errorf("%v", wrappedErr)
		return wrappedErr
	}
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
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.TxBlobKey(txHash)
	r, err := d.object(key).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, types.ErrBlobKeyNotFound
		}
		wrappedErr := fmt.Errorf("gcs read %q failed: %w", string(key), err)
		d.logger.Errorf("%v", wrappedErr)
		return nil, wrappedErr
	}
	defer r.Close()
	ciphertext, err := io.ReadAll(r)
	if err != nil {
		wrappedErr := fmt.Errorf("gcs read body %q failed: %w", string(key), err)
		d.logger.Errorf("%v", wrappedErr)
		return nil, wrappedErr
	}
	return ciphertext, nil
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
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.TxBlobKey(txHash)
	if err := d.object(key).Delete(ctx); err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			// Object doesn't exist, treat as success
			return nil
		}
		wrappedErr := fmt.Errorf("gcs delete %q failed: %w", string(key), err)
		d.logger.Errorf("%v", wrappedErr)
		return wrappedErr
	}
	return nil
}

func (t *gcsTxn) Commit() error {
	if t.finished {
		return nil
	}
	t.finished = true
	return nil
}

func (t *gcsTxn) Rollback() error {
	if t.finished {
		return nil
	}
	t.finished = true
	return nil
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
	if dst != nil {
		return append(dst[:0], data...), nil
	}
	return data, nil
}

func (d *BlobStoreGCS) listKeys(
	opts types.BlobIteratorOptions,
) ([]string, error) {
	ctx, cancel := d.opContext()
	defer cancel()
	iter := d.objects(ctx, opts.Prefix)
	keys := make([]string, 0)
	for {
		objAttrs, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}

		externalKey, err := d.externalKey(objAttrs.Name)
		if err != nil {
			return nil, err
		}
		keys = append(keys, externalKey)
	}
	sort.Strings(keys)
	if opts.Reverse {
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	}
	return keys, nil
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

func (d *BlobStoreGCS) GetBlockURL(txn types.Txn, point ocommon.Point) (*url.URL, error) {
	return nil, errors.New("gcs: GetBlockURL not supported")
}
