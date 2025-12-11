// Copyright 2025 Blink Labs Software
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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// BlobStoreGCS stores data in a Google Cloud Storage bucket.
type BlobStoreGCS struct {
	promRegistry    prometheus.Registerer
	startupCtx      context.Context
	logger          *GcsLogger
	client          *storage.Client
	bucket          *storage.BucketHandle
	startupCancel   context.CancelFunc
	bucketName      string
	credentialsFile string
}

// gcsTxn wraps GCS operations to satisfy types.Txn and types.BlobTx
// Operations are not atomic but respect the transaction interface used by the
// database layer.
type gcsTxn struct {
	store    *BlobStoreGCS
	finished bool
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

func (d *BlobStoreGCS) opContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
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
func (d *BlobStoreGCS) NewTransaction(_ bool) types.Txn {
	return &gcsTxn{store: d}
}

// Get retrieves a value from GCS within a transaction
func (d *BlobStoreGCS) Get(txn types.Txn, key []byte) ([]byte, error) {
	if _, ok := txn.(*gcsTxn); !ok {
		return nil, types.ErrTxnWrongType
	}
	if d.bucket == nil || d.client == nil {
		return nil, types.ErrBlobStoreUnavailable
	}
	ctx, cancel := d.opContext()
	if cancel != nil {
		defer cancel()
	}
	r, err := d.bucket.Object(string(key)).NewReader(ctx)
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
	if _, ok := txn.(*gcsTxn); !ok {
		return types.ErrTxnWrongType
	}
	if d.bucket == nil || d.client == nil {
		return types.ErrBlobStoreUnavailable
	}
	ctx, cancel := d.opContext()
	if cancel != nil {
		defer cancel()
	}
	w := d.bucket.Object(string(key)).NewWriter(ctx)
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
	if _, ok := txn.(*gcsTxn); !ok {
		return types.ErrTxnWrongType
	}
	if d.bucket == nil || d.client == nil {
		return types.ErrBlobStoreUnavailable
	}
	ctx, cancel := d.opContext()
	if cancel != nil {
		defer cancel()
	}
	if err := d.bucket.Object(string(key)).Delete(ctx); err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return types.ErrBlobKeyNotFound
		}
		return err
	}
	return nil
}

// NewIterator creates an iterator for GCS within a transaction
func (d *BlobStoreGCS) NewIterator(
	txn types.Txn,
	opts types.BlobIteratorOptions,
) types.BlobIterator {
	if _, ok := txn.(*gcsTxn); !ok {
		return nil
	}
	keys, err := d.listKeys(opts)
	if err != nil {
		d.logger.Errorf("gcs list failed: %v", err)
		return &gcsIterator{
			store:   d,
			keys:    []string{},
			reverse: opts.Reverse,
			err:     err,
		}
	}
	return &gcsIterator{store: d, keys: keys, reverse: opts.Reverse}
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
	keys    []string
	idx     int
	reverse bool
	err     error
}

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
	return &gcsItem{store: it.store, key: it.keys[it.idx]}
}

// Err surfaces any iterator initialization error (e.g. listKeys failures).
func (it *gcsIterator) Err() error {
	return it.err
}

func (it *gcsIterator) Close() {}

type gcsItem struct {
	store *BlobStoreGCS
	key   string
}

func (i *gcsItem) Key() []byte {
	return []byte(i.key)
}

func (i *gcsItem) ValueCopy(dst []byte) ([]byte, error) {
	// Create a temporary read-only transaction for this operation
	txn := i.store.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck

	data, err := i.store.Get(txn, []byte(i.key))
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
	if cancel != nil {
		defer cancel()
	}
	iter := d.bucket.Objects(ctx, &storage.Query{Prefix: string(opts.Prefix)})
	keys := make([]string, 0)
	for {
		objAttrs, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		keys = append(keys, objAttrs.Name)
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

	// Validate credentials file if specified
	if d.credentialsFile != "" {
		if err := validateCredentials(d.credentialsFile); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	var clientOpts []option.ClientOption
	clientOpts = append(clientOpts, storage.WithDisabledClientMetrics())
	if d.credentialsFile != "" {
		clientOpts = append(
			clientOpts,
			option.WithCredentialsFile(d.credentialsFile),
		)
	}

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
