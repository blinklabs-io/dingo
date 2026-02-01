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

package aws

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
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
	bucket        string
	prefix        string
	region        string
	timeout       time.Duration
}

// s3Txn wraps S3 operations to satisfy types.Txn and types.BlobTx
// Operations are not atomic but respect the transaction interface used by the
// database layer.
type s3Txn struct {
	store     *BlobStoreS3
	finished  bool
	readWrite bool
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
	return context.WithTimeout(context.Background(), timeout)
}

// Close implements the BlobStore interface.
func (d *BlobStoreS3) Close() error {
	return d.Stop()
}

// NewTransaction returns a lightweight transaction wrapper.
func (d *BlobStoreS3) NewTransaction(readWrite bool) types.Txn {
	return &s3Txn{store: d, readWrite: readWrite}
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

// Get retrieves a value from S3 within a transaction
func (d *BlobStoreS3) Get(txn types.Txn, key []byte) ([]byte, error) {
	if _, err := d.validateTxn(txn); err != nil {
		return nil, err
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
	ctx, cancel := d.opContext()
	defer cancel()
	if err := d.Put(ctx, string(key), val); err != nil {
		return err
	}
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
	ctx, cancel := d.opContext()
	defer cancel()
	_, err = d.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    awsString(d.fullKey(string(key))),
	})
	if err != nil {
		if isS3NotFound(err) {
			return types.ErrBlobKeyNotFound
		}
		d.logger.Errorf("s3 delete %q failed: %v", string(key), err)
		return err
	}
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
	keys, err := d.listKeys(opts)
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
	return &s3Iterator{store: d, keys: keys, reverse: opts.Reverse, txn: txn}
}

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
	ctx, cancel := d.opContext()
	defer cancel()
	// Block content by point
	key := types.BlockBlobKey(slot, hash)
	if err := d.Put(ctx, string(key), cborData); err != nil {
		return err
	}
	// Block index to point key
	indexKey := types.BlockBlobIndexKey(id)
	if err := d.Put(ctx, string(indexKey), key); err != nil {
		return err
	}
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
	if err := d.Put(ctx, string(metadataKey), tmpMetadataBytes); err != nil {
		return err
	}
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
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.BlockBlobKey(slot, hash)
	if _, err := d.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    awsString(d.fullKey(string(key))),
	}); err != nil && !isS3NotFound(err) {
		return err
	}
	indexKey := types.BlockBlobIndexKey(id)
	if _, err := d.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    awsString(d.fullKey(string(indexKey))),
	}); err != nil && !isS3NotFound(err) {
		return err
	}
	metadataKey := types.BlockBlobMetadataKey(key)
	if _, err := d.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    awsString(d.fullKey(string(metadataKey))),
	}); err != nil && !isS3NotFound(err) {
		return err
	}
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
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.UtxoBlobKey(txId, outputIdx)
	return d.Put(ctx, string(key), cborData)
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
	ctx, cancel := d.opContext()
	defer cancel()
	key := types.UtxoBlobKey(txId, outputIdx)
	_, err = d.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    awsString(d.fullKey(string(key))),
	})
	if err != nil && !isS3NotFound(err) {
		d.logger.Errorf("s3 delete %q failed: %v", string(key), err)
		return err
	}
	return nil
}

func (t *s3Txn) Commit() error {
	if t.finished {
		return nil
	}
	t.finished = true
	return nil
}

func (t *s3Txn) Rollback() error {
	if t.finished {
		return nil
	}
	t.finished = true
	return nil
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

func (d *BlobStoreS3) listKeys(
	opts types.BlobIteratorOptions,
) ([]string, error) {
	// TODO: Consider longer timeout or no timeout for large buckets with many pages
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
	keys := make([]string, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			key := strings.TrimPrefix(aws.ToString(obj.Key), d.prefix)
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if opts.Reverse {
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	}
	return keys, nil
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
	return d.prefix + key
}

func awsString(s string) *string {
	return &s
}

// getInternal reads the value at key.
func (d *BlobStoreS3) getInternal(
	ctx context.Context,
	key string,
) ([]byte, error) {
	out, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &d.bucket,
		Key:    awsString(d.fullKey(key)),
	})
	if err != nil {
		if !isS3NotFound(err) {
			d.logger.Errorf("s3 get %q failed: %v", key, err)
		}
		return nil, err
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		d.logger.Errorf("s3 read %q failed: %v", key, err)
		return nil, err
	}
	d.logger.Infof("s3 get %q ok (%d bytes)", key, len(data))
	return data, nil
}

// Put writes a value to key.
func (d *BlobStoreS3) Put(ctx context.Context, key string, value []byte) error {
	_, err := d.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &d.bucket,
		Key:    awsString(d.fullKey(key)),
		Body:   bytes.NewReader(value),
	})
	if err != nil {
		d.logger.Errorf("s3 put %q failed: %v", key, err)
		return err
	}
	d.logger.Infof("s3 put %q ok (%d bytes)", key, len(value))
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

	client := s3.NewFromConfig(awsCfg)

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

func (d *BlobStoreS3) GetBlockURL(txn types.Txn, point ocommon.Point) (*url.URL, error) {
	key := d.fullKey((string)(types.BlockBlobKey(point.Slot, point.Hash)))

	presignClient := s3.NewPresignClient(d.client)
	presignedURL, err := presignClient.PresignGetObject(
		context.TODO(),
		&s3.GetObjectInput{
			Bucket: &d.bucket,
			Key:    &key,
		},
		s3.WithPresignExpires(time.Minute))
	if err != nil {
		return nil, fmt.Errorf("s3: failed to generate presigned url: %w", err)
	}

	u, err := url.Parse(presignedURL.URL)
	if err != nil {
		return nil, fmt.Errorf("s3: failed to parse presigned url: %w", err)
	}

	return u, nil
}
