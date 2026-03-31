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

package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/prometheus/client_golang/prometheus"
)

// badgerTxn wraps a badger transaction and implements types.Txn
type badgerTxn struct {
	store    *BlobStoreBadger
	tx       *badger.Txn
	finished bool
}

func newBadgerTxn(store *BlobStoreBadger, tx *badger.Txn) *badgerTxn {
	return &badgerTxn{store: store, tx: tx}
}

// validateTxn validates a types.Txn for this BlobStore and returns the
// underlying *badgerTxn if valid.
func (d *BlobStoreBadger) validateTxn(txn types.Txn) (*badgerTxn, error) {
	if txn == nil {
		return nil, types.ErrNilTxn
	}
	badgerTxn, ok := txn.(*badgerTxn)
	if !ok {
		return nil, types.ErrTxnWrongType
	}
	if badgerTxn.store != d {
		return nil, errors.New("transaction from different store")
	}
	if err := badgerTxn.validateTxn(); err != nil {
		return nil, err
	}
	return badgerTxn, nil
}

// validateTxn checks if the transaction is still valid for use
func (t *badgerTxn) validateTxn() error {
	if t.finished {
		return errors.New("transaction already finished")
	}
	if t.tx == nil {
		return types.ErrBlobStoreUnavailable
	}
	return nil
}

func (t *badgerTxn) Commit() error {
	if t.finished {
		return nil
	}
	if t.tx == nil {
		t.finished = true
		return nil
	}
	if err := t.tx.Commit(); err != nil {
		return err
	}
	t.finished = true
	return nil
}

func (t *badgerTxn) Rollback() error {
	if t.finished {
		return nil
	}
	if t.tx != nil {
		t.tx.Discard()
	}
	t.finished = true
	return nil
}

func (d *BlobStoreBadger) SetLogger(logger *slog.Logger) {
	if logger == nil {
		d.logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
		return
	}
	d.logger = logger
}

type badgerIterator struct {
	iter *badger.Iterator
}

func (it *badgerIterator) Rewind()            { it.iter.Rewind() }
func (it *badgerIterator) Seek(prefix []byte) { it.iter.Seek(prefix) }

func (it *badgerIterator) Valid() bool { return it.iter.Valid() }

func (it *badgerIterator) ValidForPrefix(
	p []byte,
) bool {
	return it.iter.ValidForPrefix(p)
}
func (it *badgerIterator) Next() { it.iter.Next() }

func (it *badgerIterator) Item() types.BlobItem { return &badgerItem{item: it.iter.Item()} }
func (it *badgerIterator) Close()               { it.iter.Close() }
func (it *badgerIterator) Err() error           { return nil }

type errorIterator struct {
	err error
}

func (it *errorIterator) Rewind()                      {}
func (it *errorIterator) Seek(prefix []byte)           {}
func (it *errorIterator) Valid() bool                  { return false }
func (it *errorIterator) ValidForPrefix(p []byte) bool { return false }
func (it *errorIterator) Next()                        {}
func (it *errorIterator) Item() types.BlobItem         { return nil }
func (it *errorIterator) Close()                       {}
func (it *errorIterator) Err() error                   { return it.err }

type badgerItem struct {
	item *badger.Item
}

var blockMetadataBinaryMagic = [4]byte{'D', 'B', 'M', '1'}

const blockMetadataPrevHashMaxLen = 32

func buildBlockBlobKey(dst []byte, slot uint64, hash []byte) {
	copy(dst, types.BlockBlobKeyPrefix)
	binary.BigEndian.PutUint64(
		dst[len(types.BlockBlobKeyPrefix):len(types.BlockBlobKeyPrefix)+8],
		slot,
	)
	copy(dst[len(types.BlockBlobKeyPrefix)+8:], hash)
}

func buildBlockBlobIndexKey(dst []byte, blockNumber uint64) {
	copy(dst, types.BlockBlobIndexKeyPrefix)
	binary.BigEndian.PutUint64(
		dst[len(types.BlockBlobIndexKeyPrefix):],
		blockNumber,
	)
}

func buildBlockBlobMetadataKey(dst []byte, baseKey []byte) {
	copy(dst, baseKey)
	copy(dst[len(baseKey):], types.BlockBlobMetadataKeySuffix)
}

func marshalBlockMetadataInto(
	dst []byte,
	metadata types.BlockMetadata,
) error {
	prevHashLen := len(metadata.PrevHash)
	if prevHashLen > blockMetadataPrevHashMaxLen {
		return fmt.Errorf(
			"invalid block metadata prev hash length: %d",
			prevHashLen,
		)
	}
	copy(dst[:4], blockMetadataBinaryMagic[:])
	binary.BigEndian.PutUint64(dst[4:12], metadata.ID)
	binary.BigEndian.PutUint64(dst[12:20], uint64(metadata.Type))
	binary.BigEndian.PutUint64(dst[20:28], metadata.Height)
	binary.BigEndian.PutUint32(
		dst[28:32],
		uint32(prevHashLen),
	)
	copy(dst[32:], metadata.PrevHash)
	return nil
}

func unmarshalBlockMetadata(data []byte) (types.BlockMetadata, error) {
	if len(data) >= 32 && bytes.Equal(data[:4], blockMetadataBinaryMagic[:]) {
		prevHashLen := binary.BigEndian.Uint32(data[28:32])
		expectedLen := 32 + int(prevHashLen)
		if len(data) != expectedLen {
			return types.BlockMetadata{}, fmt.Errorf(
				"invalid block metadata length: got %d, want %d",
				len(data),
				expectedLen,
			)
		}
		prevHash := make([]byte, prevHashLen)
		copy(prevHash, data[32:])
		return types.BlockMetadata{
			ID:       binary.BigEndian.Uint64(data[4:12]),
			Type:     uint(binary.BigEndian.Uint64(data[12:20])),
			Height:   binary.BigEndian.Uint64(data[20:28]),
			PrevHash: prevHash,
		}, nil
	}
	var metadata types.BlockMetadata
	if _, err := cbor.Decode(data, &metadata); err != nil {
		return types.BlockMetadata{}, err
	}
	return metadata, nil
}

func (i *badgerItem) Key() []byte {
	return i.item.KeyCopy(nil)
}

func (i *badgerItem) ValueCopy(dst []byte) ([]byte, error) {
	return i.item.ValueCopy(dst)
}

// BlobStoreBadger stores all data in badger. Data may not be persisted
type BlobStoreBadger struct {
	promRegistry         prometheus.Registerer
	db                   *badger.DB
	logger               *slog.Logger
	gcTicker             *time.Ticker
	gcStopCh             chan struct{}
	dataDir              string
	gcWg                 sync.WaitGroup
	blockCacheSize       uint64
	indexCacheSize       uint64
	valueLogFileSize     int64
	memTableSize         int64
	valueThreshold       int64
	compactBlockMetadata bool
	compressionEnabled   bool
	gcEnabled            bool
	deferOpen            bool // when true, Badger is opened in Start() not New()
}

// New creates a new database. When deferOpen is set (via WithDeferOpen),
// Badger is not opened until Start() is called, allowing an injected
// logger (via SetLogger) to be used for Badger's startup logging.
func New(opts ...BlobStoreBadgerOptionFunc) (*BlobStoreBadger, error) {
	db := &BlobStoreBadger{
		// Set defaults
		gcEnabled:          true, // Enable GC by default for disk-backed stores
		compressionEnabled: true, // Snappy compression by default
		blockCacheSize:     DefaultBlockCacheSize,
		indexCacheSize:     DefaultIndexCacheSize,
		valueLogFileSize:   int64(DefaultValueLogFileSize),
		memTableSize:       int64(DefaultMemTableSize),
		valueThreshold:     int64(DefaultValueThreshold),
	}
	for _, opt := range opts {
		opt(db)
	}

	if db.deferOpen {
		return db, nil
	}
	if err := db.open(); err != nil {
		return nil, fmt.Errorf("badger open: %w", err)
	}
	return db, nil
}

// open initializes and opens the Badger database using the configured options.
func (db *BlobStoreBadger) open() error {
	var blobDb *badger.DB
	var err error

	if db.dataDir == "" {
		// No dataDir, use in-memory config
		badgerOpts := badger.DefaultOptions("").
			WithLogger(NewBadgerLogger(db.logger)).
			// The default INFO logging is a bit verbose
			WithLoggingLevel(badger.WARNING).
			WithInMemory(true).
			WithValueThreshold(db.valueThreshold).
			WithCompression(db.badgerCompression())
		blobDb, err = badger.Open(badgerOpts)
		if err != nil {
			return fmt.Errorf("badger open in-memory: %w", err)
		}
	} else {
		// Make sure that we can read data dir, and create if it doesn't exist
		if _, err := os.Stat(db.dataDir); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("failed to read data dir: %w", err)
			}
			// Create data directory
			if err := os.MkdirAll(db.dataDir, 0o700); err != nil {
				return fmt.Errorf("failed to create data dir: %w", err)
			}
		}
		blobDir := filepath.Join(
			db.dataDir,
			"blob",
		)
		badgerOpts := badger.DefaultOptions(blobDir).
			WithLogger(NewBadgerLogger(db.logger)).
			WithLoggingLevel(badger.WARNING).
			WithBlockCacheSize(int64(db.blockCacheSize)). //nolint:gosec // blockCacheSize is controlled and reasonable
			WithIndexCacheSize(int64(db.indexCacheSize)). //nolint:gosec // indexCacheSize is controlled and reasonable
			WithValueLogFileSize(db.valueLogFileSize).
			WithMemTableSize(db.memTableSize).
			WithValueThreshold(db.valueThreshold).
			WithCompression(db.badgerCompression())
		blobDb, err = badger.Open(badgerOpts)
		if err != nil {
			return fmt.Errorf("badger open on-disk: %w", err)
		}
	}
	db.db = blobDb
	if err := db.init(); err != nil {
		return err
	}
	if db.dataDir != "" {
		db.logger.Info(
			"badger blob store opened",
			"component", "database",
			"block_cache_size_mb", db.blockCacheSize/(1024*1024),
			"index_cache_size_mb", db.indexCacheSize/(1024*1024),
			"data_dir", db.dataDir,
		)
	}
	return nil
}

func (db *BlobStoreBadger) badgerCompression() options.CompressionType {
	if db.compressionEnabled {
		return options.Snappy
	}
	return options.None
}

func (d *BlobStoreBadger) init() error {
	if d.logger == nil {
		// Create logger to throw away logs
		// We do this so we don't have to add guards around every log operation
		d.logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	// Configure metrics
	if d.promRegistry != nil {
		d.registerBlobMetrics()
	}
	// Configure GC
	if d.gcEnabled {
		d.gcTicker = time.NewTicker(5 * time.Minute)
		d.gcStopCh = make(chan struct{})
		d.gcWg.Add(1)
		go d.blobGc(d.gcTicker, d.gcStopCh)
	}
	return nil
}

func (d *BlobStoreBadger) blobGc(t *time.Ticker, stop <-chan struct{}) {
	defer d.gcWg.Done()
	for {
		select {
		case <-t.C:
		again:
			err := d.DB().RunValueLogGC(0.5)
			if err != nil {
				// Log any actual errors
				if !errors.Is(err, badger.ErrNoRewrite) {
					d.logger.Warn(
						fmt.Sprintf("blob DB: GC failure: %s", err),
						"component", "database",
					)
				}
			} else {
				// Run it again if it just ran successfully
				goto again
			}
		case <-stop:
			return
		}
	}
}

// Start implements the plugin.Plugin interface. When the database was
// created with WithDeferOpen, Start opens Badger using the logger that
// was injected via SetLogger after construction.
func (d *BlobStoreBadger) Start() error {
	if d.deferOpen && d.db == nil {
		if err := d.open(); err != nil {
			return fmt.Errorf("badger deferred start: %w", err)
		}
	}
	return nil
}

// Stop implements the plugin.Plugin interface
func (d *BlobStoreBadger) Stop() error {
	return d.Close()
}

// Close gets the database handle from our BlobStore and closes it
func (d *BlobStoreBadger) Close() error {
	// Stop GC ticker if it exists
	if d.gcTicker != nil {
		d.gcTicker.Stop()
		if d.gcStopCh != nil {
			close(d.gcStopCh)
			d.gcStopCh = nil
		}
		// Wait for GC goroutine to finish
		d.gcWg.Wait()
		d.gcTicker = nil
	}
	db := d.DB()
	if db == nil {
		return nil
	}
	return db.Close()
}

// DB returns the database handle
func (d *BlobStoreBadger) DB() *badger.DB {
	return d.db
}

// DiskSize returns the on-disk size of the Badger database in bytes
// (LSM tree size + value log size).
func (d *BlobStoreBadger) DiskSize() (int64, error) {
	db := d.DB()
	if db == nil {
		return 0, nil
	}
	lsm, vlog := db.Size()
	return lsm + vlog, nil
}

// NewTransaction creates a new badger transaction
func (d *BlobStoreBadger) NewTransaction(update bool) types.Txn {
	return newBadgerTxn(d, d.DB().NewTransaction(update))
}

// Get retrieves a value from badger within a transaction
func (d *BlobStoreBadger) Get(
	txn types.Txn,
	key []byte,
) ([]byte, error) {
	badgerTxn, err := d.validateTxn(txn)
	if err != nil {
		return nil, err
	}
	item, err := badgerTxn.tx.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, types.ErrBlobKeyNotFound
		}
		return nil, err
	}
	return item.ValueCopy(nil)
}

// Set stores a key-value pair in badger within a transaction
func (d *BlobStoreBadger) Set(txn types.Txn, key, val []byte) error {
	badgerTxn, err := d.validateTxn(txn)
	if err != nil {
		return err
	}
	return badgerTxn.tx.Set(key, val)
}

// Delete removes a key from badger within a transaction
func (d *BlobStoreBadger) Delete(txn types.Txn, key []byte) error {
	badgerTxn, err := d.validateTxn(txn)
	if err != nil {
		return err
	}
	return badgerTxn.tx.Delete(key)
}

// NewIterator creates an iterator for badger within a transaction.
//
// Important: items returned by the iterator's `Item()` must only be
// accessed while the transaction used to create the iterator is still
// active. Implementations may validate transaction state at access time
// (for example `ValueCopy` may fail if the transaction has been committed
// or rolled back). Typical usage iterates and accesses item values within
// the same transaction scope.
func (d *BlobStoreBadger) NewIterator(
	txn types.Txn,
	opts types.BlobIteratorOptions,
) types.BlobIterator {
	badgerTxn, err := d.validateTxn(txn)
	if err != nil {
		return &errorIterator{err: err}
	}
	iterOpts := badger.IteratorOptions{
		Prefix:  opts.Prefix,
		Reverse: opts.Reverse,
	}
	return &badgerIterator{iter: badgerTxn.tx.NewIterator(iterOpts)}
}

// SetBlock stores a block with its metadata and index
func (d *BlobStoreBadger) SetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
	cborData []byte,
	id uint64,
	blockType uint,
	height uint64,
	prevHash []byte,
) error {
	badgerTxn, err := d.validateTxn(txn)
	if err != nil {
		return err
	}
	tmpMetadata := types.BlockMetadata{
		ID:       id,
		Type:     blockType,
		Height:   height,
		PrevHash: prevHash,
	}
	keyLen := len(types.BlockBlobKeyPrefix) + 8 + len(hash)
	indexKeyLen := len(types.BlockBlobIndexKeyPrefix) + 8
	metadataKeyLen := keyLen + len(types.BlockBlobMetadataKeySuffix)
	packedLen := keyLen + indexKeyLen + metadataKeyLen
	if d.compactBlockMetadata {
		packedLen += 32 + len(prevHash)
	}
	packed := make([]byte, packedLen)
	key := packed[:keyLen]
	buildBlockBlobKey(key, slot, hash)
	if err := badgerTxn.tx.Set(key, cborData); err != nil {
		return err
	}
	// Block index to point key
	indexKeyStart := keyLen
	indexKeyEnd := indexKeyStart + indexKeyLen
	indexKey := packed[indexKeyStart:indexKeyEnd]
	buildBlockBlobIndexKey(indexKey, id)
	if err := badgerTxn.tx.Set(indexKey, key); err != nil {
		return err
	}
	// Block metadata by point
	metadataKeyStart := indexKeyEnd
	metadataKeyEnd := metadataKeyStart + metadataKeyLen
	metadataKey := packed[metadataKeyStart:metadataKeyEnd]
	buildBlockBlobMetadataKey(metadataKey, key)
	var tmpMetadataBytes []byte
	if d.compactBlockMetadata {
		tmpMetadataBytes = packed[metadataKeyEnd:]
		if err := marshalBlockMetadataInto(tmpMetadataBytes, tmpMetadata); err != nil {
			return err
		}
	} else {
		tmpMetadataBytes, err = cbor.Encode(tmpMetadata)
		if err != nil {
			return err
		}
	}
	if err := badgerTxn.tx.Set(metadataKey, tmpMetadataBytes); err != nil {
		return err
	}
	return nil
}

// GetBlock retrieves a block's CBOR data and metadata
func (d *BlobStoreBadger) GetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	badgerTxn, err := d.validateTxn(txn)
	if err != nil {
		return nil, types.BlockMetadata{}, err
	}
	key := types.BlockBlobKey(slot, hash)
	val, err := badgerTxn.tx.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, types.BlockMetadata{}, types.ErrBlobKeyNotFound
		}
		return nil, types.BlockMetadata{}, err
	}
	cborData, err := val.ValueCopy(nil)
	if err != nil {
		return nil, types.BlockMetadata{}, err
	}
	metadataKey := types.BlockBlobMetadataKey(key)
	metadataVal, err := badgerTxn.tx.Get(metadataKey)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, types.BlockMetadata{}, types.ErrBlobKeyNotFound
		}
		return nil, types.BlockMetadata{}, err
	}
	metadataBytes, err := metadataVal.ValueCopy(nil)
	if err != nil {
		return nil, types.BlockMetadata{}, err
	}
	tmpMetadata, err := unmarshalBlockMetadata(metadataBytes)
	if err != nil {
		return nil, types.BlockMetadata{}, err
	}
	return cborData, tmpMetadata, nil
}

// DeleteBlock removes a block and its associated data
func (d *BlobStoreBadger) DeleteBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
	id uint64,
) error {
	badgerTxn, err := d.validateTxn(txn)
	if err != nil {
		return err
	}
	key := types.BlockBlobKey(slot, hash)
	if err := badgerTxn.tx.Delete(key); err != nil {
		return err
	}
	indexKey := types.BlockBlobIndexKey(id)
	if err := badgerTxn.tx.Delete(indexKey); err != nil {
		return err
	}
	metadataKey := types.BlockBlobMetadataKey(key)
	if err := badgerTxn.tx.Delete(metadataKey); err != nil {
		return err
	}
	return nil
}

// SetUtxo stores a UTxO's CBOR data
func (d *BlobStoreBadger) SetUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
	cborData []byte,
) error {
	badgerTxn, err := d.validateTxn(txn)
	if err != nil {
		return err
	}
	key := types.UtxoBlobKey(txId, outputIdx)
	return badgerTxn.tx.Set(key, cborData)
}

// GetUtxo retrieves a UTxO's CBOR data
func (d *BlobStoreBadger) GetUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) ([]byte, error) {
	badgerTxn, err := d.validateTxn(txn)
	if err != nil {
		return nil, err
	}
	key := types.UtxoBlobKey(txId, outputIdx)
	val, err := badgerTxn.tx.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, types.ErrBlobKeyNotFound
		}
		return nil, err
	}
	return val.ValueCopy(nil)
}

// DeleteUtxo removes a UTxO's data
func (d *BlobStoreBadger) DeleteUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) error {
	badgerTxn, err := d.validateTxn(txn)
	if err != nil {
		return err
	}
	key := types.UtxoBlobKey(txId, outputIdx)
	return badgerTxn.tx.Delete(key)
}

// SetTx stores a transaction's offset data
func (d *BlobStoreBadger) SetTx(
	txn types.Txn,
	txHash []byte,
	offsetData []byte,
) error {
	badgerTxn, err := d.validateTxn(txn)
	if err != nil {
		return fmt.Errorf("SetTx: validate txn: %w", err)
	}
	key := types.TxBlobKey(txHash)
	if err := badgerTxn.tx.Set(key, offsetData); err != nil {
		return fmt.Errorf("SetTx: set key %x: %w", key, err)
	}
	return nil
}

// GetTx retrieves a transaction's offset data
func (d *BlobStoreBadger) GetTx(
	txn types.Txn,
	txHash []byte,
) ([]byte, error) {
	badgerTxn, err := d.validateTxn(txn)
	if err != nil {
		return nil, fmt.Errorf("GetTx: validate txn: %w", err)
	}
	key := types.TxBlobKey(txHash)
	val, err := badgerTxn.tx.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, types.ErrBlobKeyNotFound
		}
		return nil, fmt.Errorf("GetTx: get key %x: %w", key, err)
	}
	data, err := val.ValueCopy(nil)
	if err != nil {
		return nil, fmt.Errorf("GetTx: copy value for key %x: %w", key, err)
	}
	return data, nil
}

// DeleteTx removes a transaction's offset data
func (d *BlobStoreBadger) DeleteTx(
	txn types.Txn,
	txHash []byte,
) error {
	badgerTxn, err := d.validateTxn(txn)
	if err != nil {
		return fmt.Errorf("DeleteTx: validate txn: %w", err)
	}
	key := types.TxBlobKey(txHash)
	if err := badgerTxn.tx.Delete(key); err != nil {
		return fmt.Errorf("DeleteTx: delete key %x: %w", key, err)
	}
	return nil
}

func (d *BlobStoreBadger) GetBlockURL(
	ctx context.Context,
	txn types.Txn,
	point ocommon.Point,
) (types.SignedURL, types.BlockMetadata, error) {
	return types.SignedURL{}, types.BlockMetadata{},
		errors.New("badger: GetBlockURL not supported")
}
