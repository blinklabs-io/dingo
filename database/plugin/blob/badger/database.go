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

package badger

import (
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

func (i *badgerItem) Key() []byte {
	return i.item.KeyCopy(nil)
}

func (i *badgerItem) ValueCopy(dst []byte) ([]byte, error) {
	return i.item.ValueCopy(dst)
}

// BlobStoreBadger stores all data in badger. Data may not be persisted
type BlobStoreBadger struct {
	promRegistry     prometheus.Registerer
	db               *badger.DB
	logger           *slog.Logger
	gcTicker         *time.Ticker
	gcStopCh         chan struct{}
	dataDir          string
	gcWg             sync.WaitGroup
	blockCacheSize   uint64
	indexCacheSize   uint64
	valueLogFileSize int64
	memTableSize     int64
	valueThreshold   int64
	gcEnabled        bool
}

// New creates a new database
func New(opts ...BlobStoreBadgerOptionFunc) (*BlobStoreBadger, error) {
	db := &BlobStoreBadger{
		// Set defaults
		gcEnabled:        true, // Enable GC by default for disk-backed stores
		blockCacheSize:   DefaultBlockCacheSize,
		indexCacheSize:   DefaultIndexCacheSize,
		valueLogFileSize: int64(DefaultValueLogFileSize),
		memTableSize:     int64(DefaultMemTableSize),
		valueThreshold:   int64(DefaultValueThreshold),
	}
	for _, opt := range opts {
		opt(db)
	}

	var blobDb *badger.DB
	var err error

	if db.dataDir == "" {
		// No dataDir, use in-memory config
		badgerOpts := badger.DefaultOptions("").
			WithLogger(NewBadgerLogger(db.logger)).
			// The default INFO logging is a bit verbose
			WithLoggingLevel(badger.WARNING).
			WithInMemory(true).
			WithValueThreshold(db.valueThreshold)
		blobDb, err = badger.Open(badgerOpts)
		if err != nil {
			return nil, err
		}
	} else {
		// Make sure that we can read data dir, and create if it doesn't exist
		if _, err := os.Stat(db.dataDir); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return nil, fmt.Errorf("failed to read data dir: %w", err)
			}
			// Create data directory
			if err := os.MkdirAll(db.dataDir, 0o755); err != nil {
				return nil, fmt.Errorf("failed to create data dir: %w", err)
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
			WithCompression(options.Snappy)
		blobDb, err = badger.Open(badgerOpts)
		if err != nil {
			return nil, err
		}
	}
	db.db = blobDb
	if err := db.init(); err != nil {
		return db, err
	}
	return db, nil
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

// Start implements the plugin.Plugin interface
func (d *BlobStoreBadger) Start() error {
	// Database is already started in New(), so this is a no-op
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
	return db.Close()
}

// DB returns the database handle
func (d *BlobStoreBadger) DB() *badger.DB {
	return d.db
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
