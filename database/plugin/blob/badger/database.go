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

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/prometheus/client_golang/prometheus"
)

// BlobStoreBadger stores all data in badger. Data may not be persisted
type BlobStoreBadger struct {
	promRegistry   prometheus.Registerer
	db             *badger.DB
	logger         *slog.Logger
	gcTicker       *time.Ticker
	gcStopCh       chan struct{}
	dataDir        string
	gcWg           sync.WaitGroup
	blockCacheSize uint64
	indexCacheSize uint64
	gcEnabled      bool
}

// New creates a new database
func New(opts ...BlobStoreBadgerOptionFunc) (*BlobStoreBadger, error) {
	db := &BlobStoreBadger{
		// Set defaults
		gcEnabled:      true, // Enable GC by default for disk-backed stores
		blockCacheSize: DefaultBlockCacheSize,
		indexCacheSize: DefaultIndexCacheSize,
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
			WithInMemory(true)
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
func (d *BlobStoreBadger) NewTransaction(update bool) *badger.Txn {
	return d.DB().NewTransaction(update)
}
