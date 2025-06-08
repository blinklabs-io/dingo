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
	"time"

	"github.com/blinklabs-io/dingo/database/plugin"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus"
)

// Register plugin
func init() {
	plugin.Register(
		plugin.PluginEntry{
			Type: plugin.PluginTypeMetadata,
			Name: "sqlite",
		},
	)
}

// BlobStoreBadger stores all data in badger. Data may not be persisted
type BlobStoreBadger struct {
	dataDir      string
	db           *badger.DB
	logger       *slog.Logger
	promRegistry prometheus.Registerer
	gcEnabled    bool
}

// New creates a new database
func New(
	dataDir string,
	logger *slog.Logger,
	promRegistry prometheus.Registerer,
) (*BlobStoreBadger, error) {
	var blobDb *badger.DB
	var err error
	db := &BlobStoreBadger{
		dataDir:      dataDir,
		logger:       logger,
		promRegistry: promRegistry,
	}
	if dataDir == "" {
		// No dataDir, use in-memory config
		badgerOpts := badger.DefaultOptions("").
			WithLogger(NewBadgerLogger(logger)).
			// The default INFO logging is a bit verbose
			WithLoggingLevel(badger.WARNING).
			WithInMemory(true)
		blobDb, err = badger.Open(badgerOpts)
		if err != nil {
			return nil, err
		}
	} else {
		// Make sure that we can read data dir, and create if it doesn't exist
		if _, err := os.Stat(dataDir); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return nil, fmt.Errorf("failed to read data dir: %w", err)
			}
			// Create data directory
			if err := os.MkdirAll(dataDir, fs.ModePerm); err != nil {
				return nil, fmt.Errorf("failed to create data dir: %w", err)
			}
		}
		blobDir := filepath.Join(
			dataDir,
			"blob",
		)
		// Run GC periodically
		db.gcEnabled = true
		badgerOpts := badger.DefaultOptions(blobDir).
			WithLogger(NewBadgerLogger(logger)).
			// The default INFO logging is a bit verbose
			WithLoggingLevel(badger.WARNING)
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
		go d.blobGc(time.NewTicker(5 * time.Minute))
	}
	return nil
}

func (d *BlobStoreBadger) blobGc(t *time.Ticker) {
	for range t.C {
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
	}
}

// Close gets the database handle from our BlobStore and closes it
func (d *BlobStoreBadger) Close() error {
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
