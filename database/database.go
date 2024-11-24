// Copyright 2024 Blink Labs Software
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

package database

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/plugin/opentelemetry/tracing"
)

type Database interface {
	Close() error
	Metadata() *gorm.DB
	Blob() *badger.DB
	Transaction(bool) *Txn
	updateCommitTimestamp(*Txn, int64) error
}

type BaseDatabase struct {
	logger        *slog.Logger
	metadata      *gorm.DB
	blob          *badger.DB
	blobGcEnabled bool
	blobGcTimer   *time.Ticker
}

// Metadata returns the underlying metadata DB instance
func (b *BaseDatabase) Metadata() *gorm.DB {
	return b.metadata
}

// Blob returns the underling blob DB instance
func (b *BaseDatabase) Blob() *badger.DB {
	return b.blob
}

// Transaction starts a new database transaction and returns a handle to it
func (b *BaseDatabase) Transaction(readWrite bool) *Txn {
	return NewTxn(b, readWrite)
}

// Close cleans up the database connections
func (b *BaseDatabase) Close() error {
	var err error
	// Close metadata
	sqlDB, sqlDBerr := b.metadata.DB()
	if sqlDBerr != nil {
		err = errors.Join(err, sqlDBerr)
	} else {
		metadataErr := sqlDB.Close()
		err = errors.Join(err, metadataErr)
	}
	// Close blob
	blobErr := b.blob.Close()
	err = errors.Join(err, blobErr)
	return err
}

func (b *BaseDatabase) init() error {
	if b.logger == nil {
		// Create logger to throw away logs
		// We do this so we don't have to add guards around every log operation
		b.logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	// Configure tracing for GORM
	if err := b.metadata.Use(tracing.NewPlugin(tracing.WithoutMetrics())); err != nil {
		return err
	}
	// Configure metrics for Badger DB
	b.registerBadgerMetrics()
	// Run GC periodically for Badger DB
	if b.blobGcEnabled {
		b.blobGcTimer = time.NewTicker(5 * time.Minute)
		go b.blobGc()
	}
	// Check commit timestamp
	if err := b.checkCommitTimestamp(); err != nil {
		return err
	}
	return nil
}

func (b *BaseDatabase) blobGc() {
	for range b.blobGcTimer.C {
	again:
		err := b.blob.RunValueLogGC(0.5)
		if err != nil {
			// Log any actual errors
			if !errors.Is(err, badger.ErrNoRewrite) {
				b.logger.Warn(
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

// InMemoryDatabase stores all data in memory. Data will not be persisted
type InMemoryDatabase struct {
	*BaseDatabase
}

// NewInMemory creates a new in-memory database
func NewInMemory(logger *slog.Logger) (*InMemoryDatabase, error) {
	// Open sqlite DB
	metadataDb, err := gorm.Open(
		sqlite.Open("file::memory:?cache=shared"),
		&gorm.Config{
			Logger:          gormlogger.Discard,
			CreateBatchSize: 1000,
		},
	)
	if err != nil {
		return nil, err
	}
	// Open Badger DB
	badgerOpts := badger.DefaultOptions("").
		WithLogger(NewBadgerLogger(logger)).
		// The default INFO logging is a bit verbose
		WithLoggingLevel(badger.WARNING).
		WithInMemory(true)
	blobDb, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, err
	}
	db := &InMemoryDatabase{
		BaseDatabase: &BaseDatabase{
			logger:   logger,
			metadata: metadataDb,
			blob:     blobDb,
			// We disable badger GC when using an in-memory DB, since it will only throw errors
			blobGcEnabled: false,
		},
	}
	if err := db.init(); err != nil {
		// Database is available for recovery, so return it with error
		return db, err
	}
	return db, nil
}

// PersistentDatabase stores its data on disk, providing persistence across restarts
type PersistentDatabase struct {
	*BaseDatabase
	dataDir string
}

// NewPersistent creates a new persistent database instance using the provided data directory
func NewPersistent(
	dataDir string,
	logger *slog.Logger,
) (*PersistentDatabase, error) {
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
	// Open sqlite DB
	metadataDbPath := filepath.Join(
		dataDir,
		"metadata.sqlite",
	)
	// WAL journal mode, disable sync on write, increase cache size to 50MB (from 2MB)
	metadataConnOpts := "_pragma=journal_mode(WAL)&_pragma=sync(OFF)&_pragma=cache_size(-50000)"
	metadataDb, err := gorm.Open(
		sqlite.Open(
			fmt.Sprintf("file:%s?%s", metadataDbPath, metadataConnOpts),
		),
		&gorm.Config{
			Logger:          gormlogger.Discard,
			CreateBatchSize: 1000,
		},
	)
	if err != nil {
		return nil, err
	}
	// Open Badger DB
	blobDir := filepath.Join(
		dataDir,
		"blob",
	)
	badgerOpts := badger.DefaultOptions(blobDir).
		WithLogger(NewBadgerLogger(logger)).
		// The default INFO logging is a bit verbose
		WithLoggingLevel(badger.WARNING)
	blobDb, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, err
	}
	db := &PersistentDatabase{
		BaseDatabase: &BaseDatabase{
			logger:        logger,
			metadata:      metadataDb,
			blob:          blobDb,
			blobGcEnabled: true,
		},
		dataDir: dataDir,
	}
	if err := db.init(); err != nil {
		// Database is available for recovery, so return it with error
		return db, err
	}
	return db, nil
}
