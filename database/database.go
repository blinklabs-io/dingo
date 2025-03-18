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

package database

import (
	"errors"
	"io"
	"log/slog"

	badgerPlugin "github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	badger "github.com/dgraph-io/badger/v4"
)

type Database interface {
	Close() error
	Metadata() metadata.MetadataStore
	Blob() *badger.DB
	Transaction(bool) *Txn
	updateCommitTimestamp(*Txn, int64) error
}

type BaseDatabase struct {
	logger   *slog.Logger
	metadata metadata.MetadataStore
	blob     *badger.DB
}

// Metadata returns the underlying metadata store instance
func (b *BaseDatabase) Metadata() metadata.MetadataStore {
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
	metadataErr := b.Metadata().Close()
	err = errors.Join(err, metadataErr)
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
	// Check commit timestamp
	if err := b.checkCommitTimestamp(); err != nil {
		return err
	}
	return nil
}

// InMemoryDatabase stores all data in memory. Data will not be persisted
type InMemoryDatabase struct {
	*BaseDatabase
}

// NewInMemory creates a new in-memory database
func NewInMemory(logger *slog.Logger) (*InMemoryDatabase, error) {
	// Use sqlite plugin
	metadataDb, err := metadata.New("sqlite", "", logger)
	if err != nil {
		return nil, err
	}
	// Use badger plugin
	blobDb, err := badgerPlugin.New("", logger)
	if err != nil {
		return nil, err
	}
	db := &InMemoryDatabase{
		BaseDatabase: &BaseDatabase{
			logger:   logger,
			metadata: metadataDb,
			blob:     blobDb.DB(),
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
	metadataDb, err := metadata.New("sqlite", dataDir, logger)
	if err != nil {
		return nil, err
	}
	blobDb, err := badgerPlugin.New(dataDir, logger)
	if err != nil {
		return nil, err
	}
	db := &PersistentDatabase{
		BaseDatabase: &BaseDatabase{
			logger:   logger,
			metadata: metadataDb,
			blob:     blobDb.DB(),
		},
		dataDir: dataDir,
	}
	if err := db.init(); err != nil {
		// Database is available for recovery, so return it with error
		return db, err
	}
	return db, nil
}
