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

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

type Database struct {
	logger   *slog.Logger
	blob     blob.BlobStore
	metadata metadata.MetadataStore
	dataDir  string
}

// Blob returns the underling blob store instance
func (d *Database) Blob() blob.BlobStore {
	return d.blob
}

// DataDir returns the path to the data directory used for storage
func (d *Database) DataDir() string {
	return d.dataDir
}

// Logger returns the logger instance
func (d *Database) Logger() *slog.Logger {
	return d.logger
}

// Metadata returns the underlying metadata store instance
func (d *Database) Metadata() metadata.MetadataStore {
	return d.metadata
}

// Transaction starts a new database transaction and returns a handle to it
func (d *Database) Transaction(readWrite bool) *Txn {
	return NewTxn(d, readWrite)
}

// Close cleans up the database connections
func (d *Database) Close() error {
	var err error
	// Close metadata
	metadataErr := d.Metadata().Close()
	err = errors.Join(err, metadataErr)
	// Close blob
	blobErr := d.Blob().Close()
	err = errors.Join(err, blobErr)
	return err
}

func (d *Database) init() error {
	if d.logger == nil {
		// Create logger to throw away logs
		// We do this so we don't have to add guards around every log operation
		d.logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	// Check commit timestamp
	if err := d.checkCommitTimestamp(); err != nil {
		return err
	}
	return nil
}

// New creates a new database instance with optional persistence using the provided data directory
func New(
	logger *slog.Logger,
	dataDir string,
) (*Database, error) {
	metadataDb, err := metadata.New("sqlite", dataDir, logger)
	if err != nil {
		return nil, err
	}
	blobDb, err := blob.New("badger", dataDir, logger)
	if err != nil {
		return nil, err
	}
	db := &Database{
		logger:   logger,
		blob:     blobDb,
		metadata: metadataDb,
		dataDir:  dataDir,
	}
	if err := db.init(); err != nil {
		// Database is available for recovery, so return it with error
		return db, err
	}
	return db, nil
}
