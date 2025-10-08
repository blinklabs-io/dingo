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
	"github.com/prometheus/client_golang/prometheus"
)

var DefaultConfig = &Config{
	BlobCacheSize:  1073741824,
	BlobPlugin:     "badger",
	DataDir:        ".dingo",
	MetadataPlugin: "sqlite",
}

// Config represents the configuration for a database instance
type Config struct {
	BlobCacheSize  int64
	BlobPlugin     string
	DataDir        string
	MetadataPlugin string
	Logger         *slog.Logger
	PromRegistry   prometheus.Registerer
}

// Transaction represents a transaction record
type Transaction struct {
	ID         uint
	Hash       []byte
	Type       string
	BlockHash  []byte
	BlockIndex uint32
	Inputs     []byte
	Outputs    []byte
}

// Database represents our data storage services
type Database struct {
	config   *Config
	logger   *slog.Logger
	blob     blob.BlobStore
	metadata metadata.MetadataStore
}

// Blob returns the underling blob store instance
func (d *Database) Blob() blob.BlobStore {
	return d.blob
}

// Config returns the config object used for the database instance
func (d *Database) Config() *Config {
	return d.config
}

// DataDir returns the path to the data directory used for storage
func (d *Database) DataDir() string {
	return d.config.DataDir
}

// Logger returns the logger instance
func (d *Database) Logger() *slog.Logger {
	return d.config.Logger
}

// Metadata returns the underlying metadata store instance
func (d *Database) Metadata() metadata.MetadataStore {
	return d.metadata
}

func (d *Database) NewTransaction(
	hash []byte,
	txType string,
	blockHash []byte,
	blockIndex uint32,
	inputs []byte,
	outputs []byte,
	txn *Txn,
) (Transaction, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	ret, err := d.metadata.SetTransaction(
		hash,
		txType,
		blockHash,
		blockIndex,
		inputs,
		outputs,
		txn.Metadata(),
	)
	if err != nil {
		return Transaction{}, err
	}
	return Transaction{
		ID:         ret.ID,
		Hash:       ret.Hash,
		Type:       ret.Type,
		BlockHash:  ret.BlockHash,
		BlockIndex: ret.BlockIndex,
		Inputs:     ret.Inputs,
		Outputs:    ret.Outputs,
	}, nil
}

// Transaction starts a new database transaction and returns a handle to it
func (d *Database) Transaction(readWrite bool) *Txn {
	return NewTxn(d, readWrite)
}

// BlobTxn starts a new blob-only database transaction and returns a handle to it
func (d *Database) BlobTxn(readWrite bool) *Txn {
	return NewBlobOnlyTxn(d, readWrite)
}

// MetadataTxn starts a new metadata-only database transaction and returns a handle to it
func (d *Database) MetadataTxn(readWrite bool) *Txn {
	return NewMetadataOnlyTxn(d, readWrite)
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
	config *Config,
) (*Database, error) {
	if config == nil {
		config = DefaultConfig
	}
	blobDb, err := blob.New(
		config.BlobPlugin,
		config.DataDir,
		config.Logger,
		config.PromRegistry,
		config.BlobCacheSize,
	)
	if err != nil {
		return nil, err
	}
	metadataDb, err := metadata.New(
		config.MetadataPlugin,
		config.DataDir,
		config.Logger,
		config.PromRegistry,
	)
	if err != nil {
		return nil, err
	}
	db := &Database{
		blob:     blobDb,
		metadata: metadataDb,
		logger:   config.Logger,
		config:   config,
	}
	if err := db.init(); err != nil {
		// Database is available for recovery, so return it with error
		return db, err
	}
	return db, nil
}
