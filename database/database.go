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

package database

import (
	"errors"
	"io"
	"log/slog"

	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/prometheus/client_golang/prometheus"
)

var DefaultConfig = &Config{
	BlobPlugin:     "badger",
	DataDir:        ".dingo",
	MetadataPlugin: "sqlite",
}

// Config represents the configuration for a database instance
type Config struct {
	PromRegistry   prometheus.Registerer
	Logger         *slog.Logger
	BlobPlugin     string
	DataDir        string
	MetadataPlugin string
	MaxConnections int // Connection pool size for metadata plugin (should match DatabaseWorkers)
	CacheConfig    CborCacheConfig
}

// Database represents our data storage services
type Database struct {
	config    *Config
	logger    *slog.Logger
	blob      blob.BlobStore
	metadata  metadata.MetadataStore
	cborCache *TieredCborCache
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
	if d.metadata != nil {
		err = errors.Join(err, d.metadata.Close())
	}
	if d.blob != nil {
		err = errors.Join(err, d.blob.Close())
	}
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

// New creates a new database instance with optional persistence using the provided data directory.
// When config is nil, DefaultConfig is used (DataDir = ".dingo" for persistence).
// When config is provided but DataDir is empty, storage is in-memory only.
// When config.DataDir is non-empty, it specifies the persistent storage directory.
func New(
	config *Config,
) (*Database, error) {
	var err error
	if config == nil {
		config = DefaultConfig
	}
	// Create a copy of the config to avoid mutating the original
	cfgVal := *config
	configCopy := &cfgVal
	// Apply defaults for empty fields
	if configCopy.BlobPlugin == "" {
		configCopy.BlobPlugin = DefaultConfig.BlobPlugin
	}
	if configCopy.MetadataPlugin == "" {
		configCopy.MetadataPlugin = DefaultConfig.MetadataPlugin
	}
	// Handle DataDir configuration for plugins:
	// - nil config → DefaultConfig.DataDir (".dingo" for persistence)
	// - empty DataDir → in-memory storage
	// - non-empty DataDir → persistent storage at specified path
	// NOTE: SetPluginOption mutates global plugin state, so DataDir is effectively
	// process-wide and not concurrency-safe. Multiple Database instances in the
	// same process will share and overwrite these options.
	err = plugin.SetPluginOption(
		plugin.PluginTypeBlob,
		configCopy.BlobPlugin,
		"data-dir",
		configCopy.DataDir,
	)
	if err != nil {
		return nil, err
	}
	err = plugin.SetPluginOption(
		plugin.PluginTypeMetadata,
		configCopy.MetadataPlugin,
		"data-dir",
		configCopy.DataDir,
	)
	if err != nil {
		return nil, err
	}
	// Set max-connections if configured (for SQLite plugin)
	if configCopy.MaxConnections > 0 {
		err = plugin.SetPluginOption(
			plugin.PluginTypeMetadata,
			configCopy.MetadataPlugin,
			"max-connections",
			configCopy.MaxConnections,
		)
		if err != nil {
			return nil, err
		}
	}
	blobDb, err := blob.New(
		configCopy.BlobPlugin,
	)
	if err != nil {
		return nil, err
	}
	metadataDb, err := metadata.New(
		configCopy.MetadataPlugin,
	)
	if err != nil {
		err = errors.Join(
			err,
			blobDb.Close(),
		) // Clean up blob store on metadata failure
		return nil, err
	}
	db := &Database{
		blob:     blobDb,
		metadata: metadataDb,
		logger:   configCopy.Logger,
		config:   configCopy,
	}
	// Initialize the tiered CBOR cache
	db.cborCache = NewTieredCborCache(configCopy.CacheConfig, db)
	// Register cache metrics if prometheus registry is available
	if configCopy.PromRegistry != nil {
		db.cborCache.Metrics().Register(configCopy.PromRegistry)
	}
	if err := db.init(); err != nil {
		// Database is available for recovery, so return it with error
		return db, err
	}
	return db, nil
}

// CborCache returns the tiered CBOR cache for accessing cached CBOR data.
// This can be used for metrics registration or direct cache access.
func (d *Database) CborCache() *TieredCborCache {
	return d.cborCache
}
