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

package sqlite

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/plugin/opentelemetry/tracing"
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

// MetadataStoreSqlite stores all data in sqlite. Data may not be persisted
type MetadataStoreSqlite struct {
	dataDir string
	db      *gorm.DB
	logger  *slog.Logger
}

// New creates a new in-memory database
func New(
	dataDir string,
	logger *slog.Logger,
) (*MetadataStoreSqlite, error) {
	var metadataDb *gorm.DB
	var err error
	if dataDir == "" {
		// No dataDir, use in-memory config
		metadataDb, err = gorm.Open(
			sqlite.Open("file::memory:?cache=shared"),
			&gorm.Config{
				Logger: gormlogger.Discard,
			},
		)
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
		// Open sqlite DB
		metadataDbPath := filepath.Join(
			dataDir,
			"metadata.sqlite",
		)
		// WAL journal mode, disable sync on write, increase cache size to 50MB (from 2MB)
		metadataConnOpts := "_pragma=journal_mode(WAL)&_pragma=sync(OFF)&_pragma=cache_size(-50000)"
		metadataDb, err = gorm.Open(
			sqlite.Open(
				fmt.Sprintf("file:%s?%s", metadataDbPath, metadataConnOpts),
			),
			&gorm.Config{
				Logger: gormlogger.Discard,
			},
		)
		if err != nil {
			return nil, err
		}
	}
	db := &MetadataStoreSqlite{
		db:      metadataDb,
		dataDir: dataDir,
		logger:  logger,
	}
	if err := db.init(); err != nil {
		// MetadataStoreSqlite is available for recovery, so return it with error
		return db, err
	}
	return db, nil
}

func (d *MetadataStoreSqlite) init() error {
	if d.logger == nil {
		// Create logger to throw away logs
		// We do this so we don't have to add guards around every log operation
		d.logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	// Configure tracing for GORM
	if err := d.db.Use(tracing.NewPlugin(tracing.WithoutMetrics())); err != nil {
		return err
	}
	return nil
}

// AutoMigrate wraps the gorm AutoMigrate
func (d *MetadataStoreSqlite) AutoMigrate(dst ...interface{}) error {
	return d.DB().AutoMigrate(dst...)
}

// Close gets the database handle from our MetadataStore and closes it
func (d *MetadataStoreSqlite) Close() error {
	// get DB handle from gorm.DB
	db, err := d.DB().DB()
	if err != nil {
		return err
	}
	return db.Close()
}

// Create creates a record
func (d *MetadataStoreSqlite) Create(value interface{}) *gorm.DB {
	return d.DB().Create(value)
}

// DB returns the database handle
func (d *MetadataStoreSqlite) DB() *gorm.DB {
	return d.db
}

// First returns the first DB entry
func (d *MetadataStoreSqlite) First(args interface{}) *gorm.DB {
	return d.DB().First(args)
}

// Order orders a DB query
func (d *MetadataStoreSqlite) Order(args interface{}) *gorm.DB {
	return d.DB().Order(args)
}

// Transaction creates a gorm transaction
func (d *MetadataStoreSqlite) Transaction() *gorm.DB {
	return d.DB().Begin()
}

// Where constrains a DB query
func (d *MetadataStoreSqlite) Where(
	query interface{},
	args ...interface{},
) *gorm.DB {
	return d.DB().Where(query, args...)
}
