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
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/glebarez/sqlite"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/plugin/opentelemetry/tracing"
)

// MetadataStoreSqlite stores all data in sqlite. Data may not be persisted
type MetadataStoreSqlite struct {
	promRegistry prometheus.Registerer
	db           *gorm.DB
	logger       *slog.Logger
	timerVacuum  *time.Timer
	dataDir      string
	timerMutex   sync.Mutex
	closed       bool
}

// New creates a new database
func New(
	dataDir string,
	logger *slog.Logger,
	promRegistry prometheus.Registerer,
) (*MetadataStoreSqlite, error) {
	return NewWithOptions(
		WithDataDir(dataDir),
		WithLogger(logger),
		WithPromRegistry(promRegistry),
	)
}

// NewWithOptions creates a new database with options
func NewWithOptions(opts ...SqliteOptionFunc) (*MetadataStoreSqlite, error) {
	db := &MetadataStoreSqlite{}

	// Apply options
	for _, opt := range opts {
		opt(db)
	}

	// Set defaults after options are applied (no side effects)
	if db.logger == nil {
		db.logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	// Note: Database initialization moved to Start()
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
	// Schedule daily database vacuum to free unused space
	d.scheduleDailyVacuum()
	return nil
}

func (d *MetadataStoreSqlite) runVacuum() error {
	d.timerMutex.Lock()
	closed := d.closed
	d.timerMutex.Unlock()
	if d.dataDir == "" || closed {
		return nil
	}
	if result := d.DB().Raw("VACUUM"); result.Error != nil {
		return result.Error
	}
	return nil
}

// scheduleDailyVacuum schedules a daily vacuum operation
func (d *MetadataStoreSqlite) scheduleDailyVacuum() {
	d.timerMutex.Lock()
	defer d.timerMutex.Unlock()
	if d.closed {
		return
	}

	if d.timerVacuum != nil {
		d.timerVacuum.Stop()
	}
	daily := time.Duration(24) * time.Hour
	f := func() {
		d.logger.Debug(
			"running vacuum on sqlite metadata database",
		)
		// schedule next run
		defer d.scheduleDailyVacuum()
		if err := d.runVacuum(); err != nil {
			d.logger.Error(
				"failed to free unused space in metadata store",
				"component", "database",
			)
		}
	}
	d.timerVacuum = time.AfterFunc(daily, f)
}

// AutoMigrate wraps the gorm AutoMigrate
func (d *MetadataStoreSqlite) AutoMigrate(dst ...any) error {
	return d.DB().AutoMigrate(dst...)
}

// Start implements the plugin.Plugin interface
func (d *MetadataStoreSqlite) Start() error {
	var metadataDb *gorm.DB
	var err error
	if d.dataDir == "" {
		// No dataDir, use in-memory config
		metadataDb, err = gorm.Open(
			sqlite.Open("file::memory:?cache=shared"),
			&gorm.Config{
				Logger:                 gormlogger.Discard,
				SkipDefaultTransaction: true,
			},
		)
		if err != nil {
			return err
		}
	} else {
		// Make sure that we can read data dir, and create if it doesn't exist
		if _, err := os.Stat(d.dataDir); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("failed to read data dir: %w", err)
			}
			// Create data directory
			if err := os.MkdirAll(d.dataDir, 0o755); err != nil {
				return fmt.Errorf("failed to create data dir: %w", err)
			}
		}
		// Open sqlite DB
		metadataDbPath := filepath.Join(
			d.dataDir,
			"metadata.sqlite",
		)
		// WAL journal mode, disable sync on write, increase cache size to 50MB (from 2MB)
		metadataConnOpts := "_pragma=journal_mode(WAL)&_pragma=sync(OFF)&_pragma=cache_size(-50000)"
		metadataDb, err = gorm.Open(
			sqlite.Open(
				fmt.Sprintf("file:%s?%s", metadataDbPath, metadataConnOpts),
			),
			&gorm.Config{
				Logger:                 gormlogger.Discard,
				SkipDefaultTransaction: true,
			},
		)
		if err != nil {
			return err
		}
	}
	d.db = metadataDb
	if err := d.init(); err != nil {
		// MetadataStoreSqlite is available for recovery, so return error but keep instance
		return err
	}
	// Create table schemas
	d.logger.Debug(fmt.Sprintf("creating table: %#v", &CommitTimestamp{}))
	if err := d.db.AutoMigrate(&CommitTimestamp{}); err != nil {
		return err
	}
	for _, model := range models.MigrateModels {
		d.logger.Debug(fmt.Sprintf("creating table: %#v", model))
		if err := d.db.AutoMigrate(model); err != nil {
			return err
		}
	}
	return nil
}

// Stop implements the plugin.Plugin interface
func (d *MetadataStoreSqlite) Stop() error {
	return d.Close()
}

// Close gets the database handle from our MetadataStore and closes it
func (d *MetadataStoreSqlite) Close() error {
	d.timerMutex.Lock()
	d.closed = true
	if d.timerVacuum != nil {
		d.timerVacuum.Stop()
		d.timerVacuum = nil
	}
	d.timerMutex.Unlock()

	// get DB handle from gorm.DB
	db, err := d.DB().DB()
	if err != nil {
		return err
	}
	return db.Close()
}

// Create creates a record
func (d *MetadataStoreSqlite) Create(value any) *gorm.DB {
	return d.DB().Create(value)
}

// DB returns the database handle
func (d *MetadataStoreSqlite) DB() *gorm.DB {
	return d.db
}

// First returns the first DB entry
func (d *MetadataStoreSqlite) First(args any) *gorm.DB {
	return d.DB().First(args)
}

// Order orders a DB query
func (d *MetadataStoreSqlite) Order(args any) *gorm.DB {
	return d.DB().Order(args)
}

// Transaction creates a gorm transaction
func (d *MetadataStoreSqlite) Transaction() *gorm.DB {
	return d.DB().Begin()
}

// Where constrains a DB query
func (d *MetadataStoreSqlite) Where(
	query any,
	args ...any,
) *gorm.DB {
	return d.DB().Where(query, args...)
}
