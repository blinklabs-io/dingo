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

// MetadataStoreSqlite is a SQLite-based implementation of the metadata store.
// It provides persistent storage for blockchain metadata including certificates,
// transactions, blocks, and other ledger state data.
type MetadataStoreSqlite struct {
	promRegistry            prometheus.Registerer
	db                      *gorm.DB
	logger                  *slog.Logger
	timerVacuum             *time.Timer
	timerCertificateCleanup *time.Timer
	timerMutex              sync.Mutex
	dataDir                 string
	closed                  bool
	vacuumWG                sync.WaitGroup
	certificateCleanupWG    sync.WaitGroup
}

// New creates a SQLite metadata store. Uses in-memory database if dataDir is empty.
func New(
	dataDir string,
	logger *slog.Logger,
	promRegistry prometheus.Registerer,
) (*MetadataStoreSqlite, error) {
	var metadataDb *gorm.DB
	var err error
	if dataDir == "" {
		// Use in-memory database when no data directory is specified, useful for testing
		// cache=shared allows multiple connections to share the same in-memory database
		metadataDb, err = gorm.Open(
			sqlite.Open("file::memory:?cache=shared"),
			&gorm.Config{
				Logger:                 gormlogger.Discard,
				SkipDefaultTransaction: true,
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
				Logger:                 gormlogger.Discard,
				SkipDefaultTransaction: true,
			},
		)
		if err != nil {
			return nil, err
		}
	}
	db := &MetadataStoreSqlite{
		db:           metadataDb,
		dataDir:      dataDir,
		logger:       logger,
		promRegistry: promRegistry,
	}
	if err := db.init(); err != nil {
		// MetadataStoreSqlite is available for recovery, so return it with error
		return db, err
	}
	// Create table schemas
	db.logger.Debug(fmt.Sprintf("creating table: %#v", &CommitTimestamp{}))
	if err := db.db.AutoMigrate(&CommitTimestamp{}); err != nil {
		return db, err
	}
	for _, model := range models.MigrateModels {
		db.logger.Debug(fmt.Sprintf("creating table: %#v", model))
		if err := db.db.AutoMigrate(model); err != nil {
			return db, err
		}
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
	// Schedule daily database vacuum to free unused space
	d.scheduleDailyVacuum()
	// Schedule periodic certificate cleanup for data integrity
	d.scheduleCertificateCleanup()
	return nil
}

func (d *MetadataStoreSqlite) runVacuum() error {
	d.timerMutex.Lock()
	if d.dataDir == "" || d.closed {
		d.timerMutex.Unlock()
		return nil
	}
	// Track this vacuum operation while we know the store is open
	d.vacuumWG.Add(1)
	d.timerMutex.Unlock()
	defer d.vacuumWG.Done()

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

// runCertificateCleanup performs periodic cleanup of certificate data for data integrity.
// Currently a no-op placeholder - implement cleanup logic as needed.
func (d *MetadataStoreSqlite) runCertificateCleanup() error {
	d.timerMutex.Lock()
	if d.closed {
		d.timerMutex.Unlock()
		return nil
	}
	// Track this cleanup operation while we know the store is open
	d.certificateCleanupWG.Add(1)
	d.timerMutex.Unlock()
	defer d.certificateCleanupWG.Done()

	// TODO: Implement certificate cleanup logic
	// This could include:
	// - Cleaning up orphaned certificate records
	// - Removing certificates from invalid transactions beyond rollback window
	// - Optimizing certificate indexes
	// - Removing duplicate or redundant certificate data

	d.logger.Debug(
		"running certificate cleanup on sqlite metadata database",
		"component", "database",
	)

	// Placeholder for certificate cleanup logic
	// For now, this is a no-op that can be implemented as needed

	return nil
}

// scheduleCertificateCleanup schedules periodic certificate cleanup
func (d *MetadataStoreSqlite) scheduleCertificateCleanup() {
	d.timerMutex.Lock()
	defer d.timerMutex.Unlock()
	if d.closed {
		return
	}

	if d.timerCertificateCleanup != nil {
		d.timerCertificateCleanup.Stop()
	}
	// Run certificate cleanup every 6 hours (4 times per day)
	interval := time.Duration(6) * time.Hour
	f := func() {
		// schedule next run
		defer d.scheduleCertificateCleanup()
		if err := d.runCertificateCleanup(); err != nil {
			d.logger.Error(
				"failed to cleanup certificate data in metadata store",
				"component", "database",
				"error", err,
			)
		}
	}
	d.timerCertificateCleanup = time.AfterFunc(interval, f)
}

// AutoMigrate creates or updates database schema for the given models.
func (d *MetadataStoreSqlite) AutoMigrate(dst ...any) error {
	return d.DB().AutoMigrate(dst...)
}

// Close shuts down the database connection and stops background processes.
func (d *MetadataStoreSqlite) Close() error {
	d.timerMutex.Lock()
	d.closed = true
	if d.timerVacuum != nil {
		d.timerVacuum.Stop()
		d.timerVacuum = nil
	}
	if d.timerCertificateCleanup != nil {
		d.timerCertificateCleanup.Stop()
		d.timerCertificateCleanup = nil
	}
	d.timerMutex.Unlock()

	// Wait for any in-flight vacuum operations to complete
	d.vacuumWG.Wait()

	// Wait for any in-flight certificate cleanup operations to complete
	d.certificateCleanupWG.Wait()

	// get DB handle from gorm.DB
	db, err := d.DB().DB()
	if err != nil {
		return fmt.Errorf("get database handle: %w", err)
	}
	return db.Close()
}

// Create inserts a new record into the database.
func (d *MetadataStoreSqlite) Create(value any) *gorm.DB {
	return d.DB().Create(value)
}

// DB returns the underlying GORM database handle.
func (d *MetadataStoreSqlite) DB() *gorm.DB {
	return d.db
}

// First retrieves the first record that matches the query.
func (d *MetadataStoreSqlite) First(args any) *gorm.DB {
	return d.DB().First(args)
}

// Order adds ORDER BY clause to the database query.
func (d *MetadataStoreSqlite) Order(args any) *gorm.DB {
	return d.DB().Order(args)
}

// Transaction creates a new database transaction.
func (d *MetadataStoreSqlite) Transaction() *gorm.DB {
	return d.DB().Begin()
}

// Where adds WHERE conditions to the database query.
func (d *MetadataStoreSqlite) Where(
	query any,
	args ...any,
) *gorm.DB {
	return d.DB().Where(query, args...)
}
