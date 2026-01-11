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

package postgres

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/plugin/opentelemetry/tracing"
)

// postgresTxn wraps a gorm transaction and implements types.Txn
type postgresTxn struct {
	db       *gorm.DB
	finished bool
	beginErr error
}

func newPostgresTxn(db *gorm.DB) *postgresTxn {
	return &postgresTxn{db: db}
}

func newFailedPostgresTxn(err error) *postgresTxn {
	return &postgresTxn{beginErr: err}
}

func (t *postgresTxn) Commit() error {
	if t.beginErr != nil {
		return t.beginErr
	}
	if t.finished {
		return nil
	}
	if t.db == nil {
		t.finished = true
		return nil
	}
	if result := t.db.Commit(); result.Error != nil {
		return result.Error
	}
	t.finished = true
	return nil
}

func (t *postgresTxn) Rollback() error {
	if t.beginErr != nil {
		return t.beginErr
	}
	if t.finished {
		return nil
	}
	if t.db != nil {
		if result := t.db.Rollback(); result.Error != nil {
			return result.Error
		}
	}
	t.finished = true
	return nil
}

// MetadataStorePostgres stores metadata in Postgres.
type MetadataStorePostgres struct {
	promRegistry prometheus.Registerer
	db           *gorm.DB
	logger       *slog.Logger

	host     string
	port     uint
	user     string
	password string
	database string
	sslMode  string
	timeZone string
	dsn      string // Data source name (postgres connection string)
}

// New creates a new database
func New(
	host string,
	port uint,
	user string,
	password string,
	database string,
	sslMode string,
	timeZone string,
	logger *slog.Logger,
	promRegistry prometheus.Registerer,
) (*MetadataStorePostgres, error) {
	return NewWithOptions(
		WithHost(host),
		WithPort(port),
		WithUser(user),
		WithPassword(password),
		WithDatabase(database),
		WithSSLMode(sslMode),
		WithTimeZone(timeZone),
		WithLogger(logger),
		WithPromRegistry(promRegistry),
	)
}

// NewWithOptions creates a new database with options
func NewWithOptions(opts ...PostgresOptionFunc) (*MetadataStorePostgres, error) {
	db := &MetadataStorePostgres{}

	// Apply options
	for _, opt := range opts {
		opt(db)
	}

	// Set defaults after options are applied (no side effects)
	if db.host == "" {
		db.host = "localhost"
	}
	if db.port == 0 {
		db.port = 5432
	}
	if db.user == "" {
		db.user = "postgres"
	}
	if db.database == "" {
		db.database = "postgres"
	}
	if db.sslMode == "" {
		db.sslMode = "disable"
	}
	if db.timeZone == "" {
		db.timeZone = "UTC"
	}
	if db.logger == nil {
		db.logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	// Note: Database initialization moved to Start()
	return db, nil
}

func (d *MetadataStorePostgres) init() error {
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
func (d *MetadataStorePostgres) AutoMigrate(dst ...any) error {
	return d.DB().AutoMigrate(dst...)
}

// Start implements the plugin.Plugin interface
func (d *MetadataStorePostgres) Start() error {
	dsn := strings.TrimSpace(d.dsn)

	if dsn == "" {
		parts := []string{
			"host=" + d.host,
			"user=" + d.user,
			"password=" + d.password,
			"dbname=" + d.database,
			"port=" + strconv.FormatUint(uint64(d.port), 10),
			"sslmode=" + d.sslMode,
		}
		if d.timeZone != "" {
			parts = append(parts, "TimeZone="+d.timeZone)
		}
		dsn = strings.Join(parts, " ")
	}

	metadataDb, err := gorm.Open(
		postgres.Open(dsn),
		&gorm.Config{
			Logger:                 gormlogger.Discard,
			SkipDefaultTransaction: true,
			PrepareStmt:            true,
		},
	)
	if err != nil {
		return err
	}
	d.logger.Info(
		"connected to postgres metadata store",
		"host", d.host,
		"port", d.port,
		"database", d.database,
	)
	d.db = metadataDb
	// Configure connection pool
	sqlDB, err := d.db.DB()
	if err != nil {
		return err
	}
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)

	if err := d.init(); err != nil {
		// MetadataStorePostgres is available for recovery, so return error but keep instance
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
func (d *MetadataStorePostgres) Stop() error {
	return d.Close()
}

// Close gets the database handle from our MetadataStore and closes it
func (d *MetadataStorePostgres) Close() error {
	// Guard against nil DB handle (e.g., if Start() failed or was never called)
	if d.db == nil {
		return nil
	}
	// get DB handle from gorm.DB
	db, err := d.DB().DB()
	if err != nil {
		return err
	}
	return db.Close()
}

// Create creates a record
func (d *MetadataStorePostgres) Create(value any) *gorm.DB {
	return d.DB().Create(value)
}

// DB returns the database handle
func (d *MetadataStorePostgres) DB() *gorm.DB {
	return d.db
}

// First returns the first DB entry
func (d *MetadataStorePostgres) First(args any) *gorm.DB {
	return d.DB().First(args)
}

// Order orders a DB query
func (d *MetadataStorePostgres) Order(args any) *gorm.DB {
	return d.DB().Order(args)
}

// Transaction creates a gorm transaction
func (d *MetadataStorePostgres) Transaction() types.Txn {
	db := d.DB().Begin()
	if db.Error != nil {
		d.logger.Error(
			"failed to begin transaction",
			"error", db.Error,
		)
		return newFailedPostgresTxn(db.Error)
	}
	return newPostgresTxn(db)
}

// BeginTxn starts a transaction and returns the handle with an error.
// Callers that prefer explicit error handling can use this instead of Transaction().
func (d *MetadataStorePostgres) BeginTxn() (types.Txn, error) {
	db := d.DB().Begin()
	if db.Error != nil {
		d.logger.Error(
			"failed to begin transaction",
			"error", db.Error,
		)
		return newFailedPostgresTxn(db.Error), db.Error
	}
	return newPostgresTxn(db), nil
}

// Where constrains a DB query
func (d *MetadataStorePostgres) Where(
	query any,
	args ...any,
) *gorm.DB {
	return d.DB().Where(query, args...)
}
