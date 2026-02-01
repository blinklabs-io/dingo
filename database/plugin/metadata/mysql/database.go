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

package mysql

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/plugin/opentelemetry/tracing"
)

// mysqlTxn wraps a gorm transaction and implements types.Txn
type mysqlTxn struct {
	db       *gorm.DB
	finished bool
	beginErr error
}

func newMysqlTxn(db *gorm.DB) *mysqlTxn {
	return &mysqlTxn{db: db}
}

func newFailedMysqlTxn(err error) *mysqlTxn {
	return &mysqlTxn{beginErr: err}
}

func (t *mysqlTxn) Commit() error {
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

func (t *mysqlTxn) Rollback() error {
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

// MetadataStoreMysql stores metadata in MySQL.
type MetadataStoreMysql struct {
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
	dsn      string // Data source name (MySQL connection string)
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
) (*MetadataStoreMysql, error) {
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
func NewWithOptions(opts ...MysqlOptionFunc) (*MetadataStoreMysql, error) {
	db := &MetadataStoreMysql{}

	// Apply options
	for _, opt := range opts {
		opt(db)
	}

	// Set defaults after options are applied (no side effects)
	if db.host == "" {
		db.host = "localhost"
	}
	if db.port == 0 {
		db.port = 3306
	}
	if db.user == "" {
		db.user = "root"
	}
	if db.database == "" {
		db.database = "mysql"
	}
	if db.sslMode == "" {
		db.sslMode = ""
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

func (d *MetadataStoreMysql) init() error {
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
func (d *MetadataStoreMysql) AutoMigrate(dst ...any) error {
	return d.DB().AutoMigrate(dst...)
}

// Start implements the plugin.Plugin interface
func (d *MetadataStoreMysql) Start() error {
	dsn := strings.TrimSpace(d.dsn)
	logDatabase := d.database

	if dsn == "" {
		cfg := mysql.Config{
			User:   d.user,
			Passwd: d.password,
			Net:    "tcp",
			Addr: fmt.Sprintf(
				"%s:%s",
				d.host,
				strconv.FormatUint(uint64(d.port), 10),
			),
			DBName:               d.database,
			ParseTime:            true,
			AllowNativePasswords: true,
		}
		if d.timeZone != "" {
			loc, err := time.LoadLocation(d.timeZone)
			if err != nil {
				loc = time.UTC
			}
			cfg.Loc = loc
			if cfg.Params == nil {
				cfg.Params = map[string]string{}
			}
			cfg.Params["loc"] = d.timeZone
		}
		if d.sslMode != "" {
			if cfg.Params == nil {
				cfg.Params = map[string]string{}
			}
			cfg.Params["tls"] = d.sslMode
		}
		dsn = cfg.FormatDSN()
	} else if parsedDB, ok := parseMysqlDatabaseFromDSN(dsn); ok {
		logDatabase = parsedDB
	}

	metadataDb, err := gorm.Open(
		gormmysql.Open(dsn),
		&gorm.Config{
			Logger:                 gormlogger.Discard,
			SkipDefaultTransaction: true,
			PrepareStmt:            true,
		},
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1049 {
			if created, createErr := d.ensureDatabaseExists(dsn, logDatabase); createErr == nil &&
				created {
				metadataDb, err = gorm.Open(
					gormmysql.Open(dsn),
					&gorm.Config{
						Logger:                 gormlogger.Discard,
						SkipDefaultTransaction: true,
						PrepareStmt:            true,
					},
				)
			}
		}
		if err != nil {
			return err
		}
	}
	if metadataDb == nil {
		return err
	}
	d.logger.Info(
		"connected to mysql metadata store",
		"host", d.host,
		"port", d.port,
		"database", logDatabase,
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
		// MetadataStoreMysql is available for recovery, so return error but keep instance
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

func (d *MetadataStoreMysql) ensureDatabaseExists(
	dsn string,
	dbName string,
) (bool, error) {
	if dbName == "" {
		return false, nil
	}
	adminDsn, ok := stripDatabaseFromDSN(dsn)
	if !ok {
		return false, nil
	}
	adminDb, err := gorm.Open(
		gormmysql.Open(adminDsn),
		&gorm.Config{
			Logger:                 gormlogger.Discard,
			SkipDefaultTransaction: true,
			PrepareStmt:            true,
		},
	)
	if err != nil {
		return false, err
	}
	sqlAdminDb, err := adminDb.DB()
	if err != nil {
		return false, err
	}
	defer sqlAdminDb.Close()
	if result := adminDb.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)); result.Error != nil {
		return false, result.Error
	}
	return true, nil
}

func parseMysqlDatabaseFromDSN(dsn string) (string, bool) {
	base := dsn
	if idx := strings.Index(base, "?"); idx >= 0 {
		base = base[:idx]
	}
	slash := strings.LastIndex(base, "/")
	if slash < 0 || slash == len(base)-1 {
		return "", false
	}
	return base[slash+1:], true
}

func stripDatabaseFromDSN(dsn string) (string, bool) {
	base := dsn
	params := ""
	if idx := strings.Index(dsn, "?"); idx >= 0 {
		base = dsn[:idx]
		params = dsn[idx+1:]
	}
	slash := strings.LastIndex(base, "/")
	if slash < 0 {
		return "", false
	}
	base = base[:slash+1]
	if params == "" {
		return base, true
	}
	return base + "?" + params, true
}

// Stop implements the plugin.Plugin interface
func (d *MetadataStoreMysql) Stop() error {
	return d.Close()
}

// Close gets the database handle from our MetadataStore and closes it
func (d *MetadataStoreMysql) Close() error {
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
func (d *MetadataStoreMysql) Create(value any) *gorm.DB {
	return d.DB().Create(value)
}

// DB returns the database handle
func (d *MetadataStoreMysql) DB() *gorm.DB {
	return d.db
}

// First returns the first DB entry
func (d *MetadataStoreMysql) First(args any) *gorm.DB {
	return d.DB().First(args)
}

// Order orders a DB query
func (d *MetadataStoreMysql) Order(args any) *gorm.DB {
	return d.DB().Order(args)
}

// Transaction creates a gorm transaction
func (d *MetadataStoreMysql) Transaction() types.Txn {
	db := d.DB().Begin()
	if db.Error != nil {
		d.logger.Error(
			"failed to begin transaction",
			"error", db.Error,
		)
		return newFailedMysqlTxn(db.Error)
	}
	return newMysqlTxn(db)
}

// BeginTxn starts a transaction and returns the handle with an error.
// Callers that prefer explicit error handling can use this instead of Transaction().
func (d *MetadataStoreMysql) BeginTxn() (types.Txn, error) {
	db := d.DB().Begin()
	if db.Error != nil {
		d.logger.Error(
			"failed to begin transaction",
			"error", db.Error,
		)
		return newFailedMysqlTxn(db.Error), db.Error
	}
	return newMysqlTxn(db), nil
}

// Where constrains a DB query
func (d *MetadataStoreMysql) Where(
	query any,
	args ...any,
) *gorm.DB {
	return d.DB().Where(query, args...)
}
