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

//go:build dingo_extra_plugins

package mysql

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/plugin"
	mysqldriver "github.com/go-sql-driver/mysql"
)

var validDatabaseName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func validateDatabaseName(name string) error {
	if !validDatabaseName.MatchString(name) {
		return fmt.Errorf("invalid MySQL database name %q", name)
	}
	return nil
}

type Config struct {
	Host                string        `yaml:"host"`
	Port                uint          `yaml:"port"`
	User                string        `yaml:"user"`
	Password            string        `yaml:"password"`
	Database            string        `yaml:"database"`
	SSLMode             string        `yaml:"sslMode"`
	TimeZone            string        `yaml:"timeZone"`
	DSN                 string        `yaml:"dsn"`
	PoolMaxOpenConns    int           `yaml:"poolMaxOpenConns"`
	PoolMaxIdleConns    int           `yaml:"poolMaxIdleConns"`
	PoolConnMaxLifetime time.Duration `yaml:"poolConnMaxLifetime"`
	// StatementTimeout bounds read-only statement execution (MySQL session
	// variable max_execution_time, applied in milliseconds). Zero leaves
	// the server default (unbounded) in effect. Ignored when DSN is set
	// explicitly, same as SSLMode/TimeZone.
	StatementTimeout time.Duration `yaml:"statementTimeout"`
	// LockTimeout bounds how long a statement will wait for a row lock
	// (MySQL session variable innodb_lock_wait_timeout, whose native unit
	// is whole seconds -- unlike Postgres's lock_timeout, which is
	// milliseconds). Zero leaves the server default in effect. A positive
	// value under one second is rounded up to one second rather than
	// silently becoming zero (unbounded).
	LockTimeout time.Duration `yaml:"lockTimeout"`
	// ReadTimeout/WriteTimeout are the driver's own transport-level I/O
	// deadlines per socket read/write (go-sql-driver/mysql's
	// Config.ReadTimeout/WriteTimeout), not a statement-level bound. Zero
	// leaves them disabled (unbounded), the driver's default.
	ReadTimeout  time.Duration `yaml:"readTimeout"`
	WriteTimeout time.Duration `yaml:"writeTimeout"`
}

func RegisterProvider(host *plugin.Host) error {
	return plugin.Register(
		host,
		plugin.Descriptor{
			Capability:  plugin.CapabilityStorageMetadata,
			Name:        "mysql",
			Description: "MySQL relational database",
		},
		func() Config {
			return Config{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Database: "dingo",
				TimeZone: "UTC",
			}
		},
		func(
			ctx context.Context,
			cfg Config,
			deps metadata.ProviderDependencies,
		) (*sqlstore.Store, plugin.Instance, error) {
			store, err := openStore(ctx, cfg, deps)
			if err != nil {
				return nil, nil, err
			}
			return store, plugin.Lifecycle{
				StartFunc: store.Start,
				StopFunc: func(ctx context.Context) error {
					return store.CloseContext(ctx)
				},
			}, nil
		},
	)
}

// assembleDSN builds a driver DSN from a provider-generated Config (i.e.
// cfg.DSN is empty; an explicit DSN is used verbatim by callers and never
// reaches this function). Named to match postgres.assembleDSN's role.
func assembleDSN(cfg Config) (string, error) {
	location := time.UTC
	if cfg.TimeZone != "" {
		var err error
		location, err = time.LoadLocation(cfg.TimeZone)
		if err != nil {
			return "", fmt.Errorf("load MySQL time zone: %w", err)
		}
	}
	driverConfig := mysqldriver.Config{
		User:   cfg.User,
		Passwd: cfg.Password,
		Net:    "tcp",
		Addr: net.JoinHostPort(
			cfg.Host,
			strconv.FormatUint(uint64(cfg.Port), 10),
		),
		DBName:       cfg.Database,
		ParseTime:    true,
		Loc:          location,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}
	if cfg.SSLMode != "" {
		driverConfig.TLSConfig = cfg.SSLMode
	}
	// max_execution_time and innodb_lock_wait_timeout are session
	// variables, not connection parameters -- the driver issues them as a
	// SET statement on every new physical connection (see
	// (*mysqlConn).handleParams), which is what makes them survive pool
	// churn instead of only applying to the first connection.
	if cfg.StatementTimeout > 0 || cfg.LockTimeout > 0 {
		driverConfig.Params = make(map[string]string, 2)
		if cfg.StatementTimeout > 0 {
			driverConfig.Params["max_execution_time"] = strconv.FormatInt(
				millisecondsRoundUp(cfg.StatementTimeout),
				10,
			)
		}
		if cfg.LockTimeout > 0 {
			seconds := int64(cfg.LockTimeout / time.Second)
			if cfg.LockTimeout%time.Second != 0 {
				seconds++
			}
			driverConfig.Params["innodb_lock_wait_timeout"] = strconv.FormatInt(
				seconds,
				10,
			)
		}
	}
	return driverConfig.FormatDSN(), nil
}

// millisecondsRoundUp converts d to whole milliseconds, rounding a
// sub-millisecond remainder up rather than truncating it away like
// time.Duration.Milliseconds() does. Truncating would silently turn any
// positive sub-millisecond configured value (e.g. 500us) into "0", which
// disables the timeout instead of applying the shortest one the server can
// express. Only called for d > 0.
func millisecondsRoundUp(d time.Duration) int64 {
	ms := d / time.Millisecond
	if d%time.Millisecond != 0 {
		ms++
	}
	return int64(ms)
}

func openStore(
	ctx context.Context,
	cfg Config,
	deps metadata.ProviderDependencies,
) (*sqlstore.Store, error) {
	if cfg.PoolMaxOpenConns < 0 ||
		cfg.PoolMaxIdleConns < 0 ||
		cfg.PoolConnMaxLifetime < 0 {
		return nil, errors.New("MySQL pool limits must not be negative")
	}
	if cfg.StatementTimeout < 0 || cfg.LockTimeout < 0 ||
		cfg.ReadTimeout < 0 || cfg.WriteTimeout < 0 {
		return nil, errors.New("MySQL timeouts must not be negative")
	}
	dsn := cfg.DSN
	if dsn == "" {
		var err error
		dsn, err = assembleDSN(cfg)
		if err != nil {
			return nil, err
		}
	}
	// Explicit DSNs without a configured database are commonly supplied by
	// tests or connection brokers that provision schemas themselves; retain the
	// lazy sql.Open behavior for those callers. Provider-generated DSNs (and
	// explicit DSNs with Database set) use the administrator path so a missing
	// configured database can be created before Store.Start pings it.
	provisionDatabase := cfg.DSN == ""
	if cfg.DSN != "" {
		parsed, parseErr := mysqldriver.ParseDSN(dsn)
		if parseErr != nil {
			return nil, fmt.Errorf("parse MySQL DSN: %w", parseErr)
		}
		// An explicit schema in the DSN is authoritative and should be
		// provisioned. A DSN without a schema is intentionally left alone;
		// callers may select it later through connection/session setup.
		provisionDatabase = parsed.DBName != ""
	}
	if provisionDatabase {
		if err := ensureDatabaseExists(ctx, dsn, cfg.Database); err != nil {
			return nil, err
		}
	}
	db, err := sqlstore.OpenDB("mysql", dsn, "mysql")
	if err != nil {
		return nil, err
	}
	maxOpen := cfg.PoolMaxOpenConns
	if maxOpen == 0 {
		maxOpen = deps.MaxConnections
	}
	if maxOpen <= 0 {
		maxOpen = 100
	}
	maxIdle := cfg.PoolMaxIdleConns
	if maxIdle == 0 {
		maxIdle = 10
	}
	connMaxLifetime := cfg.PoolConnMaxLifetime
	if connMaxLifetime == 0 {
		connMaxLifetime = time.Hour
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(connMaxLifetime)
	registry, err := migrations.MySQLRegistry()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	store, err := sqlstore.New(sqlstore.Config{
		WriteDB:     db,
		ReadDB:      db,
		Dialect:     sqlstore.MySQLDialect(),
		Logger:      deps.Logger,
		StorageMode: deps.StorageMode,
		Migrations:  registry,
		MigrationLocker: migrations.NewAdvisoryLocker(
			"mysql",
			0x64696e676f6d6574,
			30*time.Second,
		),
		BackupTo: func(ctx context.Context, dstPath string) error {
			return backupMySQL(ctx, dsn, dstPath)
		},
		RestoreFrom: func(ctx context.Context, srcPath string) error {
			return restoreMySQL(ctx, dsn, srcPath)
		},
		Reset: func(ctx context.Context) error {
			_, _, database, err := connArgs(dsn)
			if err != nil {
				return err
			}
			return resetDatabase(ctx, db, database)
		},
		ValidateBackup: validateMySQLBackup,
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

// ensureDatabaseExists provisions a configured database before opening the
// metadata pool. MySQL rejects Ping when the DBName in a DSN is absent, so
// connect once without a default schema and create it explicitly.
func ensureDatabaseExists(
	ctx context.Context,
	dsn, configuredName string,
) error {
	driverConfig, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("parse MySQL DSN: %w", err)
	}
	// An explicit DSN is authoritative for the schema name. Provider config
	// carries a default database (currently "dingo"), so preferring it here
	// would provision the default while the pool subsequently connects to a
	// different DBName from the DSN.
	dbName := driverConfig.DBName
	if dbName == "" {
		dbName = configuredName
	}
	if dbName == "" {
		return nil
	}
	if err := validateDatabaseName(dbName); err != nil {
		return fmt.Errorf("cannot create MySQL database: %w", err)
	}
	// Probe the configured schema with the actual application credentials
	// first. Existing databases must remain usable by least-privilege users;
	// CREATE DATABASE requires an administrative privilege even when used with
	// IF NOT EXISTS. Only fall back to the administrator connection for the
	// specific unknown-database error.
	if driverConfig.DBName != "" {
		probe, probeErr := sqlstore.OpenDB("mysql", dsn, "mysql")
		if probeErr != nil {
			return fmt.Errorf("open MySQL metadata connection: %w", probeErr)
		}
		pingErr := probe.PingContext(ctx)
		_ = probe.Close()
		if pingErr == nil {
			return nil
		}
		var mysqlErr *mysqldriver.MySQLError
		if !errors.As(pingErr, &mysqlErr) || mysqlErr.Number != 1049 {
			return fmt.Errorf(
				"ping MySQL metadata database %q: %w",
				dbName,
				pingErr,
			)
		}
	}
	driverConfig.DBName = ""
	admin, err := sqlstore.OpenDB("mysql", driverConfig.FormatDSN(), "mysql")
	if err != nil {
		return fmt.Errorf("open MySQL admin connection: %w", err)
	}
	defer admin.Close()
	if err := admin.PingContext(ctx); err != nil {
		return fmt.Errorf("ping MySQL admin connection: %w", err)
	}
	quoted := "`" + strings.ReplaceAll(dbName, "`", "``") + "`"
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quoted); err != nil {
		return fmt.Errorf("create MySQL database %q: %w", dbName, err)
	}
	return nil
}
