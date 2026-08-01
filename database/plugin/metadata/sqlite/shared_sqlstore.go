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

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	_ "github.com/glebarez/go-sqlite"
)

var sharedMemoryDBSequence atomic.Uint64

// NewSQLStore opens the direct database/sql SQLite implementation. Provider
// registration switches to this constructor only after the full metadata
// contract has moved to sqlstore.
func NewSQLStore(
	config Config,
	dependencies metadata.ProviderDependencies,
) (*sqlstore.Store, error) {
	store, _, _, err := openSQLStore(config, dependencies)
	return store, err
}

func openSQLStore(
	config Config,
	dependencies metadata.ProviderDependencies,
) (*sqlstore.Store, *sql.DB, *sql.DB, error) {
	dataDir := dependencies.DataDir
	if config.DataDir != "" {
		dataDir = config.DataDir
	}
	maxConnections := dependencies.MaxConnections
	if config.MaxConnections > 0 {
		maxConnections = config.MaxConnections
	}
	if maxConnections <= 0 {
		maxConnections = DefaultMaxConnections
	}
	registry, err := migrations.SQLiteRegistry()
	if err != nil {
		return nil, nil, nil, err
	}

	var (
		writeDB      *sql.DB
		readDB       *sql.DB
		locker       migrations.Locker
		diskSizeFunc func() (int64, error)
		maintenance  func(context.Context) error
	)
	if dataDir == "" {
		dsn := fmt.Sprintf(
			"file:dingo_sqlstore_%d?mode=memory&cache=shared"+
				"&_pragma=busy_timeout(30000)&_pragma=foreign_keys(1)",
			sharedMemoryDBSequence.Add(1),
		)
		writeDB, err = sqlstore.OpenDB("sqlite", dsn, "sqlite")
		if err != nil {
			return nil, nil, nil, err
		}
		readDB = writeDB
		writeDB.SetMaxOpenConns(maxConnections)
		writeDB.SetMaxIdleConns(maxConnections)
		locker = migrations.NewProcessLocker()
	} else {
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return nil, nil, nil, fmt.Errorf(
				"create SQLite metadata directory: %w",
				err,
			)
		}
		databasePath := filepath.Join(dataDir, "metadata.sqlite")
		// Build a proper file URI so ?, #, %, and other URI-reserved
		// characters in the configured data directory remain part of the
		// filename instead of being interpreted as DSN options/fragments.
		databaseURI := sqliteFileURI(databasePath)
		commonPragmas := "&_pragma=journal_mode(WAL)" +
			"&_pragma=synchronous(NORMAL)" +
			"&_pragma=cache_size(-50000)" +
			"&_pragma=busy_timeout(30000)" +
			"&_pragma=foreign_keys(1)" +
			"&_pragma=mmap_size(268435456)"
		writeDB, err = sqlstore.OpenDB(
			"sqlite",
			fmt.Sprintf(
				"%s?_txlock=immediate%s",
				databaseURI,
				commonPragmas,
			),
			"sqlite",
		)
		if err != nil {
			return nil, nil, nil, err
		}
		readDB, err = sqlstore.OpenDB(
			"sqlite",
			fmt.Sprintf(
				"%s?mode=ro%s",
				databaseURI,
				commonPragmas,
			),
			"sqlite",
		)
		if err != nil {
			_ = writeDB.Close()
			return nil, nil, nil, err
		}
		writeDB.SetMaxOpenConns(1)
		writeDB.SetMaxIdleConns(1)
		readDB.SetMaxOpenConns(maxConnections)
		readDB.SetMaxIdleConns(maxConnections)
		locker = migrations.NewFileLocker(databasePath + ".migrate.lock")
		diskSizeFunc = sqliteDiskSize(writeDB, databasePath)
		maintenance = func(ctx context.Context) error {
			_, err := writeDB.ExecContext(ctx, "VACUUM")
			return err
		}
	}

	store, err := sqlstore.New(sqlstore.Config{
		WriteDB:             writeDB,
		ReadDB:              readDB,
		Dialect:             sqlstore.SQLiteDialect(),
		Logger:              dependencies.Logger,
		StorageMode:         dependencies.StorageMode,
		Migrations:          registry,
		MigrationLocker:     locker,
		DiskSize:            diskSizeFunc,
		Maintenance:         maintenance,
		MaintenanceInterval: 24 * time.Hour,
	})
	if err != nil {
		if readDB != writeDB {
			_ = readDB.Close()
		}
		_ = writeDB.Close()
		return nil, nil, nil, err
	}
	return store, writeDB, readDB, nil
}

func sqliteDiskSize(
	db *sql.DB,
	databasePath string,
) func() (int64, error) {
	return func() (int64, error) {
		var pageCount, pageSize int64
		if err := db.QueryRow("PRAGMA page_count").Scan(&pageCount); err != nil {
			return 0, fmt.Errorf("SQLite page count: %w", err)
		}
		if err := db.QueryRow("PRAGMA page_size").Scan(&pageSize); err != nil {
			return 0, fmt.Errorf("SQLite page size: %w", err)
		}
		total := pageCount * pageSize
		for _, path := range []string{
			databasePath,
			databasePath + "-wal",
			databasePath + "-shm",
		} {
			info, err := os.Stat(path)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				return 0, fmt.Errorf("stat SQLite file %s: %w", path, err)
			}
			if path == databasePath {
				if info.Size() > total {
					total = info.Size()
				}
			} else {
				total += info.Size()
			}
		}
		return total, nil
	}
}

// sqliteFileURI converts an OS path to the file URI form expected by SQLite.
// Windows volume paths need an extra leading slash (file:///C:/...), while
// URL.String handles escaping reserved characters in either form.
func sqliteFileURI(databasePath string) string {
	path := filepath.ToSlash(databasePath)
	if filepath.VolumeName(databasePath) != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return (&url.URL{Scheme: "file", Path: path}).String()
}
