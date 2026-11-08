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

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
)

// Running the full SQLite migration chain dominates the cost of a test store:
// about 0.25s without the race detector and 5-6s with it, versus a few
// milliseconds to copy an already-migrated schema. Tests that need the current
// schema therefore start from a copy of one schema migrated once per test
// binary, and the store's normal Start finds a database already at the target
// version. Tests that exercise the migrations themselves must not use this.
const migratedTemplateURI = "file:sqlstore_migrated_template?mode=memory&cache=shared"

var (
	migratedTemplateOnce sync.Once
	// migratedTemplateDB is deliberately never closed: closing the last
	// connection to a shared-cache in-memory database discards it.
	migratedTemplateDB  *sql.DB
	migratedTemplateErr error
)

func buildMigratedTemplate() error {
	db, err := sql.Open("sqlite", migratedTemplateURI)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	registry, err := migrations.SQLiteRegistry()
	if err != nil {
		return err
	}
	locker := migrations.NewProcessLocker()
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: locker,
	})
	if err != nil {
		return err
	}
	// Run the migrations without Start so no maintenance ticker or prepared
	// statement outlives the template.
	runner := migrations.Runner{
		BeforeUpgrade: store.preflightRewardStakeUpgrade,
		DB:            db,
		Dialect:       store.dialect.Name(),
		Registry:      registry,
		Locker:        locker,
		Logger:        store.logger,
		Rebind:        store.dialect.Rebind,
	}
	if err := runner.Run(context.Background()); err != nil {
		return err
	}
	migratedTemplateDB = db
	return nil
}

// seedMigratedSQLite copies the migrated template into an empty SQLite
// database that has not been started yet, whether in-memory (shared-cache
// included) or file-backed. A backup restore goes through SQLite's page cache,
// so every connection of db sees the copy.
func seedMigratedSQLite(tb testing.TB, db *sql.DB) {
	tb.Helper()
	migratedTemplateOnce.Do(func() { migratedTemplateErr = buildMigratedTemplate() })
	require.NoError(tb, migratedTemplateErr)
	conn, err := db.Conn(context.Background())
	require.NoError(tb, err)
	defer func() { require.NoError(tb, conn.Close()) }()
	require.NoError(tb, conn.Raw(func(driverConn any) error {
		r, ok := driverConn.(interface {
			NewRestore(string) (*sqlite.Backup, error)
		})
		if !ok {
			return fmt.Errorf("sqlite driver connection %T cannot restore", driverConn)
		}
		backup, err := r.NewRestore(migratedTemplateURI)
		if err != nil {
			return err
		}
		_, stepErr := backup.Step(-1)
		return errors.Join(stepErr, backup.Finish())
	}))
}

// seedMigratedSQLiteDSN seeds a shared-cache in-memory database that the
// store will reach through a wrapping driver whose connections cannot restore.
// The seeding handle stays open until the test ends so the database outlives
// the gap before the store connects.
func seedMigratedSQLiteDSN(tb testing.TB, dsn string) {
	tb.Helper()
	db, err := sql.Open("sqlite", dsn)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, db.Close()) })
	seedMigratedSQLite(tb, db)
}
