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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

var testStoreSequence atomic.Uint64

func newTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB: db,
		Dialect: SQLiteDialect(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}

func TestTransactionSavepointAndCommit(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	_, err := store.writeDB.Exec(
		"CREATE TABLE item (id INTEGER PRIMARY KEY, value TEXT)",
	)
	require.NoError(t, err)
	transaction := store.Transaction()
	savepointer, ok := transaction.(interface {
		SavePoint(string) error
		RollbackTo(string) error
	})
	require.True(t, ok)
	queryer, err := store.dbFromTxn(transaction)
	require.NoError(t, err)
	_, err = queryer.ExecContext(
		context.Background(),
		"INSERT INTO item (id, value) VALUES (1, 'kept')",
	)
	require.NoError(t, err)
	require.NoError(t, savepointer.SavePoint("after_first"))
	_, err = queryer.ExecContext(
		context.Background(),
		"INSERT INTO item (id, value) VALUES (2, 'rolled-back')",
	)
	require.NoError(t, err)
	require.NoError(t, savepointer.RollbackTo("after_first"))
	require.NoError(t, transaction.Commit())
	require.NoError(t, transaction.Commit())

	var count int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM item",
	).Scan(&count))
	require.Equal(t, 1, count)
}

func TestTransactionRejectsUnsafeSavepoint(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	transaction := store.Transaction()
	savepointer := transaction.(interface {
		SavePoint(string) error
	})
	require.Error(t, savepointer.SavePoint(`bad"; DROP TABLE item`))
	require.NoError(t, transaction.Rollback())
}

func TestStoreRejectsForeignTransaction(t *testing.T) {
	t.Parallel()
	first := newTestStore(t)
	second := newTestStore(t)
	transaction := first.Transaction()
	_, err := second.dbFromTxn(transaction)
	require.ErrorContains(t, err, "another store")
	require.NoError(t, transaction.Rollback())
}

func TestStoreReadinessGatesTransactions(t *testing.T) {
	t.Parallel()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	store, err := New(Config{WriteDB: db, Dialect: SQLiteDialect()})
	require.NoError(t, err)
	transaction := store.Transaction()
	require.ErrorContains(t, transaction.Commit(), "not ready")
	require.NoError(t, store.Start(context.Background()))
	require.True(t, store.Ready())
	require.NoError(t, store.Close())
	require.False(t, store.Ready())
}

func TestStoreMigrationFailurePreventsReadiness(t *testing.T) {
	t.Parallel()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		MigrationLocker: migrations.NewProcessLocker(),
		Migrations: []migrations.Migration{{
			Version: 1,
			Name:    "broken",
			SQL: map[string]migrations.SQL{
				"sqlite": {
					Expand: []string{"invalid sql"},
				},
			},
		}},
	})
	require.NoError(t, err)
	err = store.Start(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed in expand")
	require.False(t, store.Ready())
	require.NoError(t, store.Close())
}

func TestStoreMaintenanceLifecycle(t *testing.T) {
	t.Parallel()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	started := make(chan struct{})
	store, err := New(Config{
		WriteDB: db,
		Dialect: SQLiteDialect(),
		Maintenance: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
		MaintenanceInterval: time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("maintenance did not start")
	}
	require.NoError(t, store.Close())
}

func TestStoreStartsForPostgresDialect(t *testing.T) {
	t.Parallel()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	store, err := New(Config{WriteDB: db, Dialect: PostgresDialect()})
	require.NoError(t, err)
	err = store.Start(context.Background())
	require.NoError(t, err)
	require.True(t, store.Ready())
	require.NoError(t, store.Close())
}
