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
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

func newSplitReadPoolTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	writeDB, err := sql.Open(
		"sqlite",
		"file:"+path+"?_pragma=synchronous(OFF)&_pragma=journal_mode(WAL)",
	)
	require.NoError(t, err)
	writeDB.SetMaxOpenConns(1)
	readDB, err := sql.Open(
		"sqlite",
		"file:"+path+"?mode=ro&_pragma=synchronous(OFF)",
	)
	require.NoError(t, err)
	seedMigratedSQLite(t, writeDB)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(
		Config{
			WriteDB:         writeDB,
			ReadDB:          readDB,
			Dialect:         SQLiteDialect(),
			Migrations:      registry,
			MigrationLocker: migrations.NewProcessLocker(),
		},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func TestSyncStatePreparedReadKeepsSeparateReadPool(t *testing.T) {
	t.Parallel()
	store := newSplitReadPoolTestStore(t)
	require.NoError(t, store.SetSyncState("boundary", "before", nil))
	// The fixture owns rollback; the test context cancels before cleanups.
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { require.NoError(t, txn.Rollback()) })
	require.NoError(t, store.SetSyncState("boundary", "after", txn))
	value, err := store.GetSyncState("boundary", txn)
	require.NoError(t, err)
	require.Equal(t, "after", value)
	type result struct {
		value string
		err   error
	}
	got := make(chan result, 1)
	go func() { value, err := store.GetSyncState("boundary", nil); got <- result{value, err} }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case read := <-got:
		require.NoError(t, read.err)
		require.Equal(t, "before", read.value)
	case <-ctx.Done():
		require.NoError(t, txn.Rollback())
		t.Fatal(
			"sync-state read waited on the write connection held by its caller",
		)
	}
	readTxn := store.ReadTransaction(context.Background())
	t.Cleanup(func() { require.NoError(t, readTxn.Rollback()) })
	value, err = store.GetSyncState("boundary", readTxn)
	require.NoError(t, err)
	require.Equal(t, "before", value)
}
