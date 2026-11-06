//go:build dingo_db_integration

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
	"testing"

	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/stretchr/testify/require"
)

func TestPostgresSyncStatePreparedRead(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	testSyncStatePreparedRead(
		t,
		newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema),
	)
}

func TestMySQLSyncStatePreparedRead(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	testSyncStatePreparedRead(
		t,
		newIntegrationSQLStore(t, "mysql", dsn, "mysql", database),
	)
}

func testSyncStatePreparedRead(t *testing.T, store *Store) {
	t.Helper()
	_, ok := store.lookupCachedStmt(sqlitequery.GetSyncStateQuery)
	require.True(t, ok)
	require.NoError(t, store.SetSyncState("boundary", "before", nil))
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { require.NoError(t, txn.Rollback()) })
	require.NoError(t, store.SetSyncState("boundary", "after", txn))
	value, err := store.GetSyncState("boundary", txn)
	require.NoError(t, err)
	require.Equal(t, "after", value)
	require.NoError(t, store.DeleteSyncState("boundary", txn))
	value, err = store.GetSyncState("boundary", txn)
	require.NoError(t, err)
	require.Empty(t, value)
	require.NoError(t, txn.Rollback())
	value, err = store.GetSyncState("boundary", nil)
	require.NoError(t, err)
	require.Equal(t, "before", value)
}
