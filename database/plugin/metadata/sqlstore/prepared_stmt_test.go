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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestPrepareHotStatementsPopulatesCacheOnStart proves Start eagerly
// prepares every entry in hotStatements (against the real migrated schema,
// so sumCredentialUtxoStakeQuery's utxo table exists) and that repeated
// lookups return the identical *sql.Stmt rather than a fresh one.
func TestPrepareHotStatementsPopulatesCacheOnStart(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	first, ok := store.lookupCachedStmt(sumCredentialUtxoStakeQuery)
	require.True(t, ok, "expected Start to have cached the hot statement")
	require.NotNil(t, first)

	second, ok := store.lookupCachedStmt(sumCredentialUtxoStakeQuery)
	require.True(t, ok)
	require.Same(
		t,
		first,
		second,
		"expected repeated lookups to return the same *sql.Stmt",
	)

	store.stmtMu.Lock()
	entries := len(store.stmts)
	store.stmtMu.Unlock()
	require.Equal(t, len(hotStatements), entries)
}

// TestPrepareHotStatementsIsBestEffortWithoutMigrations proves a Store
// constructed without its full migration registry (as many unrelated
// sqlstore unit tests do, via newTestStore, to exercise transaction/
// savepoint/close mechanics against a bare connection with no utxo table)
// still starts successfully: prepareHotStatements logs and skips a failing
// entry instead of failing Start.
func TestPrepareHotStatementsIsBestEffortWithoutMigrations(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)

	_, ok := store.lookupCachedStmt(sumCredentialUtxoStakeQuery)
	require.False(
		t,
		ok,
		"expected no cache entry when the underlying table does not exist",
	)

	// sumCredentialUtxoStake must still work correctly (uncached) against
	// this schema-less store's absence of the entry -- i.e. it must not
	// panic or otherwise assume the cache is always populated. There is no
	// utxo table here, so assert only that the miss path is taken (a
	// query error, rather than a cache-related failure).
	ctx := context.Background()
	_, err := store.sumCredentialUtxoStake(
		ctx,
		store.writeDB,
		models.NewStakeCredentialRef(0, credentialKeyForIndex(0)),
	)
	require.Error(t, err, "expected a query error against a missing table")
}

// TestCachedStmtInvalidatedByClose proves CloseContext both clears the
// cache and stops it from being repopulated, so nothing outlives the pool
// it was prepared against.
func TestCachedStmtInvalidatedByClose(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	_, ok := store.lookupCachedStmt(sumCredentialUtxoStakeQuery)
	require.True(t, ok)

	require.NoError(t, store.CloseContext(context.Background()))

	store.stmtMu.Lock()
	entries := len(store.stmts)
	store.stmtMu.Unlock()
	require.Equal(
		t,
		0,
		entries,
		"expected CloseContext to clear the prepared-statement cache",
	)

	_, ok = store.lookupCachedStmt(sumCredentialUtxoStakeQuery)
	require.False(t, ok, "expected a closed store to serve no cache entries")
}

// TestResetInvalidatesPreparedStatementCache proves Store.Reset -- wired for
// postgres/mysql, whose Reset callback drops and recreates schema objects
// (resetDatabase's DROP TABLE ... CASCADE) -- invalidates the cache around
// calling it, and that it stays empty afterward rather than being lazily
// repopulated (see prepareHotStatements for why lazy repopulation would risk
// a deadlock). A fake Reset callback stands in for the real
// backend-specific one; what matters here is only that Store.Reset itself
// invalidates the cache, not what the callback does.
func TestResetInvalidatesPreparedStatementCache(t *testing.T) {
	t.Parallel()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:cachedstmt_reset_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	var resetCalls int
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
		Reset: func(context.Context) error {
			resetCalls++
			return nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	before, ok := store.lookupCachedStmt(sumCredentialUtxoStakeQuery)
	require.True(t, ok)
	require.NotNil(t, before)

	require.NoError(t, store.Reset(context.Background()))
	require.Equal(t, 1, resetCalls)

	store.stmtMu.Lock()
	entries := len(store.stmts)
	store.stmtMu.Unlock()
	require.Equal(
		t,
		0,
		entries,
		"expected Reset to invalidate the prepared-statement cache",
	)

	_, ok = store.lookupCachedStmt(sumCredentialUtxoStakeQuery)
	require.False(
		t,
		ok,
		"expected Reset to leave the cache empty rather than lazily repopulate it",
	)
}

// TestStmtForQueryerUnwrapsDialectQueryer proves stmtForQueryer's recursive
// unwrap case actually reaches the *sql.Tx branch instead of falling through
// to the "use cached directly" default for a non-sqlite dialect, where db is
// always a dialectQueryer wrapping the real handle (see newDialectQueryer).
func TestStmtForQueryerUnwrapsDialectQueryer(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()

	cached, ok := store.lookupCachedStmt(sumCredentialUtxoStakeQuery)
	require.True(t, ok)

	tx, err := store.writeDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	wrapped := dialectQueryer{queryer: tx, dialect: "postgres"}
	got := stmtForQueryer(ctx, wrapped, cached)
	require.NotNil(t, got)

	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
	var total sql.NullInt64
	require.NoError(
		t,
		got.QueryRowContext(ctx, ref.Tag, ref.Key).Scan(&total),
	)
	require.False(t, total.Valid, "expected no matching rows for an unseeded credential")
}
