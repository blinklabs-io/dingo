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
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func newMigratedSQLiteStoreLazy(tb testing.TB, lazy bool) *Store {
	tb.Helper()
	db, err := OpenDB(
		"sqlite",
		fmt.Sprintf(
			"file:lazy_prepare_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
		"sqlite",
		false,
	)
	require.NoError(tb, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	seedMigratedSQLite(tb, db)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(tb, err)
	store, err := New(Config{
		WriteDB:           db,
		Dialect:           SQLiteDialect(),
		Migrations:        registry,
		MigrationLocker:   migrations.NewProcessLocker(),
		LazyHotStatements: lazy,
	})
	require.NoError(tb, err)
	require.NoError(tb, store.Start(context.Background()))
	tb.Cleanup(func() { require.NoError(tb, store.Close()) })
	return store
}

func cachedStmtCount(s *Store) int {
	s.stmtMu.Lock()
	defer s.stmtMu.Unlock()
	return len(s.stmts)
}

// TestEagerStartPreparesEveryCacheableHotStatement pins the default: Start
// prepares each cacheable hot statement against the migrated schema, so a
// statement that no longer compiles is missing from the cache here.
func TestEagerStartPreparesEveryCacheableHotStatement(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStoreLazy(t, false)
	for _, query := range hotStatements {
		if !cacheableForDialect("sqlite", query) {
			continue
		}
		_, ok := store.lookupCachedStmt(query)
		require.True(t, ok, "hot statement not prepared by Start: %s", query)
	}
}

// TestLazyHotStatementsPrepareOnFirstUse proves lazy Start prepares nothing,
// the first use on the write pool prepares and caches exactly that statement,
// and every cacheable hot statement can be prepared this way.
func TestLazyHotStatementsPrepareOnFirstUse(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStoreLazy(t, true)
	require.Zero(t, cachedStmtCount(store))
	ctx := context.Background()
	db := newDialectQueryer(store.writeDB, "sqlite")

	_, release, ok := store.borrowStmt(ctx, db, sumCredentialUtxoStakeQuery)
	require.True(t, ok)
	release()
	require.Equal(t, 1, cachedStmtCount(store))

	for _, query := range hotStatements {
		if !cacheableForDialect("sqlite", query) {
			continue
		}
		_, release, ok := store.borrowStmt(ctx, db, query)
		require.True(t, ok, "lazy prepare failed: %s", query)
		release()
	}
	// Same statement, not a fresh one, on later use.
	first, _ := store.lookupCachedStmt(sumCredentialUtxoStakeQuery)
	_, release, ok = store.borrowStmt(ctx, db, sumCredentialUtxoStakeQuery)
	require.True(t, ok)
	release()
	second, _ := store.lookupCachedStmt(sumCredentialUtxoStakeQuery)
	require.Same(t, first, second)
}

func TestLazyHotStatementsSkipUnknownQueries(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStoreLazy(t, true)
	ctx := context.Background()
	_, _, ok := store.borrowStmt(
		ctx, newDialectQueryer(store.writeDB, "sqlite"), "SELECT 1",
	)
	require.False(t, ok)
	require.Zero(t, cachedStmtCount(store))
}

func TestLazyHotStatementsReuseTransactionWrites(t *testing.T) {
	t.Parallel()
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprintf("commit=%t", commit), func(t *testing.T) {
			t.Parallel()
			store := newMigratedSQLiteStoreLazy(t, true)
			txn := store.Transaction(context.Background()).(*sqlTxn)
			t.Cleanup(func() { require.NoError(t, txn.Rollback()) })
			hash, nonce := []byte{1}, []byte{2}
			require.NoError(t, store.SetBlockNonce(hash, 1, nonce, false, txn))
			store.txStmtMu.Lock()
			first := store.lazyTxStmts[txn.tx][sqlitequery.SetBlockNonceQuery]
			store.txStmtMu.Unlock()
			require.NotNil(t, first, "transaction-only write was not cached")
			for range 10 {
				require.NoError(
					t,
					store.SetBlockNonce(hash, 1, nonce, true, txn),
				)
			}
			store.txStmtMu.Lock()
			second := store.lazyTxStmts[txn.tx][sqlitequery.SetBlockNonceQuery]
			derived := len(store.txStmts[txn.tx])
			store.txStmtMu.Unlock()
			require.Same(t, first, second)
			require.Equal(t, 1, derived, "writes retained redundant statements")
			require.Zero(t, cachedStmtCount(store))
			if commit {
				require.NoError(t, txn.Commit())
			} else {
				require.NoError(t, txn.Rollback())
			}
			store.txStmtMu.Lock()
			retained := len(store.lazyTxStmts) + len(store.txStmts)
			store.txStmtMu.Unlock()
			require.Zero(
				t,
				retained,
				"finished transaction retained statements",
			)
			_, err := first.ExecContext(
				context.Background(),
				hash,
				1,
				nonce,
				false,
			)
			require.Error(t, err)
			stored, err := store.GetBlockNonce(ocommon.NewPoint(1, hash), nil)
			require.NoError(t, err)
			if commit {
				require.Equal(t, nonce, stored)
			} else {
				require.Empty(t, stored)
			}
		})
	}
}

func TestLazyTransactionStatementsAfterCancellation(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStoreLazy(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	txn := store.Transaction(ctx).(*sqlTxn)
	hash, nonce := []byte{1}, []byte{2}
	require.NoError(t, store.SetBlockNonce(hash, 1, nonce, false, txn))
	cancel()
	require.ErrorIs(
		t, store.SetBlockNonce(hash, 1, nonce, false, txn), context.Canceled,
	)
	err := txn.Rollback()
	require.True(t, err == nil || errors.Is(err, sql.ErrTxDone))
	store.txStmtMu.Lock()
	retained := len(store.lazyTxStmts) + len(store.txStmts)
	store.txStmtMu.Unlock()
	require.Zero(t, retained)
	stored, err := store.GetBlockNonce(ocommon.NewPoint(1, hash), nil)
	require.NoError(t, err)
	require.Empty(t, stored)
}

// TestLazyHotStatementsConcurrentFirstUse races first use of one statement and
// requires a single cached statement afterwards.
func TestLazyHotStatementsConcurrentFirstUse(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStoreLazy(t, true)
	ctx := context.Background()
	db := newDialectQueryer(store.writeDB, "sqlite")
	const workers = 16
	type borrowResult struct {
		stmt *sql.Stmt
		ok   bool
	}
	results := make([]borrowResult, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			stmt, release, ok := store.borrowStmt(
				ctx, db, sumCredentialUtxoStakeQuery,
			)
			results[i] = borrowResult{stmt: stmt, ok: ok}
			if ok {
				release()
			}
		})
	}
	wg.Wait()
	for _, r := range results {
		if r.ok {
			require.NotNil(t, r.stmt)
		}
	}
	require.Equal(t, 1, cachedStmtCount(store))
}

// TestLazyHotStatementsFailedPrepareFallsBackAndIsRemembered proves a lazy
// prepare that fails (no schema) is not an error to the caller, which takes the
// uncached path, and is not retried on every call.
func TestLazyHotStatementsFailedPrepareFallsBackAndIsRemembered(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	db, err := OpenDB(
		"sqlite",
		fmt.Sprintf(
			"file:lazy_prepare_bare_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
		"sqlite",
		false,
	)
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:           db,
		Dialect:           SQLiteDialect(),
		LazyHotStatements: true,
		Logger: slog.New(slog.NewTextHandler(
			&logs,
			nil,
		)),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	q := newDialectQueryer(store.writeDB, "sqlite")

	_, _, ok := store.borrowStmt(ctx, q, sumCredentialUtxoStakeQuery)
	require.False(t, ok)
	require.Contains(t, store.stmtFailed, sumCredentialUtxoStakeQuery)
	require.NotContains(
		t,
		logs.String(),
		"sqlstore: skipping prepared-statement cache entry",
	)

	// The uncached fallback reports the real SQL error to the caller.
	_, err = store.sumCredentialUtxoStake(
		ctx,
		store.writeDB,
		models.NewStakeCredentialRef(0, credentialKeyForIndex(0)),
	)
	require.Error(t, err)

	// Once remembered, a schema that now exists is not consulted.
	_, err = store.writeDB.Exec("CREATE TABLE utxo (id INTEGER)")
	require.NoError(t, err)
	_, _, ok = store.borrowStmt(ctx, q, sumCredentialUtxoStakeQuery)
	require.False(t, ok)
}

// TestLazyHotStatementsCloseAndResetStopPreparation proves Close retires every
// lazily prepared statement and nothing is prepared after Reset-style
// invalidation, matching the eager cache.
func TestLazyHotStatementsCloseAndResetStopPreparation(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStoreLazy(t, true)
	ctx := context.Background()
	q := newDialectQueryer(store.writeDB, "sqlite")
	stmt, release, ok := store.borrowStmt(ctx, q, sumCredentialUtxoStakeQuery)
	require.True(t, ok)
	release()

	store.closePreparedStatements()
	require.Zero(t, cachedStmtCount(store))
	_, err := stmt.QueryContext(ctx, 0, 0, 0)
	require.ErrorContains(t, err, "closed")
	_, _, ok = store.borrowStmt(ctx, q, sumCredentialUtxoStakeQuery)
	require.False(t, ok)
	require.Zero(t, cachedStmtCount(store))
}
