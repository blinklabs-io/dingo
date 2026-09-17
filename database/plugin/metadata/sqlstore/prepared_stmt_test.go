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
	"reflect"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// retainedTxStmtCount reads the length of tx's own unexported stmts.v slice
// -- the exact list database/sql appends every (*sql.Tx).StmtContext result
// to and only closes (never shrinks) at commit or rollback, per
// database/sql/sql.go's Tx.stmts field and Tx.closePrepared. This is the
// same quantity a caller consulting the same cached statement many times
// within one transaction retains until that transaction ends, so counting
// it directly -- rather than via any wrapper this package's own cache adds
// -- is what actually proves retention is bounded instead of proportional to
// how many times a cached query was consulted.
func retainedTxStmtCount(tb testing.TB, tx *sql.Tx) int {
	tb.Helper()
	stmts := reflect.ValueOf(tx).Elem().FieldByName("stmts").FieldByName("v")
	require.True(tb, stmts.IsValid(), "database/sql.Tx.stmts.v not found by reflection")
	return stmts.Len()
}

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
	got := store.stmtForQueryer(ctx, wrapped, cached)
	require.NotNil(t, got)

	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
	var total sql.NullInt64
	require.NoError(
		t,
		got.QueryRowContext(ctx, ref.Tag, ref.Key).Scan(&total),
	)
	require.False(t, total.Valid, "expected no matching rows for an unseeded credential")
}

// TestTxScopedStmtReusedWithinOneTransaction proves txScopedStmt derives a
// Tx-scoped *sql.Stmt at most once per (tx, cached) pair: three calls
// against the same tx and the same cached statement return the identical
// *sql.Stmt, so only one entry is ever appended to database/sql's own
// tx.stmts list for it, rather than one per call.
func TestTxScopedStmtReusedWithinOneTransaction(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()

	cached, ok := store.lookupCachedStmt(sumCredentialUtxoStakeQuery)
	require.True(t, ok)

	tx, err := store.writeDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	first := store.txScopedStmt(ctx, tx, cached)
	second := store.txScopedStmt(ctx, tx, cached)
	third := store.txScopedStmt(ctx, tx, cached)
	require.Same(t, first, second)
	require.Same(t, first, third)
	require.Equal(
		t,
		1,
		retainedTxStmtCount(t, tx),
		"expected exactly one Tx-scoped statement retained for three calls "+
			"against the same (tx, cached) pair",
	)
}

// TestTxScopedStmtDistinctAcrossTransactions proves the per-transaction
// cache does not leak a derived statement from one transaction into another:
// two independent transactions each derive their own Tx-scoped *sql.Stmt
// from the same Store-lifetime cached statement.
func TestTxScopedStmtDistinctAcrossTransactions(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()

	cached, ok := store.lookupCachedStmt(sumCredentialUtxoStakeQuery)
	require.True(t, ok)

	txA, err := store.writeDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = txA.Rollback() })
	txB, err := store.writeDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = txB.Rollback() })

	derivedA := store.txScopedStmt(ctx, txA, cached)
	derivedB := store.txScopedStmt(ctx, txB, cached)
	require.NotSame(t, derivedA, derivedB)
}

// TestEvictTxStmtsAfterCommitAndRollback proves the per-transaction cache
// entry is removed once its transaction ends, through the real
// Commit/Rollback path (sqlTxn.releaseConnection), not just through a direct
// evictTxStmts call -- so a later transaction can never see a stale entry.
func TestEvictTxStmtsAfterCommitAndRollback(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()

	for _, commit := range []bool{true, false} {
		txn := store.Transaction(ctx)
		sqlTransaction, ok := txn.(*sqlTxn)
		require.True(t, ok)
		require.NoError(t, sqlTransaction.beginErr)

		require.NoError(t, store.withWriteTransaction(
			txn,
			func(db queryer, ctx context.Context) error {
				return store.refreshRewardLiveStakeAggregate(
					ctx, db, models.NewStakeCredentialRef(0, credentialKeyForIndex(0)), 1,
				)
			},
		))

		store.txStmtMu.Lock()
		_, hasEntry := store.txStmts[sqlTransaction.tx]
		store.txStmtMu.Unlock()
		require.True(t, hasEntry, "expected a live cache entry before the transaction ends")

		tx := sqlTransaction.tx
		if commit {
			require.NoError(t, txn.Commit())
		} else {
			require.NoError(t, txn.Rollback())
		}

		store.txStmtMu.Lock()
		_, stillHasEntry := store.txStmts[tx]
		store.txStmtMu.Unlock()
		require.False(t, stillHasEntry, "expected the cache entry to be evicted once the transaction ended")
	}
}

// TestRefreshRewardLiveStakeRefsBoundsTxScopedStatementRetention is the
// regression test for the retention bug: refreshRewardLiveStakeRefs loops
// refreshRewardLiveStakeAggregate over many stake refs inside one write
// transaction, the same shape UpdateUtxos/DeleteUtxos/the genesis import
// paths use. Before the fix, each of the two cached queries this reaches
// (rewardLiveStakeAccountQuery via queryRowCached, sumCredentialUtxoStakeQuery
// via sumCredentialUtxoStake) derived a brand new Tx-scoped *sql.Stmt per
// ref, so retention was 2 * refCount; measured in-tree with the fix reverted,
// 2000 refs retained 4000 Tx-scoped statements. This asserts retention stays
// bounded (at most one derived statement per distinct cached query this path
// consults) regardless of how many refs are processed in the transaction.
func TestRefreshRewardLiveStakeRefsBoundsTxScopedStatementRetention(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()

	const refCount = 2000
	refs := make([]models.StakeCredentialRef, refCount)
	for i := range refs {
		refs[i] = models.NewStakeCredentialRef(0, credentialKeyForIndex(i))
	}

	txn := store.Transaction(ctx)
	sqlTransaction, ok := txn.(*sqlTxn)
	require.True(t, ok)
	require.NoError(t, sqlTransaction.beginErr)

	require.NoError(t, store.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			return store.refreshRewardLiveStakeRefs(ctx, db, refs, 100)
		},
	))

	retained := retainedTxStmtCount(t, sqlTransaction.tx)
	require.NoError(t, txn.Commit())

	// None of refCount's unseeded credentials has an account row or live
	// UTxOs, so every ref takes the early DELETE-and-return path and never
	// reaches rewardLiveStakeUpsertQuery -- only rewardLiveStakeAccountQuery
	// and sumCredentialUtxoStakeQuery are consulted here, so 2 is the exact
	// bound, not just an upper one.
	require.LessOrEqual(
		t,
		retained,
		2,
		"expected bounded Tx-scoped statement retention for %d refs, got %d",
		refCount,
		retained,
	)
}

// TestTxScopedStmtDoesNotLeakAcrossConcurrentEviction reproduces,
// deterministically via txScopedStmtAfterDerive, the eviction race
// txScopedStmt's own doc comment documents: tx.StmtContext runs with
// s.txStmtMu released, so a concurrent Commit/Rollback on the same tx can run
// evictTxStmts in the window between derivation and this call's own insert,
// which then recreates the just-evicted entry.
//
// This only happens when a single tx is driven by more than one goroutine at
// once -- exactly the misuse txScopedStmt's doc comment explains dingo's real
// call sites never commit. This test deliberately performs that misuse (calls
// txScopedStmt and evictTxStmts concurrently against the same tx, forcing the
// interleaving with the hook rather than relying on scheduler timing) to
// confirm the mechanism is real, not just asserted -- it does not exercise any
// path dingo's production code reaches.
func TestTxScopedStmtDoesNotLeakAcrossConcurrentEviction(t *testing.T) {
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()

	cached, ok := store.lookupCachedStmt(sumCredentialUtxoStakeQuery)
	require.True(t, ok)

	tx, err := store.writeDB.BeginTx(ctx, nil)
	require.NoError(t, err)

	derivedStarted := make(chan struct{})
	evictionDone := make(chan struct{})
	txScopedStmtAfterDerive = func() {
		close(derivedStarted)
		<-evictionDone
	}
	t.Cleanup(func() { txScopedStmtAfterDerive = nil })

	deriveDone := make(chan *sql.Stmt, 1)
	go func() {
		deriveDone <- store.txScopedStmt(ctx, tx, cached)
	}()

	<-derivedStarted
	// Simulate sqlTxn.releaseConnection racing in during the gap between
	// derivation and insertion: commit tx for real, then evict its cache
	// entry, exactly what releaseConnection does on the real Commit path.
	require.NoError(t, tx.Commit())
	store.evictTxStmts(tx)
	close(evictionDone)

	derived := <-deriveDone
	require.NotNil(t, derived)

	store.txStmtMu.Lock()
	_, leaked := store.txStmts[tx]
	store.txStmtMu.Unlock()
	require.True(
		t,
		leaked,
		"expected the deliberately forced race to recreate s.txStmts[tx] "+
			"after eviction -- if this now fails, txScopedStmt's eviction "+
			"race was fixed and this test (and its doc comment reference) "+
			"should be updated to match",
	)
}
