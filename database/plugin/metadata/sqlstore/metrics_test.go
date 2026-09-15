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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestClassifySQLOp(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "sqlc insert with name comment",
			query: "-- name: InsertNodeSettings :execrows\nINSERT INTO node_settings (id) VALUES (?)",
			want:  "insert",
		},
		{
			name:  "sqlc select with name comment",
			query: "-- name: GetTip :one\nSELECT * FROM sync_state WHERE sync_key = ?",
			want:  "select",
		},
		{
			name:  "sqlc update, lowercase keyword",
			query: "-- name: TouchThing :exec\nupdate utxo set deleted_slot = ? where id = ?",
			want:  "update",
		},
		{
			name:  "sqlc delete",
			query: "-- name: PruneThing :execrows\nDELETE FROM auth_committee_hot WHERE id IN (?)",
			want:  "delete",
		},
		{
			name:  "no leading comment",
			query: "SELECT 1",
			want:  "select",
		},
		{
			name:  "multiple leading comment lines",
			query: "-- name: Foo :one\n-- a second comment line\nSELECT 1",
			want:  "select",
		},
		{
			name:  "leading whitespace before comment",
			query: "  \n-- name: Foo :one\nSELECT 1",
			want:  "select",
		},
		{
			name:  "CTE reported as other, not guessed at",
			query: "-- name: Foo :many\nWITH x AS (SELECT 1) SELECT * FROM x",
			want:  "other",
		},
		{
			name:  "PRAGMA reported as other",
			query: "PRAGMA journal_mode=WAL",
			want:  "other",
		},
		{
			name:  "comment with no trailing newline has nothing left to classify",
			query: "-- name: Foo :one",
			want:  "other",
		},
		{
			name:  "empty query",
			query: "",
			want:  "other",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, classifySQLOp(tc.query))
		})
	}
}

// newMigratedSQLiteStoreWithRegistry is newMigratedSQLiteStore plus a real
// prometheus.Registry wired through Config.PromRegistry, so
// Store.instrumentedQueryer actually wraps every queryer it hands out in
// countingQueryer instead of taking the nil-registry no-op path.
func newMigratedSQLiteStoreWithRegistry(
	tb testing.TB,
	reg *prometheus.Registry,
) *Store {
	tb.Helper()
	db, err := OpenDB(
		"sqlite",
		fmt.Sprintf(
			"file:metrics_test_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
		"sqlite",
		false,
	)
	require.NoError(tb, err)
	// Mirror production's real constraint (database/plugin/metadata/sqlite/
	// shared_sqlstore.go sets this on writeDB) rather than leaving the pool
	// uncapped: stmtForQueryer's countingQueryer-unwrap case is only load-
	// bearing when there is exactly one write connection to contend for, so
	// an uncapped pool would let a broken unwrap silently open a second
	// connection instead of deadlocking, and this test would pass either way.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(tb, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
		PromRegistry:    reg,
	})
	require.NoError(tb, err)
	require.NoError(tb, store.Start(context.Background()))
	tb.Cleanup(func() { require.NoError(tb, store.Close()) })
	return store
}

func counterValue(t *testing.T, reg *prometheus.Registry, op string) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "dingo_database_sql_operations_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "op" && label.GetValue() == op {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// TestSQLOperationsCounterCountsCachedAndUncachedQueries is the regression
// test for two correctness fixes this instrumentation required:
// stmtForQueryer (prepared_stmt.go) and requireAccountBaselineTransaction
// (account.go) both type-switch on the queryer a caller passes in to find
// the underlying *sql.Tx, and both had to gain a countingQueryer-unwrap case
// alongside their existing dialectQueryer one. Without that fix, a write
// transaction's *sql.Tx is hidden behind countingQueryer whenever
// Config.PromRegistry is set (as it always is once this Store's provider
// wires it up in production), and stmtForQueryer falls through to its
// default branch: calling the hot-statement cache's *sql.Stmt directly
// against writeDB's pool instead of the open transaction. writeDB has
// SetMaxOpenConns(1) in production, so that call has no free connection to
// take and blocks forever -- the same deadlock prepareHotStatements'
// eager-at-Start design already exists to avoid, reintroduced by a
// different path. This test does not reproduce the pool exhaustion directly
// (an in-memory test store's pool is not capped), but it does prove the
// unwrap actually happens: sumCredentialUtxoStake's cached statement runs
// successfully from inside a real write transaction wrapped in
// countingQueryer, and every call -- cached or not -- is still counted
// exactly once.
func TestSQLOperationsCounterCountsCachedAndUncachedQueries(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := newMigratedSQLiteStoreWithRegistry(t, reg)
	ctx := context.Background()
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))

	// sumCredentialUtxoStake is one of hotStatements' cached queries. Calling
	// it inside withWriteTransaction's implicit-transaction path (as
	// production callers do) exercises the *sql.Tx wrapped in
	// countingQueryer that stmtForQueryer must unwrap.
	before := counterValue(t, reg, "select")
	err := store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			_, err := store.sumCredentialUtxoStake(ctx, db, ref)
			return err
		},
	)
	require.NoError(t, err)
	require.Equal(
		t,
		before+1,
		counterValue(t, reg, "select"),
		"expected the cached-statement call to be counted exactly once",
	)

	// An uncached SELECT through the same instrumented path.
	before = counterValue(t, reg, "select")
	var one int
	require.NoError(t, store.writeDB.QueryRowContext(ctx, "SELECT 1").Scan(&one))
	// The raw writeDB.QueryRowContext call above bypasses
	// instrumentedQueryer entirely (it is not routed through
	// dbFromTxn/withWriteTransaction), so it must NOT have changed the
	// counter -- this asserts the counter is wired to Store's chokepoint,
	// not to every possible caller of the raw *sql.DB.
	require.Equal(t, before, counterValue(t, reg, "select"))

	beforeInsert := counterValue(t, reg, "insert")
	require.NoError(t, store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			_, err := db.ExecContext(
				ctx,
				"INSERT INTO sync_state (sync_key, value) VALUES (?, ?)",
				"metrics_test_key", "1",
			)
			return err
		},
	))
	require.Equal(t, beforeInsert+1, counterValue(t, reg, "insert"))
}

// TestImportAccountWithMetricsEnabledStillWritesBaseline is the regression
// test for the second correctness fix instrumentation required:
// requireAccountBaselineTransaction (account.go) type-switches on the
// queryer ImportAccount's write transaction passes it, looking for the
// underlying *sql.Tx to confirm the baseline write is happening inside a
// real transaction. It had the same countingQueryer-unwrap gap
// stmtForQueryer did: with a PromRegistry configured, that *sql.Tx arrives
// wrapped in countingQueryer, and without the fix this call would fail with
// "account import baseline write outside a write transaction" even though
// it plainly is one. ImportAccount routes through
// withWriteTransaction/instrumentedQueryer exactly like every other
// production write path, so this is also a general confidence check that
// enabling the counter does not break a write-path call site elsewhere in
// the package.
func TestImportAccountWithMetricsEnabledStillWritesBaseline(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := newMigratedSQLiteStoreWithRegistry(t, reg)
	key := snapshotStakingKey(0x91)

	require.NoError(t, store.ImportAccount(&models.Account{
		StakingKey:    key,
		CredentialTag: 0,
		AddedSlot:     100,
		CreatedSlot:   0,
		Active:        true,
	}, nil))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(100), got.AddedSlot)
}

// TestSQLOperationsCounterNilWhenNoRegistry proves instrumentation is a
// true no-op with no PromRegistry configured: Store.sqlOperations is nil,
// and instrumentedQueryer must return the plain dialect-translated queryer
// rather than a countingQueryer wrapping a nil counter (which would panic
// on the first WithLabelValues call).
func TestSQLOperationsCounterNilWhenNoRegistry(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	require.Nil(t, store.sqlOperations)

	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
	err := store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			_, err := store.sumCredentialUtxoStake(ctx, db, ref)
			return err
		},
	)
	require.NoError(t, err)
}

// TestBatchInsertTransactionIsCounted is the regression test for
// transactionBatchAccumulator.insertTransaction (transaction_write.go):
// it executes its cached transactionInsert statement directly against a
// *sql.Stmt, bypassing countingQueryer entirely, so without its own
// sqlOperations counting the batch-insert path used by API backfill would be
// silently absent from dingo_database_sql_operations_total.
func TestBatchInsertTransactionIsCounted(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := newMigratedSQLiteStoreWithRegistry(t, reg)
	ctx := context.Background()

	txn := store.Transaction(ctx)
	db, dbCtx, err := store.dbFromTxn(txn)
	require.NoError(t, err)

	acc, ok := store.NewBatchAccumulator().(*transactionBatchAccumulator)
	require.True(t, ok)

	before := counterValue(t, reg, "insert")
	_, err = acc.insertTransaction(
		dbCtx,
		db,
		[]byte{0x20}, []byte{0x21}, nil, 1, 0, "0", "0", "0", 0, true,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		before+1,
		counterValue(t, reg, "insert"),
		"expected the batch accumulator's prepared insert to be counted",
	)

	require.NoError(t, txn.Rollback())
}

// TestMissingCriticalDeferredIndexesIsCounted is the regression test for
// dingo_database_sql_operations_total's Help text claim that it covers
// every domain query: MissingCriticalDeferredIndexes used to call
// newDialectQueryer directly instead of instrumentedQueryer, so its
// per-index existence checks bypassed the counter entirely. It runs one
// SELECT per deferred.CriticalManifest() entry regardless of whether the
// index is actually missing (a freshly migrated store has every deferred
// index already present -- the manifest only matters to bulk-load's
// drop/rebuild cycle), so the counter delta this asserts is exactly
// len(deferred.CriticalManifest()); without the fix it stays 0.
func TestMissingCriticalDeferredIndexesIsCounted(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := newMigratedSQLiteStoreWithRegistry(t, reg)

	before := counterValue(t, reg, "select")
	missing, err := store.MissingCriticalDeferredIndexes()
	require.NoError(t, err)
	require.Empty(
		t,
		missing,
		"expected a freshly migrated store to already have every deferred index",
	)
	require.Equal(
		t,
		before+float64(len(deferred.CriticalManifest())),
		counterValue(t, reg, "select"),
		"expected MissingCriticalDeferredIndexes' per-index checks to be counted",
	)
}
