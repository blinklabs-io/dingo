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
	"encoding/binary"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// utxoForInsertCacheTest builds a minimal, valid models.Utxo for exercising
// insertUtxoModel directly: distinct txSeed/outputIdx pairs target distinct
// rows, and the same pair can be reused deliberately to exercise the ON
// CONFLICT DO NOTHING branch.
func utxoForInsertCacheTest(
	txSeed byte,
	outputIdx uint32,
	amount uint64,
) *models.Utxo {
	txID := make([]byte, 32)
	txID[31] = txSeed
	paymentKey := bytes.Repeat([]byte{txSeed}, lcommon.AddressHashSize)
	return &models.Utxo{
		TxId:       txID,
		OutputIdx:  outputIdx,
		PaymentKey: paymentKey,
		AddedSlot:  1,
		Amount:     types.Uint64(amount),
	}
}

// insertUtxoInTxn runs insertUtxoModel inside its own write transaction, the
// same one-transaction-per-output access pattern ledgerProcessBlock uses in
// production (see insertUtxoQueryIgnoreConflict's doc comment).
func insertUtxoInTxn(
	t *testing.T,
	store *Store,
	utxo *models.Utxo,
	ignoreConflict bool,
) {
	t.Helper()
	err := store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			return store.insertUtxoModel(ctx, db, utxo, ignoreConflict)
		},
	)
	require.NoError(t, err)
}

// TestInsertUtxoModelReusesCachedStatementAcrossTransactions proves
// insertUtxoModel's move onto the hot-statement cache (queryRowCached) does
// not change its behavior: distinct inserts still get distinct ids, a
// repeated (tx_id, output_idx) under ignoreConflict still resolves to the
// existing row's id via the ON CONFLICT DO NOTHING + fallback SELECT branch,
// and the stored row round-trips correctly through GetUtxo -- while the
// cached *sql.Stmt for insertUtxoQueryIgnoreConflict is the same object
// across independent write transactions, the access pattern
// ledgerProcessBlock actually uses.
func TestInsertUtxoModelReusesCachedStatementAcrossTransactions(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	first := utxoForInsertCacheTest(1, 0, 5_000_000)
	insertUtxoInTxn(t, store, first, true)
	require.NotZero(t, first.ID)

	store.stmtMu.Lock()
	cachedBefore := store.stmts[insertUtxoQueryIgnoreConflict]
	store.stmtMu.Unlock()
	require.NotNil(
		t,
		cachedBefore,
		"expected insertUtxoQueryIgnoreConflict to be cached on SQLite",
	)

	second := utxoForInsertCacheTest(2, 0, 7)
	insertUtxoInTxn(t, store, second, true)
	require.NotZero(t, second.ID)
	require.NotEqual(t, first.ID, second.ID)

	store.stmtMu.Lock()
	cachedAfter := store.stmts[insertUtxoQueryIgnoreConflict]
	store.stmtMu.Unlock()
	require.Same(
		t,
		cachedBefore,
		cachedAfter,
		"expected the same cached *sql.Stmt across independent write transactions",
	)

	// Re-inserting the same (tx_id, output_idx) under ignoreConflict must
	// take the ON CONFLICT DO NOTHING branch and resolve to the existing
	// row's id via the fallback SELECT, not error and not create a second
	// row -- exactly like before this query went through the cache.
	dup := utxoForInsertCacheTest(1, 0, 999)
	insertUtxoInTxn(t, store, dup, true)
	require.Equal(
		t,
		first.ID,
		dup.ID,
		"expected ON CONFLICT DO NOTHING to resolve to the existing row's id",
	)

	// Round-trip through the public read path: the row the cached statement
	// wrote back is a correct, complete row, not just "an insert succeeded".
	got, err := store.GetUtxo(first.TxId, first.OutputIdx, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, types.Uint64(5_000_000), got.Amount)

	got2, err := store.GetUtxo(second.TxId, second.OutputIdx, nil)
	require.NoError(t, err)
	require.NotNil(t, got2)
	require.Equal(t, types.Uint64(7), got2.Amount)
}

// TestInsertUtxoModelCachesAssetIDLookup proves getAssetIDQuery -- the other
// query insertUtxoModel now routes through the hot-statement cache -- is
// populated, reused across independent write transactions, and still
// resolves each asset's id correctly.
func TestInsertUtxoModelCachesAssetIDLookup(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	utxo := utxoForInsertCacheTest(10, 0, 2_000_000)
	utxo.Assets = []models.Asset{
		{
			Name:        []byte("token"),
			NameHex:     []byte("746f6b656e"),
			PolicyId:    bytes.Repeat([]byte{0xAA}, 28),
			Fingerprint: []byte("asset1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			Amount:      types.Uint64(42),
		},
	}
	insertUtxoInTxn(t, store, utxo, true)
	require.NotZero(t, utxo.Assets[0].ID)

	store.stmtMu.Lock()
	cachedBefore := store.stmts[getAssetIDQuery]
	store.stmtMu.Unlock()
	require.NotNil(
		t,
		cachedBefore,
		"expected getAssetIDQuery to be cached on SQLite",
	)

	utxo2 := utxoForInsertCacheTest(11, 0, 3_000_000)
	utxo2.Assets = []models.Asset{
		{
			Name:        []byte("token2"),
			NameHex:     []byte("746f6b656e32"),
			PolicyId:    bytes.Repeat([]byte{0xBB}, 28),
			Fingerprint: []byte("asset1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
			Amount:      types.Uint64(7),
		},
	}
	insertUtxoInTxn(t, store, utxo2, true)
	require.NotZero(t, utxo2.Assets[0].ID)
	require.NotEqual(t, utxo.Assets[0].ID, utxo2.Assets[0].ID)

	store.stmtMu.Lock()
	cachedAfter := store.stmts[getAssetIDQuery]
	store.stmtMu.Unlock()
	require.Same(
		t,
		cachedBefore,
		cachedAfter,
		"expected the same cached *sql.Stmt across independent write transactions",
	)
}

// TestCacheableForDialect is a table-driven unit test of the pure predicate
// prepareHotStatements now consults before caching a hot statement. The only
// case that must come back false is a RETURNING-id query on MySQL (see
// insertUtxoQuery's doc comment); every other dialect/query combination,
// including a RETURNING-id query on PostgreSQL (which supports RETURNING
// natively) and a non-RETURNING query on MySQL, must come back true.
func TestCacheableForDialect(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		dialect string
		query   string
		want    bool
	}{
		{
			"sqlite insert with returning",
			"sqlite", insertUtxoQueryIgnoreConflict, true,
		},
		{
			"sqlite insert without conflict clause",
			"sqlite", insertUtxoQuery, true,
		},
		{
			"postgres insert with returning",
			"postgres", insertUtxoQueryIgnoreConflict, true,
		},
		{
			"mysql insert with returning (ignore conflict)",
			"mysql", insertUtxoQueryIgnoreConflict, false,
		},
		{
			"mysql insert with returning (no conflict clause)",
			"mysql", insertUtxoQuery, false,
		},
		{
			"mysql plain select, no returning",
			"mysql", getAssetIDQuery, true,
		},
		{
			"mysql reward account select, no returning",
			"mysql", rewardLiveStakeAccountQuery, true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(
				t,
				tc.want,
				cacheableForDialect(tc.dialect, tc.query),
			)
		})
	}
}

// TestPrepareHotStatementsSkipsReturningIDQueryOnMySQL is the end-to-end
// counterpart to TestCacheableForDialect: it proves prepareHotStatements
// itself actually leaves insertUtxoQuery/insertUtxoQueryIgnoreConflict
// uncached against a MySQL-dialect Store, while a non-RETURNING hot
// statement (getAssetIDQuery) still gets cached -- i.e. the skip is scoped
// to the unsafe dialect+query-shape combination, not a blanket "MySQL never
// caches anything".
//
// This borrows the real migrated schema a SQLite store already built
// (newMigratedSQLiteStore) rather than running Start's migration runner
// under a MySQL-labeled dialect: migrations.SQLiteRegistry() is SQLite DDL,
// which the runner would try to execute as-is regardless of s.dialect, so
// mixing a real MySQL Start with a SQLite migration registry would fail for
// a reason unrelated to the thing this test checks. prepareHotStatements
// itself only needs an existing schema and a writeDB to call PrepareContext
// against, so a second, minimal Store value sharing the same *sql.DB (with
// dialect swapped to MySQL) exercises exactly the code path under test.
func TestPrepareHotStatementsSkipsReturningIDQueryOnMySQL(t *testing.T) {
	t.Parallel()
	sqliteStore := newMigratedSQLiteStore(t)

	mysqlStore := &Store{
		writeDB: sqliteStore.writeDB,
		dialect: MySQLDialect(),
		logger:  slog.Default(),
	}
	t.Cleanup(mysqlStore.closePreparedStatements)
	mysqlStore.prepareHotStatements(context.Background())

	_, ok := mysqlStore.lookupCachedStmt(insertUtxoQuery)
	require.False(t, ok, "expected insertUtxoQuery to be uncached on MySQL")

	_, ok = mysqlStore.lookupCachedStmt(insertUtxoQueryIgnoreConflict)
	require.False(
		t,
		ok,
		"expected insertUtxoQueryIgnoreConflict to be uncached on MySQL",
	)

	_, ok = mysqlStore.lookupCachedStmt(getAssetIDQuery)
	require.True(
		t,
		ok,
		"expected a non-RETURNING hot statement to still be cached on MySQL",
	)
}

// BenchmarkInsertUtxoModel is the before/after timing counterpart:
// legacyInsertUtxo reproduces insertUtxoModel's pre-cache one-shot
// QueryRowContext call (the exact query text and argument order that used
// to be inlined directly in insertUtxoModel), run against the same migrated
// schema and connection insertUtxoModel itself uses via the hot-statement
// cache.
func legacyInsertUtxo(
	ctx context.Context,
	db queryer,
	utxo *models.Utxo,
) (int64, error) {
	params, err := createUtxoParams(utxo)
	if err != nil {
		return 0, err
	}
	var id int64
	err = db.QueryRowContext(ctx, insertUtxoQueryIgnoreConflict,
		params.TransactionID,
		params.CollateralReturnForTxID,
		params.TxID,
		params.PaymentKey,
		params.StakingKey,
		params.CredentialTag,
		params.DatumHash,
		nullBytes(params.SpentAtTxID),
		nullBytes(params.ReferencedByTxID),
		nullBytes(params.CollateralByTxID),
		params.AddedSlot,
		params.DeletedSlot,
		params.Amount,
		params.OutputIdx,
		params.PaymentScript,
	).Scan(&id)
	return id, err
}

// utxoForBenchmarkIteration builds a UTxO with a unique tx_id per i, so each
// benchmark iteration inserts a genuinely new row (the realistic sync
// workload) instead of repeatedly hitting the ON CONFLICT DO NOTHING branch.
func utxoForBenchmarkIteration(i uint64) *models.Utxo {
	txID := make([]byte, 32)
	binary.BigEndian.PutUint64(txID[24:], i)
	return &models.Utxo{
		TxId:       txID,
		OutputIdx:  0,
		PaymentKey: bytes.Repeat([]byte{0x01}, lcommon.AddressHashSize),
		AddedSlot:  1,
		Amount:     types.Uint64(1_000_000 + i),
	}
}

func BenchmarkInsertUtxoModel(b *testing.B) {
	ctx := context.Background()

	b.Run("one_shot_uncached", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		i := uint64(0)
		for b.Loop() {
			i++
			utxo := utxoForBenchmarkIteration(i)
			if _, err := legacyInsertUtxo(ctx, store.writeDB, utxo); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("prepared_cache", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		i := uint64(0)
		for b.Loop() {
			i++
			utxo := utxoForBenchmarkIteration(i)
			if err := store.insertUtxoModel(ctx, store.writeDB, utxo, true); err != nil {
				b.Fatal(err)
			}
		}
	})
}
