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
	"maps"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestApplyPathStatementsArePreparedOnce drives the statements the per-block
// apply path issues -- transaction insert, per-input spend marking, account
// and pool lookups, tip and block-nonce writes -- twice and asserts that no
// connection ever prepares, queries or executes their text again after Start.
// A statement served by the hot-statement cache reaches the driver only as a
// reuse of the statement Start prepared; one that is re-prepared per call
// shows up in the recorder.
func TestApplyPathStatementsArePreparedOnce(t *testing.T) {
	t.Parallel()
	store, recorder := newRecordingSQLiteStore(t)

	hot := []struct{ name, query string }{
		{"transaction identity insert", transactionIdentityInsertSQL},
		{"applied transaction insert", appliedTransactionInsertSQL},
		{"utxo spend state", sqliteUtxoSQL.getSpendState},
		{"pool by key hash", poolByKeyHashQuery},
		{"account by credential", sqlitequery.GetAccountByCredentialQuery},
		{"active account", sqlitequery.GetActiveAccountByCredentialQuery},
		{"sync state", sqlitequery.GetSyncStateQuery},
		{"set tip", sqlitequery.SetTipQuery},
		{"set block nonce", sqlitequery.SetBlockNonceQuery},
	}
	prepared := map[string]bool{}
	for _, q := range recorder.snapshot() {
		prepared[q] = true
	}
	for _, h := range hot {
		name := h.name
		require.True(
			t,
			prepared[h.query],
			"expected Start to prepare %s", name,
		)
	}

	mark := len(recorder.snapshot())
	for _, seed := range []byte{0x01, 0x02} {
		fx := buildSharedCredentialTx(t, seed)
		seedConsumedUtxo(t, store, fx)
		// Applying a transaction twice takes the already-spent branch of
		// the input loop, which issues the spend-state SELECT.
		for range 2 {
			require.NoError(t, store.SetTransaction(
				fx.tx, fx.point, 0, fx.certDeposits, false, nil, 0,
			))
		}
		value, err := store.GetSyncState("absent-trust-boundary", nil)
		require.NoError(t, err)
		require.Empty(t, value)
		_, err = store.GetAccountByCredential(0, fx.ref.Key, true, nil)
		require.NoError(t, err)
		_, err = store.GetAccountByCredential(0, fx.ref.Key, false, nil)
		require.NoError(t, err)
		_, err = store.GetPool(lcommon.PoolKeyHash{seed}, true, nil)
		require.NoError(t, err)
		require.NoError(t, store.SetTip(ochainsync.Tip{
			Point:       ocommon.Point{Slot: uint64(seed), Hash: fx.point.Hash},
			BlockNumber: uint64(seed),
		}, nil))
		require.NoError(t, store.SetBlockNonce(
			fx.point.Hash, uint64(seed), []byte{seed}, false, nil,
		))
	}

	after := recorder.snapshot()[mark:]
	for _, h := range hot {
		name, query := h.name, h.query
		count := 0
		for _, q := range after {
			if q == query {
				count++
			}
		}
		require.Zero(
			t, count,
			"%s was prepared or run uncached %d times after Start", name, count,
		)
	}
}

// TestCachedStatementsFallBackWhenTxScopedStmtClosed closes the
// transaction-scoped statements txScopedStmt memoized for an exec (set tip)
// and a single-row query (pool by key hash) while their transaction is still
// open. database/sql closes them this way when the transaction's context is
// cancelled, and the asynchronous rollback that follows makes that path
// timing-dependent to reach; closing them directly reaches it every run.
func TestCachedStatementsFallBackWhenTxScopedStmtClosed(t *testing.T) {
	t.Parallel()
	store, _ := newRecordingSQLiteStore(t)
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 1, Hash: []byte{0x01}},
		BlockNumber: 1,
	}
	require.NoError(t, store.SetTip(tip, txn))
	pool, err := store.GetPool(lcommon.PoolKeyHash{0x01}, true, txn)
	require.NoError(t, err)
	require.Nil(t, pool)

	sqlTransaction, ok := txn.(*sqlTxn)
	require.True(t, ok)
	for _, query := range []string{
		sqlitequery.SetTipQuery,
		poolByKeyHashQuery,
	} {
		cached, ok := store.lookupCachedStmt(query)
		require.True(t, ok)
		require.NoError(t, store.txScopedStmt(
			t.Context(), sqlTransaction.tx, cached,
		).Close())
	}

	require.NoError(t, store.SetTip(tip, txn))
	pool, err = store.GetPool(lcommon.PoolKeyHash{0x01}, true, txn)
	require.NoError(t, err)
	require.Nil(t, pool)
	require.NoError(t, txn.Commit())
}

// TestQueryRowsCachedFallsBackWhenTxScopedStmtClosed is the multi-row
// counterpart: queryRowsCached serves the batched UTxO consume and address
// input queries, and must also retry uncached once the transaction-scoped
// statement it memoized has been closed.
func TestQueryRowsCachedFallsBackWhenTxScopedStmtClosed(t *testing.T) {
	t.Parallel()
	store, _ := newRecordingSQLiteStore(t)
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	sqlTransaction, ok := txn.(*sqlTxn)
	require.True(t, ok)
	cached, ok := store.lookupCachedStmt(sqliteUtxoSQL.getSpendState)
	require.True(t, ok)

	query := func() error {
		return store.withWriteTransaction(
			txn,
			func(db queryer, ctx context.Context) error {
				rows, err := store.queryRowsCached(
					ctx, db, sqliteUtxoSQL.getSpendState,
					sqliteUtxoSQL.refArgs([]byte{0x01}, 0)...,
				)
				if err != nil {
					return err
				}
				defer rows.Close()
				for rows.Next() {
				}
				return rows.Err()
			},
		)
	}
	require.NoError(t, query())
	require.NoError(t, store.txScopedStmt(
		t.Context(), sqlTransaction.tx, cached,
	).Close())
	require.NoError(t, query())
	require.NoError(t, txn.Commit())
}

func TestSyncStatePreparedReadObservesTransactionWrites(t *testing.T) {
	t.Parallel()
	store, recorder := newRecordingSQLiteStore(t)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { require.NoError(t, txn.Rollback()) })
	mark := len(recorder.snapshot())
	value, err := store.GetSyncState("boundary", txn)
	require.NoError(t, err)
	require.Empty(t, value)
	require.NoError(t, store.SetSyncState("boundary", "42", txn))
	value, err = store.GetSyncState("boundary", txn)
	require.NoError(t, err)
	require.Equal(t, "42", value)
	require.NoError(t, store.SetSyncState("boundary", "43", txn))
	value, err = store.GetSyncState("boundary", txn)
	require.NoError(t, err)
	require.Equal(t, "43", value)
	require.NoError(t, store.DeleteSyncState("boundary", txn))
	value, err = store.GetSyncState("boundary", txn)
	require.NoError(t, err)
	require.Empty(t, value)
	for _, query := range recorder.snapshot()[mark:] {
		require.NotEqual(
			t,
			sqlitequery.GetSyncStateQuery,
			query,
			"sync-state reads must reuse the prepared statement",
		)
	}
}

func BenchmarkGetSyncStateInTransaction(b *testing.B) {
	for _, name := range []string{"missing", "present"} {
		b.Run(name, func(b *testing.B) {
			store := newMigratedSQLiteStore(b)
			expected := ""
			if name == "present" {
				expected = "42"
				if err := store.SetSyncState("boundary", expected, nil); err != nil {
					b.Fatal(err)
				}
			}
			txn := store.Transaction(context.Background())
			b.Cleanup(func() {
				if err := txn.Rollback(); err != nil {
					b.Error(err)
				}
			})
			b.ReportAllocs()
			for b.Loop() {
				value, err := store.GetSyncState("boundary", txn)
				if err != nil {
					b.Fatal(err)
				}
				if value != expected {
					b.Fatalf("sync state = %q, want %q", value, expected)
				}
			}
		})
	}
}

// cachedStatementHits returns how many times each cached statement has been
// borrowed since the store started.
func (s *Store) cachedStatementHits() map[string]uint64 {
	s.stmtMu.Lock()
	defer s.stmtMu.Unlock()
	hits := make(map[string]uint64, len(s.stmtHits))
	maps.Copy(hits, s.stmtHits)
	return hits
}

// TestApplyPathUtxoStatementsComeFromThePreparedCache drives input
// resolution, spend marking (single and batched), output insert (single and
// batched), and the un-spend of a stale spend, and requires that each
// statement was prepared at Start, was served from the cache at least once,
// and was never prepared or run uncached afterwards. The cache's own hit
// counter is the positive evidence: the driver's record shows only that no
// uncached call happened, which a statement that never ran satisfies too.
func TestApplyPathUtxoStatementsComeFromThePreparedCache(t *testing.T) {
	t.Parallel()
	store, recorder := newRecordingSQLiteStore(t)
	u := sqliteUtxoSQL

	statements := []struct{ name, query string }{
		{"input resolution (live)", u.getLiveByRef},
		{"input resolution (including spent)", u.getIncludingSpent},
		{"spend marking", consumeUtxoSQLiteReturningSQL},
		{"spend marking (batch)", consumeUtxosBatchQuery(2, true)},
		{"spend state", u.getSpendState},
		{"output insert", u.insertResultIgnore},
		{"output insert (batch)", insertUtxoBatchQuery(2)},
		{"un-spend of a stale spend", u.clearStaleSpend},
		{"imported row id", u.idByRef},
		{"imported row insert", u.insertIgnore},
	}
	prepared := map[string]bool{}
	for _, q := range recorder.snapshot() {
		prepared[q] = true
	}
	for _, s := range statements {
		require.True(t, prepared[s.query], "expected Start to prepare %s", s.name)
	}
	mark := len(recorder.snapshot())

	// Spend marking and output insert: a one-input one-output transaction,
	// applied twice for the already-spent branch, then a transaction with two
	// inputs and two outputs for the batched forms.
	fx := buildSharedCredentialTx(t, 0x31)
	seedConsumedUtxo(t, store, fx)
	for range 2 {
		require.NoError(t, store.SetTransaction(
			fx.tx, fx.point, 0, fx.certDeposits, false, nil, 0,
		))
	}
	two := buildTwoInputTwoOutputTx(t, store, 0x32)
	require.NoError(t, store.SetTransaction(
		two.tx, two.point, 0, nil, false, nil, 0,
	))

	// Input resolution of a produced output and of an imported one.
	produced, err := store.GetUtxo(two.producedTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, produced)
	_, err = store.GetUtxoIncludingSpent(fx.consumedTxID, 0, nil)
	require.NoError(t, err)

	// Un-spend: a live re-import of an output spent after the anchor.
	imported := importConflictHash32(0x33)
	spender := importConflictHash32(0x34)
	live := []models.Utxo{{
		TxId: imported, OutputIdx: 0, AddedSlot: 100, Amount: 5_000_000,
	}}
	require.NoError(t, store.ImportUtxos(live, nil))
	markUtxoSpentDirect(t, store, imported, 0, spender, 150)
	require.NoError(t, store.ImportUtxos(live, nil))
	deleted, _ := utxoDeletedAndSpender(t, store, imported, 0)
	require.Zero(t, deleted, "the re-import must clear the stale spend")

	hits := store.cachedStatementHits()
	after := recorder.snapshot()[mark:]
	for _, s := range statements {
		require.NotZero(
			t, hits[s.query], "%s was never served from the cache", s.name,
		)
		uncached := 0
		for _, q := range after {
			if q == s.query {
				uncached++
			}
		}
		require.Zero(t, uncached, "%s was prepared or run uncached %d times after Start", s.name, uncached)
	}
}

type twoInputTwoOutputTx struct {
	tx           lcommon.Transaction
	point        ocommon.Point
	producedTxID []byte
}

// buildTwoInputTwoOutputTx seeds two live outputs and returns a transaction
// that spends both and produces two outputs, so block apply takes the batched
// spend and batched insert statements.
func buildTwoInputTwoOutputTx(
	t *testing.T,
	store *Store,
	seed byte,
) twoInputTwoOutputTx {
	t.Helper()
	fx := buildSharedCredentialTx(t, seed)
	inputs := make([]lcommon.TransactionInput, 0, 2)
	for i := range byte(2) {
		hash := make([]byte, 32)
		hash[0], hash[1], hash[2] = seed, 0xcc, i
		_, err := store.writeDB.Exec(
			`INSERT INTO utxo (tx_id, output_idx, credential_tag, added_slot, deleted_slot, amount)
VALUES (?, 0, 0, 1, 0, '1000000')`, hash,
		)
		require.NoError(t, err)
		input, err := mockledger.NewTransactionInputBuilder().
			WithTxId(hash).WithIndex(0).Build()
		require.NoError(t, err)
		inputs = append(inputs, input)
	}
	outputs := fx.tx.Outputs()
	txID := make([]byte, 32)
	txID[0], txID[1], txID[2] = seed, 0xdd, 0x01
	builder := mockledger.NewTransactionBuilder().
		WithId(txID).WithInputs(inputs...).
		WithOutputs(outputs[0], outputs[0]).WithValid(true)
	tx, err := builder.Build()
	require.NoError(t, err)
	return twoInputTwoOutputTx{
		tx:           tx,
		point:        ocommon.Point{Slot: 2000 + uint64(seed), Hash: txID},
		producedTxID: txID,
	}
}
