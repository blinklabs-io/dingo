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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
		{"transaction insert", transactionInsertSQL},
		{"utxo spend state", getUtxoSpendStateQuery},
		{"pool by key hash", poolByKeyHashQuery},
		{"account by credential", sqlitequery.GetAccountByCredentialQuery},
		{"active account", sqlitequery.GetActiveAccountByCredentialQuery},
		{"set tip", sqlitequery.SetTipQuery},
		{"set block nonce", sqlitequery.SetBlockNonceQuery},
	}
	prepared := map[string]bool{}
	for _, q := range recorder.snapshot() {
		prepared[q] = true
	}
	for _, h := range hot {
		name, query := h.name, h.query
		require.True(
			t,
			prepared[query],
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
				fx.tx, fx.point, 0, fx.certDeposits, false, nil,
			))
		}
		_, err := store.GetAccountByCredential(0, fx.ref.Key, true, nil)
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
	cached, ok := store.lookupCachedStmt(getUtxoSpendStateQuery)
	require.True(t, ok)

	query := func() error {
		return store.withWriteTransaction(
			txn,
			func(db queryer, ctx context.Context) error {
				rows, err := store.queryRowsCached(
					ctx, db, getUtxoSpendStateQuery, []byte{0x01}, 0,
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
