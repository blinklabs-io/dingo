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
	"encoding/binary"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func seedBlockAmountRows(tb testing.TB, store *Store, count int) []byte {
	tb.Helper()
	hash := bytes.Repeat([]byte{0xa4}, 32)
	txn, err := store.writeDB.BeginTx(tb.Context(), nil)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = txn.Rollback() })
	insertTx, err := txn.Prepare(`INSERT INTO "transaction"
(hash, block_hash, fee, block_index, valid) VALUES (?, ?, '7', ?, TRUE)`)
	require.NoError(tb, err)
	defer insertTx.Close()
	insertOutput, err := txn.Prepare(`INSERT INTO utxo
(transaction_id, tx_id, output_idx, amount) VALUES (?, ?, ?, '1000')`)
	require.NoError(tb, err)
	defer insertOutput.Close()
	for i := range count {
		txHash := make([]byte, 32)
		binary.BigEndian.PutUint64(txHash[24:], uint64(i+1))
		result, err := insertTx.Exec(txHash, hash, i)
		require.NoError(tb, err)
		id, err := result.LastInsertId()
		require.NoError(tb, err)
		for out := range 2 {
			_, err := insertOutput.Exec(id, txHash, out)
			require.NoError(tb, err)
		}
	}
	require.NoError(tb, txn.Commit())
	return hash
}

func TestBlockTransactionTotalsSumsOutputsAndFees(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	hash := seedBlockAmountRows(t, store, 5)
	totals, err := store.GetBlockTransactionTotals(hash, nil)
	require.NoError(t, err)
	require.Equal(t, models.BlockTransactionTotals{
		Output: "10000", Fees: "35",
	}, totals)
}

// TestBlockTransactionTotalsQueryIsIndexBacked guards the projection's stated
// contract: reads stay rooted at the block's transactions and reach outputs
// and collateral returns through their indexes, never by scanning all output
// history.
func TestBlockTransactionTotalsQueryIsIndexBacked(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	hash := seedBlockAmountRows(t, store, 5)
	rows, err := store.writeDB.QueryContext(t.Context(),
		"EXPLAIN QUERY PLAN "+blockTransactionAmountRows, hash, hash, hash)
	require.NoError(t, err)
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan.WriteString(detail + "\n")
	}
	require.NoError(t, rows.Err())
	require.Contains(t, plan.String(), "idx_transaction_block_hash")
	require.Contains(t, plan.String(), "idx_utxo_transaction_id")
	require.Contains(t, plan.String(), "idx_utxo_collateral_return_for_tx_id")
	require.NotContains(t, plan.String(), "SCAN utxo")
	require.NotContains(t, plan.String(), "SCAN u\n")
	require.NotContains(t, plan.String(), "SCAN cr\n")
	require.NotContains(t, plan.String(), "AUTOMATIC")
}

func BenchmarkBlockTransactionTotals(b *testing.B) {
	store := newMigratedSQLiteStore(b)
	hash := seedBlockAmountRows(b, store, 1000)
	want := models.BlockTransactionTotals{Output: "2000000", Fees: "7000"}
	project := func() (models.BlockTransactionTotals, error) {
		return store.GetBlockTransactionTotals(hash, nil)
	}
	hydrate := func() (models.BlockTransactionTotals, error) {
		txs, err := store.GetTransactionsByBlockHash(hash, nil)
		if err != nil {
			return models.BlockTransactionTotals{}, err
		}
		output, fees := new(big.Int), new(big.Int)
		var amount big.Int
		for _, tx := range txs {
			fees.Add(fees, amount.SetUint64(uint64(tx.Fee)))
			for _, out := range tx.Outputs {
				output.Add(output, amount.SetUint64(uint64(out.Amount)))
			}
		}
		return models.BlockTransactionTotals{
			Output: output.String(), Fees: fees.String(),
		}, nil
	}
	for _, test := range []struct {
		name string
		run  func() (models.BlockTransactionTotals, error)
	}{{"hydrated", hydrate}, {"projected", project}} {
		b.Run(test.name, func(b *testing.B) {
			got, err := test.run()
			require.NoError(b, err)
			require.Equal(b, want, got)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				got, err := test.run()
				if err != nil || got != want {
					b.Fatalf("totals = %+v, err = %v", got, err)
				}
			}
		})
	}
}
