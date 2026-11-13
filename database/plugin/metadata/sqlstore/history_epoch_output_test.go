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
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEpochOutputRangePreservesExecutionSlotAndAmounts(t *testing.T) {
	t.Parallel()
	testEpochOutputRange(t, newMigratedSQLiteStore(t))
}

func testEpochOutputRange(t *testing.T, store *Store) {
	t.Helper()
	max := new(big.Int).SetUint64(math.MaxUint64)
	for i, row := range []struct {
		slot        uint64
		valid       bool
		amount      string
		collateral  bool
		contextSlot *uint64
	}{
		{10, true, max.String(), false, nil},
		{11, true, max.String(), false, nil},
		{12, false, "7", true, nil},
		{25, true, "13", false, new(uint64(11))},
		{11, true, "17", false, new(uint64(25))},
		{0, true, "19", false, new(uint64(0))},
		{30, false, "29", true, new(uint64(11))},
	} {
		hash := bytes.Repeat([]byte{byte(i + 1)}, 32)
		transaction := store.dialect.QuoteIdentifier("transaction")
		_, err := store.writeDB.Exec(
			store.dialect.Rebind("INSERT INTO "+transaction+" (hash) VALUES (?)"),
			hash,
		)
		require.NoError(t, err)
		var id int64
		err = store.writeDB.QueryRow(store.dialect.Rebind("SELECT id FROM "+transaction+" WHERE hash=?"), hash).
			Scan(&id)
		require.NoError(t, err)
		_, err = store.writeDB.Exec(
			store.dialect.Rebind(
				"INSERT INTO applied_transaction (tx_id,slot,valid) VALUES (?,?,?)",
			),
			id,
			row.slot,
			row.valid,
		)
		require.NoError(t, err)
		if row.collateral {
			_, err = store.writeDB.Exec(
				store.dialect.Rebind(
					"INSERT INTO utxo (tx_id,collateral_return_for_tx_id,output_idx,amount,deleted_slot) VALUES (?,?,0,?,999)",
				),
				id,
				id,
				row.amount,
			)
		} else {
			_, err = store.writeDB.Exec(
				store.dialect.Rebind(
					"INSERT INTO utxo (tx_id,output_idx,amount,deleted_slot) VALUES (?,0,?,999)",
				),
				id,
				row.amount,
			)
		}
		require.NoError(t, err)
		if row.contextSlot != nil {
			_, err = store.writeDB.Exec(
				store.dialect.Rebind(
					"INSERT INTO leios_transaction_context (transaction_id,slot) VALUES (?,?)",
				),
				id,
				*row.contextSlot,
			)
			require.NoError(t, err)
		}
		if !row.valid {
			// Invalid normal outputs and valid collateral returns are not output flow.
			_, err = store.writeDB.Exec(
				store.dialect.Rebind(
					"INSERT INTO utxo (tx_id,output_idx,amount) VALUES (?,1,'100')",
				),
				id,
			)
			require.NoError(t, err)
		} else {
			_, err = store.writeDB.Exec(store.dialect.Rebind("INSERT INTO utxo (tx_id,collateral_return_for_tx_id,output_idx,amount) VALUES (?,?,2,'100')"), id, id)
			require.NoError(t, err)
		}
	}
	twice := new(big.Int).Mul(new(big.Int).Set(max), big.NewInt(2))
	for _, tc := range []struct {
		start, end uint64
		want       string
	}{
		{0, 0, "19"},
		{10, 10, max.String()},
		{11, 11, new(big.Int).Add(new(big.Int).Set(max), big.NewInt(42)).String()},
		{10, 12, new(big.Int).Add(twice, big.NewInt(49)).String()},
		{12, 12, "7"},
		{13, 24, "0"},
		{25, 25, "17"},
		{10, 9, "0"},
	} {
		got, err := store.SumTransactionOutputsInSlotRange(
			tc.start,
			tc.end,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, tc.want, got.String(), "range %d-%d", tc.start, tc.end)
	}
	_, err := store.SumTransactionOutputsInSlotRange(0, math.MaxUint64, nil)
	require.Error(t, err)
}

func TestEpochOutputRangeDoesNotScanRetainedHistory(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	_, err := store.writeDB.Exec("ANALYZE")
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		"INSERT OR REPLACE INTO sqlite_stat1(tbl,idx,stat) VALUES ('utxo','idx_utxo_collateral_return_for_tx_id','1000000 1000000')",
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec("ANALYZE sqlite_schema")
	require.NoError(t, err)
	for _, collateral := range []bool{false, true} {
		rows, err := store.writeDB.Query(
			"EXPLAIN QUERY PLAN "+store.transactionOutputRangeQuery(collateral),
			100,
			200,
			100,
			200,
		)
		require.NoError(t, err)
		var details []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
			details = append(details, detail)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		plan := strings.Join(details, "\n")
		require.NotContains(
			t,
			plan,
			"SCAN tx",
			"epoch totals must range-select transaction history",
		)
		require.NotContains(
			t,
			plan,
			"SCAN lc",
			"epoch totals must range-select closure execution slots",
		)
		require.NotContains(
			t,
			plan,
			"SCAN u",
			"epoch totals must look up the selected transactions' outputs",
		)
	}
}
