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
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// legacyConsumeUtxosBatchQuery is the row-value IN form consumeUtxosBatchQuery
// used before it was rewritten to one point lookup per input.
func legacyConsumeUtxosBatchQuery(rowCount int, returnStake bool) string {
	values := strings.TrimSuffix(strings.Repeat("(?,?),", rowCount), ",")
	returning := "tx_id, output_idx"
	if returnStake {
		returning += ", credential_tag, staking_key, amount"
	}
	return `UPDATE utxo
SET deleted_slot = ?, spent_by_tx_id = ?
WHERE deleted_slot = 0 AND spent_by_tx_id IS NULL
  AND (tx_id, output_idx) IN (` + values + `)
RETURNING ` + returning
}

func skewedUtxoTxID(i int) []byte {
	txID := make([]byte, 32)
	txID[0] = byte(i >> 24)
	txID[1] = byte(i >> 16)
	txID[2] = byte(i >> 8)
	txID[3] = byte(i)
	return txID
}

// seedSkewedUtxos inserts n rows of which one in ten is unspent. Every spent
// row carries its own deleted_slot and spent_by_tx_id, so those columns have
// many distinct values with few rows each while "deleted_slot = 0" and
// "spent_by_tx_id IS NULL" match every live row. That is the distribution
// that makes SQLite statistics report deleted_slot as selective.
func seedSkewedUtxos(tb testing.TB, store *Store, n int) {
	tb.Helper()
	tx, err := store.writeDB.Begin()
	require.NoError(tb, err)
	stmt, err := tx.Prepare(
		"INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, " +
			"added_slot, deleted_slot, spent_by_tx_id, amount, " +
			"payment_script) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
	)
	require.NoError(tb, err)
	for i := range n {
		var deletedSlot int64
		var spentBy any
		if i%10 != 0 {
			deletedSlot = int64(1_000 + i)
			spentBy = int64(i + 1)
		}
		_, err := stmt.Exec(
			skewedUtxoTxID(i), i%4, make([]byte, 28), int64(i%2),
			int64(i), deletedSlot, spentBy, "1000000", i%2,
		)
		require.NoError(tb, err)
	}
	require.NoError(tb, stmt.Close())
	require.NoError(tb, tx.Commit())
}

func connQueryPlan(
	tb testing.TB,
	conn *sql.Conn,
	query string,
	args ...any,
) string {
	tb.Helper()
	// Production runs the expanded form (utxo_ref.go).
	query, args = expandUtxoRefs(query, args)
	rows, err := conn.QueryContext(
		context.Background(), "EXPLAIN QUERY PLAN "+query, args...,
	)
	require.NoError(tb, err)
	defer func() { require.NoError(tb, rows.Close()) }()
	var lines []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(tb, rows.Scan(&id, &parent, &notUsed, &detail))
		lines = append(lines, detail)
	}
	require.NoError(tb, rows.Err())
	return strings.Join(lines, "\n")
}

type consumedRow struct {
	txID      string
	outputIdx int64
	tag       sql.NullInt64
	key       string
	amount    sql.NullString
}

// TestConsumeUtxosBatchQueryMatchesLegacy proves the point-lookup form spends
// the same rows and returns the same RETURNING rows as the previous
// row-value IN form, including inputs that are already spent, spent by the
// same transaction, or absent.
func TestConsumeUtxosBatchQueryMatchesLegacy(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedSkewedUtxos(t, store, 400)
	ctx := context.Background()

	// Rows with i%10 == 0 are live and the rest spent. Row 7 is made
	// deleted with a NULL spent_by_tx_id, and row 9 not deleted but with a
	// spent_by_tx_id: each satisfies only one of the two liveness predicates.
	// Row 19 is already spent by the spending transaction itself.
	_, err := store.writeDB.Exec(
		"UPDATE utxo SET spent_by_tx_id = NULL WHERE added_slot = 7",
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		"UPDATE utxo SET deleted_slot = 0, spent_by_tx_id = ? "+
			"WHERE added_slot = 9",
		int64(9),
	)
	require.NoError(t, err)

	_, err = store.writeDB.Exec(
		"UPDATE utxo SET deleted_slot = 77, spent_by_tx_id = ? "+
			"WHERE added_slot = 19",
		int64(5_000_000),
	)
	require.NoError(t, err)

	type input struct {
		txID []byte
		idx  int
	}
	live := func(i int) input { return input{skewedUtxoTxID(i * 10), (i * 10) % 4} }
	cases := map[string][]input{
		"all_live": {live(1), live(2), live(3)},
		"mixed": {
			live(4), {skewedUtxoTxID(1), 1}, live(5),
			{skewedUtxoTxID(7), 3}, {skewedUtxoTxID(9), 1},
			{skewedUtxoTxID(999_999), 0}, live(6), live(7),
		},
		"wrong_output_idx_of_live_tx": {live(8), {skewedUtxoTxID(80), 3}},
		"none_live": {
			{skewedUtxoTxID(1), 1}, {skewedUtxoTxID(2), 2},
			{skewedUtxoTxID(19), 3}, {skewedUtxoTxID(999_998), 0},
		},
	}
	run := func(query string, returnStake bool, in []input) (
		[]consumedRow, []string,
	) {
		t.Helper()
		tx, err := store.writeDB.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, tx.Rollback()) }()
		args := []any{int64(77), int64(5_000_000)}
		for _, in := range in {
			args = append(args, in.txID, in.idx)
		}
		rows, err := tx.QueryContext(ctx, query, args...)
		require.NoError(t, err)
		var got []consumedRow
		for rows.Next() {
			var r consumedRow
			var txID, key []byte
			if returnStake {
				require.NoError(t, rows.Scan(
					&txID, &r.outputIdx, &r.tag, &key, &r.amount,
				))
			} else {
				require.NoError(t, rows.Scan(&txID, &r.outputIdx))
			}
			r.txID, r.key = string(txID), string(key)
			got = append(got, r)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		slices.SortFunc(got, func(a, b consumedRow) int {
			return strings.Compare(
				fmt.Sprint(
					a.txID,
					a.outputIdx,
				),
				fmt.Sprint(b.txID, b.outputIdx),
			)
		})

		var state []string
		srows, err := tx.QueryContext(ctx,
			"SELECT tx_id, output_idx, deleted_slot, spent_by_tx_id "+
				"FROM utxo ORDER BY id",
		)
		require.NoError(t, err)
		for srows.Next() {
			var txID []byte
			var spentBy sql.NullInt64
			var idx, deleted int64
			require.NoError(t, srows.Scan(&txID, &idx, &deleted, &spentBy))
			state = append(
				state,
				fmt.Sprintf("%x:%d:%d:%v", txID, idx, deleted, spentBy),
			)
		}
		require.NoError(t, srows.Err())
		require.NoError(t, srows.Close())
		return got, state
	}

	for name, in := range cases {
		for _, returnStake := range []bool{false, true} {
			t.Run(
				fmt.Sprintf("%s/stake=%v", name, returnStake),
				func(t *testing.T) {
					wantRows, wantState := run(
						legacyConsumeUtxosBatchQuery(len(in), returnStake),
						returnStake, in,
					)
					gotRows, gotState := run(
						consumeUtxosBatchQuery(len(in), returnStake),
						returnStake, in,
					)
					require.Equal(t, wantRows, gotRows)
					require.Equal(t, wantState, gotState)
				},
			)
		}
	}

	// Guard against a vacuous comparison: of the eight mixed inputs only the
	// four live ones are spent.
	rows, _ := run(
		consumeUtxosBatchQuery(len(cases["mixed"]), true), true, cases["mixed"],
	)
	require.Len(t, rows, 4)
}
