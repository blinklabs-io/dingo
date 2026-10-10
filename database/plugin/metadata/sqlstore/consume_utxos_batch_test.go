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
SET deleted_slot = ?, spent_at_tx_id = ?
WHERE deleted_slot = 0 AND spent_at_tx_id IS NULL
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
// row carries its own deleted_slot and spent_at_tx_id, so those columns have
// many distinct values with few rows each while "deleted_slot = 0" and
// "spent_at_tx_id IS NULL" match every live row. That is the distribution
// that makes SQLite statistics report deleted_slot as selective.
func seedSkewedUtxos(tb testing.TB, store *Store, n int) {
	tb.Helper()
	tx, err := store.writeDB.Begin()
	require.NoError(tb, err)
	stmt, err := tx.Prepare(
		"INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, " +
			"added_slot, deleted_slot, spent_at_tx_id, amount, " +
			"payment_script) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
	)
	require.NoError(tb, err)
	for i := range n {
		var deletedSlot int64
		var spentBy []byte
		if i%10 != 0 {
			deletedSlot = int64(1_000 + i)
			spentBy = skewedUtxoTxID(i)
			spentBy[31] = 1
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

func consumeBatchArgs(n int) []any {
	args := []any{int64(1), make([]byte, 32)}
	for i := range n {
		args = append(args, skewedUtxoTxID(i*7), i%4)
	}
	return args
}

// TestConsumeUtxosBatchQueryPlansOnTxIDOutputIdx pins the plan of the batched
// spend UPDATE: one tx_id_output_idx point lookup per input, never a
// deleted_slot-leading index. The predicates "deleted_slot = 0" and
// "spent_at_tx_id IS NULL" match every live row, but sqlite_stat1 averages
// over the many spent rows and makes them look selective, so a plan that
// depends on statistics flips with the statistics. The plan must hold with no
// statistics, with stat1 and stat4, with stat1 only, and with the stat1 values
// observed on a 24M-row Preview node.
func TestConsumeUtxosBatchQueryPlansOnTxIDOutputIdx(t *testing.T) {
	// Not t.Parallel: the subtests are ordered stages that rewrite this
	// store's sqlite_stat tables.
	store := newMigratedSQLiteStore(t)
	seedSkewedUtxos(t, store, 50_000)
	ctx := context.Background()
	conn, err := store.writeDB.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	exec := func(stmt string) int64 {
		t.Helper()
		res, err := conn.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
		n, err := res.RowsAffected()
		require.NoError(t, err, stmt)
		return n
	}
	statRows := func(table string) int {
		t.Helper()
		var n int
		require.NoError(t, conn.QueryRowContext(
			ctx, "SELECT count(*) FROM "+table,
		).Scan(&n))
		return n
	}

	stages := []struct {
		name  string
		setup func()
	}{
		{"no_stats", func() {
			var n int
			require.NoError(t, conn.QueryRowContext(
				ctx,
				"SELECT count(*) FROM sqlite_master WHERE name = 'sqlite_stat1'",
			).Scan(&n))
			require.Zero(t, n, "fixture must start without sqlite_stat1")
		}},
		{"stat1_and_stat4", func() {
			exec("ANALYZE")
			require.Positive(t, statRows("sqlite_stat1"))
			require.Positive(t, statRows("sqlite_stat4"))
		}},
		{"stat1_only", func() {
			exec("DELETE FROM sqlite_stat4")
			exec("ANALYZE sqlite_master")
			require.Positive(t, statRows("sqlite_stat1"))
			require.Zero(t, statRows("sqlite_stat4"))
		}},
		{"preview_stat1", func() {
			require.EqualValues(t, 1, exec(
				"UPDATE sqlite_stat1 SET stat = '24389401 12 7 2' "+
					"WHERE idx = 'idx_utxo_deleted_payment_script'",
			))
			require.EqualValues(t, 1, exec(
				"UPDATE sqlite_stat1 SET stat = '24389401 4 1' "+
					"WHERE idx = 'tx_id_output_idx'",
			))
			exec("ANALYZE sqlite_master")
		}},
	}
	for _, stage := range stages {
		stage.setup()
		for _, returnStake := range []bool{false, true} {
			for n := 2; n <= utxoBatchSize; n++ {
				name := fmt.Sprintf(
					"%s/stake=%v/inputs=%d", stage.name, returnStake, n,
				)
				plan := connQueryPlan(
					t, conn, consumeUtxosBatchQuery(n, returnStake),
					consumeBatchArgs(n)...,
				)
				require.NotContains(t, plan, "idx_utxo_deleted", name+": "+plan)
				require.NotContains(t, plan, "SCAN utxo", name+": "+plan)
				require.Equal(
					t, n,
					strings.Count(
						plan,
						"USING INDEX tx_id_output_idx (tx_id=? AND output_idx=?)",
					),
					name+": "+plan,
				)
			}
		}
	}
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
	// deleted with a NULL spent_at_tx_id, and row 9 not deleted but with a
	// spent_at_tx_id: each satisfies only one of the two liveness predicates.
	// Row 19 is already spent by the spending transaction itself.
	_, err := store.writeDB.Exec(
		"UPDATE utxo SET spent_at_tx_id = NULL WHERE added_slot = 7",
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		"UPDATE utxo SET deleted_slot = 0, spent_at_tx_id = ? "+
			"WHERE added_slot = 9",
		skewedUtxoTxID(9),
	)
	require.NoError(t, err)

	_, err = store.writeDB.Exec(
		"UPDATE utxo SET deleted_slot = 77, spent_at_tx_id = ? "+
			"WHERE added_slot = 19",
		skewedUtxoTxID(5_000_000),
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
		args := []any{int64(77), skewedUtxoTxID(5_000_000)}
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
			"SELECT tx_id, output_idx, deleted_slot, spent_at_tx_id "+
				"FROM utxo ORDER BY id",
		)
		require.NoError(t, err)
		for srows.Next() {
			var txID, spentBy []byte
			var idx, deleted int64
			require.NoError(t, srows.Scan(&txID, &idx, &deleted, &spentBy))
			state = append(
				state,
				fmt.Sprintf("%x:%d:%d:%x", txID, idx, deleted, spentBy),
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
