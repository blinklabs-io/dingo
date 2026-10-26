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
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// overlayChain is a batch of transactions over pre-batch outputs. Batch
// transaction i spends pre-batch output i and the change output of
// transaction i-1, and pays a surviving output carrying an asset and a change
// output carrying another. A final transaction spends the last change and
// creates nothing, so the highest utxo and asset ids belong to elided rows.
type overlayChain struct {
	prebatch []lcommon.Transaction
	batch    []lcommon.Transaction
	inputs   []models.UtxoId
	// elided are the outputs the batch creates and spends again.
	elided []models.UtxoId
	stake  models.StakeCredentialRef
}

func overlayTxID(kind byte, i int) []byte {
	id := make([]byte, 32)
	id[0] = kind
	id[1] = byte(i >> 8)
	id[2] = byte(i)
	return id
}

func overlayTxInput(
	t testing.TB,
	txID []byte,
	idx uint32,
) lcommon.TransactionInput {
	t.Helper()
	input, err := mockledger.NewTransactionInputBuilder().
		WithTxId(txID).WithIndex(idx).Build()
	require.NoError(t, err)
	return input
}

func buildOverlayChain(t testing.TB, n int) overlayChain {
	t.Helper()
	staking := lcommon.NewBlake2b224([]byte("overlay-chain-staking-key"))
	payment := lcommon.NewBlake2b224([]byte("overlay-chain-payment-key"))
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
		payment.Bytes(), staking.Bytes(),
	)
	require.NoError(t, err)
	output := func(lovelace uint64, asset string) lcommon.TransactionOutput {
		builder := mockledger.NewTransactionOutputBuilder().
			WithAddress(addr.String()).WithLovelace(lovelace)
		if asset != "" {
			builder = builder.WithAssets(mockledger.Asset{
				PolicyId:  make([]byte, 28),
				AssetName: []byte(asset),
				Amount:    3,
			})
		}
		out, err := builder.Build()
		require.NoError(t, err)
		return out
	}
	chain := overlayChain{
		stake: models.NewStakeCredentialRef(0, staking.Bytes()),
	}
	for i := range n {
		pre := mockledger.NewTransactionBuilder()
		pre.WithId(overlayTxID(0x01, i))
		pre.WithOutputs(output(2_000_000, ""))
		pre.WithValid(true)
		chain.prebatch = append(chain.prebatch, pre)
		chain.inputs = append(chain.inputs, models.UtxoId{
			Hash: overlayTxID(0x01, i), Idx: 0,
		})

		inputs := []lcommon.TransactionInput{
			overlayTxInput(t, overlayTxID(0x01, i), 0),
		}
		if i > 0 {
			inputs = append(
				inputs,
				overlayTxInput(t, overlayTxID(0x02, i-1), 1),
			)
		}
		chain.elided = append(chain.elided, models.UtxoId{
			Hash: overlayTxID(0x02, i), Idx: 1,
		})
		tx := mockledger.NewTransactionBuilder()
		tx.WithId(overlayTxID(0x02, i))
		tx.WithInputs(inputs...)
		tx.WithOutputs(
			output(1_000_000, fmt.Sprintf("keep%d", i)),
			output(1_500_000, fmt.Sprintf("hop%d", i)),
		)
		tx.WithValid(true)
		chain.batch = append(chain.batch, tx)
	}
	sink := mockledger.NewTransactionBuilder()
	sink.WithId(overlayTxID(0x03, 0))
	sink.WithInputs(overlayTxInput(t, overlayTxID(0x02, n-1), 1))
	sink.WithValid(true)
	chain.batch = append(chain.batch, sink)
	return chain
}

func seedOverlayChain(t testing.TB, store *Store, chain overlayChain) {
	t.Helper()
	for i, tx := range chain.prebatch {
		require.NoError(t, store.SetTransaction(
			tx, ocommon.Point{Slot: 10 + uint64(i), Hash: tx.Hash().Bytes()},
			0, nil, true, nil, 0,
		))
	}
}

// applyOverlayChain applies the batch in one write transaction, through the
// overlay or the per-row path. Each transaction ends a block, as the ledger
// marks with FlushBatchStakeDeltas. The per-row path deletes the
// outputs the batch spent again before commit, as a trusted batch does.
func applyOverlayChain(
	t testing.TB,
	store *Store,
	chain overlayChain,
	overlay bool,
) {
	t.Helper()
	txn := store.Transaction(context.Background())
	acc := store.NewBatchAccumulator()
	if overlay {
		enabled, err := store.EnableBatchUtxoOverlay(
			acc, txn, chain.inputs, nil,
		)
		require.NoError(t, err)
		require.True(t, enabled)
	}
	for i, tx := range chain.batch {
		require.NoError(t, store.SetTransactionBatched(
			tx, ocommon.Point{Slot: 1_000 + uint64(i), Hash: tx.Hash().Bytes()},
			0, nil, true, acc, txn, 0,
		))
		require.NoError(t, store.FlushBatchStakeDeltas(acc, txn))
	}
	require.NoError(t, store.FlushBatch(acc, txn))
	if !overlay {
		require.NoError(t, store.DeleteUtxos(chain.elided, txn))
	}
	require.NoError(t, txn.Commit())
}

// overlayStoreState dumps the tables the overlay writes, ids included, and
// their id sequences.
func overlayStoreState(t testing.TB, store *Store) string {
	t.Helper()
	var out strings.Builder
	for _, query := range []string{
		`SELECT * FROM utxo`,
		`SELECT * FROM asset`,
		`SELECT * FROM utxo_address`,
		`SELECT * FROM utxo_pointer`,
		`SELECT credential_tag, staking_key, utxo_stake, reward_stake,
		        total_stake, updated_slot FROM reward_live_stake`,
		`SELECT * FROM sqlite_sequence WHERE name IN ('utxo', 'asset')`,
	} {
		rows, err := store.writeDB.QueryContext(context.Background(), query)
		require.NoError(t, err)
		cols, err := rows.Columns()
		require.NoError(t, err)
		var lines []string
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			parts := make([]string, len(cols))
			for i, col := range cols {
				if b, ok := values[i].([]byte); ok {
					parts[i] = col + "=" + hex.EncodeToString(b)
				} else {
					parts[i] = fmt.Sprintf("%s=%v", col, values[i])
				}
			}
			lines = append(lines, strings.Join(parts, "|"))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		sort.Strings(lines)
		fmt.Fprintf(&out, "## %s\n%s\n", query, strings.Join(lines, "\n"))
	}
	return out.String()
}

// newForeignKeySQLiteStore is newMigratedSQLiteStore with foreign keys
// enforced, as the sqlite provider opens production databases, so deleting a
// utxo row cascades to its asset, address and pointer rows.
func newForeignKeySQLiteStore(tb testing.TB) *Store {
	tb.Helper()
	db, err := OpenDB(
		"sqlite",
		fmt.Sprintf(
			"file:overlay_fk_%d?mode=memory&cache=shared"+
				"&_pragma=foreign_keys(1)",
			testStoreSequence.Add(1),
		),
		"sqlite",
		false,
	)
	require.NoError(tb, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(tb, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(tb, err)
	require.NoError(tb, store.Start(context.Background()))
	tb.Cleanup(func() { require.NoError(tb, store.Close()) })
	return store
}

func TestBatchUtxoOverlayMatchesPerRowPath(t *testing.T) {
	t.Parallel()
	chain := buildOverlayChain(t, 6)
	states := make([]string, 2)
	for i, overlay := range []bool{false, true} {
		store := newForeignKeySQLiteStore(t)
		seedOverlayChain(t, store, chain)
		applyOverlayChain(t, store, chain, overlay)
		states[i] = overlayStoreState(t, store)
	}
	require.Equal(t, states[0], states[1])
	require.Contains(t, states[1], "name=utxo|seq=")
}

// An input the caller did not name for the prefetch is read on first use
// and resolves the same way.
func TestBatchUtxoOverlayResolvesUnprefetchedInputs(t *testing.T) {
	t.Parallel()
	chain := buildOverlayChain(t, 4)
	reference := newForeignKeySQLiteStore(t)
	seedOverlayChain(t, reference, chain)
	applyOverlayChain(t, reference, chain, false)
	store := newForeignKeySQLiteStore(t)
	seedOverlayChain(t, store, chain)
	unnamed := chain
	unnamed.inputs = nil
	applyOverlayChain(t, store, unnamed, true)
	require.Equal(t,
		overlayStoreState(t, reference), overlayStoreState(t, store),
	)
}

// overlayOps reads dingo_database_sql_operations_total by op.
func overlayOps(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()
	ops := map[string]float64{}
	for _, op := range []string{
		"insert", "update", "delete", "select", "other",
	} {
		ops[op] = counterValue(t, reg, op)
	}
	return ops
}

func overlayBatchOps(
	t *testing.T,
	n int,
	overlay bool,
) map[string]float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	store := newMigratedSQLiteStoreWithRegistry(t, reg)
	chain := buildOverlayChain(t, n)
	seedOverlayChain(t, store, chain)
	before := overlayOps(t, reg)
	applyOverlayChain(t, store, chain, overlay)
	after := overlayOps(t, reg)
	for op := range after {
		after[op] -= before[op]
	}
	return after
}

// TestBatchUtxoOverlayStatementCount requires the overlay's SQL statements
// per batch not to grow with the batch: a batch four times as long issues
// the same number of every statement kind except one transaction row insert
// per transaction, which the overlay leaves per row.
func TestBatchUtxoOverlayStatementCount(t *testing.T) {
	t.Parallel()
	const small, large = 8, 32
	smallOps := overlayBatchOps(t, small, true)
	largeOps := overlayBatchOps(t, large, true)
	perRowOps := overlayBatchOps(t, large, false)
	t.Logf("overlay n=%d: %v", small, smallOps)
	t.Logf("overlay n=%d: %v", large, largeOps)
	t.Logf("per-row n=%d: %v", large, perRowOps)
	for _, op := range []string{"update", "delete", "select", "other"} {
		require.Equal(t, smallOps[op], largeOps[op],
			"%s statements grew with the batch", op)
	}
	require.Equal(
		t,
		float64(large-small),
		largeOps["insert"]-smallOps["insert"],
		"inserts grew by more than one transaction row per transaction",
	)
}

// TestBatchUtxoOverlayReadBarrier reads, inside the batch, rows the overlay
// has not written yet: an output it created, the spend of a pre-batch
// output, and, once the block has ended, the live-stake total both change.
// Each read must see what the per-row path would have written by then.
func TestBatchUtxoOverlayReadBarrier(t *testing.T) {
	t.Parallel()
	chain := buildOverlayChain(t, 2)
	store := newMigratedSQLiteStore(t)
	seedOverlayChain(t, store, chain)
	txn := store.Transaction(context.Background())
	acc := store.NewBatchAccumulator()
	enabled, err := store.EnableBatchUtxoOverlay(acc, txn, chain.inputs, nil)
	require.NoError(t, err)
	require.True(t, enabled)
	tx := chain.batch[0]
	require.NoError(t, store.SetTransactionBatched(
		tx, ocommon.Point{Slot: 1_000, Hash: tx.Hash().Bytes()},
		0, nil, true, acc, txn, 0,
	))

	created, err := store.GetUtxoIncludingSpent(
		overlayTxID(0x02, 0), 0, txn,
	)
	require.NoError(t, err)
	require.NotNil(t, created, "a reader missed an output the batch created")
	spent, err := store.GetUtxoIncludingSpent(overlayTxID(0x01, 0), 0, txn)
	require.NoError(t, err)
	require.NotNil(t, spent)
	require.Equal(t, uint64(1_000), spent.DeletedSlot,
		"a reader missed a spend the batch made")
	require.NoError(t, store.FlushBatchStakeDeltas(acc, txn))
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	var stake string
	require.NoError(t, db.QueryRowContext(
		ctx,
		`SELECT utxo_stake FROM reward_live_stake WHERE staking_key = ?`,
		chain.stake.Key,
	).Scan(&stake))
	// Seeded 2 x 2 ADA; the block spent 2 and paid 1 + 1.5.
	require.Equal(t, "4500000", stake,
		"live stake read inside the batch is stale")
	require.NoError(t, store.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())
}

// The overlay keeps no undo log. Rolling back to a savepoint across
// overlay changes must fail the batch, not continue from wrong state, and a
// read barrier's write is such a change: SQLite undoes the rows it wrote,
// while the overlay would still count them as written.
func TestBatchUtxoOverlaySavepointRollbackFailsClosed(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// staged is applied before the savepoint, changed after it.
		staged, changed func(*testing.T, *Store, overlayChain,
			types.MetadataBatchAccumulator, types.Txn)
	}{
		{
			name: "staged_write",
			changed: func(t *testing.T, store *Store, chain overlayChain,
				acc types.MetadataBatchAccumulator, txn types.Txn,
			) {
				tx := chain.batch[0]
				require.NoError(t, store.SetTransactionBatched(
					tx, ocommon.Point{Slot: 1_000, Hash: tx.Hash().Bytes()},
					0, nil, true, acc, txn, 0,
				))
			},
		},
		{
			name: "barrier_write",
			staged: func(t *testing.T, store *Store, chain overlayChain,
				acc types.MetadataBatchAccumulator, txn types.Txn,
			) {
				tx := chain.batch[0]
				require.NoError(t, store.SetTransactionBatched(
					tx, ocommon.Point{Slot: 1_000, Hash: tx.Hash().Bytes()},
					0, nil, true, acc, txn, 0,
				))
			},
			changed: func(t *testing.T, store *Store, _ overlayChain,
				_ types.MetadataBatchAccumulator, txn types.Txn,
			) {
				created, err := store.GetUtxoIncludingSpent(
					overlayTxID(0x02, 0), 0, txn,
				)
				require.NoError(t, err)
				require.NotNil(t, created, "the read must reach the barrier")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chain := buildOverlayChain(t, 2)
			store := newMigratedSQLiteStore(t)
			seedOverlayChain(t, store, chain)
			txn := store.Transaction(context.Background())
			defer func() { _ = txn.Rollback() }()
			acc := store.NewBatchAccumulator()
			enabled, err := store.EnableBatchUtxoOverlay(
				acc, txn, chain.inputs, nil,
			)
			require.NoError(t, err)
			require.True(t, enabled)
			if tc.staged != nil {
				tc.staged(t, store, chain, acc, txn)
			}
			savepointer := txn.(*sqlTxn)
			require.NoError(t, savepointer.SavePoint("overlay_test"))
			tc.changed(t, store, chain, acc, txn)
			require.NoError(t, savepointer.RollbackTo("overlay_test"))
			require.ErrorIs(t,
				store.FlushBatch(acc, txn), errOverlaySavepoint,
			)
		})
	}
}

// An output row that already exists makes the overlay's insert fail with
// ErrBatchUtxoOverlayConflict, which the ledger answers by applying the batch
// on the per-row path.
func TestBatchUtxoOverlayReportsConflict(t *testing.T) {
	t.Parallel()
	chain := buildOverlayChain(t, 2)
	store := newMigratedSQLiteStore(t)
	seedOverlayChain(t, store, chain)
	_, err := store.writeDB.ExecContext(context.Background(),
		`INSERT INTO utxo (tx_id, output_idx, credential_tag, added_slot,
		   deleted_slot, amount, payment_script)
		 VALUES (?, 0, 0, 5, 0, '1', 0)`, overlayTxID(0x02, 1))
	require.NoError(t, err)
	txn := store.Transaction(context.Background())
	defer func() { _ = txn.Rollback() }()
	acc := store.NewBatchAccumulator()
	enabled, err := store.EnableBatchUtxoOverlay(acc, txn, chain.inputs, nil)
	require.NoError(t, err)
	require.True(t, enabled)
	for i, tx := range chain.batch {
		require.NoError(t, store.SetTransactionBatched(
			tx, ocommon.Point{Slot: 1_000 + uint64(i), Hash: tx.Hash().Bytes()},
			0, nil, true, acc, txn, 0,
		))
	}
	require.ErrorIs(t,
		store.FlushBatch(acc, txn), types.ErrBatchUtxoOverlayConflict,
	)
}

func TestReadsOverlayTables(t *testing.T) {
	t.Parallel()
	for query, want := range map[string]bool{
		"SELECT amount FROM utxo WHERE id = ?":                     true,
		`UPDATE "utxo" SET deleted_slot = 1`:                       true,
		"SELECT * FROM utxo_pointer":                               true,
		"SELECT utxo_stake FROM reward_live_stake":                 true,
		"SELECT * FROM utxo_address WHERE utxo_id = ?":             false,
		"SELECT value FROM sync_state WHERE sync_key = ?":          false,
		"SELECT 'consumed_utxo_prune_floor'":                       false,
		`INSERT INTO "transaction" (hash) VALUES (?) RETURNING id`: false,
		"SELECT * FROM utxo_reference_input":                       false,
	} {
		require.Equal(t, want, readsOverlayTables(query), query)
	}
}

// TestBatchUtxoOverlayStatementPlans pins the plans of the overlay's bulk
// statements against a skewed utxo table: the prefetch probes
// tx_id_output_idx once per key, and the spend mark and the delete find rows
// by primary key. No plan may use a deleted_slot index or scan utxo, with no
// statistics, with statistics, and with the catch-up indexes dropped.
func TestBatchUtxoOverlayStatementPlans(t *testing.T) {
	skipPlanTestUnderRace(t)
	// Not t.Parallel: the stages rewrite this store's statistics and
	// indexes in order.
	store := newMigratedSQLiteStore(t)
	seedSkewedUtxos(t, store, 50_000)
	ctx := context.Background()
	conn, err := store.writeDB.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	const keys = 5
	prefetchArgs := make([]any, 0, 2*keys)
	markArgs := make([]any, 0, 3*keys)
	deleteArgs := make([]any, 0, keys)
	for i := range keys {
		prefetchArgs = append(prefetchArgs, skewedUtxoTxID(i*7), i%4)
		markArgs = append(markArgs, int64(i+1), int64(9), make([]byte, 32))
		deleteArgs = append(deleteArgs, int64(i+1))
	}
	deleteQuery := `DELETE FROM utxo WHERE id IN (?, ?, ?, ?, ?)`

	stages := []struct {
		name  string
		setup func()
	}{
		{"no_stats", func() {}},
		{"analyzed", func() {
			_, err := conn.ExecContext(ctx, "ANALYZE")
			require.NoError(t, err)
		}},
		{"catchup_indexes_dropped", func() {
			require.NoError(t, store.DropCatchUpIndexesContext(ctx))
		}},
	}
	for _, stage := range stages {
		stage.setup()
		prefetch := connQueryPlan(
			t, conn, overlayPrefetchQuery(keys), prefetchArgs...,
		)
		require.Contains(t, prefetch,
			"SEARCH u USING INDEX tx_id_output_idx (tx_id=? AND output_idx=?)",
			stage.name+": "+prefetch)
		mark := connQueryPlan(t, conn, overlayMarkQuery(keys), markArgs...)
		require.Contains(t, mark, "USING INTEGER PRIMARY KEY (rowid=?)",
			stage.name+": "+mark)
		del := connQueryPlan(t, conn, deleteQuery, deleteArgs...)
		require.Contains(t, del, "USING INTEGER PRIMARY KEY (rowid=?)",
			stage.name+": "+del)
		for name, plan := range map[string]string{
			"prefetch": prefetch, "mark": mark, "delete": del,
		} {
			t.Logf("%s/%s:\n%s", stage.name, name, plan)
			require.NotContains(t, plan, "deleted",
				stage.name+"/"+name+": "+plan)
			require.NotContains(t, plan, "SCAN utxo",
				stage.name+"/"+name+": "+plan)
			require.NotContains(t, plan, "SCAN u ",
				stage.name+"/"+name+": "+plan)
		}
	}
}

// BenchmarkBatchUtxoOverlay applies the same chain through the per-row path
// and the overlay at the store layer, without the ledger around it.
func BenchmarkBatchUtxoOverlay(b *testing.B) {
	const n = 400
	chain := buildOverlayChain(b, n)
	for _, overlay := range []bool{false, true} {
		name := "per_row"
		if overlay {
			name = "overlay"
		}
		b.Run(name, func(b *testing.B) {
			for range b.N {
				b.StopTimer()
				store := newMigratedSQLiteStore(b)
				seedOverlayChain(b, store, chain)
				b.StartTimer()
				applyOverlayChain(b, store, chain, overlay)
			}
			b.ReportMetric(
				float64(
					b.Elapsed().Nanoseconds(),
				)/float64(
					b.N*len(chain.batch),
				),
				"ns/tx",
			)
		})
	}
}

// Only a UNIQUE failure on a table the overlay writes under its own ids is
// a fallback conflict; the per-row path would fail on any other the same
// way.
func TestOverlayWriteErrorClassifiesOwnTables(t *testing.T) {
	t.Parallel()
	for msg, want := range map[string]bool{
		"UNIQUE constraint failed: utxo.tx_id, utxo.output_idx (2067)": true,
		"UNIQUE constraint failed: utxo.id (1555)":                     true,
		"UNIQUE constraint failed: asset.id (1555)":                    true,
		"UNIQUE constraint failed: reward_live_stake.id (1555)":        true,
		"UNIQUE constraint failed: redeemer_script.transaction_id":     false,
		"UNIQUE constraint failed: utxo_address.utxo_id":               false,
		"database is locked": false,
	} {
		err := overlayWriteError("write", errors.New(msg))
		require.Equal(t, want,
			errors.Is(err, types.ErrBatchUtxoOverlayConflict), msg)
	}
}

// Exact live-stake ids rest on no account row being deleted inside the
// batch, so such a delete must fail it rather than pass silently.
func TestBatchUtxoOverlayRefusesAccountDelete(t *testing.T) {
	t.Parallel()
	for query, want := range map[string]bool{
		"DELETE FROM account WHERE credential_tag = ?":           true,
		"-- name: X\nDELETE FROM `account` WHERE id = ?":         true,
		"DELETE FROM account;":                                   true,
		"DELETE FROM `account`;":                                 true,
		"DELETE FROM account_reward_delta WHERE added_slot > ?":  false,
		"DELETE FROM x WHERE id IN (SELECT id FROM account)":     false,
		"SELECT created_slot FROM account WHERE staking_key = ?": false,
	} {
		require.Equal(t, want, deletesAccountRows(query), query)
	}

	chain := buildOverlayChain(t, 2)
	store := newMigratedSQLiteStore(t)
	seedOverlayChain(t, store, chain)
	txn := store.Transaction(context.Background())
	defer func() { _ = txn.Rollback() }()
	acc := store.NewBatchAccumulator()
	enabled, err := store.EnableBatchUtxoOverlay(acc, txn, chain.inputs, nil)
	require.NoError(t, err)
	require.True(t, enabled)
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		"DELETE FROM account WHERE credential_tag = ? AND staking_key = ?",
		chain.stake.Tag, chain.stake.Key,
	)
	require.ErrorIs(t, err, errOverlayAccountDelete)
	require.ErrorIs(t, store.FlushBatch(acc, txn), errOverlayAccountDelete)
}
