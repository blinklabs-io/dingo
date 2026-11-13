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
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func producerTestInput(t *testing.T, hash []byte, idx uint32) lcommon.TransactionInput {
	t.Helper()
	in, err := mockledger.NewTransactionInputBuilder().
		WithTxId(hash).WithIndex(idx).Build()
	require.NoError(t, err)
	return in
}

func producerTestOutput(t *testing.T, lovelace uint64) lcommon.TransactionOutput {
	t.Helper()
	out, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp").
		WithLovelace(lovelace).Build()
	require.NoError(t, err)
	return out
}

// TestOutputsResolveToTheRowTheHashPathFinds applies a valid transaction, a
// phase-2 failure with a collateral return, and a spender of both plus an
// imported output, then checks that every reference resolves, through the
// producer and imported indexes, to the row the tx_id hash names, with the
// producing hash unchanged.
func TestOutputsResolveToTheRowTheHashPathFinds(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	valid := bytes32(0x71)
	failed := bytes32(0x72)
	imported := bytes32(0x73)
	spender := bytes32(0x74)

	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: imported, OutputIdx: 0, Amount: types.Uint64(5),
	}}, nil))

	validTx := mockledger.NewTransactionBuilder()
	validTx.WithId(valid)
	validTx.WithInputs(producerTestInput(t, bytes32(0x70), 0))
	validTx.WithOutputs(
		producerTestOutput(t, 2_000_000), producerTestOutput(t, 3_000_000),
	)
	validTx.WithValid(true)
	failedTx := mockledger.NewTransactionBuilder()
	failedTx.WithId(failed)
	failedTx.WithInputs(producerTestInput(t, bytes32(0x70), 1))
	failedTx.WithValid(false)
	spendTx := mockledger.NewTransactionBuilder()
	spendTx.WithId(spender)
	spendTx.WithInputs(
		producerTestInput(t, valid, 0),
		producerTestInput(t, failed, 1),
		producerTestInput(t, imported, 0),
	)
	spendTx.WithOutputs(producerTestOutput(t, 1))
	spendTx.WithValid(true)
	apply := func(i int, tx lcommon.Transaction) {
		require.NoError(t, store.SetTransaction(
			tx, ocommon.Point{Slot: uint64(10 + i), Hash: tx.Hash().Bytes()},
			0, nil, false, nil, 0,
		))
	}
	apply(0, validTx)
	apply(1, failedTx)
	// The mock builder never produces a collateral return, so write the one
	// the failed transaction would leave: index 1, after its one declared
	// output.
	var failedID uint
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT id FROM "transaction" WHERE hash = ?`, failed,
	).Scan(&failedID))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId: failed, OutputIdx: 1, Amount: types.Uint64(1_000_000),
		CollateralReturnForTxID: &failedID,
	}))
	apply(2, spendTx)

	refs := []models.UtxoId{
		{Hash: valid, Idx: 0},
		{Hash: valid, Idx: 1},
		{Hash: failed, Idx: 1},
		{Hash: imported, Idx: 0},
		{Hash: spender, Idx: 0},
	}
	for _, ref := range refs {
		var want int64
		require.NoError(t, store.writeDB.QueryRowContext(ctx,
			`SELECT id FROM utxo WHERE tx_id = (SELECT id FROM "transaction" WHERE hash = ?) AND output_idx = ?`,
			ref.Hash, ref.Idx,
		).Scan(&want), "%x#%d", ref.Hash, ref.Idx)
		got, err := store.GetUtxoIncludingSpent(ref.Hash, ref.Idx, nil)
		require.NoError(t, err)
		require.NotNil(t, got, "%x#%d", ref.Hash, ref.Idx)
		require.Equal(t, uint(want), got.ID, "%x#%d", ref.Hash, ref.Idx)
		require.Equal(t, ref.Hash, got.TxId)
	}
	// The collateral return is found at the index after the outputs and is
	// the failed transaction's own row.
	var returnFor int64
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT collateral_return_for_tx_id FROM utxo WHERE tx_id = (SELECT id FROM "transaction" WHERE hash = ?) AND output_idx = 1`,
		failed,
	).Scan(&returnFor))
	require.NotZero(t, returnFor)

	// Every input the spender names is spent by it, whichever identity the
	// row has.
	for _, ref := range []models.UtxoId{refs[0], refs[2], refs[3]} {
		var spentBy int64
		require.NoError(t, store.writeDB.QueryRow(
			`SELECT spent_at_tx_id FROM utxo WHERE tx_id = (SELECT id FROM "transaction" WHERE hash = ?) AND output_idx = ?`,
			ref.Hash, ref.Idx,
		).Scan(&spentBy), "%x#%d", ref.Hash, ref.Idx)
		var want int64
		require.NoError(t, store.writeDB.QueryRow(
			`SELECT id FROM "transaction" WHERE hash = ?`, spender,
		).Scan(&want))
		require.Equal(t, want, spentBy)
	}

	// No identity exists twice: re-importing the produced outputs and
	// re-applying the producer leave one row per reference.
	require.NoError(t, store.ImportUtxos([]models.Utxo{
		{TxId: valid, OutputIdx: 0, Amount: types.Uint64(1)},
		{TxId: imported, OutputIdx: 0, Amount: types.Uint64(5)},
	}, nil))
	var count int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM utxo WHERE (tx_id = (SELECT id FROM "transaction" WHERE hash = ?) AND output_idx = 0) OR (tx_id = (SELECT id FROM "transaction" WHERE hash = ?) AND output_idx = 0)`,
		valid, imported,
	).Scan(&count))
	require.Equal(t, 2, count, "an import duplicated a produced or imported output")
	require.NoError(t, store.SetTransaction(
		validTx, ocommon.Point{Slot: 10, Hash: validTx.Hash().Bytes()},
		0, nil, false, nil, 0,
	))
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM utxo WHERE tx_id = (SELECT id FROM "transaction" WHERE hash = ?)`, valid,
	).Scan(&count))
	require.Equal(t, 2, count, "re-applying a transaction duplicated its outputs")
}

// TestImportThenApplyKeepsOneRowPerOutput applies a producer over an output an
// import stored first. The import gives the producer an identity row and no
// application row; applying it reuses that row, adds its application row and
// leaves the output one row carrying the producer's id.
func TestImportThenApplyKeepsOneRowPerOutput(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	producer := bytes32(0x81)
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: producer, OutputIdx: 0, Amount: types.Uint64(2_000_000),
	}}, nil))
	var importedID int64
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT id FROM "transaction" WHERE hash = ?`, producer,
	).Scan(&importedID))
	var applied int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM applied_transaction WHERE tx_id = ?`, importedID,
	).Scan(&applied))
	require.Zero(t, applied, "an imported producer has no application row")

	tx := mockledger.NewTransactionBuilder()
	tx.WithId(producer)
	tx.WithInputs(producerTestInput(t, bytes32(0x80), 0))
	tx.WithOutputs(
		producerTestOutput(t, 2_000_000), producerTestOutput(t, 3_000_000),
	)
	tx.WithValid(true)
	require.NoError(t, store.SetTransaction(
		tx, ocommon.Point{Slot: 5, Hash: producer}, 0, nil, false, nil, 0,
	))

	var rows int
	var producerID int64
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*), MIN(tx_id) FROM utxo WHERE tx_id = ?`, importedID,
	).Scan(&rows, &producerID))
	require.Equal(t, 2, rows, "the replayed output must reuse the imported row")
	require.Equal(t, importedID, producerID)
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM applied_transaction WHERE tx_id = ?`, importedID,
	).Scan(&applied))
	require.Equal(t, 1, applied)
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM "transaction" WHERE hash = ?`, producer,
	).Scan(&rows))
	require.Equal(t, 1, rows, "applying must not add a second identity row")
}

// TestImportedProducerOverlayMatchesPerRowPath applies, in a trusted batch,
// the producer of an output an import stored, and optionally a second
// transaction spending that imported output. The import left the producer an
// identity row and no application row. The overlay reuses that row, and
// conflicts only when the producer writes the imported index itself, where
// the per-row path keeps the imported row. Either way the store must end as
// the per-row path alone leaves it.
func TestImportedProducerOverlayMatchesPerRowPath(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		importedIdx uint32
		spend       bool
		conflict    bool
	}{
		"shared_index":       {importedIdx: 1, conflict: true},
		"shared_index_spent": {importedIdx: 1, spend: true, conflict: true},
		"other_index":        {importedIdx: 5},
		"other_index_spent":  {importedIdx: 5, spend: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			producer := bytes32(0x91)
			funding := bytes32(0x90)
			spender := bytes32(0x92)
			tx := mockledger.NewTransactionBuilder()
			tx.WithId(producer)
			tx.WithInputs(producerTestInput(t, funding, 0))
			tx.WithOutputs(
				producerTestOutput(t, 2_000_000),
				producerTestOutput(t, 3_000_000),
			)
			tx.WithValid(true)
			txs := []lcommon.Transaction{tx}
			inputs := []models.UtxoId{{Hash: funding, Idx: 0}}
			if tc.spend {
				spend := mockledger.NewTransactionBuilder()
				spend.WithId(spender)
				spend.WithInputs(
					producerTestInput(t, producer, tc.importedIdx),
				)
				spend.WithOutputs(producerTestOutput(t, 1_000_000))
				spend.WithValid(true)
				txs = append(txs, spend)
				if tc.importedIdx != 1 {
					inputs = append(inputs, models.UtxoId{
						Hash: producer, Idx: tc.importedIdx,
					})
				}
			}
			point := ocommon.Point{Slot: 7, Hash: producer}
			newStore := func() *Store {
				store := newMigratedSQLiteStore(t)
				require.NoError(t, store.ImportUtxos([]models.Utxo{{
					TxId: producer, OutputIdx: tc.importedIdx,
					Amount: types.Uint64(3_000_000),
				}}, nil))
				return store
			}
			apply := func(store *Store, overlay bool) error {
				txn := store.Transaction(context.Background())
				defer func() { _ = txn.Rollback() }()
				acc := store.NewBatchAccumulator()
				if overlay {
					producers := make([][]byte, 0, len(txs))
					for _, tx := range txs {
						producers = append(producers, tx.Hash().Bytes())
					}
					enabled, err := store.EnableBatchUtxoOverlay(
						acc, txn, inputs, nil, producers,
					)
					require.NoError(t, err)
					require.True(t, enabled)
				}
				for i, tx := range txs {
					if err := store.SetTransactionBatched(
						tx, point, uint32(i), nil, true, acc, txn, 0,
					); err != nil {
						return err
					}
				}
				if err := store.FlushBatchStakeDeltas(acc, txn); err != nil {
					return err
				}
				if err := store.FlushBatch(acc, txn); err != nil {
					return err
				}
				return txn.Commit()
			}
			dump := func(store *Store) []string {
				var out []string
				for _, query := range []string{
					`SELECT id, hash FROM "transaction" ORDER BY id`,
					`SELECT tx_id, slot, block_index, valid
					 FROM applied_transaction ORDER BY tx_id`,
					`SELECT name, seq FROM sqlite_sequence ORDER BY name`,
					`SELECT id, tx_id, output_idx, amount, deleted_slot,
					        COALESCE(spent_at_tx_id, 0)
					 FROM utxo ORDER BY id`,
				} {
					rows, err := store.writeDB.Query(query)
					require.NoError(t, err)
					cols, err := rows.Columns()
					require.NoError(t, err)
					for rows.Next() {
						vals := make([]any, len(cols))
						ptrs := make([]any, len(cols))
						for i := range vals {
							ptrs[i] = &vals[i]
						}
						require.NoError(t, rows.Scan(ptrs...))
						out = append(out, fmt.Sprintf("%v", vals))
					}
					require.NoError(t, rows.Err())
					require.NoError(t, rows.Close())
				}
				return out
			}

			perRow := newStore()
			require.NoError(t, apply(perRow, false))

			batched := newStore()
			err := apply(batched, true)
			if tc.conflict {
				require.ErrorIs(t, err, types.ErrBatchUtxoOverlayConflict)
				var count int
				require.NoError(t, batched.writeDB.QueryRow(
					"SELECT COUNT(*) FROM applied_transaction",
				).Scan(&count))
				require.Zero(t, count, "the failed overlay batch was written")
				require.NoError(t, apply(batched, false))
			} else {
				require.NoError(t, err, "the overlay must reuse the imported identity")
			}
			require.Equal(t, dump(perRow), dump(batched))

			var rows int
			require.NoError(t, batched.writeDB.QueryRow(
				`SELECT COUNT(*) FROM "transaction" WHERE hash = ?`, producer,
			).Scan(&rows))
			require.Equal(t, 1, rows, "a second identity row was added")
		})
	}
}

// TestOutputLookupsDoNotScan pins the plans of the reference lookups the apply
// path, rollback and the APIs run: each reaches utxo through the producer and
// imported indexes or the primary key, and reaches transaction through the
// hash index.
func TestOutputLookupsDoNotScan(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedRollbackUtxos(t, store, 20_000, 64, 2_656_808, 40)
	analyzeStore(t, store)
	hash := make([]byte, 32)
	for name, tc := range map[string]struct {
		query string
		args  []any
	}{
		"input_resolution": {
			sqliteUtxoSQL.getLiveByRef, []any{hash, int64(0)},
		},
		"including_spent": {
			sqliteUtxoSQL.getIncludingSpent,
			[]any{hash, int64(0)},
		},
		"spend_one": {
			consumeUtxoSQLiteReturningSQL,
			[]any{int64(5), int64(1), hash, int64(0)},
		},
		"spend_batch": {consumeUtxosBatchQuery(3, true), []any{
			int64(5), int64(1),
			hash, int64(0),
			hash, int64(1),
			hash, int64(2),
		}},
		"spend_state": {sqliteUtxoSQL.getSpendState, []any{hash, int64(0)}},
		"spend_state_hash": {
			sqliteUtxoSQL.getSpendStateHash, []any{hash, int64(0)},
		},
		"by_tx_hash": {
			sqliteRefs.deletionRows[2], []any{hash, hash},
		},
	} {
		plan := explainQueryPlan(t, store, tc.query, tc.args...)
		require.NotContains(t, plan, "SCAN utxo", "%s: %s", name, plan)
		require.NotContains(t, plan, "SCAN transaction", "%s: %s", name, plan)
		require.Contains(t, plan, "idx_utxo_tx_id_output_idx", "%s: %s", name, plan)
		require.Contains(t, plan, "idx_transaction_hash", "%s: %s", name, plan)
	}
}
