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
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/labelcodec"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func newAPIModeSQLiteStore(
	t *testing.T,
	reg *prometheus.Registry,
) *Store {
	t.Helper()
	db, err := OpenDB(
		"sqlite",
		fmt.Sprintf(
			"file:witness_batch_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
		"sqlite",
		false,
	)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	cfg := Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		StorageMode:     types.StorageModeAPI,
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	}
	if reg != nil {
		cfg.PromRegistry = reg
	}
	store, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func witnessTx(
	t *testing.T,
	seed byte,
	vkeys int,
) (lcommon.Transaction, ocommon.Point) {
	t.Helper()
	txID := make([]byte, 32)
	txID[0] = seed
	witnesses := make([]lcommon.VkeyWitness, 0, vkeys)
	for i := range vkeys {
		witnesses = append(witnesses, lcommon.VkeyWitness{
			Vkey:      []byte{seed, byte(i), 0x01},
			Signature: []byte{seed, byte(i), 0x02},
		})
	}
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(txID)
	tx.WithValid(true)
	tx.WithWitnesses(
		mockledger.NewMockTransactionWitnessSet().
			WithVkeyWitnesses(witnesses...),
	)
	return tx, ocommon.Point{Slot: 100 + uint64(seed), Hash: txID}
}

func keyWitnessCount(
	t *testing.T,
	store *Store,
	txn types.Txn,
) int {
	t.Helper()
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	var count int
	require.NoError(t, db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM key_witness`,
	).Scan(&count))
	return count
}

// TestBatchedWitnessRowsAreFlushedAsOneMultiRowInsert checks that witness
// rows of every transaction in a batch window are held by the accumulator and
// written by FlushBatch with a single INSERT, instead of one INSERT per row
// at SetTransactionBatched time.
func TestBatchedWitnessRowsAreFlushedAsOneMultiRowInsert(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := newAPIModeSQLiteStore(t, reg)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewBatchAccumulator()
	defer acc.Reset()

	for seed := byte(1); seed <= 3; seed++ {
		tx, point := witnessTx(t, seed, 2)
		require.NoError(t, store.SetTransactionBatchedHistorical(
			tx, point, 0, nil, true, true, acc, txn,
		))
	}
	require.Zero(
		t,
		keyWitnessCount(t, store, txn),
		"witness rows must wait in the accumulator until FlushBatch",
	)

	insertsBefore := counterValue(t, reg, "insert")
	require.NoError(t, store.FlushBatch(acc, txn))
	require.Equal(
		t,
		float64(1),
		counterValue(t, reg, "insert")-insertsBefore,
		"FlushBatch must write all queued witness rows with one INSERT",
	)
	require.Equal(t, 6, keyWitnessCount(t, store, txn))
}

// TestBatchedWitnessRowsReplacePendingRowsOfSameTransaction pins the
// replace-wholesale semantics of the immediate path: applying a transaction
// again inside one window must not leave its earlier queued rows behind.
func TestBatchedWitnessRowsReplacePendingRowsOfSameTransaction(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewBatchAccumulator()
	defer acc.Reset()

	for range 2 {
		tx, point := witnessTx(t, 7, 2)
		require.NoError(t, store.SetTransactionBatchedHistorical(
			tx, point, 0, nil, true, true, acc, txn,
		))
	}
	require.NoError(t, store.FlushBatch(acc, txn))
	require.Equal(t, 2, keyWitnessCount(t, store, txn))
}

// TestBatchedWitnessRowsReplaceFlushedRowsOfSameTransaction covers the same
// transaction being re-applied in a later window, after its rows were
// flushed.
func TestBatchedWitnessRowsReplaceFlushedRowsOfSameTransaction(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewBatchAccumulator()
	defer acc.Reset()

	for range 2 {
		tx, point := witnessTx(t, 9, 3)
		require.NoError(t, store.SetTransactionBatchedHistorical(
			tx, point, 0, nil, true, true, acc, txn,
		))
		require.NoError(t, store.FlushBatch(acc, txn))
	}
	require.Equal(t, 3, keyWitnessCount(t, store, txn))
}

// TestBatchAccumulatorResetDropsQueuedWitnessRows checks that a discarded
// window writes nothing when the accumulator is reused.
func TestBatchAccumulatorResetDropsQueuedWitnessRows(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewBatchAccumulator()
	defer acc.Reset()

	tx, point := witnessTx(t, 5, 2)
	require.NoError(t, store.SetTransactionBatchedHistorical(
		tx, point, 0, nil, true, true, acc, txn,
	))
	acc.Reset()
	require.NoError(t, store.FlushBatch(acc, txn))
	require.Zero(t, keyWitnessCount(t, store, txn))
}

// allShapesWitnessTx builds a transaction carrying a row for every witness
// insert shape, so each shape's transaction_id column position is exercised.
func allShapesWitnessTx(
	t *testing.T,
	seed byte,
) (lcommon.Transaction, ocommon.Point) {
	t.Helper()
	txID := make([]byte, 32)
	txID[0] = seed
	var datum lcommon.Datum
	datum.SetCbor([]byte{0x01})
	var redeemerData lcommon.Datum
	redeemerData.SetCbor([]byte{0x02})
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(txID)
	tx.WithValid(true)
	tx.WithWitnesses(
		mockledger.NewMockTransactionWitnessSet().
			WithVkeyWitnesses(lcommon.VkeyWitness{
				Vkey:      []byte{seed, 0x01},
				Signature: []byte{seed, 0x02},
			}).
			WithBootstrapWitnesses(lcommon.BootstrapWitness{
				PublicKey:  []byte{seed, 0x03},
				Signature:  []byte{seed, 0x04},
				ChainCode:  []byte{seed, 0x05},
				Attributes: []byte{0xa0},
			}).
			WithPlutusV1Scripts(lcommon.PlutusV1Script{seed, 0x06}).
			WithPlutusData(datum).
			WithRedeemers(&conway.ConwayRedeemers{
				Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
						Data:    redeemerData,
						ExUnits: lcommon.ExUnits{Memory: 10, Steps: 20},
					},
				},
			}),
	)
	return tx, ocommon.Point{Slot: 200 + uint64(seed), Hash: txID}
}

func witnessTableCounts(
	t *testing.T,
	store *Store,
	txn types.Txn,
) map[string]int {
	t.Helper()
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	counts := make(map[string]int)
	for _, table := range []string{
		"key_witness", "witness_scripts", "plutus_data", "redeemer",
	} {
		var count int
		require.NoError(t, db.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM "+table,
		).Scan(&count))
		counts[table] = count
	}
	return counts
}

// TestBatchedWitnessRowsReplacePendingRowsOfEveryShape re-applies one
// transaction inside a window and checks that each witness table ends with
// the rows of a single application, matching the immediate path.
func TestBatchedWitnessRowsReplacePendingRowsOfEveryShape(t *testing.T) {
	t.Parallel()
	want := map[string]int{
		"key_witness": 2, "witness_scripts": 1, "plutus_data": 1, "redeemer": 1,
	}

	immediate := newAPIModeSQLiteStore(t, nil)
	immediateTxn := immediate.Transaction(context.Background())
	t.Cleanup(func() { _ = immediateTxn.Rollback() })
	tx, point := allShapesWitnessTx(t, 11)
	require.NoError(t, immediate.SetTransaction(
		tx, point, 0, nil, true, immediateTxn,
	))
	require.Equal(t, want, witnessTableCounts(t, immediate, immediateTxn))

	store := newAPIModeSQLiteStore(t, nil)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewBatchAccumulator()
	defer acc.Reset()
	for range 2 {
		tx, point := allShapesWitnessTx(t, 11)
		require.NoError(t, store.SetTransactionBatchedHistorical(
			tx, point, 0, nil, true, true, acc, txn,
		))
	}
	require.NoError(t, store.FlushBatch(acc, txn))
	require.Equal(t, want, witnessTableCounts(t, store, txn))
}

// apiDetailTx builds a transaction with metadata labels, one output carrying
// an inline datum, and one input, so it queues address, label and datum rows
// besides its vkey witness.
func apiDetailTx(t *testing.T, seed byte) (lcommon.Transaction, ocommon.Point) {
	t.Helper()
	fx := buildSharedCredentialTx(t, seed)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(fx.tx.Outputs()[0].Address().String()).
		WithLovelace(fx.producedAmount).
		WithDatum([]byte{0x18, 0x2a}).
		Build()
	require.NoError(t, err)
	txID := make([]byte, 32)
	txID[0] = seed
	txID[1] = 0xcc
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(txID)
	tx.WithInputs(fx.tx.Inputs()...)
	tx.WithOutputs(output)
	tx.WithMetadata([]byte{0xa2, 0x01, 0x61, 0x61, 0x02, 0x05})
	tx.WithValid(true)
	tx.WithWitnesses(
		mockledger.NewMockTransactionWitnessSet().
			WithVkeyWitnesses(lcommon.VkeyWitness{
				Vkey:      []byte{seed, 0x01},
				Signature: []byte{seed, 0x02},
			}),
	)
	return tx, ocommon.Point{Slot: 300 + uint64(seed), Hash: txID}
}

func tableCounts(
	t *testing.T,
	store *Store,
	txn types.Txn,
	tables ...string,
) map[string]int {
	t.Helper()
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	counts := make(map[string]int)
	for _, table := range tables {
		var count int
		require.NoError(t, db.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM "+table,
		).Scan(&count))
		counts[table] = count
	}
	return counts
}

// TestBatchedAPIDetailRowsWaitForFlush checks that address, metadata label
// and datum rows are queued like witness rows, and that re-applying the
// transaction inside the window leaves the rows of one application, as the
// immediate path does.
func TestBatchedAPIDetailRowsWaitForFlush(t *testing.T) {
	t.Parallel()
	tables := []string{
		"address_transaction", "transaction_metadata_label", "datum",
	}

	immediate := newAPIModeSQLiteStore(t, nil)
	immediateTxn := immediate.Transaction(context.Background())
	t.Cleanup(func() { _ = immediateTxn.Rollback() })
	tx, point := apiDetailTx(t, 21)
	require.NoError(t, immediate.SetTransaction(
		tx, point, 0, nil, true, immediateTxn,
	))
	want := tableCounts(t, immediate, immediateTxn, tables...)
	require.Equal(t, map[string]int{
		"address_transaction": 1, "transaction_metadata_label": 2, "datum": 1,
	}, want)

	store := newAPIModeSQLiteStore(t, nil)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewBatchAccumulator()
	defer acc.Reset()
	for range 2 {
		tx, point := apiDetailTx(t, 21)
		require.NoError(t, store.SetTransactionBatchedHistorical(
			tx, point, 0, nil, true, true, acc, txn,
		))
	}
	require.Equal(t, map[string]int{
		"address_transaction": 0, "transaction_metadata_label": 0, "datum": 0,
	}, tableCounts(t, store, txn, tables...))
	require.NoError(t, store.FlushBatch(acc, txn))
	require.Equal(t, want, tableCounts(t, store, txn, tables...))
}

// TestBatchedRowsOfFailedWriteAreNotQueued applies a transaction whose write
// fails after its rows were built. Its local SQL transaction rolls back, so
// none of its rows may reach a later FlushBatch.
func TestBatchedRowsOfFailedWriteAreNotQueued(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	acc := store.NewBatchAccumulator()
	defer acc.Reset()

	tx, point := apiDetailTx(t, 23)
	_, err := store.writeDB.ExecContext(context.Background(), `
INSERT INTO utxo (tx_id, output_idx, added_slot, deleted_slot, spent_at_tx_id, amount)
VALUES (?, 0, 1, 5, ?, '1')`,
		tx.Inputs()[0].Id().Bytes(),
		[]byte{0xee},
	)
	require.NoError(t, err)
	err = store.SetTransactionBatchedHistorical(
		tx, point, 0, nil, true, true, acc, nil,
	)
	require.ErrorIs(t, err, types.ErrUtxoConflict)

	require.NoError(t, store.FlushBatch(acc, nil))
	require.Equal(t, map[string]int{
		"key_witness": 0, "address_transaction": 0,
		"transaction_metadata_label": 0, "datum": 0,
	}, tableCounts(
		t, store, nil,
		"key_witness", "address_transaction",
		"transaction_metadata_label", "datum",
	))
}

// TestRowBatchFlushSplitsAtParameterLimit checks that a flush never binds
// more than the parameter limit in one statement.
func TestRowBatchFlushSplitsAtParameterLimit(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := newAPIModeSQLiteStore(t, reg)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	tx, point := witnessTx(t, 25, 0)
	require.NoError(t, store.SetTransaction(tx, point, 0, nil, true, txn))
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	var transactionID int64
	require.NoError(t, db.QueryRowContext(
		ctx, `SELECT id FROM "transaction"`,
	).Scan(&transactionID))

	var rows rowBatch
	for i := range 5 {
		rows.add(
			vkeyWitnessShape,
			[]byte{byte(i), 0x01}, []byte{byte(i), 0x02},
			transactionID, 0,
		)
	}
	insertsBefore := counterValue(t, reg, "insert")
	// Four columns per row and a limit of nine parameters: two rows per
	// statement, so five rows take three statements.
	require.NoError(t, rows.flush(ctx, db, 9))
	require.Equal(t, float64(3), counterValue(t, reg, "insert")-insertsBefore)
	require.Equal(t, 5, keyWitnessCount(t, store, txn))
	require.True(t, rows.empty())
}

type recordingQueryer struct {
	queryer
	statements []string
}

func (q *recordingQueryer) ExecContext(
	_ context.Context,
	query string,
	_ ...any,
) (sql.Result, error) {
	q.statements = append(q.statements, query)
	return driver.RowsAffected(0), nil
}

// TestRowBatchFlushTranslatesForProviders pins the PostgreSQL and MySQL
// renderings of the multi-row statements: placeholders numbered across every
// row, reserved identifiers quoted, and each ON CONFLICT clause rewritten.
func TestRowBatchFlushTranslatesForProviders(t *testing.T) {
	t.Parallel()
	var rows rowBatch
	for range 2 {
		rows.add(redeemerShape, []byte{0x01}, int64(7), 1, 2, 0, 0)
		rows.add(metadataLabelShape, int64(7), 1, 300, []byte{0x02}, nil)
		rows.add(datumShape, []byte{0x03}, []byte{0x04}, 300)
	}
	want := map[string][]string{
		"postgres": {
			`INSERT INTO redeemer (data, transaction_id, ex_units_memory, ex_units_cpu, "index", tag) VALUES ($1, $2, $3, $4, $5, $6), ($7, $8, $9, $10, $11, $12)`,
			"INSERT INTO transaction_metadata_label (transaction_id, label, slot, cbor_value, json_value) VALUES ($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10)\n" +
				metadataLabelShape.suffix,
			"INSERT INTO datum (hash, raw_datum, added_slot) VALUES ($1, $2, $3), ($4, $5, $6)\nON CONFLICT (hash) DO NOTHING",
		},
		"mysql": {
			"INSERT INTO redeemer (data, transaction_id, ex_units_memory, ex_units_cpu, `index`, tag) VALUES (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)",
			"INSERT INTO transaction_metadata_label (transaction_id, label, slot, cbor_value, json_value) VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)\n" +
				"ON DUPLICATE KEY UPDATE slot = VALUES(slot),\n    cbor_value = VALUES(cbor_value),\n    json_value = VALUES(json_value)",
			"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, ?), (?, ?, ?)\nON DUPLICATE KEY UPDATE hash = hash",
		},
	}
	for dialect, statements := range want {
		recorder := &recordingQueryer{}
		batch := rows
		require.NoError(t, batch.flush(
			context.Background(),
			newDialectQueryer(recorder, dialect),
			1000,
		))
		require.Equal(t, statements, recorder.statements, dialect)
	}
}

// TestMetadataLabelRowsKeepLastValueOfRepeatedLabel checks that a label
// repeated in one transaction queues a single row carrying its last value;
// PostgreSQL rejects a multi-row upsert that names one key twice.
func TestMetadataLabelRowsKeepLastValueOfRepeatedLabel(t *testing.T) {
	t.Parallel()
	store := &Store{storageMode: types.StorageModeAPI}
	var rows rowBatch
	store.applyTransactionMetadataLabels(&rows, 7, 300, []labelcodec.Entry{
		{Label: 1, CborValue: []byte{0x01}},
		{Label: 2, CborValue: []byte{0x02}},
		{Label: 1, CborValue: []byte{0x03}},
	})
	require.Len(t, rows.entries, 1)
	got := rows.entries[0].rows
	require.Len(t, got, 2)
	require.Equal(t, []byte{0x02}, got[0][3])
	require.Equal(t, []byte{0x03}, got[1][3])
}

func TestBatchedRowsRestoreQueueOnCallerRollback(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	acc := store.NewBatchAccumulator()
	defer acc.Reset()
	retained, point := witnessTx(t, 7, 2)
	require.NoError(t, store.SetTransactionBatchedHistorical(retained, point, 0, nil, true, true, acc, nil))
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	discarded, point := witnessTx(t, 9, 3)
	require.NoError(t, store.SetTransactionBatchedHistorical(discarded, point, 0, nil, true, true, acc, txn))
	require.NoError(t, txn.Rollback())
	require.NoError(t, store.FlushBatch(acc, nil))
	require.Equal(t, 2, keyWitnessCount(t, store, nil))
}

func TestBatchedRowsRestoreQueueOnSavepointRollback(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	acc := store.NewBatchAccumulator()
	defer acc.Reset()
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	retained, point := witnessTx(t, 7, 2)
	require.NoError(t, store.SetTransactionBatchedHistorical(retained, point, 0, nil, true, true, acc, txn))
	require.NoError(t, txn.(*sqlTxn).SavePoint("retained"))
	discarded, point := witnessTx(t, 9, 3)
	require.NoError(t, store.SetTransactionBatchedHistorical(discarded, point, 0, nil, true, true, acc, txn))
	require.NoError(t, txn.(*sqlTxn).RollbackTo("retained"))
	require.NoError(t, store.FlushBatch(acc, txn))
	require.Equal(t, 2, keyWitnessCount(t, store, txn))
}

func TestBatchedRowsResetOnRollbackBeforeFirstSavepointBinding(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	acc := store.NewBatchAccumulator()
	defer acc.Reset()
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	require.NoError(t, txn.(*sqlTxn).SavePoint("empty"))
	discarded, point := witnessTx(t, 9, 3)
	require.NoError(t, store.SetTransactionBatchedHistorical(discarded, point, 0, nil, true, true, acc, txn))
	require.NoError(t, txn.(*sqlTxn).RollbackTo("empty"))
	require.NoError(t, store.FlushBatch(acc, txn))
	require.Zero(t, keyWitnessCount(t, store, txn))
}
