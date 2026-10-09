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

	"github.com/blinklabs-io/dingo/database/models"
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

func TestAddressTransactionIndexUsesOnlyRequestedOutputFromProducer(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	producerID := make([]byte, 32)
	producerID[0] = 0xaa
	unrequestedPaymentKey := make([]byte, 28)
	unrequestedPaymentKey[0] = 0x11
	requestedPaymentKey := make([]byte, 28)
	requestedPaymentKey[0] = 0x22
	for outputIndex, paymentKey := range [][]byte{
		unrequestedPaymentKey,
		requestedPaymentKey,
	} {
		_, err := store.writeDB.Exec(`
INSERT INTO utxo (
    tx_id, output_idx, payment_key, credential_tag, added_slot,
    deleted_slot, amount, payment_script
) VALUES (?, ?, ?, 0, 1, 0, '1000000', FALSE)`,
			producerID, outputIndex, paymentKey,
		)
		require.NoError(t, err)
	}
	input, err := mockledger.NewSimpleTransactionInput(producerID, 1)
	require.NoError(t, err)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	transactionID := make([]byte, 32)
	transactionID[0] = 0xbb
	transaction, err := mockledger.NewTransactionBuilder().
		WithId(transactionID).
		WithInputs(input).
		WithOutputs(output).
		WithValid(true).
		Build()
	require.NoError(t, err)

	txn := store.Transaction(context.Background())
	defer txn.Rollback()
	require.NoError(t, store.SetTransaction(
		transaction,
		ocommon.Point{Slot: 2, Hash: transactionID},
		0,
		nil,
		true,
		txn,
	))
	require.NoError(t, txn.Commit())

	rows, err := store.writeDB.Query(`
SELECT a.payment_key
FROM address_transaction AS a
JOIN "transaction" AS t ON t.id = a.transaction_id
WHERE t.hash = ?`,
		transactionID,
	)
	require.NoError(t, err)
	defer rows.Close()
	var got [][]byte
	for rows.Next() {
		var paymentKey []byte
		require.NoError(t, rows.Scan(&paymentKey))
		got = append(got, paymentKey)
	}
	require.NoError(t, rows.Err())
	require.Contains(t, got, requestedPaymentKey)
	require.NotContains(t, got, unrequestedPaymentKey)
}

func assetOutputTx(
	t *testing.T,
	seed byte,
) (lcommon.Transaction, ocommon.Point, []byte, []byte) {
	t.Helper()
	fx := buildSharedCredentialTx(t, seed)
	policyID := make([]byte, 28)
	policyID[0] = seed
	assetName := []byte{seed, 0x42}
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(fx.tx.Outputs()[0].Address().String()).
		WithLovelace(3_000_000).
		WithAssets(mockledger.Asset{
			PolicyId:  policyID,
			AssetName: assetName,
			Amount:    7,
		}).
		Build()
	require.NoError(t, err)
	txID := make([]byte, 32)
	txID[0] = seed
	txID[1] = 0xdd
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(txID)
	tx.WithOutputs(output)
	tx.WithValid(true)
	return tx, ocommon.Point{Slot: 400 + uint64(seed), Hash: txID}, policyID, assetName
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

func TestBatchedProducedAssetRowsWaitForFlush(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewBatchAccumulator()
	defer acc.Reset()

	type assetRef struct {
		policyID []byte
		name     []byte
	}
	assets := make([]assetRef, 0, 3)
	for seed := byte(1); seed <= 3; seed++ {
		tx, point, policyID, name := assetOutputTx(t, seed)
		require.NoError(t, store.SetTransactionBatchedHistorical(
			tx, point, 0, nil, true, true, acc, txn,
		))
		assets = append(assets, assetRef{policyID: policyID, name: name})
	}
	require.Zero(
		t,
		tableCounts(t, store, txn, "asset")["asset"],
		"new UTxO asset rows must remain staged with the transaction batch",
	)
	require.NoError(t, store.FlushBatch(acc, txn))
	require.Equal(t, 3, tableCounts(t, store, txn, "asset")["asset"])
	for _, ref := range assets {
		stored, err := store.GetAssetByPolicyAndName(
			lcommon.NewBlake2b224(ref.policyID), ref.name, txn,
		)
		require.NoError(t, err)
		require.NotZero(t, stored.ID)
		require.Equal(t, types.Uint64(7), stored.Amount)
	}
}

func TestBatchedStakeDeltasCoalesceUntilFlush(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	credential := models.NewStakeCredentialRef(0, []byte("stake-key"))
	for i, row := range []struct {
		amount      string
		deletedSlot int64
	}{
		{amount: "5000000"},
		{amount: "3000000"},
		{amount: "2000000"},
		{amount: "2000000", deletedSlot: 200},
	} {
		txID := make([]byte, 32)
		txID[0] = byte(i + 1)
		_, err := store.writeDB.ExecContext(t.Context(), `
INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, added_slot, deleted_slot, amount)
VALUES (?, 0, ?, ?, 1, ?, ?)`,
			txID, credential.Key, int64(credential.Tag), row.deletedSlot, row.amount,
		)
		require.NoError(t, err)
	}
	_, err := store.writeDB.ExecContext(t.Context(), `
INSERT INTO reward_live_stake (
    credential_tag, staking_key, utxo_stake, reward_stake, total_stake,
    registered, updated_slot, calculation_version
) VALUES (?, ?, '5000000', '0', '5000000', FALSE, 50, ?)`,
		int64(credential.Tag), credential.Key, models.RewardStakeCalculationVersion,
	)
	require.NoError(t, err)

	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc, ok := store.NewBatchAccumulator().(*transactionBatchAccumulator)
	require.True(t, ok)
	defer acc.Reset()
	require.NoError(t, acc.addStakeDeltas(
		[]stakeCredentialDelta{{ref: credential, delta: 3_000_000}}, 100,
	))
	require.NoError(t, acc.addStakeDeltas(
		[]stakeCredentialDelta{
			{ref: credential, delta: 2_000_000},
			{ref: credential, delta: -2_000_000},
		},
		200,
	))
	require.Len(t, acc.stakeDeltas, 1)
	require.Equal(t, int64(3_000_000), acc.stakeDeltas[credential.MapKey()].delta)

	require.NoError(t, store.FlushBatch(acc, txn))
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	var utxoStake string
	var updatedSlot int64
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT utxo_stake, updated_slot FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
		int64(credential.Tag), credential.Key,
	).Scan(&utxoStake, &updatedSlot))
	require.Equal(t, "8000000", utxoStake)
	require.Equal(t, int64(200), updatedSlot)
}

func TestSetTransactionBatchedCoalescesStakeDeltas(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	acc := store.NewBatchAccumulator()
	defer acc.Reset()

	stakingKey := lcommon.NewBlake2b224([]byte("stake-key"))
	paymentKey := lcommon.NewBlake2b224([]byte("payment-key"))
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		paymentKey.Bytes(),
		stakingKey.Bytes(),
	)
	require.NoError(t, err)
	credential := models.NewStakeCredentialRef(0, stakingKey.Bytes())
	_, err = store.writeDB.ExecContext(t.Context(), `
INSERT INTO reward_live_stake (
    credential_tag, staking_key, utxo_stake, reward_stake, total_stake,
    registered, updated_slot, calculation_version
) VALUES (?, ?, '0', '0', '0', FALSE, 0, ?)`,
		int64(credential.Tag), credential.Key, models.RewardStakeCalculationVersion,
	)
	require.NoError(t, err)
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })

	for i, amount := range []uint64{3_000_000, 2_000_000} {
		txID := make([]byte, 32)
		txID[0] = byte(i + 1)
		output, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(address.String()).
			WithLovelace(amount).
			Build()
		require.NoError(t, err)
		tx := mockledger.NewTransactionBuilder()
		tx.WithId(txID)
		tx.WithOutputs(output)
		tx.WithValid(true)
		require.NoError(t, store.SetTransactionBatched(
			tx,
			ocommon.Point{Slot: uint64(100 + i), Hash: txID},
			0,
			nil,
			true,
			acc,
			txn,
		))
	}

	batched, ok := acc.(*transactionBatchAccumulator)
	require.True(t, ok)
	require.Len(t, batched.stakeDeltas, 1)
	require.Equal(t, int64(5_000_000), batched.stakeDeltas[credential.MapKey()].delta)
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	var utxoStake string
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT utxo_stake FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
		int64(credential.Tag), credential.Key,
	).Scan(&utxoStake))
	require.Equal(t, "0", utxoStake)

	require.NoError(t, store.FlushBatchStakeDeltas(acc, txn))
	require.Empty(t, batched.stakeDeltas)
	require.False(t, batched.rows.empty())
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT utxo_stake FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
		int64(credential.Tag), credential.Key,
	).Scan(&utxoStake))
	require.Equal(t, "5000000", utxoStake)

	require.NoError(t, store.FlushBatch(acc, txn))
	var updatedSlot int64
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT utxo_stake, updated_slot FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
		int64(credential.Tag), credential.Key,
	).Scan(&utxoStake, &updatedSlot))
	require.Equal(t, "5000000", utxoStake)
	require.Equal(t, int64(101), updatedSlot)
}

func TestFlushBatchAppliesCoalescedStakeDeltas(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	acc := store.NewBatchAccumulator()
	defer acc.Reset()
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })

	stakingKey := lcommon.NewBlake2b224([]byte("batch-stake-key"))
	paymentKey := lcommon.NewBlake2b224([]byte("batch-payment-key"))
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		paymentKey.Bytes(),
		stakingKey.Bytes(),
	)
	require.NoError(t, err)
	credential := models.NewStakeCredentialRef(0, stakingKey.Bytes())

	for i, amount := range []uint64{3_000_000, 2_000_000} {
		txID := make([]byte, 32)
		txID[0] = byte(i + 1)
		output, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(address.String()).
			WithLovelace(amount).
			Build()
		require.NoError(t, err)
		tx := mockledger.NewTransactionBuilder()
		tx.WithId(txID)
		tx.WithOutputs(output)
		tx.WithValid(true)
		require.NoError(t, store.SetTransactionBatched(
			tx,
			ocommon.Point{Slot: uint64(100 + i), Hash: txID},
			0,
			nil,
			true,
			acc,
			txn,
		))
	}
	batched, ok := acc.(*transactionBatchAccumulator)
	require.True(t, ok)
	require.Len(t, batched.stakeDeltas, 1)
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	var rowCount int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM reward_live_stake`,
	).Scan(&rowCount))
	require.Zero(t, rowCount)

	require.NoError(t, store.FlushBatch(acc, txn))
	var utxoStake string
	var updatedSlot int64
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT utxo_stake, updated_slot FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
		int64(credential.Tag), credential.Key,
	).Scan(&utxoStake, &updatedSlot))
	require.Equal(t, "5000000", utxoStake)
	require.Equal(t, int64(101), updatedSlot)
}

func TestBatchedStakeDeltasRestoreOnSavepointRollback(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	acc, ok := store.NewBatchAccumulator().(*transactionBatchAccumulator)
	require.True(t, ok)
	defer acc.Reset()
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	sqlTxn := txn.(*sqlTxn)
	credential := models.NewStakeCredentialRef(0, []byte("stake-key"))
	require.NoError(t, acc.addStakeDeltas(
		[]stakeCredentialDelta{{ref: credential, delta: 3_000_000}}, 100,
	))
	require.NoError(t, sqlTxn.bindBatch(acc))
	require.NoError(t, sqlTxn.SavePoint("stake_checkpoint"))
	require.NoError(t, acc.addStakeDeltas(
		[]stakeCredentialDelta{{ref: credential, delta: -2_000_000}}, 200,
	))
	require.NoError(t, sqlTxn.RollbackTo("stake_checkpoint"))
	require.Equal(
		t,
		pendingStakeCredentialDelta{ref: credential, delta: 3_000_000, slot: 100},
		acc.stakeDeltas[credential.MapKey()],
	)
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

	parameterLimit := store.dialect.ParameterLimit()
	rowsPerStatement := parameterLimit / len(vkeyWitnessShape.columns)
	require.Positive(t, rowsPerStatement)
	rowCount := rowsPerStatement + 1
	var rows rowBatch
	for i := range rowCount {
		rows.add(
			vkeyWitnessShape,
			[]byte(fmt.Sprintf("vkey-%d", i)),
			[]byte(fmt.Sprintf("signature-%d", i)),
			transactionID, 0,
		)
	}
	insertsBefore := counterValue(t, reg, "insert")
	require.NoError(t, rows.flush(ctx, db, parameterLimit))
	require.Equal(t, float64(2), counterValue(t, reg, "insert")-insertsBefore)
	require.Equal(t, rowCount, keyWitnessCount(t, store, txn))
	require.True(t, rows.empty())
}

func TestRowBatchFlushAssetMintBurnKeepsUniqueEvents(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)

	txHash := make([]byte, 32)
	policyID := make([]byte, 28)
	rows := rowBatch{}
	rows.add(assetMintBurnShape, txHash, policyID, []byte("first"), []byte("fp1"), uint64(100), "5", uint32(0))
	rows.add(assetMintBurnShape, txHash, policyID, []byte("second"), []byte("fp2"), uint64(100), "-2", uint32(0))
	rows.add(assetMintBurnShape, txHash, policyID, []byte("first"), []byte("fp1"), uint64(100), "5", uint32(0))

	require.NoError(t, rows.flush(ctx, db, store.dialect.ParameterLimit()))
	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM asset_mint_burn`,
	).Scan(&count))
	require.Equal(t, 2, count)
	var quantity string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT quantity FROM asset_mint_burn WHERE name = ?`, []byte("second"),
	).Scan(&quantity))
	require.Equal(t, "-2", quantity)
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
// rendering of every multi-row shape, including placeholders, quoted
// identifiers, and conflict-clause rewrites.
func TestRowBatchFlushTranslatesForProviders(t *testing.T) {
	t.Parallel()
	var rows rowBatch
	for i := range 2 {
		seed := byte(i + 1)
		rows.add(
			vkeyWitnessShape,
			[]byte{seed, 0x01}, []byte{seed, 0x02}, int64(seed), 0,
		)
		rows.add(
			bootstrapWitnessShape,
			[]byte{seed, 0x03}, []byte{seed, 0x04}, []byte{seed, 0x05},
			[]byte{seed, 0x06}, int64(seed), 1,
		)
		rows.add(witnessScriptShape, []byte{seed, 0x07}, int64(seed), 1)
		rows.add(plutusDataShape, []byte{seed, 0x08}, int64(seed))
		rows.add(redeemerShape, []byte{seed, 0x01}, int64(seed), 1, 2, int64(i), 0)
		rows.add(
			addressTransactionShape,
			[]byte{seed, 0x09}, []byte{seed, 0x0a}, 1,
			int64(seed), 300, int64(i),
		)
		rows.add(metadataLabelShape, int64(7), i+1, 300, []byte{seed, 0x02}, nil)
		rows.add(datumShape, []byte{seed, 0x03}, []byte{seed, 0x04}, 300)
	}
	want := map[string][]string{
		"postgres": {
			"INSERT INTO key_witness (vkey, signature, transaction_id, type) " +
				"VALUES ($1, $2, $3, $4), ($5, $6, $7, $8)",
			"INSERT INTO key_witness (signature, public_key, chain_code, " +
				"attributes, transaction_id, type) " +
				"VALUES ($1, $2, $3, $4, $5, $6), ($7, $8, $9, $10, $11, $12)",
			"INSERT INTO witness_scripts (script_hash, transaction_id, type) " +
				"VALUES ($1, $2, $3), ($4, $5, $6)",
			"INSERT INTO plutus_data (data, transaction_id) " +
				"VALUES ($1, $2), ($3, $4)",
			"INSERT INTO redeemer (data, transaction_id, ex_units_memory, " +
				"ex_units_cpu, \"index\", tag) " +
				"VALUES ($1, $2, $3, $4, $5, $6), ($7, $8, $9, $10, $11, $12)",
			"INSERT INTO address_transaction (payment_key, staking_key, " +
				"credential_tag, transaction_id, slot, tx_index) " +
				"VALUES ($1, $2, $3, $4, $5, $6), ($7, $8, $9, $10, $11, $12)",
			"INSERT INTO transaction_metadata_label (transaction_id, label, " +
				"slot, cbor_value, json_value) " +
				"VALUES ($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10)\n" +
				metadataLabelShape.suffix,
			"INSERT INTO datum (hash, raw_datum, added_slot) " +
				"VALUES ($1, $2, $3), ($4, $5, $6)\n" +
				"ON CONFLICT (hash) DO NOTHING",
		},
		"mysql": {
			"INSERT INTO key_witness (vkey, signature, transaction_id, type) " +
				"VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
			"INSERT INTO key_witness (signature, public_key, chain_code, " +
				"attributes, transaction_id, type) " +
				"VALUES (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)",
			"INSERT INTO witness_scripts (script_hash, transaction_id, type) " +
				"VALUES (?, ?, ?), (?, ?, ?)",
			"INSERT INTO plutus_data (data, transaction_id) " +
				"VALUES (?, ?), (?, ?)",
			"INSERT INTO redeemer (data, transaction_id, ex_units_memory, " +
				"ex_units_cpu, `index`, tag) " +
				"VALUES (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)",
			"INSERT INTO address_transaction (payment_key, staking_key, " +
				"credential_tag, transaction_id, slot, tx_index) " +
				"VALUES (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)",
			"INSERT INTO transaction_metadata_label (transaction_id, label, " +
				"slot, cbor_value, json_value) " +
				"VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)\n" +
				"ON DUPLICATE KEY UPDATE slot = VALUES(slot),\n" +
				"    cbor_value = VALUES(cbor_value),\n" +
				"    json_value = VALUES(json_value)",
			"INSERT INTO datum (hash, raw_datum, added_slot) " +
				"VALUES (?, ?, ?), (?, ?, ?)\n" +
				"ON DUPLICATE KEY UPDATE hash = hash",
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
