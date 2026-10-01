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

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
