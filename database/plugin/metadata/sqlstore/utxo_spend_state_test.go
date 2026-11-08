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
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"

	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func TestUtxoSpendStateKeepsTransactionVisibilityWithoutAssetReads(
	t *testing.T,
) {
	t.Parallel()
	store, recorder := newRecordingSQLiteStore(t)
	seedUtxoLookupRows(t, store, 1)
	id := benchmarkUtxoTxID(0)
	missing, err := store.GetUtxoSpendState(id, 1, nil)
	require.NoError(t, err)
	require.Nil(t, missing)
	mark := len(recorder.snapshot())
	state, err := store.GetUtxoSpendState(id, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Zero(t, state.DeletedSlot)
	require.Nil(t, state.SpentAtTxId)
	for _, query := range recorder.snapshot()[mark:] {
		require.NotContains(t, strings.ToLower(query), "from asset")
	}
	txn := store.Transaction(context.Background())
	defer func() { require.NoError(t, txn.Rollback()) }()
	spender := mockledger.NewTransactionBuilder()
	spender.WithId(overlayTxID(0x77, 0))
	spender.WithValid(true)
	require.NoError(
		t,
		store.SetTransaction(
			spender,
			ocommon.Point{Slot: 20},
			0,
			nil,
			true,
			txn,
			0,
		),
	)
	require.NoError(
		t,
		store.SetUtxoDeletedAtSlot(
			overlayTxInput(t, id, 0),
			20,
			spender.Hash().Bytes(),
			txn,
		),
	)
	state, err = store.GetUtxoSpendState(id, 0, txn)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(20), state.DeletedSlot)
	require.Equal(
		t,
		types.NullableHash(spender.Hash().Bytes()),
		state.SpentAtTxId,
	)
	full, err := store.GetUtxoIncludingSpent(id, 0, txn)
	require.NoError(t, err)
	require.Equal(t, full.DeletedSlot, state.DeletedSlot)
	require.Equal(t, full.SpentAtTxId, state.SpentAtTxId)
}

func TestUtxoSpendStateReadsUnflushedRetainedOverlay(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	seed, transactions := plainOverlayChain(t, 2)
	require.NoError(
		t,
		store.SetTransaction(
			seed,
			ocommon.Point{Slot: 10},
			0,
			nil,
			true,
			nil,
			0,
		),
	)
	txn := store.Transaction(context.Background())
	defer func() { require.NoError(t, txn.Rollback()) }()
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	enabled, err := store.EnableLeiosClosureUtxoOverlay(acc, txn, transactions)
	require.NoError(t, err)
	require.True(t, enabled)
	point := ocommon.Point{Slot: 100}
	for _, tx := range transactions {
		require.NoError(
			t,
			store.SetTransactionLeiosClosureBatched(
				tx,
				point,
				0,
				nil,
				true,
				nil,
				acc,
				txn,
				0,
			),
		)
	}
	overlay := acc.(*transactionBatchAccumulator).overlay
	barriers := overlay.stats.BarrierFlushes
	spent, err := store.GetUtxoSpendState(
		transactions[0].Hash().Bytes(),
		0,
		txn,
	)
	require.NoError(t, err)
	require.NotNil(t, spent)
	require.Equal(t, point.Slot, spent.DeletedSlot)
	require.Equal(
		t,
		types.NullableHash(transactions[1].Hash().Bytes()),
		spent.SpentAtTxId,
	)
	live, err := store.GetUtxoSpendState(transactions[1].Hash().Bytes(), 0, txn)
	require.NoError(t, err)
	require.NotNil(t, live)
	require.Zero(t, live.DeletedSlot)
	require.Nil(t, live.SpentAtTxId)
	require.Equal(t, barriers, overlay.stats.BarrierFlushes)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled := store.ReadTransaction(ctx)
	defer func() { _ = canceled.Rollback() }()
	_, err = store.GetUtxoSpendState(
		transactions[1].Hash().Bytes(),
		0,
		canceled,
	)
	require.ErrorIs(t, err, context.Canceled)
}

func BenchmarkConsumedUtxoSpendState(b *testing.B) {
	store := newMigratedSQLiteStore(b)
	seedUtxoLookupRows(b, store, 1_000)
	id := benchmarkUtxoTxID(500)
	txn := store.Transaction(context.Background())
	defer func() { _ = txn.Rollback() }()
	b.Run("full", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			state, err := store.GetUtxoIncludingSpent(id, 0, txn)
			if err != nil || state == nil {
				b.Fatalf("read output: %v", err)
			}
		}
	})
	b.Run("spend-state", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			state, err := store.GetUtxoSpendState(id, 0, txn)
			if err != nil || state == nil {
				b.Fatalf("read spend state: %v", err)
			}
		}
	})
}

func TestUtxoSpendStateKeepsSeparateReadPool(t *testing.T) {
	t.Parallel()
	store := newSplitReadPoolTestStore(t)
	seedUtxoLookupRows(t, store, 1)
	id := benchmarkUtxoTxID(0)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { require.NoError(t, txn.Rollback()) })
	require.NoError(
		t,
		store.SetUtxoDeletedAtSlot(overlayTxInput(t, id, 0), 42, nil, txn),
	)
	state, err := store.GetUtxoSpendState(id, 0, txn)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(42), state.DeletedSlot)
	type result struct {
		state *models.UtxoSpendState
		err   error
	}
	got := make(chan result, 1)
	go func() { s, e := store.GetUtxoSpendState(id, 0, nil); got <- result{s, e} }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case read := <-got:
		require.NoError(t, read.err)
		require.NotNil(t, read.state)
		require.Zero(t, read.state.DeletedSlot)
	case <-ctx.Done():
		require.NoError(t, txn.Rollback())
		t.Fatal(
			"spend-state read waited on the write connection held by its caller",
		)
	}
	readTxn := store.ReadTransaction(context.Background())
	t.Cleanup(func() { require.NoError(t, readTxn.Rollback()) })
	state, err = store.GetUtxoSpendState(id, 0, readTxn)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Zero(t, state.DeletedSlot)
}

func TestUtxoSpendStateKeepsNullableSlotSemantics(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedUtxoLookupRows(t, store, 1)
	id := benchmarkUtxoTxID(0)
	_, err := store.writeDB.Exec(
		"UPDATE utxo SET deleted_slot = NULL WHERE tx_id = ?",
		id,
	)
	require.NoError(t, err)
	full, err := store.GetUtxoIncludingSpent(id, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, full)
	state, err := store.GetUtxoSpendState(id, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, full.DeletedSlot, state.DeletedSlot)
	require.Equal(t, full.SpentAtTxId, state.SpentAtTxId)
}
