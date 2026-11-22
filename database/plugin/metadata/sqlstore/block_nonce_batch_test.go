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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// batchNonceFixture covers a checkpoint, two blocks sharing slot 20 with
// different hashes, and a repeated (hash, slot) that must keep its checkpoint
// flag while taking the later nonce.
func batchNonceFixture() []models.BlockNonce {
	return []models.BlockNonce{
		{Hash: []byte{0x01}, Slot: 10, Nonce: []byte{0xa1}, IsCheckpoint: true},
		{Hash: []byte{0x02}, Slot: 20, Nonce: []byte{0xa2}},
		{Hash: []byte{0x03}, Slot: 20, Nonce: []byte{0xa3}},
		{Hash: []byte{0x04}, Slot: 30, Nonce: []byte{0xa4}},
		{Hash: []byte{0x01}, Slot: 10, Nonce: []byte{0xa5}},
	}
}

func allBlockNonces(t *testing.T, store *Store) []models.BlockNonce {
	t.Helper()
	rows, err := store.GetBlockNoncesInSlotRange(0, 1000, nil)
	require.NoError(t, err)
	return rows
}

func TestQueuedBlockNoncesMatchPerRowWrites(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	batched := newMigratedSQLiteStoreWithRegistry(t, reg)
	perRow := newMigratedSQLiteStore(t)
	nonces := batchNonceFixture()

	for _, n := range nonces {
		require.NoError(t, perRow.SetBlockNonce(
			n.Hash, n.Slot, n.Nonce, n.IsCheckpoint, nil,
		))
	}

	before := counterValue(t, reg, "insert")
	acc := batched.NewBatchAccumulator()
	txn := batched.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	for _, n := range nonces {
		require.NoError(t, batched.QueueBlockNonce(n, acc, txn))
	}
	require.NoError(t, batched.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())

	require.Equal(
		t, before+1, counterValue(t, reg, "insert"),
		"a batch of nonces must be written by one multi-row statement",
	)
	want := allBlockNonces(t, perRow)
	got := allBlockNonces(t, batched)
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i].ID, got[i].ID, "row %d id", i)
		require.Equal(t, want[i].Hash, got[i].Hash, "row %d hash", i)
		require.Equal(t, want[i].Slot, got[i].Slot, "row %d slot", i)
		require.Equal(t, want[i].Nonce, got[i].Nonce, "row %d nonce", i)
		require.Equal(
			t, want[i].IsCheckpoint, got[i].IsCheckpoint, "row %d checkpoint", i,
		)
	}
	latest, ok, err := batched.GetLatestBlockNonce(nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte{0x04}, latest.Hash)
}

func TestQueuedBlockNoncesAreNotWrittenBeforeFlush(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	acc, ok := store.NewBatchAccumulator().(*transactionBatchAccumulator)
	require.True(t, ok)
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	for _, n := range batchNonceFixture() {
		require.NoError(t, store.QueueBlockNonce(n, acc, txn))
	}
	require.Equal(t, len(batchNonceFixture()), acc.Len())
	require.NoError(t, txn.Rollback())
	acc.Reset()
	require.Empty(t, allBlockNonces(t, store))
}

func TestQueuedBlockNoncesRestoreOnSavepointRollback(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	acc, ok := store.NewBatchAccumulator().(*transactionBatchAccumulator)
	require.True(t, ok)
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	sqlTxn := txn.(*sqlTxn)
	nonces := batchNonceFixture()

	require.NoError(t, store.QueueBlockNonce(nonces[0], acc, txn))
	require.NoError(t, sqlTxn.SavePoint("nonce_checkpoint"))
	require.NoError(t, store.QueueBlockNonce(nonces[1], acc, txn))
	require.NoError(t, sqlTxn.RollbackTo("nonce_checkpoint"))
	require.Equal(t, 1, acc.Len())

	require.NoError(t, store.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())
	rows := allBlockNonces(t, store)
	require.Len(t, rows, 1)
	require.Equal(t, nonces[0].Hash, rows[0].Hash)
}
