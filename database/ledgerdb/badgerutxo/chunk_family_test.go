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

package badgerutxo_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

// smallTxnStore is a store with an 8 MiB memtable, whose Badger
// transactions hold far fewer entries than the batches below write.
func smallTxnStore(t *testing.T) *badgerutxo.Store {
	t.Helper()
	s, err := badgerutxo.Open(
		t.TempDir(), badgerutxo.WithMemTableSize(8<<20),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

const familyOps = 30000

func TestRewardBlockOpsBeyondOneTransactionCommit(t *testing.T) {
	t.Parallel()
	s := smallTxnStore(t)
	start := point(1, 0x01)
	bootstrap(t, s, start)
	pool := bytes.Repeat([]byte{0xa1}, 28)
	ops := []badgerutxo.RewardBlockOp{{Slot: 2, Epoch: 1, Open: true}}
	for i := range familyOps {
		slot := uint64(10 + i) //nolint:gosec // a test index
		ops = append(ops,
			badgerutxo.RewardBlockOp{Slot: slot, Epoch: 1, Pool: pool},
			badgerutxo.RewardBlockOp{Slot: slot, Epoch: 1, Fee: 1},
		)
	}
	tip := point(10+familyOps, 0x77)
	_, err := s.ApplyLedgerBatch(
		&tip,
		badgerutxo.LedgerBatch{RewardBlocks: ops},
	)
	require.NoError(t, err)
	requireTip(t, s, tip)
	got := readBlockRows(t, s, 1, nil)
	require.Equal(t, uint64(familyOps), got.total)
	require.Equal(t, uint64(familyOps), got.fees)
	require.Equal(t, uint64(familyOps), got.pools[string(pool)])

	_, err = s.Rollback(start)
	require.NoError(t, err)
	require.False(t, readBlockRows(t, s, 1, nil).hasTotal)
}

func TestRewardBlockPruneBeyondOneTransactionCommits(t *testing.T) {
	t.Parallel()
	s := smallTxnStore(t)
	bootstrap(t, s, point(1, 0x01))
	const epochs = 30000
	slot := uint64(2)
	for first := uint64(1); first <= epochs; first += 1000 {
		var ops []badgerutxo.RewardBlockOp
		for e := first; e < first+1000; e++ {
			ops = append(ops, badgerutxo.RewardBlockOp{
				Slot: slot, Epoch: e, Open: true,
			})
		}
		applyBlockOps(t, s, slot, ops...)
		slot++
	}
	applyBlockOps(t, s, slot, badgerutxo.RewardBlockOp{
		Slot: slot, PruneBelow: epochs,
	})
	require.False(t, readBlockRows(t, s, 1, nil).hasTotal)
	require.False(t, readBlockRows(t, s, epochs-1, nil).hasTotal)
	require.True(t, readBlockRows(t, s, epochs, nil).hasTotal)
}

func TestSnapshotDeleteBeyondOneTransactionCommits(t *testing.T) {
	t.Parallel()
	s := smallTxnStore(t)
	bootstrap(t, s, point(1, 0x01))
	slot := uint64(2)
	const pools = 20000
	for first := 0; first < pools; first += 1000 {
		var ops []badgerutxo.SnapshotOp
		for i := first; i < first+1000; i++ {
			ops = append(ops, badgerutxo.SnapshotOp{
				Kind: badgerutxo.SnapshotPut,
				Row: badgerutxo.SnapshotRow{
					Epoch: 1, Type: "mark",
					Pool:  []byte{byte(i >> 16), byte(i >> 8), byte(i)},
					Value: []byte{1},
				},
			})
		}
		tip := point(slot, byte(slot))
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
			SnapshotOps: ops,
		})
		require.NoError(t, err)
		slot++
	}
	rows, err := s.Snapshots(1, "mark", 0)
	require.NoError(t, err)
	require.Len(t, rows, pools)

	tip := point(slot, byte(slot))
	_, err = s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		SnapshotOps: []badgerutxo.SnapshotOp{{
			Kind: badgerutxo.SnapshotDeleteBefore,
			Row:  badgerutxo.SnapshotRow{Epoch: 2},
		}},
	})
	require.NoError(t, err)
	requireTip(t, s, tip)
	rows, err = s.Snapshots(1, "mark", 0)
	require.NoError(t, err)
	require.Empty(t, rows)
}

// A batch whose reward block ops span transactions and stopped after the
// first of them, ahead of any journal record, is rewound by the failure-mark
// recovery on restart and applied again whole: its fee is added once, whether
// the mark takes the slot of a UTxO change or of the tip, above the fee's.
func TestRewardBlockOpsStoppedBetweenTransactionsReplay(t *testing.T) {
	t.Parallel()
	for _, withChange := range []bool{true, false} {
		dir := t.TempDir()
		openSmall := func() *badgerutxo.Store {
			s, err := badgerutxo.Open(dir, badgerutxo.WithMemTableSize(8<<20))
			require.NoError(t, err)
			return s
		}
		s := openSmall()
		bootstrap(t, s, point(1, 0x01))
		pool := bytes.Repeat([]byte{0xa1}, 28)
		ops := []badgerutxo.RewardBlockOp{
			{Slot: 5, Epoch: 1, Open: true},
			{Slot: 5, Epoch: 1, Fee: 7},
		}
		for i := range familyOps {
			ops = append(ops, badgerutxo.RewardBlockOp{
				Slot: uint64(10 + i), Epoch: 1, Pool: pool, //nolint:gosec // index
			})
		}
		tip := point(10+familyOps, 0x77)
		batch := badgerutxo.LedgerBatch{RewardBlocks: ops}
		if withChange {
			batch.Changes = []badgerutxo.Change{{
				Point: point(8, 0x08), Added: []badgerutxo.Output{out(2, 0)},
			}}
		}
		errStop := errors.New("stopped")
		s.SetAfterChunkForTest(func(int) error { return errStop })
		_, err := s.ApplyLedgerBatch(&tip, batch)
		require.ErrorIs(t, err, errStop)
		require.NoError(t, s.Close())

		s = openSmall()
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		mark, failed, err := s.Failed()
		require.NoError(t, err)
		require.True(t, failed)
		// As the live set's startup rewinds a failure mark.
		before, found, err := s.PointBefore(mark)
		require.NoError(t, err)
		if found {
			_, err = s.Rollback(before)
			require.NoError(t, err)
		}
		require.NoError(t, s.ClearFailed())

		_, err = s.ApplyLedgerBatch(&tip, batch)
		require.NoError(t, err)
		requireTip(t, s, tip)
		requireLive(t, s, withChange, ref(2, 0))
		got := readBlockRows(t, s, 1, nil)
		require.Equal(t, uint64(7), got.fees, "with a change: %v", withChange)
		require.Equal(t, uint64(familyOps), got.total)
		require.Equal(t, uint64(familyOps), got.pools[string(pool)])
	}
}

// A batch without a tip, changes or slotted rows whose snapshot delete spans
// transactions and stopped between them leaves no failure mark that startup
// cannot rewind, nor one that undoes a commit already applied.
func TestTiplessSnapshotDeleteStoppedLeavesRecoverableMark(t *testing.T) {
	t.Parallel()
	s := smallTxnStore(t)
	bootstrap(t, s, point(1, 0x01))
	slot := uint64(2)
	const pools = 20000
	for first := 0; first < pools; first += 1000 {
		var ops []badgerutxo.SnapshotOp
		for i := first; i < first+1000; i++ {
			ops = append(ops, badgerutxo.SnapshotOp{
				Kind: badgerutxo.SnapshotPut,
				Row: badgerutxo.SnapshotRow{
					Epoch: 1, Type: "mark",
					Pool:  []byte{byte(i >> 16), byte(i >> 8), byte(i)},
					Value: []byte{1},
				},
			})
		}
		tip := point(slot, byte(slot))
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
			SnapshotOps: ops,
		})
		require.NoError(t, err)
		slot++
	}
	tip := point(slot-1, byte(slot-1))

	errStop := errors.New("stopped")
	s.SetAfterChunkForTest(func(done int) error {
		if done > 1 {
			return errStop
		}
		return nil
	})
	_, err := s.ApplyLedgerBatch(nil, badgerutxo.LedgerBatch{
		SnapshotOps: []badgerutxo.SnapshotOp{{
			Kind: badgerutxo.SnapshotDeleteBefore,
			Row:  badgerutxo.SnapshotRow{Epoch: 2},
		}},
	})
	require.ErrorIs(t, err, errStop)
	s.SetAfterChunkForTest(nil)

	mark, failed, err := s.Failed()
	require.NoError(t, err)
	if failed {
		before, found, err := s.PointBefore(mark)
		require.NoError(t, err)
		require.False(t, found, "rewinds to %v", before)
	}
	requireTip(t, s, tip)
}

// snapshotPuts returns n snapshot puts of epoch's mark snapshot, captured
// at slot.
func snapshotPuts(epoch, slot uint64, n int) []badgerutxo.SnapshotOp {
	ops := make([]badgerutxo.SnapshotOp, 0, n)
	for i := range n {
		ops = append(ops, badgerutxo.SnapshotOp{
			Kind: badgerutxo.SnapshotPut,
			Row: badgerutxo.SnapshotRow{
				Epoch: epoch, Type: "mark",
				Pool:  []byte{byte(i >> 16), byte(i >> 8), byte(i)},
				Value: []byte{1},
			},
		})
	}
	return ops
}

// An epoch-boundary batch whose snapshot puts and reward block ops together
// exceed one Badger transaction commits whole, beside its UTxO change, epoch
// row and rows, and a rollback before it undoes all of it.
func TestEpochBoundaryBatchBeyondOneTransactionCommits(t *testing.T) {
	t.Parallel()
	s := smallTxnStore(t)
	start := point(1, 0x01)
	bootstrap(t, s, start)
	tip0 := point(2, 0x02)
	_, err := s.ApplyLedgerBatch(&tip0, badgerutxo.LedgerBatch{
		Epochs: []badgerutxo.EpochRow{epochRow(0, 0)},
	})
	require.NoError(t, err)

	// Reward block ops past one transaction on their own; snapshot puts
	// few enough that the rollback below deletes them in one.
	const n, pools = 20000, 4000
	boundary := uint64(100)
	pool := bytes.Repeat([]byte{0xa1}, 28)
	ops := []badgerutxo.RewardBlockOp{{Slot: boundary, Epoch: 1, Open: true}}
	for i := range n {
		slot := boundary + uint64(i) //nolint:gosec // a test index
		ops = append(ops,
			badgerutxo.RewardBlockOp{Slot: slot, Epoch: 1, Pool: pool},
			badgerutxo.RewardBlockOp{Slot: slot, Epoch: 1, Fee: 1},
		)
	}
	tip := point(boundary+n, 0x77)
	_, err = s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Changes: []badgerutxo.Change{{
			Point: point(boundary, 0x64), Added: []badgerutxo.Output{out(2, 0)},
		}},
		Epochs:       []badgerutxo.EpochRow{epochRow(1, boundary)},
		SnapshotOps:  snapshotPuts(1, boundary, pools),
		RewardBlocks: ops,
		Rows: []badgerutxo.RowPut{{
			Slot: boundary, Key: []byte{0xf0, 1}, Value: []byte{1},
		}},
	})
	require.NoError(t, err)
	requireTip(t, s, tip)
	requireLive(t, s, true, ref(2, 0))
	rows, err := s.Snapshots(1, "mark", 0)
	require.NoError(t, err)
	require.Len(t, rows, pools)
	got := readBlockRows(t, s, 1, nil)
	require.Equal(t, uint64(n), got.total)
	require.Equal(t, uint64(n), got.fees)

	_, err = s.Rollback(tip0)
	require.NoError(t, err)
	requireLive(t, s, false, ref(2, 0))
	require.False(t, readBlockRows(t, s, 1, nil).hasTotal)
	rows, err = s.Snapshots(1, "mark", 0)
	require.NoError(t, err)
	require.Empty(t, rows)
}

// Reward block ops apply after the batch's rows, as in one transaction, when
// the ops need transactions of their own and the rows do not: a fee row a
// row put creates takes the fees of the ops that follow it.
func TestRewardBlockOpsBeyondOneTransactionFollowRows(t *testing.T) {
	t.Parallel()
	s := smallTxnStore(t)
	bootstrap(t, s, point(1, 0x01))
	ops := make([]badgerutxo.RewardBlockOp, 0, familyOps)
	for i := range familyOps {
		ops = append(ops, badgerutxo.RewardBlockOp{
			Slot: uint64(10 + i), Epoch: 1, Fee: 1, //nolint:gosec // index
		})
	}
	tip := point(10+familyOps, 0x77)
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Rows: []badgerutxo.RowPut{{
			Slot:  5,
			Key:   badgerutxo.RewardBlockFeesKey(1),
			Value: badgerutxo.EncodeRewardBlockCount(0),
		}},
		RewardBlocks: ops,
	})
	require.NoError(t, err)
	requireTip(t, s, tip)
	require.Equal(t, uint64(familyOps), readBlockRows(t, s, 1, nil).fees)
}

// A batch whose rows are estimated over one transaction beside its other
// writes, with no snapshot or reward block op to move out, still applies
// its rollback: the nonce above the rollback slot is deleted.
func TestOverBudgetBatchWithoutOpsKeepsRollback(t *testing.T) {
	t.Parallel()
	for _, withChange := range []bool{true, false} {
		s, err := badgerutxo.Open(t.TempDir())
		require.NoError(t, err)
		bootstrap(t, s, point(1, 0x01))
		stale := point(15, 0x0f)
		tip1 := point(20, 0x14)
		_, err = s.ApplyLedgerBatch(&tip1, badgerutxo.LedgerBatch{
			Nonces: []badgerutxo.Nonce{{Point: stale, Nonce: []byte{0xaa}}},
		})
		require.NoError(t, err)

		const rows = 10
		puts := make([]badgerutxo.RowPut, 0, rows)
		for i := range rows {
			puts = append(puts, badgerutxo.RowPut{
				Slot: 12, Key: []byte{0xf0, byte(i)}, Value: []byte{1},
			})
		}
		// The rows alone fill the budget; the family estimate adds to them.
		s.SetBudgetForTest(2*3*rows, 1<<30)
		rollbackTo := uint64(10)
		tip := point(12, 0x0c)
		batch := badgerutxo.LedgerBatch{DeleteNoncesAfter: &rollbackTo, Rows: puts}
		if withChange {
			batch.Changes = []badgerutxo.Change{{
				Point: tip, Added: []badgerutxo.Output{out(3, 0)},
			}}
		}
		_, err = s.ApplyLedgerBatch(&tip, batch)
		require.NoError(t, err)
		requireTip(t, s, tip)
		_, ok, err := s.Nonce(stale)
		require.NoError(t, err)
		require.False(t, ok, "with a change: %v", withChange)
		require.NoError(t, s.Close())
	}
}
