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
					Value: []byte{1}, CapturedSlot: slot,
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
	rows, err := s.Snapshots(1, "mark")
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
	rows, err = s.Snapshots(1, "mark")
	require.NoError(t, err)
	require.Empty(t, rows)
}

// A batch whose reward block ops span transactions and stopped between them
// is applied again whole, and leaves the counts of one application.
func TestRewardBlockOpsStoppedBetweenTransactionsReplay(t *testing.T) {
	t.Parallel()
	s := smallTxnStore(t)
	bootstrap(t, s, point(1, 0x01))
	pool := bytes.Repeat([]byte{0xa1}, 28)
	ops := []badgerutxo.RewardBlockOp{{Slot: 2, Epoch: 1, Open: true}}
	for i := range familyOps {
		ops = append(ops, badgerutxo.RewardBlockOp{
			Slot: uint64(10 + i), Epoch: 1, Pool: pool, //nolint:gosec // index
		})
	}
	tip := point(10+familyOps, 0x77)
	errStop := errors.New("stopped")
	s.SetAfterChunkForTest(func(int) error { return errStop })
	_, err := s.ApplyLedgerBatch(
		&tip,
		badgerutxo.LedgerBatch{RewardBlocks: ops},
	)
	require.ErrorIs(t, err, errStop)
	s.SetAfterChunkForTest(nil)

	_, err = s.Rollback(point(1, 0x01))
	require.NoError(t, err)
	_, err = s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{RewardBlocks: ops})
	require.NoError(t, err)
	require.Equal(t, uint64(familyOps), readBlockRows(t, s, 1, nil).total)
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
					Value: []byte{1}, CapturedSlot: slot,
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
