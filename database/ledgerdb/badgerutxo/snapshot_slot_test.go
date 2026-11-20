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
	"errors"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

func snapAt(epoch uint64, typ string, pool byte, slot uint64) badgerutxo.SnapshotOp {
	op := snap(epoch, typ, pool)
	op.Row.CapturedSlot = slot
	return op
}

// capturedIn lists the rows captured in [from, through] as
// "<epoch><type><pool>@<slot>", and fails unless the store has the index.
func capturedIn(t *testing.T, s *badgerutxo.Store, from, through uint64) []string {
	t.Helper()
	rows, indexed, err := s.SnapshotsCapturedIn(from, through)
	require.NoError(t, err)
	require.True(t, indexed)
	out := []string{}
	for _, r := range rows {
		out = append(out, fmt.Sprintf("%d%s%s@%d", r.Epoch, r.Type, r.Pool, r.CapturedSlot))
	}
	return out
}

func requireSnapshotIndexAgrees(t *testing.T, s *badgerutxo.Store, msg string) {
	t.Helper()
	bad, err := s.SnapshotIndexMismatchForTest()
	require.NoError(t, err)
	require.Empty(t, bad, msg)
}

func indexedStore(t *testing.T) *badgerutxo.Store {
	t.Helper()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	require.NoError(t, s.IndexSnapshotSlots(
		func(badgerutxo.SnapshotRow) (uint64, error) {
			return 0, errors.New("no legacy row expected")
		},
	))
	return s
}

// The capture index follows every write of the snapshot rows: a put, a put
// that moves a row to another slot, both deletes and both rollback paths.
func TestSnapshotCaptureIndexFollowsEveryWriter(t *testing.T) {
	t.Parallel()
	for _, viaJournal := range []bool{true, false} {
		s := indexedStore(t)
		applyEpochState(t, s, point(10, 0x10),
			[]badgerutxo.EpochRow{epochRow(0, 0)},
			snapAt(0, "mark", 'a', 5), snapAt(0, "set", 'a', 50))
		applyEpochState(t, s, point(110, 0x6e),
			[]badgerutxo.EpochRow{epochRow(1, 100)},
			snapAt(1, "mark", 'a', 105), snapAt(1, "mark", 'b', 150))
		applyEpochState(t, s, point(210, 0xd2),
			[]badgerutxo.EpochRow{epochRow(2, 200)},
			snapAt(2, "mark", 'a', 205))
		msg := fmt.Sprintf("journal=%t", viaJournal)
		requireSnapshotIndexAgrees(t, s, msg)
		require.Equal(t, []string{"1marka@105", "1markb@150"},
			capturedIn(t, s, 100, 199), msg)
		require.Equal(t, []string{"0seta@50", "0marka@5"},
			capturedIn(t, s, 0, 99), msg)
		require.Empty(t, capturedIn(t, s, 106, 149), msg)
		require.Equal(t, []string{"1marka@105"}, capturedIn(t, s, 105, 105),
			msg+": a range is closed at both ends")
		require.Equal(t, []string{"1markb@150"}, capturedIn(t, s, 150, 150), msg)
		require.Empty(t, capturedIn(t, s, 200, 100), msg, "an empty range")

		// A put replaces the row and the entry it had.
		applyEpochState(t, s, point(220, 0xdc), nil, snapAt(1, "mark", 'a', 250))
		requireSnapshotIndexAgrees(t, s, msg)
		require.Equal(t, []string{"1markb@150"}, capturedIn(t, s, 100, 199), msg)
		require.Equal(t, []string{"1marka@250"}, capturedIn(t, s, 206, 299), msg)

		applyEpochState(t, s, point(230, 0xe6), nil, badgerutxo.SnapshotOp{
			Kind: badgerutxo.SnapshotDeleteEpochType,
			Row:  badgerutxo.SnapshotRow{Epoch: 1, Type: "mark"},
		})
		requireSnapshotIndexAgrees(t, s, msg)
		require.Equal(t, []string{"2marka@205"}, capturedIn(t, s, 100, 299), msg)

		if viaJournal {
			_, err := s.Rollback(point(110, 0x6e))
			require.NoError(t, err)
		} else {
			to := uint64(150)
			tip := point(150, 0x96)
			_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
				DeleteNoncesAfter: &to,
			})
			require.NoError(t, err)
		}
		requireSnapshotIndexAgrees(t, s, msg+" after the rollback")
		require.Empty(t, capturedIn(t, s, 200, 299), msg+": epoch 2 is gone")

		applyEpochState(t, s, point(300, 0x2c), nil, badgerutxo.SnapshotOp{
			Kind: badgerutxo.SnapshotDeleteBefore,
			Row:  badgerutxo.SnapshotRow{Epoch: 1},
		})
		requireSnapshotIndexAgrees(t, s, msg)
		require.Empty(t, capturedIn(t, s, 0, 99), msg, "epoch 0 is pruned")
	}
}

// A load replaces the rows and the index, marks the index complete, and a
// full bootstrap clears both again.
func TestLoadEpochStateBuildsTheCaptureIndex(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	_, indexed, err := s.SnapshotsCapturedIn(0, 1000)
	require.NoError(t, err)
	require.False(t, indexed, "a store that never indexed its rows")

	row := func(epoch uint64, pool byte, slot uint64) badgerutxo.SnapshotRow {
		r := snap(epoch, "go", pool).Row
		r.CapturedSlot = slot
		return r
	}
	require.NoError(t, s.LoadEpochState(
		[]badgerutxo.EpochRow{epochRow(3, 300)},
		[]badgerutxo.SnapshotRow{row(3, 'a', 310), row(3, 'b', 320)}, nil,
	))
	require.Equal(t, []string{"3goa@310", "3gob@320"}, capturedIn(t, s, 300, 399))
	requireSnapshotIndexAgrees(t, s, "after the load")

	require.NoError(t, s.LoadEpochState(
		[]badgerutxo.EpochRow{epochRow(3, 300)},
		[]badgerutxo.SnapshotRow{row(3, 'a', 330)}, nil,
	))
	require.Equal(t, []string{"3goa@330"}, capturedIn(t, s, 300, 399))
	requireSnapshotIndexAgrees(t, s, "after the second load")

	bootstrap(t, s, point(5, 0x05))
	_, indexed, err = s.SnapshotsCapturedIn(0, 1000)
	require.NoError(t, err)
	require.False(t, indexed, "a bootstrap drops the rows and the marker")
	requireSnapshotIndexAgrees(t, s, "after the bootstrap")
}

// A merge keeps the rows the set holds, so a merged row that replaces one
// takes the old row's entry with it, and the marker is neither set nor lost.
func TestMergeEpochStateKeepsTheCaptureIndexInStep(t *testing.T) {
	t.Parallel()
	s := indexedStore(t)
	applyEpochState(t, s, point(10, 0x10), nil,
		snapAt(1, "go", 'a', 105), snapAt(1, "go", 'b', 150))

	require.NoError(t, s.BootstrapCatchUp(point(20, 0x02),
		func(put func(badgerutxo.Output) error) error { return put(out(0x21, 0)) },
	))
	require.NoError(t, s.BeginMerge())
	moved := snap(1, "go", 'a').Row
	moved.CapturedSlot = 170
	added := snap(2, "go", 'c').Row
	added.CapturedSlot = 180
	require.NoError(t, s.LoadEpochState(
		[]badgerutxo.EpochRow{epochRow(1, 100)},
		[]badgerutxo.SnapshotRow{moved, added}, nil,
	))
	require.NoError(t, s.EndMerge(true))

	requireSnapshotIndexAgrees(t, s, "after the merge")
	require.Equal(t, []string{"1goa@170", "1gob@150", "2goc@180"},
		capturedIn(t, s, 100, 199))
}

// A store written before the index existed reads its rows whole until
// IndexSnapshotSlots gives each its slot and entry. A run interrupted after
// converting some rows finishes on the next, and a store already indexed is
// not read again.
func TestIndexSnapshotSlotsBackfillsRowsOfAnOlderStore(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))

	// The legacy value's first byte stands for its capture slot.
	legacy := func(epoch uint64, pool byte, slot byte) badgerutxo.SnapshotRow {
		return badgerutxo.SnapshotRow{
			Epoch: epoch, Type: "go", Pool: []byte{pool}, Value: []byte{slot + 1, pool},
		}
	}
	for _, r := range []badgerutxo.SnapshotRow{
		legacy(1, 'a', 10), legacy(1, 'b', 20), legacy(2, 'c', 30),
	} {
		require.NoError(t, s.PutLegacySnapshotRowForTest(r))
	}
	// Converted by a run that stopped before indexing it.
	half := legacy(3, 'd', 40)
	half.CapturedSlot = 40
	require.NoError(t, s.PutUnindexedSnapshotRowForTest(half))

	_, indexed, err := s.SnapshotsCapturedIn(0, 100)
	require.NoError(t, err)
	require.False(t, indexed)
	rows, err := s.Snapshots(1, "go")
	require.NoError(t, err)
	require.Equal(t, []byte{11, 'a'}, rows[0].Value, "a legacy row reads whole")

	slotOf := func(r badgerutxo.SnapshotRow) (uint64, error) {
		return uint64(r.Value[0] - 1), nil
	}
	require.NoError(t, s.IndexSnapshotSlots(slotOf))

	require.Equal(t, []string{"1goa@10", "1gob@20", "2goc@30", "3god@40"},
		capturedIn(t, s, 0, 100))
	require.Equal(t, []string{"2goc@30"}, capturedIn(t, s, 25, 35))
	requireSnapshotIndexAgrees(t, s, "after the backfill")
	rows, err = s.Snapshots(1, "go")
	require.NoError(t, err)
	require.Equal(t, []byte{11, 'a'}, rows[0].Value, "the value reads as written")
	require.EqualValues(t, 10, rows[0].CapturedSlot)

	require.NoError(t, s.IndexSnapshotSlots(
		func(badgerutxo.SnapshotRow) (uint64, error) {
			return 0, errors.New("an indexed store was read again")
		},
	))
}

// The store keeps a snapshot's slot ahead of its value, so a value that
// starts like the slot is refused rather than misread.
func TestSnapshotValueStartingWithTheSlotMarkIsRefused(t *testing.T) {
	t.Parallel()
	s := indexedStore(t)
	op := snap(1, "go", 'a')
	op.Row.Value = []byte{0x00, 0x01}
	tip := point(10, 0x10)
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		SnapshotOps: []badgerutxo.SnapshotOp{op},
	})
	require.Error(t, err)
}

// A merge into a store that never indexed its rows leaves it unindexed, so a
// reader keeps reading every row rather than trust a partial index.
func TestMergeEpochStateLeavesAnUnindexedStoreUnindexed(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(10, 0x01))
	require.NoError(t, s.PutLegacySnapshotRowForTest(snap(1, "go", 'a').Row))
	require.NoError(t, s.BootstrapCatchUp(point(20, 0x02),
		func(put func(badgerutxo.Output) error) error { return put(out(0x21, 0)) },
	))
	require.NoError(t, s.BeginMerge())
	added := snap(2, "go", 'c').Row
	added.CapturedSlot = 180
	require.NoError(t, s.LoadEpochState(
		[]badgerutxo.EpochRow{epochRow(1, 100)},
		[]badgerutxo.SnapshotRow{added}, nil,
	))
	require.NoError(t, s.EndMerge(true))
	_, indexed, err := s.SnapshotsCapturedIn(0, 1000)
	require.NoError(t, err)
	require.False(t, indexed)
}
