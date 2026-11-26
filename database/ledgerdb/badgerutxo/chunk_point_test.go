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
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// bigPointOutputs is more outputs than one Badger transaction of an 8 MiB
// memtable takes, about 13,000 entries.
const bigPointOutputs = 20_000

// smallLimitStore opens a store whose real Badger transaction limit is small
// enough for one block of bigPointOutputs to exceed it.
func smallLimitStore(t *testing.T, dir string) *badgerutxo.Store {
	t.Helper()
	s, err := badgerutxo.Open(dir, badgerutxo.WithMemTableSize(8<<20))
	require.NoError(t, err)
	return s
}

// applyBigPoint applies one block of bigPointOutputs outputs after start and
// returns its point.
func applyBigPoint(t *testing.T, s *badgerutxo.Store) ocommon.Point {
	t.Helper()
	changes := bigChanges(1, bigPointOutputs)
	tip := changes[0].Point
	res, err := s.ApplyBatch(&tip, changes)
	require.NoError(t, err)
	require.Empty(t, res.Missing)
	require.Empty(t, res.Duplicate)
	return tip
}

func requireBigOutputs(t *testing.T, s *badgerutxo.Store, live bool) {
	t.Helper()
	for _, i := range []uint32{0, bigPointOutputs / 2, bigPointOutputs - 1} {
		_, ok, err := s.Get(bigOut(i).Ref)
		require.NoError(t, err)
		require.Equal(t, live, ok, "output %d", i)
	}
}

func requireBigPointRolledBack(
	t *testing.T,
	s *badgerutxo.Store,
	start ocommon.Point,
) {
	t.Helper()
	requireTip(t, s, start)
	requireCount(t, s, 0)
	requireBigOutputs(t, s, false)
}

// A block whose changes are more than one Badger transaction takes is
// written in several, and the tip moves only with the last.
func TestOnePointOverTheTransactionLimitApplies(t *testing.T) {
	t.Parallel()
	s := smallLimitStore(t, t.TempDir())
	defer s.Close()
	start := point(0, 0)
	bootstrap(t, s, start)
	tip := applyBigPoint(t, s)
	requireTip(t, s, tip)
	requireCount(t, s, bigPointOutputs)
	requireBigOutputs(t, s, true)
	has, err := s.HasJournalPoint(start)
	require.NoError(t, err)
	require.True(t, has)
	// The block is one point a rollback can stop at, not one per
	// transaction it took.
	points, err := s.JournalPoints()
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.Equal(t, start.Slot, points[0].Slot)

	rb, err := s.Rollback(start)
	require.NoError(t, err)
	require.Len(t, rb.Removed, bigPointOutputs)
	requireBigPointRolledBack(t, s, start)
}

// A stop between the transactions of one block leaves the failure mark and
// the old tip, and the rewind the mark leads to undoes what was written.
func TestOnePointStoppedBetweenTransactionsRewinds(t *testing.T) {
	t.Parallel()
	s := smallLimitStore(t, t.TempDir())
	defer s.Close()
	start := point(0, 0)
	bootstrap(t, s, start)
	stop := errors.New("stopped between transactions")
	s.SetAfterChunkForTest(func(int) error { return stop })
	changes := bigChanges(1, bigPointOutputs)
	tip := changes[0].Point
	_, err := s.ApplyBatch(&tip, changes)
	require.ErrorIs(t, err, stop)
	s.SetAfterChunkForTest(nil)

	mid, err := s.MidBatchStateForTest()
	require.NoError(t, err)
	require.True(t, mid.Failed)
	require.Equal(t, tip.Slot, mid.FailedAt)
	require.Equal(t, start.Slot, mid.Tip.Slot)
	before, found, err := s.PointBefore(mid.FailedAt)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, start.Slot, before.Slot)
	_, err = s.Rollback(before)
	require.NoError(t, err)
	requireBigPointRolledBack(t, s, start)
}

// A rollback over a block written in several transactions undoes it in as
// many. It records itself pending first, so a stop between them, which
// leaves the tip at the block with part of it undone, is finished at the
// next start rather than read as the block.
func TestRollbackOverASplitPointIsPendingUntilDone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := smallLimitStore(t, dir)
	start := point(0, 0)
	bootstrap(t, s, start)
	tip := applyBigPoint(t, s)
	stop := errors.New("stopped between rollback transactions")
	var steps int
	s.SetAfterRollbackStepForTest(func(done int) error {
		steps++
		if done == 1 {
			return stop
		}
		return nil
	})
	_, err := s.Rollback(start)
	require.ErrorIs(t, err, stop)
	require.NoError(t, s.Close())

	s = smallLimitStore(t, dir)
	defer s.Close()
	target, pending, err := s.PendingRollbackForTest()
	require.NoError(t, err)
	require.True(t, pending, "a part-undone block left no mark")
	require.Equal(t, start.Slot, target.Slot)
	requireTip(t, s, tip)
	resumed, err := s.ResumeRollback()
	require.NoError(t, err)
	require.True(t, resumed)
	requireBigPointRolledBack(t, s, start)
}
