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
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// journaledRows is n puts under the test prefix Z<epoch> that each keep a
// journal entry of their own, as a boundary's reward folds do.
func journaledRows(slot uint64, epoch byte, n int, value byte) []badgerutxo.RowPut {
	prefix := []byte{'Z', epoch}
	rows := make([]badgerutxo.RowPut, 0, n)
	for i := range n {
		rows = append(rows, badgerutxo.RowPut{
			Slot: slot,
			Key: binary.BigEndian.AppendUint32(
				bytes.Clone(prefix),
				uint32(i), //nolint:gosec // a test index
			),
			Value: []byte{value},
		})
	}
	return rows
}

// A rollback across one commit whose journaled rows exceed Badger's
// transaction limit restores them in as many transactions as it takes. A
// boundary commit journals a row per credited account, far more than one
// transaction holds, and the rollback of a block after it must still run.
func TestRollbackRestoresMoreJournaledRowsThanOneTransactionHolds(t *testing.T) {
	t.Parallel()
	testutil.SkipUnderRace(
		t,
		"single-goroutine rollback over 70k journaled rows at "+
			"Badger's real transaction limit; the small-budget "+
			"rollback tests keep the chunking under -race",
	)
	s := open(t, t.TempDir())
	defer s.Close()
	start := point(0, 0)
	bootstrap(t, s, start)
	tip := point(20, 0x14)
	const n = 70_000
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Rows: journaledRows(tip.Slot, 7, n, 1),
	})
	require.NoError(t, err)
	require.Equal(t, n, countRows(t, s, []byte{'Z', 7}))

	_, err = s.Rollback(start)
	require.NoError(t, err)
	requireTip(t, s, start)
	require.Zero(t, countRows(t, s, []byte{'Z', 7}))
}

// rollbackBudget makes a transaction of about 50 undone rows.
const rollbackBudget = 2 * 6 * 50

func smallRollbackStore(t *testing.T, dir string) *badgerutxo.Store {
	t.Helper()
	s := open(t, dir)
	s.SetBudgetForTest(rollbackBudget, 1<<30)
	return s
}

func seedRollback(t *testing.T, s *badgerutxo.Store) (mid, tip ocommon.Point) {
	t.Helper()
	bootstrap(t, s, point(0, 0))
	mid, tip = point(10, 0x0a), point(20, 0x14)
	_, err := s.ApplyLedgerBatch(&mid, badgerutxo.LedgerBatch{
		Rows: journaledRows(mid.Slot, 7, 120, 9),
	})
	require.NoError(t, err)
	_, err = s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Rows: slices.Concat(
			journaledRows(tip.Slot, 7, 300, 1),
			bulkRows(tip.Slot, 8, 200, 1),
		),
	})
	require.NoError(t, err)
	return mid, tip
}

func requireRolledBackToMid(t *testing.T, s *badgerutxo.Store, mid ocommon.Point) {
	t.Helper()
	requireTip(t, s, mid)
	require.Equal(t, 120, countRows(t, s, []byte{'Z', 7}))
	require.Zero(t, countRows(t, s, []byte{'Z', 8}))
	for i := range 120 {
		v, ok, err := s.Row(binary.BigEndian.AppendUint32([]byte{'Z', 7}, uint32(i))) //nolint:gosec // a test index
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []byte{9}, v)
	}
	_, ok, err := s.PendingRollbackForTest()
	require.NoError(t, err)
	require.False(t, ok, "the rollback's last transaction clears the mark")
}

// A rollback that stops between its transactions leaves the pending mark and
// a tip ahead of the target; the next start finishes it, whichever
// transaction it stopped after, including right after the rows under the
// undone range tombstones were dropped.
func TestRollbackStoppedBetweenChunksResumesFromTheMark(t *testing.T) {
	t.Parallel()
	for _, stopAt := range []int{0, 1, 3, 7, 8} {
		dir := t.TempDir()
		s := smallRollbackStore(t, dir)
		mid, tip := seedRollback(t, s)
		stop := errors.New("stopped between rollback transactions")
		s.SetAfterRollbackStepForTest(func(done int) error {
			if done == stopAt {
				return stop
			}
			return nil
		})
		_, err := s.Rollback(mid)
		require.ErrorIs(t, err, stop, "stopAt=%d", stopAt)
		require.NoError(t, s.Close())

		s = open(t, dir)
		target, pending, err := s.PendingRollbackForTest()
		require.NoError(t, err)
		require.True(t, pending, "stopAt=%d: no mark left", stopAt)
		require.Equal(t, mid.Slot, target.Slot)
		if stopAt < 8 {
			// Only the transaction that undoes the batch moves the tip.
			requireTip(t, s, tip)
		} else {
			requireTip(t, s, mid)
		}

		resumed, err := s.ResumeRollback()
		require.NoError(t, err)
		require.True(t, resumed, "stopAt=%d", stopAt)
		requireRolledBackToMid(t, s, mid)
		resumed, err = s.ResumeRollback()
		require.NoError(t, err)
		require.False(t, resumed)
		require.NoError(t, s.Close())
	}
}

// A store with a rollback pending takes no batch until it is finished.
func TestApplyRefusedWhileARollbackIsPending(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := smallRollbackStore(t, dir)
	defer s.Close()
	mid, tip := seedRollback(t, s)
	stop := errors.New("stopped")
	s.SetAfterRollbackStepForTest(func(int) error { return stop })
	_, err := s.Rollback(mid)
	require.ErrorIs(t, err, stop)

	next := point(30, 0x1e)
	_, err = s.ApplyLedgerBatch(&next, badgerutxo.LedgerBatch{})
	require.ErrorIs(t, err, badgerutxo.ErrRollbackPending)
	requireTip(t, s, tip)
}

// A rollback made in one transaction leaves no mark and takes no step.
func TestSmallRollbackLeavesNoMark(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0))
	tip := point(10, 0x0a)
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Rows: journaledRows(tip.Slot, 7, 5, 1),
	})
	require.NoError(t, err)
	var steps int
	s.SetAfterRollbackStepForTest(func(int) error { steps++; return nil })
	_, err = s.Rollback(point(0, 0))
	require.NoError(t, err)
	require.Zero(t, steps)
	_, pending, err := s.PendingRollbackForTest()
	require.NoError(t, err)
	require.False(t, pending)
}

// A batch that carries a rollback across more state journal entries than one
// transaction holds undoes them in bounded transactions first.
func TestBatchRollbackUndoesMoreJournaledRowsThanOneTransactionHolds(t *testing.T) {
	t.Parallel()
	testutil.SkipUnderRace(
		t,
		"single-goroutine batch rollback over 70k journaled rows "+
			"at Badger's real transaction limit; the small-budget "+
			"rollback tests keep the chunking under -race",
	)
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0))
	mid, tip := point(10, 0x0a), point(20, 0x14)
	_, err := s.ApplyLedgerBatch(&mid, badgerutxo.LedgerBatch{
		Rows: journaledRows(mid.Slot, 7, 1_000, 9),
	})
	require.NoError(t, err)
	_, err = s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Rows: journaledRows(tip.Slot, 7, 70_000, 1),
	})
	require.NoError(t, err)

	to := mid.Slot
	next := point(15, 0x0f)
	_, err = s.ApplyLedgerBatch(&next, badgerutxo.LedgerBatch{
		DeleteNoncesAfter: &to,
		Rows:              journaledRows(15, 8, 3, 5),
	})
	require.NoError(t, err)
	requireTip(t, s, next)
	require.Equal(t, 1_000, countRows(t, s, []byte{'Z', 7}))
	v, ok, err := s.Row(binary.BigEndian.AppendUint32([]byte{'Z', 7}, 5))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte{9}, v)
	require.Equal(t, 3, countRows(t, s, []byte{'Z', 8}))
	_, failed, err := s.Failed()
	require.NoError(t, err)
	require.False(t, failed, "the batch clears the mark it set for its rollback")
}

// A batch rollback that stops between its transactions leaves a failure mark
// the next start rewinds from, to the point the rollback was going to.
func TestBatchRollbackStoppedBetweenChunksLeavesAMarkThatRewindsToTheTarget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := smallRollbackStore(t, dir)
	mid, tip := seedRollback(t, s)
	stop := errors.New("stopped")
	s.SetAfterRollbackStepForTest(func(done int) error {
		if done == 2 {
			return stop
		}
		return nil
	})
	to := mid.Slot
	next := point(15, 0x0f)
	_, err := s.ApplyLedgerBatch(&next, badgerutxo.LedgerBatch{
		DeleteNoncesAfter: &to,
		Rows:              journaledRows(15, 9, 3, 5),
	})
	require.ErrorIs(t, err, stop)
	require.NoError(t, s.Close())

	s = open(t, dir)
	defer s.Close()
	requireTip(t, s, tip)
	slot, failed, err := s.Failed()
	require.NoError(t, err)
	require.True(t, failed)
	require.Equal(t, mid.Slot+1, slot)
	before, found, err := s.PointBefore(slot)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, mid.Slot, before.Slot)
	_, err = s.Rollback(before)
	require.NoError(t, err)
	requireRolledBackToMid(t, s, mid)
}

// RollbackReaches answers, without changing the store, whether Rollback would
// refuse a point as older than the journal. A point the lineage never held is
// refused by Rollback for another reason, and is not reported as too old.
func TestRollbackReachesAgreesWithRollbackOnJournalDepth(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	start := point(0, 0)
	bootstrap(t, s, start)
	mid, tip := point(10, 0x0a), point(20, 0x14)
	for _, p := range []ocommon.Point{mid, tip} {
		_, err := s.ApplyLedgerBatch(&p, badgerutxo.LedgerBatch{
			Rows: journaledRows(p.Slot, 7, 3, 1),
		})
		require.NoError(t, err)
	}
	reaches := func(p ocommon.Point) bool {
		ok, err := s.RollbackReaches(p)
		require.NoError(t, err)
		return ok
	}
	require.True(t, reaches(start))
	require.True(t, reaches(mid))
	require.True(t, reaches(tip))

	// The journal now starts at the commit that moved the tip off mid.
	require.NoError(t, s.PruneJournal(mid.Slot+1))
	require.True(t, reaches(tip))
	require.True(t, reaches(mid))
	require.False(t, reaches(start))
	require.True(t, reaches(point(15, 0x0f)),
		"a point the lineage never held is not one the journal is too short for")
	requireTip(t, s, tip)

	_, err := s.Rollback(start)
	require.ErrorIs(t, err, badgerutxo.ErrRollbackBeyondJournal)
	_, err = s.Rollback(mid)
	require.NoError(t, err)
	requireTip(t, s, mid)
}
