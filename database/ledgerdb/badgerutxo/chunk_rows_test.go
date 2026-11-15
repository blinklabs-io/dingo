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
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// bulkRows is a replaced epoch of n rows under the test prefix Z<epoch>: the
// wipe, the tombstone, and the rows, which carry no journal entry of their
// own.
func bulkRows(slot uint64, epoch byte, n int, value byte) []badgerutxo.RowPut {
	prefix := []byte{'Z', epoch}
	rows := []badgerutxo.RowPut{
		{Slot: slot, Key: prefix, WipePrefix: true},
		{Slot: slot, Key: prefix, TombstonePrefix: true},
	}
	for i := range n {
		rows = append(rows, badgerutxo.RowPut{
			Slot: slot,
			Key: binary.BigEndian.AppendUint32(
				bytes.Clone(prefix),
				uint32(i), //nolint:gosec // a test index
			),
			Value:       []byte{value},
			SkipJournal: true,
		})
	}
	return rows
}

func countRows(t *testing.T, s *badgerutxo.Store, prefix []byte) int {
	t.Helper()
	n := 0
	require.NoError(t, s.Rows(prefix, func(_, _ []byte) error {
		n++
		return nil
	}))
	return n
}

// Rows too many for one transaction go in several, before the batch's tip,
// under the failure mark, with no UTxO change in the batch to carry the mark's
// slot.
func TestChunkedRowsSpanTransactionsBeforeTheTip(t *testing.T) {
	t.Parallel()
	s := chunkedStore(t, t.TempDir())
	defer s.Close()
	start := point(0, 0)
	bootstrap(t, s, start)
	tip := point(20, 0x14)
	var chunks int
	s.SetAfterChunkForTest(func(done int) error {
		m, err := s.MidBatchStateForTest()
		require.NoError(t, err)
		require.Equal(
			t,
			start.Slot,
			m.Tip.Slot,
			"the tip moves with the last chunk",
		)
		require.True(t, m.Failed)
		require.Equal(t, tip.Slot, m.FailedAt)
		chunks = done
		return nil
	})
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Rows: bulkRows(tip.Slot, 7, 200, 1),
	})
	require.NoError(t, err)
	require.Greater(t, chunks, 1, "the rows should span transactions")
	requireTip(t, s, tip)
	require.Equal(t, 200, countRows(t, s, []byte{'Z', 7}))
	_, failed, err := s.Failed()
	require.NoError(t, err)
	require.False(t, failed, "the last chunk clears the mark")

	// One tombstone stands for all of them.
	_, err = s.Rollback(start)
	require.NoError(t, err)
	require.Zero(t, countRows(t, s, []byte{'Z', 7}))
}

// A batch of rows that stopped between chunks leaves partial rows and the
// failure mark; applying the epoch again replaces them, so a different set of
// rows leaves none of the stopped attempt's.
func TestChunkedRowsStoppedBetweenChunksAreReplacedByTheReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := chunkedStore(t, dir)
	start := point(0, 0)
	bootstrap(t, s, start)
	tip := point(20, 0x14)
	stop := errors.New("stopped between chunks")
	s.SetAfterChunkForTest(func(int) error { return stop })
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Rows: bulkRows(tip.Slot, 7, 200, 1),
	})
	require.ErrorIs(t, err, stop)
	require.NoError(t, s.Close())

	s = chunkedStore(t, dir)
	defer s.Close()
	requireTip(t, s, start)
	partial := countRows(t, s, []byte{'Z', 7})
	require.Positive(t, partial)
	require.Less(t, partial, 200)
	slot, failed, err := s.Failed()
	require.NoError(t, err)
	require.True(t, failed)
	require.Equal(t, tip.Slot, slot)

	require.NoError(t, s.ClearFailed())
	_, err = s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Rows: bulkRows(tip.Slot, 7, 50, 2),
	})
	require.NoError(t, err)
	require.Equal(t, 50, countRows(t, s, []byte{'Z', 7}))
	v, ok, err := s.Row(binary.BigEndian.AppendUint32([]byte{'Z', 7}, 49))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte{2}, v)
}

// A batch that carries a rollback and rows too many for one transaction
// applies the rollback before the rows, so the rows the rollback would delete
// are the ones it finds, not the batch's own.
func TestChunkedRowsApplyAfterTheBatchsRollback(t *testing.T) {
	t.Parallel()
	s := chunkedStore(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0))
	apply := func(tip ocommon.Point, b badgerutxo.LedgerBatch) {
		_, err := s.ApplyLedgerBatch(&tip, b)
		require.NoError(t, err)
	}
	apply(point(20, 0x14), badgerutxo.LedgerBatch{
		Rows: bulkRows(20, 7, 200, 1),
	})
	to := uint64(10)
	apply(point(10, 0x0a), badgerutxo.LedgerBatch{
		DeleteNoncesAfter: &to,
		Rows:              bulkRows(30, 8, 150, 3),
	})
	require.Zero(t, countRows(t, s, []byte{'Z', 7}), "the rollback ran")
	require.Equal(
		t,
		150,
		countRows(t, s, []byte{'Z', 8}),
		"and the rows landed",
	)
}

// A rollback past a tombstone deletes the rows under it in as many
// transactions as it takes, however it is made: the journal's rollback or the
// batch's.
func TestRollbackDropsATombstonedPrefixLargerThanOneTransaction(t *testing.T) {
	t.Parallel()
	for _, viaBatch := range []bool{false, true} {
		s := open(t, t.TempDir())
		t.Cleanup(func() { _ = s.Close() })
		bootstrap(t, s, point(0, 0))
		tip := point(20, 0x14)
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
			Rows: bulkRows(tip.Slot, 7, 300_000, 1),
		})
		require.NoError(t, err)
		require.Equal(t, 300_000, countRows(t, s, []byte{'Z', 7}))
		back := point(10, 0x0a)
		if viaBatch {
			to := back.Slot
			_, err = s.ApplyLedgerBatch(
				&back, badgerutxo.LedgerBatch{DeleteNoncesAfter: &to},
			)
		} else {
			// The journal rolls back only to a point the set held.
			_, err = s.Rollback(point(0, 0))
		}
		require.NoError(t, err, "batch=%t", viaBatch)
		require.Zero(t, countRows(t, s, []byte{'Z', 7}), "batch=%t", viaBatch)
	}
}
