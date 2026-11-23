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
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// bigOut is the i'th output of a synthetic run: unique across i, with a
// payload large enough to count towards the transaction's byte budget.
func bigOut(i uint32) badgerutxo.Output {
	var id [32]byte
	binary.BigEndian.PutUint32(id[:], i)
	return badgerutxo.Output{
		Ref:  badgerutxo.NewRef(id[:], 0),
		Cbor: make([]byte, 60),
	}
}

// bigChanges returns points blocks of perPoint outputs each, at slots
// 1..points.
func bigChanges(points, perPoint int) []badgerutxo.Change {
	changes := make([]badgerutxo.Change, 0, points)
	n := uint32(0)
	for p := range points {
		c := badgerutxo.Change{Point: point(uint64(p+1), byte(p%250+1))}
		for range perPoint {
			c.Added = append(c.Added, bigOut(n))
			n++
		}
		changes = append(changes, c)
	}
	return changes
}

func TestApplyBatchLargerThanOneTransactionApplies(t *testing.T) {
	t.Parallel()
	testutil.SkipUnderRace(
		t,
		"single-goroutine apply of 120k outputs at Badger's real "+
			"transaction limit; the small-budget chunk tests keep the "+
			"chunking under -race",
	)
	s := open(t, t.TempDir())
	defer s.Close()
	start := point(0, 0)
	bootstrap(t, s, start)
	// 400 blocks of 300 outputs is over Badger's per-transaction entry
	// budget for its default memtable.
	const points, perPoint = 400, 300
	changes := bigChanges(points, perPoint)
	tip := changes[len(changes)-1].Point
	res, err := s.ApplyBatch(&tip, changes)
	require.NoError(t, err)
	require.Empty(t, res.Missing)
	require.Empty(t, res.Duplicate)
	requireTip(t, s, tip)
	requireCount(t, s, points*perPoint)
	for _, i := range []uint32{0, 1, points*perPoint/2 + 7, points*perPoint - 1} {
		_, ok, err := s.Get(bigOut(i).Ref)
		require.NoError(t, err)
		require.True(t, ok)
	}
	// The journal reaches back through every block the chunks wrote.
	rb, err := s.Rollback(start)
	require.NoError(t, err)
	require.Len(t, rb.Removed, points*perPoint)
	requireCount(t, s, 0)
	requireTip(t, s, start)
}

const chunkPoints, chunkPerPoint = 12, 10

// chunkedStore opens a store whose plan holds about four of the test's
// points to a transaction, so a batch of chunkPoints points takes three.
func chunkedStore(t *testing.T, dir string) *badgerutxo.Store {
	t.Helper()
	s := open(t, dir)
	s.SetBudgetForTest(2*4*(chunkPerPoint*2+4), 1<<30)
	return s
}

func chunkNonces(changes []badgerutxo.Change) []badgerutxo.Nonce {
	nonces := make([]badgerutxo.Nonce, 0, len(changes))
	for _, c := range changes {
		nonces = append(nonces, badgerutxo.Nonce{
			Point: c.Point,
			Nonce: []byte{byte(c.Point.Slot)},
		})
	}
	return nonces
}

func TestChunkedBatchCommitsTheRowsAndTipLast(t *testing.T) {
	t.Parallel()
	s := chunkedStore(t, t.TempDir())
	defer s.Close()
	start := point(0, 0)
	bootstrap(t, s, start)
	changes := bigChanges(chunkPoints, chunkPerPoint)
	tip := changes[len(changes)-1].Point
	var chunks []uint64
	s.SetAfterChunkForTest(func(done int) error {
		// Between chunks the tip, the nonces and the failure mark are
		// what a crash here would leave.
		m, err := s.MidBatchStateForTest()
		require.NoError(t, err)
		require.True(t, m.HasTip)
		require.Equal(t, start.Slot, m.Tip.Slot)
		require.Zero(t, m.Nonces)
		require.True(t, m.Failed)
		require.Equal(t, changes[0].Point.Slot, m.FailedAt)
		chunks = append(chunks, uint64(done))
		return nil
	})
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Changes: changes,
		Nonces:  chunkNonces(changes),
	})
	require.NoError(t, err)
	require.Greater(t, len(chunks), 1, "the batch should span transactions")
	requireTip(t, s, tip)
	requireCount(t, s, chunkPoints*chunkPerPoint)
	ns, err := s.NoncesInSlotRange(0, tip.Slot+1)
	require.NoError(t, err)
	require.Len(t, ns, chunkPoints)
	_, failed, err := s.Failed()
	require.NoError(t, err)
	require.False(t, failed, "a finished batch leaves no failure mark")
	// Every point has its own journal record, whichever chunk wrote it.
	for _, c := range changes[:len(changes)-1] {
		has, err := s.HasJournalPoint(c.Point)
		require.NoError(t, err)
		require.True(t, has, "slot %d", c.Point.Slot)
	}
	mid := changes[5].Point
	_, err = s.Rollback(mid)
	require.NoError(t, err)
	requireTip(t, s, mid)
	requireCount(t, s, 6*chunkPerPoint)
}

func TestChunkedBatchStoppedBetweenChunksRewindsFromItsMark(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := chunkedStore(t, dir)
	start := point(0, 0)
	bootstrap(t, s, start)
	changes := bigChanges(chunkPoints, chunkPerPoint)
	tip := changes[len(changes)-1].Point
	stop := errors.New("stopped between chunks")
	s.SetAfterChunkForTest(func(int) error { return stop })
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Changes: changes,
		Nonces:  chunkNonces(changes),
	})
	require.ErrorIs(t, err, stop)
	// A crash leaves what the stop does: reopen to prove it is durable.
	require.NoError(t, s.Close())
	s = open(t, dir)
	defer s.Close()
	requireTip(t, s, start)
	n, err := s.Count()
	require.NoError(t, err)
	require.NotZero(t, n, "the first chunk's outputs are in the store")
	slot, failed, err := s.Failed()
	require.NoError(t, err)
	require.True(t, failed)
	before, found, err := s.PointBefore(slot)
	require.NoError(t, err)
	require.True(
		t,
		found,
		"the chunk's journal records are at or past the mark",
	)
	require.Equal(t, start.Slot, before.Slot)
	// The tip is already the point before the batch, so the rewind has to
	// undo records that lie past the tip.
	_, err = s.Rollback(before)
	require.NoError(t, err)
	requireTip(t, s, start)
	requireCount(t, s, 0)
	_, ok, err := s.Get(bigOut(0).Ref)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestChunkedBatchKeepsAnEarlierFailureMark(t *testing.T) {
	t.Parallel()
	s := chunkedStore(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0))
	require.NoError(t, s.MarkFailed(0))
	changes := bigChanges(chunkPoints, chunkPerPoint)
	tip := changes[len(changes)-1].Point
	_, err := s.ApplyBatch(&tip, changes)
	require.NoError(t, err)
	slot, failed, err := s.Failed()
	require.NoError(t, err)
	require.True(t, failed)
	require.Zero(t, slot)
}

func TestChunkedBatchIsHiddenFromReadersUntilItEnds(t *testing.T) {
	t.Parallel()
	s := chunkedStore(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0))
	changes := bigChanges(chunkPoints, chunkPerPoint)
	tip := changes[len(changes)-1].Point
	counts := make(chan uint64, 1)
	var once sync.Once
	s.SetAfterChunkForTest(func(int) error {
		once.Do(func() {
			go func() {
				n, err := s.Count()
				if err != nil {
					n = ^uint64(0)
				}
				counts <- n
			}()
			// A read that begins now waits for the batch: it must not
			// finish while the batch is part written.
			select {
			case n := <-counts:
				t.Errorf("a read finished mid-batch, seeing %d outputs", n)
			case <-time.After(200 * time.Millisecond):
			}
		})
		return nil
	})
	_, err := s.ApplyBatch(&tip, changes)
	require.NoError(t, err)
	select {
	case n := <-counts:
		require.EqualValues(t, chunkPoints*chunkPerPoint, n)
	case <-time.After(10 * time.Second):
		t.Fatal("the read did not complete once the batch ended")
	}
}

func TestApplyBatchHalvesAPlanThatBadgerRefuses(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0))
	// The plan believes everything fits one transaction; the commit says
	// otherwise, and the batch has to land anyway.
	s.SetBudgetForTest(1<<22, 1<<32)
	const points, perPoint = 400, 300
	changes := bigChanges(points, perPoint)
	tip := changes[len(changes)-1].Point
	_, err := s.ApplyBatch(&tip, changes)
	require.NoError(t, err)
	requireTip(t, s, tip)
	requireCount(t, s, points*perPoint)
}
