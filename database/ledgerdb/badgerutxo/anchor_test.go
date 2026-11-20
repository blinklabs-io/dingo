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
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

// applyBlocks applies blocks first..last at slot 10*i, each adding output
// out(slot byte, 0) and removing the previous block's output.
func applyBlocks(t *testing.T, s *badgerutxo.Store, first byte, last byte) {
	t.Helper()
	for i := first; i <= last; i++ {
		tip := point(uint64(i)*10, i)
		_, err := s.ApplyBatch(&tip, []badgerutxo.Change{{
			Point:   tip,
			Added:   []badgerutxo.Output{out(i, 0)},
			Removed: []badgerutxo.Ref{ref(i-1, 0)},
		}})
		require.NoError(t, err)
	}
}

func TestAnchorViewIsUnknownBeforeAnAnchor(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(10, 1), out(1, 0))
	_, err := s.AnchorView()
	require.ErrorIs(t, err, badgerutxo.ErrAnchorUnknown)
}

// TestAnchorViewReadsTheSetAsOfTheAnchor applies blocks past the anchor and
// requires the view to hold the outputs live at the anchor while the live set
// keeps the newer ones.
func TestAnchorViewReadsTheSetAsOfTheAnchor(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(10, 1), out(1, 0))
	applyBlocks(t, s, 2, 3)
	atAnchor := contents(t, s)
	s.SetAnchor(point(30, 3))
	applyBlocks(t, s, 4, 6)
	live := contents(t, s)
	require.NotEqual(t, atAnchor, live)

	v, err := s.AnchorView()
	require.NoError(t, err)
	defer v.Close()
	got := map[badgerutxo.Ref]string{}
	require.NoError(t, v.Each(func(o badgerutxo.Output) error {
		got[o.Ref] = string(o.Cbor)
		return nil
	}))
	require.Equal(t, atAnchor, got)
	_, ok, err := v.Get(ref(6, 0))
	require.NoError(t, err)
	require.False(t, ok, "an output created after the anchor")
	_, ok, err = v.Get(ref(3, 0))
	require.NoError(t, err)
	require.True(t, ok, "an output spent after the anchor")

	require.Equal(t, live, contents(t, s))
	requireTip(t, s, point(60, 6))
}

// TestAnchorViewRefusesAnAnchorBelowTheJournalFloor prunes past the anchor
// before it is recorded, as a store reopened without one would.
func TestAnchorViewRefusesAnAnchorBelowTheJournalFloor(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(10, 1), out(1, 0))
	applyBlocks(t, s, 2, 6)
	require.NoError(t, s.PruneJournal(50))
	s.SetAnchor(point(20, 2))
	_, err := s.AnchorView()
	require.ErrorIs(t, err, badgerutxo.ErrAnchorBelowJournalFloor)
	require.ErrorIs(t, err, badgerutxo.ErrRollbackBeyondJournal)
}

// TestPruneJournalNeverRaisesTheFloorPastTheAnchor requires a prune request
// beyond the anchor to leave the anchor reachable: the view of it and a
// rollback to it both still work.
func TestPruneJournalNeverRaisesTheFloorPastTheAnchor(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(10, 1), out(1, 0))
	applyBlocks(t, s, 2, 3)
	atAnchor := contents(t, s)
	s.SetAnchor(point(30, 3))
	applyBlocks(t, s, 4, 8)

	require.NoError(t, s.PruneJournal(80))

	v, err := s.AnchorView()
	require.NoError(t, err)
	v.Close()
	floor, ok, err := s.JournalFloor()
	require.NoError(t, err)
	require.True(t, ok)
	require.LessOrEqual(t, floor.Slot, uint64(30))
	require.NoError(t, s.StateReachesSlot(30))
	_, err = s.Rollback(point(30, 3))
	require.NoError(t, err)
	require.Equal(t, atAnchor, contents(t, s))
}

// TestAsOfEachListsOutputsInReferenceOrder requires a view to yield the
// outputs it restored from the journal in between the ones the live set kept,
// by reference, as a digest over them in that order needs.
func TestAsOfEachListsOutputsInReferenceOrder(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(10, 1), out(1, 0), out(5, 0), out(5, 1), out(9, 0))
	tip := point(20, 2)
	_, err := s.ApplyBatch(&tip, []badgerutxo.Change{{
		Point:   tip,
		Added:   []badgerutxo.Output{out(3, 0), out(7, 0)},
		Removed: []badgerutxo.Ref{ref(5, 0), ref(5, 1)},
	}})
	require.NoError(t, err)

	v, err := s.AsOf(point(10, 1))
	require.NoError(t, err)
	defer v.Close()
	var got []badgerutxo.Ref
	require.NoError(t, v.Each(func(o badgerutxo.Output) error {
		got = append(got, o.Ref)
		return nil
	}))
	require.Equal(
		t,
		[]badgerutxo.Ref{ref(1, 0), ref(5, 0), ref(5, 1), ref(9, 0)},
		got,
	)
}
