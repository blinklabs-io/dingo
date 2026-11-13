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
	"math/rand/v2"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func contents(t *testing.T, s *badgerutxo.Store) map[badgerutxo.Ref]string {
	t.Helper()
	m := map[badgerutxo.Ref]string{}
	require.NoError(t, s.Each(func(o badgerutxo.Output) error {
		m[o.Ref] = string(o.Cbor)
		return nil
	}))
	n, err := s.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(len(m)), n, "count")
	return m
}

// TestRollbackRestoresEveryEarlierState applies a random sequence of batches,
// including outputs created and spent in one batch, re-added outputs, spends
// of outputs that are not live and a batch without a tip, and requires a
// rollback to each earlier point to restore that point's set exactly and to
// report the difference as its net change.
func TestRollbackRestoresEveryEarlierState(t *testing.T) {
	t.Parallel()
	for seed := range uint64(8) {
		rng := rand.New(rand.NewPCG(seed, 99))
		dir := t.TempDir()
		s := open(t, dir)
		bootstrap(t, s, point(1, 1), out(1, 0), out(1, 1))
		type snap struct {
			at  ocommon.Point
			set map[badgerutxo.Ref]string
		}
		snaps := []snap{{point(1, 1), contents(t, s)}}
		for i := 2; i <= 12; i++ {
			live := contents(t, s)
			var refs []badgerutxo.Ref
			for r := range live {
				refs = append(refs, r)
			}
			var c badgerutxo.Change
			for _, r := range refs {
				if rng.IntN(3) == 0 {
					c.Removed = append(c.Removed, r)
				}
			}
			c.Removed = append(c.Removed, ref(200, uint32(i))) // never live
			fresh := out(byte(i), 0)
			c.Added = append(c.Added, fresh, out(byte(i), 1))
			if len(refs) > 0 && rng.IntN(2) == 0 {
				c.Added = append(c.Added, badgerutxo.Output{
					Ref: refs[0], Cbor: []byte{0xee, byte(i)},
				})
			}
			// A second change spends an output the first created.
			spend := badgerutxo.Change{Removed: []badgerutxo.Ref{fresh.Ref}}
			slot := uint64(i * 10)
			tip := point(slot, byte(i))
			c.Point, spend.Point = tip, tip
			if i == 7 {
				avvm := ocommon.Point{Slot: slot}
				c.Point, spend.Point = avvm, avvm
				_, err := s.ApplyBatch(nil, []badgerutxo.Change{c, spend})
				require.NoError(t, err)
				continue
			}
			_, err := s.ApplyBatch(&tip, []badgerutxo.Change{c, spend})
			require.NoError(t, err)
			snaps = append(snaps, snap{tip, contents(t, s)})
		}
		require.NoError(t, s.Close())
		s = open(t, dir)
		for _, want := range snaps {
			requireAsOf(t, s, want.at, want.set)
		}
		for i := len(snaps) - 2; i >= 0; i-- {
			from := contents(t, s)
			want := snaps[i]
			res, err := s.Rollback(want.at)
			require.NoError(t, err)
			got := contents(t, s)
			require.Equal(
				t,
				want.set,
				got,
				"seed %d rollback to slot %d",
				seed,
				want.at.Slot,
			)
			requireTip(t, s, want.at)
			added := map[badgerutxo.Ref]string{}
			for _, o := range res.Added {
				added[o.Ref] = string(o.Cbor)
			}
			removed := map[badgerutxo.Ref]bool{}
			for _, r := range res.Removed {
				removed[r] = true
			}
			for r, v := range want.set {
				if from[r] != v {
					require.Equal(t, v, added[r], "net added %x", r)
				}
			}
			for r := range from {
				if _, ok := want.set[r]; !ok {
					require.True(t, removed[r], "net removed %x", r)
				}
			}
			// Rolling back to the batches' slots removes every
			// record but the ones at or before the point.
			if i > 0 {
				snaps = snaps[:i+1]
			}
		}
		require.NoError(t, s.Close())
	}
}

func TestRollbackBeyondThePrunedJournalChangesNothing(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(1, 1), out(1, 0))
	for i := 2; i <= 4; i++ {
		tip := point(uint64(i*10), byte(i))
		_, err := s.ApplyBatch(&tip, []badgerutxo.Change{
			{Point: tip, Added: []badgerutxo.Output{out(byte(i), 0)}},
		})
		require.NoError(t, err)
	}
	require.NoError(t, s.PruneJournal(30))
	before := contents(t, s)
	_, err := s.AsOf(point(10, 1))
	require.ErrorIs(t, err, badgerutxo.ErrRollbackBeyondJournal)
	_, err = s.Rollback(point(10, 1))
	require.ErrorIs(t, err, badgerutxo.ErrRollbackBeyondJournal)
	require.Equal(t, before, contents(t, s))
	requireTip(t, s, point(40, 4))

	// The record at slot 30 remains, so rolling back to slot 20, the tip
	// before it, still works.
	_, err = s.Rollback(point(20, 2))
	require.NoError(t, err)
	requireLive(t, s, false, ref(3, 0))
	requireLive(t, s, true, ref(2, 0))
	requireTip(t, s, point(20, 2))
}

// TestRollbackStopsBetweenBlocksOfOneBatch applies two blocks in one batch
// and rolls back to the first: only the second block's changes are undone.
func TestRollbackStopsBetweenBlocksOfOneBatch(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(1, 1), out(1, 0))
	first, second := point(10, 10), point(20, 20)
	_, err := s.ApplyBatch(&second, []badgerutxo.Change{
		{
			Point:   first,
			Added:   []badgerutxo.Output{out(2, 0)},
			Removed: []badgerutxo.Ref{ref(1, 0)},
		},
		{
			Point:   second,
			Added:   []badgerutxo.Output{out(3, 0)},
			Removed: []badgerutxo.Ref{ref(2, 0)},
		},
	})
	require.NoError(t, err)
	_, err = s.Rollback(first)
	require.NoError(t, err)
	requireLive(t, s, false, ref(1, 0))
	requireLive(t, s, true, ref(2, 0))
	requireLive(t, s, false, ref(3, 0))
	requireTip(t, s, first)
}

// requireAsOf requires the view of s as of at to hold exactly want, through
// both Each and Get, and to leave s unchanged.
func requireAsOf(
	t *testing.T,
	s *badgerutxo.Store,
	at ocommon.Point,
	want map[badgerutxo.Ref]string,
) {
	t.Helper()
	before := contents(t, s)
	v, err := s.AsOf(at)
	require.NoError(t, err)
	defer v.Close()
	got := map[badgerutxo.Ref]string{}
	require.NoError(t, v.Each(func(o badgerutxo.Output) error {
		_, dup := got[o.Ref]
		require.False(t, dup, "output %x listed twice", o.Ref)
		got[o.Ref] = string(o.Cbor)
		return nil
	}))
	require.Equal(t, want, got, "as of slot %d", at.Slot)
	for r := range before {
		c, ok, err := v.Get(r)
		require.NoError(t, err)
		w, live := want[r]
		require.Equal(t, live, ok, "get %x as of slot %d", r, at.Slot)
		require.Equal(t, w, string(c))
	}
	for r, w := range want {
		c, ok, err := v.Get(r)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, w, string(c))
	}
	require.Equal(t, before, contents(t, s))
}

// TestNothingBeforeABootstrapIsRestored bootstraps a set at a point and
// requires a rollback, preview or view of an earlier point to fail and change
// nothing, since the journal holds no history before the bootstrap, while the
// bootstrap point itself stays reachable once a batch follows it.
func TestNothingBeforeABootstrapIsRestored(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(100, 1), out(1, 0))
	before := contents(t, s)
	_, err := s.AsOf(point(50, 5))
	require.ErrorIs(t, err, badgerutxo.ErrRollbackBeyondJournal)
	_, err = s.RollbackPreview(point(50, 5))
	require.ErrorIs(t, err, badgerutxo.ErrRollbackBeyondJournal)
	_, err = s.Rollback(point(50, 5))
	require.ErrorIs(t, err, badgerutxo.ErrRollbackBeyondJournal)
	require.Equal(t, before, contents(t, s))
	requireTip(t, s, point(100, 1))
	requireAsOf(t, s, point(100, 1), before)

	tip := point(110, 2)
	_, err = s.ApplyBatch(&tip, []badgerutxo.Change{
		{Point: tip, Added: []badgerutxo.Output{out(2, 0)}},
	})
	require.NoError(t, err)
	requireAsOf(t, s, point(100, 1), before)
	_, err = s.AsOf(point(50, 5))
	require.ErrorIs(t, err, badgerutxo.ErrRollbackBeyondJournal)
	_, err = s.Rollback(point(100, 1))
	require.NoError(t, err)
	require.Equal(t, before, contents(t, s))
}
