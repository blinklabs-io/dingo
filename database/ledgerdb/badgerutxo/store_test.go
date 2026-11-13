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
	"math/rand/v2"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func ref(b byte, idx uint32) badgerutxo.Ref {
	return badgerutxo.NewRef(bytes.Repeat([]byte{b}, 32), idx)
}

func out(b byte, idx uint32) badgerutxo.Output {
	return badgerutxo.Output{Ref: ref(b, idx), Cbor: []byte{b, byte(idx)}}
}

func point(slot uint64, b byte) ocommon.Point {
	return ocommon.Point{Slot: slot, Hash: bytes.Repeat([]byte{b}, 32)}
}

func open(t *testing.T, dir string) *badgerutxo.Store {
	t.Helper()
	s, err := badgerutxo.Open(dir)
	require.NoError(t, err)
	return s
}

func bootstrap(
	t *testing.T,
	s *badgerutxo.Store,
	p ocommon.Point,
	outs ...badgerutxo.Output,
) {
	t.Helper()
	require.NoError(
		t,
		s.Bootstrap(p, func(put func(badgerutxo.Output) error) error {
			for _, o := range outs {
				if err := put(o); err != nil {
					return err
				}
			}
			return nil
		}),
	)
}

func requireLive(
	t *testing.T,
	s *badgerutxo.Store,
	live bool,
	r badgerutxo.Ref,
) {
	t.Helper()
	_, ok, err := s.Get(r)
	require.NoError(t, err)
	require.Equal(t, live, ok)
}

func requireCount(t *testing.T, s *badgerutxo.Store, want uint64) {
	t.Helper()
	n, err := s.Count()
	require.NoError(t, err)
	require.Equal(t, want, n)
}

func requireTip(t *testing.T, s *badgerutxo.Store, p ocommon.Point) {
	t.Helper()
	tip, ok, err := s.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, p.Slot, tip.Slot)
	require.Equal(t, p.Hash, tip.Hash)
}

func TestApplyAddsAndRemovesOutputs(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0), out(1, 0), out(1, 1))
	blk := point(10, 9)
	res, err := s.Apply(
		blk,
		[]badgerutxo.Output{out(2, 0), out(2, 1)},
		[]badgerutxo.Ref{ref(1, 0)},
	)
	require.NoError(t, err)
	require.Empty(t, res.Missing)
	require.Empty(t, res.Duplicate)
	requireLive(t, s, false, ref(1, 0))
	requireLive(t, s, true, ref(1, 1))
	requireLive(t, s, true, ref(2, 0))
	requireLive(t, s, true, ref(2, 1))
	requireCount(t, s, 3)
	requireTip(t, s, blk)

	// A rollback arrives as the outputs it restores and removes.
	back := point(0, 0)
	_, err = s.Apply(
		back,
		[]badgerutxo.Output{out(1, 0)},
		[]badgerutxo.Ref{ref(2, 0), ref(2, 1)},
	)
	require.NoError(t, err)
	requireLive(t, s, true, ref(1, 0))
	requireLive(t, s, false, ref(2, 0))
	requireCount(t, s, 2)
	requireTip(t, s, back)
}

func TestApplyReportsMissingAndDuplicate(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0), out(1, 0))
	res, err := s.Apply(
		point(1, 1),
		[]badgerutxo.Output{out(1, 0)},
		[]badgerutxo.Ref{ref(7, 0)},
	)
	require.NoError(t, err)
	require.Equal(t, []badgerutxo.Ref{ref(7, 0)}, res.Missing)
	require.Equal(t, []badgerutxo.Ref{ref(1, 0)}, res.Duplicate)
	requireCount(t, s, 1)
}

func TestStatePersistsAcrossReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := open(t, dir)
	bootstrap(t, s, point(0, 0), out(1, 0))
	blk := point(3, 3)
	_, err := s.Apply(blk, []badgerutxo.Output{out(2, 0)}, nil)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	s = open(t, dir)
	defer s.Close()
	requireCount(t, s, 2)
	requireTip(t, s, blk)
	requireLive(t, s, true, ref(2, 0))
}

func TestBootstrapReplacesContents(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0), out(1, 0))
	anchor := point(9, 9)
	bootstrap(t, s, anchor, out(4, 0), out(4, 1))
	requireLive(t, s, false, ref(1, 0))
	requireCount(t, s, 2)
	requireTip(t, s, anchor)
}

func TestEachVisitsEveryLiveOutput(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0), out(1, 0), out(2, 3))
	var got []badgerutxo.Ref
	require.NoError(t, s.Each(func(o badgerutxo.Output) error {
		got = append(got, o.Ref)
		return nil
	}))
	require.ElementsMatch(t, []badgerutxo.Ref{ref(1, 0), ref(2, 3)}, got)
}

func TestSampleReturnsDistinctLiveOutputs(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	outs := make([]badgerutxo.Output, 0, 20)
	for i := range 20 {
		outs = append(outs, out(byte(i+1), 0))
	}
	bootstrap(t, s, point(0, 0), outs...)
	got, err := s.Sample(50, rand.New(rand.NewPCG(1, 2)))
	require.NoError(t, err)
	require.Len(t, got, 20)
	seen := map[badgerutxo.Ref]bool{}
	for _, o := range got {
		require.False(t, seen[o.Ref], "duplicate in sample")
		seen[o.Ref] = true
	}
}

func TestApplyBatchIsOneChangeAndKeepsTipWhenNil(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0), out(1, 0))
	blk := point(5, 5)
	// The second change spends an output the first creates.
	_, err := s.ApplyBatch(&blk, []badgerutxo.Change{
		{Point: blk, Added: []badgerutxo.Output{out(2, 0)}, Removed: []badgerutxo.Ref{ref(1, 0)}},
		{Point: blk, Added: []badgerutxo.Output{out(3, 0)}, Removed: []badgerutxo.Ref{ref(2, 0)}},
	})
	require.NoError(t, err)
	requireLive(t, s, false, ref(1, 0))
	requireLive(t, s, false, ref(2, 0))
	requireLive(t, s, true, ref(3, 0))
	requireCount(t, s, 1)
	requireTip(t, s, blk)

	_, err = s.ApplyBatch(nil, []badgerutxo.Change{
		{Point: ocommon.Point{Slot: 6}, Removed: []badgerutxo.Ref{ref(3, 0)}},
	})
	require.NoError(t, err)
	requireCount(t, s, 0)
	requireTip(t, s, blk)
}

func TestStoreWithoutFormatMarkIsStaleUntilRebuilt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := open(t, dir)
	stale, err := s.Stale()
	require.NoError(t, err)
	require.False(t, stale, "a new store is current")
	_, err = s.Apply(point(1, 1), []badgerutxo.Output{out(1, 0)}, nil)
	require.NoError(t, err)
	require.NoError(t, s.DeleteFormatForTest())
	require.NoError(t, s.Close())

	s = open(t, dir)
	defer s.Close()
	stale, err = s.Stale()
	require.NoError(t, err)
	require.True(t, stale, "a store from an older format stays stale on reopen")
	bootstrap(t, s, point(2, 2), out(2, 0))
	stale, err = s.Stale()
	require.NoError(t, err)
	require.False(t, stale)
}
