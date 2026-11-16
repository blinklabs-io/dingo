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
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func nonce(slot uint64, b byte, checkpoint bool) badgerutxo.Nonce {
	return badgerutxo.Nonce{
		Point:      point(slot, b),
		Nonce:      []byte{b, b},
		Checkpoint: checkpoint,
	}
}

func applyNonces(
	t *testing.T,
	s *badgerutxo.Store,
	tip ocommon.Point,
	prune uint64,
	nonces ...badgerutxo.Nonce,
) {
	t.Helper()
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Nonces:            nonces,
		PruneNoncesBefore: prune,
	})
	require.NoError(t, err)
}

func slots(ns []badgerutxo.Nonce) []uint64 {
	out := make([]uint64, 0, len(ns))
	for _, n := range ns {
		out = append(out, n.Point.Slot)
	}
	return out
}

func TestNoncesReadByPointRangeAndLatest(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	applyNonces(t, s, point(30, 0x30), 0,
		nonce(10, 0x10, false), nonce(20, 0x20, true), nonce(30, 0x30, false),
	)

	got, ok, err := s.Nonce(point(20, 0x20))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte{0x20, 0x20}, got)
	_, ok, err = s.Nonce(point(20, 0x21))
	require.NoError(t, err)
	require.False(t, ok, "a nonce is keyed by the block's hash too")

	inRange, err := s.NoncesInSlotRange(10, 30)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20}, slots(inRange))
	require.True(t, inRange[1].Checkpoint)

	latest, ok, err := s.LatestNonce()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(30), latest.Point.Slot)
}

func TestNoncePruneKeepsCheckpoints(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	applyNonces(t, s, point(30, 0x30), 0,
		nonce(10, 0x10, false), nonce(20, 0x20, true), nonce(30, 0x30, false),
	)
	applyNonces(t, s, point(40, 0x40), 30, nonce(40, 0x40, false))

	all, err := s.NoncesInSlotRange(0, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{20, 30, 40}, slots(all))
}

// Rewriting a nonce keeps its checkpoint flag once set, as the metadata
// store's upsert does, and takes the new nonce.
func TestNonceRewriteKeepsTheCheckpointFlag(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	rewrite := func(n badgerutxo.Nonce, b byte, checkpoint bool) badgerutxo.Nonce {
		n.Nonce, n.Checkpoint = []byte{b, b}, checkpoint
		return n
	}
	applyNonces(t, s, point(10, 0x10), 0, nonce(10, 0x10, true))
	applyNonces(t, s, point(20, 0x20), 0,
		rewrite(nonce(10, 0x10, true), 0x11, false), nonce(20, 0x20, false))
	applyNonces(t, s, point(30, 0x30), 0,
		rewrite(nonce(20, 0x20, false), 0x21, true))
	all, err := s.NoncesInSlotRange(0, 100)
	require.NoError(t, err)
	require.Len(t, all, 2)
	for i, want := range []badgerutxo.Nonce{
		rewrite(nonce(10, 0x10, true), 0x11, true),
		rewrite(nonce(20, 0x20, false), 0x21, true),
	} {
		require.Equal(t, want.Checkpoint, all[i].Checkpoint, "slot %d", want.Point.Slot)
		require.Equal(t, want.Nonce, all[i].Nonce, "slot %d", want.Point.Slot)
	}
}

func TestRollbackDeletesTheNoncesOfUndoneBlocks(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	for slot := uint64(10); slot <= 40; slot += 10 {
		applyNonces(t, s, point(slot, byte(slot)), 0,
			nonce(slot, byte(slot), false))
	}

	_, err := s.Rollback(point(20, 20))
	require.NoError(t, err)
	all, err := s.NoncesInSlotRange(0, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20}, slots(all))
	latest, ok, err := s.LatestNonce()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(20), latest.Point.Slot)
}

func TestBootstrapDropsEarlierNonces(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	applyNonces(t, s, point(10, 0x10), 0, nonce(10, 0x10, true))
	require.NoError(t, s.LoadNonces([]badgerutxo.Nonce{nonce(5, 0x05, true)}))
	loaded, err := s.NoncesLoaded()
	require.NoError(t, err)
	require.True(t, loaded)

	bootstrap(t, s, point(5, 0x05))
	all, err := s.NoncesInSlotRange(0, 100)
	require.NoError(t, err)
	require.Empty(t, all)
	loaded, err = s.NoncesLoaded()
	require.NoError(t, err)
	require.False(t, loaded, "a bootstrap must load its nonces again")
}

func TestLoadNoncesReplacesTheStoredNonces(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	applyNonces(t, s, point(10, 0x10), 0, nonce(10, 0x10, false))
	require.NoError(t, s.LoadNonces([]badgerutxo.Nonce{
		nonce(3, 0x03, true), nonce(7, 0x07, false),
	}))
	all, err := s.NoncesInSlotRange(0, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 7}, slots(all))
}
