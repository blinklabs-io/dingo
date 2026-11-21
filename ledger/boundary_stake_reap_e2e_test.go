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

package ledger_test

import (
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBoundaryStakeReadsCountAPoolTheBoundaryReaped requires the epoch-boundary
// stake reads of a snapshot slot to still count a pool whose retirement the
// boundary into the next epoch reaped, when the set holds that boundary but its
// tip is still at the snapshot slot: SNAP precedes POOLREAP, so the snapshot
// holds the pool and its delegators, and the pool keeps electing leaders for
// the epochs the snapshot serves.
func TestBoundaryStakeReadsCountAPoolTheBoundaryReaped(t *testing.T) {
	t.Parallel()
	h := newBoundaryStakeHarness(t, "set")
	f := h.scenario.BoundaryStake(6)
	h.scenario.ApplyBoundary()
	tip, _, err := h.set.Tip()
	require.NoError(t, err)
	require.LessOrEqual(t, tip.Slot, f.Slot, "the set's tip is at the snapshot slot")
	got := h.scenario.ReadBoundaryStake(f)
	require.ElementsMatch(t, [][]byte{f.Pool}, got.Active, "active pools")
	requireFixtureFigures(t, f, got)
}

// TestBoundaryStakeReadsCountAReapedPoolWhenTheSetOnlyHoldsTheState is the
// reap scenario for a node whose metadata store holds no ledger state.
func TestBoundaryStakeReadsCountAReapedPoolWhenTheSetOnlyHoldsTheState(
	t *testing.T,
) {
	t.Parallel()
	h := newBoundaryStakeHarness(t, "set")
	f := h.scenario.BoundaryStake(6)
	h.scenario.ApplyBoundary()
	h.scenario.DrainFamilies(nodesettings.AllFamilies())
	reached, hide := h.scenario.HideStoreReads(boundaryStakeReads...)
	hide(true)
	got := h.scenario.ReadBoundaryStake(f)
	require.Zero(t, reached(), "a read reached the metadata store")
	require.ElementsMatch(t, [][]byte{f.Pool}, got.Active, "active pools")
	requireFixtureFigures(t, f, got)
}

// TestDeferredBoundarySnapshotHoldsAPoolTheBoundaryReaped requires the mark
// snapshot a Conway boundary prepares after it commits to hold a pool whose
// retirement the boundary reaped, with the stake its delegators had at SNAP, as
// the snapshot the boundary captures itself does.
func TestDeferredBoundarySnapshotHoldsAPoolTheBoundaryReaped(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"set", "set only"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			own := newBoundaryStakeHarness(t, "set")
			ownFixture := own.scenario.BoundaryStake(6)
			own.scenario.ApplyBoundary()
			want, err := own.scenario.LedgerDB().GetPoolStakeSnapshot(
				6, "mark", ownFixture.Pool, nil,
			)
			require.NoError(t, err)
			require.NotNil(t, want, "the boundary's own snapshot holds the pool")
			h := newBoundaryStakeHarness(t, "set")
			f := h.scenario.BoundaryStake(6)
			if name == "set only" {
				h.scenario.InstallDrainedSources()
				h.scenario.DrainFamilies(nodesettings.AllFamilies())
			}
			rows := h.scenario.DeferredBoundaryRows(f)
			require.Len(t, rows, 1, "the reaped pool is in the snapshot")
			require.Equal(t, f.Pool, rows[0].PoolKeyHash)
			require.Equal(t, want.TotalStake, rows[0].TotalStake)
			require.Equal(t, want.DelegatorCount, rows[0].DelegatorCount)
		})
	}
}
