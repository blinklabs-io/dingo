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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

var rewardInputReadNames = []string{
	"GetRewardAdaPots", "GetRewardSnapshot", "GetRewardSeedFailure",
	"GetRewardPoolInputs", "GetRewardStakeInputs",
	"GetRewardStakeInputsInPoolKeyHashRange", "CountEpochPoolBlocks",
	"CountPoolBlocksInSlotRange", "MergeImportedPoolBlockCounts",
}

// stakeUp gives the harness's two stake credentials, which it created in the
// metadata store only, the same stake in the set.
func stakeUp(r *rewardBlocksRun) {
	r.scenario.AddSetStake(bytes.Repeat([]byte{0x71}, 28), 20_000_000)
	r.scenario.AddSetStake(bytes.Repeat([]byte{0x72}, 28), 20_000_000)
}

func requireCaptured(t *testing.T, a ledger.RewardInputsAnswer) {
	t.Helper()
	require.NoError(t, a.Err)
	require.NotNil(t, a.Pots, "the boundary stores the epoch's pots")
	require.NotNil(t, a.Snapshot, "the boundary captures the snapshot")
	require.True(t, a.Snapshot.Authoritative)
	require.NotEmpty(t, a.Pools)
	require.NotEmpty(t, a.Stakes)
}

// After a boundary the set answers every read of the epoch's reward inputs
// with the boundary's capture, the metadata store holds none of them, and no
// read reaches it.
func TestRewardInputsAreServedFromTheSet(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, "set", nil, false)
	s := r.scenario
	r.write()
	stakeUp(r)
	s.Boundary()
	reached, _ := s.HideStoreReads(rewardInputReadNames...)
	requireCaptured(t, s.Inputs(6))
	require.Zero(t, reached(), "a read reached the metadata store")
	require.Equal(t, ledger.RewardInputsAnswer{}, s.StoreInputs(6))
}

// A rollback across the boundary leaves the set with none of the epoch's
// inputs, as the store has none, and the replayed boundary writes the rows
// the first pass did.
func TestRewardInputsAcrossABoundaryRollbackAndReplay(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, "set", nil, false)
	s := r.scenario
	r.write()
	stakeUp(r)
	reached, _ := s.HideStoreReads(rewardInputReadNames[:7]...)
	s.Boundary()
	first := s.Inputs(6)
	requireCaptured(t, first)

	s.RollbackBeforeBoundary()
	gone := s.Inputs(6)
	require.NoError(t, gone.Err)
	require.Nil(t, gone.Pots)
	require.Nil(t, gone.Snapshot)
	require.Empty(t, gone.Pools)
	require.Empty(t, gone.Stakes, "one tombstone drops the epoch's stake rows")
	require.Equal(t, s.StoreInputs(6), gone)

	s.Block(11, s.PoolB)
	s.Fee(11, 25)
	s.Boundary()
	require.Equal(t, first, s.Inputs(6))
	require.Zero(t, reached(), "a read reached the metadata store")
}

// With the family drained and the set holding no rows for an epoch, the
// reward round's reads fail rather than answer from the emptied tables, and
// a fallback snapshot capture is refused.
func TestRewardInputsFailWhenTheDrainedSetServesNothing(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, "set", nil, false)
	require.NoError(t, r.scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyRewardInputs,
	))
	r.scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyRewardInputs),
	)
	reached, hide := r.scenario.HideStoreReads(rewardInputReadNames...)
	a := r.scenario.Inputs(6)
	hide(false)
	require.NoError(t, a.Err)
	require.Empty(t, a.Pools)
	require.Empty(t, a.Stakes)
	require.Zero(t, reached(), "loaded empty reward inputs reached metadata")
}

// The boundary capture resolves each pool's reward-account auto-vote and
// reads its registration from the set when the accounts and pool families are
// drained too, and no statement reaches a drained table.
func TestRewardCaptureRunsWithItsInputFamiliesDrained(t *testing.T) {
	t.Parallel()
	guard := statedrain.NewTripGuard()
	r := newRewardBlocksRun(t, "set", guard, false)
	s := r.scenario
	r.write()
	stakeUp(r)
	src := rewardBlocksDrainSource{r: r}
	want := map[nodesettings.Family]bool{
		nodesettings.FamilyAccounts:     true,
		nodesettings.FamilyPoolsHistory: true,
	}
	var entries []statedrain.Entry
	for _, e := range append(
		statedrain.Graduated(src), statedrain.Staged(src)...,
	) {
		if want[e.Family] {
			entries = append(entries, e)
		}
	}
	require.Len(t, entries, len(want))
	require.NoError(t, statedrain.Run(t.Context(), statedrain.Env{
		Store: drainStore{s.DB, s.DrainFamilies}, Set: src,
		Guard: guard,
	}, entries))
	s.Boundary()
	require.Empty(t, guard.Trips(), "a drained table was touched")
	a := s.Inputs(6)
	requireCaptured(t, a)
}

// The boundary's prune drops an old epoch's stake inputs from the set, and
// leaves its pots, snapshot and pool inputs.
func TestRewardStakeInputPruneReachesTheSet(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, "set", nil, false)
	s := r.scenario
	r.write()
	stakeUp(r)
	s.Boundary()
	before := s.Inputs(6)
	requireCaptured(t, before)
	s.PruneBefore(7)
	after := s.Inputs(6)
	require.NoError(t, after.Err)
	require.Empty(t, after.Stakes)
	require.Equal(t, before.Pots, after.Pots)
	require.Equal(t, before.Snapshot, after.Snapshot)
	require.Equal(t, before.Pools, after.Pools)
}
