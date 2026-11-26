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
// with the boundary's capture, and no read reaches the store beside it.
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
}

// A rollback across the boundary leaves the set with none of the epoch's
// inputs, and the replayed boundary writes the rows the first pass did.
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

	s.Block(11, s.PoolB)
	s.Fee(11, 25)
	s.Boundary()
	require.Equal(t, first, s.Inputs(6))
	require.Zero(t, reached(), "a read reached the metadata store")
}

// With the set holding no rows for an epoch, the
// reward round's reads fail rather than answer nothing, and
// a fallback snapshot capture is refused.
func TestRewardInputsFailWhenTheSetServesNothing(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, "set", nil, false)
	reached, hide := r.scenario.HideStoreReads(rewardInputReadNames...)
	a := r.scenario.Inputs(6)
	hide(false)
	require.NoError(t, a.Err)
	require.Empty(t, a.Pools)
	require.Empty(t, a.Stakes)
	require.Zero(t, reached(), "loaded empty reward inputs reached metadata")
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
