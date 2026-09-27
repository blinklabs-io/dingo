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

package ledger

import (
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/dbtest"

	"github.com/stretchr/testify/require"
)

func smallEpochBoundaryBenchShape() epochBoundaryBenchShape {
	return epochBoundaryBenchShape{
		pools:             6,
		delegators:        60,
		dreps:             4,
		utxosPerDelegator: 1,
		proposals:         2,
		drepVotes:         4,
		spoVotes:          3,
		ccMembers:         3,
	}
}

// TestEpochRolloverMarkSnapshotIncludesSameBoundaryRewards pins the SNAP
// ordering contract: the mark snapshot captured at a boundary includes the
// reward round that boundary applies, so every credited delegator's frozen
// stake is its pre-boundary stake plus its reward.
func TestEpochRolloverMarkSnapshotIncludesSameBoundaryRewards(t *testing.T) {
	t.Parallel()
	f := newEpochBoundaryBenchFixture(t, smallEpochBoundaryBenchShape(), "")
	require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
		epochBoundaryBenchPrecomputeEvent(),
	))
	before, err := f.db.Metadata().GetRewardStakeInputs(
		epochBoundaryBenchEndedEpoch, nil,
	)
	require.NoError(t, err)
	outputs, err := f.db.Metadata().GetRewardAccountOutputs(8, nil)
	require.NoError(t, err)
	require.NotEmpty(t, outputs)

	f.rollover(t)
	f.ls.waitEpochBoundaryBenchBackground()

	after, err := f.db.Metadata().GetRewardStakeInputs(
		epochBoundaryBenchEndedEpoch+1, nil,
	)
	require.NoError(t, err)
	credit := make(map[string]uint64)
	for _, output := range outputs {
		if output.Spendable && !output.Guarded {
			credit[string(output.StakingKey)] += uint64(output.Amount)
		}
	}
	require.NotEmpty(t, credit)
	stakeAfter := make(map[string]uint64, len(after))
	for _, input := range after {
		stakeAfter[string(input.StakingKey)] = uint64(input.Stake)
	}
	for _, input := range before {
		key := string(input.StakingKey)
		require.Equal(
			t, uint64(input.Stake)+credit[key], stakeAfter[key],
			"mark stake of %x must include the reward credited at the"+
				" same boundary", input.StakingKey,
		)
	}
}

// TestBoundaryRechecksRegistrationChangedAfterPrecompute pins the boundary's
// eligibility recheck: a delegator deregistered after the precompute ran is
// not credited, and exactly its reward moves to the treasury, although the
// precompute recorded its output as spendable.
func TestBoundaryRechecksRegistrationChangedAfterPrecompute(t *testing.T) {
	t.Parallel()
	run := func(deregister bool) (*epochBoundaryBenchFixture, uint64) {
		f := newEpochBoundaryBenchFixture(t, epochBoundaryDumpShape(), "")
		require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
			epochBoundaryBenchPrecomputeEvent(),
		))
		if deregister {
			raw, err := dbtest.RawSQLiteMetadata(t, f.db)
			require.NoError(t, err)
			deregisterEpochBoundaryDumpDelegators(t, raw)
			require.NoError(t, raw.Close())
		}
		f.rollover(t)
		f.ls.waitEpochBoundaryBenchBackground()
		state, err := f.db.Metadata().GetNetworkState(nil)
		require.NoError(t, err)
		return f, uint64(state.Treasury)
	}
	_, treasuryKept := run(false)
	f, treasuryDeregistered := run(true)

	deregistered := make(map[string]bool)
	for d := 0; d < epochBoundaryDumpShape().delegators; d += 97 {
		deregistered[string(epochBoundaryBenchHash(0x30, uint64(d)+1))] = true
	}
	outputs, err := f.db.Metadata().GetRewardAccountOutputs(8, nil)
	require.NoError(t, err)
	var moved uint64
	for _, output := range outputs {
		if !deregistered[string(output.StakingKey)] {
			continue
		}
		require.False(
			t, output.Spendable,
			"a delegator deregistered before the boundary is unspendable",
		)
		moved += uint64(output.Amount)
	}
	require.NotZero(t, moved, "fixture must deregister a rewarded delegator")
	for key := range deregistered {
		account, err := f.db.GetAccountByCredential(0, []byte(key), true, nil)
		require.NoError(t, err)
		require.Equal(
			t, stakeRewardSeedReward(key), uint64(account.Reward),
			"a deregistered delegator is not credited",
		)
	}
	require.Equal(
		t, treasuryKept+moved, treasuryDeregistered,
		"exactly the deregistered delegators' rewards move to the treasury",
	)
}

// stakeRewardSeedReward is the reward balance the fixture seeds for a
// delegator key.
func stakeRewardSeedReward(key string) uint64 {
	index := binary.BigEndian.Uint64([]byte(key)[20:]) - 1
	return epochBoundaryBenchStake(index) / 200
}
