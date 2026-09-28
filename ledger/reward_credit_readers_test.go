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
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/stretchr/testify/require"
)

// pendingRoundReads is every reader outside the boundary that can see a
// pending round, rendered so two observations compare with require.Equal.
type pendingRoundReads struct {
	liveInputs     string
	boundaryStake  string
	stakeAtSlot    string
	localStateDist string
	drepPower      string
	drepSingle     string
	// Readers with no reward term, which a pending round cannot move.
	utxoStake  string
	controlled string
}

func renderStakeMaps(stakes, delegators map[string]uint64) string {
	lines := make([]string, 0, len(stakes))
	for key, stake := range stakes {
		lines = append(lines, fmt.Sprintf("%x=%d/%d", key, stake, delegators[key]))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func observePendingRoundReaders(
	t *testing.T,
	f *epochBoundaryBenchFixture,
	credits map[string]uint64,
) pendingRoundReads {
	t.Helper()
	meta := f.db.Metadata()
	pools, err := meta.GetDelegatedPoolKeyHashes(nil)
	require.NoError(t, err)
	require.NotEmpty(t, pools)
	boundary := epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch + 1)
	var ret pendingRoundReads

	inputs, err := meta.GetLiveStakeInputsForPools(pools, 0, nil)
	require.NoError(t, err)
	lines := make([]string, 0, len(inputs))
	for _, input := range inputs {
		lines = append(lines, fmt.Sprintf("%x/%d/%x=%d",
			input.PoolKeyHash, input.CredentialTag, input.StakingKey,
			uint64(input.Stake)))
	}
	sort.Strings(lines)
	ret.liveInputs = strings.Join(lines, "\n")

	stakes, delegators, err := meta.GetEpochBoundaryStakeByPools(
		pools, boundary-1, boundary, 0, 0, nil,
	)
	require.NoError(t, err)
	ret.boundaryStake = renderStakeMaps(stakes, delegators)

	stakes, delegators, err = meta.GetStakeByPoolsAtSlot(
		pools, boundary+10, 0, 0, nil,
	)
	require.NoError(t, err)
	ret.stakeAtSlot = renderStakeMaps(stakes, delegators)

	dist, err := f.ls.queryShelleyStakeDistribution(QueryPoint{}, nil)
	require.NoError(t, err)
	ret.localStateDist = fmt.Sprintf("%+v", dist)

	ret.drepPower = dumpDRepVotingPower(t, f)
	dreps, err := f.db.GetActiveDreps(nil)
	require.NoError(t, err)
	singles := make([]string, 0, len(dreps))
	for _, drep := range dreps {
		power, err := meta.GetDRepVotingPower(
			drep.CredentialTag, drep.Credential, 0, nil,
		)
		require.NoError(t, err)
		singles = append(singles, fmt.Sprintf("%x=%d", drep.Credential, power))
	}
	sort.Strings(singles)
	ret.drepSingle = strings.Join(singles, "\n")

	stakes, delegators, err = meta.GetStakeByPools(pools, nil)
	require.NoError(t, err)
	ret.utxoStake = renderStakeMaps(stakes, delegators)
	amounts := make([]string, 0, len(credits))
	for key := range credits {
		amount, err := f.db.GetControlledAmountByCredential(0, []byte(key), nil)
		require.NoError(t, err)
		amounts = append(amounts, fmt.Sprintf("%x=%d", key, amount))
	}
	sort.Strings(amounts)
	ret.controlled = strings.Join(amounts, "\n")
	return ret
}

// TestPendingRewardRoundAggregateReadsCountEachCreditOnce pins every aggregate
// and historical reader against a credited round both before any of its
// credits is folded and after some are, as by withdrawals: a credit must be
// counted exactly once, from the account row or from the round, so no reading
// moves when the rest are folded.
func TestPendingRewardRoundAggregateReadsCountEachCreditOnce(t *testing.T) {
	t.Parallel()
	f := creditedRewardRound(t)
	credits := rewardedCredentials(t, f)
	stored := accountRewards(t, f, credits)
	unfolded := observePendingRoundReaders(t, f, credits)

	keys := make([]string, 0, len(credits))
	for key := range credits {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	require.Greater(t, len(keys), 40)
	txn := f.db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		for _, key := range keys[:len(keys)/3] {
			if err := f.ls.foldRewardCreditFor(txn, 0, []byte(key)); err != nil {
				return err
			}
		}
		return nil
	}))
	first, last := keys[0], keys[len(keys)-1]
	require.Equal(t, stored[first]+credits[first],
		accountRewards(t, f, credits)[first], "control: a folded credit")
	require.Equal(t, stored[last], accountRewards(t, f, credits)[last],
		"control: an unfolded credit")
	partial := observePendingRoundReaders(t, f, credits)

	settleRewardCredits(t, f.ls)
	require.Equal(t, stored[last]+credits[last],
		accountRewards(t, f, credits)[last])
	after := observePendingRoundReaders(t, f, credits)

	for name, got := range map[string]pendingRoundReads{
		"unfolded": unfolded, "partly folded": partial,
	} {
		require.Equal(t, after.liveInputs, got.liveInputs,
			"%s: live stake inputs", name)
		require.Equal(t, after.boundaryStake, got.boundaryStake,
			"%s: boundary stake reconstruction", name)
		require.Equal(t, after.stakeAtSlot, got.stakeAtSlot,
			"%s: stake at slot reconstruction", name)
		require.Equal(t, after.localStateDist, got.localStateDist,
			"%s: local state query stake distribution", name)
		require.Equal(t, after.drepPower, got.drepPower,
			"%s: DRep voting power", name)
		require.Equal(t, after.drepSingle, got.drepSingle,
			"%s: single DRep voting power", name)
		require.Equal(t, after.utxoStake, got.utxoStake,
			"%s: UTxO-only pool stake", name)
		require.Equal(t, after.controlled, got.controlled,
			"%s: controlled amount", name)
	}
}
