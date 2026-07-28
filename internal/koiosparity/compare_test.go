// Copyright 2025 Blink Labs Software
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

package koiosparity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompareEpochAggregatesTotalRewards(t *testing.T) {
	now := time.Now()
	koios := &KoiosEpochInfo{
		ActiveStake:  "100",
		Fees:         "10",
		TotalRewards: "50",
	}
	dingo := &DingoEpochData{
		TotalActiveStake: "100",
		Fees:             "10",
		TotalRewards:     "50",
	}
	require.Empty(t, CompareEpochAggregates("preview", 5, koios, dingo, nil, now, 0))

	dingo.TotalRewards = "49"
	ms := CompareEpochAggregates("preview", 5, koios, dingo, nil, now, 0)
	require.Len(t, ms, 1)
	require.Equal(t, "epoch_total_rewards", ms[0].Field)
	require.Equal(t, CategoryValueMismatch, ms[0].Category)
}

func TestCompareEpochTotals(t *testing.T) {
	now := time.Now()
	koios := &KoiosTotals{
		Treasury: "6931231163186226",
		Reserves: "7792082362166766",
		Fees:     "1245791321",
		Reward:   "292608261256804",
	}
	dingo := &DingoEpochData{
		Fees:         "1245791321",
		TotalRewards: "292608261256804",
		Treasury:     "6931231163186226",
		Reserves:     "7792082362166766",
	}
	require.Empty(t, CompareEpochTotals("preview", 1367, koios, dingo, now))

	// totals.fees is the fee-pot value (reward_ada_pots.Fees), a different
	// quantity from epoch_info.fees compared by CompareEpochAggregates —
	// verify a totals-only fees divergence is reported as totals_fees, not
	// epoch_fees.
	dingo.Fees = "1245791322"
	ms := CompareEpochTotals("preview", 1367, koios, dingo, now)
	require.Len(t, ms, 1)
	require.Equal(t, "totals_fees", ms[0].Field)
	require.Equal(t, CategoryValueMismatch, ms[0].Category)
	dingo.Fees = "1245791321"

	dingo.Treasury = "0"
	ms = CompareEpochTotals("preview", 1367, koios, dingo, now)
	require.Len(t, ms, 1)
	require.Equal(t, "totals_treasury", ms[0].Field)

	// A missing /totals cache row (not yet fetched) skips comparison rather
	// than flagging anything.
	require.Empty(t, CompareEpochTotals("preview", 1367, nil, dingo, now))

	// A missing Dingo row is left to CompareEpochAggregates' "epoch_summary"
	// report; CompareEpochTotals must not duplicate it under a second field.
	require.Empty(t, CompareEpochTotals("preview", 1367, koios, nil, now))
}

func TestComparePoolEpochFixedCostAndMargin(t *testing.T) {
	now := time.Now()
	koios := &KoiosPoolEpoch{
		PoolBech32:  "pool1test",
		ActiveStake: "1000",
		BlockCnt:    2,
		Delegators:  3,
		FixedCost:   "340000000",
		Margin:      "0.1",
	}
	dingo := &DingoPoolEpochData{
		DelegatedStake: "1000",
		BlocksProduced: 2,
		DelegatorCount: 3,
		FixedCost:      "340000000",
		Margin:         "1/10",
	}
	require.Empty(t, ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{}))

	dingo.FixedCost = "340000001"
	ms := ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, "fixed_cost", ms[0].Field)

	dingo.FixedCost = "340000000"
	dingo.Margin = "1/5"
	ms = ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, "margin", ms[0].Field)
}

func TestComparePoolEpochMemberRewards(t *testing.T) {
	now := time.Now()
	koios := &KoiosPoolEpoch{
		PoolBech32:    "pool1test",
		ActiveStake:   "1000",
		BlockCnt:      2,
		Delegators:    3,
		FixedCost:     "340000000",
		Margin:        "0.1",
		MemberRewards: "123456789",
	}
	dingo := &DingoPoolEpochData{
		DelegatedStake:    "1000",
		BlocksProduced:    2,
		DelegatorCount:    3,
		FixedCost:         "340000000",
		Margin:            "1/10",
		MemberRewardTotal: "123456789",
	}
	require.Empty(t, ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{}))

	// Reward calculation not yet finished for this pool/epoch: Dingo has no
	// reward_pool_output row, so the field is skipped rather than flagged.
	dingo.MemberRewardTotal = ""
	require.Empty(t, ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{}))

	dingo.MemberRewardTotal = "1"
	ms := ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, "member_rewards", ms[0].Field)
	require.Equal(t, CategoryValueMismatch, ms[0].Category)
}
