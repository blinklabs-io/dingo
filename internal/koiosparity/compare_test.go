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
