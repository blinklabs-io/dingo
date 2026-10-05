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
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessEpochRolloverTreasuryRatificationUsesRunningBudget(
	t *testing.T,
) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	firstAddress, firstReturn, firstCredential := f.rewardAddress(t, 0x11)
	secondAddress, secondReturn, secondCredential := f.rewardAddress(t, 0x21)
	thirdAddress, thirdReturn, _ := f.rewardAddress(t, 0x31)
	fourthAddress, fourthReturn, fourthCredential := f.rewardAddress(t, 0x41)
	fifthAddress, _, _ := f.rewardAddress(t, 0x51)

	first := f.addProposal(
		t, 0x61, 501,
		map[*lcommon.Address]uint64{firstAddress: 70},
		firstReturn, 0, false,
	)
	second := f.addProposal(
		t, 0x62, 503,
		map[*lcommon.Address]uint64{secondAddress: 40},
		secondReturn, 0, false,
	)
	overflow := f.addProposal(
		t, 0x63, 505,
		map[*lcommon.Address]uint64{
			thirdAddress: ^uint64(0),
			fifthAddress: 1,
		},
		thirdReturn, 0, false,
	)
	fourth := f.addProposal(
		t, 0x64, 507,
		map[*lcommon.Address]uint64{fourthAddress: 30},
		fourthReturn, 0, false,
	)

	firstRollover := f.rollover(t, f.currentEpoch, f.currentPParams)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+1,
		firstRollover.NewCurrentEpoch.EpochId,
	)
	assert.NotNil(t, f.proposal(t, first).RatifiedEpoch)
	assert.Nil(t, f.proposal(t, second).RatifiedEpoch)
	assert.Nil(t, f.proposal(t, overflow).RatifiedEpoch)
	assert.NotNil(t, f.proposal(t, fourth).RatifiedEpoch)

	secondRollover := f.rollover(
		t,
		firstRollover.NewCurrentEpoch,
		firstRollover.NewCurrentPParams,
	)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+2,
		secondRollover.NewCurrentEpoch.EpochId,
	)
	assert.NotNil(t, f.proposal(t, first).EnactedEpoch)
	assert.Nil(t, f.proposal(t, second).RatifiedEpoch)
	assert.Nil(t, f.proposal(t, overflow).RatifiedEpoch)
	assert.NotNil(t, f.proposal(t, fourth).EnactedEpoch)
	assert.Equal(t, uint64(70), f.accountReward(t, firstCredential))
	assert.Equal(t, uint64(0), f.accountReward(t, secondCredential))
	assert.Equal(t, uint64(30), f.accountReward(t, fourthCredential))
	treasury, _, _ := networkState(t, f.db)
	assert.Zero(t, treasury)
}

func TestProcessEpochRolloverEnactmentFailureRollsBackAndRetries(
	t *testing.T,
) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	failingAddress, failingReturn, failingCredential := f.rewardAddress(t, 0x71)
	succeedingAddress, succeedingReturn, succeedingCredential := f.rewardAddress(
		t,
		0x72,
	)
	failing := f.addProposal(
		t, 0x73, 501,
		map[*lcommon.Address]uint64{failingAddress: 60},
		[]byte{0xff}, 1, true,
	)
	succeeding := f.addProposal(
		t, 0x74, 503,
		map[*lcommon.Address]uint64{succeedingAddress: 50},
		succeedingReturn, 0, true,
	)
	firstRollover := f.rollover(t, f.currentEpoch, f.currentPParams)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+1,
		firstRollover.NewCurrentEpoch.EpochId,
	)
	failedAfterRollover := f.proposal(t, failing)
	assert.Nil(t, failedAfterRollover.EnactedEpoch)
	assert.Nil(t, failedAfterRollover.RatifiedEpoch)
	assert.Nil(t, failedAfterRollover.ExpiredEpoch)
	assert.Nil(t, failedAfterRollover.DeletedSlot)
	assert.NotNil(t, f.proposal(t, succeeding).EnactedEpoch)
	assert.Zero(t, f.accountReward(t, failingCredential))
	assert.Equal(t, uint64(50), f.accountReward(t, succeedingCredential))
	treasury, reserves, _ := networkState(t, f.db)
	assert.Equal(t, uint64(50), treasury)

	retry := f.proposal(t, failing)
	retry.ReturnAddress = failingReturn
	require.NoError(t, f.db.SetGovernanceProposal(retry, nil))
	require.NoError(t, f.db.Metadata().SetNetworkState(
		70,
		reserves,
		firstRollover.NewCurrentEpoch.StartSlot+50,
		nil,
	))

	secondRollover := f.rollover(
		t,
		firstRollover.NewCurrentEpoch,
		firstRollover.NewCurrentPParams,
	)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+2,
		secondRollover.NewCurrentEpoch.EpochId,
	)
	assert.Nil(t, f.proposal(t, failing).EnactedEpoch)
	assert.NotNil(t, f.proposal(t, failing).RatifiedEpoch)
	assert.Zero(t, f.accountReward(t, failingCredential))
	assert.Equal(t, uint64(50), f.accountReward(t, succeedingCredential))
	treasury, _, _ = networkState(t, f.db)
	assert.Equal(t, uint64(70), treasury)

	thirdRollover := f.rollover(
		t,
		secondRollover.NewCurrentEpoch,
		secondRollover.NewCurrentPParams,
	)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+3,
		thirdRollover.NewCurrentEpoch.EpochId,
	)
	assert.NotNil(t, f.proposal(t, failing).EnactedEpoch)
	// The retried return account is also the withdrawal destination, so it
	// receives the 60 withdrawn plus the refunded deposit of 1.
	assert.Equal(t, uint64(61), f.accountReward(t, failingCredential))
	assert.Equal(t, uint64(50), f.accountReward(t, succeedingCredential))
	treasury, _, _ = networkState(t, f.db)
	assert.Equal(t, uint64(10), treasury)
}
