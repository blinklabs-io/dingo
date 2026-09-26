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

// TestProcessEpochRolloverRatifiesWithdrawalAgainstDonatedTreasury is the
// dingo#4467 regression through the real rollover. The treasury holds
// 100 ADA and the ending epoch received a 50 ADA donation. Conway's EPOCH
// rule adds the donation before it seeds RATIFY, so a 120 ADA withdrawal is
// accepted at this boundary and paid at the next one:
//
//	boundary 1: treasury 100 -> 150 ADA (donation), reserves unchanged,
//	            withdrawal ratified;
//	boundary 2: treasury 150 -> 30 ADA (withdrawal), reserves unchanged,
//	            destination +120 ADA, return account +1 ADA deposit.
func TestProcessEpochRolloverRatifiesWithdrawalAgainstDonatedTreasury(
	t *testing.T,
) {
	t.Parallel()

	const ada = uint64(1_000_000)
	f := newTreasuryRolloverFixture(t, 100*ada)
	destination, destinationReturn, destinationCredential := f.rewardAddress(
		t, 0x91,
	)
	require.NoError(t, f.db.Metadata().AddNetworkDonation(
		f.currentEpoch.StartSlot+10, f.currentEpoch.EpochId, 50*ada, nil,
	))
	withdrawal := f.addProposal(
		t, 0x92, f.currentEpoch.StartSlot+20,
		map[*lcommon.Address]uint64{destination: 120 * ada},
		destinationReturn, 1*ada, false,
	)
	_, reservesBefore, _ := networkState(t, f.db)

	first := f.rollover(t, f.currentEpoch, f.currentPParams)
	ratified := f.proposal(t, withdrawal)
	require.NotNil(
		t,
		ratified.RatifiedEpoch,
		"the donation counts toward the RATIFY treasury",
	)
	assert.Nil(t, ratified.EnactedEpoch)
	treasury, reserves, _ := networkState(t, f.db)
	assert.Equal(t, 150*ada, treasury)
	assert.Equal(t, reservesBefore, reserves)
	assert.Zero(t, f.accountReward(t, destinationCredential))

	f.rollover(t, first.NewCurrentEpoch, first.NewCurrentPParams)
	assert.NotNil(t, f.proposal(t, withdrawal).EnactedEpoch)
	treasury, reserves, _ = networkState(t, f.db)
	assert.Equal(t, 30*ada, treasury)
	assert.Equal(t, reservesBefore, reserves)
	assert.Equal(
		t,
		120*ada+1*ada,
		f.accountReward(t, destinationCredential),
	)
}
