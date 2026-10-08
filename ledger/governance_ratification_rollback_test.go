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
	"context"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestFailedEnactmentClearRestoresRatificationOnRollback(t *testing.T) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	withdrawAddress, _, _ := f.rewardAddress(t, 0x91)
	proposal := f.addProposal(
		t,
		0x92,
		501,
		map[*lcommon.Address]uint64{withdrawAddress: 40},
		[]byte{0xff},
		1,
		true,
	)
	before := f.proposal(t, proposal)
	require.NotNil(t, before.RatifiedSlot)
	originalRatifiedSlot := *before.RatifiedSlot

	result := f.rollover(t, f.currentEpoch, f.currentPParams)
	cleared := f.proposal(t, proposal)
	require.Nil(t, cleared.RatifiedSlot)

	rollbackPoint := result.NewCurrentEpoch.StartSlot - 1
	require.GreaterOrEqual(t, rollbackPoint, originalRatifiedSlot)
	require.NoError(
		t,
		f.db.DeleteGovernanceProposalsAfterSlot(context.Background(), rollbackPoint, nil),
	)

	restored := f.proposal(t, proposal)
	require.NotNil(
		t,
		restored.RatifiedSlot,
		"rollback must restore the earlier ratification marker",
	)
	require.Equal(t, originalRatifiedSlot, *restored.RatifiedSlot)
}

func TestEnactmentWriteHealthyControlCommitsBoundary(t *testing.T) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	withdrawAddress, returnAddress, stakeCredential := f.rewardAddress(t, 0xb1)
	proposal := f.addProposal(
		t,
		0xb2,
		501,
		map[*lcommon.Address]uint64{withdrawAddress: 40},
		returnAddress,
		0,
		true,
	)

	result := f.rollover(t, f.currentEpoch, f.currentPParams)
	after := f.proposal(t, proposal)
	require.NotNil(t, after.EnactedSlot)
	require.Equal(t, result.NewCurrentEpoch.StartSlot, *after.EnactedSlot)
	require.Equal(t, uint64(40), f.accountReward(t, stakeCredential))
	treasury, _, _ := networkState(t, f.db)
	require.Equal(t, uint64(60), treasury)
	advancedEpoch, err := f.db.Metadata().GetEpoch(
		f.currentEpoch.EpochId+1,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, advancedEpoch)
}
