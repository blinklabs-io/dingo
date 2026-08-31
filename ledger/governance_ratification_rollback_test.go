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

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailedEnactmentClearRestoresRatificationOnRollback(t *testing.T) {
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
	require.NoError(t, f.db.DeleteGovernanceProposalsAfterSlot(rollbackPoint, nil))

	restored := f.proposal(t, proposal)
	require.NotNil(
		t,
		restored.RatifiedSlot,
		"rollback must restore the earlier ratification marker",
	)
	require.Equal(t, originalRatifiedSlot, *restored.RatifiedSlot)
}

func TestNodeLocalEnactmentWriteErrorAbortsBoundary(t *testing.T) {
	f := newTreasuryRolloverFixture(t, 100)
	withdrawAddress, returnAddress, stakeCredential := f.rewardAddress(t, 0xa1)
	proposal := f.addProposal(
		t,
		0xa2,
		501,
		map[*lcommon.Address]uint64{withdrawAddress: 40},
		returnAddress,
		0,
		true,
	)
	before := f.proposal(t, proposal)
	require.NotNil(t, before.RatifiedSlot)
	originalRatifiedSlot := *before.RatifiedSlot

	raw, err := dbtest.RawSQLiteMetadata(t, f.db)
	require.NoError(t, err)
	_, err = raw.Exec(`
CREATE TRIGGER fail_governance_enact
BEFORE UPDATE OF enacted_slot ON governance_proposal
WHEN NEW.enacted_slot IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'injected enactment write failure');
END`)
	require.NoError(t, err)

	txn := f.db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		_, rolloverErr := f.ls.processEpochRollover(
			txn,
			f.currentEpoch,
			eras.ConwayEraDesc,
			f.currentPParams,
			false,
		)
		return rolloverErr
	})
	assert.Error(t, err, "a storage error must abort the boundary transaction")

	after := f.proposal(t, proposal)
	require.NotNil(t, after.RatifiedSlot)
	assert.Equal(
		t,
		originalRatifiedSlot,
		*after.RatifiedSlot,
		"an aborted boundary must preserve the earlier ratification marker",
	)
	assert.Nil(t, after.EnactedSlot)
	assert.Zero(t, f.accountReward(t, stakeCredential))
	treasury, _, _ := networkState(t, f.db)
	assert.Equal(t, uint64(100), treasury)
	advancedEpoch, epochErr := f.db.Metadata().GetEpoch(
		f.currentEpoch.EpochId+1,
		nil,
	)
	require.NoError(t, epochErr)
	assert.Nil(t, advancedEpoch)
}

func TestEnactmentWriteHealthyControlCommitsBoundary(t *testing.T) {
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
