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
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
	before := treasuryRolloverProposal(t, f, proposal)
	require.NotNil(t, before.RatifiedSlot)
	originalRatifiedSlot := *before.RatifiedSlot

	// The proposal state before the boundary is the block at the last slot of
	// the epoch, and the boundary is the block after it, so the set's journal
	// holds the cleared marker as a change the rollback undoes.
	rollbackPoint := ocommon.NewPoint(
		f.currentEpoch.StartSlot+uint64(f.currentEpoch.LengthInSlots)-1,
		bytes.Repeat([]byte{0x93}, lcommon.Blake2b256Size),
	)
	require.GreaterOrEqual(t, rollbackPoint.Slot, originalRatifiedSlot)
	commitTestLedgerChanges(
		t, f.ls, ochainsync.Tip{Point: rollbackPoint}, LedgerChanges{},
	)
	seedEmptyRewardBasisForRollover(t, f.db, f.currentEpoch, f.currentPParams)
	var result *EpochRolloverResult
	require.NoError(t, f.db.Transaction(context.Background(), true).Do(
		func(txn *database.Txn) error {
			var err error
			result, err = f.ls.processEpochRollover(
				context.Background(),
				txn,
				f.currentEpoch,
				eras.ConwayEraDesc,
				f.currentPParams,
			)
			if err != nil {
				return err
			}
			boundary := ocommon.NewPoint(
				result.NewCurrentEpoch.StartSlot,
				bytes.Repeat([]byte{0x94}, lcommon.Blake2b256Size),
			)
			f.ls.stageUtxoSet(txn, &boundary)
			return nil
		},
	))
	require.Equal(t, rollbackPoint.Slot+1, result.NewCurrentEpoch.StartSlot)
	cleared := treasuryRolloverProposal(t, f, proposal)
	require.Nil(t, cleared.RatifiedSlot)

	f.ls.stageUtxoSetRollback(nil, rollbackPoint)
	require.False(t, f.ls.utxoSetFailed.Load())

	restored := treasuryRolloverProposal(t, f, proposal)
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
	after := treasuryRolloverProposal(t, f, proposal)
	require.NotNil(t, after.EnactedSlot)
	require.Equal(t, result.NewCurrentEpoch.StartSlot, *after.EnactedSlot)
	require.Equal(t, uint64(40), f.accountReward(t, stakeCredential))
	treasury, _, _ := networkState(t, f.db)
	require.Equal(t, uint64(60), treasury)
	advancedEpoch, err := seedDB(t, f.db).GetEpoch(
		f.currentEpoch.EpochId+1,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, advancedEpoch)
}
