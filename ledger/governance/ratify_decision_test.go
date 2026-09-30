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

package governance

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// The RATIFY and EXPIRY verdicts must be computable from a read-only view of
// the boundary state, so they can be taken from a snapshot after the boundary
// commits, and must equal what the boundary itself applies.
func TestDecideRatificationOnReadOnlySnapshotMatchesBoundary(t *testing.T) {
	t.Parallel()

	const currentEpoch = uint64(741)
	const newEpoch = currentEpoch + 1

	db, store := newTallyTestDB(t)
	hardFork := seedHardForkInitiationProposal(
		t, db, currentEpoch, 11, 1, 0x90,
	)
	hardFork.ReturnAddress = append([]byte{0xE0}, testBytes(28, 0x97)...)
	// Its final RATIFY chance: EXPIRY must not also classify it expired.
	hardFork.ExpiresEpoch = currentEpoch
	require.NoError(t, db.SetGovernanceProposal(hardFork, nil))
	coldCred := testBytes(28, 0x91)
	hotCred := testBytes(28, 0x92)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{
		{ColdCredHash: coldCred, ExpiresEpoch: newEpoch + 10},
	}, nil))
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: coldCred,
		HotCredential:  hotCred,
		CertificateID:  1,
		AddedSlot:      1,
	})
	yesPool := testBytes(28, 0x93)
	seedPoolWithStake(
		t, store, yesPool, testBytes(29, 0x95), 6_283, newEpoch,
	)
	seedPoolWithStake(
		t, store, testBytes(28, 0x94), testBytes(29, 0x96), 3_717, newEpoch,
	)
	for _, vote := range []*models.GovernanceVote{
		{VoterType: models.VoterTypeCC, VoterCredential: hotCred},
		{VoterType: models.VoterTypeSPO, VoterCredential: yesPool},
	} {
		vote.ProposalID = hardFork.ID
		vote.Vote = models.VoteYes
		vote.AddedSlot = 2
		require.NoError(t, db.SetGovernanceVote(vote, nil))
	}
	returnAddr := buildRewardAddr(t, testBytes(28, 0x98))
	expiringHash := testBytes(32, 0x99)
	childHash := testBytes(32, 0x9a)
	expiringIdx := uint32(0)
	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, expiringHash, 0, currentEpoch-1, 3,
			returnAddr, 10, nil, nil, nil, nil),
		nil,
	))
	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, childHash, 0, newEpoch+5, 3,
			returnAddr, 11, expiringHash, &expiringIdx, nil, nil),
		nil,
	))

	input := func(txn *EpochInput) *EpochInput {
		txn.DB = db
		txn.PrevEpoch = currentEpoch
		txn.NewEpoch = newEpoch
		txn.BoundarySlot = newEpoch * 100
		txn.PParams = stabilityConwayPParams(9)
		txn.UpdateFn = func(
			pparams lcommon.ProtocolParameters,
			_ any,
		) (lcommon.ProtocolParameters, error) {
			return pparams, nil
		}
		return txn
	}

	readTxn := db.MetadataTxn(false)
	in := input(&EpochInput{Txn: readTxn})
	conwayPParams, err := conwayGovernanceProtocolParameters(in.PParams)
	require.NoError(t, err)
	decision, err := decideRatification(
		in, &EpochOutput{UpdatedPParams: in.PParams}, conwayPParams, 0,
	)
	readTxn.Release()
	require.NoError(t, err)
	identities := func(proposals []*models.GovernanceProposal) []string {
		ret := make([]string, 0, len(proposals))
		for _, p := range proposals {
			ret = append(ret, proposalIdentityKey(p))
		}
		return ret
	}
	require.Equal(t, []string{proposalIdentityKey(hardFork)},
		identities(decision.Ratified))
	require.Equal(t,
		[]string{proposalIdentityKey(&models.GovernanceProposal{
			TxHash: expiringHash,
		})},
		identities(decision.Expired))

	stored, err := db.GetGovernanceProposal(hardFork.TxHash, 0, nil)
	require.NoError(t, err)
	require.Nil(t, stored.RatifiedEpoch, "deciding wrote a ratified mark")

	writeTxn := db.MetadataTxn(true)
	defer writeTxn.Release()
	out, err := ProcessEpoch(input(&EpochInput{Txn: writeTxn}))
	require.NoError(t, err)
	require.NoError(t, writeTxn.Commit())
	require.Equal(t, 1, out.RatifiedCount)
	require.Equal(t, 1, out.ExpiredCount)
	require.Equal(t, 1, out.OrphanedCount)
	stored, err = db.GetGovernanceProposal(hardFork.TxHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, stored.RatifiedEpoch)
	require.Equal(t, newEpoch, *stored.RatifiedEpoch)
	child, err := db.GetGovernanceProposal(childHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, child.ExpiredEpoch)
}
