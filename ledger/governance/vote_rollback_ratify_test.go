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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// TestRatifyLevelForkRestoresVoteAcrossAllVoterTypes covers dingo#4463 at
// the RATIFY layer rather than only the database layer: a DRep, an SPO, and
// a committee member each cast Yes, flip to No after the cast slot, and a
// rollback to a slot between the cast and the flip must restore Yes for all
// three and re-ratify the proposal, exactly as the reference tallies it.
func TestRatifyLevelForkRestoresVoteAcrossAllVoterTypes(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)

	drepCred := testBytes(28, 200)
	stakeCred := testBytes(28, 201)
	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
	}))
	seedDRepStake(
		t, store, stakeCred, drepCred, models.DrepTypeAddrKeyHash, 100, 1,
	)

	poolKeyHash := testBytes(28, 202)
	rewardAccount := testBytes(28, 203)
	seedPoolWithStake(t, store, poolKeyHash, rewardAccount, 100, 5)

	coldCred := testBytes(28, 204)
	hotCred := testBytes(28, 205)
	require.NoError(t, store.SetCommitteeMembers([]*models.CommitteeMember{
		{ColdCredHash: coldCred, ExpiresEpoch: 20, AddedSlot: 1},
	}, nil))
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: coldCred,
		HotCredential:  hotCred,
		CertificateID:  1,
		AddedSlot:      1,
	})

	proposal := &models.GovernanceProposal{
		TxHash:        testBytes(32, 206),
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: 5,
		ExpiresEpoch:  20,
		AnchorHash:    testBytes(32, 207),
		ReturnAddress: testBytes(29, 208),
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))

	const (
		castSlot     = uint64(100)
		replacedSlot = uint64(200)
		rollbackSlot = uint64(150)
	)
	cast := func(voterType uint8, cred []byte, vote uint8, updatedSlot uint64) {
		slot := updatedSlot
		require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
			ProposalID:      proposal.ID,
			VoterType:       voterType,
			VoterCredential: cred,
			Vote:            vote,
			AddedSlot:       castSlot,
			VoteUpdatedSlot: &slot,
		}, nil))
	}

	pparams := &conway.ConwayProtocolParameters{}
	pparams.MinCommitteeSize = 1
	pparams.DRepVotingThresholds.HardForkInitiation = newRat(60, 100)
	pparams.PoolVotingThresholds.HardForkInitiation = newRat(51, 100)

	tallyCtx := &TallyContext{DB: db, StakeEpoch: 5, CurrentEpoch: 10}
	ratify := func() RatifyDecision {
		tally, err := TallyProposal(tallyCtx, proposal)
		require.NoError(t, err)
		return ShouldRatify(RatifyInputs{
			Tally:           tally,
			PParams:         pparams,
			ActiveDRepCount: 1,
			ActiveCCCount:   1,
			CCQuorum:        big.NewRat(2, 3),
			MajorVersion:    10,
		})
	}

	// All three voter types cast Yes at the initial slot.
	cast(models.VoterTypeDRep, drepCred, models.VoteYes, castSlot)
	cast(models.VoterTypeSPO, poolKeyHash, models.VoteYes, castSlot)
	cast(models.VoterTypeCC, hotCred, models.VoteYes, castSlot)

	initial := ratify()
	require.True(
		t,
		initial.Ratified,
		"unanimous Yes must ratify: %+v",
		initial,
	)

	// Every voter replaces their vote with No after the cast slot. Forward
	// replacement must flip the outcome (dingo#4463's "preserve normal
	// forward replacement behavior" criterion).
	cast(models.VoterTypeDRep, drepCred, models.VoteNo, replacedSlot)
	cast(models.VoterTypeSPO, poolKeyHash, models.VoteNo, replacedSlot)
	cast(models.VoterTypeCC, hotCred, models.VoteNo, replacedSlot)

	replaced := ratify()
	require.False(
		t,
		replaced.Ratified,
		"unanimous No must not ratify: %+v",
		replaced,
	)

	// Roll back to a slot between the original cast and the replacement.
	require.NoError(t, db.DeleteGovernanceVotesAfterSlot(rollbackSlot, nil))

	restored := ratify()
	require.True(
		t,
		restored.Ratified,
		"rollback to a slot before replacement must restore the Yes votes"+
			" and re-ratify: %+v",
		restored,
	)
}
