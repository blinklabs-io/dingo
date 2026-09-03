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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	stabilityTestEpoch  uint64 = 50
	stabilityProposalTx byte   = 0xAA
	stabilityDRepCred   byte   = 0xBB
	stabilityStakeCred  byte   = 0xCC
)

// stabilityConwayPParams returns Conway pparams configured for the
// requested protocol major version. Threshold values are realistic so
// post-bootstrap tests exercise the same code path as production. During
// bootstrap, DRep thresholds are zero but the action-specific SPO and CC
// requirements still apply.
func stabilityConwayPParams(major uint) *conway.ConwayProtocolParameters {
	p := mockledger.NewMockConwayProtocolParams()
	p.ProtocolVersion.Major = major
	p.MinCommitteeSize = 0
	p.DRepVotingThresholds.HardForkInitiation = newRat(60, 100)
	p.PoolVotingThresholds.HardForkInitiation = newRat(51, 100)
	return &p
}

// seedHardForkInitiationProposal inserts an active HardForkInitiation
// proposal whose enacted state would bump the protocol major version to
// targetMajor. addedSlot and txHashSeed identify the proposal so callers
// can seed multiple in the same DB. The proposal is configured to be
// returned by GetActiveGovernanceProposals(currentEpoch): proposed in
// the past, not yet expired, not enacted/expired/deleted.
func seedHardForkInitiationProposal(
	t *testing.T,
	db *database.Database,
	currentEpoch uint64,
	targetMajor uint,
	addedSlot uint64,
	txHashSeed byte,
) *models.GovernanceProposal {
	t.Helper()
	action := &lcommon.HardForkInitiationGovAction{Type: 1}
	action.ProtocolVersion.Major = targetMajor
	action.ProtocolVersion.Minor = 0
	cborBytes, err := cbor.Encode(action)
	require.NoError(t, err)

	proposal := &models.GovernanceProposal{
		TxHash:        testBytes(32, txHashSeed),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: currentEpoch - 1,
		ExpiresEpoch:  currentEpoch + 10,
		Deposit:       1_000,
		ReturnAddress: testBytes(29, 0),
		AnchorURL:     "https://example.invalid/anchor",
		AnchorHash:    testBytes(32, 0xEE),
		GovActionCbor: cborBytes,
		AddedSlot:     addedSlot,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	loaded, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	return loaded
}

// seedDRepWithStake creates an active DRep, an account delegating to
// it, and a UTxO funding that account's stake. The DRep ends up with
// stakeAmount of voting power. Returns the DRep's credential so the
// caller can attach a vote.
func seedDRepWithStake(
	t *testing.T,
	db *database.Database,
	stakeAmount uint64,
) []byte {
	t.Helper()
	drepCred := testBytes(28, stabilityDRepCred)
	stakeCred := testBytes(28, stabilityStakeCred)

	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
		AddedSlot:  1,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Drep:       drepCred,
		DrepType:   models.DrepTypeAddrKeyHash,
		AddedSlot:  1,
		Active:     true,
	}))
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       testBytes(32, 1),
		OutputIdx:  0,
		StakingKey: stakeCred,
		AddedSlot:  1,
		Amount:     types.Uint64(stakeAmount),
	}))
	return drepCred
}

// seedDRepYesVote attaches a Yes vote from drepCred to the given
// proposal.
func seedDRepYesVote(
	t *testing.T,
	db *database.Database,
	proposalID uint,
	drepCred []byte,
) {
	t.Helper()
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      proposalID,
		VoterType:       models.VoterTypeDRep,
		VoterCredential: drepCred,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
}

func seedHardForkCommitteeAndSPOVotes(
	t *testing.T,
	db *database.Database,
	store *tallyTestStore,
	proposals ...*models.GovernanceProposal,
) {
	t.Helper()
	coldCred := testBytes(28, 0xD1)
	hotCred := testBytes(28, 0xD2)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{
		{ColdCredHash: coldCred, ExpiresEpoch: stabilityTestEpoch + 10},
	}, nil))
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: coldCred,
		HotCredential:  hotCred,
		CertificateID:  1,
		AddedSlot:      1,
	})
	poolCred := testBytes(28, 0xD3)
	seedPoolWithStake(
		t, store, poolCred, testBytes(29, 0xD4), 100,
		stakeEpochFor(stabilityTestEpoch),
	)
	for _, proposal := range proposals {
		require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
			ProposalID:      proposal.ID,
			VoterType:       models.VoterTypeCC,
			VoterCredential: hotCred,
			Vote:            models.VoteYes,
			AddedSlot:       2,
		}, nil))
		require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
			ProposalID:      proposal.ID,
			VoterType:       models.VoterTypeSPO,
			VoterCredential: poolCred,
			Vote:            models.VoteYes,
			AddedSlot:       2,
		}, nil))
	}
}

func TestEvaluateRatifiableHardForkInitiation_PreConway_ReturnsNil(
	t *testing.T,
) {
	db, _ := newTallyTestDB(t)
	// pre-Conway: no governance state machine yet
	in := NewStabilityCheckInputs(
		db,
		nil,
		stabilityTestEpoch,
		false,
		nil,
		nil,
		nil,
	)
	got, err := EvaluateRatifiableHardForkInitiation(in)
	require.NoError(t, err)
	assert.Nil(t, got, "pre-Conway pparams must short-circuit to nil")
}

func TestEvaluateRatifiableHardForkInitiation_NoActiveProposals_ReturnsNil(
	t *testing.T,
) {
	db, _ := newTallyTestDB(t)
	in := NewStabilityCheckInputs(
		db, nil, stabilityTestEpoch, false, stabilityConwayPParams(9), nil, nil,
	)
	got, err := EvaluateRatifiableHardForkInitiation(in)
	require.NoError(t, err)
	assert.Nil(t, got, "empty active proposal set must yield nil")
}

func TestEvaluateRatifiableHardForkInitiation_OnlyOtherActionType_ReturnsNil(
	t *testing.T,
) {
	db, _ := newTallyTestDB(t)

	// A TreasuryWithdrawal — active but not a HardForkInitiation, so
	// the helper must skip it even if it would otherwise ratify.
	otherAction := &lcommon.TreasuryWithdrawalGovAction{}
	cborBytes, err := cbor.Encode(otherAction)
	require.NoError(t, err)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 0xDD),
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: stabilityTestEpoch - 1,
		ExpiresEpoch:  stabilityTestEpoch + 10,
		Deposit:       1_000,
		ReturnAddress: testBytes(29, 0),
		AnchorURL:     "https://example.invalid/anchor",
		AnchorHash:    testBytes(32, 0xEE),
		GovActionCbor: cborBytes,
		AddedSlot:     1,
	}, nil))

	in := NewStabilityCheckInputs(
		db, nil, stabilityTestEpoch, false, stabilityConwayPParams(9), nil, nil,
	)
	got, err := EvaluateRatifiableHardForkInitiation(in)
	require.NoError(t, err)
	assert.Nil(t, got, "non-HardForkInitiation actions must be ignored")
}

// TestEvaluateRatifiableHardForkInitiation_BootstrapRequiresCommitteeAndSPO
// pins the bootstrap hard-fork requirement in the mid-epoch helper: a DRep
// vote alone is not sufficient, while the required CC and SPO votes surface
// the transition with the target major version from the encoded action.
func TestEvaluateRatifiableHardForkInitiation_BootstrapRequiresCommitteeAndSPO(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)

	const targetMajor uint = 11
	proposal := seedHardForkInitiationProposal(
		t, db, stabilityTestEpoch, targetMajor, 1, stabilityProposalTx,
	)
	seedHardForkCommitteeAndSPOVotes(t, db, store, proposal)

	in := NewStabilityCheckInputs(
		db, nil, stabilityTestEpoch, false, stabilityConwayPParams(9), nil, nil,
	)
	got, err := EvaluateRatifiableHardForkInitiation(in)
	require.NoError(t, err)
	require.NotNil(t, got, "bootstrap CC + SPO votes must be ratifiable")
	assert.Equal(t, targetMajor, got.NewMajor,
		"target major must come from the proposal's encoded action")
	assert.Equal(t, proposal.ID, got.Proposal.ID,
		"the helper must return the proposal that ratifies")
}

func TestEvaluateRatifiableHardForkInitiation_DelegatorInactivityParity(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)

	proposal := seedHardForkInitiationProposal(
		t, db, stabilityTestEpoch, 11, 1, stabilityProposalTx,
	)
	drepCred := seedDRepWithStake(t, db, 1_000)
	seedDRepYesVote(t, db, proposal.ID, drepCred)
	seedHardForkCommitteeAndSPOVotes(t, db, store, proposal)
	rewardCred := models.NewStakeCredentialRef(
		0,
		testBytes(28, stabilityStakeCred),
	)
	require.NoError(t, db.RenewAccountExpirations(
		[]models.StakeCredentialRef{rewardCred},
		stabilityTestEpoch-1,
		nil,
	))

	gateOff := NewStabilityCheckInputs(
		db, nil, stabilityTestEpoch, false, stabilityConwayPParams(10), nil, nil,
	)
	got, err := EvaluateRatifiableHardForkInitiation(gateOff)
	require.NoError(t, err)
	require.NotNil(t, got, "gate off must preserve the expired account's vote")

	gateOn := NewStabilityCheckInputs(
		db, nil, stabilityTestEpoch, true, stabilityConwayPParams(10), nil, nil,
	)
	got, err = EvaluateRatifiableHardForkInitiation(gateOn)
	require.NoError(t, err)
	assert.Nil(
		t,
		got,
		"gate on must match boundary tally and exclude expired stake",
	)
}

// TestEvaluateRatifiableHardForkInitiation_BootstrapDRepOnly_NotRatifiable
// pins the negative side of bootstrap: a DRep yes vote does not substitute
// for the required CC and SPO votes on a hard-fork proposal.
func TestEvaluateRatifiableHardForkInitiation_BootstrapDRepOnly_NotRatifiable(
	t *testing.T,
) {
	db, _ := newTallyTestDB(t)
	proposal := seedHardForkInitiationProposal(
		t,
		db,
		stabilityTestEpoch,
		11,
		1,
		stabilityProposalTx,
	)
	drepCred := seedDRepWithStake(t, db, 1_000)
	seedDRepYesVote(t, db, proposal.ID, drepCred)

	in := NewStabilityCheckInputs(
		db, nil, stabilityTestEpoch, false, stabilityConwayPParams(9), nil, nil,
	)
	got, err := EvaluateRatifiableHardForkInitiation(in)
	require.NoError(t, err)
	assert.Nil(t, got, "DRep-only vote must not ratify a bootstrap hard fork")
}

// When two HardForkInitiation proposals are simultaneously ratifiable,
// EvaluateRatifiableHardForkInitiation must return the one with the
// lower added_slot — matching the order ProcessEpoch's RATIFY phase
// would select. This pins the parity between the mid-epoch and
// boundary paths without a full integration test.
func TestEvaluateRatifiableHardForkInitiation_MultipleRatifiable_PicksLowestAddedSlot(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)

	const (
		earlyMajor    uint   = 11
		earlySlot     uint64 = 1
		earlyHashSeed byte   = 0xA1
		lateMajor     uint   = 12
		lateSlot      uint64 = 10
		lateHashSeed  byte   = 0xA2
	)

	early := seedHardForkInitiationProposal(
		t, db, stabilityTestEpoch, earlyMajor, earlySlot, earlyHashSeed,
	)
	late := seedHardForkInitiationProposal(
		t, db, stabilityTestEpoch, lateMajor, lateSlot, lateHashSeed,
	)

	drepCred := seedDRepWithStake(t, db, 1_000)
	seedDRepYesVote(t, db, early.ID, drepCred)
	seedDRepYesVote(t, db, late.ID, drepCred)
	seedHardForkCommitteeAndSPOVotes(t, db, store, early, late)

	in := NewStabilityCheckInputs(
		db, nil, stabilityTestEpoch, false, stabilityConwayPParams(10), nil, nil,
	)
	got, err := EvaluateRatifiableHardForkInitiation(in)
	require.NoError(t, err)
	require.NotNil(t, got, "at least one HFI should be ratifiable")
	assert.Equal(t, early.ID, got.Proposal.ID,
		"the lower added_slot proposal must win")
	assert.Equal(t, earlyMajor, got.NewMajor,
		"NewMajor must come from the lower added_slot proposal")
}
