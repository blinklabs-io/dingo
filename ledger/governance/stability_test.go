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
// post-bootstrap tests exercise the same code path as production. The
// bootstrap branch in ShouldRatify (major <= 9) ignores thresholds
// entirely; only one yes vote of any kind is required.
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

func TestEvaluateRatifiableHardForkInitiation_PreConway_ReturnsNil(t *testing.T) {
	db, _ := newTallyTestDB(t)
	// pre-Conway: no governance state machine yet
	in := NewStabilityCheckInputs(db, nil, stabilityTestEpoch, false, nil, nil, nil)
	got, err := EvaluateRatifiableHardForkInitiation(in)
	require.NoError(t, err)
	assert.Nil(t, got, "pre-Conway pparams must short-circuit to nil")
}

func TestEvaluateRatifiableHardForkInitiation_NoActiveProposals_ReturnsNil(t *testing.T) {
	db, _ := newTallyTestDB(t)
	in := NewStabilityCheckInputs(
		db, nil, stabilityTestEpoch, false, stabilityConwayPParams(9), nil, nil,
	)
	got, err := EvaluateRatifiableHardForkInitiation(in)
	require.NoError(t, err)
	assert.Nil(t, got, "empty active proposal set must yield nil")
}

func TestEvaluateRatifiableHardForkInitiation_OnlyOtherActionType_ReturnsNil(t *testing.T) {
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

// TestEvaluateRatifiableHardForkInitiation_BootstrapWithDRepYesVote pins
// the bootstrap ratification semantic: in pparams with major <= 9 a
// single yes vote of any non-zero magnitude suffices to make a proposal
// ratifiable, regardless of thresholds. The mid-epoch helper must
// surface this as an upcoming transition with the correct target major
// version (extracted from the proposal's encoded action).
func TestEvaluateRatifiableHardForkInitiation_BootstrapWithDRepYesVote(t *testing.T) {
	db, _ := newTallyTestDB(t)

	const targetMajor uint = 11
	proposal := seedHardForkInitiationProposal(
		t, db, stabilityTestEpoch, targetMajor, 1, stabilityProposalTx,
	)
	drepCred := seedDRepWithStake(t, db, 1_000)
	seedDRepYesVote(t, db, proposal.ID, drepCred)

	in := NewStabilityCheckInputs(
		db, nil, stabilityTestEpoch, false, stabilityConwayPParams(9), nil, nil,
	)
	got, err := EvaluateRatifiableHardForkInitiation(in)
	require.NoError(t, err)
	require.NotNil(t, got, "bootstrap + yes vote must be ratifiable")
	assert.Equal(t, targetMajor, got.NewMajor,
		"target major must come from the proposal's encoded action")
	assert.Equal(t, proposal.ID, got.Proposal.ID,
		"the helper must return the proposal that ratifies")
}

func TestEvaluateRatifiableHardForkInitiation_DelegatorInactivityParity(t *testing.T) {
	db, _ := newTallyTestDB(t)

	proposal := seedHardForkInitiationProposal(
		t, db, stabilityTestEpoch, 11, 1, stabilityProposalTx,
	)
	drepCred := seedDRepWithStake(t, db, 1_000)
	seedDRepYesVote(t, db, proposal.ID, drepCred)
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
		db, nil, stabilityTestEpoch, false, stabilityConwayPParams(9), nil, nil,
	)
	got, err := EvaluateRatifiableHardForkInitiation(gateOff)
	require.NoError(t, err)
	require.NotNil(t, got, "gate off must preserve the expired account's vote")

	gateOn := NewStabilityCheckInputs(
		db, nil, stabilityTestEpoch, true, stabilityConwayPParams(9), nil, nil,
	)
	got, err = EvaluateRatifiableHardForkInitiation(gateOn)
	require.NoError(t, err)
	assert.Nil(t, got, "gate on must match boundary tally and exclude expired stake")
}

// TestEvaluateRatifiableHardForkInitiation_BootstrapNoVotes_NotRatifiable
// pins the negative side of bootstrap: even though thresholds are zero,
// at least ONE yes vote (DRep, SPO, or CC) is required. With zero votes
// of any kind, the proposal does not ratify.
func TestEvaluateRatifiableHardForkInitiation_BootstrapNoVotes_NotRatifiable(t *testing.T) {
	db, _ := newTallyTestDB(t)
	seedHardForkInitiationProposal(t, db, stabilityTestEpoch, 11, 1, stabilityProposalTx)

	in := NewStabilityCheckInputs(
		db, nil, stabilityTestEpoch, false, stabilityConwayPParams(9), nil, nil,
	)
	got, err := EvaluateRatifiableHardForkInitiation(in)
	require.NoError(t, err)
	assert.Nil(t, got, "no votes means no ratification, even in bootstrap")
}

// When two HardForkInitiation proposals are simultaneously ratifiable,
// EvaluateRatifiableHardForkInitiation must return the one with the
// lower added_slot — matching the order ProcessEpoch's RATIFY phase
// would select. This pins the parity between the mid-epoch and
// boundary paths without a full integration test.
func TestEvaluateRatifiableHardForkInitiation_MultipleRatifiable_PicksLowestAddedSlot(t *testing.T) {
	db, _ := newTallyTestDB(t)

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

	in := NewStabilityCheckInputs(
		db, nil, stabilityTestEpoch, false, stabilityConwayPParams(9), nil, nil,
	)
	got, err := EvaluateRatifiableHardForkInitiation(in)
	require.NoError(t, err)
	require.NotNil(t, got, "at least one HFI should be ratifiable")
	assert.Equal(t, early.ID, got.Proposal.ID,
		"the lower added_slot proposal must win")
	assert.Equal(t, earlyMajor, got.NewMajor,
		"NewMajor must come from the lower added_slot proposal")
}
