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
	"database/sql"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requireSoleCreditPostSnapshot asserts the single credit journaled for a
// credential carries the expected PostSnapshot flag.
func requireSoleCreditPostSnapshot(
	t *testing.T,
	db *sql.DB,
	credential []byte,
	want bool,
	msg string,
) {
	t.Helper()
	var postSnapshot bool
	require.NoError(t, db.QueryRow(`
SELECT post_snapshot FROM account_reward_delta
WHERE credential_tag = ? AND staking_key = ? AND withdrawal = FALSE`,
		0, credential).Scan(&postSnapshot))
	require.Equal(t, want, postSnapshot, msg)
}

func insertBoundaryAccount(
	t *testing.T,
	store *tallyTestStore,
	credential []byte,
) {
	t.Helper()
	_, err := store.raw.Exec(`INSERT INTO account (staking_key, reward, active)
VALUES (?, '0', TRUE)`, credential)
	require.NoError(t, err)
}

// TestBoundaryCreditVisibility_TreasuryWithdrawalIsExcludedFromSnapshot pins an
// enacted treasury withdrawal as post-SNAP: cardano-ledger's EPOCH rule runs
// SNAP (and POOLREAP) before ratification/enactment, so a withdrawal credited at
// the boundary is not part of that boundary's mark snapshot.
func TestBoundaryCreditVisibility_TreasuryWithdrawalIsExcludedFromSnapshot(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 0x61)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	insertBoundaryAccount(t, store, stakeCred)
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	require.NoError(t, applyTreasuryWithdrawal(
		&EnactmentContext{DB: db, Slot: 200},
		&lcommon.TreasuryWithdrawalGovAction{
			Withdrawals: map[*lcommon.Address]uint64{&rewardAddr: 7},
		},
		&models.GovernanceProposal{TxHash: testBytes(32, 0x62)},
	))

	requireSoleCreditPostSnapshot(
		t,
		store.raw,
		stakeCred,
		true,
		"enactment runs after SNAP, so a treasury withdrawal must be excluded from the mark snapshot",
	)
}

// TestBoundaryCreditVisibility_ProposalRefundIsExcludedFromSnapshot pins a
// governance proposal-deposit refund as post-SNAP, for the same reason.
func TestBoundaryCreditVisibility_ProposalRefundIsExcludedFromSnapshot(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 0x63)
	rewardAddrBytes := buildRewardAddr(t, stakeCred)
	insertBoundaryAccount(t, store, stakeCred)

	require.NoError(
		t,
		refundProposalDeposit(db, nil, &models.GovernanceProposal{
			TxHash:        testBytes(32, 0x64),
			Deposit:       7,
			ReturnAddress: rewardAddrBytes,
		}, 200),
	)

	requireSoleCreditPostSnapshot(
		t,
		store.raw,
		stakeCred,
		true,
		"a proposal-deposit refund is enacted after SNAP and must be excluded from the mark snapshot",
	)
}

// TestMithrilSeededRootUnblocksChainedProposal mirrors the failure
// shape: a chained HardForkInitiation arriving on a
// node whose only enacted-root visibility comes from a Mithril
// snapshot must be accepted by validateParentChain when the per-
// purpose root has been seeded as a synthetic enacted row.
func TestMithrilSeededRootUnblocksChainedProposal(t *testing.T) {
	t.Parallel()

	db, _ := newTallyTestDB(t)

	rootHash := testBytes(32, 0xA1)
	parentIdx := uint32(0)

	// Synthetic seeded root, equivalent to what
	// ledgerstate.seedPrevGovActionIds writes.
	enactedEpoch := uint64(500)
	enactedSlot := uint64(123_456)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        rootHash,
		ActionIndex:   parentIdx,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: 0,
		ExpiresEpoch:  0,
		EnactedEpoch:  &enactedEpoch,
		EnactedSlot:   &enactedSlot,
		Deposit:       0,
		ReturnAddress: make([]byte, 29),
		AnchorURL:     "",
		AnchorHash:    make([]byte, 32),
		AddedSlot:     0,
	}, nil))

	root, err := db.GetLastEnactedGovernanceProposal(
		[]uint8{uint8(lcommon.GovActionTypeHardForkInitiation)},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, root, "synthetic root should be visible to ratification")

	// Chained child whose parent points at the seeded root. Without
	// the synthetic row this would be rejected by validateParentChain
	// (currentRoot=nil, parent set) and silently expire — the bug.
	child := &models.GovernanceProposal{
		ActionType:      uint8(lcommon.GovActionTypeHardForkInitiation),
		ParentTxHash:    rootHash,
		ParentActionIdx: &parentIdx,
	}
	assert.True(
		t, validateParentChain(child, root),
		"chained child must validate against the seeded root",
	)

	// And without the root, the child still fails — we want the
	// regression to come back if seeding ever silently no-ops.
	assert.False(
		t, validateParentChain(child, nil),
		"sanity: chained child still fails without a root",
	)
}

// TestValidateParentChain_NoConfidenceRootAllowsCommitteeUpdate
// proves that when committee NoConfidence is the seeded root,
// a subsequent UpdateCommittee whose parent matches the root is
// accepted (purposeCommittee groups both action types).
func TestValidateParentChain_NoConfidenceRootAllowsCommitteeUpdate(
	t *testing.T,
) {
	t.Parallel()

	rootHash := testBytes(32, 0xC1)
	parentIdx := uint32(2)

	root := &models.GovernanceProposal{
		TxHash:      rootHash,
		ActionIndex: parentIdx,
		ActionType:  uint8(lcommon.GovActionTypeNoConfidence),
	}
	child := &models.GovernanceProposal{
		ActionType:      uint8(lcommon.GovActionTypeUpdateCommittee),
		ParentTxHash:    rootHash,
		ParentActionIdx: &parentIdx,
	}
	assert.True(
		t, validateParentChain(child, root),
		"UpdateCommittee chained off NoConfidence root must validate",
	)
}

// noConfidencePParams returns post-bootstrap (major 10) Conway pparams with
// a realistic SPO MotionNoConfidence threshold (0.51, Preview's real value)
// and the DRep MotionNoConfidence threshold zeroed so DRep approval is
// automatic -- isolating the assertions below to the SPO gate that
// stakeEpochFor's same-boundary fix affects. NoConfidence needs no CC
// approval (needsCCApproval returns false for it), so no committee fixture
// is needed either.
func noConfidencePParams() *conway.ConwayProtocolParameters {
	p := &conway.ConwayProtocolParameters{}
	p.ProtocolVersion.Major = 10
	p.PoolVotingThresholds.MotionNoConfidence = newRat(51, 100)
	return p
}

func seedNoConfidenceProposal(
	t *testing.T,
	db *database.Database,
	proposedEpoch, expiresEpoch uint64,
) *models.GovernanceProposal {
	t.Helper()
	actionCbor, err := cbor.Encode(&lcommon.NoConfidenceGovAction{
		Type: uint(lcommon.GovActionTypeNoConfidence),
	})
	require.NoError(t, err)
	proposal := &models.GovernanceProposal{
		TxHash:        testBytes(32, 0x60),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeNoConfidence),
		ProposedEpoch: proposedEpoch,
		ExpiresEpoch:  expiresEpoch,
		AnchorURL:     "https://example.invalid/no-confidence-ratify",
		AnchorHash:    testBytes(32, 0x61),
		ReturnAddress: testBytes(29, 0x62),
		GovActionCbor: actionCbor,
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	loaded, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	return loaded
}

// TestProcessEpoch_NoConfidence_SPOThresholdUsesSameBoundaryMark covers the
// second SPO-gated action type fix touches (HardForkInitiation
// being the first, covered by ledger's TestHardForkInitiation_* tests).
// stakeEpochFor(newEpoch) resolves to newEpoch, so the SPO tally must read
// mark[newEpoch] -- seeded here directly, matching how a standalone
// ProcessEpoch caller (as opposed to a real epoch-rollover transaction)
// seeds it. The 0.51 threshold and 0.4779/0.6283 stake ratios are the same
// real values measured on Preview for HardForkInitiation; using
// them here for NoConfidence proves the fix is the shared stakeEpochFor
// primitive, not a HardForkInitiation-specific patch.
func TestProcessEpoch_NoConfidence_SPOThresholdUsesSameBoundaryMark(
	t *testing.T,
) {
	t.Parallel()

	const newEpoch = uint64(742)

	tests := []struct {
		name       string
		yesStake   uint64
		wantRatify bool
	}{
		{"below threshold", 4_779, false}, // ratio 0.4779 < 0.51
		{"above threshold", 6_283, true},  // ratio 0.6283 >= 0.51
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, store := newTallyTestDB(t)
			proposal := seedNoConfidenceProposal(t, db, newEpoch-5, newEpoch+10)

			yesPool := testBytes(28, 0x70)
			silentPool := testBytes(28, 0x71)
			seedPoolWithStake(
				t, store, yesPool, testBytes(29, 0x72), tt.yesStake, newEpoch,
			)
			seedPoolWithStake(
				t, store, silentPool, testBytes(29, 0x73),
				10_000-tt.yesStake, newEpoch,
			)
			require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
				ProposalID:      proposal.ID,
				VoterType:       models.VoterTypeSPO,
				VoterCredential: yesPool,
				Vote:            models.VoteYes,
				AddedSlot:       2,
			}, nil))

			txn := db.MetadataTxn(true)
			defer txn.Release()
			out, err := ProcessEpoch(&EpochInput{
				DB:           db,
				Txn:          txn,
				PrevEpoch:    newEpoch - 1,
				NewEpoch:     newEpoch,
				BoundarySlot: newEpoch * 100,
				PParams:      noConfidencePParams(),
				UpdateFn: func(
					pparams lcommon.ProtocolParameters,
					_ any,
				) (lcommon.ProtocolParameters, error) {
					return pparams, nil
				},
			})
			require.NoError(t, err)
			require.NoError(t, txn.Commit())

			reloaded, err := db.GetGovernanceProposal(
				proposal.TxHash, proposal.ActionIndex, nil,
			)
			require.NoError(t, err)
			if tt.wantRatify {
				require.Equal(t, 1, out.RatifiedCount)
				require.NotNil(t, reloaded.RatifiedEpoch)
				require.Equal(t, newEpoch, *reloaded.RatifiedEpoch)
			} else {
				require.Equal(t, 0, out.RatifiedCount)
				require.Nil(t, reloaded.RatifiedEpoch)
			}
		})
	}
}

// TestMidEpochPredictionAndBoundaryReadDifferentMarks locks the one
// ratification input the mid-epoch prediction cannot share with the boundary
// it predicts. ProcessEpoch tallies mark[NewEpoch]; the mid-epoch check
// tallies mark[CurrentEpoch], because SNAP does not capture mark[NewEpoch]
// until the boundary runs. Seeding the two marks on either side of the SPO
// threshold makes the divergence observable: the same database, the same
// votes and the same pparams yield no mid-epoch prediction during epoch 741
// and a ratification at the boundary into 742.
//
// The stakes are Preview's own Plomin numbers: mark[741] at
// 0.4757 against the 0.51 pvtHardForkInitiation threshold, mark[742] at
// 0.6283.
func TestMidEpochPredictionAndBoundaryReadDifferentMarks(t *testing.T) {
	t.Parallel()

	const currentEpoch = uint64(741)
	const newEpoch = currentEpoch + 1
	const targetMajor uint = 11

	db, store := newTallyTestDB(t)
	proposal := seedHardForkInitiationProposal(
		t, db, currentEpoch, targetMajor, 1, 0x90,
	)
	// The shared seed leaves an all-zero return address, which the boundary's
	// enactment precondition rejects for a proposal carrying a deposit.
	// Ratification is what this test measures, so give it a decodable reward
	// account (header 0xE0, key-hash credential).
	proposal.ReturnAddress = append([]byte{0xE0}, testBytes(28, 0x97)...)
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))

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
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      proposal.ID,
		VoterType:       models.VoterTypeCC,
		VoterCredential: hotCred,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))

	yesPool := testBytes(28, 0x93)
	silentPool := testBytes(28, 0x94)
	seedPoolWithStake(
		t, store, yesPool, testBytes(29, 0x95), 4_757, currentEpoch,
	)
	seedPoolWithStake(
		t, store, silentPool, testBytes(29, 0x96), 5_243, currentEpoch,
	)
	seedPoolWithStake(
		t, store, yesPool, testBytes(29, 0x95), 6_283, newEpoch,
	)
	seedPoolWithStake(
		t, store, silentPool, testBytes(29, 0x96), 3_717, newEpoch,
	)
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      proposal.ID,
		VoterType:       models.VoterTypeSPO,
		VoterCredential: yesPool,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))

	got, err := EvaluateRatifiableHardForkInitiation(NewStabilityCheckInputs(
		db, nil, currentEpoch, false, stabilityConwayPParams(9), nil, nil,
	))
	require.NoError(t, err)
	require.Nil(t, got, "mid-epoch check must read mark[currentEpoch]")

	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    currentEpoch,
		NewEpoch:     newEpoch,
		BoundarySlot: newEpoch * 100,
		PParams:      stabilityConwayPParams(9),
		UpdateFn: func(
			pparams lcommon.ProtocolParameters,
			_ any,
		) (lcommon.ProtocolParameters, error) {
			return pparams, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())
	require.Equal(t, 1, out.RatifiedCount,
		"boundary tally must read mark[newEpoch]")
}

// TestProcessEpoch_MissingSameBoundarySPOState_Errors pins the failure mode
// an epoch rollover takes when LedgerState.SetCurrentBoundarySPOStakeHook was
// never installed: stakeEpochFor resolves to NewEpoch, whose mark row a real
// rollover has not written yet, so the DB fallback finds no rows at all.
// tallySPOVotes returns early on an empty distribution, which yields a zero
// SPO denominator and a permanent, silent non-ratification of every SPO-gated
// action. ProcessEpoch must reject that input instead of tallying against it.
func TestProcessEpoch_MissingSameBoundarySPOState_Errors(t *testing.T) {
	t.Parallel()

	const newEpoch = uint64(742)

	db, store := newTallyTestDB(t)
	proposal := seedNoConfidenceProposal(t, db, newEpoch-5, newEpoch+10)

	yesPool := testBytes(28, 0x80)
	// Stake exists, but only under the previous boundary's mark. That is
	// exactly what an un-wired rollover sees: mark[newEpoch] is written at
	// the end of the same rollover, long after RATIFY runs.
	seedPoolWithStake(
		t, store, yesPool, testBytes(29, 0x81), 10_000, newEpoch-1,
	)
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      proposal.ID,
		VoterType:       models.VoterTypeSPO,
		VoterCredential: yesPool,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	_, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    newEpoch - 1,
		NewEpoch:     newEpoch,
		BoundarySlot: newEpoch * 100,
		PParams:      noConfidencePParams(),
		UpdateFn: func(
			pparams lcommon.ProtocolParameters,
			_ any,
		) (lcommon.ProtocolParameters, error) {
			return pparams, nil
		},
	})
	require.ErrorIs(t, err, ErrMissingCurrentBoundarySPOState)

	reloaded, err := db.GetGovernanceProposal(
		proposal.TxHash, proposal.ActionIndex, nil,
	)
	require.NoError(t, err)
	require.Nil(t, reloaded.RatifiedEpoch)
}

// TestProcessEpoch_SuppliedSameBoundarySPOState_Ratifies is the control for
// the test above: the same database, with the same absent mark[newEpoch] row,
// ratifies once the caller supplies the same-boundary distribution the
// rollover resolves from its own SNAP-point read. The guard must fire only on
// the un-wired path, never on a wired one.
func TestProcessEpoch_SuppliedSameBoundarySPOState_Ratifies(t *testing.T) {
	t.Parallel()

	const newEpoch = uint64(742)

	db, store := newTallyTestDB(t)
	proposal := seedNoConfidenceProposal(t, db, newEpoch-5, newEpoch+10)

	yesPool := testBytes(28, 0x82)
	seedPoolWithStake(
		t, store, yesPool, testBytes(29, 0x83), 10_000, newEpoch-1,
	)
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      proposal.ID,
		VoterType:       models.VoterTypeSPO,
		VoterCredential: yesPool,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    newEpoch - 1,
		NewEpoch:     newEpoch,
		BoundarySlot: newEpoch * 100,
		PParams:      noConfidencePParams(),
		UpdateFn: func(
			pparams lcommon.ProtocolParameters,
			_ any,
		) (lcommon.ProtocolParameters, error) {
			return pparams, nil
		},
		CurrentBoundarySPOState: &SPOVotingState{
			Dist: []*models.PoolStakeSnapshot{
				{
					Epoch:        newEpoch,
					SnapshotType: "mark",
					PoolKeyHash:  yesPool,
					TotalStake:   types.Uint64(10_000),
				},
			},
			TotalStake: 10_000,
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())
	require.Equal(t, 1, out.RatifiedCount)

	reloaded, err := db.GetGovernanceProposal(
		proposal.TxHash, proposal.ActionIndex, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, reloaded.RatifiedEpoch)
	require.Equal(t, newEpoch, *reloaded.RatifiedEpoch)
}

const (
	spoNonVoterYesStake    uint64 = 60
	spoNonVoterSilentStake uint64 = 40
)

type spoNonVoterRatificationCase struct {
	actionType           lcommon.GovActionType
	rewardDefault        uint64
	thresholdNumerator   int64
	thresholdDenominator int64
	silentVote           *uint8
}

//go:fix inline
func votePointer(vote uint8) *uint8 {
	return new(vote)
}

// TestProcessEpochSPONonVoterDenominators matches the same 60/40 stake
// distribution on both sides of the Conway bootstrap rule. A silent pool on a
// HardForkInitiation remains in the denominator, so 60% passes exactly 60% but
// not 61%. A silent pool on a bootstrap ParameterChange is Abstain, so the same
// explicit Yes stake passes both thresholds.
func TestProcessEpochSPONonVoterDenominators(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		actionType  lcommon.GovActionType
		defaultDRep uint64
		numerator   int64
		denominator int64
		wantRatify  bool
	}{
		{
			name:        "hard fork at denominator equality",
			actionType:  lcommon.GovActionTypeHardForkInitiation,
			defaultDRep: models.DrepTypeAlwaysAbstain,
			numerator:   3,
			denominator: 5,
			wantRatify:  true,
		},
		{
			name:        "hard fork above denominator ratio",
			actionType:  lcommon.GovActionTypeHardForkInitiation,
			defaultDRep: models.DrepTypeAlwaysAbstain,
			numerator:   61,
			denominator: 100,
			wantRatify:  false,
		},
		{
			name:        "bootstrap parameter change at implicit-no ratio",
			actionType:  lcommon.GovActionTypeParameterChange,
			defaultDRep: models.DrepTypeAlwaysNoConfidence,
			numerator:   3,
			denominator: 5,
			wantRatify:  true,
		},
		{
			name:        "bootstrap parameter change above implicit-no ratio",
			actionType:  lcommon.GovActionTypeParameterChange,
			defaultDRep: models.DrepTypeAlwaysNoConfidence,
			numerator:   61,
			denominator: 100,
			wantRatify:  true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ratified := runSPONonVoterRatification(
				t,
				spoNonVoterRatificationCase{
					actionType:           testCase.actionType,
					rewardDefault:        testCase.defaultDRep,
					thresholdNumerator:   testCase.numerator,
					thresholdDenominator: testCase.denominator,
				},
			)
			assert.Equal(t, testCase.wantRatify, ratified)
		})
	}
}

// TestProcessEpochSPONonVoterRatification exercises the complete persisted
// RATIFY path and the explicit-vote controls. Reward-account defaults cannot
// turn a silent hard-fork pool into Abstain, while an explicit Abstain still
// does. Bootstrap makes a silent pool Abstain before reward defaults, while an
// explicit No still remains No.
func TestProcessEpochSPONonVoterRatification(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		actionType  lcommon.GovActionType
		defaultDRep uint64
		silentVote  *uint8
		wantRatify  bool
	}{
		{
			name:        "hard fork silent pool is implicit no",
			actionType:  lcommon.GovActionTypeHardForkInitiation,
			defaultDRep: models.DrepTypeAlwaysAbstain,
			wantRatify:  false,
		},
		{
			name:        "hard fork explicit abstain is excluded",
			actionType:  lcommon.GovActionTypeHardForkInitiation,
			defaultDRep: models.DrepTypeAlwaysAbstain,
			silentVote:  votePointer(models.VoteAbstain),
			wantRatify:  true,
		},
		{
			name:        "bootstrap parameter-change silent pool abstains",
			actionType:  lcommon.GovActionTypeParameterChange,
			defaultDRep: models.DrepTypeAlwaysNoConfidence,
			wantRatify:  true,
		},
		{
			name:        "bootstrap parameter-change explicit no remains no",
			actionType:  lcommon.GovActionTypeParameterChange,
			defaultDRep: models.DrepTypeAlwaysNoConfidence,
			silentVote:  votePointer(models.VoteNo),
			wantRatify:  false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ratified := runSPONonVoterRatification(
				t,
				spoNonVoterRatificationCase{
					actionType:           testCase.actionType,
					rewardDefault:        testCase.defaultDRep,
					thresholdNumerator:   3,
					thresholdDenominator: 4,
					silentVote:           testCase.silentVote,
				},
			)
			assert.Equal(t, testCase.wantRatify, ratified)
		})
	}
}

func runSPONonVoterRatification(
	t *testing.T,
	testCase spoNonVoterRatificationCase,
) bool {
	t.Helper()
	db, store := newTallyTestDB(t)
	proposal := seedSPONonVoterProposal(t, db, testCase.actionType)

	coldCredential := testBytes(28, 0xE1)
	hotCredential := testBytes(28, 0xE2)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredHash: coldCredential,
		ExpiresEpoch: stabilityTestEpoch + 10,
		AddedSlot:    1,
	}}, nil))
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: coldCredential,
		HotCredential:  hotCredential,
		CertificateID:  1,
		AddedSlot:      1,
	})

	yesPool := testBytes(28, 0xE3)
	silentPool := testBytes(28, 0xE4)
	snapshotEpoch := stakeEpochFor(stabilityTestEpoch)
	seedPoolWithStake(
		t, store, yesPool, testBytes(28, 0xE5), spoNonVoterYesStake,
		snapshotEpoch,
	)
	seedPoolWithStake(
		t, store, silentPool, testBytes(28, 0xE6), spoNonVoterSilentStake,
		snapshotEpoch,
	)
	seedRewardAccountDelegation(
		t, store, testBytes(28, 0xE6), nil, testCase.rewardDefault,
	)
	resolveSnapshotAutoVotes(t, db, snapshotEpoch)

	for _, vote := range []*models.GovernanceVote{
		{
			ProposalID:      proposal.ID,
			VoterType:       models.VoterTypeCC,
			VoterCredential: hotCredential,
			Vote:            models.VoteYes,
			AddedSlot:       2,
		},
		{
			ProposalID:      proposal.ID,
			VoterType:       models.VoterTypeSPO,
			VoterCredential: yesPool,
			Vote:            models.VoteYes,
			AddedSlot:       2,
		},
	} {
		require.NoError(t, db.SetGovernanceVote(vote, nil))
	}
	if testCase.silentVote != nil {
		require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
			ProposalID:      proposal.ID,
			VoterType:       models.VoterTypeSPO,
			VoterCredential: silentPool,
			Vote:            *testCase.silentVote,
			AddedSlot:       2,
		}, nil))
	}

	pparams := conwayPParamsFixture(bootstrapProtocolVersion)
	threshold := newRat(
		testCase.thresholdNumerator,
		testCase.thresholdDenominator,
	)
	switch testCase.actionType {
	case lcommon.GovActionTypeHardForkInitiation:
		pparams.PoolVotingThresholds.HardForkInitiation = threshold
	case lcommon.GovActionTypeParameterChange:
		pparams.PoolVotingThresholds.PpSecurityGroup = threshold
	default:
		t.Fatalf("unsupported governance action type %d", testCase.actionType)
	}

	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    stabilityTestEpoch - 1,
		NewEpoch:     stabilityTestEpoch,
		BoundarySlot: 500,
		PParams:      pparams,
		UpdateFn: func(
			parameters lcommon.ProtocolParameters,
			_ any,
		) (lcommon.ProtocolParameters, error) {
			return parameters, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	stored, err := db.GetGovernanceProposal(
		proposal.TxHash,
		proposal.ActionIndex,
		nil,
	)
	require.NoError(t, err)
	ratified := out.RatifiedCount == 1
	assert.Equal(t, ratified, stored.RatifiedEpoch != nil)
	assert.Equal(t, ratified, stored.RatifiedSlot != nil)
	return ratified
}

func seedSPONonVoterProposal(
	t *testing.T,
	db *database.Database,
	actionType lcommon.GovActionType,
) *models.GovernanceProposal {
	t.Helper()
	returnAddress, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		testBytes(28, 0xF8),
	)
	require.NoError(t, err)
	returnAddressBytes, err := returnAddress.Bytes()
	require.NoError(t, err)

	var actionCBOR []byte
	switch actionType {
	case lcommon.GovActionTypeHardForkInitiation:
		action := &lcommon.HardForkInitiationGovAction{
			Type: uint(lcommon.GovActionTypeHardForkInitiation),
		}
		action.ProtocolVersion.Major = bootstrapProtocolVersion + 1
		actionCBOR, err = cbor.Encode(action)
	case lcommon.GovActionTypeParameterChange:
		maxTxSize := uint(16_384)
		actionCBOR, err = cbor.Encode(&conway.ConwayParameterChangeGovAction{
			Type: uint(lcommon.GovActionTypeParameterChange),
			ParamUpdate: conway.ConwayProtocolParameterUpdate{
				MaxTxSize: &maxTxSize,
			},
		})
	default:
		t.Fatalf("unsupported governance action type %d", actionType)
	}
	require.NoError(t, err)

	proposal := &models.GovernanceProposal{
		TxHash:        testBytes(32, byte(actionType)+0xF0),
		ActionIndex:   0,
		ActionType:    uint8(actionType),
		ProposedEpoch: stabilityTestEpoch - 1,
		ExpiresEpoch:  stabilityTestEpoch + 10,
		Deposit:       1_000,
		ReturnAddress: returnAddressBytes,
		AnchorURL:     "https://example.invalid/spo-nonvoter",
		AnchorHash:    testBytes(32, 0xF9),
		GovActionCbor: actionCBOR,
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	stored, err := db.GetGovernanceProposal(
		proposal.TxHash,
		proposal.ActionIndex,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, stored)
	return stored
}

// TestRatifyLevelForkRestoresVoteAcrossAllVoterTypes covers at
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
	// replacement must flip the outcome ( "preserve normal
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
