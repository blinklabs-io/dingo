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
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestMidEpochPredictionAndBoundaryReadDifferentMarks locks the one
// ratification input the mid-epoch prediction cannot share with the boundary
// it predicts. ProcessEpoch tallies mark[NewEpoch]; the mid-epoch check
// tallies mark[CurrentEpoch], because SNAP does not capture mark[NewEpoch]
// until the boundary runs. Seeding the two marks on either side of the SPO
// threshold makes the divergence observable: the same database, the same
// votes and the same pparams yield no mid-epoch prediction during epoch 741
// and a ratification at the boundary into 742.
//
// The stakes are Preview's own Plomin numbers (dingo#4441): mark[741] at
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
