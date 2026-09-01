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
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTallyDRepVotesIncludesAlwaysAbstain(t *testing.T) {
	db, store := newTallyTestDB(t)
	drepCred := testBytes(28, 1)
	stakeCred := testBytes(28, 2)
	abstainStakeCred := testBytes(28, 3)

	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
	}))
	seedDRepStake(
		t, store, stakeCred, drepCred, models.DrepTypeAddrKeyHash, 60,
		1,
	)
	seedDRepStake(
		t, store, abstainStakeCred, nil, models.DrepTypeAlwaysAbstain,
		40, 2,
	)

	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	}
	err := tallyDRepVotes(
		&TallyContext{DB: db},
		[]*models.GovernanceVote{{
			VoterType:       models.VoterTypeDRep,
			VoterCredential: drepCred,
			Vote:            models.VoteYes,
		}},
		tally,
	)
	require.NoError(t, err)

	assert.Equal(t, uint64(100), tally.DRepTotalStake)
	assert.Equal(t, uint64(60), tally.DRepYesStake)
	assert.Equal(t, uint64(0), tally.DRepNoStake)
	assert.Equal(t, uint64(40), tally.DRepAbstainStake)
	assert.Equal(t, "1/1", tally.DRepYesRatio().String())
}

func TestTallyDRepVotesIncludesAlwaysNoConfidence(t *testing.T) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 4)
	seedDRepStake(
		t, store, stakeCred, nil, models.DrepTypeAlwaysNoConfidence,
		30, 3,
	)

	noConfidenceTally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeNoConfidence),
	}
	err := tallyDRepVotes(
		&TallyContext{DB: db},
		nil,
		noConfidenceTally,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(30), noConfidenceTally.DRepTotalStake)
	assert.Equal(t, uint64(30), noConfidenceTally.DRepYesStake)
	assert.Equal(t, uint64(0), noConfidenceTally.DRepNoStake)

	updateCommitteeTally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeUpdateCommittee),
	}
	err = tallyDRepVotes(
		&TallyContext{DB: db},
		nil,
		updateCommitteeTally,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(30), updateCommitteeTally.DRepTotalStake)
	assert.Equal(t, uint64(0), updateCommitteeTally.DRepYesStake)
	assert.Equal(t, uint64(30), updateCommitteeTally.DRepNoStake)

	pparams := conwayPParamsFixture(10)
	noConfidenceDecision := ShouldRatify(RatifyInputs{
		Tally:           noConfidenceTally,
		PParams:         pparams,
		ActiveDRepCount: 0,
		MajorVersion:    10,
	})
	assert.True(t, noConfidenceDecision.DRepApproved)

	updateCommitteeDecision := ShouldRatify(RatifyInputs{
		Tally:   updateCommitteeTally,
		PParams: pparams,
		GovAction: &lcommon.UpdateCommitteeGovAction{
			Type:       uint(lcommon.GovActionTypeUpdateCommittee),
			CredEpochs: map[*lcommon.Credential]uint{},
		},
		ActiveDRepCount: 0,
		MajorVersion:    10,
	})
	assert.False(t, updateCommitteeDecision.DRepApproved)
}

func TestTallyDRepVotesSeparatesSameHashByCredentialTag(t *testing.T) {
	db, store := newTallyTestDB(t)
	drepCred := testBytes(28, 9)
	keyStakeCred := testBytes(28, 10)
	scriptStakeCred := testBytes(28, 11)

	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		CredentialTag: 0,
		Credential:    drepCred,
		Active:        true,
	}))
	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		CredentialTag: 1,
		Credential:    drepCred,
		Active:        true,
	}))
	seedDRepStake(
		t, store, keyStakeCred, drepCred, models.DrepTypeAddrKeyHash, 60,
		12,
	)
	seedDRepStake(
		t, store, scriptStakeCred, drepCred, models.DrepTypeScriptHash, 40,
		13,
	)

	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	}
	err := tallyDRepVotes(
		&TallyContext{DB: db},
		[]*models.GovernanceVote{
			{
				VoterType:          models.VoterTypeDRep,
				VoterCredentialTag: 0,
				VoterCredential:    drepCred,
				Vote:               models.VoteYes,
			},
			{
				VoterType:          models.VoterTypeDRep,
				VoterCredentialTag: 1,
				VoterCredential:    drepCred,
				Vote:               models.VoteNo,
			},
		},
		tally,
	)
	require.NoError(t, err)

	assert.Equal(t, uint64(100), tally.DRepTotalStake)
	assert.Equal(t, uint64(60), tally.DRepYesStake)
	assert.Equal(t, uint64(40), tally.DRepNoStake)
	assert.Equal(t, uint64(0), tally.DRepAbstainStake)
}

// TestAddUint64Overflow exercises addUint64 at the exact uint64 max
// boundary: maxUint64-1 plus 1 is the largest sum that fits, plus 2
// overflows.
func TestAddUint64Overflow(t *testing.T) {
	maxUint64 := ^uint64(0)

	sum, err := addUint64(maxUint64-1, 1)
	require.NoError(t, err)
	assert.Equal(t, maxUint64, sum)

	_, err = addUint64(maxUint64-1, 2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows uint64")
}

// TestTallyDRepVotesTotalStakeOverflow drives the per-DRep DRepTotalStake
// accumulation in tallyDRepVotes to the exact uint64 max boundary via two
// synthetic DReps, bypassing the database entirely.
func TestTallyDRepVotesTotalStakeOverflow(t *testing.T) {
	maxUint64 := ^uint64(0)
	credA := models.StakeCredentialRef{Key: testBytes(28, 1)}
	credB := models.StakeCredentialRef{Key: testBytes(28, 2)}

	newState := func(powerB uint64) *DRepVotingState {
		return &DRepVotingState{
			Dreps: []*models.Drep{
				{CredentialTag: credA.Tag, Credential: credA.Key, Active: true},
				{CredentialTag: credB.Tag, Credential: credB.Key, Active: true},
			},
			Powers: map[string]uint64{
				credA.MapKey(): maxUint64 - 1,
				credB.MapKey(): powerB,
			},
		}
	}

	t.Run("just below overflow succeeds", func(t *testing.T) {
		tally := &ProposalTally{}
		err := tallyDRepVotes(&TallyContext{DRepState: newState(1)}, nil, tally)
		require.NoError(t, err)
		assert.Equal(t, maxUint64, tally.DRepTotalStake)
	})

	t.Run("just above overflow fails", func(t *testing.T) {
		tally := &ProposalTally{}
		err := tallyDRepVotes(&TallyContext{DRepState: newState(2)}, nil, tally)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "overflows uint64")
	})
}

// TestTallyDRepVotesVirtualPowerOverflow drives the AlwaysAbstain +
// AlwaysNoConfidence combination in tallyDRepVotes to the exact uint64 max
// boundary.
func TestTallyDRepVotesVirtualPowerOverflow(t *testing.T) {
	maxUint64 := ^uint64(0)

	newState := func(noConfidence uint64) *DRepVotingState {
		return &DRepVotingState{
			AbstainPower:      maxUint64 - 1,
			NoConfidencePower: noConfidence,
		}
	}

	t.Run("just below overflow succeeds", func(t *testing.T) {
		tally := &ProposalTally{}
		err := tallyDRepVotes(&TallyContext{DRepState: newState(1)}, nil, tally)
		require.NoError(t, err)
		assert.Equal(t, maxUint64, tally.DRepTotalStake)
	})

	t.Run("just above overflow fails", func(t *testing.T) {
		tally := &ProposalTally{}
		err := tallyDRepVotes(&TallyContext{DRepState: newState(2)}, nil, tally)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "overflows uint64")
	})
}

// TestTallySPOVotesYesStakeOverflow drives the explicit-vote SPOYesStake
// accumulation in tallySPOVotes to the exact uint64 max boundary via two
// synthetic pool snapshot rows, bypassing the database entirely.
func TestTallySPOVotesYesStakeOverflow(t *testing.T) {
	maxUint64 := ^uint64(0)
	poolA := testBytes(28, 1)
	poolB := testBytes(28, 2)

	newState := func(stakeB uint64) *SPOVotingState {
		return &SPOVotingState{
			Dist: []*models.PoolStakeSnapshot{
				{PoolKeyHash: poolA, TotalStake: types.Uint64(maxUint64 - 1)},
				{PoolKeyHash: poolB, TotalStake: types.Uint64(stakeB)},
			},
		}
	}
	votes := []*models.GovernanceVote{
		{VoterCredential: poolA, Vote: models.VoteYes},
		{VoterCredential: poolB, Vote: models.VoteYes},
	}

	t.Run("just below overflow succeeds", func(t *testing.T) {
		tally := &ProposalTally{}
		err := tallySPOVotes(&TallyContext{SPOState: newState(1)}, votes, tally)
		require.NoError(t, err)
		assert.Equal(t, maxUint64, tally.SPOYesStake)
	})

	t.Run("just above overflow fails", func(t *testing.T) {
		tally := &ProposalTally{}
		err := tallySPOVotes(&TallyContext{SPOState: newState(2)}, votes, tally)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "overflows uint64")
	})
}

// TestTallySPOVotesExplicitNoAndAbstainStakeOverflow drives the explicit-vote
// SPONoStake and SPOAbstainStake accumulations in tallySPOVotes to the exact
// uint64 max boundary via two synthetic pool snapshot rows.
func TestTallySPOVotesExplicitNoAndAbstainStakeOverflow(t *testing.T) {
	maxUint64 := ^uint64(0)
	poolA := testBytes(28, 1)
	poolB := testBytes(28, 2)

	newState := func(stakeB uint64) *SPOVotingState {
		return &SPOVotingState{
			Dist: []*models.PoolStakeSnapshot{
				{PoolKeyHash: poolA, TotalStake: types.Uint64(maxUint64 - 1)},
				{PoolKeyHash: poolB, TotalStake: types.Uint64(stakeB)},
			},
		}
	}

	t.Run("no stake", func(t *testing.T) {
		votes := []*models.GovernanceVote{
			{VoterCredential: poolA, Vote: models.VoteNo},
			{VoterCredential: poolB, Vote: models.VoteNo},
		}
		t.Run("just below overflow succeeds", func(t *testing.T) {
			tally := &ProposalTally{}
			err := tallySPOVotes(
				&TallyContext{SPOState: newState(1)},
				votes,
				tally,
			)
			require.NoError(t, err)
			assert.Equal(t, maxUint64, tally.SPONoStake)
		})
		t.Run("just above overflow fails", func(t *testing.T) {
			tally := &ProposalTally{}
			err := tallySPOVotes(
				&TallyContext{SPOState: newState(2)},
				votes,
				tally,
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "overflows uint64")
		})
	})

	t.Run("abstain stake", func(t *testing.T) {
		votes := []*models.GovernanceVote{
			{VoterCredential: poolA, Vote: models.VoteAbstain},
			{VoterCredential: poolB, Vote: models.VoteAbstain},
		}
		t.Run("just below overflow succeeds", func(t *testing.T) {
			tally := &ProposalTally{}
			err := tallySPOVotes(
				&TallyContext{SPOState: newState(1)},
				votes,
				tally,
			)
			require.NoError(t, err)
			assert.Equal(t, maxUint64, tally.SPOAbstainStake)
		})
		t.Run("just above overflow fails", func(t *testing.T) {
			tally := &ProposalTally{}
			err := tallySPOVotes(
				&TallyContext{SPOState: newState(2)},
				votes,
				tally,
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "overflows uint64")
		})
	})
}

// TestTallySPOVotesAutoVoteOverflow drives the reward-account auto-vote
// SPOAbstainStake and SPONoStake accumulations in tallySPOVotes to the exact
// uint64 max boundary, via resolved snapshot rows carrying no explicit vote.
func TestTallySPOVotesAutoVoteOverflow(t *testing.T) {
	maxUint64 := ^uint64(0)
	poolA := testBytes(28, 1)
	poolB := testBytes(28, 2)

	newState := func(autoVote uint8, stakeB uint64) *SPOVotingState {
		return &SPOVotingState{
			Dist: []*models.PoolStakeSnapshot{
				{
					PoolKeyHash:                   poolA,
					TotalStake:                    types.Uint64(maxUint64 - 1),
					RewardAccountAutoVote:         autoVote,
					RewardAccountAutoVoteResolved: true,
				},
				{
					PoolKeyHash:                   poolB,
					TotalStake:                    types.Uint64(stakeB),
					RewardAccountAutoVote:         autoVote,
					RewardAccountAutoVoteResolved: true,
				},
			},
		}
	}

	t.Run("abstain auto-vote", func(t *testing.T) {
		t.Run("just below overflow succeeds", func(t *testing.T) {
			tally := &ProposalTally{}
			state := newState(models.PoolRewardAccountAutoVoteAbstain, 1)
			err := tallySPOVotes(&TallyContext{SPOState: state}, nil, tally)
			require.NoError(t, err)
			assert.Equal(t, maxUint64, tally.SPOAbstainStake)
		})
		t.Run("just above overflow fails", func(t *testing.T) {
			tally := &ProposalTally{}
			state := newState(models.PoolRewardAccountAutoVoteAbstain, 2)
			err := tallySPOVotes(&TallyContext{SPOState: state}, nil, tally)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "overflows uint64")
		})
	})

	// A non-NoConfidence action routes the NoConfidence auto-vote to
	// SPONoStake (see tallySPOVotes' RewardAccountAutoVote switch).
	t.Run("no-confidence auto-vote routed to no stake", func(t *testing.T) {
		newTally := func() *ProposalTally {
			return &ProposalTally{
				ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
			}
		}

		t.Run("just below overflow succeeds", func(t *testing.T) {
			state := newState(models.PoolRewardAccountAutoVoteNoConfidence, 1)
			tally := newTally()
			err := tallySPOVotes(&TallyContext{SPOState: state}, nil, tally)
			require.NoError(t, err)
			assert.Equal(t, maxUint64, tally.SPONoStake)
		})
		t.Run("just above overflow fails", func(t *testing.T) {
			state := newState(models.PoolRewardAccountAutoVoteNoConfidence, 2)
			tally := newTally()
			err := tallySPOVotes(&TallyContext{SPOState: state}, nil, tally)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "overflows uint64")
		})
	})

	// A NoConfidence action routes the NoConfidence auto-vote to
	// SPOYesStake instead — a separate checked addition from the branch
	// above.
	t.Run("no-confidence auto-vote routed to yes stake", func(t *testing.T) {
		newTally := func() *ProposalTally {
			return &ProposalTally{
				ActionType: uint8(lcommon.GovActionTypeNoConfidence),
			}
		}

		t.Run("just below overflow succeeds", func(t *testing.T) {
			state := newState(models.PoolRewardAccountAutoVoteNoConfidence, 1)
			tally := newTally()
			err := tallySPOVotes(&TallyContext{SPOState: state}, nil, tally)
			require.NoError(t, err)
			assert.Equal(t, maxUint64, tally.SPOYesStake)
		})
		t.Run("just above overflow fails", func(t *testing.T) {
			state := newState(models.PoolRewardAccountAutoVoteNoConfidence, 2)
			tally := newTally()
			err := tallySPOVotes(&TallyContext{SPOState: state}, nil, tally)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "overflows uint64")
		})
	})
}

// TestLoadSPOVotingStateTotalStakeOverflow drives LoadSPOVotingState's
// total-stake accumulation to the exact uint64 max boundary via two real
// "mark" snapshot rows persisted through the database.
func TestLoadSPOVotingStateTotalStakeOverflow(t *testing.T) {
	maxUint64 := ^uint64(0)

	newDB := func(stakeB uint64) *database.Database {
		db, store := newTallyTestDB(t)
		seedPoolWithStake(
			t, store, testBytes(28, 1), testBytes(28, 2), maxUint64-1, 9,
		)
		seedPoolWithStake(
			t, store, testBytes(28, 3), testBytes(28, 4), stakeB, 9,
		)
		return db
	}

	t.Run("just below overflow succeeds", func(t *testing.T) {
		state, err := LoadSPOVotingState(newDB(1), nil, 9)
		require.NoError(t, err)
		assert.Equal(t, maxUint64, state.TotalStake)
	})

	t.Run("just above overflow fails", func(t *testing.T) {
		_, err := LoadSPOVotingState(newDB(2), nil, 9)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "overflows uint64")
	})
}

func TestTallyProposalRequiresSeatedAuthorizedCommitteeMembers(t *testing.T) {
	db, store := newTallyTestDB(t)
	coldA := testBytes(28, 10)
	hotA := testBytes(28, 11)
	coldB := testBytes(28, 12)
	unseatedCold := testBytes(28, 13)
	unseatedHot := testBytes(28, 14)

	require.NoError(t, store.SetCommitteeMembers([]*models.CommitteeMember{
		{ColdCredHash: coldA, ExpiresEpoch: 20, AddedSlot: 1},
		{ColdCredHash: coldB, ExpiresEpoch: 20, AddedSlot: 1},
	}, nil))
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: coldA,
		HotCredential:  hotA,
		CertificateID:  1,
		AddedSlot:      1,
	})
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: unseatedCold,
		HotCredential:  unseatedHot,
		CertificateID:  2,
		AddedSlot:      1,
	})
	proposal := &models.GovernanceProposal{
		TxHash:        testBytes(32, 15),
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch: 9,
		ExpiresEpoch:  20,
		AnchorHash:    testBytes(32, 16),
		ReturnAddress: testBytes(29, 17),
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      proposal.ID,
		VoterType:       models.VoterTypeCC,
		VoterCredential: hotA,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      proposal.ID,
		VoterType:       models.VoterTypeCC,
		VoterCredential: unseatedHot,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))

	tally, err := TallyProposal(
		&TallyContext{DB: db, CurrentEpoch: 10}, proposal,
	)
	require.NoError(t, err)

	assert.Equal(t, 1, tally.CCTotalCount)
	assert.Equal(t, 1, tally.CCYesCount)
	assert.Equal(t, big.NewRat(1, 1), tally.CCYesRatio())
}

func TestLoadCommitteeVotingStateExcludesSeatedMembersWithoutHotAuth(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)
	coldA := testBytes(28, 21)
	hotA := testBytes(28, 22)
	coldB := testBytes(28, 23)
	unseatedCold := testBytes(28, 24)
	unseatedHot := testBytes(28, 25)

	require.NoError(t, store.SetCommitteeMembers([]*models.CommitteeMember{
		{ColdCredHash: coldA, ExpiresEpoch: 20, AddedSlot: 1},
		{ColdCredHash: coldB, ExpiresEpoch: 20, AddedSlot: 1},
	}, nil))
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: coldA,
		HotCredential:  hotA,
		CertificateID:  1,
		AddedSlot:      1,
	})
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: unseatedCold,
		HotCredential:  unseatedHot,
		CertificateID:  2,
		AddedSlot:      1,
	})

	state, err := LoadCommitteeVotingState(db, nil, 10)
	require.NoError(t, err)

	assert.Equal(t, 1, state.ActiveMemberCount)
	assert.Equal(t, []string{string(hotA)}, state.MemberHotCredentials)
	assert.Contains(t, state.HotCredentialPresence, string(hotA))
	assert.NotContains(t, state.HotCredentialPresence, string(unseatedHot))
}

// TestLoadCommitteeVotingStateExcludesResignedMembers asserts that a
// seated member whose latest resignation postdates their latest hot-key
// authorization is not counted in ActiveMemberCount. Resigned members
// cannot vote, so including them in the denominator per CIP-1694 would
// make them act as implicit No votes.
func TestLoadCommitteeVotingStateExcludesResignedMembers(t *testing.T) {
	db, store := newTallyTestDB(t)
	activeCold := testBytes(28, 30)
	activeHot := testBytes(28, 31)
	resignedCold := testBytes(28, 32)
	resignedHot := testBytes(28, 33)

	require.NoError(t, store.SetCommitteeMembers([]*models.CommitteeMember{
		{ColdCredHash: activeCold, ExpiresEpoch: 20, AddedSlot: 1},
		{ColdCredHash: resignedCold, ExpiresEpoch: 20, AddedSlot: 1},
	}, nil))
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: activeCold,
		HotCredential:  activeHot,
		CertificateID:  1,
		AddedSlot:      1,
	})
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: resignedCold,
		HotCredential:  resignedHot,
		CertificateID:  2,
		AddedSlot:      1,
	})
	seedTallyCommitteeResignation(t, store, models.ResignCommitteeCold{
		ColdCredential: resignedCold,
		CertificateID:  3,
		AddedSlot:      2,
	})

	state, err := LoadCommitteeVotingState(db, nil, 10)
	require.NoError(t, err)

	assert.Equal(t, 1, state.ActiveMemberCount)
	assert.Equal(t, []string{string(activeHot)}, state.MemberHotCredentials)
	assert.NotContains(t, state.HotCredentialPresence, string(resignedHot))
}

// TestTallyCCVotesExcludesResignedFromDenominator asserts that a
// resigned member is not counted in CCTotalCount when tallying votes,
// so the yes-ratio uses only active members as the denominator.
func TestTallyCCVotesExcludesResignedFromDenominator(t *testing.T) {
	db, store := newTallyTestDB(t)
	yesCold := testBytes(28, 40)
	yesHot := testBytes(28, 41)
	resignedCold := testBytes(28, 42)
	resignedHot := testBytes(28, 43)

	require.NoError(t, store.SetCommitteeMembers([]*models.CommitteeMember{
		{ColdCredHash: yesCold, ExpiresEpoch: 20, AddedSlot: 1},
		{ColdCredHash: resignedCold, ExpiresEpoch: 20, AddedSlot: 1},
	}, nil))
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: yesCold,
		HotCredential:  yesHot,
		CertificateID:  1,
		AddedSlot:      1,
	})
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: resignedCold,
		HotCredential:  resignedHot,
		CertificateID:  2,
		AddedSlot:      1,
	})
	seedTallyCommitteeResignation(t, store, models.ResignCommitteeCold{
		ColdCredential: resignedCold,
		CertificateID:  3,
		AddedSlot:      2,
	})

	tally := &ProposalTally{}
	err := tallyCCVotes(
		&TallyContext{DB: db, CurrentEpoch: 10},
		[]*models.GovernanceVote{{
			VoterType:       models.VoterTypeCC,
			VoterCredential: yesHot,
			Vote:            models.VoteYes,
		}},
		tally,
	)
	require.NoError(t, err)

	assert.Equal(t, 1, tally.CCTotalCount)
	assert.Equal(t, 1, tally.CCYesCount)
	assert.Equal(t, big.NewRat(1, 1), tally.CCYesRatio())
}

func TestTallyCCVotesExcludesExpiredCommitteeMembers(t *testing.T) {
	db, store := newTallyTestDB(t)
	cold := testBytes(28, 15)
	hot := testBytes(28, 16)

	require.NoError(t, store.SetCommitteeMembers([]*models.CommitteeMember{
		{ColdCredHash: cold, ExpiresEpoch: 9, AddedSlot: 1},
	}, nil))
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: cold,
		HotCredential:  hot,
		CertificateID:  1,
		AddedSlot:      1,
	})

	tally := &ProposalTally{}
	err := tallyCCVotes(
		&TallyContext{DB: db, CurrentEpoch: 10},
		[]*models.GovernanceVote{{
			VoterType:       models.VoterTypeCC,
			VoterCredential: hot,
			Vote:            models.VoteYes,
		}},
		tally,
	)
	require.NoError(t, err)

	assert.Zero(t, tally.CCTotalCount)
	assert.Zero(t, tally.CCYesCount)
}

func TestTallyProposalCommitteeTermEpochIsInclusive(t *testing.T) {
	testCases := []struct {
		name         string
		currentEpoch uint64
		wantActive   bool
	}{
		{name: "before term epoch", currentEpoch: 9, wantActive: true},
		{name: "at term epoch", currentEpoch: 10, wantActive: true},
		{name: "after term epoch", currentEpoch: 11, wantActive: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db, store := newTallyTestDB(t)
			cold := testBytes(28, 70)
			hot := testBytes(28, 71)

			require.NoError(t, store.SetCommitteeMembers(
				[]*models.CommitteeMember{{
					ColdCredHash: cold,
					ExpiresEpoch: 10,
					AddedSlot:    1,
				}},
				nil,
			))
			seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
				ColdCredential: cold,
				HotCredential:  hot,
				CertificateID:  1,
				AddedSlot:      1,
			})
			proposal := &models.GovernanceProposal{
				TxHash:        testBytes(32, 72),
				ActionType:    uint8(lcommon.GovActionTypeInfo),
				ProposedEpoch: 1,
				ExpiresEpoch:  20,
				AnchorHash:    testBytes(32, 73),
				ReturnAddress: testBytes(29, 74),
				AddedSlot:     1,
			}
			require.NoError(t, db.SetGovernanceProposal(proposal, nil))
			require.NoError(t, db.SetGovernanceVote(
				&models.GovernanceVote{
					ProposalID:      proposal.ID,
					VoterType:       models.VoterTypeCC,
					VoterCredential: hot,
					Vote:            models.VoteYes,
					AddedSlot:       2,
				},
				nil,
			))

			tally, err := TallyProposal(&TallyContext{
				DB:           db,
				CurrentEpoch: testCase.currentEpoch,
			}, proposal)
			require.NoError(t, err)
			if testCase.wantActive {
				assert.Equal(t, 1, tally.CCTotalCount)
				assert.Equal(t, 1, tally.CCYesCount)
				assert.Equal(t, big.NewRat(1, 1), tally.CCYesRatio())
			} else {
				assert.Zero(t, tally.CCTotalCount)
				assert.Zero(t, tally.CCYesCount)
				assert.Zero(t, tally.CCYesRatio().Sign())
			}
		})
	}
}

// TestTallyCCVotesNonVotingMembersAreNotCountedAsNo guards against the
// zero-value collision where models.VoteNo == 0 would silently equal a
// missing map entry. A seated, authorized CC member who has not cast
// any vote must contribute to CCTotalCount but to none of the
// Yes/No/Abstain bucket counts.
func TestTallyCCVotesNonVotingMembersAreNotCountedAsNo(t *testing.T) {
	db, store := newTallyTestDB(t)
	voterCold := testBytes(28, 17)
	voterHot := testBytes(28, 18)
	silentCold := testBytes(28, 19)
	silentHot := testBytes(28, 20)

	require.NoError(t, store.SetCommitteeMembers([]*models.CommitteeMember{
		{ColdCredHash: voterCold, ExpiresEpoch: 20, AddedSlot: 1},
		{ColdCredHash: silentCold, ExpiresEpoch: 20, AddedSlot: 1},
	}, nil))
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: voterCold,
		HotCredential:  voterHot,
		CertificateID:  1,
		AddedSlot:      1,
	})
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: silentCold,
		HotCredential:  silentHot,
		CertificateID:  2,
		AddedSlot:      1,
	})

	tally := &ProposalTally{}
	err := tallyCCVotes(
		&TallyContext{DB: db, CurrentEpoch: 10},
		[]*models.GovernanceVote{
			{
				VoterType:       models.VoterTypeCC,
				VoterCredential: voterHot,
				Vote:            models.VoteYes,
			},
		},
		tally,
	)
	require.NoError(t, err)

	assert.Equal(t, 2, tally.CCTotalCount)
	assert.Equal(t, 1, tally.CCYesCount)
	assert.Equal(
		t,
		0,
		tally.CCNoCount,
		"silent member must not be counted as No",
	)
	assert.Equal(t, 0, tally.CCAbstainCount)
	assert.Equal(t, big.NewRat(1, 2), tally.CCYesRatio())
}

func newTallyTestDB(
	t *testing.T,
) (*database.Database, *tallyTestStore) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	return db, &tallyTestStore{
		MetadataStore: db.Metadata(),
		raw:           raw,
	}
}

type tallyTestStore struct {
	metadata.MetadataStore
	raw *sql.DB
}

func seedTallyCommitteeAuth(
	t *testing.T,
	store *tallyTestStore,
	auth models.AuthCommitteeHot,
) {
	t.Helper()
	_, err := store.raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`,
		auth.ColdCredential,
		auth.HotCredential,
		auth.CertificateID,
		auth.AddedSlot,
	)
	require.NoError(t, err)
}

func seedTallyCommitteeResignation(
	t *testing.T,
	store *tallyTestStore,
	resignation models.ResignCommitteeCold,
) {
	t.Helper()
	_, err := store.raw.Exec(`
INSERT INTO resign_committee_cold (
    cold_credential, certificate_id, added_slot
) VALUES (?, ?, ?)`,
		resignation.ColdCredential,
		resignation.CertificateID,
		resignation.AddedSlot,
	)
	require.NoError(t, err)
}

func seedDRepStake(
	t *testing.T,
	store *tallyTestStore,
	stakeCred []byte,
	drepCred []byte,
	drepType uint64,
	amount uint64,
	id byte,
) {
	t.Helper()
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Drep:       drepCred,
		DrepType:   drepType,
		AddedSlot:  1,
		Reward:     0,
		Active:     true,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       testBytes(32, id),
		OutputIdx:  0,
		StakingKey: stakeCred,
		AddedSlot:  1,
		Amount:     types.Uint64(amount),
	}))
}

func testBytes(length int, seed byte) []byte {
	out := make([]byte, length)
	for i := range out {
		out[i] = seed
	}
	return out
}

// seedPoolWithStake registers a pool with the given reward account
// stake credential and writes a "mark" stake-snapshot row for the
// given epoch so tallySPOVotes finds it. The snapshot's
// RewardAccountAutoVote is left at None — callers populate it via
// resolveSnapshotAutoVotes after seeding any Account delegation.
func seedPoolWithStake(
	t *testing.T,
	store *tallyTestStore,
	poolKeyHash []byte,
	rewardAccount []byte,
	stake uint64,
	epoch uint64,
) {
	t.Helper()
	pool := &models.Pool{
		PoolKeyHash:   poolKeyHash,
		RewardAccount: rewardAccount,
	}
	require.NoError(t, store.ImportPool(pool, &models.PoolRegistration{
		PoolKeyHash:   poolKeyHash,
		RewardAccount: rewardAccount,
		AddedSlot:     0,
	}, nil))
	require.NoError(t, store.SavePoolStakeSnapshot(&models.PoolStakeSnapshot{
		Epoch:        epoch,
		SnapshotType: "mark",
		PoolKeyHash:  poolKeyHash,
		TotalStake:   types.Uint64(stake),
	}, nil))
}

// seedRewardAccountDelegation writes an Account row that pins the
// reward-account stake credential to a specific DRep delegation type.
// Use models.DrepTypeAlwaysAbstain or models.DrepTypeAlwaysNoConfidence
// to exercise the auto-vote paths.
func seedRewardAccountDelegation(
	t *testing.T,
	store *tallyTestStore,
	stakeCred []byte,
	drepCred []byte,
	drepType uint64,
) {
	t.Helper()
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Drep:       drepCred,
		DrepType:   drepType,
		AddedSlot:  1,
		Active:     true,
	}))
}

// resolveSnapshotAutoVotes drives the production snapshot-capture
// pathway in tests: it fetches the "mark" snapshots at the given
// epoch, runs ResolvePoolRewardAccountAutoVotes against live Pool +
// Account state, and writes the resolved auto-vote back. Callers
// invoke this after seeding both pool and reward-account delegation
// so the snapshot row carries the same RewardAccountAutoVote value
// the live rotation/import path would have produced.
func resolveSnapshotAutoVotes(
	t *testing.T,
	db *database.Database,
	epoch uint64,
) {
	t.Helper()
	snapshots, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(
		epoch, "mark", nil,
	)
	require.NoError(t, err)
	require.NoError(t, db.ResolvePoolRewardAccountAutoVotes(snapshots, nil))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	for _, s := range snapshots {
		_, err = raw.Exec(`
UPDATE pool_stake_snapshot
SET reward_account_auto_vote = ?,
    reward_account_auto_vote_resolved = ?
WHERE id = ?`,
			s.RewardAccountAutoVote,
			s.RewardAccountAutoVoteResolved,
			s.ID,
		)
		require.NoError(t, err)
	}
}

// TestTallySPOVotesExplicitVoteWins exercises the original behaviour:
// pools with an explicit vote bypass the reward-account auto-vote
// machinery even when their reward account is delegated to
// AlwaysNoConfidence.
func TestTallySPOVotesExplicitVoteWins(t *testing.T) {
	db, store := newTallyTestDB(t)
	poolKeyHash := testBytes(28, 50)
	rewardAccount := testBytes(28, 51)

	seedPoolWithStake(t, store, poolKeyHash, rewardAccount, 100, 5)
	seedRewardAccountDelegation(
		t, store, rewardAccount, nil, models.DrepTypeAlwaysNoConfidence,
	)
	resolveSnapshotAutoVotes(t, db, 5)

	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	}
	err := tallySPOVotes(
		&TallyContext{DB: db, StakeEpoch: 5},
		[]*models.GovernanceVote{{
			VoterType:       models.VoterTypeSPO,
			VoterCredential: poolKeyHash,
			Vote:            models.VoteYes,
		}},
		tally,
	)
	require.NoError(t, err)

	assert.Equal(t, uint64(100), tally.SPOTotalStake)
	assert.Equal(t, uint64(100), tally.SPOYesStake)
	assert.Equal(t, uint64(0), tally.SPONoStake)
	assert.Equal(t, uint64(0), tally.SPOAbstainStake)
}

// TestTallySPOVotesAlwaysAbstainDelegation asserts that a pool with no
// explicit vote whose reward account delegates to AlwaysAbstain has
// its stake bucketed as abstain (and thus excluded from the SPO yes
// ratio denominator).
func TestTallySPOVotesAlwaysAbstainDelegation(t *testing.T) {
	db, store := newTallyTestDB(t)
	poolKeyHash := testBytes(28, 60)
	rewardAccount := testBytes(28, 61)

	seedPoolWithStake(t, store, poolKeyHash, rewardAccount, 200, 7)
	seedRewardAccountDelegation(
		t, store, rewardAccount, nil, models.DrepTypeAlwaysAbstain,
	)
	resolveSnapshotAutoVotes(t, db, 7)

	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	}
	err := tallySPOVotes(
		&TallyContext{DB: db, StakeEpoch: 7},
		nil,
		tally,
	)
	require.NoError(t, err)

	assert.Equal(t, uint64(200), tally.SPOTotalStake)
	assert.Equal(t, uint64(200), tally.SPOAbstainStake)
	assert.Equal(t, uint64(0), tally.SPOYesStake)
	assert.Equal(t, uint64(0), tally.SPONoStake)
	// 0 / (total - abstain) = 0 / 0 ⇒ ratioOf returns the zero rat.
	assert.Equal(t, 0, tally.SPOYesRatio().Sign())
}

func TestResolvePoolRewardAccountAutoVotesIsCredentialTagAware(t *testing.T) {
	db, store := newTallyTestDB(t)
	poolKeyHash := testBytes(28, 62)
	rewardAccount := testBytes(28, 63)

	pool := &models.Pool{
		PoolKeyHash:                poolKeyHash,
		RewardAccount:              rewardAccount,
		RewardAccountCredentialTag: 1,
	}
	require.NoError(t, store.ImportPool(pool, &models.PoolRegistration{
		PoolKeyHash:                poolKeyHash,
		RewardAccount:              rewardAccount,
		RewardAccountCredentialTag: 1,
		AddedSlot:                  0,
	}, nil))
	require.NoError(t, store.SavePoolStakeSnapshot(&models.PoolStakeSnapshot{
		Epoch:        8,
		SnapshotType: "mark",
		PoolKeyHash:  poolKeyHash,
		TotalStake:   types.Uint64(250),
	}, nil))
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		CredentialTag: 0,
		StakingKey:    rewardAccount,
		DrepType:      models.DrepTypeAlwaysNoConfidence,
		AddedSlot:     1,
		Active:        true,
	}))
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		CredentialTag: 1,
		StakingKey:    rewardAccount,
		DrepType:      models.DrepTypeAlwaysAbstain,
		AddedSlot:     1,
		Active:        true,
	}))

	resolveSnapshotAutoVotes(t, db, 8)

	snapshot, err := store.GetPoolStakeSnapshot(
		8, "mark", poolKeyHash, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.True(t, snapshot.RewardAccountAutoVoteResolved)
	assert.Equal(
		t,
		models.PoolRewardAccountAutoVoteAbstain,
		snapshot.RewardAccountAutoVote,
	)
}

// TestTallySPOVotesAlwaysNoConfidenceFlipsByActionType asserts that
// AlwaysNoConfidence reward-account delegation produces an auto-Yes on
// NoConfidence actions and an auto-No on non-NoConfidence actions,
// mirroring the AlwaysNoConfidence DRep handling.
func TestTallySPOVotesAlwaysNoConfidenceFlipsByActionType(t *testing.T) {
	noConfidencePoolKey := testBytes(28, 70)
	noConfidenceRewardAcct := testBytes(28, 71)

	cases := []struct {
		name               string
		actionType         lcommon.GovActionType
		expectYesStake     uint64
		expectNoStake      uint64
		expectAbstainStake uint64
	}{
		{
			name:               "NoConfidence action → auto Yes",
			actionType:         lcommon.GovActionTypeNoConfidence,
			expectYesStake:     300,
			expectNoStake:      0,
			expectAbstainStake: 0,
		},
		{
			name:               "TreasuryWithdrawal action → auto No",
			actionType:         lcommon.GovActionTypeTreasuryWithdrawal,
			expectYesStake:     0,
			expectNoStake:      300,
			expectAbstainStake: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, store := newTallyTestDB(t)
			seedPoolWithStake(
				t, store, noConfidencePoolKey, noConfidenceRewardAcct,
				300, 11,
			)
			seedRewardAccountDelegation(
				t, store, noConfidenceRewardAcct, nil,
				models.DrepTypeAlwaysNoConfidence,
			)
			resolveSnapshotAutoVotes(t, db, 11)

			tally := &ProposalTally{ActionType: uint8(tc.actionType)}
			err := tallySPOVotes(
				&TallyContext{DB: db, StakeEpoch: 11},
				nil,
				tally,
			)
			require.NoError(t, err)
			assert.Equal(t, uint64(300), tally.SPOTotalStake)
			assert.Equal(t, tc.expectYesStake, tally.SPOYesStake)
			assert.Equal(t, tc.expectNoStake, tally.SPONoStake)
			assert.Equal(t, tc.expectAbstainStake, tally.SPOAbstainStake)
		})
	}
}

// TestTallySPOVotesOrdinaryDRepNoAutoVote asserts that pools with a
// reward account delegated to an ordinary credential-backed DRep do
// NOT auto-vote: their stake stays in SPOTotalStake (implicit no) and
// is not added to any bucket.
func TestTallySPOVotesOrdinaryDRepNoAutoVote(t *testing.T) {
	db, store := newTallyTestDB(t)
	poolKeyHash := testBytes(28, 80)
	rewardAccount := testBytes(28, 81)
	regularDRep := testBytes(28, 82)

	seedPoolWithStake(t, store, poolKeyHash, rewardAccount, 150, 4)
	seedRewardAccountDelegation(
		t, store, rewardAccount, regularDRep, models.DrepTypeAddrKeyHash,
	)
	resolveSnapshotAutoVotes(t, db, 4)

	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	}
	err := tallySPOVotes(
		&TallyContext{DB: db, StakeEpoch: 4},
		nil,
		tally,
	)
	require.NoError(t, err)

	assert.Equal(t, uint64(150), tally.SPOTotalStake)
	assert.Equal(t, uint64(0), tally.SPOYesStake)
	assert.Equal(t, uint64(0), tally.SPONoStake)
	assert.Equal(t, uint64(0), tally.SPOAbstainStake)
}

// TestTallySPOVotesNoRewardAccountDelegation asserts the no-delegation
// case: a pool whose reward account exists but has no DRep set, and a
// pool whose reward account is not registered at all, both contribute
// only to SPOTotalStake (implicit no).
func TestTallySPOVotesNoRewardAccountDelegation(t *testing.T) {
	db, store := newTallyTestDB(t)
	registeredPool := testBytes(28, 90)
	registeredRewardAcct := testBytes(28, 91)
	unregisteredPool := testBytes(28, 92)
	unregisteredRewardAcct := testBytes(28, 93)

	seedPoolWithStake(
		t, store, registeredPool, registeredRewardAcct, 50, 8,
	)
	seedPoolWithStake(
		t, store, unregisteredPool, unregisteredRewardAcct, 70, 8,
	)
	// Only the first pool's reward account is registered. The Account
	// row has no DRep delegation set (zero value DrepType + nil Drep).
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: registeredRewardAcct,
		AddedSlot:  1,
		Active:     true,
	}))
	resolveSnapshotAutoVotes(t, db, 8)

	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	}
	err := tallySPOVotes(
		&TallyContext{DB: db, StakeEpoch: 8},
		nil,
		tally,
	)
	require.NoError(t, err)

	assert.Equal(t, uint64(120), tally.SPOTotalStake)
	assert.Equal(t, uint64(0), tally.SPOYesStake)
	assert.Equal(t, uint64(0), tally.SPONoStake)
	assert.Equal(t, uint64(0), tally.SPOAbstainStake)
}

// TestTallySPOVotesDeregisteredRewardAccountDoesNotAutoVote asserts
// that a pool whose reward-account stake credential is deregistered
// (Account.Active == false) but still carries a stale
// AlwaysAbstain/AlwaysNoConfidence flag must NOT auto-vote — its
// stake falls back to implicit no, contributing only to
// SPOTotalStake. Protects against the active-filter regression flagged
// in code review.
func TestTallySPOVotesDeregisteredRewardAccountDoesNotAutoVote(t *testing.T) {
	db, store := newTallyTestDB(t)
	abstainPool := testBytes(28, 110)
	abstainAcct := testBytes(28, 111)
	noConfidencePool := testBytes(28, 112)
	noConfidenceAcct := testBytes(28, 113)

	seedPoolWithStake(t, store, abstainPool, abstainAcct, 100, 12)
	seedPoolWithStake(
		t, store, noConfidencePool, noConfidenceAcct, 200, 12,
	)
	// Both reward accounts carry a predefined-DRep flag but are inactive
	// (deregistered).
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: abstainAcct,
		DrepType:   models.DrepTypeAlwaysAbstain,
		AddedSlot:  1,
		Active:     false,
	}))
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: noConfidenceAcct,
		DrepType:   models.DrepTypeAlwaysNoConfidence,
		AddedSlot:  1,
		Active:     false,
	}))
	resolveSnapshotAutoVotes(t, db, 12)

	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeNoConfidence),
	}
	err := tallySPOVotes(
		&TallyContext{DB: db, StakeEpoch: 12},
		nil,
		tally,
	)
	require.NoError(t, err)

	assert.Equal(t, uint64(300), tally.SPOTotalStake)
	assert.Equal(
		t,
		uint64(0),
		tally.SPOYesStake,
		"deregistered AlwaysNoConfidence delegation must not auto-Yes on NoConfidence action",
	)
	assert.Equal(t, uint64(0), tally.SPONoStake)
	assert.Equal(t, uint64(0), tally.SPOAbstainStake,
		"deregistered AlwaysAbstain delegation must not auto-abstain")
}

// TestTallySPOVotesMixedExplicitAndAutoVotes is the end-to-end mix:
// one pool votes Yes explicitly, one delegates AlwaysAbstain, one
// delegates AlwaysNoConfidence (auto-No on a non-NoConfidence action),
// and one has no auto-vote at all (implicit no). The buckets must add
// up so SPOYesRatio reflects only the explicit Yes against the
// active-stake denominator.
func TestTallySPOVotesMixedExplicitAndAutoVotes(t *testing.T) {
	db, store := newTallyTestDB(t)

	explicitYesPool := testBytes(28, 100)
	explicitYesAcct := testBytes(28, 101)
	abstainPool := testBytes(28, 102)
	abstainAcct := testBytes(28, 103)
	noConfidencePool := testBytes(28, 104)
	noConfidenceAcct := testBytes(28, 105)
	silentPool := testBytes(28, 106)
	silentAcct := testBytes(28, 107)

	seedPoolWithStake(t, store, explicitYesPool, explicitYesAcct, 100, 9)
	seedPoolWithStake(t, store, abstainPool, abstainAcct, 200, 9)
	seedPoolWithStake(t, store, noConfidencePool, noConfidenceAcct, 50, 9)
	seedPoolWithStake(t, store, silentPool, silentAcct, 25, 9)

	seedRewardAccountDelegation(
		t, store, abstainAcct, nil, models.DrepTypeAlwaysAbstain,
	)
	seedRewardAccountDelegation(
		t, store, noConfidenceAcct, nil,
		models.DrepTypeAlwaysNoConfidence,
	)
	// silentAcct: no Account row ⇒ no auto-vote.
	resolveSnapshotAutoVotes(t, db, 9)

	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	}
	err := tallySPOVotes(
		&TallyContext{DB: db, StakeEpoch: 9},
		[]*models.GovernanceVote{{
			VoterType:       models.VoterTypeSPO,
			VoterCredential: explicitYesPool,
			Vote:            models.VoteYes,
		}},
		tally,
	)
	require.NoError(t, err)

	assert.Equal(t, uint64(375), tally.SPOTotalStake)
	assert.Equal(t, uint64(100), tally.SPOYesStake)
	assert.Equal(t, uint64(50), tally.SPONoStake)
	assert.Equal(t, uint64(200), tally.SPOAbstainStake)
	// yes / (total - abstain) = 100 / (375 - 200) = 100 / 175 = 4/7
	assert.Equal(t, big.NewRat(4, 7), tally.SPOYesRatio())
}

// TestTallySPOVotesUnresolvedSnapshotRowFallsBackToImplicitNo asserts
// that a snapshot row with RewardAccountAutoVoteResolved=false is
// treated as PoolRewardAccountAutoVoteNone regardless of what
// RewardAccountAutoVote happens to contain — covering the Mithril
// set/go import path that intentionally skips resolution, plus any
// pre-CIP-1694 row left over from an upgrade. Without this guard, a
// stale or never-resolved Abstain/NoConfidence value would silently
// flip the tally.
func TestTallySPOVotesUnresolvedSnapshotRowFallsBackToImplicitNo(t *testing.T) {
	db, store := newTallyTestDB(t)
	poolKeyHash := testBytes(28, 130)
	rewardAccount := testBytes(28, 131)

	// Write a snapshot row directly with RewardAccountAutoVote set to
	// Abstain but Resolved=false. This mimics either a Mithril-imported
	// set/go row that the import path declined to resolve, or a row
	// from a pre-flag schema where the column happens to be non-zero.
	pool := &models.Pool{
		PoolKeyHash:   poolKeyHash,
		RewardAccount: rewardAccount,
	}
	require.NoError(t, store.ImportPool(pool, &models.PoolRegistration{
		PoolKeyHash:   poolKeyHash,
		RewardAccount: rewardAccount,
		AddedSlot:     0,
	}, nil))
	require.NoError(t, store.SavePoolStakeSnapshot(&models.PoolStakeSnapshot{
		Epoch:                 15,
		SnapshotType:          "mark",
		PoolKeyHash:           poolKeyHash,
		TotalStake:            types.Uint64(500),
		RewardAccountAutoVote: models.PoolRewardAccountAutoVoteAbstain,
		// RewardAccountAutoVoteResolved intentionally zero-value (false).
	}, nil))

	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	}
	err := tallySPOVotes(
		&TallyContext{DB: db, StakeEpoch: 15},
		nil,
		tally,
	)
	require.NoError(t, err)

	assert.Equal(t, uint64(500), tally.SPOTotalStake)
	assert.Equal(
		t,
		uint64(0),
		tally.SPOAbstainStake,
		"unresolved row must not bucket stake into Abstain even when the column says Abstain",
	)
	assert.Equal(t, uint64(0), tally.SPOYesStake)
	assert.Equal(t, uint64(0), tally.SPONoStake)
}

// TestTallySPOVotesSnapshotIsFrozenAgainstLiveStateChanges proves the
// snapshot-correctness property that motivated the schema change:
// once a pool's RewardAccountAutoVote is captured on the snapshot row,
// later changes to the pool's reward account, the reward-account
// holder's DRep delegation, or the account's active flag must NOT
// shift the tally for that epoch. Mirrors the cardano-ledger
// ssDelegations/ssDReps semantics flagged in the PR review.
func TestTallySPOVotesSnapshotIsFrozenAgainstLiveStateChanges(t *testing.T) {
	db, store := newTallyTestDB(t)
	poolKeyHash := testBytes(28, 120)
	originalRewardAcct := testBytes(28, 121)
	rotatedRewardAcct := testBytes(28, 122)
	regularDRep := testBytes(28, 123)

	// Snapshot-era state: pool reward account delegated to
	// AlwaysAbstain. Resolver captures Abstain on the snapshot.
	seedPoolWithStake(
		t, store, poolKeyHash, originalRewardAcct, 400, 13,
	)
	seedRewardAccountDelegation(
		t, store, originalRewardAcct, nil, models.DrepTypeAlwaysAbstain,
	)
	resolveSnapshotAutoVotes(t, db, 13)

	// Post-snapshot mutations: re-delegate the original credential to
	// a regular DRep AND rotate the pool's reward account to a brand
	// new credential. Under live-state lookup this would zero out the
	// abstain bucket; the snapshot-frozen tally must ignore both.
	_, err := store.raw.Exec(`
UPDATE account SET drep = ?, drep_type = ? WHERE staking_key = ?`,
		regularDRep,
		models.DrepTypeAddrKeyHash,
		originalRewardAcct,
	)
	require.NoError(t, err)
	_, err = store.raw.Exec(
		"UPDATE pool SET reward_account = ? WHERE pool_key_hash = ?",
		rotatedRewardAcct,
		poolKeyHash,
	)
	require.NoError(t, err)

	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	}
	err = tallySPOVotes(
		&TallyContext{DB: db, StakeEpoch: 13},
		nil,
		tally,
	)
	require.NoError(t, err)

	assert.Equal(t, uint64(400), tally.SPOTotalStake)
	assert.Equal(
		t,
		uint64(400),
		tally.SPOAbstainStake,
		"snapshot-era AlwaysAbstain must survive a post-snapshot redelegation + reward-account rotation",
	)
	assert.Equal(t, uint64(0), tally.SPOYesStake)
	assert.Equal(t, uint64(0), tally.SPONoStake)
}
