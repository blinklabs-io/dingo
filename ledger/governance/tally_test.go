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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	sqliteplugin "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTallyDRepVotesIncludesAlwaysAbstain(t *testing.T) {
	db, store := newTallyTestDB(t)
	drepCred := testBytes(28, 1)
	stakeCred := testBytes(28, 2)
	abstainStakeCred := testBytes(28, 3)

	require.NoError(t, store.DB().Create(&models.Drep{
		Credential: drepCred,
		Active:     true,
	}).Error)
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
		Tally:           updateCommitteeTally,
		PParams:         pparams,
		ActiveDRepCount: 0,
		MajorVersion:    10,
	})
	assert.False(t, updateCommitteeDecision.DRepApproved)
}

func TestTallyCCVotesRequiresSeatedAuthorizedCommitteeMembers(t *testing.T) {
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
	require.NoError(t, store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: coldA,
		HotCredential:  hotA,
		CertificateID:  1,
		AddedSlot:      1,
	}).Error)
	require.NoError(t, store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: unseatedCold,
		HotCredential:  unseatedHot,
		CertificateID:  2,
		AddedSlot:      1,
	}).Error)

	tally := &ProposalTally{}
	err := tallyCCVotes(
		&TallyContext{DB: db, CurrentEpoch: 10},
		[]*models.GovernanceVote{
			{
				VoterType:       models.VoterTypeCC,
				VoterCredential: hotA,
				Vote:            models.VoteYes,
			},
			{
				VoterType:       models.VoterTypeCC,
				VoterCredential: unseatedHot,
				Vote:            models.VoteYes,
			},
		},
		tally,
	)
	require.NoError(t, err)

	assert.Equal(t, 2, tally.CCTotalCount)
	assert.Equal(t, 1, tally.CCYesCount)
	assert.Equal(t, big.NewRat(1, 2), tally.CCYesRatio())
}

func TestLoadCommitteeVotingStateCountsSeatedMembersWithoutHotAuth(t *testing.T) {
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
	require.NoError(t, store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: coldA,
		HotCredential:  hotA,
		CertificateID:  1,
		AddedSlot:      1,
	}).Error)
	require.NoError(t, store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: unseatedCold,
		HotCredential:  unseatedHot,
		CertificateID:  2,
		AddedSlot:      1,
	}).Error)

	state, err := LoadCommitteeVotingState(db, nil, 10)
	require.NoError(t, err)

	assert.Equal(t, 2, state.ActiveMemberCount)
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
	require.NoError(t, store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: activeCold,
		HotCredential:  activeHot,
		CertificateID:  1,
		AddedSlot:      1,
	}).Error)
	require.NoError(t, store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: resignedCold,
		HotCredential:  resignedHot,
		CertificateID:  2,
		AddedSlot:      1,
	}).Error)
	require.NoError(t, store.DB().Create(&models.ResignCommitteeCold{
		ColdCredential: resignedCold,
		CertificateID:  3,
		AddedSlot:      2,
	}).Error)

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
	require.NoError(t, store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: yesCold,
		HotCredential:  yesHot,
		CertificateID:  1,
		AddedSlot:      1,
	}).Error)
	require.NoError(t, store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: resignedCold,
		HotCredential:  resignedHot,
		CertificateID:  2,
		AddedSlot:      1,
	}).Error)
	require.NoError(t, store.DB().Create(&models.ResignCommitteeCold{
		ColdCredential: resignedCold,
		CertificateID:  3,
		AddedSlot:      2,
	}).Error)

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
	require.NoError(t, store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: cold,
		HotCredential:  hot,
		CertificateID:  1,
		AddedSlot:      1,
	}).Error)

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
	require.NoError(t, store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: voterCold,
		HotCredential:  voterHot,
		CertificateID:  1,
		AddedSlot:      1,
	}).Error)
	require.NoError(t, store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: silentCold,
		HotCredential:  silentHot,
		CertificateID:  2,
		AddedSlot:      1,
	}).Error)

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
	assert.Equal(t, 0, tally.CCNoCount, "silent member must not be counted as No")
	assert.Equal(t, 0, tally.CCAbstainCount)
	assert.Equal(t, big.NewRat(1, 2), tally.CCYesRatio())
}

func newTallyTestDB(
	t *testing.T,
) (*database.Database, *sqliteplugin.MetadataStoreSqlite) {
	t.Helper()
	db, err := database.New(&database.Config{
		DataDir:        "",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok)
	return db, store
}

func seedDRepStake(
	t *testing.T,
	store *sqliteplugin.MetadataStoreSqlite,
	stakeCred []byte,
	drepCred []byte,
	drepType uint64,
	amount uint64,
	id byte,
) {
	t.Helper()
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeCred,
		Drep:       drepCred,
		DrepType:   drepType,
		AddedSlot:  1,
		Reward:     0,
		Active:     true,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       testBytes(32, id),
		OutputIdx:  0,
		StakingKey: stakeCred,
		AddedSlot:  1,
		Amount:     types.Uint64(amount),
	}).Error)
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
// given epoch so tallySPOVotes finds it.
func seedPoolWithStake(
	t *testing.T,
	store *sqliteplugin.MetadataStoreSqlite,
	poolKeyHash []byte,
	rewardAccount []byte,
	stake uint64,
	epoch uint64,
) {
	t.Helper()
	require.NoError(t, store.DB().Create(&models.Pool{
		PoolKeyHash:   poolKeyHash,
		RewardAccount: rewardAccount,
	}).Error)
	require.NoError(t, store.DB().Create(&models.PoolStakeSnapshot{
		Epoch:        epoch,
		SnapshotType: "mark",
		PoolKeyHash:  poolKeyHash,
		TotalStake:   types.Uint64(stake),
	}).Error)
}

// seedRewardAccountDelegation writes an Account row that pins the
// reward-account stake credential to a specific DRep delegation type.
// Use models.DrepTypeAlwaysAbstain or models.DrepTypeAlwaysNoConfidence
// to exercise the auto-vote paths.
func seedRewardAccountDelegation(
	t *testing.T,
	store *sqliteplugin.MetadataStoreSqlite,
	stakeCred []byte,
	drepCred []byte,
	drepType uint64,
) {
	t.Helper()
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeCred,
		Drep:       drepCred,
		DrepType:   drepType,
		AddedSlot:  1,
		Active:     true,
	}).Error)
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

// TestTallySPOVotesAlwaysNoConfidenceFlipsByActionType asserts that
// AlwaysNoConfidence reward-account delegation produces an auto-Yes on
// NoConfidence actions and an auto-No on non-NoConfidence actions,
// mirroring the AlwaysNoConfidence DRep handling.
func TestTallySPOVotesAlwaysNoConfidenceFlipsByActionType(t *testing.T) {
	noConfidencePoolKey := testBytes(28, 70)
	noConfidenceRewardAcct := testBytes(28, 71)

	cases := []struct {
		name              string
		actionType        lcommon.GovActionType
		expectYesStake    uint64
		expectNoStake     uint64
		expectAbstainStake uint64
	}{
		{
			name:              "NoConfidence action → auto Yes",
			actionType:        lcommon.GovActionTypeNoConfidence,
			expectYesStake:    300,
			expectNoStake:     0,
			expectAbstainStake: 0,
		},
		{
			name:              "TreasuryWithdrawal action → auto No",
			actionType:        lcommon.GovActionTypeTreasuryWithdrawal,
			expectYesStake:    0,
			expectNoStake:     300,
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
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: registeredRewardAcct,
		AddedSlot:  1,
		Active:     true,
	}).Error)

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
