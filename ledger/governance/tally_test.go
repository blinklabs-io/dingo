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
