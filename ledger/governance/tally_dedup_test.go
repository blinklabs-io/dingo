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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoadDRepVotingStateMatchesLazyTally asserts that tallying a
// proposal against a precomputed DRepVotingState produces results
// identical to the lazy path that loads the state inside
// tallyDRepVotes. The precomputed path is what ProcessEpoch uses to
// avoid recomputing the heavy account/utxo voting-power query once per
// proposal; it must be consensus-identical to the per-call path.
func TestLoadDRepVotingStateMatchesLazyTally(t *testing.T) {
	db, store := newTallyTestDB(t)
	drepCred := testBytes(28, 1)
	stakeCred := testBytes(28, 2)
	abstainStakeCred := testBytes(28, 3)

	require.NoError(t, store.DB().Create(&models.Drep{
		Credential: drepCred,
		Active:     true,
	}).Error)
	seedDRepStake(
		t, store, stakeCred, drepCred, models.DrepTypeAddrKeyHash, 60, 1,
	)
	seedDRepStake(
		t, store, abstainStakeCred, nil, models.DrepTypeAlwaysAbstain, 40, 2,
	)

	votes := []*models.GovernanceVote{{
		VoterType:       models.VoterTypeDRep,
		VoterCredential: drepCred,
		Vote:            models.VoteYes,
	}}

	lazyTally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	}
	require.NoError(t, tallyDRepVotes(&TallyContext{DB: db}, votes, lazyTally))

	state, err := LoadDRepVotingState(db, nil, 0)
	require.NoError(t, err)
	precomputedTally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	}
	require.NoError(t, tallyDRepVotes(
		&TallyContext{DB: db, DRepState: state},
		votes,
		precomputedTally,
	))

	assert.Equal(t, *lazyTally, *precomputedTally)
	assert.Equal(t, uint64(100), precomputedTally.DRepTotalStake)
	assert.Equal(t, uint64(60), precomputedTally.DRepYesStake)
	assert.Equal(t, uint64(40), precomputedTally.DRepAbstainStake)
}

// TestPrecomputedDRepStateReusedAcrossProposals asserts that a single
// DRepVotingState can be reused to tally multiple proposals with
// different action types and still apply the action-type-dependent
// AlwaysNoConfidence bucketing correctly. This is the reuse pattern
// ProcessEpoch relies on: load once, tally every proposal.
func TestPrecomputedDRepStateReusedAcrossProposals(t *testing.T) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 4)
	seedDRepStake(
		t, store, stakeCred, nil, models.DrepTypeAlwaysNoConfidence, 30, 3,
	)

	state, err := LoadDRepVotingState(db, nil, 0)
	require.NoError(t, err)

	noConfidence := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeNoConfidence),
	}
	require.NoError(t, tallyDRepVotes(
		&TallyContext{DB: db, DRepState: state}, nil, noConfidence,
	))
	assert.Equal(t, uint64(30), noConfidence.DRepTotalStake)
	assert.Equal(t, uint64(30), noConfidence.DRepYesStake)
	assert.Equal(t, uint64(0), noConfidence.DRepNoStake)

	updateCommittee := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeUpdateCommittee),
	}
	require.NoError(t, tallyDRepVotes(
		&TallyContext{DB: db, DRepState: state}, nil, updateCommittee,
	))
	assert.Equal(t, uint64(30), updateCommittee.DRepTotalStake)
	assert.Equal(t, uint64(0), updateCommittee.DRepYesStake)
	assert.Equal(t, uint64(30), updateCommittee.DRepNoStake)
}

// TestLoadSPOVotingStateMatchesLazyTally asserts the SPO voting-power
// snapshot precompute is consensus-identical to the lazy per-call path.
func TestLoadSPOVotingStateMatchesLazyTally(t *testing.T) {
	db, store := newTallyTestDB(t)
	poolKeyHash := testBytes(28, 50)
	rewardAccount := testBytes(28, 51)

	seedPoolWithStake(t, store, poolKeyHash, rewardAccount, 100, 5)
	seedRewardAccountDelegation(
		t, store, rewardAccount, nil, models.DrepTypeAlwaysNoConfidence,
	)
	resolveSnapshotAutoVotes(t, db, 5)

	votes := []*models.GovernanceVote{{
		VoterType:       models.VoterTypeSPO,
		VoterCredential: poolKeyHash,
		Vote:            models.VoteYes,
	}}

	lazyTally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	}
	require.NoError(t, tallySPOVotes(
		&TallyContext{DB: db, StakeEpoch: 5}, votes, lazyTally,
	))

	state, err := LoadSPOVotingState(db, nil, 5)
	require.NoError(t, err)
	precomputedTally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	}
	require.NoError(t, tallySPOVotes(
		&TallyContext{DB: db, StakeEpoch: 5, SPOState: state},
		votes,
		precomputedTally,
	))

	assert.Equal(t, *lazyTally, *precomputedTally)
	assert.Equal(t, uint64(100), precomputedTally.SPOTotalStake)
	assert.Equal(t, uint64(100), precomputedTally.SPOYesStake)
}
