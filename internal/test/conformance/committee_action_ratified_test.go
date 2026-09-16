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

package conformance

import (
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// unreachableThreshold and trivialThreshold isolate one side of a
// committeeActionRatified decision at a time: an unreachable committee
// threshold proves a pass could only have come from the MotionNoConfidence
// path, and a trivial (zero) pool threshold auto-approves the SPO side so a
// test can exercise the DRep side alone.
var (
	unreachableThreshold = cbor.Rat{Rat: big.NewRat(999999, 1000000)}
	trivialThreshold     = cbor.Rat{Rat: big.NewRat(0, 1)}
)

// noConfidenceCommitteeParams builds Conway protocol parameters with an
// easy-to-clear MotionNoConfidence DRep threshold and a CommitteeNormal/
// CommitteeNoConfidence DRep threshold no real stake distribution could
// ever clear, so a test can tell which threshold committeeActionRatified
// actually applied from the pass/fail outcome alone. Pool thresholds are
// all trivial so every test here isolates the DRep side.
func noConfidenceCommitteeParams() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 10},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNormal:       unreachableThreshold,
			CommitteeNoConfidence: unreachableThreshold,
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    trivialThreshold,
			CommitteeNormal:       trivialThreshold,
			CommitteeNoConfidence: trivialThreshold,
		},
	}
}

// TestCommitteeActionRatifiedNoConfidenceUsesMotionThresholdAndImplicitYes
// proves both halves of the fix requested by CodeRabbit and wolf31o2 on PR
// #4333 in one assertion: a NoConfidence proposal whose only backing is a
// DRep delegated AlwaysNoConfidence (no proposal carries an explicit vote at
// all) ratifies only if committeeActionRatified (a) judges NoConfidence
// against DRepVotingThresholds.MotionNoConfidence rather than
// CommitteeNormal/CommitteeNoConfidence, and (b) counts that
// AlwaysNoConfidence-delegated stake as a yes vote, not just a denominator
// contribution. Reverting either half of the fix flips this to false: this
// test fails against the pre-fix (fad9390e-era) code, which shared the
// unreachable committee threshold and denominator-only accounting between
// both action types.
func TestCommitteeActionRatifiedNoConfidenceUsesMotionThresholdAndImplicitYes(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	m.protocolParams = noConfidenceCommitteeParams()

	credential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xd1),
	}
	m.govState.DRepDelegationsByCredential[credential] = common.Drep{
		Type: common.DrepTypeNoConfidence,
	}
	m.govState.RewardAccountBalances[credential] = 1_000_000

	proposal := &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType: common.GovActionTypeNoConfidence,
			Votes:      map[string]uint8{},
		},
	}

	txn := m.db.Transaction(false)
	defer txn.Release()

	ratified, err := m.committeeActionRatified(txn, proposal, 5)
	require.NoError(t, err)
	require.True(
		t,
		ratified,
		"NoConfidence must ratify off MotionNoConfidence plus the "+
			"AlwaysNoConfidence implicit yes vote",
	)
}

// TestCommitteeActionRatifiedUpdateCommitteeKeepsNoConfidenceDenominatorOnly
// is the companion negative case: the identical AlwaysNoConfidence
// delegation and stake, but for an UpdateCommittee proposal, must NOT
// ratify. cardano-ledger only grants AlwaysNoConfidence an automatic yes on
// an actual NoConfidence action; on UpdateCommittee that delegated stake
// belongs in the denominator alone, so with no other voters the yes ratio
// is 0 and a reachable (1/2) CommitteeNoConfidence threshold is not met.
// This guards against a fix that stops discriminating the two action types
// in the other direction (treating every action as if it were
// NoConfidence).
func TestCommitteeActionRatifiedUpdateCommitteeKeepsNoConfidenceDenominatorOnly(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	m.protocolParams = &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 10},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    trivialThreshold,
			CommitteeNormal:       cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNoConfidence: cbor.Rat{Rat: big.NewRat(1, 2)},
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    trivialThreshold,
			CommitteeNormal:       trivialThreshold,
			CommitteeNoConfidence: trivialThreshold,
		},
	}

	credential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xd2),
	}
	m.govState.DRepDelegationsByCredential[credential] = common.Drep{
		Type: common.DrepTypeNoConfidence,
	}
	m.govState.RewardAccountBalances[credential] = 1_000_000

	proposal := &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType: common.GovActionTypeUpdateCommittee,
			Votes:      map[string]uint8{},
		},
	}

	txn := m.db.Transaction(false)
	defer txn.Release()

	ratified, err := m.committeeActionRatified(txn, proposal, 5)
	require.NoError(t, err)
	require.False(
		t,
		ratified,
		"UpdateCommittee must not grant AlwaysNoConfidence delegation an "+
			"implicit yes vote",
	)
}

// TestProcessEpochBoundaryRatifiesUpdateCommitteeWithoutCommitteeVote closes
// the gap a PR review found in the two tests above: both call
// committeeActionRatified directly, so neither one proves ratifyProposals
// actually routes UpdateCommittee/NoConfidence proposals to it. Reverting
// just that routing (back to the pre-#4007 hasCC-requiring heuristic) while
// keeping committeeActionRatified and both direct-call tests left the whole
// package green, including those two tests -- nothing exercised the
// decision of *which* ratification path a real proposal takes.
//
// This test drives the real entry point, ProcessEpochBoundary, the way the
// harness calls it for every vector: a DRep and an SPO each cast an
// explicit yes vote (the exact shape issue #4007's "CC re-election" vector
// carries) and no committee vote is ever recorded. It only ratifies if
// ProcessEpochBoundary's call into ratifyProposals actually reaches
// committeeActionRatified for this action type; the old heuristic requires
// a committee yes-vote that never exists here, so this proposal stays
// stuck pending under it.
func TestProcessEpochBoundaryRatifiesUpdateCommitteeWithoutCommitteeVote(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	reachable := cbor.Rat{Rat: big.NewRat(1, 2)}
	m.protocolParams = &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 10},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    reachable,
			CommitteeNormal:       reachable,
			CommitteeNoConfidence: reachable,
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    reachable,
			CommitteeNormal:       reachable,
			CommitteeNoConfidence: reachable,
		},
	}

	// One DRep, backed by real delegated stake, votes yes.
	drepCredentialHash := testHash28(0xe1)
	drepStakeCredential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xe2),
	}
	m.govState.DRepDelegationsByCredential[drepStakeCredential] = common.Drep{
		Type:       common.DrepTypeAddrKeyHash,
		Credential: drepCredentialHash[:],
	}
	m.govState.DRepRegistrationsByCredential[mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: drepCredentialHash,
	}] = true
	m.govState.RewardAccountBalances[drepStakeCredential] = 1_000_000

	// One pool, backed by real delegated stake, votes yes.
	poolHash := testHash28(0xe3)
	poolStakeCredential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xe4),
	}
	m.govState.PoolRegistrations[poolHash] = true
	m.govState.PoolDelegationsByCredential[poolStakeCredential] = poolHash
	m.govState.RewardAccountBalances[poolStakeCredential] = 1_000_000

	// Vote keys match the real format committeeActionRatified reads:
	// "<voter type digit>:<hex credential hash>" (see recordVotesInGovState).
	votes := map[string]uint8{
		formatVoteKey(common.VoterTypeDRepKeyHash, drepCredentialHash): 1,
		formatVoteKey(common.VoterTypeStakingPoolKeyHash, poolHash):    1,
	}

	const govActionID = "e5e5e5e5#0"
	m.govState.Proposals[govActionID] = &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType:     common.GovActionTypeUpdateCommittee,
			SubmittedEpoch: 0,
			ExpiresAfter:   10,
			Votes:          votes,
		},
	}

	require.NoError(t, m.ProcessEpochBoundary(1))

	ratified := m.govState.Proposals[govActionID].RatifiedEpoch
	require.NotNil(
		t,
		ratified,
		"an UpdateCommittee proposal with DRep+SPO yes votes and no "+
			"committee vote must ratify through the real "+
			"ProcessEpochBoundary/ratifyProposals path",
	)
}

// formatVoteKey builds a GovActionInfo.Votes key exactly as
// recordVotesInGovState does: "<voter type digit>:<hex credential hash>".
func formatVoteKey(voterType uint8, credential common.Blake2b224) string {
	return string(rune('0'+voterType)) + ":" + hex.EncodeToString(credential[:])
}
