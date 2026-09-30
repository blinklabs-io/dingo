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
	"bytes"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
// verifies that a NoConfidence proposal whose only backing is a
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

// TestProcessEpochBoundaryRatifiesNoConfidenceWithoutCommitteeVote is
// TestProcessEpochBoundaryRatifiesUpdateCommitteeWithoutCommitteeVote's
// NoConfidence twin. A PR review found that the UpdateCommittee test alone
// only pins that half of ratifyProposals's routing: reverting just the
// NoConfidence arm back to the pre-#4007 hasCC-requiring heuristic (leaving
// UpdateCommittee routed through committeeActionRatified) left every test,
// including both routing tests and both direct-call tests, green.
func TestProcessEpochBoundaryRatifiesNoConfidenceWithoutCommitteeVote(
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

	drepCredentialHash := testHash28(0xf1)
	drepStakeCredential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xf2),
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

	poolHash := testHash28(0xf3)
	poolStakeCredential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xf4),
	}
	m.govState.PoolRegistrations[poolHash] = true
	m.govState.PoolDelegationsByCredential[poolStakeCredential] = poolHash
	m.govState.RewardAccountBalances[poolStakeCredential] = 1_000_000

	votes := map[string]uint8{
		formatVoteKey(common.VoterTypeDRepKeyHash, drepCredentialHash): 1,
		formatVoteKey(common.VoterTypeStakingPoolKeyHash, poolHash):    1,
	}

	const govActionID = "f5f5f5f5#0"
	m.govState.Proposals[govActionID] = &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType:     common.GovActionTypeNoConfidence,
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
		"a NoConfidence proposal with DRep+SPO yes votes and no committee "+
			"vote must ratify through the real ProcessEpochBoundary/"+
			"ratifyProposals path",
	)
}

// TestProcessEpochBoundaryRatifiesNoConfidenceWithNoExplicitVotes pins a
// blocker a PR review found: ratifyProposals returned early on
// `len(proposal.Votes) == 0` before ever reaching the NoConfidence/
// UpdateCommittee branch, so a proposal backed only by an implicit
// AlwaysNoConfidence delegation -- no proposal.Votes entry at all, exactly
// TestCommitteeActionRatifiedNoConfidenceUsesMotionThresholdAndImplicitYes's
// state -- was silently skipped every epoch boundary and never ratified,
// even though committeeActionRatified alone (called directly) correctly
// says yes. Driving that same state through the real ProcessEpochBoundary
// entry point is what exposes the gap a direct call cannot.
func TestProcessEpochBoundaryRatifiesNoConfidenceWithNoExplicitVotes(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	m.protocolParams = noConfidenceCommitteeParams()

	credential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xfa),
	}
	m.govState.DRepDelegationsByCredential[credential] = common.Drep{
		Type: common.DrepTypeNoConfidence,
	}
	m.govState.RewardAccountBalances[credential] = 1_000_000

	const govActionID = "fbfbfbfb#0"
	m.govState.Proposals[govActionID] = &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType:     common.GovActionTypeNoConfidence,
			SubmittedEpoch: 0,
			ExpiresAfter:   10,
			Votes:          map[string]uint8{},
		},
	}

	require.NoError(t, m.ProcessEpochBoundary(1))

	require.NotNil(
		t,
		m.govState.Proposals[govActionID].RatifiedEpoch,
		"a NoConfidence proposal backed only by an implicit "+
			"AlwaysNoConfidence delegation, with no entries in "+
			"proposal.Votes at all, must still ratify through "+
			"ProcessEpochBoundary",
	)
}

// TestCommitteeActionRatifiedRefusesDuringConwayBootstrap pins the Conway
// bootstrap gate directly: the exact same DRep/SPO-backed NoConfidence
// setup that TestProcessEpochBoundaryRatifiesNoConfidenceWithoutCommitteeVote
// proves ratifies at protocol major 10 must NOT ratify at major 9, since
// ledger/governance's ShouldRatify refuses NoConfidence and UpdateCommittee
// outright during bootstrap regardless of votes. A PR review found this
// gate was added without any test pinning it: deleting it left every
// existing test green.
func TestCommitteeActionRatifiedRefusesDuringConwayBootstrap(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	pparamsAt := func(major uint) *conway.ConwayProtocolParameters {
		return &conway.ConwayProtocolParameters{
			ProtocolVersion: common.ProtocolParametersProtocolVersion{
				Major: major,
			},
			DRepVotingThresholds: conway.DRepVotingThresholds{
				MotionNoConfidence: cbor.Rat{Rat: big.NewRat(1, 2)},
			},
			// Trivial (zero): this test isolates the DRep side and the
			// bootstrap gate, not SPO stake -- no pool is set up below.
			PoolVotingThresholds: conway.PoolVotingThresholds{
				MotionNoConfidence: trivialThreshold,
			},
		}
	}

	credential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xf6),
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

	m.protocolParams = pparamsAt(9)
	ratifiedAtBootstrap, err := m.committeeActionRatified(txn, proposal, 5)
	require.NoError(t, err)
	require.False(
		t,
		ratifiedAtBootstrap,
		"protocol major 9 (Conway bootstrap) must refuse NoConfidence "+
			"ratification regardless of votes",
	)

	m.protocolParams = pparamsAt(10)
	ratifiedAfterBootstrap, err := m.committeeActionRatified(txn, proposal, 5)
	require.NoError(t, err)
	require.True(
		t,
		ratifiedAfterBootstrap,
		"the same state must ratify once past bootstrap (major 10), "+
			"proving major 9 alone caused the refusal above",
	)
}

// TestRatifyProposalsGatesTreasuryWithdrawalDuringConwayBootstrap pins the
// bootstrap gate a PR review asked for on ratifyProposals's vote-shape
// heuristic path: unlike UpdateCommittee/NoConfidence, TreasuryWithdrawal
// and NewConstitution have no stake tally to hand to ShouldRatify, so
// ratifyProposals must check inConwayBootstrap directly for them.
func TestRatifyProposalsGatesTreasuryWithdrawalDuringConwayBootstrap(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	m.protocolParams = &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 9},
	}

	const govActionID = "f7f7f7f7#0"
	m.govState.Proposals[govActionID] = &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType:     common.GovActionTypeTreasuryWithdrawal,
			SubmittedEpoch: 0,
			ExpiresAfter:   10,
			Votes: map[string]uint8{
				formatVoteKey(
					common.VoterTypeConstitutionalCommitteeHotKeyHash,
					testHash28(0xf8),
				): 1,
				formatVoteKey(common.VoterTypeDRepKeyHash, testHash28(0xf9)): 1,
			},
		},
	}

	require.NoError(t, m.ProcessEpochBoundary(1))
	require.Nil(
		t,
		m.govState.Proposals[govActionID].RatifiedEpoch,
		"TreasuryWithdrawal must not ratify during Conway bootstrap "+
			"regardless of votes",
	)

	m.protocolParams = &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 10},
	}
	require.NoError(t, m.ProcessEpochBoundary(2))
	require.NotNil(
		t,
		m.govState.Proposals[govActionID].RatifiedEpoch,
		"the same votes must ratify once past bootstrap (major 10), "+
			"proving major 9 alone caused the refusal above",
	)
}

// TestCommitteeActionRatifiedRefusesUpdateCommitteeOverTermLimit pins
// committeeTermsWithinLimit end to end: a PR review found that
// proposal.ProposedMembersByCredential is empty at ratification in every
// vector the corpus and this file's other tests exercise, so
// syntheticUpdateCommitteeGovAction's loop over it never runs anywhere --
// the term-limit check ShouldRatify performs off that synthetic action has
// no coverage proving it can actually refuse. This constructs a proposed
// member whose expiry is far beyond CommitteeTermLimit and gives the
// proposal trivial (always-met) DRep/SPO thresholds, so the only thing that
// can block ratification is the term-limit check.
func TestCommitteeActionRatifiedRefusesUpdateCommitteeOverTermLimit(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	m.protocolParams = &conway.ConwayProtocolParameters{
		ProtocolVersion:    common.ProtocolParametersProtocolVersion{Major: 10},
		CommitteeTermLimit: 5,
		DRepVotingThresholds: conway.DRepVotingThresholds{
			CommitteeNormal:       trivialThreshold,
			CommitteeNoConfidence: trivialThreshold,
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			CommitteeNormal:       trivialThreshold,
			CommitteeNoConfidence: trivialThreshold,
		},
	}

	const currentEpoch = 5
	proposal := &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType: common.GovActionTypeUpdateCommittee,
			Votes:      map[string]uint8{},
			ProposedMembersByCredential: map[mockledger.RewardAccountKey]uint64{
				{
					CredType:   common.CredentialTypeAddrKeyHash,
					Credential: testHash28(0xfc),
				}: currentEpoch + 100, // 100 > CommitteeTermLimit (5)
			},
		},
	}

	txn := m.db.Transaction(false)
	defer txn.Release()

	ratified, err := m.committeeActionRatified(txn, proposal, currentEpoch)
	require.NoError(t, err)
	require.False(
		t,
		ratified,
		"an UpdateCommittee proposal whose member term exceeds "+
			"CommitteeTermLimit must not ratify even when DRep/SPO "+
			"thresholds are trivially met",
	)
}

// TestCommitteeActionRatifiedUsesPassedEpochNotManagerField pins the fix for
// an epoch-source inconsistency a PR review found unpinned: reverting
// drepStakeForCommitteeAction's IsDRepCredentialActive call back to
// m.currentEpoch left the whole package green, because every other test
// either sets m.currentEpoch to match the currentEpoch argument or never
// exercises a DRep whose active window depends on which of the two is used.
// This sets m.currentEpoch to 0 and passes a different currentEpoch (5) to
// committeeActionRatified, with a credential-backed DRep active only
// through epoch 3: using currentEpoch correctly excludes it (expired), so
// the proposal's only vote is gone and ratification must fail; using
// m.currentEpoch would wrongly count it as still active and ratify.
func TestCommitteeActionRatifiedUsesPassedEpochNotManagerField(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	m.protocolParams = &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 10},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence: cbor.Rat{Rat: big.NewRat(1, 2)},
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence: trivialThreshold,
		},
	}
	// m.currentEpoch is left at its zero value deliberately -- the real
	// path (ProcessEpochBoundary) always sets it to match the currentEpoch
	// argument, but a direct call (as every test in this file makes) can
	// exercise the two diverging, which is exactly what this test needs.

	drepCredentialHash := testHash28(0xfd)
	drepStakeCredential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xfe),
	}
	m.govState.DRepDelegationsByCredential[drepStakeCredential] = common.Drep{
		Type:       common.DrepTypeAddrKeyHash,
		Credential: drepCredentialHash[:],
	}
	drepKey := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: drepCredentialHash,
	}
	m.govState.DRepRegistrationsByCredential[drepKey] = true
	m.govState.DRepExpiries[drepKey] = 3 // active only through epoch 3
	m.govState.RewardAccountBalances[drepStakeCredential] = 1_000_000

	proposal := &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType: common.GovActionTypeNoConfidence,
			Votes: map[string]uint8{
				formatVoteKey(common.VoterTypeDRepKeyHash, drepCredentialHash): 1,
			},
		},
	}

	txn := m.db.Transaction(false)
	defer txn.Release()

	ratified, err := m.committeeActionRatified(txn, proposal, 5)
	require.NoError(t, err)
	require.False(
		t,
		ratified,
		"a DRep whose registration expired at epoch 3 must be excluded "+
			"when committeeActionRatified is called with currentEpoch 5, "+
			"leaving no yes stake to ratify with",
	)
}

// formatVoteKey builds a GovActionInfo.Votes key exactly as
// recordVotesInGovState does: "<voter type digit>:<hex credential hash>".
func formatVoteKey(voterType uint8, credential common.Blake2b224) string {
	return string(rune('0'+voterType)) + ":" + hex.EncodeToString(credential[:])
}

// TestCommitteeActionRatifiedExcludesProposalDepositFromSPOStake pins a PR
// #4333 review finding: an active proposal deposit raises the return
// account's DRep voting power, but it is not delegated stake behind a pool
// and must not enter the SPO tally. Production reads SPO stake straight from
// the stake distribution snapshot (tallySPOVotes over LoadSPOVotingState's
// Dist), which carries no deposit adjustment.
//
// The stake is arranged so the deposit decides the outcome. The yes pool
// holds 2,000,000 and the silent (implicit no) pool holds 1,000,000, so the
// SPO ratio is 2/3 against a 1/2 threshold and the proposal ratifies. Route
// a 3,000,000 deposit to the silent pool's delegator and, if the SPO tally
// counted it, that pool would hold 4,000,000, dropping the ratio to 1/3 and
// refusing the proposal. Passing the deposit map back into
// spoStakeForCommitteeAction therefore fails this test.
func TestCommitteeActionRatifiedExcludesProposalDepositFromSPOStake(
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

	// DRep side: an AlwaysNoConfidence delegator is an implicit yes on a
	// NoConfidence action, so the DRep ratio is 1 and the decision turns on
	// the SPO side alone. This credential delegates to no pool, so it stays
	// out of the SPO tally entirely.
	drepStakeCredential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xa1),
	}
	m.govState.DRepDelegationsByCredential[drepStakeCredential] = common.Drep{
		Type: common.DrepTypeNoConfidence,
	}
	m.govState.RewardAccountBalances[drepStakeCredential] = 1_000_000

	// Yes pool: 2,000,000 of delegated stake, voting yes explicitly.
	yesPoolHash := testHash28(0xa2)
	yesPoolDelegator := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xa3),
	}
	m.govState.PoolRegistrations[yesPoolHash] = true
	m.govState.PoolDelegationsByCredential[yesPoolDelegator] = yesPoolHash
	m.govState.RewardAccountBalances[yesPoolDelegator] = 2_000_000

	// Silent pool: 1,000,000 of delegated stake and no reward-account DRep
	// delegation, so it is an implicit no and contributes to the denominator.
	silentPoolHash := testHash28(0xa4)
	silentPoolDelegator := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xa5),
	}
	m.govState.PoolRegistrations[silentPoolHash] = true
	m.govState.PoolDelegationsByCredential[silentPoolDelegator] = silentPoolHash
	m.govState.RewardAccountBalances[silentPoolDelegator] = 1_000_000

	// An unrelated active proposal whose deposit is returned to the silent
	// pool's delegator. Large enough to invert the SPO ratio if counted.
	depositReturnAccount := silentPoolDelegator
	m.govState.Proposals["dddddddd#0"] = &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType:     common.GovActionTypeInfo,
			SubmittedEpoch: 0,
			ExpiresAfter:   10,
			Votes:          map[string]uint8{},
			Deposit:        3_000_000,
			ReturnAccount:  &depositReturnAccount,
		},
	}

	proposal := &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType:   common.GovActionTypeNoConfidence,
			ExpiresAfter: 10,
			Votes: map[string]uint8{
				formatVoteKey(
					common.VoterTypeStakingPoolKeyHash,
					yesPoolHash,
				): 1,
			},
		},
	}

	txn := m.db.Transaction(false)
	defer txn.Release()

	// The assertion deliberately goes through committeeActionRatified rather
	// than calling spoStakeForCommitteeAction directly: a direct call would
	// bind this test to that helper's signature, so restoring the deposit
	// argument would break the build instead of failing the assertion. Going
	// through the decision keeps the revert behavioural.
	ratified, err := m.committeeActionRatified(txn, proposal, 5)
	require.NoError(t, err)
	require.True(
		t,
		ratified,
		"SPO ratio is 2/3 against a 1/2 threshold once the proposal "+
			"deposit is excluded from pool stake",
	)
}

// gOuroboros common.CommitteeVotingState: CommitteeHotCredentialColdCredentials
// returns every cold credential currently authorizing the hot credential,
// seated or not, and omits only resigned ones.
func TestCommitteeHotCredentialColdCredentialsIncludesUnseatedAuthorization(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	hot := testHash28(0x81)
	seatedCold := testHash28(0x82)
	require.NoError(t, m.LoadInitialState(
		&conformance.ParsedInitialState{
			CurrentEpoch:     5,
			CommitteeMembers: map[common.Blake2b224]uint64{seatedCold: 999},
			HotKeyAuthorizations: map[common.Blake2b224]common.Blake2b224{
				seatedCold: hot,
			},
		},
		&conway.ConwayProtocolParameters{},
	))
	keyCredential := func(hash common.Blake2b224) common.Credential {
		return common.Credential{
			CredType:   common.CredentialTypeAddrKeyHash,
			Credential: hash,
		}
	}
	pendingCold := testHash28(0x83)
	resignedCold := testHash28(0x84)
	persist := func(seed string, slot uint64, certs ...common.Certificate) {
		tx, err := syntheticTransaction(seed, certs)
		require.NoError(t, err)
		require.NoError(t, m.db.SetTransactionMetadataOnly(
			tx,
			ocommon.Point{Slot: slot, Hash: syntheticBlockHash(slot)},
			0,
			map[int]uint64{},
			nil,
		))
	}
	persist("pending-auth", 10,
		&common.AuthCommitteeHotCertificate{
			CertType:       uint(common.CertificateTypeAuthCommitteeHot),
			ColdCredential: keyCredential(pendingCold),
			HotCredential:  keyCredential(hot),
		},
		&common.AuthCommitteeHotCertificate{
			CertType:       uint(common.CertificateTypeAuthCommitteeHot),
			ColdCredential: keyCredential(resignedCold),
			HotCredential:  keyCredential(hot),
		},
	)
	persist("pending-resign", 11,
		&common.ResignCommitteeColdCertificate{
			CertType:       uint(common.CertificateTypeResignCommitteeCold),
			ColdCredential: keyCredential(resignedCold),
		},
	)

	provider := NewDingoStateProvider(m)
	coldCredentials, err := provider.CommitteeHotCredentialColdCredentials(
		keyCredential(hot),
	)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]common.Credential{
			keyCredential(seatedCold),
			keyCredential(pendingCold),
		},
		coldCredentials,
	)
	elected, err := provider.CommitteeCredentialIsElected(
		keyCredential(pendingCold),
	)
	require.NoError(t, err)
	require.False(t, elected)
	coldCredentials, err = provider.CommitteeHotCredentialColdCredentials(
		common.Credential{
			CredType:   common.CredentialTypeScriptHash,
			Credential: hot,
		},
	)
	require.NoError(t, err)
	require.Empty(t, coldCredentials)
}

// Stored committee credentials of the wrong length are corrupt state: the
// harness's committee lookups must fail rather than truncate them into a
// 28-byte credential that matches a real hot key or cold credential.
func TestCommitteeVotingStateRejectsMalformedStoredHashes(t *testing.T) {
	hot := testHash28(0x91)
	cold := testHash28(0x92)
	keyCredential := func(hash common.Blake2b224) common.Credential {
		return common.Credential{
			CredType:   common.CredentialTypeAddrKeyHash,
			Credential: hash,
		}
	}
	overlong := func(hash common.Blake2b224) []byte {
		return append(append([]byte(nil), hash[:]...), 0xff)
	}
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, raw *sql.DB)
		lookup func(p *DingoStateProvider) error
	}{
		{
			name: "authorized hot credential",
			mutate: func(t *testing.T, raw *sql.DB) {
				_, err := raw.Exec(
					`UPDATE auth_committee_hot SET host_credential = ?`,
					overlong(hot),
				)
				require.NoError(t, err)
			},
			lookup: func(p *DingoStateProvider) error {
				_, err := p.CommitteeHotCredentialMember(keyCredential(hot))
				if err != nil {
					return err
				}
				_, err = p.CommitteeHotCredentialColdCredentials(
					keyCredential(hot),
				)
				return err
			},
		},
		{
			name: "seated cold credential",
			mutate: func(t *testing.T, raw *sql.DB) {
				_, err := raw.Exec(
					`UPDATE committee_member SET cold_cred_hash = ?`,
					overlong(cold),
				)
				require.NoError(t, err)
			},
			lookup: func(p *DingoStateProvider) error {
				_, err := p.CommitteeCredentialIsElected(keyCredential(cold))
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := NewDingoStateManager()
			require.NoError(t, err)
			defer func() { require.NoError(t, m.Close()) }()
			require.NoError(t, m.LoadInitialState(
				&conformance.ParsedInitialState{
					CurrentEpoch:     5,
					CommitteeMembers: map[common.Blake2b224]uint64{cold: 999},
					HotKeyAuthorizations: map[common.Blake2b224]common.Blake2b224{
						cold: hot,
					},
				},
				&conway.ConwayProtocolParameters{},
			))
			raw, err := dbtest.RawSQLiteMetadata(t, m.db)
			require.NoError(t, err)
			tc.mutate(t, raw)

			require.ErrorContains(
				t,
				tc.lookup(NewDingoStateProvider(m)),
				"invalid blake2b-224 hash",
			)
		})
	}
}

// The harness mirrors LedgerView's committee windows: an unseated
// credential's authorization lasts until the next epoch boundary, as
// cardano-ledger's EPOCH updateCommitteeState drops it there.
func TestCommitteeHotCredentialMemberDropsUnseatedAuthorizationAtBoundary(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	seatedCold := testHash28(0xa1)
	require.NoError(t, m.LoadInitialState(
		&conformance.ParsedInitialState{
			CurrentEpoch:     5,
			CommitteeMembers: map[common.Blake2b224]uint64{seatedCold: 999},
		},
		&conway.ConwayProtocolParameters{},
	))
	pending := common.Credential{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xa2),
	}
	hot := common.Credential{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xa3),
	}
	action, err := common.NewUpdateCommitteeGovAction(
		nil,
		nil,
		map[*common.Credential]uint64{&pending: 999},
		cbor.Rat{Rat: big.NewRat(2, 3)},
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)
	require.NoError(t, m.db.SetGovernanceProposal(
		&models.GovernanceProposal{
			TxHash:        testHash32(0xa4),
			ActionType:    uint8(common.GovActionTypeUpdateCommittee),
			ExpiresEpoch:  1000,
			GovActionCbor: encoded,
			AnchorHash:    testHash32(0xa5),
			ReturnAddress: bytes.Repeat([]byte{0xa6}, 29),
		},
		nil,
	))
	tx, err := syntheticTransaction(
		"pending-authorization",
		[]common.Certificate{&common.AuthCommitteeHotCertificate{
			CertType:       uint(common.CertificateTypeAuthCommitteeHot),
			ColdCredential: pending,
			HotCredential:  hot,
		}},
	)
	require.NoError(t, err)
	require.NoError(t, m.ApplyTransaction(tx, 10))

	provider := NewDingoStateProvider(m)
	member, err := provider.CommitteeHotCredentialMember(hot)
	require.NoError(t, err)
	require.NotNil(t, member, "the authorization holds for its own epoch")

	require.NoError(t, m.ProcessEpochBoundary(6))
	member, err = provider.CommitteeHotCredentialMember(hot)
	require.NoError(t, err)
	require.Nil(t, member, "the boundary drops an unseated authorization")
	coldMember, err := provider.CommitteeCredentialMember(pending)
	require.NoError(t, err)
	require.NotNil(t, coldMember, "the credential is still a potential member")
	require.Nil(t, coldMember.HotKey)
}

const expectedBlueprintVectorCount = 2575

// TestRulesConformanceVectors runs the Cardano Blueprint ledger-rule
// conformance corpus using Dingo's ledger implementation via the shared
// harness from
// ouroboros-mock/conformance.
//
// The test vectors exercise ledger rules across the pinned eras, including:
// - UTxO validation (inputs, outputs, fees, collateral)
// - Certificate processing (stake, pool, DRep, committee)
// - Governance (proposals, voting, enactment)
// - Script execution (native scripts, Plutus V1/V2/V3)
//
// Test vectors are embedded in the ouroboros-mock module and extracted at test
// time. This asserts and reports from a single corpus replay; see
// corpus_test.go for why the replay is memoized per backend and what the
// previous separate statistics pass cost.
func TestRulesConformanceVectors(t *testing.T) {
	results := sqliteCorpusResults(t)
	reportCorpus(t, "sqlite", results)
	require.Equal(t, expectedBlueprintVectorCount, len(results))
	assertCorpus(t, "sqlite", results)
}

// One full replay of the Blueprint vector corpus is expensive -- it was 917s of
// this package's Linux CI time with real Postgres and MySQL attached -- and
// the corpus exercises gouroboros ledger rules, which do not vary by storage
// backend. Replaying it more than once per backend therefore buys no rule
// coverage.
//
// What the per-backend replays do buy is dialect divergence, and that is not
// hypothetical: #3599 found two real bugs this way, neither of them a rule bug.
// loadPoolAssociations held a pool_registration cursor open while issuing
// nested per-row queries on one connection, which SQLite tolerates and MySQL
// and PostgreSQL do not; and go-sql-driver/mysql reports rows changed rather
// than rows matched, so a DRep voting twice in an epoch with an unchanged
// expiry looked like a missing row. Both were found by driving the storage
// layer through the corpus's variety of access patterns, which needs one pass
// per dialect, not several.
//
// So each backend replays the corpus exactly once per `go test` process, and
// every consumer -- the pass/fail gate, the progress statistics, and the
// cross-backend comparison -- reads that one memoized result set. Before this,
// a Linux CI run replayed the corpus eight times: SQLite four (a plain pass, a
// statistics pass, and a fresh baseline rebuilt inside each of the two
// comparison tests), Postgres twice, and MySQL twice.

// corpusRun is one backend's memoized corpus replay. err is retained rather
// than failing inside the sync.Once, so that every test reading this backend
// reports the same construction or replay failure instead of only whichever
// test happened to trigger the Once first.
type corpusRun struct {
	results []conformance.VectorResult
	err     error
}

var (
	testdataOnce sync.Once
	testdataDir  string
	testdataRoot string
	testdataErr  error
)

// corpusTestdataRoot extracts the embedded vector corpus once per process.
// It deliberately does not use t.TempDir(): the extraction is shared across
// tests, so tying it to the lifetime of whichever test triggered it first
// would delete it while later tests still name its paths in failure output.
// TestMain removes it (see cleanupCorpusTestdata).
func corpusTestdataRoot() (string, error) {
	testdataOnce.Do(func() {
		dir, err := os.MkdirTemp("", "dingo-conformance-vectors-")
		if err != nil {
			testdataErr = fmt.Errorf("create testdata dir: %w", err)
			return
		}
		// Record the directory before extraction, not after: a failure
		// below still leaves a real directory on disk, and
		// cleanupCorpusTestdata's empty-string early return would skip it.
		testdataDir = dir
		// ExtractEmbeddedTestdata returns the extracted root, which is a
		// "testdata" subdirectory of dir, not dir itself. Use the returned
		// path; dir is only what gets removed on cleanup.
		root, err := conformance.ExtractEmbeddedTestdata(dir)
		if err != nil {
			testdataErr = fmt.Errorf("extract embedded testdata: %w", err)
			return
		}
		testdataRoot = root
	})
	return testdataRoot, testdataErr
}

// cleanupCorpusTestdata removes the shared extraction. Safe to call when the
// corpus was never extracted, which is the case for a run whose tests all
// skipped.
func cleanupCorpusTestdata() error {
	if testdataDir == "" {
		return nil
	}
	return os.RemoveAll(testdataDir)
}

// replayCorpus runs the whole corpus once against sm and returns per-vector
// results. It uses RunAllVectorsWithResults rather than RunAllVectors so the
// single pass can serve both the gate and the statistics; assertCorpus turns
// the results back into per-vector subtests, so no subtest naming is lost.
func replayCorpus(sm *DingoStateManager) corpusRun {
	root, err := corpusTestdataRoot()
	if err != nil {
		return corpusRun{err: err}
	}
	harness := conformance.NewHarness(sm, conformance.HarnessConfig{
		TestdataRoot: root,
	})
	results, err := harness.RunAllVectorsWithResults()
	if err != nil {
		return corpusRun{err: fmt.Errorf("run vectors: %w", err)}
	}
	return corpusRun{results: results}
}

var (
	sqliteCorpusOnce sync.Once
	sqliteCorpusRun  corpusRun
)

// sqliteCorpusResults returns the SQLite backend's memoized corpus replay.
// SQLite needs no external service, so this is the backend every run has and
// the baseline the Postgres and MySQL comparisons measure against.
func sqliteCorpusResults(t *testing.T) []conformance.VectorResult {
	t.Helper()
	sqliteCorpusOnce.Do(func() {
		sm, err := NewDingoStateManager()
		if err != nil {
			sqliteCorpusRun = corpusRun{
				err: fmt.Errorf("new sqlite state manager: %w", err),
			}
			return
		}
		defer sm.Close()
		sqliteCorpusRun = replayCorpus(sm)
	})
	require.NoError(t, sqliteCorpusRun.err, "sqlite corpus replay")
	return sqliteCorpusRun.results
}

// assertCorpus is the pass/fail gate. Each vector becomes a named subtest, as
// harness.RunAllVectors produced, so a failure still identifies its vector by
// path in the test output; the result carries the event index the vector
// failed at, which the assertion path did not report.
func assertCorpus(
	t *testing.T,
	backend string,
	results []conformance.VectorResult,
) {
	t.Helper()
	require.NotEmpty(
		t,
		results,
		"%s: corpus replay produced no vectors; vector discovery or "+
			"extraction is broken, and an empty corpus would otherwise "+
			"report as a pass",
		backend,
	)
	for _, result := range results {
		t.Run(result.Path, func(t *testing.T) {
			if result.Success {
				return
			}
			t.Fatalf(
				"vector failed on %s at event %d of %d: %v (%s)",
				backend,
				result.FailedEvent,
				result.EventCount,
				result.Error,
				result.Title,
			)
		})
	}
}

// reportCorpus logs the progress statistics that a separate second replay per
// backend used to produce.
func reportCorpus(
	t *testing.T,
	backend string,
	results []conformance.VectorResult,
) {
	t.Helper()
	passed, failed := corpusCounts(results)

	t.Logf("Conformance Test Results (%s):", backend)
	t.Logf("  Total vectors: %d", len(results))
	t.Logf("  Passed: %d", passed)
	t.Logf("  Failed: %d", failed)
	if len(results) > 0 {
		t.Logf(
			"  Pass rate: %.1f%%",
			float64(passed)/float64(len(results))*100,
		)
	}
	coverage := conformance.SummarizeCoverage(results)
	t.Logf("  Coverage groups: %d", len(coverage))
	for _, key := range conformance.SortedCoverageKeys(coverage) {
		counts := coverage[key]
		t.Logf(
			"  Coverage %s/%s: total=%d passed=%d failed=%d",
			key.Era,
			key.RuleFamily,
			counts.Total,
			counts.Passed,
			counts.Failed,
		)
	}

	if failed > 0 && testing.Verbose() {
		t.Log("First failures:")
		failCount := 0
		for _, result := range results {
			if !result.Success && failCount < 5 {
				t.Logf("  %s: %v", result.Title, result.Error)
				failCount++
			}
		}
		if failed > 5 {
			t.Logf("  ... and %d more failures", failed-5)
		}
	}
}

// corpusCounts returns the passed and failed vector counts.
func corpusCounts(results []conformance.VectorResult) (int, int) {
	var passed, failed int
	for _, result := range results {
		if result.Success {
			passed++
		} else {
			failed++
		}
	}
	return passed, failed
}

// assertBackendMatchesSqlite compares an external backend's replay against the
// SQLite baseline. Vector discovery is backend-invariant, so a different count
// means extraction or discovery diverged rather than a rule behaving
// differently; a vector failing here that SQLite passed is a dialect
// divergence, which is what running the corpus on this backend is for.
//
// The vector comparison is deliberately one-directional. The opposite
// divergence -- SQLite failing a vector this backend passes -- is not silent:
// assertCorpus runs over every backend's own results, including SQLite's in
// TestRulesConformanceVectors, and fails on any vector that backend failed. So
// a divergence in either direction turns the run red; naming it here as well
// would only duplicate the SQLite gate. What this direction adds is
// attribution, pointing at the backend rather than at the corpus.
func assertBackendMatchesSqlite(
	t *testing.T,
	backend string,
	results []conformance.VectorResult,
) {
	t.Helper()
	assertCorpusSetsMatch(t, backend, sqliteCorpusResults(t), results)
}

// corpusAsserter is the subset of *testing.T the corpus comparison needs. It
// exists so the comparison can be exercised against a recorder rather than
// only through a real corpus replay; a zero-value testing.T is not usable for
// that, since require's FailNow needs a running test goroutine.
type corpusAsserter interface {
	require.TestingT
	Helper()
}

// assertCorpusSetsMatch is assertBackendMatchesSqlite's comparison, split out
// so it can be exercised without replaying the corpus.
func assertCorpusSetsMatch(
	t corpusAsserter,
	backend string,
	sqliteResults []conformance.VectorResult,
	results []conformance.VectorResult,
) {
	t.Helper()

	require.Equal(
		t,
		len(sqliteResults),
		len(results),
		"%s backend exercised a different number of vectors than sqlite; "+
			"vector discovery/extraction should be backend-invariant",
		backend,
	)

	// Compare the path sets, not just their sizes. Equal counts over
	// different paths would otherwise slip through, and the pass lookup
	// below cannot catch it on its own: a backend path absent from the
	// sqlite map reads as false, which is exactly what the assertion
	// expects, so {a,b} against {a,c} would pass on both checks.
	require.ElementsMatch(
		t,
		corpusPaths(sqliteResults),
		corpusPaths(results),
		"%s backend exercised different vectors than sqlite; vector "+
			"discovery/extraction should be backend-invariant",
		backend,
	)

	sqlitePassed := make(map[string]bool, len(sqliteResults))
	for _, result := range sqliteResults {
		sqlitePassed[result.Path] = result.Success
	}
	for _, result := range results {
		if result.Success {
			continue
		}
		// No presence guard here: ElementsMatch above FailNows on any path
		// set difference, so every backend path exists in sqlitePassed by
		// the time this loop runs.
		require.Falsef(
			t,
			sqlitePassed[result.Path],
			"%s backend failed a vector sqlite passed (%s at event %d): %v",
			backend,
			result.Title,
			result.FailedEvent,
			result.Error,
		)
	}
}

// corpusPaths returns each result's vector path, for set comparison between
// backends.
func corpusPaths(results []conformance.VectorResult) []string {
	paths := make([]string, len(results))
	for i, result := range results {
		paths[i] = result.Path
	}
	return paths
}

// recordingAsserter records whether an assertion failed, without the Goexit a
// real *testing.T performs, so a single call's outcome can be inspected.
type recordingAsserter struct {
	failed bool
}

func (r *recordingAsserter) Errorf(string, ...any) { r.failed = true }
func (r *recordingAsserter) FailNow()              { r.failed = true }
func (r *recordingAsserter) Helper()               {}

// TestAssertCorpusSetsMatchRejectsDifferentPaths proves the comparison fails
// when two backends run the same number of vectors with different paths.
//
// The count check alone cannot see this, and neither can the pass lookup: a
// backend path absent from the sqlite map reads as false, which is what that
// assertion expects. So {a,b} against {a,c} passed both checks before the path
// set comparison was added.
func TestAssertCorpusSetsMatchRejectsDifferentPaths(t *testing.T) {
	sqliteResults := []conformance.VectorResult{
		{Path: "a", Success: true},
		{Path: "b", Success: true},
	}
	backendResults := []conformance.VectorResult{
		{Path: "a", Success: true},
		{Path: "c", Success: true},
	}

	rec := &recordingAsserter{}
	assertCorpusSetsMatch(rec, "probe", sqliteResults, backendResults)
	require.True(
		t,
		rec.failed,
		"equal counts over different vector paths must fail the comparison",
	)
}

// TestAssertCorpusSetsMatchRejectsExtraBackendVector proves a backend vector
// the sqlite baseline never ran is reported.
//
// It is caught by the path set comparison, not by any per-vector presence
// check. An earlier revision added such a check after ElementsMatch and a test
// asserting it; both were dead, because ElementsMatch FailNows first on a real
// *testing.T. The recording asserter used here does not stop on failure, so
// that test passed on the ElementsMatch failure while claiming to exercise the
// guard -- it asserted something already true.
func TestAssertCorpusSetsMatchRejectsExtraBackendVector(t *testing.T) {
	sqliteResults := []conformance.VectorResult{{Path: "a", Success: true}}
	backendResults := []conformance.VectorResult{{Path: "z", Success: false}}

	rec := &recordingAsserter{}
	assertCorpusSetsMatch(rec, "probe", sqliteResults, backendResults)
	require.True(
		t,
		rec.failed,
		"a failed vector absent from the sqlite baseline must be reported",
	)
}

// TestAssertCorpusSetsMatchAcceptsIdenticalRuns proves the comparison stays
// quiet when both backends ran the same vectors with the same outcomes, so the
// checks above cannot pass by simply failing everything.
func TestAssertCorpusSetsMatchAcceptsIdenticalRuns(t *testing.T) {
	results := []conformance.VectorResult{
		{Path: "a", Success: true},
		{Path: "b", Success: true},
	}

	rec := &recordingAsserter{}
	assertCorpusSetsMatch(rec, "probe", results, results)
	require.False(
		t,
		rec.failed,
		"identical runs must not be reported as divergent",
	)
}

// This file is the entry-point replay machinery; entry_points_test.go holds
// the assertions. It is a _test.go file because every identifier in it is
// test-only and unexported, and .golangci.yml sets run.tests: false, so the
// same code in a plain .go file is reported as unused by the linter while
// still being exercised by the package's tests.
//
// The shared ouroboros-mock harness validates each vector transaction with
// common.VerifyTransaction over conformance.ConformanceValidationRules -- a
// list of upstream gouroboros rule functions. Nothing in that path reaches
// Dingo's own era validation entry points (eras.EraDesc.ValidateTxFunc, i.e.
// ValidateTxByron .. ValidateTxDijkstra), which are what the node actually
// runs against live transactions and which differ from the upstream list:
// Conway and Dijkstra substitute Dingo implementations for the committee
// certificate, unknown voter, Plutus, fee and PlutusV1/V2 feature rules, and
// the pre-Alonzo eras replace the upstream fee and max-size rules outright.
//
// The consequence is that the corpus pass rate is independent of those entry
// points: with ValidateTxConway stubbed to `return nil`, all 315 vectors still
// report as passing. This file routes every corpus vector transaction through
// the production entry point for its era as well, and records evidence that
// the entry point actually consulted ledger state derived from the
// transaction, so a bypassed or fixture-only validator cannot read as a pass.

// validateTxFunc is eras.EraDesc.ValidateTxFunc's signature.
type validateTxFunc = func(
	common.Transaction,
	uint64,
	common.LedgerState,
	common.ProtocolParameters,
) error

// eraEntryPoint is one era's production transaction-validation entry point,
// read from Dingo's era registry rather than restated here. Restating the
// list would defeat the purpose: an era whose ValidateTxFunc is dropped from
// the registry has to show up as a missing entry point, not as a local copy
// that keeps working.
type eraEntryPoint struct {
	Validate        validateTxFunc
	Name            string
	Id              uint
	MinMajorVersion uint
	MaxMajorVersion uint
}

// dingoEraEntryPoints reads the production validation entry point out of each
// era descriptor. An era with no ValidateTxFunc is reported rather than
// skipped: a nil entry point is exactly the "validation path bypassed" state
// these tests exist to catch.
func dingoEraEntryPoints(eraList []eras.EraDesc) ([]eraEntryPoint, error) {
	if len(eraList) == 0 {
		return nil, errors.New("era registry is empty")
	}
	entries := make([]eraEntryPoint, 0, len(eraList))
	var missing []string
	for _, era := range eraList {
		if era.ValidateTxFunc == nil {
			missing = append(missing, era.Name)
			continue
		}
		entries = append(entries, eraEntryPoint{
			Validate:        era.ValidateTxFunc,
			Name:            era.Name,
			Id:              era.Id,
			MinMajorVersion: era.MinMajorVersion,
			MaxMajorVersion: era.MaxMajorVersion,
		})
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf(
			"eras with no production validation entry point: %v",
			missing,
		)
	}
	return entries, nil
}

// entryPointForProtocolVersion resolves the era entry point covering a
// protocol major version, mirroring eras.EraForVersionIn.
func entryPointForProtocolVersion(
	entries []eraEntryPoint,
	majorVersion uint,
) (eraEntryPoint, bool) {
	for _, entry := range entries {
		if majorVersion >= entry.MinMajorVersion &&
			majorVersion <= entry.MaxMajorVersion {
			return entry, true
		}
	}
	return eraEntryPoint{}, false
}

// protocolMajorVersion reports the protocol major version carried by pp. It
// is what selects the era, and therefore which production entry point a
// vector's transactions belong to.
//
// Every parameter type from Shelley onward implements
// common.PoolRuleProtocolParameters; the Utxorpc projection is the fallback
// for anything that does not.
func protocolMajorVersion(pp common.ProtocolParameters) (uint, error) {
	if pp == nil {
		return 0, errors.New("nil protocol parameters")
	}
	if versioned, ok := pp.(common.PoolRuleProtocolParameters); ok {
		return versioned.ProtocolMajorVersion(), nil
	}
	upp, err := pp.Utxorpc()
	if err != nil {
		return 0, fmt.Errorf("project protocol parameters: %w", err)
	}
	version := upp.GetProtocolVersion()
	if version == nil {
		return 0, errors.New("protocol parameters carry no protocol version")
	}
	return uint(version.GetMajor()), nil
}

// observedLedgerState wraps the real DingoStateProvider and records the reads
// a validation entry point performs through it.
//
// It embeds the concrete provider rather than the common.LedgerState
// interface so that the optional capabilities Dingo's era validation asserts
// for -- eras.CommitteeCredentialState among them -- keep resolving. Wrapping
// the interface would silently strip them and change what the entry point
// validates.
type observedLedgerState struct {
	*DingoStateProvider
	utxoLookups map[string]struct{}
	reads       int
}

// The wrapper must keep satisfying everything the real provider satisfies,
// including the optional capability Dingo's Conway and Dijkstra validation
// type-asserts for. Losing it here would change what the entry point
// validates while still reporting as "routed".
var (
	_ common.LedgerState            = (*observedLedgerState)(nil)
	_ common.EpochState             = (*observedLedgerState)(nil)
	_ eras.CommitteeCredentialState = (*observedLedgerState)(nil)
)

func newObservedLedgerState(
	provider *DingoStateProvider,
) *observedLedgerState {
	return &observedLedgerState{
		DingoStateProvider: provider,
		utxoLookups:        make(map[string]struct{}),
	}
}

// reset clears the recorded reads so one observer can serve consecutive
// routings without attributing an earlier transaction's lookups to a later
// one.
func (o *observedLedgerState) reset() {
	clear(o.utxoLookups)
	o.reads = 0
}

// UtxoById records the input reference before delegating. The lookup is
// recorded even when it fails: an entry point that asked for an input it
// could not resolve still executed, which is what is being observed.
func (o *observedLedgerState) UtxoById(
	id common.TransactionInput,
) (common.Utxo, error) {
	o.reads++
	if id != nil {
		o.utxoLookups[utxoLookupKey(id)] = struct{}{}
	}
	return o.DingoStateProvider.UtxoById(id)
}

// NetworkId records the read performed by the network-id rules.
func (o *observedLedgerState) NetworkId() uint {
	o.reads++
	return o.DingoStateProvider.NetworkId()
}

// CostModels records the read performed by the script rules.
func (o *observedLedgerState) CostModels() map[common.PlutusLanguage]common.CostModel {
	o.reads++
	return o.DingoStateProvider.CostModels()
}

// utxoLookupKey is the canonical identity of a transaction input, used to
// match what an entry point looked up against what the transaction declared.
// It deliberately does not use TransactionInput.String(), whose format is not
// part of any contract.
func utxoLookupKey(id common.TransactionInput) string {
	txId := id.Id()
	return fmt.Sprintf("%x#%d", txId[:], id.Index())
}

// entryPointRouting is the evidence produced by routing one vector
// transaction through a production era validation entry point.
type entryPointRouting struct {
	// Err is what the entry point returned. A non-nil Err is not a test
	// failure: Dingo's rule set is a strict superset of the corpus rule set
	// (it keeps the fee and max-size rules the corpus excludes because the
	// vectors carry Haskell-computed values), so a vector the corpus accepts
	// may still be rejected here. What is asserted is that the entry point
	// ran and read transaction-derived state, not what it decided.
	Err error

	// EraName is the era whose entry point was used.
	EraName string

	// EntryPoint identifies the production function that ran.
	EntryPoint string

	// EventIndex is the transaction event's index within the vector.
	EventIndex int

	// DeclaredInputs is the number of inputs the transaction declares.
	DeclaredInputs int

	// LookedUpInputs is how many of those declared inputs the entry point
	// resolved through the ledger state. This is the transaction-derived
	// signal: a no-op, bypassed, or fixture-only validator resolves none.
	LookedUpInputs int

	// StateReads is the total number of observed ledger-state reads.
	StateReads int
}

// vectorEntryPointEvidence records one vector's trip through the production
// entry points, preserving per-vector identity so an aggregate cannot hide a
// vector whose validation path never ran.
type vectorEntryPointEvidence struct {
	// Err is a replay failure (decode, initial state, epoch boundary). It is
	// a test failure: it means the vector produced no entry-point evidence.
	Err error

	Path     string
	Title    string
	TxEvents int
	Routings []entryPointRouting
}

// routeTransaction runs one transaction through a production era validation
// entry point against the observed ledger state and returns the evidence.
func routeTransaction(
	entry eraEntryPoint,
	entryPointName string,
	tx common.Transaction,
	slot uint64,
	ls *observedLedgerState,
	pp common.ProtocolParameters,
	eventIndex int,
) entryPointRouting {
	inputs := tx.Inputs()
	declared := make(map[string]struct{}, len(inputs))
	for _, input := range inputs {
		if input == nil {
			continue
		}
		declared[utxoLookupKey(input)] = struct{}{}
	}

	ls.reset()
	err := entry.Validate(tx, slot, ls, pp)

	lookedUp := 0
	for key := range declared {
		if _, ok := ls.utxoLookups[key]; ok {
			lookedUp++
		}
	}

	return entryPointRouting{
		Err:            err,
		EraName:        entry.Name,
		EntryPoint:     entryPointName,
		EventIndex:     eventIndex,
		DeclaredInputs: len(declared),
		LookedUpInputs: lookedUp,
		StateReads:     ls.reads,
	}
}

// entryPointExecutionFault reports why a routing fails to prove that the
// production validation path executed, or nil when it does prove it.
//
// The predicate is deliberately independent of the validation verdict. It is
// satisfied only by evidence the entry point could not have produced without
// looking at this transaction: the ledger-state lookups of the inputs the
// transaction itself declares. A validator that returns a canned verdict --
// nil, an error, or a value copied from the vector fixture -- performs none
// of those lookups and is reported here.
func entryPointExecutionFault(routing entryPointRouting) error {
	if routing.EraName == "" || routing.EntryPoint == "" {
		return errors.New(
			"no production era validation entry point was resolved for this transaction",
		)
	}
	if routing.DeclaredInputs == 0 {
		// A transaction with no inputs is invalid in every era (Byron's
		// InputSetEmpty rule and the UtxoValidateInputSetEmptyUtxo rule from
		// Shelley onward), so there is nothing to look up and acceptance is
		// itself proof the path did not run.
		if routing.Err == nil {
			return fmt.Errorf(
				"%s accepted a transaction with no inputs; a validating entry point must reject one",
				routing.EntryPoint,
			)
		}
		return nil
	}
	if routing.LookedUpInputs == 0 {
		return fmt.Errorf(
			"%s resolved none of the transaction's %d declared inputs through the ledger state (%d total state reads); the production validation path did not run",
			routing.EntryPoint,
			routing.DeclaredInputs,
			routing.StateReads,
		)
	}
	return nil
}

// entryPointFuncName is the reporting name of an era's production entry
// point, e.g. "eras.ValidateTxConway".
func entryPointFuncName(entry eraEntryPoint) string {
	return "eras.ValidateTx" + entry.Name
}

// collectEntryPointVectors walks the same corpus roots the shared harness
// walks, so this pass and the harness pass see the same vector set.
func collectEntryPointVectors(testdataRoot string) ([]string, error) {
	var all []string
	for _, sub := range []string{"eras", "synthetic"} {
		root := filepath.Join(testdataRoot, sub)
		paths, err := conformance.CollectVectorFiles(root)
		if err != nil {
			// A corpus that ships no synthetic/ directory is legitimate, so
			// that one case is skipped. Every other failure is reported: an
			// unreadable vector or an IO error would otherwise shrink the
			// vector set silently, and a partial corpus that still routes
			// some transactions reports as full entry-point coverage --
			// the exact failure mode these tests exist to catch.
			if sub == "synthetic" && errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("collect %s vectors: %w", sub, err)
		}
		all = append(all, paths...)
	}
	if len(all) == 0 {
		return nil, fmt.Errorf("no vectors found under %s", testdataRoot)
	}
	sort.Strings(all)
	return all, nil
}

// replayEntryPoints replays the corpus against sm, routing every transaction
// event through the production era entry point resolved from the vector's own
// protocol parameters.
//
// It is a separate replay from the shared harness's, because the harness has
// no hook for a caller-supplied validator and never reaches Dingo's entry
// points. State advancement mirrors the harness: successful transactions are
// applied, epoch events cross the boundary, and a rollback event restores the
// initial state and re-applies the journaled transactions at or below the
// target slot. The one modelled difference is that only transactions are
// journaled, not epoch events, so a rollback that follows an epoch boundary
// is reported as an error rather than replayed -- no corpus vector does that
// today.
func replayEntryPoints(
	sm *DingoStateManager,
	testdataRoot string,
	entries []eraEntryPoint,
) ([]vectorEntryPointEvidence, error) {
	paths, err := collectEntryPointVectors(testdataRoot)
	if err != nil {
		return nil, err
	}
	loader := conformance.NewPParamsLoaderFromTestdata(testdataRoot)
	provider := NewDingoStateProvider(sm)
	observer := newObservedLedgerState(provider)

	evidence := make([]vectorEntryPointEvidence, 0, len(paths))
	for _, path := range paths {
		ev := replayVectorEntryPoints(sm, loader, observer, entries, path)
		// The corpus is extracted to a fresh temp directory per process, so
		// the absolute path is not a stable subtest name. Report the path
		// relative to the corpus root instead.
		if rel, err := filepath.Rel(testdataRoot, path); err == nil {
			ev.Path = rel
		}
		evidence = append(evidence, ev)
	}
	return evidence, nil
}

// appliedTx is a journaled transaction, retained so a rollback event can
// re-apply the transactions at or below its target slot.
type appliedTx struct {
	tx   common.Transaction
	slot uint64
}

func replayVectorEntryPoints(
	sm *DingoStateManager,
	loader *conformance.PParamsLoader,
	observer *observedLedgerState,
	entries []eraEntryPoint,
	path string,
) vectorEntryPointEvidence {
	ev := vectorEntryPointEvidence{Path: path}

	vector, err := conformance.DecodeTestVector(path)
	if err != nil {
		ev.Err = fmt.Errorf("decode vector: %w", err)
		return ev
	}
	ev.Title = vector.Title

	initialState, err := conformance.ParseInitialState(vector.InitialState)
	if err != nil {
		ev.Err = fmt.Errorf("parse initial state: %w", err)
		return ev
	}
	pp, err := loader.LoadForVector(vector, initialState)
	if err != nil {
		ev.Err = fmt.Errorf("load protocol parameters: %w", err)
		return ev
	}
	if err := sm.Reset(); err != nil {
		ev.Err = fmt.Errorf("reset state: %w", err)
		return ev
	}
	if err := sm.LoadInitialState(initialState, pp); err != nil {
		ev.Err = fmt.Errorf("load initial state: %w", err)
		return ev
	}

	epoch := initialState.CurrentEpoch
	var applied []appliedTx
	var epochCrossed bool

	for idx, event := range vector.Events {
		switch event.Type {
		case conformance.EventTypeTransaction:
			ev.TxEvents++
			tx, err := decodeVectorTransaction(event.TxBytes)
			if err != nil {
				// The harness tolerates a decode failure on an
				// expected-failure event; so does this pass, but the event is
				// not counted as one that reached an entry point.
				if event.Success {
					ev.Err = fmt.Errorf(
						"event %d: decode transaction: %w",
						idx,
						err,
					)
					return ev
				}
				ev.TxEvents--
				continue
			}
			routing, err := routeVectorTransaction(
				entries, observer, tx, event.Slot, pp, idx,
			)
			if err != nil {
				ev.Err = err
				return ev
			}
			ev.Routings = append(ev.Routings, routing)
			if event.Success {
				if err := sm.ApplyTransaction(tx, event.Slot); err != nil {
					ev.Err = fmt.Errorf("event %d: apply: %w", idx, err)
					return ev
				}
				applied = append(applied, appliedTx{tx: tx, slot: event.Slot})
			}
		case conformance.EventTypePassEpoch:
			epoch += event.EpochDelta
			if err := sm.ProcessEpochBoundary(epoch); err != nil {
				ev.Err = fmt.Errorf("event %d: epoch boundary: %w", idx, err)
				return ev
			}
			pp = sm.GetProtocolParameters()
			epochCrossed = true
		case conformance.EventTypeRollback:
			if epochCrossed {
				// The harness restores initialProtocolParams and replays its
				// journaled epoch events on rollback. This pass journals only
				// transactions, so it can neither undo an enacted parameter
				// change nor re-cross a boundary. No vector in the corpus
				// rolls back after an epoch event, so rather than model a
				// path nothing exercises -- and silently route later
				// transactions through an era selected from stale parameters
				// -- fail loudly if one ever appears.
				ev.Err = fmt.Errorf(
					"event %d: rollback after an epoch boundary is not modelled by this replay; journal epoch events and restore the vector's initial protocol parameters before relying on it",
					idx,
				)
				return ev
			}
			retained, err := rollbackEntryPointReplay(
				sm, initialState, pp, applied, event.RollbackSlot,
			)
			if err != nil {
				ev.Err = fmt.Errorf("event %d: rollback: %w", idx, err)
				return ev
			}
			applied = retained
			epoch = initialState.CurrentEpoch
		case conformance.EventTypePassTick:
			// No state effect; the harness only advances its slot cursor.
		}
	}
	return ev
}

// routeVectorTransaction resolves the era entry point from the active
// protocol parameters and routes tx through it.
func routeVectorTransaction(
	entries []eraEntryPoint,
	observer *observedLedgerState,
	tx common.Transaction,
	slot uint64,
	pp common.ProtocolParameters,
	eventIndex int,
) (entryPointRouting, error) {
	major, err := protocolMajorVersion(pp)
	if err != nil {
		return entryPointRouting{}, fmt.Errorf(
			"event %d: resolve protocol major version: %w",
			eventIndex,
			err,
		)
	}
	entry, ok := entryPointForProtocolVersion(entries, major)
	if !ok {
		return entryPointRouting{}, fmt.Errorf(
			"event %d: no era covers protocol major version %d",
			eventIndex,
			major,
		)
	}
	if entry.Name != entryPointCorpusDecodeEra {
		// decodeVectorTransaction decodes the corpus as Conway. A vector whose
		// parameters place it in another era would be handed to that era's
		// entry point as a Conway transaction, so fail loudly instead of
		// reporting coverage the run does not have.
		return entryPointRouting{}, fmt.Errorf(
			"event %d: protocol major version %d selects era %s but the corpus is decoded as %s; add a decoder for %s before claiming its entry point is covered",
			eventIndex,
			major,
			entry.Name,
			entryPointCorpusDecodeEra,
			entry.Name,
		)
	}
	return routeTransaction(
		entry,
		entryPointFuncName(entry),
		tx,
		slot,
		observer,
		pp,
		eventIndex,
	), nil
}

// rollbackEntryPointReplay mirrors the shared harness's rollback: reset,
// reload the vector's initial state, and re-apply the journaled transactions
// at or below the target slot. Re-applied transactions are not routed again;
// they already produced their evidence on first execution.
//
// pp is the vector's initial protocol parameters. replayVectorEntryPoints
// refuses a rollback that follows an epoch boundary, so the parameters still
// active here are the ones LoadForVector produced, which is what the harness
// restores explicitly from its own initialProtocolParams.
func rollbackEntryPointReplay(
	sm *DingoStateManager,
	initialState *conformance.ParsedInitialState,
	pp common.ProtocolParameters,
	applied []appliedTx,
	targetSlot uint64,
) ([]appliedTx, error) {
	retained := make([]appliedTx, 0, len(applied))
	for _, entry := range applied {
		if entry.slot <= targetSlot {
			retained = append(retained, entry)
		}
	}
	if err := sm.Reset(); err != nil {
		return nil, fmt.Errorf("reset: %w", err)
	}
	if err := sm.LoadInitialState(initialState, pp); err != nil {
		return nil, fmt.Errorf("reload initial state: %w", err)
	}
	for _, entry := range retained {
		if err := sm.ApplyTransaction(entry.tx, entry.slot); err != nil {
			return nil, fmt.Errorf("replay slot %d: %w", entry.slot, err)
		}
	}
	return retained, nil
}

// entryPointCorpusDecodeEra is the era decodeVectorTransaction decodes as.
const entryPointCorpusDecodeEra = conway.EraNameConway

// decodeVectorTransaction decodes a vector transaction. The corpus is Conway,
// and the shared harness decodes it the same way; routeVectorTransaction
// cross-checks the decoded era against the era the vector's protocol
// parameters select, so a future corpus in another era cannot pass unnoticed.
func decodeVectorTransaction(txBytes []byte) (common.Transaction, error) {
	tx := &conway.ConwayTransaction{}
	if _, err := cbor.Decode(txBytes, tx); err != nil {
		return nil, err
	}
	return tx, nil
}

// entryPointEraList is the era table these tests cover. Dijkstra is included
// deliberately: it is off by default at runtime but its ValidateTxFunc is a
// production entry point, and per-era rule duplication means an entry point
// that is only covered for Conway proves nothing about the others.
func entryPointEraList() []eras.EraDesc {
	return eras.ActiveEras(true)
}

// entryPointCorpusRun is the memoized entry-point replay. It is a second
// replay of the corpus, separate from sqliteCorpusResults: the shared
// ouroboros-mock harness validates with its own upstream rule list and offers
// no hook for a caller-supplied validator, so there is no way to observe
// Dingo's entry points from inside the harness pass. corpus_test.go's
// "replay once per backend" reasoning still holds for storage-dialect
// coverage; what this pass buys is different, and is not obtainable from the
// harness replay at any count.
//
// The cost is the state replay, not the validation. Running this pass with
// the entry-point call removed takes the same wall clock as running it with
// the call in place, so eras.ValidateTx* is free at this corpus size; what
// the pass pays for is a second Reset/LoadInitialState/ApplyTransaction pass
// over the corpus, which is roughly what the harness replay itself costs.
// Measured on one machine, the package went from 585s to 891s under -race.
// It runs against SQLite only -- the Postgres and MySQL replays exist for
// storage-dialect coverage, and the entry points do not vary by backend.
//
// It replays once per process, like sqliteCorpusResults, so a build that adds
// more consumers of this evidence does not add more replays.
type entryPointCorpusRun struct {
	err      error
	entries  []eraEntryPoint
	evidence []vectorEntryPointEvidence
}

var (
	entryPointCorpusOnce sync.Once
	entryPointCorpusData entryPointCorpusRun
)

func entryPointCorpusEvidence(t *testing.T) entryPointCorpusRun {
	t.Helper()
	entryPointCorpusOnce.Do(func() {
		entries, err := dingoEraEntryPoints(entryPointEraList())
		if err != nil {
			entryPointCorpusData = entryPointCorpusRun{err: err}
			return
		}
		root, err := corpusTestdataRoot()
		if err != nil {
			entryPointCorpusData = entryPointCorpusRun{err: err}
			return
		}
		sm, err := NewDingoStateManager()
		if err != nil {
			entryPointCorpusData = entryPointCorpusRun{
				err: fmt.Errorf("new sqlite state manager: %w", err),
			}
			return
		}
		defer sm.Close()
		evidence, err := replayEntryPoints(sm, root, entries)
		entryPointCorpusData = entryPointCorpusRun{
			err:      err,
			entries:  entries,
			evidence: evidence,
		}
	})
	require.NoError(t, entryPointCorpusData.err, "entry point corpus replay")
	return entryPointCorpusData
}

// TestDingoEraRegistryExposesValidationEntryPoints fails when any era's
// production transaction-validation entry point is missing from the registry.
//
// A nil ValidateTxFunc is the cheapest way to bypass validation for an era,
// and nothing else in the conformance package notices: the shared harness
// never reads the registry.
func TestDingoEraRegistryExposesValidationEntryPoints(t *testing.T) {
	entries, err := dingoEraEntryPoints(entryPointEraList())
	require.NoError(t, err)
	require.Len(t, entries, len(entryPointEraList()))

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name)
	}
	t.Logf("era validation entry points: %v", names)

	// The registry must cover every protocol major version the corpus can
	// select, without a gap between adjacent eras.
	for i := 1; i < len(entries); i++ {
		require.Equal(
			t,
			entries[i-1].MaxMajorVersion+1,
			entries[i].MinMajorVersion,
			"protocol major version gap between %s and %s leaves versions with no validation entry point",
			entries[i-1].Name,
			entries[i].Name,
		)
	}
}

// TestDingoEraEntryPointsReportsMissingValidator proves the registry check
// above detects the state it exists to catch. Without it,
// TestDingoEraRegistryExposesValidationEntryPoints would assert something
// that is true of any table, including one whose entry points were removed.
func TestDingoEraEntryPointsReportsMissingValidator(t *testing.T) {
	eraList := entryPointEraList()
	require.NotEmpty(t, eraList)

	bypassed := make([]eras.EraDesc, len(eraList))
	copy(bypassed, eraList)
	bypassed[len(bypassed)-1].ValidateTxFunc = nil

	_, err := dingoEraEntryPoints(bypassed)
	require.ErrorContains(
		t,
		err,
		"no production validation entry point",
		"an era with no validation entry point must be reported",
	)
	require.ErrorContains(t, err, eraList[len(eraList)-1].Name)

	_, err = dingoEraEntryPoints(nil)
	require.Error(t, err, "an empty era registry must be reported")
}

// TestConformanceVectorsExerciseDingoEraEntryPoints routes every corpus
// vector through Dingo's production validation entry point for its era and
// asserts, per vector, that the entry point actually executed against ledger
// state derived from that vector's transactions.
//
// TestRulesConformanceVectors cannot make this assertion: it reports the
// shared harness's verdict, which is produced by upstream gouroboros rules
// and stays green with ValidateTxConway stubbed out entirely.
func TestConformanceVectorsExerciseDingoEraEntryPoints(t *testing.T) {
	run := entryPointCorpusEvidence(t)
	require.NotEmpty(
		t,
		run.evidence,
		"corpus replay produced no vectors; an empty corpus would otherwise "+
			"report as full entry-point coverage",
	)

	reportEntryPointCoverage(t, run)

	var routedVectors int
	for _, ev := range run.evidence {
		t.Run(ev.Path, func(t *testing.T) {
			require.NoError(t, ev.Err, "vector %s: %s", ev.Path, ev.Title)
			require.Len(
				t,
				ev.Routings,
				ev.TxEvents,
				"vector %s routed %d of %d transaction events through a "+
					"production entry point",
				ev.Path,
				len(ev.Routings),
				ev.TxEvents,
			)
			for _, routing := range ev.Routings {
				require.NoErrorf(
					t,
					entryPointExecutionFault(routing),
					"vector %s (%s) event %d",
					ev.Path,
					ev.Title,
					routing.EventIndex,
				)
			}
		})
		if len(ev.Routings) > 0 {
			routedVectors++
		}
	}

	require.Positive(
		t,
		routedVectors,
		"no vector routed a transaction through a production validation "+
			"entry point",
	)
}

// reportEntryPointCoverage logs which eras the corpus actually reached, so an
// aggregate pass cannot be read as covering eras the corpus never touches.
func reportEntryPointCoverage(t *testing.T, run entryPointCorpusRun) {
	t.Helper()
	perEra := make(map[string]int)
	var routings int
	for _, ev := range run.evidence {
		for _, routing := range ev.Routings {
			perEra[routing.EntryPoint]++
			routings++
		}
	}
	eraNames := make([]string, 0, len(perEra))
	for name := range perEra {
		eraNames = append(eraNames, name)
	}
	sort.Strings(eraNames)

	t.Logf("Dingo validation entry point coverage (sqlite):")
	t.Logf("  Vectors replayed: %d", len(run.evidence))
	t.Logf("  Transactions routed: %d", routings)
	for _, name := range eraNames {
		t.Logf("  %s: %d transactions", name, perEra[name])
	}
	for _, entry := range run.entries {
		if perEra[entryPointFuncName(entry)] == 0 {
			t.Logf(
				"  %s: 0 transactions (no %s vectors in this corpus; covered "+
					"by TestDingoEraEntryPointsRejectInputlessTransaction only)",
				entryPointFuncName(entry),
				entry.Name,
			)
		}
	}
}

// TestEntryPointExecutionFaultDetectsBypassedValidator proves the detector
// used by TestConformanceVectorsExerciseDingoEraEntryPoints actually
// discriminates: it accepts the production entry point and rejects both a
// no-op validator and one that returns the vector fixture's own verdict
// without consulting ledger state.
//
// Without this, the coverage assertion above would be unfalsifiable, which is
// the same failure mode as the aggregate pass rate it exists to backstop.
func TestEntryPointExecutionFaultDetectsBypassedValidator(t *testing.T) {
	root, err := corpusTestdataRoot()
	require.NoError(t, err)
	entries, err := dingoEraEntryPoints(entryPointEraList())
	require.NoError(t, err)

	sm, err := NewDingoStateManager()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	probe := loadEntryPointProbeTransaction(t, root, sm)
	observer := newObservedLedgerState(NewDingoStateProvider(sm))
	major, err := protocolMajorVersion(probe.pp)
	require.NoError(t, err)
	production, ok := entryPointForProtocolVersion(entries, major)
	require.True(t, ok, "no era covers protocol major version %d", major)

	route := func(validate validateTxFunc) entryPointRouting {
		entry := production
		entry.Validate = validate
		return routeTransaction(
			entry,
			entryPointFuncName(production),
			probe.tx,
			probe.slot,
			observer,
			probe.pp,
			0,
		)
	}

	t.Run("production entry point is accepted", func(t *testing.T) {
		routing := route(production.Validate)
		require.NoError(t, entryPointExecutionFault(routing))
		require.Positive(
			t,
			routing.LookedUpInputs,
			"%s must resolve the transaction's declared inputs",
			entryPointFuncName(production),
		)
	})

	t.Run("no-op validator is detected", func(t *testing.T) {
		routing := route(func(
			common.Transaction,
			uint64,
			common.LedgerState,
			common.ProtocolParameters,
		) error {
			return nil
		})
		require.Error(
			t,
			entryPointExecutionFault(routing),
			"a validator that accepts everything without reading state must "+
				"be reported as a bypassed validation path",
		)
	})

	t.Run("fixture-only verdict is detected", func(t *testing.T) {
		// The worst case for an outcome-based check: a validator that returns
		// exactly the verdict the vector fixture declares. Every accept/reject
		// comparison against the corpus would agree with it.
		routing := route(func(
			common.Transaction,
			uint64,
			common.LedgerState,
			common.ProtocolParameters,
		) error {
			if probe.expectSuccess {
				return nil
			}
			return errors.New("vector fixture says this transaction fails")
		})
		require.Error(
			t,
			entryPointExecutionFault(routing),
			"a verdict copied from the vector fixture must be reported as a "+
				"bypassed validation path",
		)
	})

	t.Run(
		"rejecting validator that reads no state is detected",
		func(t *testing.T) {
			routing := route(func(
				common.Transaction,
				uint64,
				common.LedgerState,
				common.ProtocolParameters,
			) error {
				return errors.New("rejected without looking")
			})
			require.Error(
				t,
				entryPointExecutionFault(routing),
				"returning an error is not evidence the validation path ran",
			)
		},
	)
}

// entryPointProbe is a single real corpus transaction plus the state it was
// loaded against, used to exercise the detector.
type entryPointProbe struct {
	tx            common.Transaction
	pp            common.ProtocolParameters
	path          string
	slot          uint64
	expectSuccess bool
}

// loadEntryPointProbeTransaction loads the first corpus vector carrying a
// transaction with at least one declared input, and leaves sm holding that
// vector's initial state. Using real vector data rather than a constructed
// transaction is deliberate: the detector must be shown to work on the same
// input the coverage assertion runs on.
func loadEntryPointProbeTransaction(
	t *testing.T,
	root string,
	sm *DingoStateManager,
) entryPointProbe {
	t.Helper()
	paths, err := collectEntryPointVectors(root)
	require.NoError(t, err)
	loader := conformance.NewPParamsLoaderFromTestdata(root)

	for _, path := range paths {
		vector, err := conformance.DecodeTestVector(path)
		if err != nil {
			continue
		}
		initialState, err := conformance.ParseInitialState(vector.InitialState)
		if err != nil {
			continue
		}
		pp, err := loader.LoadForVector(vector, initialState)
		if err != nil {
			continue
		}
		for _, event := range vector.Events {
			if event.Type != conformance.EventTypeTransaction {
				continue
			}
			tx, err := decodeVectorTransaction(event.TxBytes)
			if err != nil || len(tx.Inputs()) == 0 {
				continue
			}
			require.NoError(t, sm.Reset())
			require.NoError(t, sm.LoadInitialState(initialState, pp))
			return entryPointProbe{
				tx:            tx,
				pp:            pp,
				path:          filepath.Base(path),
				slot:          event.Slot,
				expectSuccess: event.Success,
			}
		}
	}
	t.Fatal("no corpus vector carries a transaction with declared inputs")
	return entryPointProbe{}
}

// TestDingoEraEntryPointsRejectInputlessTransaction covers every era in the
// registry, not just the era the corpus happens to contain.
//
// The corpus is Conway-only, and validation rules are duplicated per era, so
// Conway coverage says nothing about ValidateTxShelley or ValidateTxDijkstra.
// A transaction with no inputs is invalid in every era (Byron's own
// InputSetEmpty rule, and UtxoValidateInputSetEmptyUtxo from Shelley onward),
// which makes it a rule the whole table can be held to. The paired no-op
// assertion is what makes this a detector rather than a restatement: the same
// input is accepted by a validator that does nothing.
func TestDingoEraEntryPointsRejectInputlessTransaction(t *testing.T) {
	entries, err := dingoEraEntryPoints(entryPointEraList())
	require.NoError(t, err)

	sm, err := NewDingoStateManager()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })
	observer := newObservedLedgerState(NewDingoStateProvider(sm))

	probes := inputlessEraProbes()
	for _, entry := range entries {
		t.Run(entry.Name, func(t *testing.T) {
			probe, ok := probes[entry.Name]
			require.Truef(
				t,
				ok,
				"era %s has no input-less transaction probe; add one so its "+
					"production entry point is covered",
				entry.Name,
			)
			require.Empty(t, probe.tx.Inputs())

			routing := routeTransaction(
				entry,
				entryPointFuncName(entry),
				probe.tx,
				0,
				observer,
				probe.pp,
				0,
			)
			require.NoErrorf(
				t,
				entryPointExecutionFault(routing),
				"%s accepted a transaction with no inputs",
				entryPointFuncName(entry),
			)

			bypassed := entry
			bypassed.Validate = func(
				common.Transaction,
				uint64,
				common.LedgerState,
				common.ProtocolParameters,
			) error {
				return nil
			}
			bypassedRouting := routeTransaction(
				bypassed,
				entryPointFuncName(entry),
				probe.tx,
				0,
				observer,
				probe.pp,
				0,
			)
			require.Errorf(
				t,
				entryPointExecutionFault(bypassedRouting),
				"a no-op replacement for %s must be detected",
				entryPointFuncName(entry),
			)
		})
	}
}

// eraProbe is an era-appropriate transaction and the protocol parameters its
// entry point requires.
type eraProbe struct {
	tx common.Transaction
	pp common.ProtocolParameters
}

// inputlessEraProbes returns one input-less transaction per era, keyed by era
// name.
//
// From Shelley onward each entry point type-asserts its own parameter type,
// so the parameters have to match or the entry point returns
// eras.ErrIncompatibleProtocolParams before reaching any rule -- which would
// satisfy the input-less assertion for the wrong reason.
//
// Byron is the exception and carries nil: ValidateTxByron never asserts on
// pp, and gouroboros has no Byron protocol-parameters type to supply. It runs
// its structural rules unconditionally (byronValidateInputsNotEmpty is what
// rejects the probe) and its UTxO-aware rules whenever a ledger state is
// given, passing pp through to rules that ignore it.
func inputlessEraProbes() map[string]eraProbe {
	return map[string]eraProbe{
		byron.EraNameByron: {
			tx: &byron.ByronTransaction{},
			pp: nil,
		},
		shelley.EraNameShelley: {
			tx: &shelley.ShelleyTransaction{},
			pp: &shelley.ShelleyProtocolParameters{},
		},
		allegra.EraNameAllegra: {
			tx: &allegra.AllegraTransaction{},
			pp: &allegra.AllegraProtocolParameters{},
		},
		mary.EraNameMary: {
			tx: &mary.MaryTransaction{},
			pp: &mary.MaryProtocolParameters{},
		},
		alonzo.EraNameAlonzo: {
			tx: &alonzo.AlonzoTransaction{},
			pp: &alonzo.AlonzoProtocolParameters{},
		},
		babbage.EraNameBabbage: {
			tx: &babbage.BabbageTransaction{},
			pp: &babbage.BabbageProtocolParameters{},
		},
		conway.EraNameConway: {
			tx: &conway.ConwayTransaction{},
			pp: &conway.ConwayProtocolParameters{},
		},
		dijkstra.EraNameDijkstra: {
			tx: &dijkstra.DijkstraTransaction{},
			pp: &dijkstra.DijkstraProtocolParameters{},
		},
	}
}

// This package builds in two configurations, and process-level teardown differs
// between them: the dingo_extra_plugins build additionally owns a Postgres
// schema, a MySQL database, and their paired blob directories. That previously
// meant two TestMain functions selected by build tag, each with its own cleanup
// chain and its own copy of processCleanupExitCode.
//
// That split is what this replaces. Two entry points had to be kept in step by
// hand, a change to one silently did not apply to the other, and only the
// tagged copy of the exit-code helper had a test -- so the untagged build could
// have regressed without any run noticing. There is now one TestMain, compiled
// in both configurations, and the tag-specific teardown registers itself.

// processCleanups holds teardown that must run once after every test in the
// process, in registration order. A build configuration contributes to it from
// an init function, so TestMain itself needs no build tags and cannot drift
// between configurations.
var processCleanups []func() error

// registerProcessCleanup adds fn to the process teardown chain. Call it from an
// init function in a build-tagged file.
func registerProcessCleanup(fn func() error) {
	processCleanups = append(processCleanups, fn)
}

// runProcessCleanups runs every registered process teardown in registration
// order and reports whether any of them failed.
//
// Every step always runs: a failure is logged and recorded rather than
// returning early, so one failed drop or removal never leaves a sibling
// resource uncleaned as a side effect -- for example the blob directory paired
// with a schema that failed to drop. TestMain is the only caller that runs it
// against the real chain; TestProcessCleanupChainRunsEveryStepAfterAFailure
// calls this same function against a substituted chain so the guarantee is
// tested where it is implemented rather than in a copy of the loop.
func runProcessCleanups() bool {
	cleanupFailed := false
	for _, cleanup := range processCleanups {
		if err := cleanup(); err != nil {
			log.Printf("conformance: process cleanup: %v", err)
			cleanupFailed = true
		}
	}
	return cleanupFailed
}

// TestMain runs every registered process teardown after the tests finish, then
// removes the shared vector extraction (see corpusTestdataRoot). The vector
// extraction removal runs even when a registered cleanup failed, and any
// failure is reflected in the exit code.
func TestMain(m *testing.M) {
	code := m.Run()

	cleanupFailed := runProcessCleanups()
	if err := cleanupCorpusTestdata(); err != nil {
		log.Printf("conformance: remove shared vector extraction: %v", err)
		cleanupFailed = true
	}

	os.Exit(processCleanupExitCode(code, cleanupFailed))
}

// processCleanupExitCode folds a process-cleanup failure into the test run's
// own exit code. A cleanup failure must fail the run even when every test
// passed -- otherwise the leaked schema, database, or directory this cleanup
// exists to remove is invisible to anything that only checks the exit code (CI,
// a local `go test && echo ok`). A real test failure's exit code is never
// downgraded, only upgraded from 0.
func processCleanupExitCode(testExitCode int, cleanupFailed bool) int {
	if cleanupFailed && testExitCode == 0 {
		return 1
	}
	return testExitCode
}

// TestProcessCleanupExitCodeFailsOnCleanupFailure proves a process-cleanup
// failure makes TestMain report a nonzero exit code even when every test in the
// process passed -- a reviewer's forced RemoveAll permission failure otherwise
// logged "permission denied" but left `go test` exiting 0, silently leaking the
// per-run schema, database, or directory this cleanup exists to remove.
//
// This test is deliberately untagged, so it covers both build configurations.
// Previously it lived beside the tagged TestMain and the untagged build carried
// an uncovered copy of the helper.
func TestProcessCleanupExitCodeFailsOnCleanupFailure(t *testing.T) {
	require.Equal(t, 0, processCleanupExitCode(0, false))
	require.Equal(t, 1, processCleanupExitCode(0, true))
	require.Equal(t, 2, processCleanupExitCode(2, true))
	require.Equal(t, 2, processCleanupExitCode(2, false))
}

// TestProcessCleanupChainRunsEveryStepAfterAFailure proves one failing cleanup
// does not skip the ones registered after it, and that the failure still
// reaches the exit code. The ordering guarantee is the point: the Postgres and
// MySQL teardowns each drop a remote resource and then remove the local blob
// directory paired with it.
//
// It calls runProcessCleanups, the function TestMain calls, against a
// substituted chain, so a future early return on the first cleanup error fails
// this test.
func TestProcessCleanupChainRunsEveryStepAfterAFailure(t *testing.T) {
	before := processCleanups
	t.Cleanup(func() { processCleanups = before })
	processCleanups = nil

	var ran []string
	registerProcessCleanup(func() error {
		ran = append(ran, "first")
		return os.ErrPermission
	})
	registerProcessCleanup(func() error {
		ran = append(ran, "second")
		return nil
	})

	cleanupFailed := runProcessCleanups()

	require.Equal(t, []string{"first", "second"}, ran)
	require.True(t, cleanupFailed)
	require.Equal(t, 1, processCleanupExitCode(0, cleanupFailed))
}
