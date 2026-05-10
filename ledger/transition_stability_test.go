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

package ledger

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stabilityFixtureEpoch parameters: Shelley-style 432_000-slot epoch,
// safeZone = ceil(3*432/0.05) = 25_920, voting deadline distance from
// epoch end is 2 * safeZone = 51_840. So an epoch ending at slot
// 532_000 has its voting deadline at slot 480_160.
const (
	stabilityFixtureEpochID    uint64 = 500
	stabilityFixtureEpochStart uint64 = 100_000
	stabilityFixtureEpochLen   uint   = 432_000
	stabilityFixtureEpochEnd   uint64 = stabilityFixtureEpochStart +
		uint64(stabilityFixtureEpochLen)
	stabilityFixtureVotingDeadline uint64 = stabilityFixtureEpochEnd - 2*25_920
)

// stabilityFixtureLedgerState assembles a LedgerState wired with
// real-shaped Shelley genesis (so calculateStabilityWindowForEra returns
// the expected 25_920) and an in-memory DB. The caller seeds proposal /
// vote rows on db, sets currentTip and transitionInfo, and invokes
// evaluateHardForkInitiationStability.
//
// Setting currentPParams to Conway pparams with the supplied major
// version exercises the post-Conway code path inside the helper.
// Bootstrap (major <= 9) uses the simplest ratification semantics
// (single yes vote suffices) so a typical test only needs one DRep
// fixture.
func stabilityFixtureLedgerState(
	t *testing.T,
	major uint,
) (*LedgerState, *database.Database) {
	t.Helper()
	db := newTestDB(t)
	pparams := mockledger.NewMockConwayProtocolParams()
	pparams.ProtocolVersion.Major = major
	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		currentEpoch:   newTestEpoch(stabilityFixtureEpochID, stabilityFixtureEpochStart, stabilityFixtureEpochLen, eras.ConwayEraDesc.Id),
		currentPParams: &pparams,
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	return ls, db
}

// seedRatifiableBootstrapHardForkInitiation primes the DB so that the
// governance ratifiability helper returns a non-nil result when the
// bootstrap (major<=9) ratification rule is in effect — a HardForkInitiation
// proposal in the active set plus a single DRep yes vote.
func seedRatifiableBootstrapHardForkInitiation(
	t *testing.T,
	db *database.Database,
	currentEpoch uint64,
	targetMajor uint,
) *models.GovernanceProposal {
	t.Helper()
	action := &lcommon.HardForkInitiationGovAction{Type: 1}
	action.ProtocolVersion.Major = targetMajor
	action.ProtocolVersion.Minor = 0
	cborBytes, err := cbor.Encode(action)
	require.NoError(t, err)

	proposal := &models.GovernanceProposal{
		TxHash:        repeatByte(32, 0xAA),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: currentEpoch - 1,
		ExpiresEpoch:  currentEpoch + 10,
		Deposit:       1_000,
		ReturnAddress: repeatByte(29, 0),
		AnchorURL:     "https://example.invalid/anchor",
		AnchorHash:    repeatByte(32, 0xEE),
		GovActionCbor: cborBytes,
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	loaded, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, loaded)

	drepCred := repeatByte(28, 0xBB)
	stakeCred := repeatByte(28, 0xCC)
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
		TxId:       repeatByte(32, 0x01),
		OutputIdx:  0,
		StakingKey: stakeCred,
		AddedSlot:  1,
		Amount:     types.Uint64(1_000),
	}))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeDRep,
		VoterCredential: drepCred,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
	return loaded
}

func repeatByte(length int, b byte) []byte {
	out := make([]byte, length)
	for i := range out {
		out[i] = b
	}
	return out
}

// TestEvaluateHardForkInitiationStability_PreDeadline_NoChange pins the
// "votes can still flip the outcome" guard: while the current tip is
// before the voting deadline (epochEnd - 2*stabilityWindow), the helper
// must not surface the upcoming transition even if the in-flight
// proposal currently meets thresholds — a yet-to-arrive No vote could
// still defeat it.
func TestEvaluateHardForkInitiationStability_PreDeadline_NoChange(t *testing.T) {
	ls, db := stabilityFixtureLedgerState(t, 9 /* bootstrap */)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	// Tip one slot before the voting deadline.
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(stabilityFixtureVotingDeadline-1, []byte("tip")),
	}

	ls.evaluateHardForkInitiationStability()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"pre-deadline must not promote to TransitionKnown")
}

// TestEvaluateHardForkInitiationStability_PostDeadline_Ratifiable_SetsKnown
// is the core happy-path: after the voting deadline, with a ratifiable
// HardForkInitiation in flight, the helper must set TransitionKnown for
// the epoch the boundary will fire (currentEpoch + 1).
func TestEvaluateHardForkInitiationStability_PostDeadline_Ratifiable_SetsKnown(t *testing.T) {
	ls, db := stabilityFixtureLedgerState(t, 9 /* bootstrap */)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(stabilityFixtureVotingDeadline, []byte("tip")),
	}

	ls.evaluateHardForkInitiationStability()

	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State,
		"post-deadline + ratifiable must set TransitionKnown")
	assert.Equal(t, stabilityFixtureEpochID+1, ls.transitionInfo.KnownEpoch,
		"target epoch is the next epoch boundary")
}

// TestEvaluateHardForkInitiationStability_PostDeadline_NotRatifiable_NoChange
// pins the negative side: post-deadline without a ratifiable proposal,
// transitionInfo stays Unknown. (No proposal seeded; helper returns nil.)
func TestEvaluateHardForkInitiationStability_PostDeadline_NotRatifiable_NoChange(t *testing.T) {
	ls, _ := stabilityFixtureLedgerState(t, 9)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(stabilityFixtureVotingDeadline+5_000, []byte("tip")),
	}

	ls.evaluateHardForkInitiationStability()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"no ratifiable proposal means transitionInfo stays Unknown")
}

// TestEvaluateHardForkInitiationStability_PreConwayPParams_NoOp pins the
// short-circuit when the chain is pre-Conway: no governance state
// machine exists, so the helper must not even attempt the DB lookup
// (and certainly must not promote transitionInfo).
func TestEvaluateHardForkInitiationStability_PreConwayPParams_NoOp(t *testing.T) {
	ls, db := stabilityFixtureLedgerState(t, 9)
	// Even with a "ratifiable" proposal seeded, swapping pparams to nil
	// (or any non-Conway type) makes the helper short-circuit before
	// it hits the proposal store.
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentPParams = nil
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(stabilityFixtureVotingDeadline+5_000, []byte("tip")),
	}

	ls.evaluateHardForkInitiationStability()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"pre-Conway pparams must short-circuit without promotion")
}

// TestEvaluateHardForkInitiationStability_AlreadyKnownForSameEpoch_Idempotent
// pins the short-circuit when transitionInfo already reports the same
// upcoming boundary. The function must not redundantly mutate state
// (a redundant mutation is harmless to behaviour but makes per-block
// invocations noisier than necessary).
func TestEvaluateHardForkInitiationStability_AlreadyKnownForSameEpoch_Idempotent(t *testing.T) {
	ls, db := stabilityFixtureLedgerState(t, 9)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(stabilityFixtureVotingDeadline+5_000, []byte("tip")),
	}
	ls.transitionInfo = hardfork.NewTransitionKnown(stabilityFixtureEpochID + 1)

	ls.evaluateHardForkInitiationStability()

	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	assert.Equal(t, stabilityFixtureEpochID+1, ls.transitionInfo.KnownEpoch)
}

// TestEvaluateHardForkInitiationStability_PreservesKnownFromOtherSource
// pins the deference to higher-priority sources of TransitionKnown.
// Both evaluateTriggerAtEpoch (test override) and reconstructTransitionInfo
// (pparams-bump detection) may set Known for a specific epoch. The
// mid-epoch governance detector must not clobber that decision even if
// it would otherwise fire, so on-chain HFI ratifiability cannot
// override an operator-configured TestXHardForkAtEpoch boundary.
func TestEvaluateHardForkInitiationStability_PreservesKnownFromOtherSource(t *testing.T) {
	ls, db := stabilityFixtureLedgerState(t, 9)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(stabilityFixtureVotingDeadline+5_000, []byte("tip")),
	}
	const externalTargetEpoch = stabilityFixtureEpochID + 7
	ls.transitionInfo = hardfork.NewTransitionKnown(externalTargetEpoch)

	ls.evaluateHardForkInitiationStability()

	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	assert.Equal(t, uint64(externalTargetEpoch), ls.transitionInfo.KnownEpoch,
		"a Known target set elsewhere must not be overwritten by mid-epoch detection")
}

// TestEvaluateHardForkInitiationStability_IntraEraHFI_DoesNotSetKnown
// pins the era-boundary gate: TransitionKnown signals an upcoming era
// transition, not just any pparams bump. A HardForkInitiation that
// proposes a new ProtocolVersion still inside the current era's
// version range (e.g. Plomin's pv9 → pv10, both Conway) is an
// intra-era bump and must not be surfaced as TransitionKnown — clients
// would otherwise see era-history responses claiming the era ends at
// epoch+1 when in fact the era continues.
//
// The check matches the era-filter the boundary path's
// IsHardForkTransition applies, so mid-epoch detection and the
// boundary's enactment dispatch agree on what counts as a transition.
func TestEvaluateHardForkInitiationStability_IntraEraHFI_DoesNotSetKnown(t *testing.T) {
	ls, db := stabilityFixtureLedgerState(t, 9 /* Conway, bootstrap */)
	// Target major 10 — still in Conway (Conway covers pv9-pv10).
	// A ratifiable proposal here represents an intra-era pparams
	// bump, not an era transition.
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 10,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(stabilityFixtureVotingDeadline+5_000, []byte("tip")),
	}

	ls.evaluateHardForkInitiationStability()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"intra-era HardForkInitiation must not be surfaced as TransitionKnown")
}

// TestEvaluateHardForkInitiationStability_UpgradesImpossibleToKnown pins
// the priority order: TransitionKnown is strictly more informative than
// TransitionImpossible (the latter only says "no transition this epoch
// before safe-zone end", the former says "transition will happen at
// epoch+1"). When both could apply, Known wins.
func TestEvaluateHardForkInitiationStability_UpgradesImpossibleToKnown(t *testing.T) {
	ls, db := stabilityFixtureLedgerState(t, 9)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(stabilityFixtureVotingDeadline+5_000, []byte("tip")),
	}
	ls.transitionInfo = hardfork.NewTransitionImpossible()

	ls.evaluateHardForkInitiationStability()

	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State,
		"a ratifiable proposal must upgrade Impossible to Known")
	assert.Equal(t, stabilityFixtureEpochID+1, ls.transitionInfo.KnownEpoch)
}
