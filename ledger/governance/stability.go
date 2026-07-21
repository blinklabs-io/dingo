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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// StabilityCheckInputs collects the state needed to determine whether
// any active HardForkInitiation proposal would ratify if the boundary
// tick fired now. Inputs mirror those consumed by ProcessEpoch's
// RATIFY phase. The mid-epoch path duplicates the build instead of
// sharing because ProcessEpoch is the boundary path used on every
// mainnet rollover; refactoring it just to support mid-epoch evaluation
// would risk regressions for marginal benefit.
//
// OnProposalDecodeFailure, if non-nil, is invoked once per proposal
// whose stored CBOR fails to decode during the ratifiability scan. The
// callback owns side effects (logging, metrics) so the helper itself
// stays free of logger/metric dependencies.
type StabilityCheckInputs struct {
	DB                      *database.Database
	Txn                     *database.Txn
	CurrentEpoch            uint64
	DelegatorInactivityOn   bool
	PParams                 lcommon.ProtocolParameters
	ConwayGenesis           *conway.ConwayGenesis
	OnProposalDecodeFailure func(proposal *models.GovernanceProposal, err error)
}

func NewStabilityCheckInputs(
	db *database.Database,
	txn *database.Txn,
	currentEpoch uint64,
	delegatorInactivityOn bool,
	pparams lcommon.ProtocolParameters,
	conwayGenesis *conway.ConwayGenesis,
	onProposalDecodeFailure func(proposal *models.GovernanceProposal, err error),
) StabilityCheckInputs {
	return StabilityCheckInputs{
		DB:                      db,
		Txn:                     txn,
		CurrentEpoch:            currentEpoch,
		DelegatorInactivityOn:   delegatorInactivityOn,
		PParams:                 pparams,
		ConwayGenesis:           conwayGenesis,
		OnProposalDecodeFailure: onProposalDecodeFailure,
	}
}

// RatifiableHardForkInitiation describes a HardForkInitiation proposal
// whose tally currently meets ratification thresholds. NewMajor and
// NewMinor are the protocol version the action would enact at the next
// epoch boundary.
type RatifiableHardForkInitiation struct {
	Proposal *models.GovernanceProposal
	NewMajor uint
	NewMinor uint
}

// EvaluateRatifiableHardForkInitiation searches the active proposal set
// for a HardForkInitiation that would be marked Ratified by the
// boundary tick if it ran right now, and returns the first such
// proposal along with the protocol version it would enact.
//
// "Would ratify now" means: tally + ShouldRatify against the live vote
// state, current pparams, and the current snapshots of active DReps
// and committee membership.
//
// The result is intended for mid-epoch surfacing of upcoming era
// transitions to clients via TransitionInfo. Callers should gate use
// on the voting deadline (currentSlot >= epochEnd - 2*stabilityWindow);
// before that point, the answer can flip when new votes arrive.
//
// Returns nil with no error when no HardForkInitiation is currently
// ratifiable, or when the chain is pre-Conway.
func EvaluateRatifiableHardForkInitiation(
	in StabilityCheckInputs,
) (*RatifiableHardForkInitiation, error) {
	if in.DB == nil {
		return nil, errors.New("nil database")
	}
	conwayPParams, ok := in.PParams.(*conway.ConwayProtocolParameters)
	if !ok {
		// Pre-Conway: no governance state machine exists, so no
		// HardForkInitiation can be in flight.
		return nil, nil
	}

	proposals, err := in.DB.GetActiveGovernanceProposals(
		in.CurrentEpoch, in.Txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get active proposals: %w", err)
	}

	hardForkProposals := make([]*models.GovernanceProposal, 0)
	for _, p := range proposals {
		if lcommon.GovActionType(p.ActionType) ==
			lcommon.GovActionTypeHardForkInitiation {
			hardForkProposals = append(hardForkProposals, p)
		}
	}
	if len(hardForkProposals) == 0 {
		return nil, nil
	}

	// Build the inputs ShouldRatify needs. The active-DRep, committee
	// state, parent-chain root, and quorum reads are the same the
	// boundary path performs; doing them again here is the cost of not
	// sharing with ProcessEpoch.
	tallyCtx := &TallyContext{
		DB:                    in.DB,
		Txn:                   in.Txn,
		StakeEpoch:            stakeEpochFor(in.CurrentEpoch),
		CurrentEpoch:          in.CurrentEpoch,
		DelegatorInactivityOn: in.DelegatorInactivityOn,
	}

	activeDRepCount, err := countActiveDReps(in.DB, in.Txn, in.CurrentEpoch)
	if err != nil {
		return nil, fmt.Errorf("count active dreps: %w", err)
	}

	committeeState, err := LoadCommitteeVotingState(
		in.DB, in.Txn, in.CurrentEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("load committee voting state: %w", err)
	}
	tallyCtx.CommitteeState = committeeState

	committeeRoot, err := in.DB.GetLastEnactedGovernanceProposal(
		purposeActionTypes(purposeCommittee), in.Txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get committee root: %w", err)
	}
	committeeNoConfidence := committeeNoConfidenceState(committeeRoot)

	ccQuorum, err := conwayRatifyQuorum(
		nil, in.DB, in.Txn, in.ConwayGenesis,
	)
	if err != nil {
		return nil, fmt.Errorf("compute committee quorum: %w", err)
	}

	// Parent-chain root for HardForkInitiation lineage: a HardForkInitiation
	// must descend from the most recently enacted HardForkInitiation, or
	// from genesis if none has enacted yet.
	hardForkRoot, err := in.DB.GetLastEnactedGovernanceProposal(
		purposeActionTypes(purposeHardFork), in.Txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get hardfork root: %w", err)
	}

	for _, proposal := range hardForkProposals {
		if !validateParentChain(proposal, hardForkRoot) {
			continue
		}
		tally, err := TallyProposal(tallyCtx, proposal)
		if err != nil {
			return nil, fmt.Errorf("tally proposal: %w", err)
		}
		decision := ShouldRatify(RatifyInputs{
			Tally:                 tally,
			PParams:               conwayPParams,
			ParamUpdate:           nil, // not used for HardForkInitiation
			ActiveDRepCount:       activeDRepCount,
			ActiveCCCount:         committeeState.ActiveMemberCount,
			CCQuorum:              ccQuorum,
			MajorVersion:          conwayPParams.ProtocolVersion.Major,
			CommitteeNoConfidence: committeeNoConfidence,
		})
		if !decision.Ratified {
			continue
		}
		// Decode to extract the target protocol version; an active
		// HardForkInitiation that fails to decode here is a
		// data-integrity bug, but the conservative behaviour is to
		// skip it rather than abort the whole check.
		action, err := decodeGovAction(
			proposal.GovActionCbor, proposal.ActionType,
		)
		if err != nil {
			if in.OnProposalDecodeFailure != nil {
				in.OnProposalDecodeFailure(proposal, err)
			}
			continue
		}
		hf, ok := action.(*lcommon.HardForkInitiationGovAction)
		if !ok {
			continue
		}
		return &RatifiableHardForkInitiation{
			Proposal: proposal,
			NewMajor: hf.ProtocolVersion.Major,
			NewMinor: hf.ProtocolVersion.Minor,
		}, nil
	}
	return nil, nil
}
