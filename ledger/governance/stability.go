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
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
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
	DB                      ledgerdb.LedgerDB
	Txn                     ledgerdb.Txn
	CurrentEpoch            uint64
	DelegatorInactivityOn   bool
	PParams                 lcommon.ProtocolParameters
	ConwayGenesis           *conway.ConwayGenesis
	OnProposalDecodeFailure func(proposal *models.GovernanceProposal, err error)
}

func NewStabilityCheckInputs(
	db ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
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
// proposal along with the protocol version it would enact. It evaluates
// proposals in the boundary's priority order so an earlier delaying action
// prevents a later HardForkInitiation from being forecast.
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
// It is a prediction, not a preview of the boundary's own arithmetic. The
// SPO denominator it tallies against is mark[CurrentEpoch], while the
// boundary into CurrentEpoch+1 will tally mark[CurrentEpoch+1], a snapshot
// SNAP does not capture until that boundary runs -- see
// predictedBoundaryStakeEpochFor for why the two cannot be the same read and
// what a stake shift between them costs. Every other input here is likewise
// the current epoch's (active DReps, committee state, roots), where the
// boundary uses the new epoch's.
//
// Returns nil with no error when no HardForkInitiation is currently
// ratifiable, or when the chain is pre-Conway.
func EvaluateRatifiableHardForkInitiation(
	ctx context.Context,
	in StabilityCheckInputs,
) (*RatifiableHardForkInitiation, error) {
	if in.DB == nil {
		return nil, errors.New("nil database")
	}
	conwayPParams, err := conwayGovernanceProtocolParameters(in.PParams)
	if err != nil {
		return nil, err
	}
	if conwayPParams == nil {
		// Pre-Conway: no governance state machine exists, so no
		// HardForkInitiation can be in flight.
		return nil, nil
	}

	proposals, err := in.DB.GetActiveGovernanceProposals(
		ctx,
		in.CurrentEpoch, in.Txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get active proposals: %w", err)
	}

	hardForkPriority := govActionPriority(&models.GovernanceProposal{
		ActionType: uint8(lcommon.GovActionTypeHardForkInitiation),
	})
	forecastProposals := proposals[:0]
	hasHardFork := false
	for _, proposal := range proposals {
		if govActionPriority(proposal) > hardForkPriority {
			continue
		}
		forecastProposals = append(forecastProposals, proposal)
		if lcommon.GovActionType(proposal.ActionType) ==
			lcommon.GovActionTypeHardForkInitiation {
			hasHardFork = true
		}
	}
	if !hasHardFork {
		return nil, nil
	}
	proposals = forecastProposals
	sort.SliceStable(proposals, func(i, j int) bool {
		return govActionPriority(proposals[i]) <
			govActionPriority(proposals[j])
	})

	// Build the inputs ShouldRatify needs. The active-DRep, committee
	// state, parent-chain root, and quorum reads are the same the
	// boundary path performs; doing them again here is the cost of not
	// sharing with ProcessEpoch.
	tallyCtx := &TallyContext{
		DB:                    in.DB,
		Txn:                   in.Txn,
		StakeEpoch:            predictedBoundaryStakeEpochFor(in.CurrentEpoch),
		CurrentEpoch:          in.CurrentEpoch,
		MajorVersion:          conwayPParams.ProtocolVersion.Major,
		DelegatorInactivityOn: in.DelegatorInactivityOn,
	}

	activeDRepCount, err := countActiveDReps(
		ctx,
		in.DB,
		in.Txn,
		in.CurrentEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("count active dreps: %w", err)
	}

	committeeState, err := LoadCommitteeVotingState(
		ctx,
		in.DB, in.Txn, in.CurrentEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("load committee voting state: %w", err)
	}
	tallyCtx.CommitteeState = committeeState

	rootsByPurpose := make(
		map[govActionPurpose]*models.GovernanceProposal,
		len(chainedPurposes),
	)
	for _, purpose := range chainedPurposes {
		root, err := in.DB.GetLastEnactedGovernanceProposal(
			ctx,
			purposeActionTypes(purpose), in.Txn,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"get current root for purpose %d: %w", purpose, err,
			)
		}
		rootsByPurpose[purpose] = root
	}
	committeeNoConfidence := committeeNoConfidenceState(
		rootsByPurpose[purposeCommittee],
	)

	ccQuorum, err := conwayRatifyQuorum(
		ctx,
		nil, in.DB, in.Txn, in.ConwayGenesis,
	)
	if err != nil {
		return nil, fmt.Errorf("compute committee quorum: %w", err)
	}
	for _, proposal := range proposals {
		actionType := lcommon.GovActionType(proposal.ActionType)
		purpose := govActionPurposeOf(actionType)
		if !validateParentChain(proposal, rootsByPurpose[purpose]) {
			continue
		}
		// Decode before ratification so the same decoded-action-aware input
		// shape is used by this mid-epoch check and the boundary RATIFY loop.
		action, err := decodeGovAction(
			proposal.GovActionCbor, proposal.ActionType,
		)
		if err != nil {
			if in.OnProposalDecodeFailure != nil {
				in.OnProposalDecodeFailure(proposal, err)
			}
			continue
		}
		tally, err := TallyProposal(ctx, tallyCtx, proposal)
		if err != nil {
			return nil, fmt.Errorf("tally proposal: %w", err)
		}
		decision := ShouldRatify(RatifyInputs{
			Tally:           tally,
			PParams:         conwayPParams,
			GovAction:       action,
			CurrentEpoch:    in.CurrentEpoch,
			ActiveDRepCount: activeDRepCount,
			ActiveCCCount:   committeeState.ActiveMemberCount,
			CommitteeAbsent: committeeAbsent(
				rootsByPurpose[purposeCommittee], in.ConwayGenesis,
				committeeState.CommitteePresent,
			),
			CCQuorum:              ccQuorum,
			MajorVersion:          conwayPParams.ProtocolVersion.Major,
			CommitteeNoConfidence: committeeNoConfidence,
		})
		if !decision.Ratified {
			continue
		}
		if _, err := ratificationEnactmentPrecondition(
			in.PParams,
			nil,
			proposal,
			math.MaxUint64,
		); err != nil {
			continue
		}
		if actionType != lcommon.GovActionTypeHardForkInitiation {
			if isDelayingActionPurpose(purpose) {
				return nil, nil
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
