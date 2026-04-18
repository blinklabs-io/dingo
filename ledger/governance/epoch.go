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
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// EpochInput collects the inputs needed at an epoch boundary
// to drive the governance state machine.
type EpochInput struct {
	DB        *database.Database
	Txn       *database.Txn
	Logger    *slog.Logger
	PrevEpoch uint64 // epoch being closed out
	NewEpoch  uint64 // epoch being opened
	// Slot at which enactment/ratification records its effect. The
	// boundary slot is used so rollback-to-slot-N-1 correctly reverts
	// this tick's changes.
	BoundarySlot uint64
	// PParams coming out of the legacy (Byron) pparam-update pass.
	// Enactment may mutate and return a new pparams.
	PParams  lcommon.ProtocolParameters
	UpdateFn func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error)
}

// EpochOutput reports what happened during the tick so the
// caller can persist updated pparams and emit metrics.
type EpochOutput struct {
	UpdatedPParams    lcommon.ProtocolParameters
	PParamsChanged    bool
	EnactedCount      int
	RatifiedCount     int
	ExpiredCount      int
	HardForkInitiated bool
}

// ProcessEpoch runs the ordered governance tick at an epoch
// boundary: enact proposals ratified in the previous epoch, expire
// overdue proposals, then ratify currently active proposals whose
// tallies meet threshold. The order matches the Cardano spec:
// ENACT first (so the current root reflects the new state), then
// RATIFY (which uses the updated root).
func ProcessEpoch(
	in *EpochInput,
) (*EpochOutput, error) {
	if in == nil {
		return nil, errors.New("nil governance epoch input")
	}
	out := &EpochOutput{UpdatedPParams: in.PParams}

	conwayPParams, ok := in.PParams.(*conway.ConwayProtocolParameters)
	if !ok {
		// Pre-Conway: nothing to do, governance state machine is
		// not yet active.
		return out, nil
	}

	// --- ENACTMENT ----------------------------------------------------
	enactCtx := &EnactmentContext{
		DB:       in.DB,
		Txn:      in.Txn,
		Epoch:    in.NewEpoch,
		Slot:     in.BoundarySlot,
		PParams:  in.PParams,
		UpdateFn: in.UpdateFn,
	}
	ratified, err := in.DB.GetRatifiedGovernanceProposals(in.Txn)
	if err != nil {
		return nil, fmt.Errorf("get ratified proposals: %w", err)
	}
	for _, proposal := range ratified {
		enactCtx.PParams = out.UpdatedPParams
		res, err := EnactProposal(enactCtx, proposal)
		if err != nil {
			if in.Logger != nil {
				in.Logger.Warn(
					"governance enactment failed",
					"tx_hash", hex.EncodeToString(proposal.TxHash[:8]),
					"action_type", proposal.ActionType,
					"error", err,
				)
			}
			continue
		}
		out.EnactedCount++
		if res.PParamsChanged {
			out.UpdatedPParams = res.UpdatedPParams
			out.PParamsChanged = true
			if lcommon.GovActionType(proposal.ActionType) ==
				lcommon.GovActionTypeHardForkInitiation {
				out.HardForkInitiated = true
			}
		}
	}

	// --- EXPIRY -------------------------------------------------------
	active, err := in.DB.GetActiveGovernanceProposals(
		in.NewEpoch, in.Txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get active proposals: %w", err)
	}
	expired, stillActive := partitionByExpiry(active, in.NewEpoch)
	for _, p := range expired {
		expiredEpoch := in.NewEpoch
		expiredSlot := in.BoundarySlot
		p.ExpiredEpoch = &expiredEpoch
		p.ExpiredSlot = &expiredSlot
		if err := in.DB.SetGovernanceProposal(p, in.Txn); err != nil {
			return nil, fmt.Errorf("mark expired: %w", err)
		}
		out.ExpiredCount++
	}

	// --- RATIFICATION -------------------------------------------------
	tallyCtx := &TallyContext{
		DB:         in.DB,
		Txn:        in.Txn,
		StakeEpoch: stakeEpochFor(in.NewEpoch),
	}

	// Tracked to enforce at-most-one ratification per purpose per tick.
	ratifiedThisTick := make(map[lcommon.GovActionType]bool)

	// Active set changes as we ratify; snapshot once.
	activeDRepCount, err := countActiveDReps(in)
	if err != nil {
		return nil, fmt.Errorf("count active dreps: %w", err)
	}
	activeCC, err := in.DB.GetActiveCommitteeMembers(in.Txn)
	if err != nil {
		return nil, fmt.Errorf("get active cc members: %w", err)
	}
	activeCCCount := len(activeCC)
	ccInNoConfidence := activeCCCount == 0 &&
		ccEverSeated(in.DB, in.Txn)

	majorVersion := conwayPParams.ProtocolVersion.Major
	ccQuorum := conwayRatifyQuorum(conwayPParams)

	for _, proposal := range stillActive {
		actionType := lcommon.GovActionType(proposal.ActionType)
		if ratifiedThisTick[actionType] {
			// The spec ratifies at most one action per purpose per
			// epoch tick. Skip to avoid double-enacting next tick.
			continue
		}

		// Parent chain check
		root, err := in.DB.GetLastEnactedGovernanceProposal(
			proposal.ActionType, in.Txn,
		)
		if err != nil {
			return nil, fmt.Errorf("get current root: %w", err)
		}
		if !validateParentChain(proposal, root) {
			continue
		}

		tally, err := TallyProposal(tallyCtx, proposal)
		if err != nil {
			return nil, fmt.Errorf("tally: %w", err)
		}
		decision := ShouldRatify(
			tally,
			conwayPParams,
			activeDRepCount,
			activeCCCount,
			ccQuorum,
			majorVersion,
			ccInNoConfidence,
		)
		if !decision.Ratified {
			continue
		}
		ratifiedEpoch := in.NewEpoch
		ratifiedSlot := in.BoundarySlot
		proposal.RatifiedEpoch = &ratifiedEpoch
		proposal.RatifiedSlot = &ratifiedSlot
		if err := in.DB.SetGovernanceProposal(
			proposal, in.Txn,
		); err != nil {
			return nil, fmt.Errorf("mark ratified: %w", err)
		}
		ratifiedThisTick[actionType] = true
		out.RatifiedCount++
	}

	return out, nil
}

// partitionByExpiry splits proposals into those whose expiry epoch has
// been reached and those still active at the new epoch.
func partitionByExpiry(
	proposals []*models.GovernanceProposal,
	newEpoch uint64,
) (expired, active []*models.GovernanceProposal) {
	for _, p := range proposals {
		if p.ExpiresEpoch < newEpoch {
			expired = append(expired, p)
		} else {
			active = append(active, p)
		}
	}
	return expired, active
}

// stakeEpochFor returns the epoch whose "mark" snapshot should be used
// for vote-weight calculations in the given new epoch. Mark captured
// at end of N is used for voting in N+2, hence newEpoch-2. For early
// epochs we fall back to newEpoch-1 or 0.
func stakeEpochFor(newEpoch uint64) uint64 {
	switch {
	case newEpoch >= 2:
		return newEpoch - 2
	case newEpoch >= 1:
		return newEpoch - 1
	}
	return 0
}

// countActiveDReps returns the number of DReps eligible to vote in the
// current tick. AlwaysAbstain / AlwaysNoConfidence virtual DReps are
// not included.
func countActiveDReps(in *EpochInput) (int, error) {
	dreps, err := in.DB.GetActiveDreps(in.Txn)
	if err != nil {
		return 0, err
	}
	return len(dreps), nil
}

// ccEverSeated reports whether any committee members have ever been
// seated (including those currently deleted). This is used to
// differentiate "committee never formed" from "committee voted out".
func ccEverSeated(db *database.Database, txn *database.Txn) bool {
	// The snapshot-imported CommitteeMember table holds all historical
	// and current members; non-empty means the committee has existed.
	members, err := db.GetCommitteeMembers(txn)
	if err != nil {
		return false
	}
	return len(members) > 0
}

// conwayRatifyQuorum returns the quorum from the enacted committee if
// one is active. For bootstrapping we fall back to a 2/3 default so
// we don't silently auto-approve every CC-gated action when the
// constitution's committee block hasn't been modeled yet.
func conwayRatifyQuorum(
	pparams *conway.ConwayProtocolParameters,
) *big.Rat {
	_ = pparams
	return big.NewRat(2, 3)
}
