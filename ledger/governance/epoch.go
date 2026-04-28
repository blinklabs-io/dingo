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
	"sort"

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
	// ConwayGenesis supplies the initial committee quorum threshold
	// used until a live per-committee quorum is persisted in state.
	// Nil falls back to the hardcoded default.
	ConwayGenesis *conway.ConwayGenesis
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
	// Conway path requires database access for proposal lookups and
	// an UpdateFn for parameter-change enactment. A missing DB or
	// UpdateFn here would surface as a nil pointer panic deep inside
	// EnactProposal or in.DB.GetRatifiedGovernanceProposals; fail fast
	// with a descriptive error instead. A nil Txn would let each DB
	// call open its own transaction, which could leave the tick half-
	// applied on error (e.g., enacted proposal marked as enacted but
	// its side effects not persisted), so require it too.
	if in.DB == nil {
		return nil, errors.New("nil governance epoch database")
	}
	if in.Txn == nil {
		return nil, errors.New("nil governance epoch transaction")
	}
	if in.UpdateFn == nil {
		return nil, errors.New("nil governance epoch pparams update fn")
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
			// Abort the tick: enactment may have partially applied
			// side effects (committee changes, treasury debit), and
			// continuing would leave the proposal unmarked-enacted,
			// so the next tick would re-apply it. Returning the
			// error lets the surrounding DB transaction roll back.
			return nil, fmt.Errorf(
				"enact proposal %s#%d: %w",
				shortHash(proposal.TxHash),
				proposal.ActionIndex,
				err,
			)
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
	// Fetch proposals whose expiry epoch is in the past but which have
	// not yet been enacted, expired, or deleted. The active-proposals
	// query used below excludes these by construction (it filters
	// `expires_epoch >= NewEpoch`), so we need a dedicated read to mark
	// them expired and return their deposits.
	expired, err := in.DB.GetExpiringGovernanceProposals(
		in.NewEpoch, in.Txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get expiring proposals: %w", err)
	}
	for _, p := range expired {
		if err := refundProposalDeposit(
			in.DB,
			in.Txn,
			p,
			in.BoundarySlot,
		); err != nil {
			return nil, fmt.Errorf(
				"refund expired proposal deposit %s#%d: %w",
				shortHash(p.TxHash),
				p.ActionIndex,
				err,
			)
		}
		expiredEpoch := in.NewEpoch
		expiredSlot := in.BoundarySlot
		p.ExpiredEpoch = &expiredEpoch
		p.ExpiredSlot = &expiredSlot
		if err := in.DB.SetGovernanceProposal(p, in.Txn); err != nil {
			return nil, fmt.Errorf("mark expired: %w", err)
		}
		out.ExpiredCount++
	}

	// Active proposals still in play: not expired past the new epoch,
	// not enacted, not marked expired, not soft-deleted.
	stillActive, err := in.DB.GetActiveGovernanceProposals(
		in.NewEpoch, in.Txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get active proposals: %w", err)
	}

	// --- RATIFICATION -------------------------------------------------
	tallyCtx := &TallyContext{
		DB:           in.DB,
		Txn:          in.Txn,
		StakeEpoch:   stakeEpochFor(in.NewEpoch),
		CurrentEpoch: in.NewEpoch,
	}

	// Active set changes as we ratify; snapshot once.
	activeDRepCount, err := countActiveDReps(in)
	if err != nil {
		return nil, fmt.Errorf("count active dreps: %w", err)
	}

	// Pre-fetch the current chain root for each chained purpose. The
	// root cannot change during the RATIFY loop (ratifications are
	// marks, not enactments), so one read per purpose replaces the
	// old per-proposal call to GetLastEnactedGovernanceProposal.
	// Querying by purpose (not bare action type) lets NoConfidence
	// and UpdateCommittee share the same committee-purpose root.
	rootsByPurpose := make(
		map[govActionPurpose]*models.GovernanceProposal,
		len(chainedPurposes),
	)
	for _, p := range chainedPurposes {
		root, err := in.DB.GetLastEnactedGovernanceProposal(
			purposeActionTypes(p), in.Txn,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"get current root for purpose %d: %w", p, err,
			)
		}
		rootsByPurpose[p] = root
	}

	committeeState, err := LoadCommitteeVotingState(
		in.DB, in.Txn, in.NewEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("load committee voting state: %w", err)
	}
	tallyCtx.CommitteeState = committeeState
	activeCCCount := committeeState.ActiveMemberCount
	ccInNoConfidence := committeeNoConfidenceState(
		rootsByPurpose[purposeCommittee],
	)

	// Per the Conway spec, RATIFY operates on post-ENACT state. If the
	// enactment loop mutated pparams (e.g., ParameterChange or
	// HardForkInitiation), refresh the Conway pparams view so major
	// version and threshold reads reflect the updated values.
	if out.PParamsChanged {
		if p, ok := out.UpdatedPParams.(*conway.ConwayProtocolParameters); ok {
			conwayPParams = p
		}
	}

	majorVersion := conwayPParams.ProtocolVersion.Major
	// Computed after ENACT and reused across the RATIFY loop. The
	// RATIFY loop marks proposals but does not enact committee state.
	ccQuorum, err := conwayRatifyQuorum(
		in.Logger, in.DB, in.Txn, in.ConwayGenesis,
	)
	if err != nil {
		return nil, fmt.Errorf("get committee quorum: %w", err)
	}

	// Track ratifications per purpose (not per action type) so
	// NoConfidence and UpdateCommittee in the same tick don't both
	// fire — the spec allows at most one ratification per purpose.
	ratifiedThisTickByPurpose := make(map[govActionPurpose]bool)

	sort.SliceStable(stillActive, func(i, j int) bool {
		return govActionPriority(stillActive[i]) <
			govActionPriority(stillActive[j])
	})

	for _, proposal := range stillActive {
		actionType := lcommon.GovActionType(proposal.ActionType)
		purpose := govActionPurposeOf(actionType)
		if purpose != purposeNone && ratifiedThisTickByPurpose[purpose] {
			// The spec ratifies at most one action per purpose per
			// epoch tick. Skip to avoid double-enacting next tick.
			continue
		}

		// Parent chain check: look up the root by purpose so that,
		// e.g., an UpdateCommittee validates against the most recent
		// enacted committee-purpose action (which may be a
		// NoConfidence).
		var root *models.GovernanceProposal
		if purpose != purposeNone {
			root = rootsByPurpose[purpose]
		}
		if !validateParentChain(proposal, root) {
			continue
		}

		tally, err := TallyProposal(tallyCtx, proposal)
		if err != nil {
			return nil, fmt.Errorf("tally: %w", err)
		}
		// For ParameterChange, decode the action so thresholds can
		// take the touched parameter groups into account (especially
		// so SPOs only gate security-group changes, and DReps select
		// the most restrictive touched group).
		var paramUpdate *conway.ConwayProtocolParameterUpdate
		if lcommon.GovActionType(proposal.ActionType) ==
			lcommon.GovActionTypeParameterChange {
			action, decodeErr := decodeGovAction(
				proposal.GovActionCbor, proposal.ActionType,
			)
			if decodeErr != nil {
				// A decode failure means we cannot tell which
				// parameter groups are touched; silently falling
				// through with paramUpdate==nil would let SPO
				// checks return nil and allow security-group
				// changes to ratify without SPO approval. Skip
				// this proposal and surface the error so it is
				// investigated.
				if in.Logger != nil {
					in.Logger.Error(
						"skipping proposal: failed to decode parameter change action",
						"tx_hash", shortHash(proposal.TxHash),
						"action_index", proposal.ActionIndex,
						"error", decodeErr,
						"component", "governance",
					)
				}
				continue
			}
			a, ok := action.(*conway.ConwayParameterChangeGovAction)
			if !ok {
				if in.Logger != nil {
					in.Logger.Error(
						"skipping proposal: decoded action is not a parameter change",
						"tx_hash", shortHash(proposal.TxHash),
						"action_index", proposal.ActionIndex,
						"got_type", fmt.Sprintf("%T", action),
						"component", "governance",
					)
				}
				continue
			}
			paramUpdate = &a.ParamUpdate
		}
		decision := ShouldRatify(RatifyInputs{
			Tally:                 tally,
			PParams:               conwayPParams,
			ParamUpdate:           paramUpdate,
			ActiveDRepCount:       activeDRepCount,
			ActiveCCCount:         activeCCCount,
			CCQuorum:              ccQuorum,
			MajorVersion:          majorVersion,
			CommitteeNoConfidence: ccInNoConfidence,
		})
		if !decision.Ratified {
			continue
		}
		// Per CIP-1694, the deposit is returned at enactment (or
		// expiry), not at ratification. EnactProposal handles the
		// refund on the next epoch tick.
		ratifiedEpoch := in.NewEpoch
		ratifiedSlot := in.BoundarySlot
		proposal.RatifiedEpoch = &ratifiedEpoch
		proposal.RatifiedSlot = &ratifiedSlot
		if err := in.DB.SetGovernanceProposal(
			proposal, in.Txn,
		); err != nil {
			return nil, fmt.Errorf("mark ratified: %w", err)
		}
		if purpose != purposeNone {
			ratifiedThisTickByPurpose[purpose] = true
		}
		out.RatifiedCount++
		if isDelayingActionPurpose(purpose) {
			break
		}
	}

	return out, nil
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
	active := 0
	for _, drep := range dreps {
		if drepActiveAtEpoch(drep, in.NewEpoch) {
			active++
		}
	}
	return active, nil
}

func drepActiveAtEpoch(drep *models.Drep, currentEpoch uint64) bool {
	return drep != nil &&
		(drep.ExpiryEpoch == 0 || drep.ExpiryEpoch > currentEpoch)
}

func committeeNoConfidenceState(
	committeeRoot *models.GovernanceProposal,
) bool {
	return committeeRoot != nil &&
		lcommon.GovActionType(committeeRoot.ActionType) ==
			lcommon.GovActionTypeNoConfidence
}

func govActionPriority(proposal *models.GovernanceProposal) int {
	if proposal == nil {
		return 5
	}
	actionType := lcommon.GovActionType(proposal.ActionType)
	if actionType == lcommon.GovActionTypeNoConfidence {
		return 0
	}
	switch govActionPurposeOf(actionType) {
	case purposeCommittee:
		return 1
	case purposeConstitution:
		return 2
	case purposeHardFork:
		return 3
	case purposeNone, purposeParameterChange:
		return 4
	default:
		return 5
	}
}

func isDelayingActionPurpose(purpose govActionPurpose) bool {
	switch purpose {
	case purposeCommittee, purposeConstitution, purposeHardFork:
		return true
	case purposeNone, purposeParameterChange:
		return false
	default:
		return false
	}
}

// refundProposalDeposit credits the proposal deposit back to the
// proposer's reward account. The caller marks the proposal final only
// after this succeeds so a failed credit rolls back the epoch tick.
func refundProposalDeposit(
	db *database.Database,
	txn *database.Txn,
	proposal *models.GovernanceProposal,
	slot uint64,
) error {
	if proposal == nil || proposal.Deposit == 0 {
		return nil
	}
	if db == nil {
		return errors.New("nil database")
	}
	stakeCredential, err := rewardAccountStakeCredential(
		proposal.ReturnAddress,
	)
	if err != nil {
		return err
	}
	if err := db.AddAccountReward(
		stakeCredential,
		proposal.Deposit,
		slot,
		txn,
	); err != nil {
		return fmt.Errorf("credit reward account: %w", err)
	}
	return nil
}

func rewardAccountStakeCredential(returnAddress []byte) ([]byte, error) {
	addr, err := lcommon.NewAddressFromBytes(returnAddress)
	if err != nil {
		return nil, fmt.Errorf("decode return reward account: %w", err)
	}
	switch addr.Type() {
	case lcommon.AddressTypeNoneKey, lcommon.AddressTypeNoneScript:
	default:
		return nil, fmt.Errorf(
			"return address is not a reward account: address type %d",
			addr.Type(),
		)
	}
	stakeHash := addr.StakeKeyHash()
	return append([]byte(nil), stakeHash[:]...), nil
}

// shortHash returns a hex-encoded prefix of a tx hash for logging.
// Safe when the hash is shorter than 8 bytes (malformed DB rows).
func shortHash(h []byte) string {
	return hex.EncodeToString(h[:min(len(h), 8)])
}

// defaultCCQuorum is the last-resort fallback when Conway genesis is
// unavailable (e.g., pre-Conway networks or in tests). Matches the
// common Conway genesis default so CC-gated actions cannot silently
// auto-approve.
var defaultCCQuorum = big.NewRat(2, 3)

// conwayRatifyQuorum returns the CC quorum used by ShouldRatify. It
// prefers enacted committee state, reads the initial threshold from
// Conway genesis when available, and falls back to the 2/3 default.
func conwayRatifyQuorum(
	logger *slog.Logger,
	db *database.Database,
	txn *database.Txn,
	genesis *conway.ConwayGenesis,
) (*big.Rat, error) {
	if db != nil {
		quorum, err := db.GetCommitteeQuorum(txn)
		if err != nil {
			return nil, err
		}
		if quorum != nil {
			return quorum, nil
		}
	}
	if genesis != nil && genesis.Committee.Threshold != nil &&
		genesis.Committee.Threshold.Rat != nil {
		return genesis.Committee.Threshold.Rat, nil
	}
	if logger != nil {
		logger.Debug(
			"using fallback CC quorum (Conway genesis unavailable)",
			"quorum", "2/3",
			"component", "governance",
		)
	}
	return defaultCCQuorum, nil
}
