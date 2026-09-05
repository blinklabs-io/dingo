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
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// slowGovernanceTallyThreshold bounds how long the per-epoch governance
// tally is expected to take. Beyond it, ProcessEpoch logs a warning so
// an unexpectedly slow (or pathological) tally surfaces in operator logs
// instead of presenting as a silent stalled epoch rollover.
const slowGovernanceTallyThreshold = 30 * time.Second

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
	// DelegatorInactivityOn mirrors LedgerStateConfig.DelegatorInactivityEnabled
	// (CIP-0163): when true, the DRep voting-power denominator excludes
	// reward accounts whose expiration_epoch is nonzero and stale relative
	// to NewEpoch. Defaults false (gate off), keeping the tally
	// byte-identical to the pre-CIP behavior.
	DelegatorInactivityOn bool
}

// EpochOutput reports what happened during the tick so the
// caller can persist updated pparams and emit metrics.
type EpochOutput struct {
	UpdatedPParams    lcommon.ProtocolParameters
	PParamsChanged    bool
	CostModelsChanged bool
	EnactedCount      int
	RatifiedCount     int
	ExpiredCount      int
	OrphanedCount     int
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

	conwayPParams, err := conwayGovernanceProtocolParameters(in.PParams)
	if err != nil {
		return nil, err
	}
	if conwayPParams == nil {
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
	initialNetworkState, err := in.DB.Metadata().
		GetNetworkState(in.Txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf("get initial network state: %w", err)
	}
	var treasuryWithdrawalRemaining uint64
	if initialNetworkState != nil {
		treasuryWithdrawalRemaining = uint64(initialNetworkState.Treasury)
	}
	enactCtx := &EnactmentContext{
		DB:                             in.DB,
		Txn:                            in.Txn,
		Epoch:                          in.NewEpoch,
		Slot:                           in.BoundarySlot,
		PParams:                        in.PParams,
		UpdateFn:                       in.UpdateFn,
		TreasuryWithdrawalRemaining:    treasuryWithdrawalRemaining,
		TreasuryWithdrawalRemainingSet: true,
	}
	// A boundary transaction can commit before the separate tip advance. If
	// restart replays that boundary, stake-reward application first rewrites
	// the absolute network-state pot row, so proposals already marked enacted
	// at this exact boundary must replay their treasury side effects.
	replayedEnacted, err := in.DB.GetEnactedGovernanceProposalsAt(
		in.NewEpoch,
		in.BoundarySlot,
		in.Txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get boundary-enacted proposals: %w", err)
	}
	ratified, err := in.DB.GetRatifiedGovernanceProposals(in.Txn)
	if err != nil {
		return nil, fmt.Errorf("get ratified proposals: %w", err)
	}
	applyEnactmentResult := func(
		proposal *models.GovernanceProposal,
		res *EnactmentResult,
		replay bool,
	) {
		if !replay {
			out.EnactedCount++
		}
		if res.PParamsChanged {
			out.UpdatedPParams = res.UpdatedPParams
			out.PParamsChanged = true
			out.CostModelsChanged = out.CostModelsChanged || res.CostModelsChanged
			if lcommon.GovActionType(proposal.ActionType) ==
				lcommon.GovActionTypeHardForkInitiation {
				out.HardForkInitiated = true
			}
		}
	}
	enactProposal := func(
		proposal *models.GovernanceProposal,
		replay bool,
	) (bool, error) {
		// Legacy databases can contain proposals ratified before the current
		// deterministic enactability checks existed. Classify those known
		// semantic failures before EnactProposal performs any writes. Once this
		// preflight succeeds, every EnactProposal error is operational and must
		// abort the enclosing epoch transaction.
		if !replay {
			if _, err := ratificationEnactmentPrecondition(
				out.UpdatedPParams,
				in.UpdateFn,
				proposal,
				enactCtx.TreasuryWithdrawalRemaining,
			); err != nil {
				if err := in.DB.ClearGovernanceProposalRatification(
					proposal.TxHash,
					proposal.ActionIndex,
					in.BoundarySlot,
					in.Txn,
				); err != nil {
					return false, fmt.Errorf(
						"return deterministically non-enactable proposal %s#%d to pending: %w",
						shortHash(proposal.TxHash),
						proposal.ActionIndex,
						err,
					)
				}
				proposal.RatifiedEpoch = nil
				proposal.RatifiedSlot = nil
				if in.Logger != nil {
					in.Logger.Warn(
						"governance proposal failed deterministic enactment preflight; returned it to pending",
						"component",
						"governance",
						"tx_hash",
						shortHash(proposal.TxHash),
						"action_index",
						proposal.ActionIndex,
						"error",
						err,
						"epoch",
						in.NewEpoch,
					)
				}
				return false, nil
			}
		}

		candidatePParams, err := cloneGovernanceProtocolParameters(
			out.UpdatedPParams,
		)
		if err != nil {
			return false, fmt.Errorf("clone enactment pparams: %w", err)
		}
		enactCtx.PParams = candidatePParams

		res, err := EnactProposal(enactCtx, proposal)
		if err != nil {
			operation := "enact proposal"
			if replay {
				// A replay restores the side effects of a proposal already durably
				// marked enacted at this boundary. It is fatal for the same reason
				// as an operational error after successful preflight: continuing
				// would commit an enacted marker without its effects.
				operation = "replay enacted proposal"
			}
			return false, fmt.Errorf(
				"%s %s#%d: %w",
				operation,
				shortHash(proposal.TxHash),
				proposal.ActionIndex,
				err,
			)
		}
		applyEnactmentResult(proposal, res, replay)
		return true, nil
	}
	successfullyEnacted := append(
		make(
			[]*models.GovernanceProposal,
			0,
			len(replayedEnacted)+len(ratified),
		),
		replayedEnacted...,
	)
	for _, proposal := range replayedEnacted {
		if _, err := enactProposal(proposal, true); err != nil {
			return nil, err
		}
	}
	for _, proposal := range ratified {
		enacted, err := enactProposal(
			proposal,
			false,
		)
		if err != nil {
			return nil, err
		}
		if enacted {
			successfullyEnacted = append(successfullyEnacted, proposal)
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
	// Same replay window as enacted proposals: expired deposits that were
	// routed to treasury must be restored after the reward pot reset.
	replayedExpired, err := in.DB.GetExpiredGovernanceProposalsAt(
		in.NewEpoch,
		in.BoundarySlot,
		in.Txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get boundary-expired proposals: %w", err)
	}
	expireProposal := func(p *models.GovernanceProposal, replay bool) error {
		if err := refundProposalDeposit(
			in.DB,
			in.Txn,
			p,
			in.BoundarySlot,
		); err != nil {
			return fmt.Errorf(
				"refund expired proposal deposit %s#%d: %w",
				shortHash(p.TxHash),
				p.ActionIndex,
				err,
			)
		}
		if replay {
			return nil
		}
		expiredEpoch := in.NewEpoch
		expiredSlot := in.BoundarySlot
		p.ExpiredEpoch = &expiredEpoch
		p.ExpiredSlot = &expiredSlot
		if err := in.DB.SetGovernanceProposal(p, in.Txn); err != nil {
			return fmt.Errorf("mark expired: %w", err)
		}
		out.ExpiredCount++
		return nil
	}
	for _, p := range replayedExpired {
		if err := expireProposal(p, true); err != nil {
			return nil, err
		}
	}
	for _, p := range expired {
		if err := expireProposal(p, false); err != nil {
			return nil, err
		}
	}

	// --- COMPETING SUBTREE REMOVAL ---------------------------------------
	// Enactment advances a purpose chain: descendants of the enacted action
	// remain valid, while competing siblings and their descendants are
	// removed. Natural expiry removes descendants of the expired action.
	expiredSeeds := append(
		append(make([]*models.GovernanceProposal, 0,
			len(replayedExpired)+len(expired)), replayedExpired...),
		expired...,
	)
	orphanCount, err := removeOrphanedProposals(
		in.DB,
		in.Txn,
		successfullyEnacted,
		expiredSeeds,
		in.NewEpoch,
		in.BoundarySlot,
		in.Logger,
	)
	if err != nil {
		return nil, fmt.Errorf("remove orphaned proposals: %w", err)
	}
	out.OrphanedCount = orphanCount

	// Active proposals still in play: not expired past the new epoch,
	// not enacted, not marked expired, not soft-deleted.
	stillActive, err := in.DB.GetActiveGovernanceProposals(
		in.NewEpoch, in.Txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get active proposals: %w", err)
	}

	// --- RATIFICATION -------------------------------------------------
	//
	// The inputs assembled below (TallyContext, activeDRepCount,
	// rootsByPurpose, committeeState, ccQuorum, conwayPParams,
	// majorVersion, ccInNoConfidence) feed ShouldRatify. A parallel
	// build for HardForkInitiation specifically exists in
	// EvaluateRatifiableHardForkInitiation (governance/stability.go),
	// which runs the same check mid-epoch to surface upcoming
	// transitions before the boundary tick fires. Adding a new
	// ratification input here without updating the mid-epoch path
	// will silently make the two answers diverge — keep them in sync.
	tallyCtx := &TallyContext{
		DB:                    in.DB,
		Txn:                   in.Txn,
		StakeEpoch:            stakeEpochFor(in.NewEpoch),
		CurrentEpoch:          in.NewEpoch,
		DelegatorInactivityOn: in.DelegatorInactivityOn,
	}

	// Active set changes as we ratify; snapshot once.
	activeDRepCount, err := countActiveDReps(in.DB, in.Txn, in.NewEpoch)
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

	// Precompute the proposal-independent DRep and SPO voting
	// denominators once per epoch tick and reuse them across every
	// proposal's tally. DRep voting power and the pool stake snapshot do
	// not change while the RATIFY loop runs, so loading them per proposal
	// (as the lazy path inside the tally functions does) just repeats the
	// heavy account/utxo voting-power query for every active proposal.
	// On a freshly Mithril-restored database at an epoch boundary with
	// many active proposals, that repetition stalled the epoch rollover —
	// and the entire ledger pipeline behind it — for hours.
	//
	// Skip the loads entirely when there are no active proposals: the
	// RATIFY loop below never calls TallyProposal, so this heavy read
	// would be pure overhead (and a needless failure surface) on a no-op
	// epoch boundary.
	drepState := &DRepVotingState{}
	spoState := &SPOVotingState{}
	if len(stillActive) > 0 {
		drepState, err = LoadDRepVotingState(
			in.DB, in.Txn, in.NewEpoch, in.DelegatorInactivityOn,
		)
		if err != nil {
			return nil, fmt.Errorf("load drep voting state: %w", err)
		}
		tallyCtx.DRepState = drepState
		spoState, err = LoadSPOVotingState(in.DB, in.Txn, tallyCtx.StakeEpoch)
		if err != nil {
			return nil, fmt.Errorf("load spo voting state: %w", err)
		}
		tallyCtx.SPOState = spoState
	}

	// Per the Conway spec, RATIFY operates on post-ENACT state. If the
	// enactment loop mutated pparams (e.g., ParameterChange or
	// HardForkInitiation), refresh the Conway pparams view so major
	// version and threshold reads reflect the updated values.
	if out.PParamsChanged {
		updatedConwayPParams, err := conwayGovernanceProtocolParameters(
			out.UpdatedPParams,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"resolve updated governance pparams: %w",
				err,
			)
		}
		if updatedConwayPParams == nil {
			return nil, fmt.Errorf(
				"governance pparams update returned pre-Conway type %T",
				out.UpdatedPParams,
			)
		}
		conwayPParams = updatedConwayPParams
	}

	majorVersion := conwayPParams.ProtocolVersion.Major
	// RATIFY uses the post-ENACT protocol version for both threshold
	// selection and action-specific SPO non-voter semantics.
	tallyCtx.MajorVersion = majorVersion
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
	// RATIFY carries the post-ENACT treasury in its enactment state. Accepted
	// withdrawals consume this budget immediately, even though they are not
	// enacted until a later boundary and even when an unregistered destination
	// would leave the corresponding lovelace in Dingo's physical treasury pot.
	ratificationTreasuryRemaining := enactCtx.TreasuryWithdrawalRemaining

	sort.SliceStable(stillActive, func(i, j int) bool {
		return govActionPriority(stillActive[i]) <
			govActionPriority(stillActive[j])
	})

	// Log the tally scale before the loop so an unexpectedly slow or
	// stalled tally is visible in operator logs (a hang shows a
	// "starting" line with no matching completion) rather than
	// presenting as a silent stalled epoch rollover.
	tallyStart := time.Now()
	if in.Logger != nil && len(stillActive) > 0 {
		in.Logger.Info(
			"governance epoch tally starting",
			"component", "governance",
			"epoch", in.NewEpoch,
			"active_proposals", len(stillActive),
			"active_dreps", len(drepState.Dreps),
			"pool_snapshot_rows", len(spoState.Dist),
		)
	}

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
			// A chained proposal that references a parent we
			// don't have an enacted root for is the silent
			// failure mode behind issue #2195: on a Mithril-
			// bootstrapped node missing per-purpose seeded
			// roots, every chained proposal hits this branch and
			// silently expires. Log a warning so the next
			// occurrence shows up in operator logs instead of
			// only as a block-producer divergence at the next
			// enactment boundary.
			if in.Logger != nil &&
				root == nil &&
				proposal.ParentTxHash != nil &&
				purpose != purposeNone {
				in.Logger.Warn(
					"skipping chained proposal: no enacted root for purpose; possible mithril bootstrap gap (#2195)",
					"component",
					"governance",
					"tx_hash",
					shortHash(proposal.TxHash),
					"action_index",
					proposal.ActionIndex,
					"action_type",
					proposal.ActionType,
					"parent_tx_hash",
					hex.EncodeToString(proposal.ParentTxHash),
					"epoch",
					in.NewEpoch,
				)
			}
			continue
		}

		tally, err := TallyProposal(tallyCtx, proposal)
		if err != nil {
			return nil, fmt.Errorf("tally: %w", err)
		}
		// Decode the action once for every action-specific ratification
		// predicate. ParameterChange uses the touched parameter groups for
		// threshold selection in both Conway and Dijkstra, while
		// UpdateCommittee checks proposed member expiries against the current
		// epoch and committee term limit.
		action, decodeErr := decodeGovActionForPParams(
			proposal.GovActionCbor,
			proposal.ActionType,
			out.UpdatedPParams,
		)
		if decodeErr != nil {
			if in.Logger != nil {
				in.Logger.Error(
					"skipping proposal: failed to decode governance action",
					"tx_hash",
					shortHash(proposal.TxHash),
					"action_index",
					proposal.ActionIndex,
					"action_type",
					proposal.ActionType,
					"error",
					decodeErr,
					"component",
					"governance",
				)
			}
			continue
		}
		var parameterChange lcommon.ParameterChangeGovAction
		if lcommon.GovActionType(proposal.ActionType) ==
			lcommon.GovActionTypeParameterChange {
			a, ok := action.(lcommon.ParameterChangeGovAction)
			if !ok {
				if in.Logger != nil {
					in.Logger.Error(
						"skipping proposal: decoded action is not a parameter change",
						"tx_hash",
						shortHash(proposal.TxHash),
						"action_index",
						proposal.ActionIndex,
						"got_type",
						fmt.Sprintf("%T", action),
						"component",
						"governance",
					)
				}
				continue
			}
			parameterChange = a
		}
		decision := ShouldRatify(RatifyInputs{
			Tally:                 tally,
			PParams:               conwayPParams,
			ParameterChange:       parameterChange,
			GovAction:             action,
			CurrentEpoch:          in.NewEpoch,
			ActiveDRepCount:       activeDRepCount,
			ActiveCCCount:         activeCCCount,
			CCQuorum:              ccQuorum,
			MajorVersion:          majorVersion,
			CommitteeNoConfidence: ccInNoConfidence,
		})
		if !decision.Ratified {
			continue
		}
		nextTreasuryRemaining, enactabilityErr := ratificationEnactmentPrecondition(
			out.UpdatedPParams,
			in.UpdateFn,
			proposal,
			ratificationTreasuryRemaining,
		)
		if enactabilityErr != nil {
			if in.Logger != nil {
				in.Logger.Warn(
					"skipping proposal: enactment precondition failed",
					"component", "governance",
					"tx_hash", shortHash(proposal.TxHash),
					"action_index", proposal.ActionIndex,
					"action_type", proposal.ActionType,
					"error", enactabilityErr,
					"epoch", in.NewEpoch,
				)
			}
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
		ratificationTreasuryRemaining = nextTreasuryRemaining
		out.RatifiedCount++
		if isDelayingActionPurpose(purpose) {
			break
		}
	}

	if in.Logger != nil && len(stillActive) > 0 {
		elapsed := time.Since(tallyStart)
		if elapsed >= slowGovernanceTallyThreshold {
			in.Logger.Warn(
				"governance epoch tally slow",
				"component", "governance",
				"epoch", in.NewEpoch,
				"active_proposals", len(stillActive),
				"active_dreps", len(drepState.Dreps),
				"duration", elapsed.String(),
			)
		} else {
			in.Logger.Debug(
				"governance epoch tally complete",
				"component", "governance",
				"epoch", in.NewEpoch,
				"active_proposals", len(stillActive),
				"duration", elapsed.String(),
			)
		}
	}

	return out, nil
}

func cloneGovernanceProtocolParameters(
	pparams lcommon.ProtocolParameters,
) (lcommon.ProtocolParameters, error) {
	return eras.CloneGovernanceProtocolParameters(pparams)
}

// ratificationEnactmentPrecondition checks the deterministic failure surfaces
// required before RATIFY may accept a proposal. It also returns the running
// treasury budget after accepting a treasury withdrawal. Database writes are
// deliberately not attempted here. Once this preflight succeeds, an error from
// the later ENACT pass is treated as operational and aborts the epoch.
func ratificationEnactmentPrecondition(
	pparams lcommon.ProtocolParameters,
	updateFn func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error),
	proposal *models.GovernanceProposal,
	treasuryRemaining uint64,
) (uint64, error) {
	if proposal == nil {
		return treasuryRemaining, errors.New("nil proposal")
	}
	if proposal.Deposit > 0 {
		if _, _, err := rewardAccountStakeCredential(
			proposal.ReturnAddress,
		); err != nil {
			return treasuryRemaining, fmt.Errorf(
				"proposal deposit return: %w",
				err,
			)
		}
	}
	action, err := decodeGovActionForPParams(
		proposal.GovActionCbor,
		proposal.ActionType,
		pparams,
	)
	if err != nil {
		return treasuryRemaining, fmt.Errorf("decode gov action: %w", err)
	}

	switch a := action.(type) {
	case *conway.ConwayParameterChangeGovAction:
		candidate, err := cloneGovernanceProtocolParameters(pparams)
		if err != nil {
			return treasuryRemaining, err
		}
		if _, err := updateFn(candidate, a.ParamUpdate); err != nil {
			return treasuryRemaining, fmt.Errorf("apply param update: %w", err)
		}
	case *gdijkstra.DijkstraParameterChangeGovAction:
		candidate, err := cloneGovernanceProtocolParameters(pparams)
		if err != nil {
			return treasuryRemaining, err
		}
		if _, err := updateFn(candidate, a.ParamUpdate); err != nil {
			return treasuryRemaining, fmt.Errorf("apply param update: %w", err)
		}
	case *lcommon.HardForkInitiationGovAction:
		candidate, err := cloneGovernanceProtocolParameters(pparams)
		if err != nil {
			return treasuryRemaining, err
		}
		if _, err := setProtocolVersion(
			candidate,
			a.ProtocolVersion.Major,
			a.ProtocolVersion.Minor,
		); err != nil {
			return treasuryRemaining, fmt.Errorf("schedule hard fork: %w", err)
		}
	case *lcommon.TreasuryWithdrawalGovAction:
		total, err := treasuryWithdrawalTotal(a)
		if err != nil {
			return treasuryRemaining, err
		}
		if total > treasuryRemaining {
			return treasuryRemaining, fmt.Errorf(
				"treasury withdrawal of %d exceeds running ratification budget %d",
				total,
				treasuryRemaining,
			)
		}
		for rewardAddr := range a.Withdrawals {
			if rewardAddr == nil {
				return treasuryRemaining, errors.New(
					"nil treasury withdrawal reward address",
				)
			}
			rewardAddrBytes, err := rewardAddr.Bytes()
			if err != nil {
				return treasuryRemaining, fmt.Errorf(
					"encode treasury withdrawal reward address: %w",
					err,
				)
			}
			if _, _, err := rewardAccountStakeCredential(
				rewardAddrBytes,
			); err != nil {
				return treasuryRemaining, fmt.Errorf(
					"treasury withdrawal reward account: %w",
					err,
				)
			}
		}
		return treasuryRemaining - total, nil
	case *lcommon.UpdateCommitteeGovAction:
		if a.Quorum.Rat == nil || a.Quorum.Sign() <= 0 {
			return treasuryRemaining, errors.New(
				"committee quorum must be positive",
			)
		}
	case *lcommon.InfoGovAction:
		// RATIFY rejects Info actions before calling this preflight. A legacy
		// row can nevertheless already carry a ratification marker, and the
		// existing ENACT path finalizes that row without action-specific side
		// effects. It is therefore not a deterministic EnactProposal failure.
	case *lcommon.NoConfidenceGovAction,
		*lcommon.NewConstitutionGovAction:
		// These actions have no additional deterministic local precondition.
	default:
		return treasuryRemaining, fmt.Errorf(
			"unsupported gov action type %T",
			action,
		)
	}
	return treasuryRemaining, nil
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

// countActiveDReps returns the number of credential-backed DReps
// eligible to vote in currentEpoch. AlwaysAbstain / AlwaysNoConfidence
// virtual DReps are not counted.
func countActiveDReps(
	db *database.Database,
	txn *database.Txn,
	currentEpoch uint64,
) (int, error) {
	dreps, err := db.GetActiveDreps(txn)
	if err != nil {
		return 0, err
	}
	active := 0
	for _, drep := range dreps {
		if drepActiveAtEpoch(drep, currentEpoch) {
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

// refundProposalDeposit returns the proposal deposit to the proposer when
// the return reward account is still registered. If the reward account is
// missing or inactive, the unclaimed deposit returns to the treasury.
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
	credentialTag, stakeCredential, err := rewardAccountStakeCredential(
		proposal.ReturnAddress,
	)
	if err != nil {
		return err
	}
	credited, err := CreditRegisteredRewardAccountAfterSnapshot(
		db,
		txn,
		credentialTag,
		stakeCredential,
		proposal.Deposit,
		slot,
		// The proposal tx hash plus action index is the per-event credit
		// discriminator: it keeps two refunds to the same return account in
		// one epoch as distinct journal rows and makes a crash-replayed
		// boundary refund idempotent.
		proposalRewardSourceHash(proposal),
	)
	if err != nil {
		return err
	}
	if !credited {
		if err := AddUnclaimedToTreasury(
			db,
			txn,
			proposal.Deposit,
			slot,
		); err != nil {
			return fmt.Errorf(
				"return unclaimed proposal deposit to treasury: %w",
				err,
			)
		}
	}
	return nil
}

// removeOrphanedProposals removes the losing branches of governance purpose
// chains. Descendants of enacted proposals remain eligible to follow the new
// root. Active siblings that share the enacted proposal's former parent and
// purpose are removed with their full subtrees. Expired proposals instead
// remove their own descendant subtrees.
func removeOrphanedProposals(
	db *database.Database,
	txn *database.Txn,
	enacted []*models.GovernanceProposal,
	expired []*models.GovernanceProposal,
	epoch uint64,
	slot uint64,
	logger *slog.Logger,
) (int, error) {
	active, err := db.GetActiveGovernanceProposals(epoch, txn)
	if err != nil {
		return 0, fmt.Errorf("get active governance proposals: %w", err)
	}
	children := make(map[string][]*models.GovernanceProposal)
	for _, proposal := range active {
		children[proposalParentKey(proposal)] = append(
			children[proposalParentKey(proposal)],
			proposal,
		)
	}
	queue := make([]*models.GovernanceProposal, 0)
	for _, proposal := range expired {
		queue = append(queue, children[proposalIdentityKey(proposal)]...)
	}
	for _, winner := range enacted {
		winnerPurpose := govActionPurposeOf(
			lcommon.GovActionType(winner.ActionType),
		)
		if winnerPurpose == purposeNone {
			continue
		}
		for _, sibling := range children[proposalParentKey(winner)] {
			if govActionPurposeOf(
				lcommon.GovActionType(sibling.ActionType),
			) == winnerPurpose {
				queue = append(queue, sibling)
			}
		}
	}
	removed := make(map[string]struct{})
	count := 0
	for len(queue) > 0 {
		proposal := queue[0]
		queue = queue[1:]
		identity := proposalIdentityKey(proposal)
		if _, ok := removed[identity]; ok {
			continue
		}
		removed[identity] = struct{}{}
		if err := refundProposalDeposit(db, txn, proposal, slot); err != nil {
			return count, fmt.Errorf(
				"refund removed proposal deposit %s#%d: %w",
				shortHash(proposal.TxHash), proposal.ActionIndex, err,
			)
		}
		expiredEpoch := epoch
		expiredSlot := slot
		proposal.ExpiredEpoch = &expiredEpoch
		proposal.ExpiredSlot = &expiredSlot
		if err := db.SetGovernanceProposal(proposal, txn); err != nil {
			return count, fmt.Errorf(
				"mark removed proposal expired %s#%d: %w",
				shortHash(proposal.TxHash), proposal.ActionIndex, err,
			)
		}
		if logger != nil {
			logger.Info(
				"removed competing governance proposal",
				"component", "governance",
				"tx_hash", shortHash(proposal.TxHash),
				"action_index", proposal.ActionIndex,
				"epoch", epoch,
			)
		}
		queue = append(queue, children[identity]...)
		count++
	}
	return count, nil
}

func proposalIdentityKey(proposal *models.GovernanceProposal) string {
	if proposal == nil {
		return ""
	}
	return fmt.Sprintf("%x#%d", proposal.TxHash, proposal.ActionIndex)
}

func proposalParentKey(proposal *models.GovernanceProposal) string {
	if proposal == nil || len(proposal.ParentTxHash) == 0 ||
		proposal.ParentActionIdx == nil {
		return "root"
	}
	return fmt.Sprintf(
		"%x#%d",
		proposal.ParentTxHash,
		*proposal.ParentActionIdx,
	)
}

func rewardAccountStakeCredential(returnAddress []byte) (uint8, []byte, error) {
	addr, err := lcommon.NewAddressFromBytes(returnAddress)
	if err != nil {
		return 0, nil, fmt.Errorf("decode return reward account: %w", err)
	}
	var credentialTag uint8
	switch addr.Type() {
	case lcommon.AddressTypeNoneKey:
		credentialTag = uint8(lcommon.CredentialTypeAddrKeyHash)
	case lcommon.AddressTypeNoneScript:
		credentialTag = uint8(lcommon.CredentialTypeScriptHash)
	default:
		return 0, nil, fmt.Errorf(
			"return address is not a reward account: address type %d",
			addr.Type(),
		)
	}
	stakeHash := addr.StakeKeyHash()
	return credentialTag, append([]byte(nil), stakeHash[:]...), nil
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
