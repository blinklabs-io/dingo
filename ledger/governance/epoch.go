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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
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
}

// EpochOutput reports what happened during the tick so the
// caller can persist updated pparams and emit metrics.
type EpochOutput struct {
	UpdatedPParams    lcommon.ProtocolParameters
	PParamsChanged    bool
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
	enactProposal := func(proposal *models.GovernanceProposal, replay bool) error {
		enactCtx.PParams = out.UpdatedPParams
		res, err := EnactProposal(enactCtx, proposal)
		if err != nil {
			// Abort the tick: enactment may have partially applied
			// side effects (committee changes, treasury debit), and
			// continuing would leave the proposal unmarked-enacted,
			// so the next tick would re-apply it. Returning the
			// error lets the surrounding DB transaction roll back.
			return fmt.Errorf(
				"enact proposal %s#%d: %w",
				shortHash(proposal.TxHash),
				proposal.ActionIndex,
				err,
			)
		}
		if !replay {
			out.EnactedCount++
		}
		if res.PParamsChanged {
			out.UpdatedPParams = res.UpdatedPParams
			out.PParamsChanged = true
			if lcommon.GovActionType(proposal.ActionType) ==
				lcommon.GovActionTypeHardForkInitiation {
				out.HardForkInitiated = true
			}
		}
		return nil
	}
	for _, proposal := range replayedEnacted {
		if err := enactProposal(proposal, true); err != nil {
			return nil, err
		}
	}
	for _, proposal := range ratified {
		if err := enactProposal(proposal, false); err != nil {
			return nil, err
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

	// --- ORPHAN REMOVAL --------------------------------------------------
	// Proposals that reference a just-enacted or just-expired proposal as
	// their parent are "orphaned": their anchor is gone from the active
	// pool and can never become a chain root. The spec (Conway EPOCH)
	// calls this set `removedDueToEnactment` and requires deposits to be
	// returned (or sent to treasury) for all of them. The sweep is
	// transitive — orphans of orphans are also removed — so we run a BFS
	// over the dependency graph seeded with every enacted and expired
	// proposal from this tick.
	orphanSeeds := make(
		[]*models.GovernanceProposal,
		0,
		len(replayedEnacted)+len(ratified)+
			len(replayedExpired)+len(expired),
	)
	orphanSeeds = append(orphanSeeds, replayedEnacted...)
	orphanSeeds = append(orphanSeeds, ratified...)
	orphanSeeds = append(orphanSeeds, replayedExpired...)
	orphanSeeds = append(orphanSeeds, expired...)
	orphanCount, err := removeOrphanedProposals(
		in.DB, in.Txn, orphanSeeds, in.NewEpoch, in.BoundarySlot, in.Logger,
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
		DB:           in.DB,
		Txn:          in.Txn,
		StakeEpoch:   stakeEpochFor(in.NewEpoch),
		CurrentEpoch: in.NewEpoch,
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
	var drepState *DRepVotingState
	var spoState *SPOVotingState
	if len(stillActive) > 0 {
		drepState, err = LoadDRepVotingState(in.DB, in.Txn, in.NewEpoch)
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
					"component", "governance",
					"tx_hash", shortHash(proposal.TxHash),
					"action_index", proposal.ActionIndex,
					"action_type", proposal.ActionType,
					"parent_tx_hash", hex.EncodeToString(proposal.ParentTxHash),
					"epoch", in.NewEpoch,
				)
			}
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
	credited, err := CreditRegisteredRewardAccount(
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

// removeOrphanedProposals performs a BFS over the live proposal dependency
// graph starting from seeds (enacted + expired proposals from this tick).
// For each seed it finds active proposals that reference it as their parent.
// Those dependents are "orphaned" — their anchor is no longer pending and can
// never become a chain root — so their deposits are refunded and they are
// marked expired at the boundary slot (using expired_epoch/expired_slot so
// the existing slot-based rollback path reverts this tick cleanly). The sweep
// is transitive: each newly-orphaned proposal is itself added to the queue so
// its dependents are also swept.
func removeOrphanedProposals(
	db *database.Database,
	txn *database.Txn,
	seeds []*models.GovernanceProposal,
	epoch uint64,
	slot uint64,
	logger *slog.Logger,
) (int, error) {
	queue := append(make([]*models.GovernanceProposal, 0, len(seeds)), seeds...)
	count := 0
	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]
		children, err := db.GetChildGovernanceProposals(
			parent.TxHash, parent.ActionIndex, txn,
		)
		if err != nil {
			return count, fmt.Errorf(
				"get children of proposal %s#%d: %w",
				shortHash(parent.TxHash),
				parent.ActionIndex,
				err,
			)
		}
		for _, child := range children {
			if err := refundProposalDeposit(db, txn, child, slot); err != nil {
				return count, fmt.Errorf(
					"refund orphaned proposal deposit %s#%d: %w",
					shortHash(child.TxHash),
					child.ActionIndex,
					err,
				)
			}
			expiredEpoch := epoch
			expiredSlot := slot
			child.ExpiredEpoch = &expiredEpoch
			child.ExpiredSlot = &expiredSlot
			if err := db.SetGovernanceProposal(child, txn); err != nil {
				return count, fmt.Errorf(
					"mark orphaned proposal expired %s#%d: %w",
					shortHash(child.TxHash),
					child.ActionIndex,
					err,
				)
			}
			if logger != nil {
				logger.Info(
					"removed orphaned governance proposal",
					"component", "governance",
					"tx_hash", shortHash(child.TxHash),
					"action_index", child.ActionIndex,
					"parent_tx_hash", shortHash(parent.TxHash),
					"parent_action_index", parent.ActionIndex,
					"epoch", epoch,
				)
			}
			queue = append(queue, child)
			count++
		}
	}
	return count, nil
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
