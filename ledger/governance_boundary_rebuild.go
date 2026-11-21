package ledger

import (
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// errGovernanceBoundaryUnreachable reports a boundary whose pre-boundary state
// the stores can no longer restore. It ends the rebuild: every older boundary
// is further from the tip, so it is unreachable too.
var errGovernanceBoundaryUnreachable = errors.New(
	"governance boundary state is not restorable",
)

// rebuildGovernanceBoundarySnapshots stores the boundary snapshot of every
// Conway-or-later epoch that has none, newest first, by replaying that
// boundary's rollover in a transaction that is thrown away. A database that
// upgraded mid-window has blocks and ledger state for those boundaries but no
// row.
//
// Each replay truncates the transaction to the last block below the boundary
// exactly as a ledger rollback does, runs the boundary's rollover with RATIFY
// forced on against the committed mark distribution, and reads back the row
// the shared governance code saved. The transaction is then rolled back, so the
// live ledger, tip and UTxO set are untouched, and the row is saved in a fresh
// transaction. A boundary already holding a row is skipped, which makes the
// pass idempotent and resumable after a crash between boundaries.
//
// A boundary the stores cannot restore ends the pass: the live UTxO set's
// journal no longer reaches it, its target lies below the Mithril trust
// boundary, the consumed-output sweep removed what the restore needs, or
// retention pruned the reward basis its rollover reads. Its row and every
// older one stay absent and reads of them report
// models.ErrGovernanceBoundarySnapshotNotRetained.
func (ls *LedgerState) rebuildGovernanceBoundarySnapshots(
	ctx context.Context,
) error {
	epochs, err := ls.ledgerDB().GetEpochs(nil)
	if err != nil {
		return fmt.Errorf("list epochs: %w", err)
	}
	pending, err := loadPendingRatification(ls.db, nil)
	if err != nil {
		return fmt.Errorf("read pending ratification: %w", err)
	}
	for i := len(epochs) - 1; i > 0; i-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		epoch, prev := epochs[i], epochs[i-1]
		if epoch.EraId < eras.ConwayEraDesc.Id ||
			prev.EpochId+1 != epoch.EpochId {
			// A jumped epoch runs no boundary rules, so it never had a row.
			continue
		}
		if pending != nil && pending.Epoch == epoch.EpochId {
			// The boundary's own job writes this row.
			continue
		}
		has, err := ls.hasGovernanceBoundarySnapshot(ctx, epoch, nil)
		if err != nil {
			return err
		}
		if has {
			continue
		}
		err = ls.rebuildGovernanceBoundarySnapshot(ctx, epoch, prev)
		if errors.Is(err, errGovernanceBoundaryUnreachable) {
			ls.config.Logger.Warn(
				"governance boundary snapshots older than this epoch cannot "+
					"be rebuilt",
				"component", "ledger",
				"epoch", epoch.EpochId,
				"reason", err,
			)
			return nil
		}
		if err != nil {
			return fmt.Errorf(
				"rebuild governance boundary snapshot for epoch %d: %w",
				epoch.EpochId, err,
			)
		}
	}
	return nil
}

func (ls *LedgerState) hasGovernanceBoundarySnapshot(
	ctx context.Context,
	epoch models.Epoch,
	txn *database.Txn,
) (bool, error) {
	snapshot, err := ls.db.GetGovernanceBoundarySnapshotAtSlot(
		ctx, epoch.StartSlot, txn,
	)
	if errors.Is(err, models.ErrGovernanceBoundarySnapshotNotRetained) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf(
			"read governance boundary snapshot of epoch %d: %w",
			epoch.EpochId, err,
		)
	}
	return snapshot != nil && snapshot.Epoch == epoch.EpochId, nil
}

func (ls *LedgerState) rebuildGovernanceBoundarySnapshot(
	ctx context.Context,
	epoch, prev models.Epoch,
) error {
	block, err := database.BlockBeforeSlot(ctx, ls.db, epoch.StartSlot)
	if err != nil {
		return fmt.Errorf(
			"%w: no block before slot %d: %w",
			errGovernanceBoundaryUnreachable, epoch.StartSlot, err,
		)
	}
	point := ocommon.NewPoint(block.Slot, block.Hash)
	onChain, err := ls.primaryChainContainsPoint(ctx, point)
	if err != nil {
		return err
	}
	if !onChain {
		return fmt.Errorf(
			"%w: block at slot %d is not on the primary chain",
			errGovernanceBoundaryUnreachable, point.Slot,
		)
	}
	return ls.rebuildGovernanceBoundarySnapshotAt(ctx, epoch, prev, point)
}

// rebuildGovernanceBoundarySnapshotAt rebuilds the row of epoch from the
// ledger state as it stood at point, the last applied block below the
// boundary.
func (ls *LedgerState) rebuildGovernanceBoundarySnapshotAt(
	ctx context.Context,
	epoch, prev models.Epoch,
	point ocommon.Point,
) error {
	// The mark distribution RATIFY tallies against is dropped with the epoch
	// when the transaction truncates, so read it from the committed state.
	spo, err := governance.LoadSPOVotingState(ls.ledgerDB(), nil, epoch.EpochId)
	if err != nil {
		return err
	}
	if len(spo.Dist) == 0 {
		before, err := governance.LoadSPOVotingState(
			ls.ledgerDB(), nil, prev.EpochId,
		)
		if err != nil {
			return err
		}
		if len(before.Dist) > 0 {
			return fmt.Errorf(
				"%w: mark snapshot of epoch %d is missing",
				errGovernanceBoundaryUnreachable, epoch.EpochId,
			)
		}
	}
	ls.RLock()
	mithrilSlot := ls.mithrilLedgerSlot
	ls.RUnlock()
	if mithrilSlot > 0 && point.Slot < mithrilSlot {
		return fmt.Errorf(
			"%w: slot %d is below the Mithril trust boundary %d",
			errGovernanceBoundaryUnreachable, point.Slot, mithrilSlot,
		)
	}
	below, floor, err := ls.rollbackBelowConsumedUtxoPruneFloor(point)
	if err != nil {
		return err
	}
	if below {
		return fmt.Errorf(
			"%w: slot %d is below the consumed UTxO prune floor %d",
			errGovernanceBoundaryUnreachable, point.Slot, floor,
		)
	}

	snapshot, err := ls.replayGovernanceBoundary(
		ctx, epoch, point, mithrilSlot, spo,
	)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return nil
	}
	// Another writer may have stored the row while the replay ran.
	has, err := ls.hasGovernanceBoundarySnapshot(ctx, epoch, nil)
	if err != nil || has {
		return err
	}
	return ls.db.SaveGovernanceBoundarySnapshot(ctx, snapshot, nil)
}

// replayGovernanceBoundary runs the boundary of epoch in a transaction
// truncated to point and returns the snapshot it saved there. The transaction
// is always rolled back; it must never commit, because that would apply its
// staged live UTxO set rollback.
func (ls *LedgerState) replayGovernanceBoundary(
	ctx context.Context,
	epoch models.Epoch,
	point ocommon.Point,
	mithrilSlot uint64,
	spo *governance.SPOVotingState,
) (*models.GovernanceBoundarySnapshot, error) {
	txn := ls.ledgerDB().Begin(ctx, true)
	defer txn.Release()
	snapshot, err := ls.replayGovernanceBoundaryInTxn(
		ctx, txn, epoch, point, mithrilSlot, spo,
	)
	if rbErr := txn.Rollback(); rbErr != nil && err == nil {
		err = fmt.Errorf("discard boundary replay: %w", rbErr)
	}
	return snapshot, err
}

func (ls *LedgerState) replayGovernanceBoundaryInTxn(
	ctx context.Context,
	txn ledgerdb.Txn,
	epoch models.Epoch,
	point ocommon.Point,
	mithrilSlot uint64,
	spo *governance.SPOVotingState,
) (*models.GovernanceBoundarySnapshot, error) {
	if _, _, err := ls.truncateLedgerStateInTxn(
		ctx, txn, point, mithrilSlot, false,
	); err != nil {
		if errors.Is(err, ErrUtxoAsOfUnavailable) {
			return nil, fmt.Errorf(
				"%w: %w", errGovernanceBoundaryUnreachable, err,
			)
		}
		return nil, err
	}
	// Read the state the boundary found, from the truncated transaction.
	epochs, err := ls.ledgerDB().GetEpochs(txn)
	if err != nil {
		return nil, fmt.Errorf("list epochs after truncation: %w", err)
	}
	if len(epochs) == 0 || epochs[len(epochs)-1].EpochId+1 != epoch.EpochId {
		return nil, fmt.Errorf(
			"%w: truncated ledger does not end in epoch %d",
			errGovernanceBoundaryUnreachable, epoch.EpochId-1,
		)
	}
	prev := epochs[len(epochs)-1]
	prevEra, ok := ls.eraById(prev.EraId)
	if !ok || prevEra == nil {
		return nil, fmt.Errorf("unknown era ID %d", prev.EraId)
	}
	pparams, err := ls.loadPersistedProtocolParameters(
		prev.EpochId, *prevEra, txn,
	)
	if err != nil {
		return nil, fmt.Errorf("load pre-boundary protocol parameters: %w", err)
	}
	if pparams == nil {
		pparams, err = ls.computeGenesisProtocolParameters(*prevEra)
		if err != nil {
			return nil, fmt.Errorf("genesis protocol parameters: %w", err)
		}
	}
	path, ok := ls.eraTransitionPath(prev.EraId, epoch.EraId, true)
	if !ok {
		return nil, fmt.Errorf(
			"cannot replay boundary from era %d to era %d",
			prev.EraId, epoch.EraId,
		)
	}
	target, err := epochAtSlot(prev, prev.StartSlot+uint64(prev.LengthInSlots))
	if err != nil {
		return nil, err
	}
	workingPParams, workingEraID, _, hardForkMajors, plutusV2, err := ls.prepareEraTransitionsForEpochWithOptions(
		ctx, txn, target, *prevEra, pparams, path, true, true,
	)
	if err != nil {
		return nil, err
	}
	workingEra, ok := ls.eraById(workingEraID)
	if !ok || workingEra == nil {
		return nil, fmt.Errorf("unknown era ID %d", workingEraID)
	}
	if _, err := ls.processEpochRolloverWithOptions(
		ctx,
		txn,
		prev,
		*workingEra,
		workingPParams,
		len(path) > 0,
		plutusV2,
		hardForkMajors,
		true,
		&boundaryReplay{spoState: spo},
	); err != nil {
		if errors.Is(err, errRequiredStakeRewardBasisUnavailable) ||
			errors.Is(err, errRewardStakeInputsUnrecoverable) {
			// Retention pruned the reward basis the boundary's reward
			// round reads, so the stores cannot restore it.
			return nil, fmt.Errorf(
				"%w: %w", errGovernanceBoundaryUnreachable, err,
			)
		}
		return nil, err
	}
	snapshot, err := ls.db.GetGovernanceBoundarySnapshotAtSlot(
		ctx, epoch.StartSlot, dbadapter.Unwrap(txn),
	)
	if errors.Is(err, models.ErrGovernanceBoundarySnapshotNotRetained) {
		// The boundary ran no governance rules, so no row belongs to it.
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if snapshot == nil || snapshot.Epoch != epoch.EpochId {
		return nil, nil
	}
	return snapshot, nil
}
