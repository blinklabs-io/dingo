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

package database

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

// truncateAfterSlotDuration records how long a full TruncateAfterSlot sweep
// takes. The sweep's cost is a function of table size rather than rollback
// depth, so on a large metadata database a single depth-1 rollback can hold the
// ledger write lock for tens of seconds. That window is not otherwise
// observable: the ledger simply goes silent between "rolling back to X" and
// "chain rolled back", and chainsync cannot make progress for its duration.
//
// The histogram is a package-level singleton rather than a promauto
// registration so that registering the same or multiple registries never
// panics; see RegisterBlockByHashMetrics for the same reasoning.
var (
	truncateAfterSlotDurationOnce sync.Once
	truncateAfterSlotDuration     *prometheus.HistogramVec
)

// truncateResultSuccess and truncateResultFailure are the values of the
// histogram's "result" label. A failed sweep can have a very different
// duration profile from a successful one (it aborts at whichever sweep
// failed), so mixing them into one distribution would misreport the
// chain-freeze cost.
const (
	truncateResultSuccess = "success"
	truncateResultFailure = "failure"
)

func truncateAfterSlotDurationHistogram() *prometheus.HistogramVec {
	truncateAfterSlotDurationOnce.Do(func() {
		truncateAfterSlotDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "dingo_database_truncate_after_slot_duration_seconds",
				Help: "Duration of TruncateAfterSlot metadata rollback sweeps",
				// 1ms to ~65s: a healthy rollback is milliseconds,
				// a large-database sweep is tens of seconds.
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 17),
			},
			[]string{"result"},
		)
	})
	return truncateAfterSlotDuration
}

// RegisterTruncateMetrics exposes the rollback-truncation duration histogram on
// the given Prometheus registry. Registering the same registry more than once
// is a no-op.
func RegisterTruncateMetrics(reg prometheus.Registerer) error {
	if reg == nil {
		return nil
	}
	err := reg.Register(truncateAfterSlotDurationHistogram())
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
		return err
	}
	return nil
}

// rollbackScope says which stores a rollback to a point reverts.
type rollbackScope uint8

const (
	// rollbackChain moves the ledger's database to the point. Its ledger
	// state is in the LedgerDB, which rolls back from its own journal, so
	// only block-store state is reverted and no SQL statement is issued.
	rollbackChain rollbackScope = iota
	// rollbackRows reverts the metadata rows above the point and leaves the
	// tip where it is.
	rollbackRows
	// rollbackStoredLedger reverts the metadata rows above the point and
	// moves the tip, for a v0.94.2 database whose ledger state is still in
	// its metadata store.
	rollbackStoredLedger
)

// TruncateAfterSlot moves the ledger's database back to point: it removes
// the block-store state recorded for blocks after point.Slot (the offsets of
// the transactions the LedgerDB indexed, when SetLiveUtxoSource named it,
// Leios certification markers and pins, the history-expiry cursor), sets the
// tip to point and returns the
// resulting tip and block nonce. The ledger state itself is in the LedgerDB,
// whose own rollback the caller stages on the same transaction; this issues
// no SQL statement.
//
// mithrilFloor floors the transaction offset deletion at the Mithril ledger
// boundary, if any (pass 0 if there is none): gap blocks applied during a
// Mithril bootstrap record offsets above the ledger tip, and the snapshot is
// never rewound past.
//
// It performs no in-memory cache updates of its own: callers that hold
// in-memory state (epoch cache, era, protocol parameters, chain tip) reload
// it after this returns.
//
// If txn is nil, a new read-write transaction is opened and committed
// internally; otherwise the caller is responsible for committing txn.
func (d *Database) TruncateAfterSlot(
	ctx context.Context,
	point ocommon.Point,
	mithrilFloor uint64,
	txn *Txn,
) (retTip ochainsync.Tip, retNonce []byte, retErr error) {
	return d.rollbackAfterSlot(ctx, point, mithrilFloor, txn, rollbackChain)
}

// RevertStoredLedgerAfterSlot reverts a v0.94.2 database whose ledger state
// is still in its metadata store to point, before that state moves into the
// LedgerDB: every metadata row and blob-referenced UTxO/transaction CBOR
// added strictly after point.Slot is removed (certificates, account reward
// deltas, account/pool/DRep delegation state, protocol parameters,
// governance, epochs, reward state, block nonces, network state, UTxOs and
// transactions), UTxOs spent after point.Slot are restored as unspent, and
// the tip is set to point. txn must carry both a metadata and a blob handle
// (Database.Transaction), so the rows and the block-store state
// commit together; a nil txn opens and commits one.
//
// mithrilFloor floors the UTxO/transaction deletion slot as it does for
// TruncateAfterSlot.
func (d *Database) RevertStoredLedgerAfterSlot(
	ctx context.Context,
	point ocommon.Point,
	mithrilFloor uint64,
	txn *Txn,
) (ochainsync.Tip, []byte, error) {
	return d.rollbackAfterSlot(
		ctx, point, mithrilFloor, txn, rollbackStoredLedger,
	)
}

// RollbackMetadataAfterSlot reverts slot-scoped metadata after point while
// leaving the stored blocks and database tip intact. It applies no Mithril
// floor: every UTxO and transaction row after point is removed.
func (d *Database) RollbackMetadataAfterSlot(
	ctx context.Context,
	point ocommon.Point,
	txn *Txn,
) error {
	_, _, err := d.rollbackAfterSlot(ctx, point, 0, txn, rollbackRows)
	return err
}

func (d *Database) rollbackAfterSlot(
	ctx context.Context,
	point ocommon.Point,
	mithrilFloor uint64,
	txn *Txn,
	scope rollbackScope,
) (retTip ochainsync.Tip, retNonce []byte, retErr error) {
	started := time.Now()
	// Set only when this call opened and committed its own transaction.
	// Distinct from `owned`, which is also false when the caller supplied
	// txn and therefore cannot express "durably committed here".
	committedInternally := false
	defer func() {
		elapsed := time.Since(started)
		// The duration is recorded either way -- a sweep that failed still
		// held the ledger write lock for that long -- but under a label so
		// success and failure are distinguishable.
		result := truncateResultSuccess
		if retErr != nil {
			result = truncateResultFailure
		}
		truncateAfterSlotDurationHistogram().
			WithLabelValues(result).
			Observe(elapsed.Seconds())
		if d.logger == nil {
			return
		}
		// Logged at Info: the ledger holds its write lock across this call,
		// so its duration is the length of a chain-freeze window and needs
		// to be visible without enabling Debug.
		//
		// Note this reports the sweep, not the commit. When the caller
		// supplies txn, it owns the commit and can still roll back after
		// this returns, so the message deliberately says the sweep
		// completed rather than claiming the truncation was durable.
		if retErr != nil {
			d.logger.Warn(
				"metadata rollback truncation failed",
				"component", "database",
				"rollback_slot", point.Slot,
				"duration", elapsed.String(),
				"duration_seconds", elapsed.Seconds(),
				"error", retErr,
			)
			return
		}
		d.logger.Info(
			"metadata rollback truncation sweep complete",
			"component", "database",
			"rollback_slot", point.Slot,
			"duration", elapsed.String(),
			"duration_seconds", elapsed.Seconds(),
			"committed", committedInternally,
		)
	}()
	owned := false
	if txn == nil {
		txn = d.Transaction(ctx, true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	if scope != rollbackChain && txn.Metadata() == nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"%w: reverting metadata rows needs a metadata transaction",
			types.ErrNilTxn,
		)
	}

	if err := d.lowerHistoryExpiryCursor(point.Slot, txn); err != nil {
		return ochainsync.Tip{}, nil, err
	}
	if err := d.lowerHistoryExpiryHeld(point.Slot, txn); err != nil {
		return ochainsync.Tip{}, nil, err
	}

	if scope != rollbackChain {
		if err := d.revertLedgerRowsAfterSlot(ctx, point, txn); err != nil {
			return ochainsync.Tip{}, nil, err
		}
	}
	// Before the UTxO deletes, which can exhaust the blob transaction's
	// budget: a stale marker would keep an endorser block that the selected
	// chain no longer certifies. Only a database that has not finished the
	// Leios store migration holds markers.
	if err := d.deleteLeiosEBCertifiedFromSlot(point.Slot, txn); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"delete Leios certification markers after rollback: %w",
			err,
		)
	}
	// The LeiosDB commits on its own, so the pins certified by the removed
	// ranking blocks are rolled back only after this transaction commits. A
	// crash between the two leaves pins above the chain, which the pin walk
	// reconciles against the immutable point on its next run.
	txn.AfterCommit(func() {
		if _, err := d.RollbackLeiosPins(point); err != nil {
			d.logger.Error(
				"failed to roll back Leios pins after truncate",
				"component", "database",
				"slot", point.Slot,
				"error", err,
			)
		}
	})
	deleteSlot := max(mithrilFloor, point.Slot)
	if scope == rollbackChain {
		if err := d.deleteIndexedTransactionOffsets(deleteSlot, txn); err != nil {
			return ochainsync.Tip{}, nil, err
		}
	} else {
		// Delete rolled-back UTxOs (blob offsets and metadata).
		//
		// Floor the deletion slot at mithrilFloor. UTxOs produced by gap
		// blocks during Mithril bootstrap are written via
		// SetGapBlockTransaction without advancing the ledger tip, so their
		// added_slot values are well above the persisted ledger tip. A
		// rollback whose target is below the Mithril boundary would
		// otherwise bulk-delete every gap-block-produced UTxO, leaving the
		// chain unable to validate the first post-gap block that consumes
		// one of them. The Mithril snapshot is the trust anchor — we never
		// rewind below it — so the authoritative deletion slot is the
		// rollback target or the Mithril boundary, whichever is later.
		if err := d.UtxosDeleteRolledback(ctx, deleteSlot, txn); err != nil {
			return ochainsync.Tip{}, nil, fmt.Errorf(
				"remove rolled-back UTxOs: %w",
				err,
			)
		}
		// Delete rolled-back transaction offsets and metadata
		if err := d.TransactionsDeleteRolledback(ctx, deleteSlot, txn); err != nil {
			return ochainsync.Tip{}, nil, fmt.Errorf(
				"remove rolled-back transactions: %w",
				err,
			)
		}
		// Restore spent UTxOs. Use the same floored slot as the delete calls
		// above so gap-block transactions and the UTxOs they consumed stay
		// in sync: preserving a tx at slot S while restoring its consumed
		// UTxO at deleted_slot=S would leave the tx pointing at a live UTxO
		// it claims to have spent.
		if err := d.UtxosUnspend(ctx, deleteSlot, txn); err != nil {
			return ochainsync.Tip{}, nil, fmt.Errorf(
				"restore spent UTxOs after rollback: %w",
				err,
			)
		}
	}

	if scope == rollbackRows {
		if owned {
			if err := txn.Commit(); err != nil {
				return ochainsync.Tip{}, nil, fmt.Errorf(
					"commit transaction: %w",
					err,
				)
			}
			owned = false
			committedInternally = true
		}
		return ochainsync.Tip{}, nil, nil
	}

	// Build new tip value
	newTip := ochainsync.Tip{
		Point: point,
	}
	var newNonce []byte
	// Origin is the empty-hash point. A block at slot 0 (e.g. the first
	// Preview block) is a real point whose height and nonce must load.
	if point.Slot > 0 || len(point.Hash) > 0 {
		truncateBlock, err := BlockByPointTxn(txn, point)
		if err != nil {
			return ochainsync.Tip{}, nil, fmt.Errorf(
				"failed to get rollback block: %w",
				err,
			)
		}
		newTip.BlockNumber = truncateBlock.Number
		// Load nonce for rollback point
		newNonce, err = d.truncateTargetNonce(point, txn)
		if err != nil {
			return ochainsync.Tip{}, nil, fmt.Errorf(
				"failed to get block nonce: %w",
				err,
			)
		}
		// GetBlockNonce returns (nil, nil) -- not an error -- when no row
		// matches the point. Routine operation only ever keeps block_nonce
		// rows for the last 3 epochs plus each epoch's single checkpoint row
		// (see ledger/state.go's cleanupBlockNoncesBefore), so a disaster-
		// recovery truncate to an older point can land on a slot whose own
		// row has already been pruned. Silently continuing with an empty
		// nonce here would seed the resumed evolving-nonce fold
		// (LedgerState.loadTip -> ledgerProcessBlocks' runningNonce) with
		// the wrong value, corrupting every block nonce computed for the
		// rest of the epoch and, through it, the following epoch's nonce --
		// causing VRF verification to fail for every header in that epoch,
		// from every honest peer, with no error at truncate time to explain
		// why. Byron-era blocks are the sole legitimate empty-nonce case
		// (PBFT has no Praos nonce), so exempt them.
		if len(newNonce) == 0 &&
			truncateBlock.Type != byron.BlockTypeByronEbb &&
			truncateBlock.Type != byron.BlockTypeByronMain {
			// A pruned target nonce is USUALLY reconstructible whenever a
			// checkpoint row (is_checkpoint=1, retained forever, written
			// once per epoch) survives at or before the target's own slot:
			// LedgerState's startup heal (healTruncateGapBlockNonces) folds
			// the evolving nonce forward from that checkpoint through the
			// still-present block CBOR between it and the target -- this
			// truncate only ever deletes blocks strictly AFTER its target,
			// never at or before it, so that history is guaranteed present
			// -- and persists the correct nonce before any epoch nonce or
			// VRF check runs. This package intentionally has no ledger/era
			// or chain-topology knowledge to perform that fold itself (see
			// AGENTS.md's database/ledger boundary), so this check is
			// deliberately coarse: it only verifies that SOME checkpoint row
			// exists at or before the target slot, not that it sits on the
			// current primary chain (a checkpoint written for a
			// later-abandoned fork is never guaranteed cleaned up by every
			// rollback path). The ledger layer's heal re-derives the anchor
			// with full primary-chain awareness and is the actual safety
			// boundary: it hard-fails startup rather than silently leaving
			// the tip nonce empty if the checkpoint this check found turns
			// out not to be usable after all.
			//
			// No checkpoint before the target means reconstruction has
			// nothing to fold from -- reject exactly as before, since
			// resuming nonce computation from an empty value would
			// silently corrupt every epoch nonce computed afterward.
			hasCheckpoint, cpErr := d.truncateHasNonceCheckpoint(
				point.Slot,
				txn,
			)
			if cpErr != nil {
				return ochainsync.Tip{}, nil, fmt.Errorf(
					"check for a block_nonce checkpoint before truncate target: %w",
					cpErr,
				)
			}
			if !hasCheckpoint {
				return ochainsync.Tip{}, nil, fmt.Errorf(
					"truncate target at slot %d (hash %x) has no stored "+
						"block nonce and no earlier checkpoint exists to "+
						"reconstruct it from: the block_nonce row may have "+
						"been pruned by routine 3-epoch retention; resuming "+
						"nonce computation from an empty value would "+
						"silently corrupt the epoch nonce for every epoch "+
						"computed afterward and fail VRF verification for "+
						"the whole following epoch -- choose a truncate "+
						"target within the retained window, or restore the "+
						"block_nonce history for this point first",
					point.Slot,
					point.Hash,
				)
			}
			// Leave newNonce nil: the caller's tip will carry no nonce
			// until LedgerState's startup heal reconstructs it.
		}
	}
	// Write tip to DB
	if err := d.SetTip(newTip, txn); err != nil {
		return ochainsync.Tip{}, nil, fmt.Errorf(
			"failed to set tip: %w",
			err,
		)
	}
	if scope == rollbackStoredLedger {
		if err := d.lowerMetadataTipRow(newTip, txn); err != nil {
			return ochainsync.Tip{}, nil, err
		}
	}
	txn.markTipRevert()

	if owned {
		if err := txn.Commit(); err != nil {
			return ochainsync.Tip{}, nil, fmt.Errorf(
				"commit transaction: %w",
				err,
			)
		}
		owned = false
		committedInternally = true
	}
	return newTip, newNonce, nil
}

// revertLedgerRowsAfterSlot reverts the ledger-state rows the metadata store
// holds for blocks after point.
func (d *Database) revertLedgerRowsAfterSlot(
	ctx context.Context,
	point ocommon.Point,
	txn *Txn,
) error {
	// Restore pool state before deleting any certificates: unlike account
	// restoration (which reads the Account row's own denormalized
	// AddedSlot field, independent of certificate rows),
	// RestorePoolStateAtSlot detects which pools need their denormalized
	// pledge/cost/margin/VRF/reward-account fields reverted by querying
	// PoolRegistration rows with added_slot > slot -- the very rows
	// DeleteCertificatesAfterSlot removes. Restoring first while those
	// rows still exist lets it correctly identify affected pools and
	// reload their fields from the surviving prior registration; deleting
	// certificates first (as this used to) leaves that query finding
	// nothing, silently keeping every re-registered pool's stale,
	// discarded values.
	if err := d.RestorePoolStateAtSlot(
		ctx,
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"restore pool state after rollback: %w",
			err,
		)
	}
	// Delete certificates (they reference transactions)
	if err := d.DeleteCertificatesAfterSlot(
		ctx,
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"delete certificates after rollback: %w",
			err,
		)
	}
	// Revert reward-account changes before account restoration can delete
	// accounts registered after the rollback slot.
	if err := d.DeleteAccountRewardsAfterSlot(
		ctx,
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"delete account reward deltas after rollback: %w",
			err,
		)
	}
	// Restore account delegation state
	if err := d.RestoreAccountStateAtSlot(
		ctx,
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"restore account state after rollback: %w",
			err,
		)
	}
	// Restore DRep state
	if err := d.RestoreDrepStateAtSlot(
		ctx,
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"restore DRep state after rollback: %w",
			err,
		)
	}
	if err := d.DeletePParamsAfterSlot(
		ctx,
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"delete protocol params after rollback: %w",
			err,
		)
	}
	if err := d.DeletePParamUpdatesAfterSlot(
		ctx,
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"delete protocol param updates after rollback: %w",
			err,
		)
	}
	if err := d.DeleteGovernanceProposalsAfterSlot(
		ctx,
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"delete governance proposals after rollback: %w",
			err,
		)
	}
	if err := d.DeleteGovernanceVotesAfterSlot(
		ctx,
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"delete governance votes after rollback: %w",
			err,
		)
	}
	if err := d.DeleteConstitutionsAfterSlot(
		ctx,
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"delete constitutions after rollback: %w",
			err,
		)
	}
	// Delete rolled-back committee state
	if err := d.DeleteCommitteeMembersAfterSlot(
		ctx,
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"delete committee state after rollback: %w",
			err,
		)
	}
	// Delete epoch entries whose nonces were computed from rolled-back
	// blocks. Epochs starting after the rollback slot used blocks that no
	// longer exist, so their nonces are stale and must be recomputed
	// during re-sync.
	if err := d.DeleteEpochsAfterSlot(
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"delete epochs after rollback: %w",
			err,
		)
	}
	// An epoch row survives a rollback that stays inside its epoch, so an era
	// translation applied mid-epoch must be undone explicitly.
	if err := d.revertMidEpochEraSwitchAfterSlot(point.Slot, txn); err != nil {
		return fmt.Errorf(
			"revert mid-epoch era switch after rollback: %w",
			err,
		)
	}
	// Pool stake snapshots and epoch summaries are keyed by epoch number, not
	// slot, so DeleteEpochsAfterSlot does not reach them. Drop those of every
	// epoch the rollback rewound past: a restored ledger snapshot has no
	// distribution for an epoch whose boundary it never crossed, and a
	// competing fork that does not reach that epoch must not leave one behind
	// for leader-schedule, stake-distribution or epoch API reads.
	if err := d.deleteEpochKeyedStateAfterRemainingEpochs(
		point.Slot,
		txn,
	); err != nil {
		return err
	}
	// Delete reward-state rows captured at epoch boundaries that no
	// longer exist on the selected chain.
	if err := d.DeleteRewardStateAfterSlot(
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"delete reward state after rollback: %w",
			err,
		)
	}
	// Delete block nonce rows from the abandoned fork. Epoch nonces are
	// derived from slot-range block_nonce lookups, so same-slot
	// competitors and later fork rows must not survive rollback.
	if err := d.deleteStoredBlockNoncesAfterPoint(
		point,
		txn,
	); err != nil {
		return fmt.Errorf(
			"delete block nonces after rollback: %w",
			err,
		)
	}
	// Delete rolled-back network state records
	if err := d.DeleteNetworkStateAfterSlot(
		ctx,
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"delete network state after rollback: %w",
			err,
		)
	}
	// Delete rolled-back treasury donation records
	if err := d.DeleteNetworkDonationsAfterSlot(
		ctx,
		point.Slot,
		txn,
	); err != nil {
		return fmt.Errorf(
			"delete network donations after rollback: %w",
			err,
		)
	}
	return nil
}

// deleteIndexedTransactionOffsets removes the offsets of the transactions
// the LedgerDB indexed after slot. A blob delete failure leaves unreachable
// objects, which are counted and logged, and does not stop the rollback.
func (d *Database) deleteIndexedTransactionOffsets(
	slot uint64,
	txn *Txn,
) error {
	if d.txIndex == nil {
		return nil
	}
	hashes, err := d.txIndex.TxHashesAfterSlot(nil, slot)
	if err != nil {
		return fmt.Errorf(
			"list indexed transactions after slot %d: %w", slot, err,
		)
	}
	if blobErr := deleteTxBlobs(d, hashes, txn); blobErr != nil {
		d.logger.Error(
			"rolled-back transaction blob delete left unreachable objects",
			"error", blobErr,
			"slot", slot,
			"transactions", len(hashes),
		)
	}
	return nil
}

// deleteEpochKeyedStateAfterRemainingEpochs deletes the pool stake snapshot
// and epoch summary rows of every epoch above the newest epoch a rollback to
// slot leaves.
func (d *Database) deleteEpochKeyedStateAfterRemainingEpochs(
	slot uint64,
	txn *Txn,
) error {
	lastEpoch, found, err := d.newestEpochAtOrBefore(slot, txn)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	store := d.stakeSnapshotStore()
	if err := store.DeletePoolStakeSnapshotsAfterEpoch(
		lastEpoch,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"delete pool stake snapshots after rollback: %w",
			err,
		)
	}
	if err := store.DeleteEpochSummariesAfterEpoch(
		lastEpoch,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"delete epoch summaries after rollback: %w",
			err,
		)
	}
	return nil
}
