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

package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros_cbor "github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const backfillPhase = "metadata"

// Backfill replays stored blocks to populate historical metadata.
// It is triggered automatically during Mithril sync when
// storageMode is "api".
type Backfill struct {
	db           *database.Database
	nodeCfg      *cardano.CardanoNodeConfig
	logger       *slog.Logger
	epochs       []models.Epoch
	pparamsCache map[uint64]lcommon.ProtocolParameters

	// Running state tracked across blocks.
	currentPParams lcommon.ProtocolParameters
	currentEraId   uint
	lastEpochId    uint64
	runningNonce   []byte
	computeNonces  bool
	epochIdx       int // moving index into epochs for O(1) lookup
}

// NewBackfill creates a new Backfill instance.
func NewBackfill(
	db *database.Database,
	nodeCfg *cardano.CardanoNodeConfig,
	logger *slog.Logger,
) *Backfill {
	return &Backfill{
		db:            db,
		nodeCfg:       nodeCfg,
		logger:        logger,
		computeNonces: nodeCfg != nil,
	}
}

// NeedsBackfill checks if there's an incomplete backfill checkpoint.
// Returns true only when a checkpoint exists and is not yet completed.
func (b *Backfill) NeedsBackfill() (bool, error) {
	cp, err := b.db.Metadata().GetBackfillCheckpoint(
		backfillPhase, nil,
	)
	if err != nil {
		return false, fmt.Errorf(
			"checking backfill checkpoint: %w", err,
		)
	}
	return cp != nil && !cp.Completed, nil
}

// loadEpochs loads all epoch boundaries from the database.
func (b *Backfill) loadEpochs() error {
	epochs, err := b.db.GetEpochs(nil)
	if err != nil {
		return fmt.Errorf("loading epochs: %w", err)
	}
	b.epochs = epochs
	b.pparamsCache = make(map[uint64]lcommon.ProtocolParameters)
	return nil
}

// slotToEpoch finds the epoch containing the given slot.
// Returns the epoch ID and era ID. If no epoch contains the
// slot, returns 0, 0.
//
// Uses a moving index (epochIdx) that advances forward as
// blocks arrive in slot order, giving O(1) amortized lookup.
// Falls back to reverse scan for out-of-order lookups.
func (b *Backfill) slotToEpoch(
	slot uint64,
) (epochId uint64, eraId uint) {
	n := len(b.epochs)
	if n == 0 {
		return 0, 0
	}
	// Try advancing the moving index forward
	for b.epochIdx < n-1 &&
		slot >= b.epochs[b.epochIdx+1].StartSlot {
		b.epochIdx++
	}
	ep := &b.epochs[b.epochIdx]
	if slot >= ep.StartSlot {
		return ep.EpochId, ep.EraId
	}
	// Fallback: reverse scan for out-of-order lookups
	for i := b.epochIdx - 1; i >= 0; i-- {
		ep = &b.epochs[i]
		if slot >= ep.StartSlot {
			b.epochIdx = i
			return ep.EpochId, ep.EraId
		}
	}
	return 0, 0
}

// updateQuorum returns the pparam update quorum from the
// shelley genesis config. Returns 0 if not available.
func (b *Backfill) updateQuorum() int {
	if b.nodeCfg == nil {
		return 0
	}
	sg := b.nodeCfg.ShelleyGenesis()
	if sg == nil {
		return 0
	}
	return sg.UpdateQuorum
}

// bootstrapEraChain walks through all eras from Shelley
// (era 1) up to and including targetEraId, calling each
// era's HardForkFunc in sequence. This is needed on
// networks like preview where the first epoch starts at a
// later era (e.g. Alonzo) and earlier eras were traversed
// at genesis.
func (b *Backfill) bootstrapEraChain(
	targetEraId uint,
) error {
	for _, era := range eras.Eras {
		if era.Id == 0 {
			continue // Skip Byron (no pparams)
		}
		if era.Id > targetEraId {
			break
		}
		if era.HardForkFunc == nil {
			continue
		}
		newPP, err := era.HardForkFunc(
			b.nodeCfg, b.currentPParams,
		)
		if err != nil {
			return fmt.Errorf(
				"bootstrap era %d (%s): %w",
				era.Id, era.Name, err,
			)
		}
		b.currentPParams = newPP
		b.currentEraId = era.Id
		b.logger.Info(
			"bootstrapped era pparams",
			"component", "backfill",
			"era", era.Name,
			"era_id", era.Id,
		)
	}
	return nil
}

// resolvePParams walks all epochs from genesis up to and
// including targetEpoch, computing protocol parameters at
// each step. At era transitions it calls HardForkFunc; at
// regular epoch boundaries it applies any stored pparam
// update proposals. Results are cached and stored in the DB.
func (b *Backfill) resolvePParams(
	targetEpoch uint64,
) error {
	if b.nodeCfg == nil {
		b.logger.Warn(
			"no node config, skipping pparams resolution",
			"component", "backfill",
		)
		return nil
	}

	// On networks like preview, the first epoch may
	// start at a later era. Bootstrap through all
	// intermediate eras to build the pparams chain.
	if len(b.epochs) > 0 && b.epochs[0].EraId > 1 {
		if err := b.bootstrapEraChain(
			b.epochs[0].EraId,
		); err != nil {
			return fmt.Errorf(
				"bootstrapping era chain: %w", err,
			)
		}
	}

	quorum := b.updateQuorum()
	prevEraId := b.currentEraId
	for i := range b.epochs {
		ep := &b.epochs[i]
		if ep.EpochId > targetEpoch {
			break
		}
		// Era transition: hard fork produces new pparams
		if ep.EraId != prevEraId {
			era := eras.GetEraById(ep.EraId)
			if era != nil && era.HardForkFunc != nil {
				newPP, err := era.HardForkFunc(
					b.nodeCfg, b.currentPParams,
				)
				if err != nil {
					return fmt.Errorf(
						"hard fork to era %d at epoch %d: %w",
						ep.EraId, ep.EpochId, err,
					)
				}
				// Store genesis pparams for this era
				ppCbor, encErr := ouroboros_cbor.Encode(
					&newPP,
				)
				if encErr != nil {
					return fmt.Errorf(
						"encode pparams for era %d: %w",
						ep.EraId, encErr,
					)
				}
				if err := b.db.SetPParams(
					ppCbor, ep.StartSlot,
					ep.EpochId, ep.EraId, nil,
				); err != nil {
					return fmt.Errorf(
						"store pparams for epoch %d: %w",
						ep.EpochId, err,
					)
				}
				b.currentPParams = newPP
			}
			prevEraId = ep.EraId
			b.currentEraId = ep.EraId
		} else if b.currentPParams != nil {
			// Same era: apply pparam update proposals
			era := eras.GetEraById(ep.EraId)
			if era != nil &&
				era.DecodePParamsUpdateFunc != nil &&
				era.PParamsUpdateFunc != nil {
				newPP, ppErr := b.db.ComputeAndApplyPParamUpdates(
					ep.StartSlot, ep.EpochId,
					ep.EraId, quorum,
					b.currentPParams,
					era.DecodePParamsUpdateFunc,
					era.PParamsUpdateFunc, nil,
				)
				if ppErr != nil {
					b.logger.Warn(
						"pparam update resolution failed",
						"component", "backfill",
						"epoch", ep.EpochId,
						"error", ppErr,
					)
				} else {
					b.currentPParams = newPP
				}
			}
		}
		// Store pparams for first epoch if bootstrapped
		if i == 0 && b.currentPParams != nil {
			ppCbor, encErr := ouroboros_cbor.Encode(
				&b.currentPParams,
			)
			if encErr != nil {
				return fmt.Errorf(
					"encode pparams for first epoch %d: %w",
					ep.EpochId, encErr,
				)
			}
			if err := b.db.SetPParams(
				ppCbor, ep.StartSlot,
				ep.EpochId, ep.EraId, nil,
			); err != nil {
				return fmt.Errorf(
					"store pparams for first epoch %d: %w",
					ep.EpochId, err,
				)
			}
		}
		b.pparamsCache[ep.EpochId] = b.currentPParams
		b.lastEpochId = ep.EpochId
	}
	return nil
}

// processEpochBoundary handles the transition to a new
// epoch during block iteration. If resolvePParams already
// cached pparams for this epoch, use them directly. Only
// falls back to computing when no cache entry exists
// (e.g. nodeCfg was nil during resolve).
func (b *Backfill) processEpochBoundary(
	epochId uint64, eraId uint,
) {
	// Use cached pparams from resolvePParams if available
	if pp, ok := b.pparamsCache[epochId]; ok {
		b.currentPParams = pp
	}
	b.currentEraId = eraId
	b.lastEpochId = epochId
}

// recoverNonce loads the running nonce from the DB for resume.
// If the last block's nonce can't be found (e.g. previous run
// didn't compute nonces), nonce computation is disabled.
func (b *Backfill) recoverNonce(lastSlot uint64) {
	if !b.computeNonces {
		return
	}
	// Byron era: nonce starts as nil
	_, eraId := b.slotToEpoch(lastSlot)
	if eraId == 0 {
		return
	}
	// Get the block at lastSlot to look up its stored nonce
	it := b.db.BlocksFromSlot(lastSlot)
	defer it.Close()
	blk, err := it.NextRaw()
	if err != nil || blk == nil || blk.Slot != lastSlot {
		b.logger.Warn(
			"cannot find block for nonce recovery, "+
				"disabling nonce computation",
			"component", "backfill",
			"slot", lastSlot,
		)
		b.computeNonces = false
		return
	}
	point := ocommon.NewPoint(blk.Slot, blk.Hash)
	nonce, err := b.db.GetBlockNonce(point, nil)
	if err != nil || len(nonce) == 0 {
		b.logger.Warn(
			"no stored nonce for resume point, "+
				"disabling nonce computation",
			"component", "backfill",
			"slot", lastSlot,
		)
		b.computeNonces = false
		return
	}
	b.runningNonce = nonce
}

// computeBlockNonce calculates and stores the VRF rolling
// nonce for the given block. Updates b.runningNonce.
// isCheckpoint should be true for the first block in each
// epoch (must be computed before processEpochBoundary
// updates lastEpochId).
func (b *Backfill) computeBlockNonce(
	parsedBlock gledger.Block,
	point ocommon.Point,
	eraId uint,
	isCheckpoint bool,
	txn *database.Txn,
) error {
	if !b.computeNonces {
		return nil
	}
	era := eras.GetEraById(eraId)
	if era == nil || era.CalculateEtaVFunc == nil {
		return nil
	}
	nonce, err := era.CalculateEtaVFunc(
		b.nodeCfg, b.runningNonce, parsedBlock,
	)
	if err != nil {
		// Calculation failures are non-fatal: some eras or
		// early blocks may not support nonce computation.
		b.logger.Debug(
			"block nonce calculation failed",
			"component", "backfill",
			"slot", point.Slot,
			"error", err,
		)
		return nil
	}
	b.runningNonce = nonce

	if err := b.db.SetBlockNonce(
		point.Hash, point.Slot,
		nonce, isCheckpoint, txn,
	); err != nil {
		return fmt.Errorf(
			"storing block nonce at slot %d: %w",
			point.Slot, err,
		)
	}
	return nil
}

// getPParams returns cached pparams for the given epoch.
// Falls back to currentPParams if not in cache.
func (b *Backfill) getPParams(
	epochId uint64,
) lcommon.ProtocolParameters {
	if pp, ok := b.pparamsCache[epochId]; ok {
		return pp
	}
	// Not in cache: use current pparams and cache it
	b.pparamsCache[epochId] = b.currentPParams
	return b.currentPParams
}

// calculateCertDeposits computes deposit amounts for each
// certificate in the transaction.
func (b *Backfill) calculateCertDeposits(
	tx lcommon.Transaction,
	eraId uint,
	pp lcommon.ProtocolParameters,
) map[int]uint64 {
	certs := tx.Certificates()
	certDeposits := make(map[int]uint64, len(certs))
	if pp == nil {
		return certDeposits
	}
	era := eras.GetEraById(eraId)
	if era == nil || era.CertDepositFunc == nil {
		return certDeposits
	}
	for i, cert := range certs {
		deposit, err := era.CertDepositFunc(cert, pp)
		if err != nil {
			if errors.Is(
				err, eras.ErrIncompatibleProtocolParams,
			) {
				continue
			}
			b.logger.Debug(
				"cert deposit calculation failed",
				"component", "backfill",
				"cert_index", i,
				"error", err,
			)
			continue
		}
		certDeposits[i] = deposit
	}
	return certDeposits
}

// processBlockGovernance calls governance processing for
// valid Conway-era transactions that have proposals or votes.
func (b *Backfill) processBlockGovernance(
	tx lcommon.Transaction,
	point ocommon.Point,
	epochId uint64,
	pp lcommon.ProtocolParameters,
	txn *database.Txn,
) error {
	if !tx.IsValid() {
		return nil
	}
	proposals := tx.ProposalProcedures()
	votes := tx.VotingProcedures()
	if len(proposals) == 0 && len(votes) == 0 {
		return nil
	}
	conwayPP, ok := pp.(*conway.ConwayProtocolParameters)
	if !ok {
		return nil
	}
	if len(proposals) > 0 {
		if err := ledger.ProcessGovernanceProposals(
			tx, point, epochId,
			conwayPP.GovActionValidityPeriod,
			b.db, txn,
		); err != nil {
			return fmt.Errorf(
				"governance proposals: %w", err,
			)
		}
	}
	if len(votes) > 0 {
		if err := ledger.ProcessGovernanceVotes(
			tx, point, epochId,
			conwayPP.DRepInactivityPeriod,
			b.db, txn,
		); err != nil {
			return fmt.Errorf(
				"governance votes: %w", err,
			)
		}
	}
	return nil
}

// Run executes the backfill. Blocks until complete or context
// cancelled. Safe to call multiple times; uses checkpoints for
// resume.
func (b *Backfill) Run(ctx context.Context) error {
	cp, err := b.db.Metadata().GetBackfillCheckpoint(
		backfillPhase, nil,
	)
	if err != nil {
		return fmt.Errorf("reading backfill checkpoint: %w", err)
	}
	if cp != nil && cp.Completed {
		b.logger.Info(
			"backfill already completed",
			"component", "backfill",
		)
		return nil
	}

	tipBlocks, err := database.BlocksRecent(b.db, 1)
	if err != nil {
		return fmt.Errorf("reading chain tip: %w", err)
	}
	if len(tipBlocks) == 0 {
		b.logger.Info(
			"no blocks in blob store, nothing to backfill",
			"component", "backfill",
		)
		return nil
	}
	tipSlot := tipBlocks[0].Slot

	// Load epoch boundaries for slot-to-epoch mapping.
	if err := b.loadEpochs(); err != nil {
		return fmt.Errorf("loading epoch data: %w", err)
	}

	// Resolve protocol parameters for all epochs. This seeds
	// genesis pparams at era transitions and applies stored
	// pparam update proposals from any previous run.
	tipEpochId, _ := b.slotToEpoch(tipSlot)
	if err := b.resolvePParams(tipEpochId); err != nil {
		return fmt.Errorf(
			"resolving protocol parameters: %w", err,
		)
	}

	var startSlot uint64
	now := time.Now()
	if cp == nil {
		cp = &models.BackfillCheckpoint{
			Phase:      backfillPhase,
			LastSlot:   0,
			TotalSlots: tipSlot,
			StartedAt:  now,
			UpdatedAt:  now,
		}
		if err := b.db.Metadata().SetBackfillCheckpoint(
			cp, nil,
		); err != nil {
			return fmt.Errorf(
				"creating backfill checkpoint: %w", err,
			)
		}
		b.logger.Info(
			"starting metadata backfill",
			"component", "backfill",
			"tip_slot", tipSlot,
		)
		// Fresh start: reset epoch/era tracking to the
		// first epoch so processEpochBoundary doesn't
		// misinterpret the first block as an era change.
		if len(b.epochs) > 0 {
			b.lastEpochId = b.epochs[0].EpochId
			b.currentEraId = b.epochs[0].EraId
			pp := b.getPParams(b.epochs[0].EpochId)
			if pp != nil {
				b.currentPParams = pp
			}
		}
	} else {
		startSlot = cp.LastSlot + 1
		b.logger.Info(
			"resuming metadata backfill",
			"component", "backfill",
			"resume_from_slot", cp.LastSlot,
			"tip_slot", tipSlot,
		)
		// Recover running nonce from the last processed
		// block so nonce computation can continue.
		b.recoverNonce(cp.LastSlot)

		// Set epoch tracking to resume point
		epochId, eraId := b.slotToEpoch(cp.LastSlot)
		b.lastEpochId = epochId
		b.currentEraId = eraId
		pp := b.getPParams(epochId)
		if pp != nil {
			b.currentPParams = pp
		}
	}

	it := b.db.BlocksFromSlot(startSlot)
	defer it.Close()

	var (
		processedBlocks int
		processedTxs    int
		lastLogTime     = time.Now()
		startTime       = time.Now()
	)

	for {
		if err := ctx.Err(); err != nil {
			b.saveCheckpoint(cp)
			return fmt.Errorf("cancelled: %w", err)
		}

		blk, err := it.NextRaw()
		if err != nil {
			b.saveCheckpoint(cp)
			return fmt.Errorf("iterating blocks: %w", err)
		}
		if blk == nil {
			break // iteration complete
		}

		epochId, eraId := b.slotToEpoch(blk.Slot)

		// Capture before processEpochBoundary updates
		// lastEpochId, so the first block in each epoch
		// is correctly marked as a nonce checkpoint.
		isNewEpoch := epochId != b.lastEpochId

		// Detect epoch boundary and update pparams
		if isNewEpoch {
			b.processEpochBoundary(epochId, eraId)
		}

		pp := b.getPParams(epochId)

		// Process block. Nesting avoids early-continue so
		// every path reaches the common tail below.
		var blockTxCount int

		parsedBlock, parseErr := gledger.NewBlockFromCbor(
			blk.BlockType, blk.Cbor,
		)
		if parseErr != nil {
			b.logger.Warn(
				"skipping unparseable block",
				"component", "backfill",
				"slot", blk.Slot,
				"error", parseErr,
			)
		} else {
			point := ocommon.NewPoint(
				blk.Slot, blk.Hash,
			)

			// Compute and store block nonce
			if nErr := b.computeBlockNonce(
				parsedBlock, point, eraId,
				isNewEpoch, nil,
			); nErr != nil {
				b.saveCheckpoint(cp)
				return fmt.Errorf(
					"block nonce at slot %d: %w",
					blk.Slot, nErr,
				)
			}

			txs := parsedBlock.Transactions()
			if len(txs) > 0 {
				indexer := database.NewBlockIndexer(
					blk.Slot, blk.Hash,
				)
				offsets, oErr := indexer.ComputeOffsets(
					blk.Cbor, parsedBlock,
				)
				if oErr != nil {
					b.logger.Warn(
						"skipping block with offset error",
						"component", "backfill",
						"slot", blk.Slot,
						"error", oErr,
					)
				} else {
					if pErr := b.processBlockTxs(
						txs, point, epochId, eraId,
						pp, offsets,
					); pErr != nil {
						b.saveCheckpoint(cp)
						return fmt.Errorf(
							"processing block at slot %d: %w",
							blk.Slot, pErr,
						)
					}
					blockTxCount = len(txs)
				}
			}
		}

		// Common tail: update progress for every block.
		processedTxs += blockTxCount
		cp.LastSlot = blk.Slot
		processedBlocks++

		if processedBlocks%1000 == 0 {
			b.saveCheckpoint(cp)
		}
		b.maybeLogProgress(
			cp, processedBlocks, processedTxs,
			tipSlot, startSlot, startTime, &lastLogTime,
		)
	}

	cp.Completed = true
	cp.UpdatedAt = time.Now()
	if err := b.db.Metadata().SetBackfillCheckpoint(
		cp, nil,
	); err != nil {
		return fmt.Errorf(
			"saving final backfill checkpoint: %w", err,
		)
	}

	elapsed := time.Since(startTime)
	b.logger.Info(
		"metadata backfill complete",
		"component", "backfill",
		"blocks_processed", processedBlocks,
		"transactions_stored", processedTxs,
		"elapsed", elapsed.Round(time.Second),
	)
	return nil
}

// processBlockTxs stores transactions and processes governance
// within a single database transaction.
func (b *Backfill) processBlockTxs(
	txs []lcommon.Transaction,
	point ocommon.Point,
	epochId uint64,
	eraId uint,
	pp lcommon.ProtocolParameters,
	offsets *database.BlockIngestionResult,
) error {
	txn := b.db.Transaction(true)
	defer txn.Release()
	for i, tx := range txs {
		updateEpoch, paramUpdates := tx.ProtocolParameterUpdates()
		certDeposits := b.calculateCertDeposits(
			tx, eraId, pp,
		)
		if err := b.db.SetTransaction(
			tx, point, uint32(i), // #nosec G115
			updateEpoch, paramUpdates,
			certDeposits, offsets, txn,
		); err != nil {
			return fmt.Errorf("storing TX: %w", err)
		}
		if err := b.processBlockGovernance(
			tx, point, epochId, pp, txn,
		); err != nil {
			return fmt.Errorf(
				"governance at slot %d tx %d: %w",
				point.Slot, i, err,
			)
		}
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf(
			"commit at slot %d: %w", point.Slot, err,
		)
	}
	return nil
}

// saveCheckpoint persists the current backfill progress.
func (b *Backfill) saveCheckpoint(cp *models.BackfillCheckpoint) {
	cp.UpdatedAt = time.Now()
	if err := b.db.Metadata().SetBackfillCheckpoint(
		cp, nil,
	); err != nil {
		b.logger.Warn(
			"failed to save backfill checkpoint",
			"component", "backfill",
			"error", err,
		)
	}
}

// maybeLogProgress logs backfill progress with rate and ETA,
// throttled to at most once every 10 seconds.
func (b *Backfill) maybeLogProgress(
	cp *models.BackfillCheckpoint,
	processedBlocks int,
	processedTxs int,
	tipSlot uint64,
	startSlot uint64,
	startTime time.Time,
	lastLogTime *time.Time,
) {
	now := time.Now()
	if now.Sub(*lastLogTime) < 10*time.Second {
		return
	}
	*lastLogTime = now

	elapsed := now.Sub(startTime)
	blocksPerSec := float64(processedBlocks) / elapsed.Seconds()
	var pct float64
	if tipSlot > 0 {
		pct = float64(cp.LastSlot) / float64(tipSlot) * 100
	}

	attrs := []any{
		"component", "backfill",
		"slot", cp.LastSlot,
		"blocks", processedBlocks,
		"transactions", processedTxs,
		"blocks_per_sec", fmt.Sprintf("%.0f", blocksPerSec),
		"progress", fmt.Sprintf("%.1f%%", pct),
	}

	slotsProcessed := cp.LastSlot - startSlot
	if blocksPerSec > 0 && slotsProcessed > 0 &&
		cp.LastSlot < tipSlot {
		remainingSlots := float64(tipSlot - cp.LastSlot)
		remainingBlocks := remainingSlots *
			float64(processedBlocks) /
			float64(slotsProcessed)
		if remainingBlocks > 0 {
			etaSec := remainingBlocks / blocksPerSec
			eta := time.Duration(etaSec) * time.Second
			attrs = append(
				attrs, "eta", eta.Round(time.Second),
			)
		}
	}

	b.logger.Info("backfill progress", attrs...)
}
