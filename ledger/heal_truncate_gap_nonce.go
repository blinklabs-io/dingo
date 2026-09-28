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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// healTruncateGapBlockNonces reconstructs the current tip's block_nonce when
// a disaster-recovery truncate (database/lifecycle.Truncate, CIP-0135) landed
// on a target whose own block_nonce row had already been pruned by routine
// 3-epoch retention (cleanupBlockNoncesBefore /
// DeleteBlockNoncesBeforeSlotWithoutCheckpoints).
//
// database.TruncateAfterSlot allows such a truncate to proceed -- rather
// than rejecting it outright -- as long as a checkpoint row (is_checkpoint,
// retained forever regardless of the 3-epoch window; written once per epoch,
// see ledgerProcessBlocks's "First block we persist in the current epoch
// becomes the checkpoint") survives at or before the target's own slot. This
// heal performs the reconstruction that decision defers: it folds the
// evolving nonce forward from that checkpoint through every intervening
// block's still-present CBOR up to the tip and persists a correct nonce for
// each. Truncate's own DeleteBlocksAfter only removes blocks strictly AFTER
// its target -- resuming sync from the target requires everything at or
// below it to remain -- so that block history is guaranteed present; what is
// missing is only the computed block_nonce cache pruning already discarded.
//
// This shares its core per-block fold (foldBlockEtaV) with
// healMithrilGapBlockNonces so the two nonce-repair paths can never compute a
// block's evolving-nonce contribution differently, but differs in anchor
// selection (the nearest checkpoint at or before the tip, not the highest
// valid nonce below a recorded Mithril trust boundary) and needs no separate
// completion marker: the tip's own valid nonce IS the completion signal this
// heal checks on entry, so a crash mid-heal simply leaves it empty and the
// heal harmlessly redoes the whole fold on the next start (a pure function of
// already-stored chain data, like the Mithril heal).
//
// It must run after loadTip (and after healMithrilGapBlockNonces, so a
// Mithril-healed tip nonce is already reflected) and before any epoch nonce
// is computed by header verification -- the same ordering constraint as the
// Mithril heal, and for the same reason: an epoch nonce folded from a wrong
// or empty evolving nonce fails VRF verification for every header in that
// epoch from every honest peer, with no error at truncate time to explain
// why.
//
// When no reconstructable checkpoint exists, this HARD-FAILS (returns an
// error, which both call sites treat as fatal) rather than warning and
// leaving state as-is. This deliberately does NOT mirror
// healMithrilGapBlockNonces's own no-anchor case: database.TruncateAfterSlot's
// own guard is a coarse, chain-topology-blind check (it verifies a
// checkpoint row exists at or before the target slot, not that it sits on
// the current primary chain -- see its doc comment), so by the time this
// heal runs with an empty tip nonce and Byron already excluded,
// TruncateAfterSlot has already promised a checkpoint exists. If this
// heal's own primary-chain-aware search still can't find one, that promise
// was not actually kept -- most plausibly because the only checkpoint row
// at or before the target belongs to a since-abandoned fork that survived
// whatever rollback should have pruned it -- and continuing with an empty
// nonce would reproduce exactly the corruption this whole mechanism exists
// to prevent, just discovered one layer later. A tip with an empty nonce
// and truly no checkpoint below it at all (e.g. a test harness that seeds
// the primary chain index directly without ever computing nonces) is
// equally not repairable, so it fails the same way.
func (ls *LedgerState) healTruncateGapBlockNonces(ctx context.Context) error {
	ls.RLock()
	tipPoint := ls.currentTip.Point
	tipNonce := bytes.Clone(ls.currentTipBlockNonce)
	ls.RUnlock()

	// Genesis has no block and no nonce to reconstruct.
	if tipPoint.Slot == 0 {
		return nil
	}
	// A valid tip nonce means either there was never a gap, or a previous
	// run of this heal (or healMithrilGapBlockNonces) already repaired it.
	// This is what makes re-entry after a crash safe: the fold below is
	// only ever attempted while this is false.
	if len(tipNonce) == lcommon.Blake2b256Size {
		return nil
	}

	tipBlock, err := database.BlockByPoint(ls.db, tipPoint)
	if err != nil {
		return fmt.Errorf(
			"load tip block for truncate gap nonce heal: %w",
			err,
		)
	}
	if tipBlock.Type == byron.BlockTypeByronEbb ||
		tipBlock.Type == byron.BlockTypeByronMain {
		// Byron blocks carry no Praos VRF nonce; an empty tip nonce here is
		// correct, not a gap. Mirrors TruncateAfterSlot's own Byron
		// exemption.
		return nil
	}

	// Seed: the nearest checkpoint at or before the tip. Malformed rows
	// (e.g. a DB predating evolving-nonce storage) are ignored rather than
	// allowed to mask a usable lower-slot checkpoint.
	rows, err := ls.db.GetBlockNoncesInSlotRange(0, tipPoint.Slot+1, nil)
	if err != nil {
		return fmt.Errorf(
			"query checkpoint block nonces for truncate gap heal: %w",
			err,
		)
	}
	checkpointCandidates := make([]models.BlockNonce, 0, len(rows))
	for i := range rows {
		row := rows[i]
		if !row.IsCheckpoint || len(row.Nonce) != lcommon.Blake2b256Size {
			continue
		}
		checkpointCandidates = append(checkpointCandidates, row)
	}
	sort.SliceStable(checkpointCandidates, func(i, j int) bool {
		if checkpointCandidates[i].Slot != checkpointCandidates[j].Slot {
			return checkpointCandidates[i].Slot > checkpointCandidates[j].Slot
		}
		return bytes.Compare(
			checkpointCandidates[i].Hash,
			checkpointCandidates[j].Hash,
		) > 0
	})

	var (
		anchorRow  *models.BlockNonce
		anchorIter *chain.ChainIterator
	)
	if ls.chain != nil {
		for i := range checkpointCandidates {
			candidate := &checkpointCandidates[i]
			contains, cErr := ls.primaryChainContainsExactPoint(
				ocommon.Point{Slot: candidate.Slot, Hash: candidate.Hash},
			)
			if cErr != nil {
				return fmt.Errorf(
					"check truncate-gap checkpoint on primary chain: %w",
					cErr,
				)
			}
			if !contains {
				continue
			}
			iter, iErr := ls.chain.FromPointContext(
				ctx,
				ocommon.Point{Slot: candidate.Slot, Hash: candidate.Hash},
				false,
			)
			if iErr != nil {
				if errors.Is(iErr, models.ErrBlockNotFound) {
					continue
				}
				return fmt.Errorf(
					"create primary-chain iterator from truncate-gap checkpoint: %w",
					iErr,
				)
			}
			anchorRow = candidate
			anchorIter = iter
			break
		}
		if anchorRow == nil {
			// Every checkpoint row at or before the tip belongs to a
			// since-abandoned fork (or none exist at all): TruncateAfterSlot's
			// own guard checked only row existence, not primary-chain
			// membership, so its promise that a usable checkpoint exists was
			// not kept. Continuing with an empty tip nonce here would
			// reproduce the exact silent VRF corruption this mechanism
			// exists to prevent -- fail loudly instead.
			return fmt.Errorf(
				"truncate gap nonce heal found no block_nonce checkpoint "+
					"at or before tip slot %d that is actually on the "+
					"primary chain: database.TruncateAfterSlot's own guard "+
					"only checks that a checkpoint row exists, not that it "+
					"sits on the current primary chain, so a checkpoint "+
					"belonging to a since-abandoned fork can pass that "+
					"guard without being usable here; resuming with an "+
					"empty nonce would corrupt VRF verification for the "+
					"whole following epoch",
				tipPoint.Slot,
			)
		}
	} else if len(checkpointCandidates) > 0 {
		anchorRow = &checkpointCandidates[0]
	}
	if anchorRow == nil {
		// No usable checkpoint at all -- either no chain index is attached
		// (offline tooling) and no checkpoint row exists regardless, or (with
		// a chain index attached) already returned above. A tip with an
		// empty nonce and nothing to reconstruct from means this chain never
		// wrote block_nonce history through normal ledger block application
		// (e.g. a test harness that seeds the chain index directly) -- not a
		// repairable gap, so fail the same way rather than silently leaving
		// the tip nonce empty.
		return fmt.Errorf(
			"truncate gap nonce heal found no block_nonce checkpoint row "+
				"at or before tip slot %d: resuming with an empty nonce "+
				"would corrupt VRF verification for the whole following "+
				"epoch",
			tipPoint.Slot,
		)
	}
	if anchorIter != nil {
		defer anchorIter.Cancel()
	}
	anchorSlot := anchorRow.Slot
	anchorNonce := anchorRow.Nonce

	if anchorSlot >= tipPoint.Slot {
		// The checkpoint IS the tip (or, impossibly, after it). A valid
		// checkpoint row exactly at tipPoint.Slot would already have been
		// loaded as ls.currentTipBlockNonce by loadTip, so this is
		// unreachable in practice; guard anyway rather than run a
		// zero-or-negative-length fold that could look like success.
		return fmt.Errorf(
			"truncate gap nonce heal found checkpoint at slot %d, at or "+
				"after tip slot %d, but the tip's own block_nonce row is "+
				"missing or invalid",
			anchorSlot,
			tipPoint.Slot,
		)
	}

	// Preserve existing checkpoint flags on any rows in range that already
	// exist (there should be none below the tip after a fresh gap, but a
	// heal re-run after a partial previous attempt could find some).
	existingRows, err := ls.db.GetBlockNoncesInSlotRange(
		anchorSlot+1,
		tipPoint.Slot+1,
		nil,
	)
	if err != nil {
		return fmt.Errorf(
			"query existing post-checkpoint block nonces: %w",
			err,
		)
	}
	checkpointSlots := make(map[uint64]bool, len(existingRows))
	for _, row := range existingRows {
		if row.IsCheckpoint {
			checkpointSlots[row.Slot] = true
		}
	}

	ls.config.Logger.Warn(
		"reconstructing truncate gap block nonces",
		"checkpoint_slot", anchorSlot,
		"tip_slot", tipPoint.Slot,
		"component", "ledger",
	)

	evolvingNonce := cloneNonce(anchorNonce)
	var (
		processed     int
		folded        int
		byronSkipped  int
		lastFoldedSlt uint64
	)

	logProgress := func(currentSlot uint64) {
		if processed == 0 || processed%mithrilGapNonceProgressInterval != 0 {
			return
		}
		ls.config.Logger.Info(
			"reconstructing truncate gap block nonces progress",
			"checkpoint_slot", anchorSlot,
			"tip_slot", tipPoint.Slot,
			"processed_blocks", processed,
			"folded_blocks", folded,
			"byron_skipped", byronSkipped,
			"current_slot", currentSlot,
			"last_folded_slot", lastFoldedSlt,
			"component", "ledger",
		)
	}

	type nonceRow struct {
		hash  []byte
		slot  uint64
		nonce []byte
	}
	var pending []nonceRow
	writeRows := func(rows []nonceRow) error {
		txn := ls.db.Transaction(true)
		defer txn.Release()
		return txn.Do(func(txn *database.Txn) error {
			for _, row := range rows {
				if err := ls.db.SetBlockNonce(
					row.hash,
					row.slot,
					row.nonce,
					checkpointSlots[row.slot],
					txn,
				); err != nil {
					return fmt.Errorf(
						"store reconstructed block nonce at slot %d: %w",
						row.slot,
						err,
					)
				}
			}
			return nil
		})
	}
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := writeRows(pending); err != nil {
			return err
		}
		pending = pending[:0]
		return nil
	}

	handleBlock := func(block models.Block) error {
		processed++
		newNonce, didFold, err := ls.foldBlockEtaV(evolvingNonce, block)
		if err != nil {
			return fmt.Errorf("reconstruct truncate gap nonce: %w", err)
		}
		if !didFold {
			byronSkipped++
			logProgress(block.Slot)
			return nil
		}
		evolvingNonce = newNonce
		pending = append(pending, nonceRow{
			hash:  block.Hash,
			slot:  block.Slot,
			nonce: cloneNonce(evolvingNonce),
		})
		if len(pending) >= mithrilGapNonceWriteBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
		folded++
		lastFoldedSlt = block.Slot
		logProgress(block.Slot)
		return nil
	}

	if ls.chain != nil {
		if anchorIter == nil {
			return errors.New(
				"missing primary-chain iterator for truncate gap nonce reconstruction",
			)
		}
		iter := anchorIter
		for {
			res, err := iter.Next(false)
			if err != nil {
				if errors.Is(err, chain.ErrIteratorChainTip) {
					break
				}
				return fmt.Errorf(
					"iterate primary chain for truncate gap nonce reconstruction: %w",
					err,
				)
			}
			if res.Rollback {
				return fmt.Errorf(
					"unexpected rollback at slot %d while reconstructing truncate gap nonces",
					res.Point.Slot,
				)
			}
			if res.Block.Slot > tipPoint.Slot {
				break
			}
			if err := handleBlock(res.Block); err != nil {
				return err
			}
		}
	} else {
		if err := database.ForEachBlockInRangeDB(
			ls.db,
			anchorSlot+1,
			tipPoint.Slot+1,
			handleBlock,
		); err != nil {
			return fmt.Errorf(
				"re-fold truncate gap block nonces: %w",
				err,
			)
		}
	}
	if err := flush(); err != nil {
		return err
	}

	if lastFoldedSlt != tipPoint.Slot {
		return fmt.Errorf(
			"truncate gap nonce reconstruction did not reach tip slot %d "+
				"(last folded slot %d); the tip block itself may be "+
				"missing from local storage",
			tipPoint.Slot,
			lastFoldedSlt,
		)
	}

	ls.Lock()
	if ls.currentTip.Point.Slot == tipPoint.Slot &&
		bytes.Equal(ls.currentTip.Point.Hash, tipPoint.Hash) {
		ls.currentTipBlockNonce = cloneNonce(evolvingNonce)
	}
	clear(ls.epochNonceHexCache)
	ls.publishSnapshotsLocked()
	ls.Unlock()

	ls.config.Logger.Warn(
		"reconstructed truncate gap block nonces",
		"checkpoint_slot", anchorSlot,
		"tip_slot", tipPoint.Slot,
		"folded_blocks", folded,
		"byron_skipped", byronSkipped,
		"final_evolving_nonce", hex.EncodeToString(evolvingNonce),
		"component", "ledger",
	)
	return nil
}
