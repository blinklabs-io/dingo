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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	mithrilGapNonceProgressInterval = 10_000
	mithrilGapNonceWriteBatchSize   = 2_000
)

// healMithrilGapBlockNonces reconstructs per-block evolving nonces for a
// Mithril-bootstrapped chain whose "gap blocks" were imported without folding
// their VRF output into the evolving nonce.
//
// Mithril import stores the ledger-state snapshot's evolving nonce at the
// ledger-state slot (the anchor) and then copies the immutable/gap blocks that
// extend the chain from the anchor up to a later tip, recording the Mithril
// trust boundary at that later tip (see mithril/sync_import.go and
// mithril/sync_gap.go). Those gap blocks have their transactions imported but
// their VRF outputs are NEVER folded into the evolving nonce, and no
// block_nonce rows are written for them. Live chainsync only folds blocks past
// the trust boundary, so it resumes from the stale anchor nonce, dropping every
// gap block's VRF contribution. The candidate nonce frozen for the first
// post-bootstrap epoch boundary is therefore wrong, the computed epoch nonce
// diverges from the network, and every leader-VRF check in that epoch fails
// with "issue verifying proof" — the node rejects the first block of the new
// epoch and wedges.
//
// This heal re-folds the evolving nonce from the anchor's stored nonce through
// every canonical block up to the current tip (gap blocks first, then any
// blocks live chainsync already mis-folded), persisting a corrected
// block_nonce for each. Blocks come from the primary chain index — a raw
// block-blob slot scan would also visit retained fork blobs and synthetic
// Leios endorser blobs, folding non-canonical VRF output into the evolving
// nonce. It is a pure function of already-stored chain data (block CBOR + the
// anchor nonce) and is idempotent: the trust-boundary block's nonce row is the
// completion marker and is committed last, so a crash mid-heal simply resumes
// on the next start (already-committed rows below the boundary become the new
// anchor), and once the marker exists the heal detects completion and no-ops.
// Writes are committed in batches so an arbitrarily long post-boundary tail
// does not accumulate into a single unbounded transaction.
//
// It must run at startup after the tip and trust boundary are loaded and before
// header verification computes any epoch nonce.
func (ls *LedgerState) healMithrilGapBlockNonces(ctx context.Context) error {
	ls.RLock()
	boundary := ls.mithrilLedgerSlot
	boundaryHash := bytes.Clone(ls.mithrilLedgerHash)
	tipPoint := ls.currentTip.Point
	ls.RUnlock()

	// Not a Mithril bootstrap: nothing to reconcile.
	if boundary == 0 {
		return nil
	}
	// The tip must be at or past the trust boundary for a gap to exist between
	// the anchor and the boundary. A tip below the boundary is not a normal
	// post-bootstrap state; leave it untouched.
	if tipPoint.Slot < boundary {
		return nil
	}

	// Completion / no-gap detection: the heal commits a valid 32-byte evolving
	// nonce at the trust-boundary point as its completion marker (written last,
	// see below). A valid boundary nonce therefore means either there was no
	// gap (the anchor is the boundary block, carrying the import checkpoint) or
	// the heal has already run — nothing to do, which is what makes the heal
	// idempotent across restarts. When the import recorded the trust-boundary
	// hash, require that exact point: a fork row at the same slot is not a
	// completion marker for the canonical boundary block. A boundary row whose
	// nonce is malformed (missing/short — e.g. a block imported without its VRF
	// folded) is NOT a valid marker and must NOT short-circuit reconstruction,
	// or the post-bootstrap epoch nonce would stay wrong. This mirrors the
	// anchor selection below, which likewise ignores malformed rows.
	boundaryRows, err := ls.db.GetBlockNoncesInSlotRange(
		boundary,
		boundary+1,
		nil,
	)
	if err != nil {
		return fmt.Errorf("query trust-boundary block nonce: %w", err)
	}
	for i := range boundaryRows {
		row := boundaryRows[i]
		if len(row.Nonce) != lcommon.Blake2b256Size {
			continue
		}
		if len(boundaryHash) == 0 || bytes.Equal(row.Hash, boundaryHash) {
			return nil
		}
	}

	// Seed: the import anchor is the highest-slot valid stored block_nonce
	// strictly before the trust boundary. Its value is the ledger-state
	// snapshot's evolving nonce (eta_v at the anchor block), which correctly
	// embeds the entire chain up to that block. Malformed rows are ignored
	// rather than allowed to mask a usable lower-slot anchor.
	anchorRows, err := ls.db.GetBlockNoncesInSlotRange(0, boundary, nil)
	if err != nil {
		return fmt.Errorf("query Mithril anchor block nonce: %w", err)
	}
	anchorCandidates := make([]models.BlockNonce, 0, len(anchorRows))
	for i := range anchorRows {
		row := anchorRows[i]
		if len(row.Nonce) != lcommon.Blake2b256Size {
			continue
		}
		anchorCandidates = append(anchorCandidates, row)
	}
	sort.SliceStable(anchorCandidates, func(i, j int) bool {
		if anchorCandidates[i].Slot != anchorCandidates[j].Slot {
			return anchorCandidates[i].Slot > anchorCandidates[j].Slot
		}
		return bytes.Compare(
			anchorCandidates[i].Hash,
			anchorCandidates[j].Hash,
		) > 0
	})
	var (
		anchorRow  *models.BlockNonce
		anchorIter *chain.ChainIterator
	)
	if len(anchorCandidates) == 0 {
		// No usable anchor nonce (e.g. a DB predating evolving-nonce storage).
		// Reconstructing from an unknown seed would be worse than leaving the
		// state as-is, so decline rather than guess. Never weaken nonce
		// correctness.
		ls.config.Logger.Warn(
			"skipped Mithril gap block nonce reconstruction: "+
				"no anchor evolving nonce below trust boundary",
			"trust_boundary_slot", boundary,
			"component", "ledger",
		)
		return nil
	}
	if ls.chain != nil {
		for i := range anchorCandidates {
			candidate := &anchorCandidates[i]
			contains, err := ls.primaryChainContainsExactPoint(
				ocommon.Point{
					Slot: candidate.Slot,
					Hash: candidate.Hash,
				},
			)
			if err != nil {
				return fmt.Errorf(
					"check Mithril anchor block on primary chain: %w",
					err,
				)
			}
			if !contains {
				continue
			}
			iter, err := ls.chain.FromPointContext(
				ctx,
				ocommon.Point{Slot: candidate.Slot, Hash: candidate.Hash},
				false,
			)
			if err != nil {
				if errors.Is(err, models.ErrBlockNotFound) {
					continue
				}
				return fmt.Errorf(
					"create primary-chain iterator from Mithril anchor: %w",
					err,
				)
			}
			anchorRow = candidate
			anchorIter = iter
			break
		}
		if anchorRow == nil {
			ls.config.Logger.Warn(
				"skipped Mithril gap block nonce reconstruction: "+
					"no valid anchor nonce below trust boundary is on "+
					"the primary chain",
				"trust_boundary_slot", boundary,
				"component", "ledger",
			)
			return nil
		}
	} else {
		anchorRow = &anchorCandidates[0]
	}
	anchorSlot := anchorRow.Slot
	anchorNonce := anchorRow.Nonce
	if anchorIter != nil {
		defer anchorIter.Cancel()
	}

	// Preserve existing checkpoint flags on rows we overwrite so pruning
	// retention is unchanged, and ensure the trust-boundary block is
	// checkpointed so post-boundary folding can always re-seed from it.
	existingRows, err := ls.db.GetBlockNoncesInSlotRange(
		anchorSlot+1,
		tipPoint.Slot+1,
		nil,
	)
	if err != nil {
		return fmt.Errorf("query existing post-anchor block nonces: %w", err)
	}
	checkpointSlots := make(map[uint64]bool, len(existingRows)+1)
	for _, row := range existingRows {
		if row.IsCheckpoint {
			checkpointSlots[row.Slot] = true
		}
	}
	checkpointSlots[boundary] = true

	ls.config.Logger.Warn(
		"reconstructing Mithril gap block nonces",
		"anchor_slot", anchorSlot,
		"trust_boundary_slot", boundary,
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
			"reconstructing Mithril gap block nonces progress",
			"anchor_slot", anchorSlot,
			"trust_boundary_slot", boundary,
			"tip_slot", tipPoint.Slot,
			"processed_blocks", processed,
			"folded_blocks", folded,
			"byron_skipped", byronSkipped,
			"current_slot", currentSlot,
			"last_folded_slot", lastFoldedSlt,
			"component", "ledger",
		)
	}

	// Reconstructed rows are committed in batches. The trust-boundary row is
	// withheld until every other row has committed: its presence is the
	// completion marker, so writing it last keeps a crash mid-heal resumable
	// instead of marking a partial reconstruction as done.
	type nonceRow struct {
		hash  []byte
		slot  uint64
		nonce []byte
	}
	var (
		pending          []nonceRow
		boundaryNonceRow *nonceRow
	)
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
			return fmt.Errorf("reconstruct gap nonce: %w", err)
		}
		if !didFold {
			// Byron blocks carry no Praos VRF contribution.
			byronSkipped++
			logProgress(block.Slot)
			return nil
		}
		evolvingNonce = newNonce
		row := nonceRow{
			hash:  block.Hash,
			slot:  block.Slot,
			nonce: cloneNonce(evolvingNonce),
		}
		if block.Slot == boundary {
			boundaryNonceRow = &row
		} else {
			pending = append(pending, row)
			if len(pending) >= mithrilGapNonceWriteBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		folded++
		lastFoldedSlt = block.Slot
		logProgress(block.Slot)
		return nil
	}

	// Walk the canonical chain from the anchor to the ledger tip.
	if ls.chain != nil {
		if anchorIter == nil {
			return errors.New(
				"missing primary-chain iterator for Mithril gap nonce reconstruction",
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
					"iterate primary chain for gap nonce reconstruction: %w",
					err,
				)
			}
			if res.Rollback {
				return fmt.Errorf(
					"unexpected rollback at slot %d while reconstructing gap nonces",
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
		// No chain index attached (tests/tooling): fall back to the slot-range
		// blob scan.
		if err := database.ForEachBlockInRangeDB(
			ls.db,
			anchorSlot+1,
			tipPoint.Slot+1,
			handleBlock,
		); err != nil {
			return fmt.Errorf(
				"re-fold Mithril gap and post-boundary block nonces: %w",
				err,
			)
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if boundaryNonceRow != nil {
		if err := writeRows([]nonceRow{*boundaryNonceRow}); err != nil {
			return err
		}
	} else {
		// No ranking block produced a nonce at the trust-boundary slot (e.g. a
		// Byron boundary block): without the completion marker the heal will
		// re-run on the next start. Harmless but worth surfacing.
		ls.config.Logger.Warn(
			"Mithril gap block nonce reconstruction completed without a "+
				"trust-boundary nonce row; heal will re-run on next start",
			"trust_boundary_slot", boundary,
			"component", "ledger",
		)
	}

	// Refresh the in-memory tip nonce, which loadTip populated from the stale
	// pre-heal value, and drop any epoch-nonce hex cache derived from it.
	if len(tipPoint.Hash) > 0 {
		tipNonce, err := ls.db.GetBlockNonce(tipPoint, nil)
		if err != nil {
			return fmt.Errorf("reload tip block nonce after heal: %w", err)
		}
		ls.Lock()
		if ls.currentTip.Point.Slot == tipPoint.Slot &&
			bytes.Equal(ls.currentTip.Point.Hash, tipPoint.Hash) {
			ls.currentTipBlockNonce = tipNonce
		}
		clear(ls.epochNonceHexCache)
		ls.publishSnapshotsLocked()
		ls.Unlock()
	} else {
		ls.Lock()
		clear(ls.epochNonceHexCache)
		ls.Unlock()
	}

	ls.config.Logger.Warn(
		"reconstructed Mithril gap block nonces",
		"anchor_slot", anchorSlot,
		"trust_boundary_slot", boundary,
		"folded_blocks", folded,
		"byron_skipped", byronSkipped,
		"last_folded_slot", lastFoldedSlt,
		"final_evolving_nonce", hex.EncodeToString(evolvingNonce),
		"component", "ledger",
	)
	return nil
}

func (ls *LedgerState) primaryChainContainsExactPoint(
	point ocommon.Point,
) (bool, error) {
	if point.Slot == 0 && len(point.Hash) == 0 {
		return true, nil
	}
	if ls.chain == nil || point.Slot == ^uint64(0) {
		return false, nil
	}
	block, err := ls.chain.BlockBeforeSlot(point.Slot + 1)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return false, nil
		}
		return false, err
	}
	return block.Slot == point.Slot && bytes.Equal(block.Hash, point.Hash), nil
}
