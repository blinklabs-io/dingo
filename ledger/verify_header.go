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
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// verifyBlockHeader performs cryptographic verification of a block header.
// This includes VRF proof verification and KES signature verification.
// Byron-era blocks are skipped because they use a different consensus
// mechanism (PBFT) and do not have VRF/KES fields.
//
// Parameters:
//   - block: the block whose header to verify
//   - epochNonce: the epoch nonce (eta0) for VRF verification
//   - slotsPerKesPeriod: number of slots per KES period from Shelley genesis
//
// Returns an error if verification fails, nil if the block passes
// verification or is a Byron-era block.
func verifyBlockHeader(
	block ledger.Block,
	epochNonce []byte,
	slotsPerKesPeriod uint64,
) error {
	// Skip Byron-era blocks - they use PBFT consensus, not Praos,
	// and do not have VRF/KES/OpCert fields
	if block.Era().Id == byron.EraIdByron {
		return nil
	}

	// Epoch nonce is required for post-Byron blocks
	if len(epochNonce) == 0 {
		return fmt.Errorf(
			"epoch nonce not available for block at slot %d",
			block.SlotNumber(),
		)
	}

	eta0Hex := hex.EncodeToString(epochNonce)

	// Use gouroboros VerifyBlock for VRF + KES verification.
	// We skip body hash validation, transaction validation, and stake
	// pool validation here because:
	// - Body hash validation requires full block CBOR which may not
	//   always be available at this stage
	// - Transaction validation requires full ledger state and protocol
	//   parameters which are handled elsewhere
	// - Stake pool validation requires pool registration lookups
	//   which are handled elsewhere
	config := lcommon.VerifyConfig{
		SkipBodyHashValidation:    true,
		SkipTransactionValidation: true,
		SkipStakePoolValidation:   true,
	}

	isValid, _, _, _, err := ledger.VerifyBlock(
		block,
		eta0Hex,
		slotsPerKesPeriod,
		config,
	)
	if err != nil {
		return fmt.Errorf(
			"block header verification failed at slot %d: %w",
			block.SlotNumber(),
			err,
		)
	}
	if !isValid {
		return fmt.Errorf(
			"block header verification returned invalid at slot %d",
			block.SlotNumber(),
		)
	}

	return nil
}

// verifyBlockHeaderCrypto extracts the necessary parameters from the
// LedgerState and delegates to verifyBlockHeader for cryptographic
// verification of a block's VRF proof and KES signature.
//
// This is called from the blockfetch path (processBlockEvents) before
// blocks are handed to the ledger processing pipeline. It performs
// epoch-aware parameter lookup: the block's slot is matched against the
// full epoch cache to find the correct epoch nonce, so blocks that
// arrive during or after an epoch transition are always verified against
// the right epoch's parameters.
//
// If no epoch with a valid nonce can be found for the block's slot
// (e.g., the epoch rollover has not yet been processed), the block is
// rejected rather than silently skipping verification. This prevents
// an attacker from forging headers that bypass verification by
// targeting the epoch boundary window.
func (ls *LedgerState) verifyBlockHeaderCrypto(
	block ledger.Block,
) error {
	// Skip Byron-era blocks early to avoid parameter lookups
	if block.Era().Id == byron.EraIdByron {
		return nil
	}

	blockSlot := block.SlotNumber()

	// Look up the epoch for this block's slot from the epoch cache.
	// This is an epoch-aware lookup that searches through all known
	// epochs rather than only the current one, ensuring that blocks
	// at epoch boundaries are verified against the correct nonce.
	epoch, err := ls.epochForSlot(blockSlot)
	if err != nil {
		// Epoch cache doesn't cover this slot yet. Blockfetch can
		// deliver blocks past the epoch boundary before the ledger
		// processing goroutine runs the full epoch rollover. Eagerly
		// compute the next epoch(s) so verification can proceed.
		epoch, err = ls.ensureEpochForSlot(blockSlot)
		if err != nil {
			return fmt.Errorf(
				"block header verification rejected: no epoch data for slot %d: %w",
				blockSlot,
				err,
			)
		}
	}

	// Reject blocks for which we have epoch data but no nonce.
	// A missing nonce means the epoch rollover has not completed
	// or the epoch is too far in the future.
	if len(epoch.Nonce) == 0 {
		return fmt.Errorf(
			"block header verification rejected: "+
				"epoch %d has no nonce for slot %d "+
				"(epoch rollover may not have been processed yet)",
			epoch.EpochId,
			blockSlot,
		)
	}

	// Get slotsPerKesPeriod from Shelley genesis configuration
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return fmt.Errorf(
			"shelley genesis not available for block header verification at slot %d",
			blockSlot,
		)
	}
	slotsPerKesPeriod := uint64(shelleyGenesis.SlotsPerKESPeriod) //nolint:gosec

	return verifyBlockHeader(block, epoch.Nonce, slotsPerKesPeriod)
}

// epochForSlot searches the epoch cache for the epoch containing the
// given slot. It takes a snapshot of the epoch cache under RLock to
// avoid racing with concurrent epoch rollover updates.
//
// Returns the matching epoch or an error if no epoch covers the slot.
func (ls *LedgerState) epochForSlot(slot uint64) (models.Epoch, error) {
	ls.RLock()
	cacheCopy := make([]models.Epoch, len(ls.epochCache))
	copy(cacheCopy, ls.epochCache)
	ls.RUnlock()

	if len(cacheCopy) == 0 {
		return models.Epoch{}, errors.New("epoch cache is empty")
	}

	for _, ep := range cacheCopy {
		if ep.LengthInSlots == 0 {
			continue
		}
		epochEnd := ep.StartSlot + uint64(ep.LengthInSlots)
		if slot >= ep.StartSlot && slot < epochEnd {
			return ep, nil
		}
	}

	// Find the last epoch with a valid (non-zero) length for a
	// meaningful error message.
	var lastValidEnd uint64
	var hasValidEpoch bool
	for i := len(cacheCopy) - 1; i >= 0; i-- {
		if cacheCopy[i].LengthInSlots > 0 {
			lastValidEnd = cacheCopy[i].StartSlot +
				uint64(cacheCopy[i].LengthInSlots)
			hasValidEpoch = true
			break
		}
	}
	if !hasValidEpoch {
		return models.Epoch{}, fmt.Errorf(
			"slot %d not covered by any known epoch (cache has %d epochs, all with zero length)",
			slot,
			len(cacheCopy),
		)
	}
	return models.Epoch{}, fmt.Errorf(
		"slot %d not covered by any known epoch (cache has %d epochs, last ends at slot %d)",
		slot,
		len(cacheCopy),
		lastValidEnd,
	)
}

// ensureEpochForSlot advances the epoch cache until it covers the target
// slot, then returns the epoch. This handles the case where blockfetch
// delivers blocks past an epoch boundary before the ledger processing
// goroutine has run the full epoch rollover. The epoch nonce is computed
// from chain data (the last block before the boundary), which is available
// because blockfetch delivers blocks in order.
func (ls *LedgerState) ensureEpochForSlot(
	targetSlot uint64,
) (models.Epoch, error) {
	const maxAdvance = 5 // Safety limit against runaway loops
	for range maxAdvance {
		if err := ls.advanceEpochCache(); err != nil {
			return models.Epoch{}, fmt.Errorf(
				"advance epoch cache: %w",
				err,
			)
		}
		epoch, err := ls.epochForSlot(targetSlot)
		if err == nil {
			return epoch, nil
		}
	}
	return models.Epoch{}, fmt.Errorf(
		"could not advance epoch cache to cover slot %d after %d advances",
		targetSlot,
		maxAdvance,
	)
}

// advanceEpochCache computes the next epoch's parameters and nonce from
// chain data and appends it to the in-memory epoch cache. This is a
// lightweight alternative to the full processEpochRollover — it only
// populates the nonce and epoch boundaries needed for header verification,
// without running pparam updates, snapshot rotation, or DB writes.
// The full rollover will run later in ledgerProcessBlocks and replace
// the cache with the authoritative DB-backed version.
func (ls *LedgerState) advanceEpochCache() error {
	// Read last epoch under read lock
	ls.RLock()
	if len(ls.epochCache) == 0 {
		ls.RUnlock()
		return errors.New("epoch cache is empty")
	}
	lastEpoch := ls.epochCache[len(ls.epochCache)-1]
	ls.RUnlock()

	if lastEpoch.LengthInSlots == 0 {
		return errors.New("last epoch has zero length")
	}

	newStartSlot := lastEpoch.StartSlot + uint64(lastEpoch.LengthInSlots)

	// Compute epoch nonce (requires DB access, done outside lock)
	nonce, evolvingNonce, candidateNonce, labNonce, err := ls.computeEpochNonceForSlot(newStartSlot, lastEpoch)
	if err != nil {
		return fmt.Errorf(
			"compute epoch nonce for epoch %d: %w",
			lastEpoch.EpochId+1,
			err,
		)
	}

	newEpoch := models.Epoch{
		EpochId:             lastEpoch.EpochId + 1,
		StartSlot:           newStartSlot,
		LengthInSlots:       lastEpoch.LengthInSlots,
		SlotLength:          lastEpoch.SlotLength,
		EraId:               lastEpoch.EraId,
		Nonce:               nonce,
		EvolvingNonce:       evolvingNonce,
		CandidateNonce:      candidateNonce,
		LastEpochBlockNonce: labNonce,
	}

	// Update cache under write lock, checking for concurrent advance
	ls.Lock()
	lastCached := ls.epochCache[len(ls.epochCache)-1]
	if lastCached.EpochId >= newEpoch.EpochId {
		// Another goroutine or ledger processing already advanced
		ls.Unlock()
		return nil
	}
	ls.epochCache = append(ls.epochCache, newEpoch)
	ls.Unlock()

	ls.config.Logger.Debug(
		"eagerly advanced epoch cache for header verification",
		"new_epoch", newEpoch.EpochId,
		"start_slot", newEpoch.StartSlot,
		"nonce", hex.EncodeToString(newEpoch.Nonce),
		"prev_epoch_id", lastEpoch.EpochId,
		"prev_epoch_nonce", hex.EncodeToString(lastEpoch.Nonce),
		"component", "ledger",
	)

	return nil
}

// computeEpochNonceForSlot computes the epoch nonce, evolving nonce,
// and labNonce for a new epoch starting at epochStartSlot. This
// mirrors calculateEpochNonce but uses non-transactional DB lookups
// since we're not inside the ledger processing pipeline.
//
// Returns (epochNonce, evolvingNonce, candidateNonce, labNonce, error).
func (ls *LedgerState) computeEpochNonceForSlot(
	epochStartSlot uint64,
	prevEpoch models.Epoch,
) ([]byte, []byte, []byte, []byte, error) {
	if ls.config.CardanoNodeConfig == nil {
		return nil, nil, nil, nil, errors.New("CardanoNodeConfig is nil")
	}
	genesisHashHex := ls.config.CardanoNodeConfig.ShelleyGenesisHash
	if genesisHashHex == "" {
		return nil, nil, nil, nil, errors.New(
			"shelley genesis hash not available",
		)
	}
	genesisHash, err := hex.DecodeString(genesisHashHex)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"decode genesis hash: %w", err,
		)
	}

	// For the initial epoch (no nonce yet), return genesis hash
	// for both epoch nonce and initial evolving nonce.
	if len(prevEpoch.Nonce) == 0 {
		return genesisHash, genesisHash, genesisHash, nil, nil
	}

	prevEvolvingNonce := prevEpoch.EvolvingNonce
	if len(prevEvolvingNonce) == 0 {
		prevEvolvingNonce = genesisHash
	}

	// The candidate nonce carries across epochs independently of the
	// evolving nonce. Fall back to genesis hash when not stored (e.g.,
	// epochs created before this field existed).
	prevCandidateNonce := prevEpoch.CandidateNonce
	if len(prevCandidateNonce) == 0 {
		prevCandidateNonce = genesisHash
	}

	candidateNonce, evolvingNonce, err := ls.computeCandidateNonce(
		nil, // non-transactional
		prevEvolvingNonce,
		prevCandidateNonce,
		prevEpoch.StartSlot,
		uint64(prevEpoch.LengthInSlots),
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"compute candidate nonce: %w", err,
		)
	}

	// Compute the labNonce to SAVE for the next epoch's formula.
	prevEpochEndSlot := prevEpoch.StartSlot +
		uint64(prevEpoch.LengthInSlots)
	var labNonceToSave []byte
	boundaryBlock, err := database.BlockBeforeSlot(
		ls.db,
		prevEpochEndSlot,
	)
	if err != nil {
		if !errors.Is(err, models.ErrBlockNotFound) {
			return nil, nil, nil, nil, fmt.Errorf(
				"lookup boundary block: %w", err,
			)
		}
	} else if len(boundaryBlock.PrevHash) > 0 {
		labNonceToSave = boundaryBlock.PrevHash
	}

	// Use the LAGGED lastEpochBlockNonce in the formula.
	lastEpochBlockNonce := prevEpoch.LastEpochBlockNonce
	if len(lastEpochBlockNonce) == 0 {
		// NeutralNonce is the identity element of ⭒:
		//   candidateNonce ⭒ NeutralNonce = candidateNonce
		ls.config.Logger.Debug(
			"computed epoch nonce for cache advance "+
				"(NeutralNonce, using candidateNonce)",
			"new_epoch_start_slot", epochStartSlot,
			"prev_epoch_id", prevEpoch.EpochId,
			"candidate_nonce",
			hex.EncodeToString(candidateNonce),
			"epoch_nonce",
			hex.EncodeToString(candidateNonce),
			"component", "ledger",
		)
		return candidateNonce, evolvingNonce, candidateNonce, labNonceToSave, nil
	}

	result, err := lcommon.CalculateEpochNonce(
		candidateNonce,
		lastEpochBlockNonce,
		nil,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"calculate epoch nonce: %w", err,
		)
	}

	ls.config.Logger.Debug(
		"computed epoch nonce for cache advance",
		"new_epoch_start_slot", epochStartSlot,
		"prev_epoch_id", prevEpoch.EpochId,
		"last_epoch_block_nonce",
		hex.EncodeToString(lastEpochBlockNonce),
		"candidate_nonce", hex.EncodeToString(candidateNonce),
		"evolving_nonce", hex.EncodeToString(evolvingNonce),
		"epoch_nonce", hex.EncodeToString(result.Bytes()),
		"component", "ledger",
	)

	return result.Bytes(), evolvingNonce, candidateNonce, labNonceToSave, nil
}
