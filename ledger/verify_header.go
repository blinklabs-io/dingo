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
		return fmt.Errorf(
			"block header verification rejected: no epoch data for slot %d: %w",
			blockSlot,
			err,
		)
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
