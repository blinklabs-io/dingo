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
	"fmt"

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
// This is called from the blockfetch path (processBlockEvent) which may
// receive blocks before the ledger has processed an epoch rollover. If the
// block's slot falls outside the current epoch range, we skip verification
// here because the epoch nonce would be stale. The ledger processing
// pipeline (ledgerProcessBlocks) handles epoch rollover before processing
// blocks of the new epoch, so header integrity is still assured.
func (ls *LedgerState) verifyBlockHeaderCrypto(
	block ledger.Block,
) error {
	// Skip Byron-era blocks early to avoid parameter lookups
	if block.Era().Id == byron.EraIdByron {
		return nil
	}

	// Read currentEpoch under RLock since this method is called outside
	// the main lock in processBlockEvents, and currentEpoch may be updated
	// concurrently during epoch rollovers.
	ls.RLock()
	currentEpoch := ls.currentEpoch
	ls.RUnlock()

	// Check that the block falls within the current epoch. If the block's
	// slot is outside the current epoch range, the epoch nonce would be
	// incorrect:
	//   - Future epoch: nonce is stale (epoch rollover hasn't happened yet)
	//   - Past epoch: nonce is for a different epoch (during gap-fill/re-sync)
	// Skip verification and let ledgerProcessBlocks handle it with the
	// correct nonce for the block's actual epoch.
	epochEnd := currentEpoch.StartSlot + uint64(currentEpoch.LengthInSlots)
	if currentEpoch.LengthInSlots == 0 ||
		block.SlotNumber() < currentEpoch.StartSlot ||
		block.SlotNumber() >= epochEnd {
		return nil
	}

	epochNonce := currentEpoch.Nonce

	// Get slotsPerKesPeriod from Shelley genesis configuration
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return fmt.Errorf(
			"shelley genesis not available for block header verification at slot %d",
			block.SlotNumber(),
		)
	}
	slotsPerKesPeriod := uint64(shelleyGenesis.SlotsPerKESPeriod) //nolint:gosec

	return verifyBlockHeader(block, epochNonce, slotsPerKesPeriod)
}
