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
	"math/big"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
)

// computeCandidateNonce computes the candidate nonce and end-of-epoch
// evolving nonce for the given epoch.
//
// In Ouroboros Praos, the PrtclState nonces (evolving and candidate)
// carry across epoch boundaries without resetting. Each block updates
// them via the Nonce semigroup (⭒) operator:
//
//	NeutralNonce ⭒ Nonce(x) = Nonce(x)                   (identity)
//	Nonce(a) ⭒ Nonce(b) = Nonce(blake2b_256(a || b))     (combine)
//
// The VRF contribution is blake2b_256(vrfOutput), so:
//
//	η_v' = η_v ⭒ Nonce(blake2b_256(vrfOutput))
//
// The candidate nonce tracks the evolving nonce until the stability
// window cutoff (slot + stabilityWindow >= firstSlotNextEpoch), then
// freezes. The frozen value is used in the epoch nonce formula:
//
//	epochNonce(N+1) = blake2b_256(candidateNonce(N) || labNonce(N))
//
// The evolving nonce continues to be updated past the cutoff with
// all remaining blocks. Its end-of-epoch value becomes the starting
// point for the next epoch.
//
// Parameters:
//   - txn: optional database transaction (nil to create a new one)
//   - prevEvolvingNonce: evolving nonce at the end of the previous
//     epoch (genesis hash for the first Shelley epoch)
//   - prevCandidateNonce: candidate nonce at the end of the previous
//     epoch (genesis hash for the first Shelley epoch). In Haskell,
//     psCandidateNonce carries across epochs independently of the
//     evolving nonce. When 4k/f >= epochLength, the candidate is
//     never updated and this value is returned as-is.
//   - epochStartSlot: first slot of the epoch
//   - epochLengthInSlots: total slots in the epoch
//
// Returns:
//   - candidateNonce: the evolving nonce frozen at the stability
//     window cutoff
//   - evolvingNonce: the evolving nonce after all blocks in the epoch
func (ls *LedgerState) computeCandidateNonce(
	txn *database.Txn,
	prevEvolvingNonce []byte,
	prevCandidateNonce []byte,
	epochStartSlot uint64,
	epochLengthInSlots uint64,
) ([]byte, []byte, error) {
	stabilityWindow := ls.nonceStabilityWindow()
	epochEndSlot := epochStartSlot + epochLengthInSlots

	// Start from the previous epoch's evolving nonce
	evolvingNonce := make([]byte, len(prevEvolvingNonce))
	copy(evolvingNonce, prevEvolvingNonce)

	// candidateNonce starts at prevCandidateNonce (carried across
	// epochs, matching Haskell's psCandidateNonce). It tracks the
	// evolving nonce until the stability window cutoff. When
	// stabilityWindow >= epochLength, no blocks update it, so it
	// stays at prevCandidateNonce.
	candidateNonce := make([]byte, len(prevCandidateNonce))
	copy(candidateNonce, prevCandidateNonce)

	// Determine the cutoff slot. Blocks at or past this slot do NOT
	// update candidateNonce (it freezes), but still update evolvingNonce.
	var cutoffSlot uint64
	if stabilityWindow >= epochLengthInSlots {
		// All blocks are past the cutoff — candidateNonce stays at
		// prevCandidateNonce, but evolvingNonce is still updated.
		cutoffSlot = epochStartSlot
	} else {
		cutoffSlot = epochStartSlot + epochLengthInSlots -
			stabilityWindow
	}

	var blockCount, preCutoffCount int

	iterFn := func(block models.Block) error {
		// Byron blocks have no VRF contribution. Skip them
		// before decoding to avoid failures on devnet EBBs
		// whose CBOR layout may differ from mainnet.
		if block.Type == byron.BlockTypeByronEbb ||
			block.Type == byron.BlockTypeByronMain {
			return nil
		}
		parsedBlock, err := block.Decode()
		if err != nil {
			return fmt.Errorf(
				"decode block at slot %d: %w",
				block.Slot,
				err,
			)
		}
		eraId := uint(parsedBlock.Era().Id)
		era := eras.GetEraById(eraId)
		if era == nil || era.CalculateEtaVFunc == nil {
			// Unknown era - no VRF contribution
			return nil
		}
		newNonce, err := era.CalculateEtaVFunc(
			ls.config.CardanoNodeConfig,
			evolvingNonce,
			parsedBlock,
		)
		if err != nil {
			return fmt.Errorf(
				"calculate etaV at slot %d: %w",
				block.Slot,
				err,
			)
		}
		evolvingNonce = newNonce
		blockCount++

		// Before the cutoff, candidateNonce tracks evolvingNonce
		if block.Slot < cutoffSlot {
			candidateNonce = make([]byte, len(evolvingNonce))
			copy(candidateNonce, evolvingNonce)
			preCutoffCount++
		}
		return nil
	}

	if txn != nil {
		err := database.ForEachBlockInRange(
			txn,
			epochStartSlot,
			epochEndSlot,
			iterFn,
		)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"iterate blocks [%d, %d): %w",
				epochStartSlot,
				epochEndSlot,
				err,
			)
		}
	} else {
		err := database.ForEachBlockInRangeDB(
			ls.db,
			epochStartSlot,
			epochEndSlot,
			iterFn,
		)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"iterate blocks [%d, %d): %w",
				epochStartSlot,
				epochEndSlot,
				err,
			)
		}
	}

	ls.config.Logger.Debug(
		"computed candidate nonce from block VRF outputs",
		"epoch_start_slot", epochStartSlot,
		"cutoff_slot", cutoffSlot,
		"stability_window", stabilityWindow,
		"block_count", blockCount,
		"pre_cutoff_count", preCutoffCount,
		"candidate_nonce", hex.EncodeToString(candidateNonce),
		"evolving_nonce", hex.EncodeToString(evolvingNonce),
		"component", "ledger",
	)

	return candidateNonce, evolvingNonce, nil
}

// nonceStabilityWindow returns the randomness stabilisation window
// (in slots) for nonce computation. In Ouroboros Praos (Conway+),
// this determines when the evolving nonce is frozen: blocks with
// slot + window >= firstSlotNextEpoch do NOT contribute.
//
// The value is 4k/f where k is securityParam and f is
// activeSlotsCoeff from Shelley genesis. Note: pre-Conway eras used
// 3k/f (computeStabilityWindow), but Conway uses 4k/f
// (computeRandomnessStabilisationWindow) as per the Praos protocol.
// See cardano-ledger StabilityWindow.hs.
func (ls *LedgerState) nonceStabilityWindow() uint64 {
	if ls.config.CardanoNodeConfig == nil {
		return 0
	}
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return 0
	}
	k := shelleyGenesis.SecurityParam
	if k <= 0 {
		return 0
	}
	activeSlotsCoeff := shelleyGenesis.ActiveSlotsCoeff.Rat
	if activeSlotsCoeff == nil ||
		activeSlotsCoeff.Num().Sign() <= 0 {
		return 0
	}
	// 4k/f = 4 * k * denom / num
	numerator := new(big.Int).SetInt64(int64(k))
	numerator.Mul(numerator, big.NewInt(4))
	numerator.Mul(numerator, activeSlotsCoeff.Denom())
	denominator := new(big.Int).Set(activeSlotsCoeff.Num())
	result := new(big.Int).Div(numerator, denominator)
	// Ceiling division: if there's a remainder, add 1
	remainder := new(big.Int).Mod(numerator, denominator)
	if remainder.Sign() != 0 {
		result.Add(result, big.NewInt(1))
	}
	if !result.IsUint64() {
		return 0
	}
	return result.Uint64()
}
