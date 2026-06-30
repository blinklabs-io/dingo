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
	"math/big"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// lookupBlockBeforeSlot finds the highest-slot block before slot in
// the blob store, optionally inside a database transaction.
func lookupBlockBeforeSlot(
	db *database.Database,
	txn *database.Txn,
	slot uint64,
) (models.Block, error) {
	if txn != nil {
		return database.BlockBeforeSlotTxn(txn, slot)
	}
	return database.BlockBeforeSlot(db, slot)
}

// errNoncesMissing is returned by computeCandidateNonceFast when blocks
// exist in the epoch but no pre-stored nonce rows are found. The caller
// falls back to the slow CBOR-decode path.
var errNoncesMissing = errors.New("block nonce rows missing for epoch")

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
//	epochNonce(N+1) = blake2b_256(candidateNonce(N) || lastEpochBlockNonce(N))
//
// The evolving nonce continues to be updated past the cutoff with
// all remaining blocks. Its end-of-epoch value becomes the starting
// point for the next epoch.
//
// Parameters:
//   - txn: optional database transaction (nil to create a new one)
//   - eraId: the era of the source epoch. Selects the
//     randomness-stabilisation formula (3k/f for TPraos eras,
//     4k/f for Praos) that determines the candidate-freeze cutoff.
//   - prevEvolvingNonce: evolving nonce at the end of the previous
//     epoch (genesis hash for the first Shelley epoch)
//   - prevCandidateNonce: candidate nonce at the end of the previous
//     epoch (genesis hash for the first Shelley epoch). In Haskell,
//     psCandidateNonce carries across epochs independently of the
//     evolving nonce. When the stability window >= epochLength, the
//     candidate is never updated and this value is returned as-is.
//   - epochStartSlot: first slot of the epoch
//   - epochLengthInSlots: total slots in the epoch
//
// Returns:
//   - candidateNonce: the evolving nonce frozen at the stability
//     window cutoff
//   - evolvingNonce: the evolving nonce after all blocks in the epoch
func (ls *LedgerState) computeCandidateNonce(
	txn *database.Txn,
	eraId uint,
	prevEvolvingNonce []byte,
	prevCandidateNonce []byte,
	epochStartSlot uint64,
	epochLengthInSlots uint64,
) ([]byte, []byte, error) {
	stabilityWindow := ls.nonceStabilityWindow(eraId)
	epochEndSlot := epochStartSlot + epochLengthInSlots

	// Determine the cutoff slot. Blocks at or past this slot do NOT
	// update candidateNonce (it freezes), but still update evolvingNonce.
	var cutoffSlot uint64
	if stabilityWindow >= epochLengthInSlots {
		cutoffSlot = epochStartSlot
	} else {
		cutoffSlot = epochStartSlot + epochLengthInSlots -
			stabilityWindow
	}

	// Fast path: look up pre-stored evolving nonces from the
	// block_nonce table. Each block's evolving nonce is stored
	// during normal block processing (SetBlockNonce), so we can
	// retrieve the candidate and end-of-epoch nonces with two
	// indexed queries instead of re-decoding every block's CBOR.
	candidateNonce, evolvingNonce, err := ls.computeCandidateNonceFast(
		txn,
		prevEvolvingNonce,
		prevCandidateNonce,
		epochStartSlot,
		epochEndSlot,
		cutoffSlot,
	)
	if err == nil {
		return candidateNonce, evolvingNonce, nil
	}
	ls.config.Logger.Debug(
		"fast candidate nonce lookup unavailable, falling back to CBOR decode",
		"epoch_start_slot", epochStartSlot,
		"reason", err.Error(),
		"component", "ledger",
	)

	// Slow path: iterate every block, decode from CBOR, and
	// recompute VRF nonces. This is needed when block nonces
	// are not yet stored (e.g., first sync before nonce storage
	// was introduced).
	return ls.computeCandidateNonceSlow(
		txn,
		prevEvolvingNonce,
		prevCandidateNonce,
		epochStartSlot,
		epochEndSlot,
		cutoffSlot,
		stabilityWindow,
	)
}

// computeCandidateNonceFast retrieves pre-stored evolving nonces from the
// block_nonce table. Returns errNoncesMissing if the nonce table lags
// behind the blob store for this epoch, forcing the caller to fall
// back to the slow path. The metadata pipeline batches commits, so
// `GetLastBlockNonceInRange` alone can return a partial accumulation
// of the epoch when called outside the ledger transaction (e.g., from
// header verification's advanceEpochCache). Anchoring on the blob
// store's last block — which is fully populated by blockfetch before
// header verification fires — prevents that hazard.
func (ls *LedgerState) computeCandidateNonceFast(
	txn *database.Txn,
	prevEvolvingNonce []byte,
	prevCandidateNonce []byte,
	epochStartSlot uint64,
	epochEndSlot uint64,
	cutoffSlot uint64,
) ([]byte, []byte, error) {
	// Identify the actual last block of the epoch in the blob store.
	// The evolving nonce is the nonce of THIS block, not whatever
	// block_nonce row happens to be the most recent committed.
	lastBlock, blockErr := lookupBlockBeforeSlot(ls.db, txn, epochEndSlot)
	hasBlocks := blockErr == nil && lastBlock.Slot >= epochStartSlot
	if blockErr != nil && !errors.Is(blockErr, models.ErrBlockNotFound) {
		return nil, nil, fmt.Errorf(
			"lookup last block in epoch: %w", blockErr,
		)
	}

	var evolvingNonce []byte
	if hasBlocks {
		nonce, err := ls.db.GetBlockNonce(
			ocommon.Point{
				Slot: lastBlock.Slot,
				Hash: lastBlock.Hash,
			},
			txn,
		)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"get nonce for last epoch block: %w", err,
			)
		}
		if len(nonce) == 0 {
			// The last block in the blob store has no committed
			// block_nonce row yet — the metadata pipeline is
			// behind. Force the slow path so we recompute the
			// epoch's evolving nonce from CBOR with all blocks
			// included.
			return nil, nil, errNoncesMissing
		}
		evolvingNonce = nonce
	} else {
		// No blocks in epoch — use previous values
		evolvingNonce = make([]byte, len(prevEvolvingNonce))
		copy(evolvingNonce, prevEvolvingNonce)
	}

	// Get the candidate nonce (frozen at the stability window cutoff).
	// Same hazard, same anchor: use the actual last block before the
	// cutoff in the blob store.
	var candidateNonce []byte
	if cutoffSlot <= epochStartSlot {
		// All blocks are past the cutoff — candidate stays at previous
		candidateNonce = make([]byte, len(prevCandidateNonce))
		copy(candidateNonce, prevCandidateNonce)
	} else {
		lastPreCutoff, preErr := lookupBlockBeforeSlot(
			ls.db, txn, cutoffSlot,
		)
		hasPreCutoff := preErr == nil &&
			lastPreCutoff.Slot >= epochStartSlot
		if preErr != nil && !errors.Is(preErr, models.ErrBlockNotFound) {
			return nil, nil, fmt.Errorf(
				"lookup last pre-cutoff block: %w", preErr,
			)
		}
		if hasPreCutoff {
			nonce, err := ls.db.GetBlockNonce(
				ocommon.Point{
					Slot: lastPreCutoff.Slot,
					Hash: lastPreCutoff.Hash,
				},
				txn,
			)
			if err != nil {
				return nil, nil, fmt.Errorf(
					"get nonce for last pre-cutoff block: %w",
					err,
				)
			}
			if len(nonce) == 0 {
				return nil, nil, errNoncesMissing
			}
			candidateNonce = nonce
		} else {
			// No blocks before cutoff — candidate stays at previous
			candidateNonce = make([]byte, len(prevCandidateNonce))
			copy(candidateNonce, prevCandidateNonce)
		}
	}

	ls.config.Logger.Debug(
		"computed candidate nonce from stored block nonces (fast path)",
		"epoch_start_slot", epochStartSlot,
		"cutoff_slot", cutoffSlot,
		"candidate_nonce", hex.EncodeToString(candidateNonce),
		"evolving_nonce", hex.EncodeToString(evolvingNonce),
		"component", "ledger",
	)

	return candidateNonce, evolvingNonce, nil
}

// computeCandidateNonceSlow re-decodes every block in the epoch from CBOR
// and recomputes VRF nonces. This is the fallback when pre-stored nonces
// are not available.
func (ls *LedgerState) computeCandidateNonceSlow(
	txn *database.Txn,
	prevEvolvingNonce []byte,
	prevCandidateNonce []byte,
	epochStartSlot uint64,
	epochEndSlot uint64,
	cutoffSlot uint64,
	stabilityWindow uint64,
) ([]byte, []byte, error) {
	// Start from the previous epoch's evolving nonce
	evolvingNonce := make([]byte, len(prevEvolvingNonce))
	copy(evolvingNonce, prevEvolvingNonce)

	// candidateNonce starts at prevCandidateNonce (carried across
	// epochs, matching Haskell's psCandidateNonce).
	candidateNonce := make([]byte, len(prevCandidateNonce))
	copy(candidateNonce, prevCandidateNonce)

	var blockCount, preCutoffCount int

	iterFn := func(block models.Block) error {
		// Byron blocks have no VRF contribution.
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
		era, ok := ls.eraById(eraId)
		if !ok || era == nil {
			return fmt.Errorf(
				"recompute candidate nonce at slot %d block %x: unknown era ID %d",
				block.Slot,
				block.Hash,
				eraId,
			)
		}
		if era.CalculateEtaVFunc == nil {
			return fmt.Errorf(
				"recompute candidate nonce at slot %d block %x: era ID %d (%s) has no etaV calculator",
				block.Slot,
				block.Hash,
				eraId,
				era.Name,
			)
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
		"computed candidate nonce from block VRF outputs (slow path)",
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
// (in slots) for nonce computation in the given era. Blocks at or past
// (firstSlotNextEpoch - window) do NOT update the candidate nonce; the
// evolving nonce continues to update.
//
// The multiplier of k is era-specific and does NOT split cleanly along
// TPraos/Praos lines. Babbage runs Praos but uses the smaller 3k/f
// window for backwards compatibility with the TPraos eras that came
// before it; the 4k/f window only kicks in starting with Conway:
//
//   - Shelley/Allegra/Mary/Alonzo (TPraos): 3k/f
//   - Babbage (Praos, but 3k/f for backwards compatibility): 3k/f
//   - Conway and onwards (Praos): 4k/f
//
// Using 4k/f for Babbage shifts its candidate-freeze cutoff vs peers
// and produces an eta0 for the Babbage→Conway transition that does
// not match peers. The first Conway block this node forges then
// VRF-fails on every relay, and every peer Conway header VRF-fails
// locally. Using 4k/f for TPraos eras has the same effect at every
// TPraos epoch boundary, most visibly at Shelley→Allegra where
// epoch 0's misframed candidate yields a wrong eta0 for epoch 1.
//
// Era IDs follow gouroboros: Byron=0, Shelley=1, Allegra=2, Mary=3,
// Alonzo=4, Babbage=5, Conway=6. Byron has no Praos randomness
// window and returns 0.
func (ls *LedgerState) nonceStabilityWindow(eraId uint) uint64 {
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
	multiplier, ok := nonceStabilityWindowKMultiplier(eraId)
	if !ok {
		return 0
	}
	// (multiplier * k) / f = (multiplier * k * f.denom) / f.num
	numerator := new(big.Int).SetInt64(int64(k))
	numerator.Mul(numerator, big.NewInt(multiplier))
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

// nonceStabilityWindowKMultiplier returns the k multiplier for the
// given era's randomness-stabilisation formula. The multiplier
// changes from 3 to 4 at the Babbage→Conway boundary, NOT at the
// TPraos→Praos boundary (Alonzo→Babbage): Babbage runs Praos but
// retains the 3k/f window for backwards compatibility. Each era is
// enumerated explicitly so adding a new era is a deliberate edit
// here (a missing era returns ok=false rather than silently
// inheriting a window). Byron has no Praos nonce protocol and
// returns ok=false.
func nonceStabilityWindowKMultiplier(eraId uint) (int64, bool) {
	switch eraId {
	case shelley.EraIdShelley,
		allegra.EraIdAllegra,
		mary.EraIdMary,
		alonzo.EraIdAlonzo,
		babbage.EraIdBabbage:
		return 3, true
	case conway.EraIdConway, dijkstra.EraIdDijkstra:
		return 4, true
	default:
		return 0, false
	}
}
