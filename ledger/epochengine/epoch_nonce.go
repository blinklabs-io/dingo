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

package epochengine

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// CalculateEpochNonce computes the epoch nonce for epoch N+1, the
// end-of-epoch evolving nonce, and the lastEpochBlockNonce to carry
// into epoch N+1.
//
// The Ouroboros Praos formula is:
//
//	epochNonce(N+1) = candidateNonce(N) ⭒ lastEpochBlockNonce(N)
//
// where lastEpochBlockNonce(N) is the hash of the last block in epoch N.
// If epoch N has no blocks, the consensus state carries the previous
// lastEpochBlockNonce forward.
//
// The ⭒ operator has NeutralNonce as identity:
//
//	x ⭒ NeutralNonce = x
//
// For the very first epoch transition (0→1), lastEpochBlockNonce
// is nil (NeutralNonce), so epochNonce = candidateNonce.
//
// newEpochPParams are the protocol parameters epoch N+1 will run under, i.e.
// the result of this boundary's own parameter enactment. Their extraEntropy is
// the third term of the cardano-ledger TICKN assembly:
//
//	epochNonce(N+1) = candidateNonce(N) ⭒ lastEpochBlockNonce(N) ⭒ extraEntropy
//
// It is read from the enacted parameters rather than the ending epoch's
// because TICKN is driven by a ledger view forecast to the new epoch, which
// applies the boundary's parameter update first (TICKF /
// validatingTickTransitionFORECAST). Mainnet set a non-neutral extraEntropy
// for exactly one epoch, 259, whose eta0 is the value the update enacted at
// that boundary carried.
//
// Returns (epochNonce, evolvingNonce, candidateNonce, labNonce, error).
// The caller must store candidateNonce as the new epoch's CandidateNonce
// and labNonce as the new epoch's LastEpochBlockNonce so an empty next
// epoch can carry it forward.
func (e *Engine) CalculateEpochNonce(ctx context.Context,
	txn ledgerdb.Txn,
	epochStartSlot uint64,
	currentEra eras.EraDesc,
	currentEpoch models.Epoch,
	newEpochPParams lcommon.ProtocolParameters,
) ([]byte, []byte, []byte, []byte, error) {
	// No epoch nonce exists in Byron. At a Byron-to-Shelley boundary the caller
	// applies the transition before rollover, so currentEra is Shelley and the
	// empty source-epoch nonce takes the genesis-nonce initialization below.
	if currentEra.Id == 0 {
		return nil, nil, nil, nil, nil
	}
	if e.config.CardanoNodeConfig == nil {
		return nil, nil, nil, nil, errors.New("CardanoNodeConfig is nil")
	}
	if e.config.CardanoNodeConfig.ShelleyGenesisHash == "" {
		return nil, nil, nil, nil, errors.New(
			"could not get Shelley genesis hash",
		)
	}
	genesisHashBytes, err := hex.DecodeString(
		e.config.CardanoNodeConfig.ShelleyGenesisHash,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"decode genesis hash: %w", err,
		)
	}

	// For the initial epoch creation (no blocks yet), the epoch nonce and
	// initial evolving/candidate nonces are all the genesis nonce, and the
	// carried lastEpochBlockNonce is Neutral (nil). At genesis cardano-ledger
	// initializes praosStateLastEpochBlockNonce to NeutralNonce, so the FIRST
	// from-genesis epoch boundary takes the identity branch: eta = candidate ⭒
	// NeutralNonce = candidate. Devnet confirms cardano's epoch-1 epochNonce ==
	// its candidate (identity). Do NOT seed this with the genesis nonce — that
	// combines instead of using identity and diverges at the first boundary.
	// The Mithril path never takes this branch (bootstrap epoch imports
	// a non-nil lastEpochBlockNonce).
	if len(currentEpoch.Nonce) == 0 {
		return genesisHashBytes, genesisHashBytes, genesisHashBytes, nil, nil
	}

	// In Ouroboros Praos, the evolving nonce carries across epoch
	// boundaries without resetting (PrtclState is never reset).
	// For migration compatibility (epochs stored before this
	// field existed), fall back to genesis hash.
	prevEvolvingNonce := currentEpoch.EvolvingNonce
	if len(prevEvolvingNonce) == 0 {
		prevEvolvingNonce = genesisHashBytes
	}

	// The candidate nonce also carries across epochs independently
	// of the evolving nonce. When 4k/f >= epochLength (e.g., short
	// devnet epochs), the candidate is never updated by any block
	// and stays at its carried value. Fall back to genesis hash
	// for epochs stored before this field existed.
	prevCandidateNonce := currentEpoch.CandidateNonce
	if len(prevCandidateNonce) == 0 {
		prevCandidateNonce = genesisHashBytes
	}

	// When importing from a snapshot, currentEpoch may carry tip-time
	// nonce state (evolving/candidate already advanced through the
	// imported tip slot). In that case, continue accumulation from the
	// next slot rather than replaying from epoch start.
	computeStartSlot := currentEpoch.StartSlot
	computeEpochLength := uint64(currentEpoch.LengthInSlots)
	epochEndSlot := currentEpoch.StartSlot +
		uint64(currentEpoch.LengthInSlots)
	tipSlot, tipBlockNonce := e.host.TipNonceState()
	tipBlockNonceCopy := append([]byte(nil), tipBlockNonce...)
	if tipSlot >= currentEpoch.StartSlot &&
		tipSlot < epochEndSlot &&
		len(currentEpoch.CandidateNonce) == 32 &&
		len(currentEpoch.EvolvingNonce) == 32 &&
		len(tipBlockNonceCopy) == 32 &&
		bytes.Equal(currentEpoch.EvolvingNonce, tipBlockNonceCopy) {
		if nextSlot := tipSlot + 1; nextSlot < epochEndSlot {
			computeStartSlot = nextSlot
			computeEpochLength = epochEndSlot - nextSlot
		} else {
			// Tip already at/after epoch end: no additional blocks to fold.
			computeEpochLength = 0
		}
	} else if len(currentEpoch.EvolvingNonce) == 32 {
		// Resume fallback: if epoch nonce state was checkpointed at an
		// earlier slot (snapshot import), locate that anchor by matching
		// stored block nonces and continue from the following slot.
		// If no anchor is found, fall through to the defaults which
		// compute from epoch start — this is always correct (just
		// slower) and handles genesis sync where the epoch's
		// EvolvingNonce was set at creation and never updated.
		nonceRows, nonceErr := e.host.BlockNoncesInSlotRange(
			currentEpoch.StartSlot,
			epochEndSlot,
			txn,
		)
		if nonceErr != nil {
			return nil, nil, nil, nil, fmt.Errorf(
				"fetch block nonces in epoch range: %w",
				nonceErr,
			)
		}
		for _, row := range nonceRows {
			if len(row.Nonce) == 32 &&
				bytes.Equal(currentEpoch.EvolvingNonce, row.Nonce) {
				if row.Slot+1 < epochEndSlot {
					computeStartSlot = row.Slot + 1
					computeEpochLength = epochEndSlot -
						computeStartSlot
				} else {
					computeEpochLength = 0
				}
				break
			}
		}
	}

	// Compute candidateNonce (frozen at stability window cutoff)
	// and evolvingNonce (after all blocks) from the remaining
	// current-epoch blocks. Each block's VRF output is accumulated
	// via the Nonce semigroup (⭒) starting from prevEvolvingNonce.
	// The stability window depends on the SOURCE epoch's era — the
	// one being closed — not the era we are transitioning into. The
	// 3k/f window covers Shelley, Allegra, Mary, Alonzo, and Babbage
	// (Babbage runs Praos but retains the smaller window for
	// backwards compatibility); 4k/f kicks in at Conway. The two
	// formulas only disagree at the Babbage→Conway boundary, but the
	// source-vs-target distinction matters for every transition: currentEra
	// describes the successor epoch here, so the source epoch ID must come from
	// currentEpoch.EraId when selecting the nonce window
	// and produce an epoch nonce that diverges from peers. The
	// observed symptom at Alonzo→Babbage was every header in the new
	// Praos epoch VRF-failing; the same shape repeats at
	// Babbage→Conway when this rule is broken.
	candidateNonce, evolvingNonce, err := e.host.ComputeCandidateNonce(
		ctx,
		txn,
		currentEpoch.EraId,
		prevEvolvingNonce,
		prevCandidateNonce,
		computeStartSlot,
		computeEpochLength,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"compute candidate nonce: %w", err,
		)
	}

	// The epoch nonce mixes the frozen candidate with the nonce derived from
	// the last block of the PREVIOUS epoch. In cardano-ledger this is
	// praosStateLastEpochBlockNonce: the value CARRIED in the closing epoch's
	// state (set at the prior epoch boundary) — i.e. the hash of the last block
	// of the epoch *before* the one closing here — NOT the last block of the
	// closing epoch itself. Using the closing epoch's own last block shifts the
	// lab by one epoch and diverges eta from the network, so every leader-VRF
	// check in the new epoch fails:
	//   epochNonce(N+1) = candidateNonce(N) ⭒ currentEpoch(N).LastEpochBlockNonce
	labForEta := cloneNonce(currentEpoch.LastEpochBlockNonce)

	// The carried lab for the NEXT boundary is stored on the new epoch record:
	// prevHashToNonce(lastBlock.prevHash) = the PARENT hash of the last block of
	// the epoch being closed (a one-block Praos lag), NOT the last block's own
	// hash. See epochLabNonce.
	labNonceToSave, err := e.host.EpochLabNonce(
		ctx,
		txn,
		currentEpoch.StartSlot,
		epochEndSlot,
		currentEpoch.LastEpochBlockNonce,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	extraEntropy := ExtraEntropyFromPParams(newEpochPParams)

	// If nil/empty, it's NeutralNonce (identity): result is
	// just candidateNonce.
	if len(labForEta) == 0 && len(extraEntropy) == 0 {
		// NeutralNonce is the identity element of ⭒:
		//   candidateNonce ⭒ NeutralNonce = candidateNonce
		// So the epoch nonce is just the candidate nonce.
		e.config.Logger.Debug(
			"epoch nonce computed (NeutralNonce, using candidateNonce)",
			"component", "ledger",
			"epoch_start_slot", epochStartSlot,
			"candidate_nonce",
			hex.EncodeToString(candidateNonce),
			"lab_nonce_to_save",
			hex.EncodeToString(labNonceToSave),
			"epoch_nonce",
			hex.EncodeToString(candidateNonce),
			"evolving_nonce",
			hex.EncodeToString(evolvingNonce),
		)
		return candidateNonce, evolvingNonce, candidateNonce, labNonceToSave, nil
	}

	// candidateNonce ⭒ labForEta ⭒ extraEntropy
	if len(candidateNonce) < 32 ||
		(len(labForEta) > 0 && len(labForEta) < 32) {
		return nil, nil, nil, nil, fmt.Errorf(
			"epoch nonce requires 32-byte inputs: "+
				"candidateNonce=%d, labForEta=%d",
			len(candidateNonce),
			len(labForEta),
		)
	}
	result, err := AssembleEpochNonce(
		candidateNonce,
		labForEta,
		extraEntropy,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"calculate epoch nonce: %w", err,
		)
	}
	e.config.Logger.Debug(
		"epoch nonce computed",
		"component", "ledger",
		"epoch_start_slot", epochStartSlot,
		"candidate_nonce", hex.EncodeToString(candidateNonce),
		"lab_for_eta",
		hex.EncodeToString(labForEta),
		"lab_nonce_to_save",
		hex.EncodeToString(labNonceToSave),
		"epoch_nonce", hex.EncodeToString(result),
		"evolving_nonce", hex.EncodeToString(evolvingNonce),
		"extra_entropy", hex.EncodeToString(extraEntropy),
	)
	return result, evolvingNonce, candidateNonce, labNonceToSave, nil
}

func cloneNonce(b []byte) []byte {
	return append([]byte(nil), b...)
}
