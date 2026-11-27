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
	"math/big"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// NonceStabilityWindow returns the randomness stabilisation window
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
func NonceStabilityWindow(cfg *cardano.CardanoNodeConfig, eraId uint) uint64 {
	if cfg == nil {
		return 0
	}
	shelleyGenesis := cfg.ShelleyGenesis()
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
