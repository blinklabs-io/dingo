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

package cardano

import (
	"fmt"
	"math/big"
	"time"
)

// validateGenesisConsistency cross-checks invariants that must hold
// between, or within, genesis files but are not captured by any single
// file's hash. It asserts two things.
//
// First, that when both a Byron and a Shelley genesis are
// present, the Byron startTime (Unix seconds) equals the Shelley
// systemStart. On every real Cardano network these are the same instant:
// the Shelley systemStart is the chain start time inherited from Byron.
// All slot-to-time and time-to-slot conversion (ledger/hardfork_summary.go,
// ledger/slot.go) is anchored on the Shelley systemStart, so a config
// whose two genesis files disagree would compute wrong wall-clock times
// for Byron-era slots. Failing closed at load time surfaces such a
// misconfiguration immediately rather than as silent time drift later.
//
// Second, that the Shelley epochLength leaves room for the randomness
// stabilisation window; see validateEpochLengthFitsNonceWindow.
func (c *CardanoNodeConfig) validateGenesisConsistency() error {
	if c.byronGenesis == nil || c.shelleyGenesis == nil {
		return nil
	}
	byronStart := int64(c.byronGenesis.StartTime)
	shelleyStart := c.shelleyGenesis.SystemStart.Unix()
	if byronStart != shelleyStart {
		return fmt.Errorf(
			"genesis system start mismatch: Byron startTime %d does not match Shelley systemStart %d (%s); slot-to-time conversion is anchored on the Shelley systemStart and would be wrong for Byron-era slots",
			byronStart,
			shelleyStart,
			c.shelleyGenesis.SystemStart.UTC().Format(time.RFC3339),
		)
	}
	if err := c.validateEpochLengthFitsNonceWindow(); err != nil {
		return err
	}
	return nil
}

// nonceWindowKMultiplier is the k multiplier in the randomness
// stabilisation window used from Conway onwards (4k/f). Earlier eras use
// 3k/f, so checking against the Conway window is the conservative bound:
// a genesis that satisfies it satisfies every era's window.
const nonceWindowKMultiplier = 4

// validateEpochLengthFitsNonceWindow asserts that the Shelley genesis
// epochLength is strictly longer than the randomness stabilisation
// window 4k/f.
//
// Praos freezes the candidate nonce once a block's slot reaches
// firstSlotNextEpoch - 4k/f. Once the window reaches the epoch's own length
// there is no unfrozen portion left, and ledger/candidate_nonce.go pins the
// cutoff to the epoch's first slot, so epoch nonces stop tracking the chain.
// The comparison here is strict for that reason: an epoch exactly as long as
// its window is already degenerate. A genesis in that state also runs epoch
// rollover far more often than the security parameter assumes, which is how
// the bundled devnet used to schedule an epoch boundary every 0.5 seconds.
// Failing closed at load time surfaces the misconfiguration before the node
// forges on it.
func (c *CardanoNodeConfig) validateEpochLengthFitsNonceWindow() error {
	if c.shelleyGenesis == nil {
		return nil
	}
	k := c.shelleyGenesis.SecurityParam
	epochLength := c.shelleyGenesis.EpochLength
	activeSlotsCoeff := c.shelleyGenesis.ActiveSlotsCoeff.Rat
	// A genesis missing any of these is not one this check can speak to;
	// leave it to whatever consumes the zero value.
	if k <= 0 || epochLength <= 0 || activeSlotsCoeff == nil ||
		activeSlotsCoeff.Num().Sign() <= 0 {
		return nil
	}
	// window = ceil(4 * k / f), matching ledger.nonceStabilityWindow.
	numerator := big.NewInt(int64(k))
	numerator.Mul(numerator, big.NewInt(nonceWindowKMultiplier))
	numerator.Mul(numerator, activeSlotsCoeff.Denom())
	window, remainder := new(big.Int).QuoRem(
		numerator,
		activeSlotsCoeff.Num(),
		new(big.Int),
	)
	if remainder.Sign() != 0 {
		window.Add(window, big.NewInt(1))
	}
	if window.Cmp(big.NewInt(int64(epochLength))) < 0 {
		return nil
	}
	return fmt.Errorf(
		"genesis epochLength %d does not exceed the randomness stabilisation window of %s slots (4k/f with securityParam %d and activeSlotsCoeff %s); the candidate nonce would freeze at every epoch start, so raise epochLength or lower securityParam",
		epochLength,
		window.String(),
		k,
		activeSlotsCoeff.RatString(),
	)
}
