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

package forging

import (
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// CurrentKESPeriod returns the KES period that wall-clock time `now` falls
// in, given the chain's Shelley genesis. It is a pure function so callers
// can drive it with synthetic genesis and timestamps in tests.
//
// Semantics:
//   - Before SystemStart, period is 0 (the chain has not begun).
//   - SlotLength is a rational number of seconds per slot; the math is
//     done in big.Rat to avoid losing precision on chains where slot
//     length is a fraction (e.g. 1/20s on devnets).
//   - Returns an error if SlotsPerKESPeriod is non-positive, SlotLength
//     is non-positive, or the computed period overflows uint64.
func CurrentKESPeriod(genesis *shelley.ShelleyGenesis, now time.Time) (uint64, error) {
	if genesis == nil {
		return 0, errors.New("shelley genesis is nil")
	}
	if genesis.SlotsPerKESPeriod <= 0 {
		return 0, fmt.Errorf(
			"genesis slotsPerKESPeriod must be positive, got %d",
			genesis.SlotsPerKESPeriod,
		)
	}
	if genesis.SlotLength.Rat == nil {
		return 0, errors.New("genesis slotLength is nil")
	}
	slotLengthSeconds := new(big.Rat).Set(genesis.SlotLength.Rat)
	if slotLengthSeconds.Sign() <= 0 {
		return 0, fmt.Errorf(
			"genesis slotLength must be positive, got %s",
			slotLengthSeconds.RatString(),
		)
	}
	if now.Before(genesis.SystemStart) {
		return 0, nil
	}
	// slot = elapsedSeconds / slotLengthSeconds, computed exactly in big.Rat.
	// Convert through nanoseconds to keep both sides as integers.
	elapsedNanos := now.Sub(genesis.SystemStart).Nanoseconds()
	num := new(big.Int).Mul(
		big.NewInt(elapsedNanos),
		slotLengthSeconds.Denom(),
	)
	denom := new(big.Int).Mul(
		slotLengthSeconds.Num(),
		big.NewInt(1_000_000_000),
	)
	slot := new(big.Int).Quo(num, denom)
	period := new(big.Int).Quo(
		slot,
		big.NewInt(int64(genesis.SlotsPerKESPeriod)),
	)
	if !period.IsUint64() {
		return 0, fmt.Errorf(
			"computed KES period %s overflows uint64",
			period.String(),
		)
	}
	return period.Uint64(), nil
}
