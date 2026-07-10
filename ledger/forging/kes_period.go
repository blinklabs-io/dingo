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

// CurrentKESPeriod returns the KES period containing currentSlot.
//
// Semantics:
//   - currentSlot must be the era-aware absolute slot number from the slot
//     clock. Reconstructing it from Shelley wall-clock slot length is wrong
//     on networks with a Byron prefix.
//   - Returns an error if slotsPerKESPeriod is zero.
func CurrentKESPeriod(
	currentSlot uint64,
	slotsPerKESPeriod uint64,
) (uint64, error) {
	if slotsPerKESPeriod == 0 {
		return 0, errors.New("slotsPerKESPeriod must be positive")
	}
	return currentSlot / slotsPerKESPeriod, nil
}

// CurrentKESPeriodFromGenesis returns the KES period containing currentSlot,
// using slotsPerKESPeriod from Shelley genesis.
func CurrentKESPeriodFromGenesis(
	genesis *shelley.ShelleyGenesis,
	currentSlot uint64,
) (uint64, error) {
	if genesis == nil {
		return 0, errors.New("shelley genesis is nil")
	}
	if genesis.SlotsPerKESPeriod <= 0 {
		return 0, fmt.Errorf(
			"genesis slotsPerKESPeriod must be positive, got %d",
			genesis.SlotsPerKESPeriod,
		)
	}
	// #nosec G115 -- validated positive above, genesis protocol values fit uint64.
	return CurrentKESPeriod(currentSlot, uint64(genesis.SlotsPerKESPeriod))
}

// WallClockKESPeriod returns the KES period implied by Shelley genesis wall
// time. It is retained for tests and diagnostics on networks without a Byron
// prefix. Block production startup must use CurrentKESPeriodFromGenesis with
// an era-aware slot clock instead.
func WallClockKESPeriod(
	genesis *shelley.ShelleyGenesis,
	now time.Time,
) (uint64, error) {
	if genesis == nil {
		return 0, errors.New("shelley genesis is nil")
	}
	if now.Before(genesis.SystemStart) {
		return 0, nil
	}
	slot, err := wallClockSlot(genesis, now)
	if err != nil {
		return 0, err
	}
	return CurrentKESPeriodFromGenesis(genesis, slot)
}

func wallClockSlot(
	genesis *shelley.ShelleyGenesis,
	now time.Time,
) (uint64, error) {
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
	if !slot.IsUint64() {
		return 0, fmt.Errorf(
			"computed slot %s overflows uint64",
			slot.String(),
		)
	}
	return slot.Uint64(), nil
}
