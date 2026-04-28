// Copyright 2025 Blink Labs Software
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
	"errors"
	"math"
	"math/big"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// ErrBeforeGenesis is returned by TimeToSlot when the given time is before
// the chain's genesis start. The caller should wait until genesis.
var ErrBeforeGenesis = errors.New("time is before genesis start")

// SlotToTime returns the wall-clock start time of the given slot.
//
// Slot 0 always maps to Shelley genesis SystemStart, regardless of whether
// the epoch cache is populated. Other slots are resolved via the
// hardfork.Summary built from the LedgerState's epoch cache; slots past the
// last known epoch are projected using the current era's slot length (the
// Summary's current era is unbounded, mirroring the legacy projection
// behavior).
func (ls *LedgerState) SlotToTime(slot uint64) (time.Time, error) {
	if slot > math.MaxInt64 {
		return time.Time{}, errors.New("slot is larger than time.Duration")
	}
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return time.Time{}, errors.New("could not get genesis config")
	}
	if slot == 0 {
		return shelleyGenesis.SystemStart, nil
	}
	sum, err := ls.HardForkSummary()
	if err != nil {
		return time.Time{}, err
	}
	return sum.SlotToTime(slot)
}

// TimeToSlot returns the slot containing the given wall-clock time.
//
// Returns ErrBeforeGenesis when t is before SystemStart. When the epoch cache
// is empty but t is within 5 seconds of now, falls back to a coarse
// approximation using the Shelley genesis slot length — this is useful at
// chain genesis before any epochs have been persisted.
func (ls *LedgerState) TimeToSlot(t time.Time) (uint64, error) {
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return 0, errors.New("could not get genesis config")
	}
	if t.Before(shelleyGenesis.SystemStart) {
		return 0, ErrBeforeGenesis
	}
	sum, err := ls.HardForkSummary()
	if err != nil {
		// time.Since(t) == now - t, so it is negative for future times.
		// Guard both directions so arbitrary future times don't match the
		// "near now" fallback.
		if d := time.Since(t); d >= -5*time.Second && d < 5*time.Second {
			return nearNowSlot(shelleyGenesis), nil
		}
		return 0, errors.New("time not found in known epochs")
	}
	slot, sumErr := sum.TimeToSlot(t)
	if sumErr != nil {
		// With an unbounded current era in HardForkSummary this is
		// unreachable for t >= SystemStart, but we preserve the legacy
		// error message for any future bounded-summary configuration.
		return 0, errors.New("time not found in known epochs")
	}
	return slot, nil
}

// SlotToEpoch returns the epoch containing the given slot.
//
// Slots within the known epoch cache resolve to the cached epoch's
// parameters; slots past the cache are projected using the current era's
// parameters. Returns an error for an empty cache or for slots before the
// first known epoch (matching legacy error messages).
func (ls *LedgerState) SlotToEpoch(slot uint64) (models.Epoch, error) {
	sum, err := ls.HardForkSummary()
	if err != nil {
		return models.Epoch{}, errors.New("no epochs in cache")
	}
	info, err := sum.SlotToEpoch(slot)
	if err != nil {
		if errors.Is(err, hardfork.ErrPastHorizon) {
			// ErrPastHorizon fires for slots outside every era's bounds —
			// either below the first era's start or past the last bounded
			// era's end. Don't claim a direction we don't know.
			return models.Epoch{}, errors.New(
				"slot is outside the known epoch range",
			)
		}
		return models.Epoch{}, err
	}
	return models.Epoch{
		EpochId:   info.Epoch,
		StartSlot: info.StartSlot,
		EraId:     info.EraID,
		// info.SlotLength is a positive, protocol-bounded duration; the
		// millisecond quotient fits in uint.
		// #nosec G115
		SlotLength:    uint(info.SlotLength / time.Millisecond),
		LengthInSlots: uint(info.LengthInSlots),
		// Nonce stays nil: unknown for projected epochs, and callers must
		// consult the DB for the persisted nonce of known epochs.
	}, nil
}

// nearNowSlot estimates the current slot from the Shelley genesis slot length,
// used as a fallback when the epoch cache is empty and the caller asks about
// a time within 5s of now.
func nearNowSlot(sg *shelley.ShelleyGenesis) uint64 {
	if sg == nil || sg.SlotLength.Rat == nil ||
		sg.SlotLength.Num().Sign() <= 0 {
		return 0
	}
	// Shelley genesis stores slot length as seconds per slot; compute ms.
	slotLenMs := new(big.Int).Div(
		new(big.Int).Mul(big.NewInt(1000), sg.SlotLength.Num()),
		sg.SlotLength.Denom(),
	).Uint64()
	if slotLenMs == 0 {
		return 0
	}
	// If SystemStart is in the future (clock skew or node started before the
	// configured genesis), time.Since is negative; don't wrap it through
	// uint64 — return 0 so callers see "genesis hasn't happened yet".
	elapsed := time.Since(sg.SystemStart)
	if elapsed <= 0 {
		return 0
	}
	sinceStartMs := uint64(elapsed / time.Millisecond)
	return sinceStartMs / slotLenMs
}
