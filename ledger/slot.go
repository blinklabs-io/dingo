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
	"fmt"
	"math"
	"math/big"
	"slices"
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
// hardfork.Summary built from the LedgerState's epoch cache. The current era
// can be projected only through its configured safe-zone horizon.
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
	when, sumErr := sum.SlotToTime(slot)
	if sumErr != nil {
		// The operational slot clock converts the next wall-clock slot on
		// every tick. While the applied ledger is far behind the wall clock
		// (from-genesis sync, `dingo load`, restart after downtime) that slot
		// is past the forecast horizon by definition, and the bounded Summary
		// is the wrong instrument: this is wall-clock arithmetic, not a
		// consensus forecast. Without this the clock cannot resolve the next
		// slot boundary, so it logs an error and retries every 100ms for the
		// whole catch-up instead of ticking.
		//
		// Mirrors the current-era extrapolation TimeToSlot already applies for
		// the same reason, and is gated the same way: only a slot whose
		// extrapolated time is near now qualifies, so arbitrary future slots
		// and the bounded Summary used by header validation are unaffected.
		if errors.Is(sumErr, hardfork.ErrPastHorizon) {
			if extrapolated, ok := currentEraTimeAtSlot(sum, slot); ok &&
				isNearNow(extrapolated) {
				return extrapolated, nil
			}
		}
		return time.Time{}, sumErr
	}
	return when, nil
}

// TimeToSlot returns the slot containing the given wall-clock time.
//
// Returns ErrBeforeGenesis when t is before SystemStart. Near-now calls used by
// the operational slot clock retain current-era extrapolation when the ledger
// is empty or behind the HFC forecast horizon; arbitrary time queries remain
// bounded.
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
		if isNearNow(t) {
			return nearNowSlot(shelleyGenesis), nil
		}
		return 0, errors.New("time not found in known epochs")
	}
	slot, sumErr := sum.TimeToSlot(t)
	if sumErr != nil {
		// CurrentSlot drives operational timing while a node catches up after
		// downtime. Preserve its legacy current-era extrapolation without
		// weakening the bounded Summary used by header validation and arbitrary
		// time queries.
		if errors.Is(sumErr, hardfork.ErrPastHorizon) && isNearNow(t) {
			if currentSlot, ok := currentEraSlotAtTime(sum, t); ok {
				return currentSlot, nil
			}
		}
		return 0, fmt.Errorf("time not found in known epochs: %w", sumErr)
	}
	return slot, nil
}

// SlotToEpoch returns the epoch containing the given slot.
//
// Slots within the known epoch cache resolve to the cached epoch's parameters;
// slots past the cache are projected using the current era's parameters only
// through the configured safe-zone horizon. Returns an error for an empty
// cache or for slots outside that range.
func (ls *LedgerState) SlotToEpoch(slot uint64) (models.Epoch, error) {
	sum, err := ls.HardForkSummary()
	if err != nil {
		return models.Epoch{}, errors.New("no epochs in cache")
	}
	info, err := sum.SlotToEpoch(slot)
	if err != nil {
		if errors.Is(err, hardfork.ErrPastHorizon) {
			return models.Epoch{}, fmt.Errorf(
				"slot is outside the known epoch range: %w",
				err,
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

// EpochInfo returns boundary information for the given epoch.
//
// Epochs within the known epoch cache resolve to cached-era parameters; epochs
// past the cache are projected using the current era's parameters only through
// the configured safe-zone horizon. Returns an error for an empty cache or for
// epochs outside that range.
func (ls *LedgerState) EpochInfo(epoch uint64) (models.Epoch, error) {
	cache := ls.loadConsensusSnapshot().epochCache
	for _, cachedEpoch := range slices.Backward(cache) {
		if cachedEpoch.EpochId == epoch {
			return epochBoundaryInfo(cachedEpoch), nil
		}
	}

	sum, err := ls.HardForkSummary()
	if err != nil {
		return models.Epoch{}, errors.New("no epochs in cache")
	}
	info, err := sum.EpochInfo(epoch)
	if err != nil {
		if errors.Is(err, hardfork.ErrPastHorizon) {
			return models.Epoch{}, fmt.Errorf(
				"epoch is outside the known epoch range: %w",
				err,
			)
		}
		return models.Epoch{}, err
	}
	return models.Epoch{
		EpochId:   info.Epoch,
		StartSlot: info.StartSlot,
		EraId:     info.EraID,
		// info.SlotLength is a positive, protocol-bounded duration.
		// #nosec G115
		SlotLength: uint(info.SlotLength / time.Millisecond),
		// info.LengthInSlots is protocol-bounded and persisted as uint.
		// #nosec G115
		LengthInSlots: uint(info.LengthInSlots),
	}, nil
}

func epochBoundaryInfo(epoch models.Epoch) models.Epoch {
	return models.Epoch{
		EpochId:       epoch.EpochId,
		StartSlot:     epoch.StartSlot,
		EraId:         epoch.EraId,
		SlotLength:    epoch.SlotLength,
		LengthInSlots: epoch.LengthInSlots,
	}
}

func isNearNow(t time.Time) bool {
	// time.Since(t) == now - t, so it is negative for future times. Guard both
	// directions so arbitrary future times do not match the operational fallback.
	d := time.Since(t)
	return d >= -5*time.Second && d < 5*time.Second
}

func currentEraSlotAtTime(
	sum *hardfork.Summary,
	t time.Time,
) (uint64, bool) {
	if sum == nil || len(sum.Eras) == 0 {
		return 0, false
	}
	current := sum.Eras[len(sum.Eras)-1]
	relativeTime := t.Sub(sum.SystemStart)
	if relativeTime < current.Start.RelativeTime ||
		current.Params.SlotLength <= 0 {
		return 0, false
	}
	slotOffset := (relativeTime - current.Start.RelativeTime) /
		current.Params.SlotLength
	// slotOffset is non-negative and time.Duration-bounded.
	slotsIntoEra := uint64(slotOffset) // #nosec G115
	if slotsIntoEra > math.MaxUint64-current.Start.Slot {
		return 0, false
	}
	return current.Start.Slot + slotsIntoEra, true
}

// currentEraTimeAtSlot extrapolates a slot's wall-clock start time from the
// current era's parameters, ignoring the era's forecast horizon. It is the
// inverse of currentEraSlotAtTime and carries the same caveat: the result is
// only meaningful for operational near-now timing, because a slot past the
// horizon may in reality fall in a later era with a different slot length.
func currentEraTimeAtSlot(
	sum *hardfork.Summary,
	slot uint64,
) (time.Time, bool) {
	if sum == nil || len(sum.Eras) == 0 {
		return time.Time{}, false
	}
	current := sum.Eras[len(sum.Eras)-1]
	if slot < current.Start.Slot || current.Params.SlotLength <= 0 {
		return time.Time{}, false
	}
	slotsIntoEra := slot - current.Start.Slot
	// Keep the duration multiplication inside time.Duration's range.
	maxSlots := uint64(math.MaxInt64) / uint64(current.Params.SlotLength)
	if slotsIntoEra > maxSlots {
		return time.Time{}, false
	}
	// slotsIntoEra is bounded above, so the conversion cannot overflow.
	inEra := time.Duration(slotsIntoEra) * // #nosec G115
		current.Params.SlotLength
	if inEra > math.MaxInt64-current.Start.RelativeTime {
		return time.Time{}, false
	}
	return sum.SystemStart.Add(current.Start.RelativeTime + inEra), true
}

// shelleySlotLengthMs returns the Shelley genesis slot length in milliseconds,
// or 0 if the genesis is missing or malformed. Shelley genesis stores slot
// length as seconds per slot.
func shelleySlotLengthMs(sg *shelley.ShelleyGenesis) uint64 {
	if sg == nil || sg.SlotLength.Rat == nil ||
		sg.SlotLength.Num().Sign() <= 0 {
		return 0
	}
	return new(big.Int).Div(
		new(big.Int).Mul(big.NewInt(1000), sg.SlotLength.Num()),
		sg.SlotLength.Denom(),
	).Uint64()
}

// shelleySlotLength returns the Shelley-era slot length as a duration, or 0 if
// the genesis is unavailable. The Shelley slot length governs every
// post-Shelley era (including Dijkstra), so it is the unit for converting the
// slot-denominated Leios pipeline windows (CIP-0164) to wall-clock waits.
func (ls *LedgerState) shelleySlotLength() time.Duration {
	if ls.config.CardanoNodeConfig == nil {
		return 0
	}
	slotLenMs := shelleySlotLengthMs(
		ls.config.CardanoNodeConfig.ShelleyGenesis(),
	)
	// slotLenMs is a small, protocol-bounded slot length in milliseconds.
	// #nosec G115
	return time.Duration(slotLenMs) * time.Millisecond
}

// EndorserBlockWaitDuration returns the wall-clock window to wait for a
// referenced/certified endorser block's transaction closure to become
// available, derived from the Leios pipeline timing (EndorserBlockWaitSlots,
// the certify-by deadline) and the Shelley slot length. It returns 0 when the
// wait is disabled or the slot length is unknown. This is the same window
// ledger application uses to gate a ranking block on its endorser block (see
// ensureReferencedEndorserBlocks), so NtC serving and ledger application wait
// for the same healthy closure-delivery window.
func (ls *LedgerState) EndorserBlockWaitDuration() time.Duration {
	if ls.config.EndorserBlockWaitSlots == 0 {
		return 0
	}
	slotLen := ls.shelleySlotLength()
	if slotLen <= 0 {
		return 0
	}
	// EndorserBlockWaitSlots is a small protocol window.
	// #nosec G115
	return time.Duration(ls.config.EndorserBlockWaitSlots) * slotLen
}

// nearNowSlot estimates the current slot from the Shelley genesis slot length,
// used as a fallback when the epoch cache is empty and the caller asks about
// a time within 5s of now.
func nearNowSlot(sg *shelley.ShelleyGenesis) uint64 {
	slotLenMs := shelleySlotLengthMs(sg)
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
