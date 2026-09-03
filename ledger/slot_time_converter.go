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

// operationalWindow is the minimum tolerance for the near-now fallbacks. Eras
// with a longer slot length widen it (see withinOperationalWindow).
const operationalWindow = 5 * time.Second

// SlotTimeConverterDeps supplies the accessors a SlotTimeConverter needs to
// convert between slots and wall-clock time. LedgerState remains the source
// of truth for era history (the consensus snapshot) and genesis config; the
// converter depends on them only through this narrow set of read-only
// callbacks, so it never reaches back into LedgerState's locking or working
// state directly.
type SlotTimeConverterDeps struct {
	// HardForkSummary returns the current hardfork.Summary describing era
	// history, or an error when it cannot be built (e.g. no epochs known
	// yet).
	HardForkSummary func() (*hardfork.Summary, error)
	// ShelleyGenesis returns the Shelley genesis config, or nil if it has
	// not been loaded.
	ShelleyGenesis func() *shelley.ShelleyGenesis
	// EpochCache returns the current epoch cache snapshot, most-recent-last.
	EpochCache func() []models.Epoch
}

// SlotTimeConverter converts between Cardano slots and wall-clock time.
//
// Era-boundary math is delegated to a hardfork.Summary obtained from the
// injected HardForkSummary accessor; this type layers the operational
// near-now fallbacks on top (see withinOperationalWindow) so the slot clock
// can keep resolving the next slot boundary while the applied ledger is
// behind the wall clock (from-genesis sync, `dingo load`, restart after
// downtime).
//
// A SlotTimeConverter holds no lock of its own: era history and genesis are
// read fresh from the injected accessors on every call, so it is safe for
// concurrent use as long as those accessors are.
type SlotTimeConverter struct {
	deps SlotTimeConverterDeps

	// nowFunc overrides the wall clock used by the operational near-now
	// fallbacks. Nil outside tests, which inject a fixed clock so those
	// fallbacks are deterministic.
	nowFunc func() time.Time
}

// NewSlotTimeConverter creates a SlotTimeConverter with the given
// dependencies.
func NewSlotTimeConverter(deps SlotTimeConverterDeps) *SlotTimeConverter {
	return &SlotTimeConverter{deps: deps}
}

// now returns the converter's wall clock. Tests inject a fixed clock so the
// operational near-now fallbacks are deterministic rather than dependent on
// when the suite happens to run.
func (c *SlotTimeConverter) now() time.Time {
	if c.nowFunc != nil {
		return c.nowFunc()
	}
	return time.Now()
}

// shelleyGenesis returns the Shelley genesis config, or nil if unavailable.
func (c *SlotTimeConverter) shelleyGenesis() *shelley.ShelleyGenesis {
	if c.deps.ShelleyGenesis == nil {
		return nil
	}
	return c.deps.ShelleyGenesis()
}

// hardForkSummary returns the current hardfork.Summary, or an error if the
// dependency is unset or fails.
func (c *SlotTimeConverter) hardForkSummary() (*hardfork.Summary, error) {
	if c.deps.HardForkSummary == nil {
		return nil, errors.New("ledger: no hardfork summary source configured")
	}
	return c.deps.HardForkSummary()
}

// epochCache returns the current epoch cache snapshot, or nil if the
// dependency is unset.
func (c *SlotTimeConverter) epochCache() []models.Epoch {
	if c.deps.EpochCache == nil {
		return nil
	}
	return c.deps.EpochCache()
}

// SlotToTime returns the wall-clock start time of the given slot.
//
// Slot 0 always maps to Shelley genesis SystemStart, regardless of whether
// the epoch cache is populated. Other slots are resolved via the
// hardfork.Summary built from the current epoch cache. The current era can be
// projected only through its configured safe-zone horizon.
func (c *SlotTimeConverter) SlotToTime(slot uint64) (time.Time, error) {
	if slot > math.MaxInt64 {
		return time.Time{}, errors.New("slot is larger than time.Duration")
	}
	shelleyGenesis := c.shelleyGenesis()
	if shelleyGenesis == nil {
		return time.Time{}, errors.New("could not get genesis config")
	}
	if slot == 0 {
		return shelleyGenesis.SystemStart, nil
	}
	sum, err := c.hardForkSummary()
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
				withinOperationalWindow(
					c.now(),
					extrapolated,
					currentEraSlotLength(sum),
				) {
				return extrapolated, nil
			}
		}
		return time.Time{}, sumErr
	}
	return when, nil
}

// SlotToTimeInEra converts a slot without the forecast horizon, using the
// current era's parameters when the bounded Summary refuses the slot.
//
// Transaction validation needs this. Building a Plutus script context converts
// the transaction's validity interval to POSIX time, and a legal validity bound
// may sit well past the horizon: on Preview a canonical block at slot 3516512
// carries a bound of 3593399, 50999 slots beyond it. Refusing the conversion
// fails the transaction, so its outputs are never created, the next block that
// spends them trips missing-input recovery, and the replay fails the same way --
// the node cannot follow a chain the reference implementation follows
// (issue #3844).
//
// The horizon is the right bound for *forecasting* across a possible era change,
// which is why header validation and the NtC era-history query keep using
// SlotToTime. It is the wrong bound for a slot inside a block already being
// applied: epoch length and slot length are constant within an era, so the
// projection is exact rather than a guess.
// The guards below mirror SlotToTime rather than delegating to it: the summary
// is rebuilt from the epoch cache on every hardForkSummary call, and delegating
// would build it twice on the past-horizon path -- which is the common path
// here, and runs per transaction.
func (c *SlotTimeConverter) SlotToTimeInEra(slot uint64) (time.Time, error) {
	if slot > math.MaxInt64 {
		return time.Time{}, errors.New("slot is larger than time.Duration")
	}
	shelleyGenesis := c.shelleyGenesis()
	if shelleyGenesis == nil {
		return time.Time{}, errors.New("could not get genesis config")
	}
	if slot == 0 {
		return shelleyGenesis.SystemStart, nil
	}
	sum, err := c.hardForkSummary()
	if err != nil {
		return time.Time{}, err
	}
	when, sumErr := sum.SlotToTime(slot)
	if sumErr == nil {
		return when, nil
	}
	if errors.Is(sumErr, hardfork.ErrPastHorizon) {
		// Unconditional, unlike SlotToTime's near-now gate: a slot inside the
		// current era converts exactly, so there is nothing to guard against.
		if extrapolated, ok := currentEraTimeAtSlot(sum, slot); ok {
			return extrapolated, nil
		}
	}
	// Before the current era's start, or beyond time.Duration's range: the
	// era's parameters cannot answer, so the original error stands.
	return time.Time{}, sumErr
}

// TimeToSlot returns the slot containing the given wall-clock time.
//
// Returns ErrBeforeGenesis when t is before SystemStart. Near-now calls used by
// the operational slot clock retain current-era extrapolation when the ledger
// is empty or behind the HFC forecast horizon; arbitrary time queries remain
// bounded.
func (c *SlotTimeConverter) TimeToSlot(t time.Time) (uint64, error) {
	shelleyGenesis := c.shelleyGenesis()
	if shelleyGenesis == nil {
		return 0, errors.New("could not get genesis config")
	}
	if t.Before(shelleyGenesis.SystemStart) {
		return 0, ErrBeforeGenesis
	}
	sum, err := c.hardForkSummary()
	if err != nil {
		if isNearNow(c.now(), t) {
			return nearNowSlot(shelleyGenesis, c.now()), nil
		}
		return 0, fmt.Errorf("time not found in known epochs: %w", err)
	}
	slot, sumErr := sum.TimeToSlot(t)
	if sumErr != nil {
		// CurrentSlot drives operational timing while a node catches up after
		// downtime. Preserve its legacy current-era extrapolation without
		// weakening the bounded Summary used by header validation and arbitrary
		// time queries.
		// Same slot-length-aware window as SlotToTime: these two are inverses,
		// so a fixed tolerance here would reject the very boundary time
		// SlotToTime just handed out on an era with slots longer than it.
		if errors.Is(sumErr, hardfork.ErrPastHorizon) &&
			withinOperationalWindow(c.now(), t, currentEraSlotLength(sum)) {
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
func (c *SlotTimeConverter) SlotToEpoch(slot uint64) (models.Epoch, error) {
	sum, err := c.hardForkSummary()
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
func (c *SlotTimeConverter) EpochInfo(epoch uint64) (models.Epoch, error) {
	cache := c.epochCache()
	for _, cachedEpoch := range slices.Backward(cache) {
		if cachedEpoch.EpochId == epoch {
			return epochBoundaryInfo(cachedEpoch), nil
		}
	}

	sum, err := c.hardForkSummary()
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

// shelleySlotLength returns the Shelley-era slot length as a duration, or 0 if
// the genesis is unavailable. The Shelley slot length governs every
// post-Shelley era (including Dijkstra), so it is the unit for converting the
// slot-denominated Leios pipeline windows (CIP-0164) to wall-clock waits.
func (c *SlotTimeConverter) shelleySlotLength() time.Duration {
	slotLenMs := shelleySlotLengthMs(c.shelleyGenesis())
	// slotLenMs is a small, protocol-bounded slot length in milliseconds.
	// #nosec G115
	return time.Duration(slotLenMs) * time.Millisecond
}

// EndorserBlockWaitDuration returns the wall-clock window corresponding to
// waitSlots slots at the Shelley-era slot length, or 0 when waitSlots is 0 or
// the slot length is unknown.
func (c *SlotTimeConverter) EndorserBlockWaitDuration(
	waitSlots uint64,
) time.Duration {
	if waitSlots == 0 {
		return 0
	}
	slotLen := c.shelleySlotLength()
	if slotLen <= 0 {
		return 0
	}
	// Keep the multiplication inside time.Duration's range; a configured
	// waitSlots large enough to overflow is a misconfiguration, so disable
	// the wait rather than return a wrapped (possibly negative) duration.
	if waitSlots > uint64(math.MaxInt64)/uint64(slotLen) {
		return 0
	}
	// waitSlots is now bounded above, so the conversion cannot overflow.
	// #nosec G115
	return time.Duration(waitSlots) * slotLen
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

// isNearNow reports whether t is within the operational window of now.
func isNearNow(now, t time.Time) bool {
	return withinOperationalWindow(now, t, 0)
}

// withinOperationalWindow reports whether t is close enough to now to be an
// operational timing query rather than an arbitrary one.
//
// The tolerance is at least operationalWindow but never less than one slot
// length, because the slot clock's purpose is to resolve the *next* slot
// boundary: that time is up to one slot length in the future, so a fixed 5s
// window would reject it on any era with longer slots (Byron is 20s in real
// Cardano shapes) and drop the clock back into the error-retry loop this
// fallback exists to avoid. Times many slot lengths away are still rejected in
// both directions.
func withinOperationalWindow(
	now, t time.Time,
	slotLength time.Duration,
) bool {
	// One slot length covers the clock's next-boundary query exactly; the extra
	// operationalWindow keeps the comparison off that exact edge and absorbs
	// scheduling jitter between computing the slot and checking the window.
	tolerance := operationalWindow
	if slotLength > 0 {
		tolerance = slotLength + operationalWindow
	}
	// now.Sub(t) is negative for future times. Guard both directions so
	// arbitrary future times do not match the operational fallback.
	d := now.Sub(t)
	return d >= -tolerance && d < tolerance
}

// currentEraSlotLength returns the current era's slot length, or 0 when the
// summary has no era to read it from.
func currentEraSlotLength(sum *hardfork.Summary) time.Duration {
	if sum == nil || len(sum.Eras) == 0 {
		return 0
	}
	return sum.Eras[len(sum.Eras)-1].Params.SlotLength
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

// nearNowSlot estimates the current slot from the Shelley genesis slot
// length, used as a fallback when the epoch cache is empty and the caller
// asks about a time within 5s of now.
//
// now is the converter's clock (see SlotTimeConverter.now), not the real wall
// clock: TimeToSlot gates this fallback with isNearNow(c.now(), t), so the
// slot this computes must be derived from that same clock or a test-injected
// nowFunc would desync the gate from the computation.
func nearNowSlot(sg *shelley.ShelleyGenesis, now time.Time) uint64 {
	slotLenMs := shelleySlotLengthMs(sg)
	if slotLenMs == 0 {
		return 0
	}
	// If SystemStart is in the future (clock skew or node started before the
	// configured genesis), elapsed is negative; don't wrap it through
	// uint64 — return 0 so callers see "genesis hasn't happened yet".
	elapsed := now.Sub(sg.SystemStart)
	if elapsed <= 0 {
		return 0
	}
	sinceStartMs := uint64(elapsed / time.Millisecond)
	return sinceStartMs / slotLenMs
}
