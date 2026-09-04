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
	"math"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests exercise SlotTimeConverter directly through NewSlotTimeConverter
// and its injected SlotTimeConverterDeps, without constructing a LedgerState,
// so they cover the extracted subsystem in isolation from the rest of the
// ledger package.

// openEraSummary builds a single-era, unbounded (SafeZoneSlots == 0)
// hardfork.Summary starting at slot 0 / epoch 0 at systemStart.
func openEraSummary(
	systemStart time.Time,
	slotLength time.Duration,
	epochSize uint64,
) *hardfork.Summary {
	return &hardfork.Summary{
		SystemStart: systemStart,
		Eras: []hardfork.EraSummary{
			{
				EraID: 4,
				Start: hardfork.Bound{},
				Params: hardfork.EraParams{
					EpochSize:  epochSize,
					SlotLength: slotLength,
				},
			},
		},
	}
}

// boundedEraSummary builds a single-era hardfork.Summary closed at
// endSlot/endEpoch, so any slot/time at or past that boundary reports
// hardfork.ErrPastHorizon.
func boundedEraSummary(
	systemStart time.Time,
	slotLength time.Duration,
	epochSize uint64,
	endSlot uint64,
) *hardfork.Summary {
	end := hardfork.Bound{
		RelativeTime: time.Duration(endSlot) * slotLength,
		Slot:         endSlot,
		Epoch:        endSlot / epochSize,
	}
	return &hardfork.Summary{
		SystemStart: systemStart,
		Eras: []hardfork.EraSummary{
			{
				EraID: 4,
				Start: hardfork.Bound{},
				End:   &end,
				Params: hardfork.EraParams{
					EpochSize:  epochSize,
					SlotLength: slotLength,
				},
			},
		},
	}
}

func testShelleyGenesis(t testing.TB) *shelley.ShelleyGenesis {
	t.Helper()
	return newTestEraHistoryCfg(t).ShelleyGenesis()
}

func TestSlotTimeConverter_SlotZeroIsSystemStart(t *testing.T) {
	genesis := testShelleyGenesis(t)
	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return genesis },
		HardForkSummary: func() (*hardfork.Summary, error) {
			return nil, errors.New("summary unavailable")
		},
	})

	when, err := conv.SlotToTime(0)
	require.NoError(t, err)
	assert.Equal(t, genesis.SystemStart, when,
		"slot 0 must map to genesis SystemStart even if the summary errors")
}

func TestSlotTimeConverter_NoGenesisErrors(t *testing.T) {
	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return nil },
	})

	_, err := conv.SlotToTime(1)
	require.Error(t, err)

	_, err = conv.TimeToSlot(time.Now())
	require.Error(t, err)
}

func TestSlotTimeConverter_RoundTrip(t *testing.T) {
	genesis := testShelleyGenesis(t)
	const slotLength = time.Second
	const epochSize = 100
	sum := openEraSummary(genesis.SystemStart, slotLength, epochSize)

	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis:  func() *shelley.ShelleyGenesis { return genesis },
		HardForkSummary: func() (*hardfork.Summary, error) { return sum, nil },
	})

	when, err := conv.SlotToTime(250)
	require.NoError(t, err)
	assert.Equal(t, genesis.SystemStart.Add(250*slotLength), when)

	slot, err := conv.TimeToSlot(when)
	require.NoError(t, err)
	assert.Equal(t, uint64(250), slot)

	epoch, err := conv.SlotToEpoch(250)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), epoch.EpochId)
	assert.Equal(t, uint64(200), epoch.StartSlot)
	assert.Equal(t, uint(100), epoch.LengthInSlots)
}

func TestSlotTimeConverter_TimeToSlot_BeforeGenesis(t *testing.T) {
	genesis := testShelleyGenesis(t)
	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return genesis },
	})

	_, err := conv.TimeToSlot(genesis.SystemStart.Add(-time.Second))
	require.ErrorIs(t, err, ErrBeforeGenesis)
}

func TestSlotTimeConverter_HardForkSummaryUnset(t *testing.T) {
	genesis := testShelleyGenesis(t)
	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return genesis },
	})

	_, err := conv.SlotToTime(1)
	require.Error(t, err)

	_, err = conv.SlotToEpoch(1)
	require.Error(t, err)
	assert.Equal(t, "no epochs in cache", err.Error())

	_, err = conv.EpochInfo(1)
	require.Error(t, err)
	assert.Equal(t, "no epochs in cache", err.Error())
}

// TestSlotTimeConverter_EpochInfoPrefersCache proves EpochInfo answers from
// the injected EpochCache before consulting the (possibly unavailable or
// wrong) hardfork.Summary — mirroring the former LedgerState.EpochInfo
// behavior of preferring the authoritative persisted epoch cache.
func TestSlotTimeConverter_EpochInfoPrefersCache(t *testing.T) {
	cached := []models.Epoch{
		{
			EpochId:       7,
			StartSlot:     700,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         4,
		},
	}
	summaryCalls := 0
	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		EpochCache: func() []models.Epoch { return cached },
		HardForkSummary: func() (*hardfork.Summary, error) {
			summaryCalls++
			return nil, errors.New("must not be consulted")
		},
	})

	info, err := conv.EpochInfo(7)
	require.NoError(t, err)
	assert.Zero(t, summaryCalls,
		"a cache hit must not consult the hardfork summary")
	assert.Equal(t, cached[0].StartSlot, info.StartSlot)
	assert.Equal(t, cached[0].LengthInSlots, info.LengthInSlots)
}

// TestSlotTimeConverter_PastHorizon proves that a slot/time past the bounded
// era's end reports hardfork.ErrPastHorizon once outside the operational
// near-now window, and that the near-now extrapolation still resolves the
// very next slot boundary.
func TestSlotTimeConverter_PastHorizon(t *testing.T) {
	genesis := testShelleyGenesis(t)
	const slotLength = time.Second
	const epochSize = 100
	const endSlot = 500
	sum := boundedEraSummary(
		genesis.SystemStart,
		slotLength,
		epochSize,
		endSlot,
	)

	// now is fixed just past the horizon boundary, so the next-slot
	// extrapolation the operational slot clock relies on stays deterministic.
	now := genesis.SystemStart.Add(endSlot * slotLength)
	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis:  func() *shelley.ShelleyGenesis { return genesis },
		HardForkSummary: func() (*hardfork.Summary, error) { return sum, nil },
	})
	conv.nowFunc = func() time.Time { return now }

	// The next slot past the horizon must still resolve via extrapolation.
	when, err := conv.SlotToTime(endSlot)
	require.NoError(t, err)
	assert.Equal(t, now, when)

	// A slot far past the horizon must not be extrapolated.
	_, err = conv.SlotToTime(endSlot + 1_000_000)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon)

	// SlotToEpoch has no near-now fallback and remains strictly bounded.
	_, err = conv.SlotToEpoch(endSlot)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon)
}

func TestSlotTimeConverter_EndorserBlockWaitDuration(t *testing.T) {
	genesis := testShelleyGenesis(t)
	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return genesis },
	})

	assert.Equal(t, time.Duration(0), conv.EndorserBlockWaitDuration(0),
		"zero wait slots must disable the wait")

	got := conv.EndorserBlockWaitDuration(5)
	assert.Positive(t, got, "a positive wait window must be computed")

	noGenesis := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return nil },
	})
	assert.Equal(t, time.Duration(0), noGenesis.EndorserBlockWaitDuration(5),
		"missing genesis must disable the wait rather than panic")
}

// TestSlotTimeConverter_EndorserBlockWaitDurationOverflow proves that a
// waitSlots value large enough to overflow the wait-slots*slotLength
// multiplication is rejected (returns 0) instead of silently wrapping to a
// negative time.Duration.
func TestSlotTimeConverter_EndorserBlockWaitDurationOverflow(t *testing.T) {
	genesis := testShelleyGenesis(t)
	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return genesis },
	})

	got := conv.EndorserBlockWaitDuration(math.MaxUint64)
	assert.Equal(
		t,
		time.Duration(0),
		got,
		"an overflowing waitSlots must disable the wait rather than wrap negative",
	)
}

// TestSlotTimeConverter_TimeToSlotNearNowUsesInjectedClock proves that
// TimeToSlot's near-now fallback (taken when the hardfork summary is
// unavailable) derives the returned slot from the converter's own clock, not
// the real wall clock — so a test-injected nowFunc consistently gates and
// computes the fallback from the same time.
func TestSlotTimeConverter_TimeToSlotNearNowUsesInjectedClock(t *testing.T) {
	genesis := testShelleyGenesis(t)
	// A synthetic "now" far from the real wall clock: if the fallback ever
	// read time.Since(SystemStart) directly, this would compute a wildly
	// different (and likely far-future-looking) slot than the one implied by
	// the injected clock.
	fakeNow := genesis.SystemStart.Add(365 * 24 * time.Hour)

	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return genesis },
		HardForkSummary: func() (*hardfork.Summary, error) {
			return nil, errors.New("no epochs in cache")
		},
	})
	conv.nowFunc = func() time.Time { return fakeNow }

	slot, err := conv.TimeToSlot(fakeNow)
	require.NoError(t, err)

	slotLenMs := shelleySlotLengthMs(genesis)
	require.Positive(t, slotLenMs)
	wantSlot := uint64(fakeNow.Sub(genesis.SystemStart)/time.Millisecond) /
		slotLenMs
	assert.Equal(t, wantSlot, slot,
		"the near-now fallback must derive its slot from the injected clock, "+
			"not the real wall clock")
}

// TestSlotTimeConverter_SlotToTimeInEraIgnoresHorizon is the dingo #3844
// regression. A canonical Preview block at slot 3516512 carries a Plutus
// transaction whose validity upper bound is 3593399 — 50999 slots past the
// forecast horizon. Building the script context has to convert that bound, and
// refusing it fails the transaction, so the node cannot apply a block the
// reference implementation applied.
//
// Within an era the epoch and slot lengths are constant, so the conversion is
// exact at any slot in that era; the horizon bounds forecasting across a
// possible era change, which the script context is not doing.
func TestSlotTimeConverter_SlotToTimeInEraIgnoresHorizon(t *testing.T) {
	genesis := testShelleyGenesis(t)
	const slotLength = time.Second
	const epochSize = 100
	const endSlot = 500
	sum := boundedEraSummary(
		genesis.SystemStart,
		slotLength,
		epochSize,
		endSlot,
	)

	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis:  func() *shelley.ShelleyGenesis { return genesis },
		HardForkSummary: func() (*hardfork.Summary, error) { return sum, nil },
	})
	// Fixed far from the horizon so no near-now extrapolation can apply and
	// the result is attributable to the in-era conversion alone.
	conv.nowFunc = func() time.Time { return genesis.SystemStart }

	const farSlot = endSlot + 1_000_000

	// The bounded path stays bounded: this is what header validation and the
	// NtC era-history query keep using.
	_, err := conv.SlotToTime(farSlot)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"the forecast horizon must still bound the general converter")

	// The validation path resolves it exactly.
	when, err := conv.SlotToTimeInEra(farSlot)
	require.NoError(t, err)
	assert.Equal(t, genesis.SystemStart.Add(farSlot*slotLength), when,
		"a slot inside the current era converts exactly, horizon or not")

	// A slot inside the horizon is unchanged by the in-era path.
	inside, err := conv.SlotToTimeInEra(endSlot - 1)
	require.NoError(t, err)
	bounded, err := conv.SlotToTime(endSlot - 1)
	require.NoError(t, err)
	assert.Equal(t, bounded, inside,
		"below the horizon both paths must agree")
}

// TestSlotTimeConverter_SlotToTimeInEraBuildsSummaryOnce pins the cost of the
// past-horizon path. HardForkSummary walks the whole epoch cache and allocates
// on every call, and this conversion runs per transaction, so resolving a slot
// must not build the summary twice.
//
// It also pins the shared prelude (slotToTimePrelude) through this entry point,
// so a change there cannot silently alter the in-era path.
func TestSlotTimeConverter_SlotToTimeInEraBuildsSummaryOnce(t *testing.T) {
	genesis := testShelleyGenesis(t)
	const slotLength = time.Second
	const epochSize = 100
	const endSlot = 500
	sum := boundedEraSummary(
		genesis.SystemStart,
		slotLength,
		epochSize,
		endSlot,
	)

	var builds int
	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return genesis },
		HardForkSummary: func() (*hardfork.Summary, error) {
			builds++
			return sum, nil
		},
	})
	conv.nowFunc = func() time.Time { return genesis.SystemStart }

	_, err := conv.SlotToTimeInEra(endSlot + 1_000_000)
	require.NoError(t, err)
	assert.Equal(t, 1, builds,
		"the past-horizon fallback must reuse the summary it already built")

	builds = 0
	_, err = conv.SlotToTimeInEra(endSlot - 1)
	require.NoError(t, err)
	assert.Equal(t, 1, builds, "the in-horizon path builds it once too")

	// Slot 0 answers from genesis without building a summary at all.
	builds = 0
	when, err := conv.SlotToTimeInEra(0)
	require.NoError(t, err)
	assert.Equal(t, genesis.SystemStart, when)
	assert.Equal(t, 0, builds, "slot 0 needs no summary")

	// A missing genesis is an error, matching SlotToTime.
	noGenesis := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis:  func() *shelley.ShelleyGenesis { return nil },
		HardForkSummary: func() (*hardfork.Summary, error) { return sum, nil },
	})
	_, err = noGenesis.SlotToTimeInEra(1)
	require.Error(t, err)
}
