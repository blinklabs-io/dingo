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
		HardForkSummary: func(uint64) (*hardfork.Summary, error) {
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
		HardForkSummary: func(uint64) (*hardfork.Summary, error) { return sum, nil },
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
		HardForkSummary: func(uint64) (*hardfork.Summary, error) {
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
		HardForkSummary: func(uint64) (*hardfork.Summary, error) { return sum, nil },
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
		HardForkSummary: func(uint64) (*hardfork.Summary, error) {
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

// The Preview chain state around the block that wedged issue #3844, taken from
// the chain itself (Koios preview, blocks 168143-168145) and from Preview's
// genesis parameters (securityParam 432, activeSlotsCoeff 0.05, 86400-slot
// epochs at 1s per slot, so the safe zone is 3k/f = 25920 slots):
//
//	block 168143  slot 3516450  the tip the wedged replay had published
//	block 168144  slot 3516496  the wedging block's immediate predecessor
//	block 168145  slot 3516512  the block that could not be applied, carrying a
//	                            Plutus transaction with invalidHereafter 3593399
//
// Anchoring the safe zone at the published tip puts the horizon at 3542400 (the
// epoch 41 boundary): 3516450 + 1 + 25920 = 3542371 snaps up 29 slots to that
// boundary, and the transaction's bound is 50999 slots past it. Anchoring at
// the real predecessor puts it at 3628800 (epoch 42): 3516496 + 1 + 25920 =
// 3542417 is one slot past the epoch 41 boundary, so it snaps to the next one
// and the same transaction converts. One block of tip staleness costs a whole
// epoch of horizon.
const (
	previewEpochSize        = 86_400
	previewSafeZoneSlots    = 25_920
	previewEraStartSlot     = 3_456_000 // epoch 40
	previewEraStartEpoch    = 40
	previewPublishedTipSlot = 3_516_450
	previewParentSlot       = 3_516_496
	previewBlockSlot        = 3_516_512
	previewTxUpperBound     = 3_593_399
	previewTipHorizonSlot   = 3_542_400 // epoch 41
	previewParentHorizon    = 3_628_800 // epoch 42
	previewEraID            = 6
)

// previewSummaryAnchoredAt mirrors LedgerState.hardForkSummaryAnchoredAt for
// the Preview era above: the safe zone is measured from
// max(published tip, horizonAnchorSlot) and snapped up to an epoch boundary by
// the same hardfork.BuildSummary the ledger uses.
func previewSummaryAnchoredAt(
	t testing.TB,
	systemStart time.Time,
	horizonAnchorSlot uint64,
) *hardfork.Summary {
	t.Helper()
	sum, err := hardfork.BuildSummary(
		hardfork.Shape{SystemStart: systemStart},
		nil,
		hardfork.EraSummary{
			EraID: previewEraID,
			Start: hardfork.Bound{
				Slot:  previewEraStartSlot,
				Epoch: previewEraStartEpoch,
			},
			Params: hardfork.EraParams{
				EpochSize:     previewEpochSize,
				SlotLength:    time.Second,
				SafeZoneSlots: previewSafeZoneSlots,
			},
		},
		max(uint64(previewPublishedTipSlot), horizonAnchorSlot),
		hardfork.NewTransitionUnknown(),
	)
	require.NoError(t, err)
	return &sum
}

// previewSlotTime is the wall-clock start of slot for the era built by
// previewSummaryAnchoredAt, whose relative time starts at zero at the era's
// first slot.
func previewSlotTime(systemStart time.Time, slot uint64) time.Time {
	return systemStart.Add(
		time.Duration(slot-previewEraStartSlot) * time.Second,
	)
}

// TestSlotTimeConverter_SlotToTimeWithHorizonFromAnchorsAtParent is the dingo
// #3844 regression, and it is deliberately two-sided.
//
// The accept half: the canonical Preview block at slot 3516512 carries a Plutus
// transaction whose validity upper bound is 3593399. Its script context has to
// convert that bound, and the conversion must succeed, because cardano-node
// applied the block. Measuring the safe zone from the block's own predecessor
// is what makes it succeed.
//
// The reject half: the horizon itself stays. cardano-ledger fails a Plutus
// transaction whose validity bound cannot be translated
// (TimeTranslationPastHorizon), so a bound genuinely past the anchored horizon
// must still be refused rather than extrapolated in-era. Converting it anyway
// would accept blocks the network rejects.
func TestSlotTimeConverter_SlotToTimeWithHorizonFromAnchorsAtParent(
	t *testing.T,
) {
	genesis := testShelleyGenesis(t)
	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return genesis },
		HardForkSummary: func(
			horizonAnchorSlot uint64,
		) (*hardfork.Summary, error) {
			return previewSummaryAnchoredAt(
				t,
				genesis.SystemStart,
				horizonAnchorSlot,
			), nil
		},
	})
	// Pinned to the era start so no near-now extrapolation can account for any
	// result below: every slot under test is weeks away from it.
	conv.nowFunc = func() time.Time {
		return previewSlotTime(genesis.SystemStart, previewEraStartSlot)
	}

	// The published tip is where the horizon used to be measured from, and it
	// is what wedged the replay.
	_, err := conv.SlotToTimeWithHorizonFrom(0, previewTxUpperBound)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"the published tip must still leave this bound past the horizon; "+
			"if it does not, the fixture no longer reproduces #3844")
	_, err = conv.SlotToTime(previewTxUpperBound)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"the unanchored converter is unchanged")

	// Anchored at the block's real predecessor, the same bound converts.
	when, err := conv.SlotToTimeWithHorizonFrom(
		previewParentSlot,
		previewTxUpperBound,
	)
	require.NoError(t, err,
		"the block that wedged the replay must convert its Plutus validity "+
			"bound when the horizon is measured from its own predecessor")
	assert.Equal(
		t,
		previewSlotTime(genesis.SystemStart, previewTxUpperBound),
		when,
	)

	// The anchored horizon is a real bound, not a formality.
	_, err = conv.SlotToTimeWithHorizonFrom(
		previewParentSlot,
		previewParentHorizon,
	)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"a bound past the anchored horizon must still be refused, matching "+
			"cardano-ledger's TimeTranslationPastHorizon")

	// An anchor behind the published tip cannot shrink the horizon.
	_, err = conv.SlotToTimeWithHorizonFrom(
		previewEraStartSlot,
		previewTipHorizonSlot-1,
	)
	require.NoError(t, err)
}

// TestSlotTimeConverter_SlotToTimeWithHorizonFromSharesPrelude pins the shared
// entry conditions (slotToTimePrelude) and the summary build count through the
// anchored path. HardForkSummary walks the whole epoch cache and allocates on
// every call, and this conversion runs once per Plutus transaction.
func TestSlotTimeConverter_SlotToTimeWithHorizonFromSharesPrelude(t *testing.T) {
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
	var lastAnchor uint64
	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return genesis },
		HardForkSummary: func(
			horizonAnchorSlot uint64,
		) (*hardfork.Summary, error) {
			builds++
			lastAnchor = horizonAnchorSlot
			return sum, nil
		},
	})
	conv.nowFunc = func() time.Time { return genesis.SystemStart }

	_, err := conv.SlotToTimeWithHorizonFrom(endSlot-2, endSlot-1)
	require.NoError(t, err)
	assert.Equal(t, 1, builds, "the summary is built once per conversion")
	assert.Equal(t, uint64(endSlot-2), lastAnchor,
		"the anchor must reach the summary source unchanged")

	// Slot 0 answers from genesis without building a summary at all.
	builds = 0
	when, err := conv.SlotToTimeWithHorizonFrom(endSlot-2, 0)
	require.NoError(t, err)
	assert.Equal(t, genesis.SystemStart, when)
	assert.Equal(t, 0, builds, "slot 0 needs no summary")

	// A missing genesis is an error, matching SlotToTime.
	noGenesis := NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return nil },
		HardForkSummary: func(uint64) (*hardfork.Summary, error) {
			return sum, nil
		},
	})
	_, err = noGenesis.SlotToTimeWithHorizonFrom(0, 1)
	require.Error(t, err)
}

// TestSlotTimeConverter_NearNowUsesCachedCurrentEra guards the operational
// near-now fallback against using a forecast successor's parameters.
// HardForkSummary appends that successor to keep header forecasting alive
// across an announced boundary, but the latest persisted epoch identifies the
// era whose parameters are actually current.
func TestSlotTimeConverter_NearNowUsesCachedCurrentEra(t *testing.T) {
	genesis := testShelleyGenesis(t)
	currentSlotLength := time.Second
	currentEraEnd := hardfork.Bound{
		RelativeTime: 500 * currentSlotLength,
		Slot:         500,
		Epoch:        5,
	}
	forecastEnd := hardfork.Bound{
		RelativeTime: 700 * currentSlotLength,
		Slot:         600,
		Epoch:        6,
	}
	sum := &hardfork.Summary{
		SystemStart: genesis.SystemStart,
		Eras: []hardfork.EraSummary{
			{
				EraID: 4,
				Start: hardfork.Bound{},
				End:   &currentEraEnd,
				Params: hardfork.EraParams{
					EpochSize:  100,
					SlotLength: currentSlotLength,
				},
			},
			{
				EraID: 5,
				Start: currentEraEnd,
				End:   &forecastEnd,
				Params: hardfork.EraParams{
					EpochSize:  100,
					SlotLength: 2 * currentSlotLength,
				},
			},
		},
	}
	conv := NewSlotTimeConverter(SlotTimeConverterDeps{
		EpochCache: func() []models.Epoch {
			return []models.Epoch{{
				EpochId:       4,
				StartSlot:     400,
				SlotLength:    1000,
				LengthInSlots: 100,
				EraId:         4,
			}}
		},
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return genesis },
		HardForkSummary: func(uint64) (*hardfork.Summary, error) {
			return sum, nil
		},
	})

	const pastSlot = 650
	// The slot clock asks about the slot it is living in, so pin the clock to
	// the time the cached current era gives that slot. Reading the forecast
	// successor's 2s slot length instead would place it 150s away and fail the
	// operational window.
	wantTime := genesis.SystemStart.Add(pastSlot * currentSlotLength)
	conv.nowFunc = func() time.Time { return wantTime }

	when, err := conv.SlotToTime(pastSlot)
	require.NoError(t, err)
	assert.Equal(
		t,
		wantTime,
		when,
		"near-now extrapolation must use the cached current-era slot length",
	)
}

// TestSlotTimeConverter_SnapshotMismatchFailsClosed models an era publication
// between the independently resolved hard-fork summary and epoch cache. The
// cache's current era does not exist in the older summary, so no extrapolation
// can safely identify the current era. Every path must preserve the bounded
// Summary's ErrPastHorizon result instead of selecting an unrelated era by
// position.
func TestSlotTimeConverter_SnapshotMismatchFailsClosed(t *testing.T) {
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
		EpochCache: func() []models.Epoch {
			return []models.Epoch{{
				EpochId:       5,
				StartSlot:     endSlot,
				SlotLength:    2_000,
				LengthInSlots: epochSize,
				EraId:         5,
			}}
		},
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return genesis },
		HardForkSummary: func(uint64) (*hardfork.Summary, error) {
			return sum, nil
		},
	})
	boundaryTime := genesis.SystemStart.Add(endSlot * slotLength)
	conv.nowFunc = func() time.Time { return boundaryTime }

	tests := []struct {
		name    string
		convert func() error
		message string
	}{
		{
			name: "slot to time",
			convert: func() error {
				_, err := conv.SlotToTime(endSlot)
				return err
			},
			message: "the operational path must remain horizon-bounded on a snapshot mismatch",
		},
		{
			name: "slot to time with horizon anchor",
			convert: func() error {
				_, err := conv.SlotToTimeWithHorizonFrom(endSlot-1, endSlot)
				return err
			},
			message: "the validation path must never extrapolate past its anchored horizon",
		},
		{
			name: "time to slot",
			convert: func() error {
				_, err := conv.TimeToSlot(boundaryTime)
				return err
			},
			message: "the inverse operational path must remain horizon-bounded on a snapshot mismatch",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.ErrorIs(t, test.convert(), hardfork.ErrPastHorizon,
				test.message)
		})
	}
}
