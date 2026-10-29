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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// gatedHorizonSlotTimeProvider reports slot as past the forecast horizon until
// covered is set, standing in for a ledger that has not yet advanced far
// enough to forecast it.
type gatedHorizonSlotTimeProvider struct {
	SlotTimeProvider
	slot    uint64
	covered *atomic.Bool
}

func (p gatedHorizonSlotTimeProvider) SlotToTime(
	slot uint64,
) (time.Time, error) {
	if slot == p.slot && !p.covered.Load() {
		return time.Time{}, hardfork.ErrPastHorizon
	}
	return p.SlotTimeProvider.SlotToTime(slot)
}

type admissionResult struct {
	accepted bool
	err      error
}

func awaitAdmissionAsync(
	ctx context.Context,
	ls *LedgerState,
	e ChainsyncEvent,
) <-chan admissionResult {
	results := make(chan admissionResult, 1)
	go func() {
		accepted, err := ls.AwaitChainsyncHeaderAdmission(ctx, e)
		results <- admissionResult{accepted: accepted, err: err}
	}()
	return results
}

// A header past the forecast horizon cannot be validated, so admission holds
// it until a ledger publication brings its slot into range. A publication
// that leaves it out of range keeps it held.
func TestAwaitChainsyncHeaderAdmissionHoldsHeaderPastForecastHorizon(
	t *testing.T,
) {
	t.Parallel()

	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	covered := &atomic.Bool{}
	ls.slotClock.provider = gatedHorizonSlotTimeProvider{
		SlotTimeProvider: ls.slotClock.provider,
		slot:             90,
		covered:          covered,
	}

	var witnessCalls atomic.Int32
	e := futureHeaderEvent(90, arrival)
	e.ForecastHorizonWait = func() { witnessCalls.Add(1) }
	results := awaitAdmissionAsync(
		t.Context(),
		ls,
		e,
	)
	testutil.RequireNoReceive(
		t,
		results,
		100*time.Millisecond,
		"a header past the forecast horizon must wait for the ledger",
	)

	ls.notifySnapshotPublished()
	testutil.RequireNoReceive(
		t,
		results,
		100*time.Millisecond,
		"a publication that leaves the slot past the horizon must keep waiting",
	)

	covered.Store(true)
	ls.notifySnapshotPublished()
	got := testutil.RequireReceive(
		t,
		results,
		testutil.AsyncWait,
		"admission did not resume once the ledger could forecast the slot",
	)
	require.NoError(t, got.err)
	require.True(t, got.accepted)
	require.EqualValues(t, 1, witnessCalls.Load())
	require.Empty(
		t,
		*waits,
		"a historical header has no slot onset to wait for",
	)
}

func TestAwaitChainsyncHeaderAdmissionPastForecastHorizonHonorsCancellation(
	t *testing.T,
) {
	t.Parallel()

	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.slotClock.provider = pastHorizonSlotTimeProvider{
		SlotTimeProvider: ls.slotClock.provider,
		rejectedSlot:     90,
	}

	ctx, cancel := context.WithCancel(t.Context())
	results := awaitAdmissionAsync(ctx, ls, futureHeaderEvent(90, arrival))
	testutil.RequireNoReceive(
		t,
		results,
		100*time.Millisecond,
		"a header past the forecast horizon must wait for the ledger",
	)

	cancel()
	got := testutil.RequireReceive(
		t,
		results,
		testutil.AsyncWait,
		"admission did not return after its context ended",
	)
	require.ErrorIs(t, got.err, context.Canceled)
	require.False(t, got.accepted)
}

// A blockfetch request that ends without publishing a snapshot must still wake
// a held header, which then finds no local work and asks for a resync.
func TestAwaitChainsyncHeaderAdmissionWakesOnBlockfetchCapacityChange(
	t *testing.T,
) {
	t.Parallel()

	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.slotClock.provider = pastHorizonSlotTimeProvider{
		SlotTimeProvider: ls.slotClock.provider,
		rejectedSlot:     90,
	}

	results := awaitAdmissionAsync(
		t.Context(),
		ls,
		futureHeaderEvent(90, arrival),
	)
	testutil.RequireNoReceive(
		t,
		results,
		100*time.Millisecond,
		"a header past the forecast horizon must wait while fetch work exists",
	)

	ls.chainsyncBlockfetchMutex.Lock()
	ls.chainsyncBlockfetchReadyChan = nil
	ls.signalChainsyncCapacityChangedLocked()
	ls.chainsyncBlockfetchMutex.Unlock()
	got := testutil.RequireReceive(
		t,
		results,
		testutil.AsyncWait,
		"admission did not wake when blockfetch capacity changed",
	)
	require.ErrorIs(t, got.err, ErrChainsyncHeaderNeedsResync)
	require.False(t, got.accepted)
}

// With no local work left to advance the forecast, the header rechecks
// cancellation and the horizon before asking for a resync.
func TestAwaitChainsyncHeaderAdmissionRechecksBeforeResync(t *testing.T) {
	t.Parallel()

	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)

	t.Run("cancelled", func(t *testing.T) {
		t.Parallel()
		ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)
		ls.slotClock.provider = pastHorizonSlotTimeProvider{
			SlotTimeProvider: ls.slotClock.provider,
			rejectedSlot:     90,
		}
		ctx, cancel := context.WithCancel(t.Context())
		e := futureHeaderEvent(90, arrival)
		e.ForecastHorizonWait = cancel
		results := awaitAdmissionAsync(ctx, ls, e)
		got := testutil.RequireReceive(
			t,
			results,
			testutil.AsyncWait,
			"admission did not return after cancellation",
		)
		require.ErrorIs(t, got.err, context.Canceled)
		require.False(t, got.accepted)
	})

	t.Run("horizon moved", func(t *testing.T) {
		t.Parallel()
		ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)
		covered := &atomic.Bool{}
		ls.slotClock.provider = gatedHorizonSlotTimeProvider{
			SlotTimeProvider: ls.slotClock.provider,
			slot:             90,
			covered:          covered,
		}
		e := futureHeaderEvent(90, arrival)
		e.ForecastHorizonWait = func() { covered.Store(true) }
		results := awaitAdmissionAsync(t.Context(), ls, e)
		got := testutil.RequireReceive(
			t,
			results,
			testutil.AsyncWait,
			"admission did not resume once the slot was forecastable",
		)
		require.NoError(t, got.err)
		require.True(t, got.accepted)
	})
}

func TestSnapshotPublishedChanClosesOnPublish(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	published := ls.snapshotPublishedChan()
	select {
	case <-published:
		t.Fatal("channel closed before any publication")
	default:
	}

	ls.publishSnapshotsLocked()
	testutil.RequireReceive(
		t,
		published,
		testutil.AsyncWait,
		"publication did not close the channel obtained before it",
	)
	select {
	case <-ls.snapshotPublishedChan():
		t.Fatal(
			"a channel obtained after publication must wait for the next one",
		)
	default:
	}
}

// The ledger header path must not queue a header past the forecast horizon
// for blockfetch. It re-intersects the connection instead, so the header is
// delivered again through admission once the ledger can validate it.
func TestChainsyncHeaderPastForecastHorizonIsNotQueued(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		pastHorizon bool
		// trusted marks the header as covered by a Mithril snapshot, so it
		// skips crypto verification; the horizon still applies.
		trusted bool
	}{
		{name: "past the horizon", pastHorizon: true},
		{name: "trusted header past the horizon", pastHorizon: true, trusted: true},
		{name: "within the horizon", pastHorizon: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bus := newTestEventBus(t, nil)
			t.Cleanup(bus.Stop)
			resyncs := make(chan event.ChainsyncResyncEvent, 1)
			bus.SubscribeFunc(
				event.ChainsyncResyncEventType,
				func(evt event.Event) {
					if e, ok := evt.Data.(event.ChainsyncResyncEvent); ok {
						resyncs <- e
					}
				},
			)
			cm, err := chain.NewManager(context.Background(), nil, nil)
			require.NoError(t, err)
			testChain := cm.PrimaryChain()

			header := mockHeader{slot: 1000, blockNumber: 100}
			point := ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			)
			covered := &atomic.Bool{}
			covered.Store(!tc.pastHorizon)
			systemStart := time.Date(
				2026,
				time.August,
				22,
				12,
				0,
				0,
				0,
				time.UTC,
			)
			ls := &LedgerState{
				chain: testChain,
				slotClock: NewSlotClock(
					gatedHorizonSlotTimeProvider{
						SlotTimeProvider: newMockSlotTimeProvider(
							systemStart,
							time.Second,
							100,
						),
						slot:    header.SlotNumber(),
						covered: covered,
					},
					DefaultSlotClockConfig(),
				),
				config: LedgerStateConfig{
					Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
					EventBus: bus,
					BlockfetchRequestRangeFunc: func(
						ouroboros.ConnectionId,
						ocommon.Point,
						ocommon.Point,
					) (uint64, error) {
						return 0, nil
					},
				},
			}
			if tc.trusted {
				ls.mithrilLedgerSlot = header.SlotNumber()
			}
			ls.publishSnapshotsLocked()
			t.Cleanup(func() {
				if ls.chainsyncBlockfetchTimeoutTimer != nil {
					ls.chainsyncBlockfetchTimeoutTimer.Stop()
				}
			})
			connId := testRecycleConnId()

			require.NoError(
				t,
				ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
					ConnectionId: connId,
					BlockHeader:  header,
					Point:        point,
					Tip: ochainsync.Tip{
						Point: ocommon.NewPoint(
							point.Slot+1,
							[]byte("tip"),
						),
						BlockNumber: header.BlockNumber() + 1,
					},
				}),
			)

			if !tc.pastHorizon {
				require.True(t, testChain.FirstHeaderMatchesPoint(point))
				testutil.RequireNoReceive(
					t,
					resyncs,
					100*time.Millisecond,
					"a header within the horizon must not force a re-intersection",
				)
				return
			}
			require.Zero(
				t,
				testChain.HeaderCount(),
				"a header past the forecast horizon must not be queued",
			)
			got := testutil.RequireReceive(
				t,
				resyncs,
				testutil.AsyncWait,
				"a withheld header must re-intersect its connection",
			)
			require.Equal(t, connId, got.ConnectionId)
			require.Equal(t, "header past forecast horizon", got.Reason)
		})
	}
}

func TestResolveForkAnchor(t *testing.T) {
	t.Parallel()

	const tipSlot = 500
	hash := func(label string) []byte { return []byte(label) }
	type peerRecord struct {
		slot   uint64
		parent string
	}
	for _, tc := range []struct {
		name     string
		peer     map[string]peerRecord
		queued   map[string]bool
		held     map[string]bool
		blocks   map[string]uint64
		limit    int
		start    string
		wantSlot uint64
		wantOk   bool
	}{
		{
			name: "fork resolves at the session intersection block",
			peer: map[string]peerRecord{
				"h3": {300, "h2"},
				"h2": {200, "b1"},
			},
			held:     map[string]bool{"b1": true},
			blocks:   map[string]uint64{"b1": 100},
			start:    "h3",
			wantSlot: 100,
			wantOk:   true,
		},
		{
			name:     "fork resolves at a delivered header the local chain holds",
			peer:     map[string]peerRecord{"h2": {250, "b1"}, "b1": {150, "b0"}},
			held:     map[string]bool{"b1": true},
			start:    "h2",
			wantSlot: 150,
			wantOk:   true,
		},
		{
			name:   "a queued local header means the peer extends the tip",
			peer:   map[string]peerRecord{"q1": {600, "b9"}},
			queued: map[string]bool{"q1": true},
			start:  "q1",
		},
		{
			name:   "a held block above the ledger tip means the peer extends the tip",
			held:   map[string]bool{"b6": true},
			blocks: map[string]uint64{"b6": 600},
			start:  "b6",
		},
		{
			name:   "a stored block off the local chain is unresolved",
			blocks: map[string]uint64{"x1": 100},
			start:  "x1",
		},
		{
			name:  "an unknown ancestor is unresolved",
			start: "missing",
		},
		{
			name:   "a header built on origin intersects at slot 0",
			start:  string(make([]byte, 32)),
			wantOk: true,
		},
		{
			name: "a delivered chain built on origin intersects at slot 0",
			peer: map[string]peerRecord{
				"h1": {100, string(make([]byte, 32))},
			},
			start:  "h1",
			wantOk: true,
		},
		{
			name: "the walk stops at the history limit",
			peer: map[string]peerRecord{
				"h4": {400, "h3"},
				"h3": {300, "h2"},
				"h2": {200, "b1"},
			},
			held:   map[string]bool{"b1": true},
			blocks: map[string]uint64{"b1": 100},
			limit:  2,
			start:  "h4",
		},
		{
			name: "fork resolves at a held block reached by the last permitted hop",
			peer: map[string]peerRecord{
				"h3": {300, "h2"},
				"h2": {200, "b1"},
			},
			held:     map[string]bool{"b1": true},
			blocks:   map[string]uint64{"b1": 100},
			limit:    2,
			start:    "h3",
			wantSlot: 100,
			wantOk:   true,
		},
		{
			name: "origin reached by the last permitted hop intersects at slot 0",
			peer: map[string]peerRecord{
				"h2": {200, "h1"},
				"h1": {100, string(make([]byte, 32))},
			},
			limit:  2,
			start:  "h2",
			wantOk: true,
		},
		{
			name: "a stored block off the local chain at the limit is unresolved",
			peer: map[string]peerRecord{
				"h3": {300, "h2"},
				"h2": {200, "x1"},
			},
			blocks: map[string]uint64{"x1": 100},
			limit:  2,
			start:  "h3",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			limit := tc.limit
			if limit == 0 {
				limit = 16
			}
			slot, ok := resolveForkAnchor(
				hash(tc.start),
				tipSlot,
				limit,
				forkAnchorLookups{
					peerHeader: func(h []byte) (ocommon.Point, []byte, bool) {
						record, found := tc.peer[string(h)]
						if !found {
							return ocommon.Point{}, nil, false
						}
						return ocommon.NewPoint(record.slot, h),
							hash(record.parent), true
					},
					queuedHeader: func(p ocommon.Point) bool {
						return tc.queued[string(p.Hash)]
					},
					heldBlock: func(p ocommon.Point) bool {
						return tc.held[string(p.Hash)]
					},
					blockByHash: func(h []byte) (ocommon.Point, bool) {
						slot, found := tc.blocks[string(h)]
						return ocommon.NewPoint(slot, h), found
					},
				},
			)
			require.Equal(t, tc.wantOk, ok)
			require.Equal(t, tc.wantSlot, slot)
		})
	}
}

// A fork that left the local chain before the epoch's nonce cutoff is
// forecast from there, as the reference forecasts a candidate from its
// intersection, so its horizon ends an epoch earlier than the tip's.
func TestForecastSummaryFromIntersectionBeforeNonceCutoff(t *testing.T) {
	t.Parallel()

	cfg := minimalShelleyGenesisCfg(t)
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1_000,
			LengthInSlots: 432_000,
			EraId:         eras.ConwayEraDesc.Id,
		}},
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(302_400, []byte("tip")),
		},
		config: LedgerStateConfig{CardanoNodeConfig: cfg},
	}
	shape := hardfork.Shape{
		SystemStart: cfg.ShelleyGenesis().SystemStart,
		Eras: []hardfork.ShapeEntry{{
			EraID: eras.ConwayEraDesc.Id,
			Params: hardfork.EraParams{
				EpochSize:     432_000,
				SlotLength:    time.Second,
				SafeZoneSlots: 129_600,
				GenesisWindow: 129_600,
			},
		}},
	}
	ls.cachedShape.Store(&shape)
	ls.publishSnapshotsLocked()

	const headerSlot = 500_000
	fromTip, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Equal(t, uint64(864_000), fromTip.Eras[0].End.Slot)
	_, err = fromTip.SlotToEpoch(headerSlot)
	require.NoError(t, err)

	fromIntersection, err := ls.forecastSummaryFrom(300_000)
	require.NoError(t, err)
	require.Equal(t, uint64(432_000), fromIntersection.Eras[0].End.Slot)
	_, err = fromIntersection.SlotToEpoch(headerSlot)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon)

	again, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(864_000),
		again.Eras[0].End.Slot,
		"a forecast from an intersection must not replace the cached tip summary",
	)
}

// Admission measures a header's forecast from where the peer's chain leaves
// the local chain. A fork that left before the epoch's nonce cutoff is held at
// a slot the tip's own forecast covers; the same slot extending the local
// chain is admitted.
func TestAwaitChainsyncHeaderAdmissionForecastsForkFromIntersection(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	primary := cm.PrimaryChain()
	blocks, err := fixtures.GenerateConwayChain(
		1,
		lcommon.Blake2b256{},
		300_000,
		1_000,
		2,
	)
	require.NoError(t, err)
	for _, block := range blocks {
		require.NoError(t, primary.AddBlock(context.Background(), block, nil))
	}

	cfg := minimalShelleyGenesisCfg(t)
	ls := &LedgerState{
		db:    db,
		chain: primary,
		epochCache: []models.Epoch{{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1_000,
			LengthInSlots: 432_000,
			EraId:         eras.ConwayEraDesc.Id,
		}},
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(302_400, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	systemStart := cfg.ShelleyGenesis().SystemStart
	shape := hardfork.Shape{
		SystemStart: systemStart,
		Eras: []hardfork.ShapeEntry{{
			EraID: eras.ConwayEraDesc.Id,
			Params: hardfork.EraParams{
				EpochSize:     432_000,
				SlotLength:    time.Second,
				SafeZoneSlots: 129_600,
				GenesisWindow: 129_600,
			},
		}},
	}
	ls.cachedShape.Store(&shape)
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.slotClock = NewSlotClock(
		newSlotTimeConverterProvider(ls.timeConv()),
		DefaultSlotClockConfig(),
	)
	ls.publishSnapshotsLocked()

	const headerSlot = 500_000
	arrival := systemStart.Add(600_000 * time.Second)
	headerOn := func(parent lcommon.Blake2b256, label string) ChainsyncEvent {
		header := mockHeader{
			hash:        lcommon.Blake2b256Hash([]byte(label)),
			prevHash:    parent,
			blockNumber: 3,
			slot:        headerSlot,
		}
		return ChainsyncEvent{
			BlockHeader: header,
			ArrivalTime: arrival,
			Point:       ocommon.NewPoint(headerSlot, header.Hash().Bytes()),
		}
	}

	extending := headerOn(blocks[1].Hash(), "extending")
	require.False(t, ls.headerBeyondForecastHorizon(t.Context(), extending))
	accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(), extending)
	require.NoError(t, err)
	require.True(t, accepted)

	fork := headerOn(blocks[0].Hash(), "fork")
	require.True(t, ls.headerBeyondForecastHorizon(t.Context(), fork))
	ctx, cancel := context.WithCancel(t.Context())
	results := awaitAdmissionAsync(ctx, ls, fork)
	testutil.RequireNoReceive(
		t,
		results,
		100*time.Millisecond,
		"a fork header past its intersection's forecast must wait",
	)
	cancel()
	got := testutil.RequireReceive(
		t,
		results,
		testutil.AsyncWait,
		"admission did not return after its context ended",
	)
	require.ErrorIs(t, got.err, context.Canceled)
}

// Headers queued behind a far-from-tip batch are what advance the ledger, and
// the ledger is what moves the forecast horizon. Admission must therefore
// start blockfetch for the headers already queued instead of waiting for the
// next header to complete the batch, which it is itself withholding.
func TestAwaitChainsyncHeaderAdmissionStartsBlockfetchForQueuedHeaders(
	t *testing.T,
) {
	t.Parallel()

	cm, err := chain.NewManager(context.Background(), nil, nil)
	require.NoError(t, err)
	testChain := cm.PrimaryChain()
	queued := mockHeader{slot: 10, blockNumber: 1}
	require.NoError(t, testChain.AddBlockHeader(context.Background(), queued))

	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	covered := &atomic.Bool{}
	ranges := make(chan ocommon.Point, 4)
	ls := &LedgerState{
		chain: testChain,
		ctx:   t.Context(),
		slotClock: NewSlotClock(
			gatedHorizonSlotTimeProvider{
				SlotTimeProvider: newMockSlotTimeProvider(
					systemStart,
					time.Second,
					100,
				),
				slot:    90,
				covered: covered,
			},
			DefaultSlotClockConfig(),
		),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				_ ouroboros.ConnectionId,
				start ocommon.Point,
				_ ocommon.Point,
			) (uint64, error) {
				ranges <- start
				return 0, nil
			},
		},
	}
	ls.publishSnapshotsLocked()
	t.Cleanup(func() {
		if ls.chainsyncBlockfetchTimeoutTimer != nil {
			ls.chainsyncBlockfetchTimeoutTimer.Stop()
		}
	})

	e := futureHeaderEvent(90, arrival)
	e.ConnectionId = testRecycleConnId()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	results := awaitAdmissionAsync(ctx, ls, e)

	start := testutil.RequireReceive(
		t,
		ranges,
		testutil.AsyncWait,
		"a header held past the forecast horizon must not strand the queued headers that advance the ledger",
	)
	require.Equal(t, queued.SlotNumber(), start.Slot)

	covered.Store(true)
	ls.notifySnapshotPublished()
	got := testutil.RequireReceive(
		t,
		results,
		testutil.AsyncWait,
		"admission did not resume once the ledger could forecast the slot",
	)
	require.NoError(t, got.err)
	require.True(t, got.accepted)
}

type horizonFlushRequest struct {
	connId ouroboros.ConnectionId
	start  ocommon.Point
}

func TestHorizonFlushPublishesHeaderInvalidation(t *testing.T) {
	t.Parallel()

	for _, fail := range []bool{false, true} {
		name := "successful dispatch"
		if fail {
			name = "failed dispatch"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newHeaderStreamLedger(t)
			header := announcingHeader(
				577, "horizon-header", lcommon.NewBlake2b256(nil), 1,
				lcommon.NewBlake2b256([]byte("announced-eb")),
			)
			require.NoError(
				t,
				fixture.ls.chain.AddVerifiedBlockHeader(t.Context(), header),
			)
			fixture.ls.chain.PublishPendingChainUpdates()
			announcement := testutil.RequireReceive(
				t,
				fixture.ch,
				testutil.AsyncWait,
				"announcement",
			)
			announced, ok := announcement.Data.(chain.ChainHeaderAnnouncementEvent)
			require.True(t, ok)
			fixture.ls.headerPipelineConnId = fixture.connId
			fixture.ls.config.BlockfetchRequestRangeFunc = func(
				ouroboros.ConnectionId, ocommon.Point, ocommon.Point,
			) (uint64, error) {
				if fail {
					return 0, errors.New("simulated blockfetch request failure")
				}
				return 1, nil
			}
			t.Cleanup(func() {
				if fixture.ls.chainsyncBlockfetchTimeoutTimer != nil {
					fixture.ls.chainsyncBlockfetchTimeoutTimer.Stop()
				}
			})

			fixture.ls.flushQueuedHeadersForHorizonWait(fixture.connId)

			if !fail {
				require.Equal(t, 1, fixture.ls.chain.HeaderCount())
				testutil.RequireNoReceive(t, fixture.ch, 100*time.Millisecond,
					"successful horizon flush must keep the announcement armed")
				return
			}
			require.Zero(t, fixture.ls.chain.HeaderCount())
			invalidation := testutil.RequireReceive(
				t,
				fixture.ch,
				testutil.AsyncWait,
				"failed horizon flush must publish invalidation without another event",
			)
			invalid, ok := invalidation.Data.(chain.ChainHeaderInvalidationEvent)
			require.True(t, ok)
			require.Equal(
				t,
				chain.HeaderInvalidationQueueCleared,
				invalid.Reason,
			)
			require.Contains(t, invalid.RbHashes, header.hash)
			require.Greater(t, invalid.Seq, announced.Seq)
		})
	}
}

// newHorizonFlushTestLedger returns a ledger holding one queued header with
// slot 90 past the forecast horizon, recording every blockfetch range request.
func newHorizonFlushTestLedger(
	t *testing.T,
	live func(ouroboros.ConnectionId) bool,
) (*LedgerState, chan horizonFlushRequest) {
	t.Helper()
	cm, err := chain.NewManager(context.Background(), nil, nil)
	require.NoError(t, err)
	testChain := cm.PrimaryChain()
	require.NoError(t, testChain.AddBlockHeader(
		context.Background(),
		mockHeader{slot: 10, blockNumber: 1},
	))
	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	requests := make(chan horizonFlushRequest, 4)
	ls := &LedgerState{
		chain: testChain,
		ctx:   t.Context(),
		slotClock: NewSlotClock(
			gatedHorizonSlotTimeProvider{
				SlotTimeProvider: newMockSlotTimeProvider(
					systemStart,
					time.Second,
					100,
				),
				slot:    90,
				covered: &atomic.Bool{},
			},
			DefaultSlotClockConfig(),
		),
		config: LedgerStateConfig{
			Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
			ConnectionLiveFunc: live,
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				_ ocommon.Point,
			) (uint64, error) {
				requests <- horizonFlushRequest{connId: connId, start: start}
				return 0, nil
			},
		},
	}
	ls.publishSnapshotsLocked()
	t.Cleanup(func() {
		if ls.chainsyncBlockfetchTimeoutTimer != nil {
			ls.chainsyncBlockfetchTimeoutTimer.Stop()
		}
	})
	return ls, requests
}

// The queued headers were delivered by the header pipeline owner, so that is
// the connection that holds them. A different peer held at the horizon, such
// as one whose chain forks below the ledger tip, must not become the fetch
// origin for a range it never announced.
func TestAwaitChainsyncHeaderAdmissionFlushFetchesFromPipelineOwner(
	t *testing.T,
) {
	t.Parallel()

	ls, requests := newHorizonFlushTestLedger(t, nil)
	owner := testRecycleConnId()
	ls.headerPipelineConnId = owner
	e := futureHeaderEvent(
		90,
		time.Date(2026, time.August, 22, 12, 1, 40, 0, time.UTC),
	)
	e.ConnectionId = ouroboros.ConnectionId{
		LocalAddr:  owner.LocalAddr,
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got := testutil.RequireReceive(
		t,
		awaitAdmissionAsync(ctx, ls, e),
		testutil.AsyncWait,
		"canceled admission did not return",
	)
	require.ErrorIs(t, got.err, context.Canceled)

	req := testutil.RequireReceive(
		t,
		requests,
		testutil.AsyncWait,
		"queued headers were not flushed while a header awaited the horizon",
	)
	require.True(
		t,
		sameConnectionId(owner, req.connId),
		"queued range requested from %s, not pipeline owner %s",
		req.connId.String(),
		owner.String(),
	)
	require.True(t, sameConnectionId(owner, ls.selectedBlockfetchConnId))
}

func TestAwaitChainsyncHeaderAdmissionDrainsQueuedWorkBeforeDisconnect(
	t *testing.T,
) {
	t.Parallel()

	ls, requests := newHorizonFlushTestLedger(t, nil)
	owner := testRecycleConnId()
	ls.headerPipelineConnId = owner
	ls.config.BlockfetchRequestRangeFunc = func(
		connId ouroboros.ConnectionId,
		start ocommon.Point,
		_ ocommon.Point,
	) (uint64, error) {
		requests <- horizonFlushRequest{connId: connId, start: start}
		return 1, nil
	}
	arrival := time.Date(2026, time.August, 22, 12, 1, 40, 0, time.UTC)
	header := mockHeader{
		hash:        lcommon.Blake2b256Hash([]byte("disconnected")),
		prevHash:    lcommon.Blake2b256Hash([]byte("missing parent")),
		blockNumber: 3,
		slot:        90,
	}
	e := ChainsyncEvent{
		ConnectionId: owner,
		BlockHeader:  header,
		ArrivalTime:  arrival,
		Point: ocommon.NewPoint(
			header.SlotNumber(),
			header.Hash().Bytes(),
		),
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	results := awaitAdmissionAsync(ctx, ls, e)

	req := testutil.RequireReceive(
		t,
		requests,
		testutil.AsyncWait,
		"a disconnected future header must first flush queued local work",
	)
	require.EqualValues(t, 10, req.start.Slot)
	testutil.RequireNoReceive(
		t,
		results,
		100*time.Millisecond,
		"admission must wait while queued work can advance the forecast",
	)

	cancel()
	got := testutil.RequireReceive(
		t,
		results,
		testutil.AsyncWait,
		"admission did not return after cancellation",
	)
	require.ErrorIs(t, got.err, context.Canceled)
	require.False(t, got.accepted)
}

// A connection whose close was already processed must not be made the
// blockfetch origin or selected connection by a horizon wait it left behind.
func TestAwaitChainsyncHeaderAdmissionFlushSkipsClosedConnection(
	t *testing.T,
) {
	t.Parallel()

	ls, requests := newHorizonFlushTestLedger(
		t,
		func(ouroboros.ConnectionId) bool { return false },
	)
	e := futureHeaderEvent(
		90,
		time.Date(2026, time.August, 22, 12, 1, 40, 0, time.UTC),
	)
	e.ConnectionId = testRecycleConnId()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got := testutil.RequireReceive(
		t,
		awaitAdmissionAsync(ctx, ls, e),
		testutil.AsyncWait,
		"canceled admission did not return",
	)
	require.ErrorIs(t, got.err, context.Canceled)

	require.Empty(t, requests, "blockfetch started on a closed connection")
	require.Empty(t, connIdKey(ls.selectedBlockfetchConnId))
}

// A horizon flush whose blockfetch cannot be dispatched must not leave the
// queued headers stranded behind a wait only their application can end: like
// the header-driven start, it falls back to the active best peer.
func TestAwaitChainsyncHeaderAdmissionFlushFallsBackToActivePeer(
	t *testing.T,
) {
	t.Parallel()

	ls, _ := newHorizonFlushTestLedger(t, nil)
	owner := testRecycleConnId()
	active := ouroboros.ConnectionId{
		LocalAddr:  owner.LocalAddr,
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3003},
	}
	ls.headerPipelineConnId = owner
	ls.config.GetActiveConnectionFunc = func() *ouroboros.ConnectionId {
		return &active
	}
	requests := make(chan ouroboros.ConnectionId, 4)
	ls.config.BlockfetchRequestRangeFunc = func(
		connId ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) (uint64, error) {
		if sameConnectionId(connId, owner) {
			return 0, errors.New("simulated blockfetch request failure")
		}
		requests <- connId
		return 0, nil
	}
	e := futureHeaderEvent(
		90,
		time.Date(2026, time.August, 22, 12, 1, 40, 0, time.UTC),
	)
	e.ConnectionId = owner
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got := testutil.RequireReceive(
		t,
		awaitAdmissionAsync(ctx, ls, e),
		testutil.AsyncWait,
		"canceled admission did not return",
	)
	require.ErrorIs(t, got.err, context.Canceled)

	connId := testutil.RequireReceive(
		t,
		requests,
		testutil.AsyncWait,
		"a failed horizon flush did not fall back to the active best peer",
	)
	require.True(t, sameConnectionId(active, connId))
	require.True(t, sameConnectionId(active, ls.selectedBlockfetchConnId))
}

// When no connection can take the queued range, the horizon flush drops the
// queue and requests a re-sync, as the header-driven start does, rather than
// returning to a wait that nothing can now release.
func TestAwaitChainsyncHeaderAdmissionFlushFailureRequestsResync(
	t *testing.T,
) {
	t.Parallel()

	ls, _ := newHorizonFlushTestLedger(t, nil)
	owner := testRecycleConnId()
	ls.headerPipelineConnId = owner
	ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) (uint64, error) {
		return 0, errors.New("simulated blockfetch request failure")
	}
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus
	resyncSubID, resyncCh := bus.SubscribeWithBuffer(
		event.ChainsyncResyncEventType,
		4,
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, resyncSubID)
	})
	e := futureHeaderEvent(
		90,
		time.Date(2026, time.August, 22, 12, 1, 40, 0, time.UTC),
	)
	e.ConnectionId = owner
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got := testutil.RequireReceive(
		t,
		awaitAdmissionAsync(ctx, ls, e),
		testutil.AsyncWait,
		"canceled admission did not return",
	)
	require.ErrorIs(t, got.err, context.Canceled)

	resyncEvt := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"a failed horizon flush must request a re-sync",
	)
	data, ok := resyncEvt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok, "unexpected event payload %T", resyncEvt.Data)
	require.True(t, sameConnectionId(owner, data.ConnectionId))
	require.Zero(t, ls.chain.HeaderCount())
}

// A connected header past the forecast horizon cannot wait when no queued or
// in-flight blockfetch can publish the ledger state that releases it.
func TestAwaitChainsyncHeaderAdmissionResyncsWhenNoWorkCanAdvanceForecast(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	primary := cm.PrimaryChain()
	blocks, err := fixtures.GenerateConwayChain(
		1,
		lcommon.Blake2b256{},
		300_000,
		1_000,
		2,
	)
	require.NoError(t, err)
	for _, block := range blocks {
		require.NoError(t, primary.AddBlock(context.Background(), block, nil))
	}

	cfg := minimalShelleyGenesisCfg(t)
	ls := &LedgerState{
		db:    db,
		chain: primary,
		epochCache: []models.Epoch{{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1_000,
			LengthInSlots: 432_000,
			EraId:         eras.ConwayEraDesc.Id,
		}},
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(302_400, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	systemStart := cfg.ShelleyGenesis().SystemStart
	const headerSlot = 500_000
	arrival := systemStart.Add(600_000 * time.Second)
	clock := NewSlotClock(
		pastHorizonSlotTimeProvider{
			SlotTimeProvider: newMockSlotTimeProvider(
				systemStart,
				time.Second,
				1_000_000,
			),
			rejectedSlot: headerSlot,
		},
		DefaultSlotClockConfig(),
	)
	clock.nowFunc = func() time.Time { return arrival }
	ls.slotClock = clock
	ls.publishSnapshotsLocked()

	headerOn := func(parent lcommon.Blake2b256, label string) ChainsyncEvent {
		header := mockHeader{
			hash:        lcommon.Blake2b256Hash([]byte(label)),
			prevHash:    parent,
			blockNumber: 3,
			slot:        headerSlot,
		}
		return ChainsyncEvent{
			BlockHeader: header,
			ArrivalTime: arrival,
			Point:       ocommon.NewPoint(headerSlot, header.Hash().Bytes()),
		}
	}

	building := headerOn(
		lcommon.NewBlake2b256(primary.Tip().Point.Hash),
		"building",
	)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	got := testutil.RequireReceive(
		t,
		awaitAdmissionAsync(ctx, ls, building),
		testutil.AsyncWait,
		"admission held a connected header with no queued block work",
	)
	require.ErrorIs(t, got.err, ErrChainsyncHeaderNeedsResync)
	require.False(t, got.accepted)

	orphan := headerOn(
		lcommon.Blake2b256Hash([]byte("unknown parent")),
		"orphan",
	)
	results := awaitAdmissionAsync(t.Context(), ls, orphan)
	got = testutil.RequireReceive(
		t,
		results,
		testutil.AsyncWait,
		"a header whose predecessor the local chain does not hold must not wait for a publication",
	)
	require.ErrorIs(t, got.err, ErrChainsyncHeaderDisconnected)
	require.False(t, got.accepted)
}

// Stored blocks the ledger has not applied yet are local work: applying them
// publishes the snapshots that move the horizon, so a held header waits for
// that rather than closing a connection the work may need. Once the ledger
// catches up with nothing left to apply, the header asks for a resync.
func TestAwaitChainsyncHeaderAdmissionWaitsForUnappliedChainBlocks(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	primary := cm.PrimaryChain()
	blocks, err := fixtures.GenerateConwayChain(
		1,
		lcommon.Blake2b256{},
		300_000,
		1_000,
		2,
	)
	require.NoError(t, err)
	for _, block := range blocks {
		require.NoError(t, primary.AddBlock(context.Background(), block, nil))
	}
	applied := blocks[0]

	cfg := minimalShelleyGenesisCfg(t)
	ls := &LedgerState{
		db:    db,
		chain: primary,
		epochCache: []models.Epoch{{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1_000,
			LengthInSlots: 432_000,
			EraId:         eras.ConwayEraDesc.Id,
		}},
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				applied.SlotNumber(),
				applied.Hash().Bytes(),
			),
			BlockNumber: applied.BlockNumber(),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	systemStart := cfg.ShelleyGenesis().SystemStart
	const headerSlot = 500_000
	arrival := systemStart.Add(600_000 * time.Second)
	clock := NewSlotClock(
		pastHorizonSlotTimeProvider{
			SlotTimeProvider: newMockSlotTimeProvider(
				systemStart,
				time.Second,
				1_000_000,
			),
			rejectedSlot: headerSlot,
		},
		DefaultSlotClockConfig(),
	)
	clock.nowFunc = func() time.Time { return arrival }
	ls.slotClock = clock
	ls.publishSnapshotsLocked()

	header := mockHeader{
		hash:        lcommon.Blake2b256Hash([]byte("past horizon")),
		prevHash:    lcommon.NewBlake2b256(primary.Tip().Point.Hash),
		blockNumber: 3,
		slot:        headerSlot,
	}
	results := awaitAdmissionAsync(t.Context(), ls, ChainsyncEvent{
		BlockHeader: header,
		ArrivalTime: arrival,
		Point:       ocommon.NewPoint(headerSlot, header.Hash().Bytes()),
	})
	testutil.RequireNoReceive(
		t,
		results,
		100*time.Millisecond,
		"a header must wait while stored blocks remain to be applied",
	)
	require.Zero(t, ls.chain.HeaderCount())

	ls.Lock()
	ls.currentTip = primary.Tip()
	ls.publishSnapshotsLocked()
	ls.Unlock()
	got := testutil.RequireReceive(
		t,
		results,
		testutil.AsyncWait,
		"admission did not ask for a resync once nothing was left to apply",
	)
	require.ErrorIs(t, got.err, ErrChainsyncHeaderNeedsResync)
	require.False(t, got.accepted)
}

// chainAheadOfAppliedTip compares the chain tip with the applied tip as points.
// A chain tip in the applied tip's slot under another hash is a block the
// ledger has not applied, so it counts as ahead like a later slot does; a
// chain tip in an earlier slot cannot carry a block that extends the forecast.
func TestChainAheadOfAppliedTip(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	primary := cm.PrimaryChain()
	blocks, err := fixtures.GenerateConwayChain(
		1,
		lcommon.Blake2b256{},
		300_000,
		1_000,
		2,
	)
	require.NoError(t, err)
	for _, block := range blocks {
		require.NoError(t, primary.AddBlock(context.Background(), block, nil))
	}
	chainTip := primary.Tip().Point

	cases := map[string]struct {
		applied ocommon.Point
		want    bool
	}{
		"applied tip is the chain tip": {
			applied: chainTip,
		},
		"chain tip in a later slot": {
			applied: ocommon.NewPoint(chainTip.Slot-1, []byte("earlier")),
			want:    true,
		},
		"chain tip in the same slot under another hash": {
			applied: ocommon.NewPoint(chainTip.Slot, []byte("other fork")),
			want:    true,
		},
		"chain tip in an earlier slot": {
			applied: ocommon.NewPoint(chainTip.Slot+1, []byte("later")),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ls := &LedgerState{chain: primary}
			ls.tip.Store(&tipSnapshot{
				currentTip: ochainsync.Tip{Point: tc.applied},
			})
			require.Equal(t, tc.want, ls.chainAheadOfAppliedTip())
		})
	}
}

// connectsToLocalChain decides whether a held header can still be released by
// blocks the node already has. Every way the walk reaches the local chain must
// count as connected, and only an ancestry that leaves it, or that the
// recorded history cannot resolve, counts as disconnected.
func TestConnectsToLocalChain(t *testing.T) {
	t.Parallel()

	hash := func(label string) []byte { return []byte(label) }
	type peerRecord struct {
		slot   uint64
		parent string
	}
	for _, tc := range []struct {
		name   string
		peer   map[string]peerRecord
		queued map[string]bool
		held   map[string]bool
		blocks map[string]uint64
		limit  int
		start  string
		want   bool
	}{
		{
			name: "delivered headers reach a queued header",
			peer: map[string]peerRecord{
				"h2": {700, "q1"},
				"q1": {600, "b9"},
			},
			queued: map[string]bool{"q1": true},
			start:  "h2",
			want:   true,
		},
		{
			name:  "delivered headers reach a block the local chain holds",
			peer:  map[string]peerRecord{"h2": {250, "b1"}, "b1": {150, "b0"}},
			held:  map[string]bool{"b1": true},
			start: "h2",
			want:  true,
		},
		{
			name: "delivered headers end at the session intersection block",
			peer: map[string]peerRecord{
				"h3": {300, "h2"},
				"h2": {200, "b1"},
			},
			held:   map[string]bool{"b1": true},
			blocks: map[string]uint64{"b1": 100},
			start:  "h3",
			want:   true,
		},
		{
			name:   "a predecessor that is a held block below the tip",
			held:   map[string]bool{"b1": true},
			blocks: map[string]uint64{"b1": 100},
			start:  "b1",
			want:   true,
		},
		{
			name:   "a stored block off the local chain is disconnected",
			blocks: map[string]uint64{"x1": 100},
			start:  "x1",
		},
		{
			name:  "an unknown predecessor is disconnected",
			start: "missing",
		},
		{
			name: "delivered headers ending at an unknown ancestor are disconnected",
			peer: map[string]peerRecord{
				"h3": {300, "h2"},
				"h2": {200, "missing"},
			},
			start: "h3",
		},
		{
			name:  "a header built on origin is connected",
			start: string(make([]byte, 32)),
			want:  true,
		},
		{
			name: "a delivered chain built on origin is connected",
			peer: map[string]peerRecord{
				"h1": {100, string(make([]byte, 32))},
			},
			start: "h1",
			want:  true,
		},
		{
			name: "an ancestry needing one more peer header than the limit is disconnected",
			peer: map[string]peerRecord{
				"h4": {400, "h3"},
				"h3": {300, "h2"},
				"h2": {200, "b1"},
			},
			held:   map[string]bool{"b1": true},
			blocks: map[string]uint64{"b1": 100},
			limit:  2,
			start:  "h4",
		},
		{
			name: "a held block reached by the last permitted hop is connected",
			peer: map[string]peerRecord{
				"h3": {300, "h2"},
				"h2": {200, "b1"},
			},
			held:   map[string]bool{"b1": true},
			blocks: map[string]uint64{"b1": 100},
			limit:  2,
			start:  "h3",
			want:   true,
		},
		{
			name: "origin reached by the last permitted hop is connected",
			peer: map[string]peerRecord{
				"h2": {200, "h1"},
				"h1": {100, string(make([]byte, 32))},
			},
			limit: 2,
			start: "h2",
			want:  true,
		},
		{
			name: "a stored block off the local chain at the limit is disconnected",
			peer: map[string]peerRecord{
				"h3": {300, "h2"},
				"h2": {200, "x1"},
			},
			blocks: map[string]uint64{"x1": 100},
			limit:  2,
			start:  "h3",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			limit := tc.limit
			if limit == 0 {
				limit = 16
			}
			got := connectsToLocalChain(
				hash(tc.start),
				limit,
				forkAnchorLookups{
					peerHeader: func(h []byte) (ocommon.Point, []byte, bool) {
						record, found := tc.peer[string(h)]
						if !found {
							return ocommon.Point{}, nil, false
						}
						return ocommon.NewPoint(record.slot, h),
							hash(record.parent), true
					},
					queuedHeader: func(p ocommon.Point) bool {
						return tc.queued[string(p.Hash)]
					},
					heldBlock: func(p ocommon.Point) bool {
						return tc.held[string(p.Hash)]
					},
					blockByHash: func(h []byte) (ocommon.Point, bool) {
						slot, found := tc.blocks[string(h)]
						return ocommon.NewPoint(slot, h), found
					},
				},
			)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestAdmissionHoldsQueuedParentWithoutPeerHistory(
	t *testing.T,
) {
	t.Parallel()

	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)
	cm, err := chain.NewManager(context.Background(), nil, nil)
	require.NoError(t, err)
	ls.chain = cm.PrimaryChain()
	ls.config.BlockfetchRequestRangeFunc = func(
		ouroboros.ConnectionId, ocommon.Point, ocommon.Point,
	) (uint64, error) {
		return 1, nil
	}
	t.Cleanup(func() {
		if ls.chainsyncBlockfetchTimeoutTimer != nil {
			ls.chainsyncBlockfetchTimeoutTimer.Stop()
		}
	})
	parentHash := lcommon.Blake2b256Hash([]byte("queued parent"))
	require.NoError(t, ls.chain.AddBlockHeader(context.Background(), mockHeader{
		hash:        parentHash,
		prevHash:    lcommon.Blake2b256{},
		blockNumber: 1,
		slot:        50,
	}))
	require.NoError(t, ls.chain.AddBlockHeader(context.Background(), mockHeader{
		hash:        lcommon.Blake2b256Hash([]byte("queued head")),
		prevHash:    parentHash,
		blockNumber: 2,
		slot:        60,
	}))
	ls.publishSnapshotsLocked()
	covered := &atomic.Bool{}
	ls.slotClock.provider = gatedHorizonSlotTimeProvider{
		SlotTimeProvider: ls.slotClock.provider,
		slot:             90,
		covered:          covered,
	}

	e := futureHeaderEvent(90, arrival)
	e.BlockHeader = mockHeader{
		hash:        lcommon.Blake2b256Hash([]byte("future header")),
		prevHash:    parentHash,
		blockNumber: 2,
		slot:        90,
	}
	results := awaitAdmissionAsync(t.Context(), ls, e)
	testutil.RequireNoReceive(
		t,
		results,
		100*time.Millisecond,
		"a header past the forecast horizon must wait for the ledger",
	)

	ls.notifySnapshotPublished()
	testutil.RequireNoReceive(
		t,
		results,
		100*time.Millisecond,
		"a publication that leaves the slot past the horizon must keep waiting",
	)

	covered.Store(true)
	ls.notifySnapshotPublished()
	got := testutil.RequireReceive(
		t,
		results,
		testutil.AsyncWait,
		"admission did not resume once the ledger could forecast the slot",
	)
	require.NoError(t, got.err)
	require.True(t, got.accepted)
	require.Empty(t, *waits, "a historical header has no slot onset to wait for")
}

func TestForkWalkRecognizesTerminalLocalPoint(t *testing.T) {
	t.Parallel()
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprintf("queued=%t", queued), func(t *testing.T) {
			lookups := forkAnchorLookups{
				peerHeader: func(hash []byte) (ocommon.Point, []byte, bool) {
					if bytes.Equal(hash, []byte("start")) {
						return ocommon.NewPoint(20, hash), []byte("anchor"), true
					}
					return ocommon.NewPoint(10, hash), []byte("older"), true
				},
				queuedHeader: func(p ocommon.Point) bool {
					return queued && bytes.Equal(p.Hash, []byte("anchor"))
				},
				heldBlock: func(p ocommon.Point) bool {
					return !queued && bytes.Equal(p.Hash, []byte("anchor"))
				},
				blockByHash: func([]byte) (ocommon.Point, bool) { return ocommon.Point{}, false },
			}
			require.True(t,
				connectsToLocalChain([]byte("start"), 1, lookups),
				"terminal local point must connect without another ancestry hop",
			)
			anchor, below := resolveForkAnchor([]byte("start"), 30, 1, lookups)
			if queued {
				require.False(t, below)
			} else {
				require.True(t, below, "terminal applied point must resolve the fork anchor")
				require.Equal(t, uint64(10), anchor)
			}
		})
	}
}

func TestFarFromTipBlockfetchLowWatermark(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		lowMark   bool
		headers   int
		wantFetch bool
	}{
		{name: "default holds below the four-range threshold", headers: 600},
		{name: "low watermark starts at one range", lowMark: true, headers: BlockfetchBatchSize, wantFetch: true},
		{name: "low watermark waits below one range", lowMark: true, headers: BlockfetchBatchSize - 1},
		{name: "default starts at the four-range threshold", headers: BlockfetchBatchSize * 4, wantFetch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cm, err := chain.NewManager(context.Background(), nil, nil)
			require.NoError(t, err)
			requests := make(chan [2]ocommon.Point, 8)
			ls := &LedgerState{
				chain: cm.PrimaryChain(),
				config: LedgerStateConfig{
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
					BlockfetchLowWatermarkEnabled: tc.lowMark,
					BlockfetchRequestRangeFunc: func(
						_ ouroboros.ConnectionId,
						start ocommon.Point,
						end ocommon.Point,
					) (uint64, error) {
						requests <- [2]ocommon.Point{start, end}
						return 1, nil
					},
				},
			}
			ls.publishSnapshotsLocked()
			t.Cleanup(func() {
				if ls.chainsyncBlockfetchTimeoutTimer != nil {
					ls.chainsyncBlockfetchTimeoutTimer.Stop()
				}
			})
			connId := testRecycleConnId()
			tip := ochainsync.Tip{
				Point:       ocommon.NewPoint(100_000_000, []byte("tip")),
				BlockNumber: 10_000_000,
			}

			prev := lcommon.NewBlake2b256(nil)
			for i := 1; i <= tc.headers; i++ {
				hash := lcommon.NewBlake2b256(
					testHashBytes(fmt.Sprintf("lowmark-%d", i)),
				)
				header := mockHeader{
					hash:        hash,
					prevHash:    prev,
					blockNumber: uint64(i),
					slot:        uint64(i),
				}
				prev = hash
				require.NoError(
					t,
					ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
						ConnectionId: connId,
						BlockHeader:  header,
						Point: ocommon.NewPoint(
							header.SlotNumber(),
							hash.Bytes(),
						),
						Tip: tip,
					}),
				)
			}

			if !tc.wantFetch {
				testutil.RequireNoReceive(
					t,
					requests,
					100*time.Millisecond,
					"blockfetch must wait for the configured header count",
				)
				return
			}
			testutil.RequireReceive(
				t,
				requests,
				testutil.AsyncWait,
				"blockfetch must start once the gate is met",
			)
			require.Less(t, ls.chain.HeaderCount(), ls.allowedQueuedHeaders()+1)
		})
	}
}

// The forecast horizon holds the header that follows the queued ones; once the
// ledger can forecast its slot and the connection re-delivers it, the far-from-
// tip gate decides whether blockfetch starts at one range or at four ranges.
func TestFarFromTipBlockfetchLowWatermarkAfterHorizonRelease(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		lowMark   bool
		wantFetch bool
	}{
		{name: "default keeps waiting for the four-range threshold"},
		{name: "low watermark starts at one range", lowMark: true, wantFetch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bus := event.NewEventBus(nil, nil)
			t.Cleanup(bus.Stop)
			resyncs := make(chan event.ChainsyncResyncEvent, 1)
			bus.SubscribeFunc(
				event.ChainsyncResyncEventType,
				func(evt event.Event) {
					if e, ok := evt.Data.(event.ChainsyncResyncEvent); ok {
						resyncs <- e
					}
				},
			)
			cm, err := chain.NewManager(context.Background(), nil, nil)
			require.NoError(t, err)
			requests := make(chan [2]ocommon.Point, 8)
			// The last header of the first range is the one the horizon holds.
			heldSlot := uint64(BlockfetchBatchSize)
			covered := &atomic.Bool{}
			systemStart := time.Date(
				2026,
				time.August,
				22,
				12,
				0,
				0,
				0,
				time.UTC,
			)
			ls := &LedgerState{
				chain: cm.PrimaryChain(),
				slotClock: NewSlotClock(
					gatedHorizonSlotTimeProvider{
						SlotTimeProvider: newMockSlotTimeProvider(
							systemStart,
							time.Second,
							100,
						),
						slot:    heldSlot,
						covered: covered,
					},
					DefaultSlotClockConfig(),
				),
				config: LedgerStateConfig{
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
					EventBus:                      bus,
					BlockfetchLowWatermarkEnabled: tc.lowMark,
					BlockfetchRequestRangeFunc: func(
						_ ouroboros.ConnectionId,
						start ocommon.Point,
						end ocommon.Point,
					) (uint64, error) {
						requests <- [2]ocommon.Point{start, end}
						return 1, nil
					},
				},
			}
			ls.publishSnapshotsLocked()
			t.Cleanup(func() {
				if ls.chainsyncBlockfetchTimeoutTimer != nil {
					ls.chainsyncBlockfetchTimeoutTimer.Stop()
				}
			})
			connId := testRecycleConnId()
			tip := ochainsync.Tip{
				Point:       ocommon.NewPoint(100_000_000, []byte("tip")),
				BlockNumber: 10_000_000,
			}
			prev := lcommon.NewBlake2b256(nil)
			deliver := func(i int) {
				t.Helper()
				hash := lcommon.NewBlake2b256(
					testHashBytes(fmt.Sprintf("horizon-lowmark-%d", i)),
				)
				header := mockHeader{
					hash:        hash,
					prevHash:    prev,
					blockNumber: uint64(i),
					slot:        uint64(i),
				}
				require.NoError(t, ls.handleEventChainsyncBlockHeader(
					ChainsyncEvent{
						ConnectionId: connId,
						BlockHeader:  header,
						Point: ocommon.NewPoint(
							header.SlotNumber(),
							hash.Bytes(),
						),
						Tip: tip,
					},
				))
				// A header the horizon withholds is not queued, so the next
				// delivery must still chain from the last queued header.
				if ls.chain.HeaderCount() == i {
					prev = hash
				}
			}

			for i := 1; i < BlockfetchBatchSize; i++ {
				deliver(i)
			}
			deliver(BlockfetchBatchSize)
			require.Equal(t, BlockfetchBatchSize-1, ls.chain.HeaderCount())
			got := testutil.RequireReceive(
				t,
				resyncs,
				testutil.AsyncWait,
				"a header past the horizon must re-intersect its connection",
			)
			require.Equal(t, "header past forecast horizon", got.Reason)
			testutil.RequireNoReceive(
				t,
				requests,
				100*time.Millisecond,
				"no blockfetch may start while the horizon holds the header",
			)

			covered.Store(true)
			deliver(BlockfetchBatchSize)
			require.Equal(t, BlockfetchBatchSize, ls.chain.HeaderCount())

			if !tc.wantFetch {
				testutil.RequireNoReceive(
					t,
					requests,
					100*time.Millisecond,
					"the default gate must keep waiting for the four-range threshold",
				)
				return
			}
			testutil.RequireReceive(
				t,
				requests,
				testutil.AsyncWait,
				"the released range must be requested at one range of headers",
			)
		})
	}
}

// An observer header past the forecast horizon waits for a publication that
// brings its slot into range, and neither fetches nor requests a resync.
func TestAwaitObserverHeaderHorizonHoldsUntilForecastable(t *testing.T) {
	t.Parallel()

	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)
	covered := &atomic.Bool{}
	ls.slotClock.provider = gatedHorizonSlotTimeProvider{
		SlotTimeProvider: ls.slotClock.provider,
		slot:             90,
		covered:          covered,
	}
	var witnessCalls atomic.Int32
	e := futureHeaderEvent(90, arrival)
	e.ForecastHorizonWait = func() { witnessCalls.Add(1) }

	results := make(chan error, 1)
	go func() { results <- ls.AwaitObserverHeaderHorizon(t.Context(), e) }()
	testutil.RequireNoReceive(
		t, results, 100*time.Millisecond,
		"an observer header past the forecast horizon must wait",
	)
	ls.notifySnapshotPublished()
	testutil.RequireNoReceive(
		t, results, 100*time.Millisecond,
		"a publication that leaves the slot past the horizon must keep waiting",
	)
	covered.Store(true)
	ls.notifySnapshotPublished()
	require.NoError(t, testutil.RequireReceive(
		t, results, testutil.AsyncWait,
		"observer wait did not resume once the slot was forecastable",
	))
	require.Zero(t, witnessCalls.Load(),
		"an observer must not publish a forecast witness")
}

func TestAwaitObserverHeaderHorizonHonorsCancellation(t *testing.T) {
	t.Parallel()

	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)
	ls.slotClock.provider = gatedHorizonSlotTimeProvider{
		SlotTimeProvider: ls.slotClock.provider,
		slot:             90,
		covered:          &atomic.Bool{},
	}
	ctx, cancel := context.WithCancel(t.Context())
	results := make(chan error, 1)
	go func() {
		results <- ls.AwaitObserverHeaderHorizon(
			ctx, futureHeaderEvent(90, arrival),
		)
	}()
	testutil.RequireNoReceive(t, results, 50*time.Millisecond, "must wait")
	cancel()
	err := testutil.RequireReceive(
		t, results, testutil.AsyncWait, "cancellation must end the wait",
	)
	require.ErrorIs(t, err, context.Canceled)
}

// newObserverForkTestLedger returns a ledger whose tip lies above blocks[0] of
// its primary chain, and an observer header at slot 500_000 built on
// blocks[0]: a fork whose intersection's forecast does not cover the header,
// although the tip's does. Each walk of the peer's ancestry consults
// PeerHeaderLookupFunc once per hop, counted in lookups.
func newObserverForkTestLedger(
	t *testing.T,
) (*LedgerState, *chain.Chain, []lcommon.Block, ChainsyncEvent, *atomic.Int32) {
	t.Helper()
	return newObserverForkTestLedgerAt(t, 300_000, 1_000, 302_400)
}

// newObserverForkTestLedgerAt is newObserverForkTestLedger with the two stored
// blocks at firstSlot and firstSlot+step, and the ledger tip at tipSlot.
func newObserverForkTestLedgerAt(
	t *testing.T,
	firstSlot, step uint64,
	tipSlot uint64,
) (*LedgerState, *chain.Chain, []lcommon.Block, ChainsyncEvent, *atomic.Int32) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	primary := cm.PrimaryChain()
	blocks, err := fixtures.GenerateConwayChain(
		1,
		lcommon.Blake2b256{},
		firstSlot,
		step,
		2,
	)
	require.NoError(t, err)
	for _, block := range blocks {
		require.NoError(t, primary.AddBlock(context.Background(), block, nil))
	}
	lookups := &atomic.Int32{}
	cfg := minimalShelleyGenesisCfg(t)
	ls := &LedgerState{
		db:    db,
		chain: primary,
		epochCache: []models.Epoch{{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1_000,
			LengthInSlots: 432_000,
			EraId:         eras.ConwayEraDesc.Id,
		}},
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
			PeerHeaderLookupFunc: func(
				ouroboros.ConnectionId,
				[]byte,
			) (ChainsyncEvent, []byte, bool) {
				lookups.Add(1)
				return ChainsyncEvent{}, nil, false
			},
		},
	}
	systemStart := cfg.ShelleyGenesis().SystemStart
	shape := hardfork.Shape{
		SystemStart: systemStart,
		Eras: []hardfork.ShapeEntry{{
			EraID: eras.ConwayEraDesc.Id,
			Params: hardfork.EraParams{
				EpochSize:     432_000,
				SlotLength:    time.Second,
				SafeZoneSlots: 129_600,
				GenesisWindow: 129_600,
			},
		}},
	}
	ls.cachedShape.Store(&shape)
	ls.slotClock = NewSlotClock(
		newSlotTimeConverterProvider(ls.timeConv()),
		DefaultSlotClockConfig(),
	)
	ls.publishSnapshotsLocked()

	const headerSlot = 500_000
	header := mockHeader{
		hash:        lcommon.Blake2b256Hash([]byte("observer fork")),
		prevHash:    blocks[0].Hash(),
		blockNumber: 3,
		slot:        headerSlot,
	}
	e := ChainsyncEvent{
		BlockHeader: header,
		ArrivalTime: systemStart.Add(600_000 * time.Second),
		Point:       ocommon.NewPoint(headerSlot, header.Hash().Bytes()),
	}
	return ls, primary, blocks, e, lookups
}

// A held observer header wakes on every publication. A publication that leaves
// it past its fork's forecast horizon must not walk the peer's ancestry again.
func TestAwaitObserverHeaderHorizonResolvesForkAnchorOnce(t *testing.T) {
	t.Parallel()

	ls, _, _, e, lookups := newObserverForkTestLedger(t)
	results := make(chan error, 1)
	go func() { results <- ls.AwaitObserverHeaderHorizon(t.Context(), e) }()
	require.Eventually(t, func() bool { return lookups.Load() > 0 },
		testutil.AsyncWait, time.Millisecond,
		"the held header's fork anchor was never resolved")
	testutil.RequireNoReceive(
		t, results, 100*time.Millisecond,
		"a fork header past its intersection's forecast must wait",
	)
	resolved := lookups.Load()
	for range 3 {
		ls.notifySnapshotPublished()
		testutil.RequireNoReceive(
			t, results, 50*time.Millisecond,
			"a publication that leaves the slot past the horizon must keep waiting",
		)
	}
	require.Equal(t, resolved, lookups.Load(),
		"a wake that cannot release the header must not resolve its anchor again")
}

// A rollback can withdraw the block a held header's fork anchor rests on, so
// the next wake resolves the anchor again. Here the rollback leaves the
// header's parent as the chain tip, which releases it.
func TestAwaitObserverHeaderHorizonReresolvesAfterRollback(t *testing.T) {
	t.Parallel()

	ls, primary, blocks, e, lookups := newObserverForkTestLedger(t)
	results := make(chan error, 1)
	go func() { results <- ls.AwaitObserverHeaderHorizon(t.Context(), e) }()
	require.Eventually(t, func() bool { return lookups.Load() > 0 },
		testutil.AsyncWait, time.Millisecond,
		"the held header's fork anchor was never resolved")
	testutil.RequireNoReceive(
		t, results, 100*time.Millisecond,
		"a fork header past its intersection's forecast must wait",
	)
	require.NoError(t, primary.RollbackUnbounded(
		t.Context(),
		ocommon.NewPoint(blocks[0].SlotNumber(), blocks[0].Hash().Bytes()),
	))
	ls.notifySnapshotPublished()
	require.NoError(t, testutil.RequireReceive(
		t, results, testutil.AsyncWait,
		"a rollback to the header's parent must release it",
	))
}

// An observer whose ancestry the node cannot trace to the local chain has no
// fork anchor: the walk ends at a parent that is neither recorded nor stored,
// and the header is forecast from the ledger tip. The header's slot is within
// that forecast, so the wait returns at once rather than holding the header
// against an anchor it could not establish.
func TestAwaitObserverHeaderHorizonReleasesUntracedAncestry(t *testing.T) {
	t.Parallel()

	ls, _, _, _, _ := newObserverForkTestLedger(t)
	unknownParent := lcommon.Blake2b256Hash([]byte("never delivered"))
	recorded := mockHeader{
		hash:        lcommon.Blake2b256Hash([]byte("observer recorded")),
		prevHash:    unknownParent,
		blockNumber: 3,
		slot:        310_000,
	}
	ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		if !bytes.Equal(hash, recorded.Hash().Bytes()) {
			return ChainsyncEvent{}, nil, false
		}
		return ChainsyncEvent{
			BlockHeader: recorded,
			Point: ocommon.NewPoint(
				recorded.slot, recorded.Hash().Bytes(),
			),
		}, recorded.PrevHash().Bytes(), true
	}
	for _, tc := range []struct {
		name     string
		prevHash lcommon.Blake2b256
	}{
		{name: "parent unknown", prevHash: unknownParent},
		{name: "recorded ancestor built on unknown parent", prevHash: recorded.Hash()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			header := mockHeader{
				hash:        lcommon.Blake2b256Hash([]byte("observer " + tc.name)),
				prevHash:    tc.prevHash,
				blockNumber: 4,
				slot:        320_000,
			}
			e := ChainsyncEvent{
				BlockHeader: header,
				Point:       ocommon.NewPoint(header.slot, header.Hash().Bytes()),
			}
			_, forked, _ := ls.chainsyncForkAnchor(t.Context(), e)
			require.False(t, forked,
				"an ancestry that reaches no local block resolves no fork anchor")
			results := make(chan error, 1)
			go func() { results <- ls.AwaitObserverHeaderHorizon(t.Context(), e) }()
			require.NoError(t, testutil.RequireReceive(
				t, results, testutil.AsyncWait,
				"a header forecastable from the ledger tip with no fork anchor must not wait",
			))
		})
	}
}

// A stored parent resolves the anchor to that parent's own slot, so a header
// one block above it is inside the anchor's forecast even when the parent lies
// below the ledger tip.
func TestAwaitObserverHeaderHorizonStoredParentAnchorsAtParent(t *testing.T) {
	t.Parallel()

	ls, _, blocks, _, _ := newObserverForkTestLedger(t)
	header := mockHeader{
		hash:        lcommon.Blake2b256Hash([]byte("observer near fork")),
		prevHash:    blocks[0].Hash(),
		blockNumber: 3,
		slot:        320_000,
	}
	e := ChainsyncEvent{
		BlockHeader: header,
		Point:       ocommon.NewPoint(header.slot, header.Hash().Bytes()),
	}
	anchor, forked, _ := ls.chainsyncForkAnchor(t.Context(), e)
	require.True(t, forked, "a stored parent below the ledger tip is a fork anchor")
	require.Equal(t, blocks[0].SlotNumber(), anchor,
		"the anchor is the stored parent's own slot")
	results := make(chan error, 1)
	go func() { results <- ls.AwaitObserverHeaderHorizon(t.Context(), e) }()
	require.NoError(t, testutil.RequireReceive(
		t, results, testutil.AsyncWait,
		"a header inside its parent's forecast must not wait",
	))
}

// raceBlobStore runs a hook once, right after the next read of indexKey
// returns, so a test can mutate the chain inside the unlocked window of a
// persistent HoldsPoint read.
type raceBlobStore struct {
	blob.BlobStore
	indexKey []byte
	after    *atomic.Pointer[func()]
}

func (s raceBlobStore) Get(txn dbtypes.Txn, key []byte) ([]byte, error) {
	val, err := s.BlobStore.Get(txn, key)
	if bytes.Equal(key, s.indexKey) {
		if hook := s.after.Swap(nil); hook != nil {
			(*hook)()
		}
	}
	return val, err
}

// A chain mutation that overlaps the read deciding whether the local chain
// holds the header's parent is not a verdict that the parent is absent. Taking
// it as one walks the peer's ancestry back to an older block and anchors the
// forecast there, which parks a header the true anchor can forecast.
func TestAwaitObserverHeaderHorizonRacedHoldsPointIsNotAbsence(t *testing.T) {
	t.Parallel()

	ls, primary, blocks, _, _ := newObserverForkTestLedgerAt(
		t, 100_000, 210_000, 312_400,
	)
	extra, err := fixtures.GenerateConwayChain(
		3, blocks[1].Hash(), 311_000, 1_000, 1,
	)
	require.NoError(t, err)
	var raced atomic.Bool
	hook := func() {
		require.NoError(t, primary.AddBlock(context.Background(), extra[0], nil))
		raced.Store(true)
	}
	store := raceBlobStore{
		BlobStore: ls.db.Blob(),
		// Block IDs are dense from 1, so blocks[1] is entry 2.
		indexKey: dbtypes.BlockBlobIndexKey(2),
		after:    &atomic.Pointer[func()]{},
	}
	store.after.Store(&hook)
	ls.db.SetBlobStore(store)
	reg := prometheus.NewRegistry()
	ls.metrics.init(reg)

	// The peer's chain runs blocks[0] -> blocks[1] -> header. Anchored at
	// blocks[1] (slot 310_000) the forecast covers slot 500_000; anchored at
	// blocks[0] (slot 100_000) it ends at 432_000.
	peerParent := lcommon.Blake2b256Hash([]byte("peer ancestor"))
	ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		if !bytes.Equal(hash, peerParent.Bytes()) {
			return ChainsyncEvent{}, nil, false
		}
		return ChainsyncEvent{
			Point: ocommon.NewPoint(
				blocks[1].SlotNumber(), blocks[1].Hash().Bytes(),
			),
		}, blocks[0].Hash().Bytes(), true
	}
	header := mockHeader{
		hash:        lcommon.Blake2b256Hash([]byte("observer racing anchor")),
		prevHash:    peerParent,
		blockNumber: 4,
		slot:        500_000,
	}
	e := ChainsyncEvent{
		BlockHeader: header,
		Point:       ocommon.NewPoint(header.slot, header.Hash().Bytes()),
	}
	results := make(chan error, 1)
	go func() { results <- ls.AwaitObserverHeaderHorizon(t.Context(), e) }()
	require.NoError(t, testutil.RequireReceive(
		t, results, testutil.AsyncWait,
		"a header inside its parent's forecast must not wait on an anchor "+
			"resolved from a raced membership read",
	))
	require.True(t, raced.Load(), "the chain mutation never overlapped the read")
	require.Equal(t, float64(1), syncMetricValue(
		t, reg, "dingo_ledger_fork_anchor_membership_retries_total",
	), "the overlapped read is repeated once")
}

// When chain mutations overlap every attempt to read the fork anchor's
// membership, the header's forecast is unknown. The observer wait releases
// such a header, but admission and the header queue gate must keep it out
// rather than take the unknown anchor for a forecastable one.
func TestHeaderBeyondForecastHorizonUnsettledAnchorIsNotForecastable(
	t *testing.T,
) {
	t.Parallel()

	ls, primary, blocks, _, _ := newObserverForkTestLedgerAt(
		t, 100_000, 210_000, 312_400,
	)
	extra, err := fixtures.GenerateConwayChain(
		3, blocks[1].Hash(), 311_000, 1_000, holdsPointSettledAttempts,
	)
	require.NoError(t, err)
	var added atomic.Int32
	after := &atomic.Pointer[func()]{}
	var hook func()
	hook = func() {
		n := added.Add(1)
		require.NoError(t, primary.AddBlock(
			context.Background(), extra[n-1], nil,
		))
		if int(n) < len(extra) {
			after.Store(&hook)
		}
	}
	after.Store(&hook)
	ls.db.SetBlobStore(raceBlobStore{
		BlobStore: ls.db.Blob(),
		indexKey:  dbtypes.BlockBlobIndexKey(2),
		after:     after,
	})
	peerParent := lcommon.Blake2b256Hash([]byte("peer ancestor"))
	ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		if !bytes.Equal(hash, peerParent.Bytes()) {
			return ChainsyncEvent{}, nil, false
		}
		return ChainsyncEvent{
			Point: ocommon.NewPoint(
				blocks[1].SlotNumber(), blocks[1].Hash().Bytes(),
			),
		}, blocks[0].Hash().Bytes(), true
	}
	header := mockHeader{
		hash:        lcommon.Blake2b256Hash([]byte("ingress racing anchor")),
		prevHash:    peerParent,
		blockNumber: 4,
		slot:        320_000,
	}
	e := ChainsyncEvent{
		BlockHeader: header,
		Point:       ocommon.NewPoint(header.slot, header.Hash().Bytes()),
	}
	require.True(t, ls.headerBeyondForecastHorizon(t.Context(), e),
		"a header whose fork anchor is unknown must not be admitted")
	require.Equal(t, int32(holdsPointSettledAttempts), added.Load(),
		"every membership read must have overlapped a chain mutation")
	require.False(t, ls.headerBeyondForecastHorizon(t.Context(), e),
		"a settled read anchors the header at its forecastable parent")
}
