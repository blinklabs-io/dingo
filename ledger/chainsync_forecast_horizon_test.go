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
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
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
	covered := &atomic.Bool{}
	ls.slotClock.provider = gatedHorizonSlotTimeProvider{
		SlotTimeProvider: ls.slotClock.provider,
		slot:             90,
		covered:          covered,
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

func TestAwaitChainsyncHeaderAdmissionPastForecastHorizonHonorsCancellation(
	t *testing.T,
) {
	t.Parallel()

	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)
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
		t.Fatal("a channel obtained after publication must wait for the next one")
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
			testChain := cm.PrimaryChain()

			header := mockHeader{slot: 1000, blockNumber: 100}
			point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())
			covered := &atomic.Bool{}
			covered.Store(!tc.pastHorizon)
			systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
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

			require.NoError(t, ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
				ConnectionId: connId,
				BlockHeader:  header,
				Point:        point,
				Tip: ochainsync.Tip{
					Point:       ocommon.NewPoint(point.Slot+1, []byte("tip")),
					BlockNumber: header.BlockNumber() + 1,
				},
			}))

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
				"h3": {300, "h2"},
				"h2": {200, "b1"},
			},
			held:   map[string]bool{"b1": true},
			blocks: map[string]uint64{"b1": 100},
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
