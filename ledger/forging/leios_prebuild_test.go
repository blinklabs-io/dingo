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
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func testLeiosPrebuildRequest(slot uint64, parent byte) leiosPrebuildRequest {
	return leiosPrebuildRequest{
		slot:              slot,
		parent:            []byte{parent},
		selectionDeadline: time.Now().Add(time.Minute),
		deadline:          time.Now().Add(2 * time.Minute),
	}
}

func testLeiosPrebuildResult(
	request leiosPrebuildRequest,
) *preparedLeiosEndorserBlock {
	return &preparedLeiosEndorserBlock{
		slot:    request.slot,
		parent:  request.parent,
		caps:    request.caps,
		current: func(context.Context, []byte, uint64) bool { return true },
	}
}

func TestLeiosPrebuildReplacesByCancellingAndJoining(t *testing.T) {
	t.Parallel()
	firstStarted := make(chan struct{})
	firstFinished := make(chan struct{})
	secondReady := make(chan struct{}, 1)
	var calls atomic.Int64
	worker := newLeiosPrebuildWorker(
		t.Context(),
		func(ctx context.Context, request leiosPrebuildRequest) (*preparedLeiosEndorserBlock, error) {
			if calls.Add(1) == 1 {
				close(firstStarted)
				<-ctx.Done()
				close(firstFinished)
				return nil, ctx.Err()
			}
			select {
			case <-firstFinished:
			default:
				t.Error("replacement began before the old request exited")
			}
			return testLeiosPrebuildResult(request), nil
		},
		func(result string, _ time.Duration, _ error) {
			if result == "ready" {
				secondReady <- struct{}{}
			}
		},
	)
	t.Cleanup(worker.stop)
	worker.request(testLeiosPrebuildRequest(10, 1))
	testutil.RequireReceive(t, firstStarted, 5*time.Second, "first request")
	worker.request(testLeiosPrebuildRequest(10, 2))
	testutil.RequireReceive(t, secondReady, 5*time.Second, "replacement")
	candidate, reason := worker.take(t.Context(), 10, []byte{2}, leiosEBCaps{})
	require.Equal(t, "hit", reason)
	require.NotNil(t, candidate)
	require.EqualValues(t, 2, calls.Load())
}

func TestLeiosPrebuildCacheRequiresCurrentParentAndCapacity(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"hit", "parent_changed", "capacity_changed", "stale"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			ready := make(chan struct{}, 1)
			var current atomic.Bool
			current.Store(true)
			request := testLeiosPrebuildRequest(10, 1)
			worker := newLeiosPrebuildWorker(
				t.Context(),
				func(_ context.Context, request leiosPrebuildRequest) (*preparedLeiosEndorserBlock, error) {
					result := testLeiosPrebuildResult(request)
					result.current = func(context.Context, []byte, uint64) bool { return current.Load() }
					return result, nil
				},
				func(result string, _ time.Duration, _ error) {
					if result == "ready" {
						ready <- struct{}{}
					}
				},
			)
			t.Cleanup(worker.stop)
			worker.request(request)
			testutil.RequireReceive(
				t,
				ready,
				5*time.Second,
				"prepared candidate",
			)
			candidate, reason := worker.take(
				t.Context(),
				9,
				request.parent,
				request.caps,
			)
			require.Nil(t, candidate)
			require.Equal(t, "miss", reason)
			parent, caps := request.parent, request.caps
			switch scenario {
			case "parent_changed":
				parent = []byte{2}
			case "capacity_changed":
				caps.maxRefs = 1
			case "stale":
				current.Store(false)
			}
			candidate, reason = worker.take(t.Context(), 10, parent, caps)
			require.Equal(t, scenario, reason)
			require.Equal(t, scenario == "hit", candidate != nil)
			candidate, reason = worker.take(t.Context(), 10, parent, caps)
			require.Nil(t, candidate)
			require.Equal(t, "miss", reason)
		})
	}
}

func TestLeiosPrebuildExpiresInFlightWorkAtLeaderSlot(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	finished := make(chan struct{})
	worker := newLeiosPrebuildWorker(
		t.Context(),
		func(ctx context.Context, request leiosPrebuildRequest) (*preparedLeiosEndorserBlock, error) {
			close(started)
			<-ctx.Done()
			close(finished)
			return testLeiosPrebuildResult(request), nil
		},
		nil,
	)
	t.Cleanup(worker.stop)
	worker.request(testLeiosPrebuildRequest(10, 1))
	testutil.RequireReceive(t, started, 5*time.Second, "prebuild")
	worker.expire(10)
	testutil.RequireReceive(t, finished, 5*time.Second, "cancelled prebuild")
	worker.stop()
	candidate, reason := worker.take(t.Context(), 10, []byte{1}, leiosEBCaps{})
	require.Nil(t, candidate)
	require.Equal(t, "miss", reason)
}

func TestLeiosPrebuildStopJoinsWorkerCleanup(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	cleanup := make(chan struct{})
	release := make(chan struct{})
	stopped := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	worker := newLeiosPrebuildWorker(
		ctx,
		func(ctx context.Context, _ leiosPrebuildRequest) (*preparedLeiosEndorserBlock, error) {
			close(started)
			<-ctx.Done()
			close(cleanup)
			<-release
			return nil, ctx.Err()
		},
		nil,
	)
	forger := &BlockForger{
		mode:          ModeDev,
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		slotDuration:  time.Hour,
		leiosPrebuild: worker,
		running:       true,
		cancel:        cancel,
	}
	t.Cleanup(func() { unblock(); forger.Stop(); worker.stop() })
	forger.wg.Add(1)
	go forger.runLoop(ctx)
	worker.request(testLeiosPrebuildRequest(10, 1))
	testutil.RequireReceive(t, started, 5*time.Second, "prebuild")
	go func() { forger.Stop(); close(stopped) }()
	testutil.RequireReceive(t, cleanup, 5*time.Second, "worker cleanup")
	testutil.RequireNoReceive(
		t,
		stopped,
		20*time.Millisecond,
		"Stop must join outstanding cleanup",
	)
	unblock()
	testutil.RequireReceive(t, stopped, 5*time.Second, "joined worker")
	require.False(t, forger.IsRunning())
}

func TestLeiosPrebuildRecoversCallbackPanic(t *testing.T) {
	t.Parallel()
	failed := make(chan struct{}, 1)
	ready := make(chan struct{}, 1)
	var calls atomic.Int64
	worker := newLeiosPrebuildWorker(
		t.Context(),
		func(_ context.Context, request leiosPrebuildRequest) (*preparedLeiosEndorserBlock, error) {
			if calls.Add(1) == 1 {
				panic("preparation failed")
			}
			return testLeiosPrebuildResult(request), nil
		},
		func(result string, _ time.Duration, _ error) {
			if result == "failed" {
				failed <- struct{}{}
			}
			if result == "ready" {
				ready <- struct{}{}
			}
		},
	)
	t.Cleanup(worker.stop)
	worker.request(testLeiosPrebuildRequest(10, 1))
	testutil.RequireReceive(t, failed, 5*time.Second, "recovered callback")
	worker.request(testLeiosPrebuildRequest(11, 1))
	testutil.RequireReceive(t, ready, 5*time.Second, "subsequent request")
	candidate, reason := worker.take(t.Context(), 11, []byte{1}, leiosEBCaps{})
	require.NotNil(t, candidate)
	require.Equal(t, "hit", reason)
}

func TestLeiosUnsignedPreparationDoesNotPublishOrFence(t *testing.T) {
	t.Parallel()
	raw := makeMinimalTxCbor(t, 0x31, 0)
	tx, err := conway.NewConwayTransactionFromCbor(raw)
	require.NoError(t, err)
	caster := &forgerTestLeiosCaster{}
	fence := &fenceTestStore{}
	forger := &BlockForger{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:    time.Now,
		leiosMempool: forgerTestMempoolProvider{
			txs: []MempoolTransaction{
				{
					Hash: tx.Hash().String(),
					Cbor: raw,
					Type: conway.TxTypeConway,
				},
			},
		},
		leiosEBCaster: caster,
		fenceStore:    fence,
	}
	prepared, err := forger.prepareLeiosEndorserBlock(
		t.Context(),
		10,
		leiosSelectionBasis{},
		leiosEBCaps{},
		time.Now().Add(time.Hour),
	)
	require.NoError(t, err)
	require.NotNil(t, prepared)
	require.Empty(t, caster.cbor)
	require.Empty(t, fence.stored)
	require.Zero(t, fence.loads)
	require.Nil(t, forger.creds)
	announcement, err := forger.broadcastPreparedLeiosEndorserBlock(
		t.Context(),
		prepared,
	)
	require.NoError(t, err)
	require.Equal(t, lcommon.NewBlake2b256(prepared.hash), announcement.Hash)
	require.NotEmpty(t, caster.cbor)
	require.Empty(t, fence.stored)
}

func TestLeiosPrebuildScheduleUsesUpcomingLeaderWithoutPublishing(
	t *testing.T,
) {
	t.Parallel()
	requests := make(chan leiosPrebuildRequest, 1)
	worker := newLeiosPrebuildWorker(
		t.Context(),
		func(_ context.Context, request leiosPrebuildRequest) (*preparedLeiosEndorserBlock, error) {
			requests <- request
			return nil, nil
		},
		nil,
	)
	t.Cleanup(worker.stop)
	caster := &forgerTestLeiosCaster{}
	fence := &fenceTestStore{}
	checker := &forgerTestLeiosChecker{
		allowed:         false,
		prepareCallback: func(uint64) (bool, string, error) { return true, "", nil },
	}
	clock := &retryTestSlotClock{
		currentSlot:  10,
		chainTipSlot: 9,
		chainTipHash: []byte{1},
		slotEnd:      time.Now().Add(time.Second),
	}
	forger := &BlockForger{
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		slotDuration:  time.Second,
		slotClock:     clock,
		leaderChecker: forgerTestLeader{},
		leiosChecker:  checker,
		leiosPrebuild: worker,
		leiosEBCaster: caster,
		fenceStore:    fence,
		eraParams: &mockPParamsProvider{
			pparams: scaleTestPParams(),
		},
		metrics: initForgingMetrics(prometheus.NewRegistry()),
	}
	forger.scheduleLeiosPrebuild(10)
	request := testutil.RequireReceive(
		t,
		requests,
		5*time.Second,
		"scheduled future leader",
	)
	require.EqualValues(t, 11, request.slot)
	require.Equal(t, clock.chainTipHash, request.parent)
	require.True(t, request.selectionDeadline.Before(clock.slotEnd))
	require.True(t, request.deadline.Before(clock.slotEnd))
	require.Empty(t, caster.cbor)
	require.Empty(t, fence.stored)
}

func TestLeiosPrebuildScheduleUsesNetworkSlotTimes(t *testing.T) {
	t.Parallel()
	requests := make(chan leiosPrebuildRequest, 1)
	worker := newLeiosPrebuildWorker(
		t.Context(),
		func(_ context.Context, request leiosPrebuildRequest) (*preparedLeiosEndorserBlock, error) {
			requests <- request
			return nil, nil
		},
		nil,
	)
	t.Cleanup(worker.stop)
	start := time.Now().Add(200 * time.Millisecond)
	clock := &retryTestSlotClock{
		currentSlot:  10,
		chainTipSlot: 9,
		chainTipHash: []byte{1},
		slotEnd:      start,
		slotLength:   200 * time.Millisecond,
	}
	forger := &BlockForger{
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		slotDuration:  time.Second,
		slotClock:     clock,
		leaderChecker: forgerTestLeader{},
		leiosChecker:  &forgerTestLeiosChecker{allowed: true},
		leiosPrebuild: worker,
		eraParams: &mockPParamsProvider{
			pparams: scaleTestPParams(),
		},
	}
	forger.scheduleLeiosPrebuild(10)
	request := testutil.RequireReceive(
		t,
		requests,
		5*time.Second,
		"network slot schedule",
	)
	require.Equal(t, start.Add(-50*time.Millisecond), request.selectionDeadline)
	require.Equal(t, start.Add(-20*time.Millisecond), request.deadline)
}

func TestLeiosPrebuildRestartWaitsForPreviousWorker(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	nextCtx, cancelNext := context.WithCancel(t.Context())
	started := make(chan struct{})
	cleanup := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	worker := newLeiosPrebuildWorker(
		ctx,
		func(ctx context.Context, _ leiosPrebuildRequest) (*preparedLeiosEndorserBlock, error) {
			close(started)
			<-ctx.Done()
			close(cleanup)
			<-release
			return nil, ctx.Err()
		},
		nil,
	)
	forger := &BlockForger{
		mode:          ModeDev,
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		slotDuration:  time.Hour,
		leiosPrebuild: worker,
		running:       true,
		cancel:        cancel,
	}
	t.Cleanup(func() { unblock(); cancelNext(); forger.Stop(); worker.stop() })
	forger.wg.Add(1)
	go forger.runLoop(ctx)
	worker.request(testLeiosPrebuildRequest(10, 1))
	testutil.RequireReceive(t, started, 5*time.Second, "prebuild")
	stopped := make(chan struct{})
	go func() { forger.Stop(); close(stopped) }()
	testutil.RequireReceive(t, cleanup, 5*time.Second, "worker cleanup")
	startInvoked := make(chan struct{})
	restart := make(chan error, 1)
	go func() { close(startInvoked); restart <- forger.Start(nextCtx) }()
	testutil.RequireReceive(t, startInvoked, 5*time.Second, "restart")
	testutil.RequireNoReceive(
		t,
		restart,
		20*time.Millisecond,
		"restart must wait for prior worker cleanup",
	)
	unblock()
	testutil.RequireReceive(t, stopped, 5*time.Second, "previous lifecycle")
	require.NoError(
		t,
		testutil.RequireReceive(t, restart, 5*time.Second, "new lifecycle"),
	)
	forger.Stop()
}

func prebuildCacheForgerFixture(
	t *testing.T,
) (*BlockForger, *preparedLeiosEndorserBlock, *forgerTestLeiosCaster) {
	t.Helper()
	raw := makeMinimalTxCbor(t, 0x41, 0)
	tx, err := conway.NewConwayTransactionFromCbor(raw)
	require.NoError(t, err)
	caster := &forgerTestLeiosCaster{}
	forger := &BlockForger{
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:          time.Now,
		leiosChecker: &forgerTestLeiosChecker{allowed: true},
		leiosMempool: forgerTestMempoolProvider{
			txs: []MempoolTransaction{
				{
					Hash: tx.Hash().String(),
					Cbor: raw,
					Type: conway.TxTypeConway,
				},
			},
		},
		leiosEBCaster:           caster,
		forgeEBSelectionReserve: time.Hour,
		eraParams: &mockPParamsProvider{
			pparams: &conway.ConwayProtocolParameters{},
		},
		metrics: initForgingMetrics(prometheus.NewRegistry()),
	}
	caps, ok := forger.leiosEBCapsForSlot(10)
	require.True(t, ok)
	candidate, err := forger.prepareLeiosEndorserBlock(
		t.Context(),
		10,
		leiosSelectionBasis{parentHash: []byte{1}},
		caps,
		time.Now().Add(time.Hour),
	)
	require.NoError(t, err)
	candidate.current = func(context.Context, []byte, uint64) bool { return true }
	candidate.prebuilt = true
	worker := newLeiosPrebuildWorker(t.Context(), nil, nil)
	t.Cleanup(worker.stop)
	worker.mu.Lock()
	worker.result = candidate
	worker.mu.Unlock()
	forger.leiosPrebuild = worker
	return forger, candidate, caster
}

func TestLeiosPrebuildHitSkipsSelectionButRechecksCommittee(t *testing.T) {
	t.Parallel()
	for _, eligible := range []bool{false, true} {
		t.Run(fmt.Sprint(eligible), func(t *testing.T) {
			t.Parallel()
			forger, candidate, caster := prebuildCacheForgerFixture(t)
			forger.leiosMempool = nil
			forger.leiosChecker.(*forgerTestLeiosChecker).allowed = eligible
			var receipt LeiosValidationReceipt
			announcement, err := forger.checkAndForgeLeiosEB(
				t.Context(),
				10,
				leiosSelectionBasis{
					parentHash: []byte{1},
					slot:       10,
					receipt:    &receipt,
				},
			)
			require.NoError(t, err)
			if !eligible {
				require.Nil(t, announcement)
				require.Empty(t, caster.cbor)
				return
			}
			require.NotNil(t, announcement)
			require.Equal(
				t,
				lcommon.NewBlake2b256(candidate.hash),
				announcement.Hash,
			)
			require.NotEmpty(t, caster.cbor)
			require.EqualValues(
				t,
				1,
				promtest.ToFloat64(
					forger.metrics.leiosPrebuildTotal.WithLabelValues("hit"),
				),
			)
		})
	}
}

func TestLeiosPrebuildCertifiedClosureUsesFreshSelection(t *testing.T) {
	t.Parallel()
	forger, cached, caster := prebuildCacheForgerFixture(t)
	raw := makeMinimalTxCbor(t, 0x42, 0)
	tx, err := conway.NewConwayTransactionFromCbor(raw)
	require.NoError(t, err)
	forger.leiosMempool = forgerTestMempoolProvider{
		txs: []MempoolTransaction{
			{Hash: tx.Hash().String(), Cbor: raw, Type: conway.TxTypeConway},
		},
	}
	var receipt LeiosValidationReceipt
	announcement, err := forger.checkAndForgeLeiosEB(
		t.Context(),
		10,
		leiosSelectionBasis{
			parentHash: []byte{1},
			slot:       10,
			receipt:    &receipt,
			closure:    &leiosCertifiedClosure{},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, announcement)
	require.NotEqual(t, lcommon.NewBlake2b256(cached.hash), announcement.Hash)
	require.Equal(t, [][]byte{blockTxFromMempool(t, raw)}, caster.txBodies)
	require.EqualValues(
		t,
		0,
		promtest.ToFloat64(
			forger.metrics.leiosPrebuildTotal.WithLabelValues("hit"),
		),
	)
}

func TestPreparedLeiosManifestMatchesWireConstruction(t *testing.T) {
	t.Parallel()
	txs := make([]MempoolTransaction, 0, 3)
	bodies := make([][]byte, 0, 3)
	for i := range byte(3) {
		raw := makeMinimalTxCbor(t, 0x61+i, 0)
		tx, err := conway.NewConwayTransactionFromCbor(raw)
		require.NoError(t, err)
		txs = append(
			txs,
			MempoolTransaction{
				Hash: tx.Hash().String(),
				Cbor: raw,
				Type: conway.TxTypeConway,
			},
		)
		bodies = append(bodies, blockTxFromMempool(t, raw))
	}
	raw, hash, expected, err := buildLeiosEB(txs, leiosEBCaps{})
	require.NoError(t, err)
	actual, actualHash, actualBodies, err := buildPreparedLeiosEB(
		bodies,
		leiosEBCaps{},
	)
	require.NoError(t, err)
	require.Equal(t, raw, actual)
	require.Equal(t, hash, actualHash)
	require.Equal(t, expected, actualBodies)
	_, _, _, err = buildPreparedLeiosEB(bodies, leiosEBCaps{maxRefs: 2})
	require.ErrorContains(t, err, "capacity")
}
