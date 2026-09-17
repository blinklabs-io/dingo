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
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

func testRecycleConnId() ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}
}

// TestChainsyncHeaderVerificationFailurePublishesRecycleEvent verifies that
// a header crypto verification failure on the chainsync path publishes a
// ledger.ConnectionRecycleRequestedEvent with reason "header_verification_failure".
func TestChainsyncHeaderVerificationFailurePublishesRecycleEvent(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	ls := &LedgerState{
		validationEnabled: true,
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 2_000,
				Nonce:         make([]byte, 32),
			},
		},
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
		},
	}
	ls.publishSnapshotsLocked()

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId,
		BlockHeader:  mockHeader{slot: 1000, blockNumber: 100},
		Point:        ocommon.Point{Slot: 1000},
	})
	require.Error(t, err)

	got := testutil.RequireReceive(
		t,
		recycled,
		testutil.AsyncWait,
		"recycle event not published",
	)
	assert.Equal(t, connId, got.ConnectionId)
	assert.Equal(t, "header_verification_failure", got.Reason)
}

func TestChainsyncHeaderVerificationMissingEpochDefersToBlockfetch(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	cm, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	testChain := cm.PrimaryChain()
	ls := &LedgerState{
		validationEnabled: true,
		chain:             testChain,
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
			BlockfetchRequestRangeFunc: func(
				ouroboros.ConnectionId,
				ocommon.Point,
				ocommon.Point,
			) error {
				return nil
			},
		},
	}
	ls.publishSnapshotsLocked()
	t.Cleanup(func() {
		if ls.chainsyncBlockfetchTimeoutTimer != nil {
			ls.chainsyncBlockfetchTimeoutTimer.Stop()
		}
	})
	header := mockHeader{slot: 1000, blockNumber: 100}
	point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())

	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId,
		BlockHeader:  header,
		Point:        point,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				point.Slot+1,
				[]byte("unbound-tip"),
			),
			BlockNumber: header.BlockNumber() + 1,
		},
	})
	require.NoError(t, err)

	testutil.RequireNoReceive(
		t,
		recycled,
		100*time.Millisecond,
		"missing epoch should defer header verification, not recycle peer",
	)
	assert.True(t, testChain.FirstHeaderMatchesPoint(point))
	assert.False(t, testChain.FirstVerifiedHeaderMatchesPoint(point))
	assert.Zero(t, ls.syncUpstreamTipSlot.Load())
}

func TestChainsyncHeaderVerificationEmptyEpochNonceDefersToBlockfetch(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	requested := make(chan ocommon.Point, 1)
	cm, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	testChain := cm.PrimaryChain()
	ls := &LedgerState{
		validationEnabled: true,
		chain:             testChain,
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 2_000,
			},
		},
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
			BlockfetchRequestRangeFunc: func(
				_ ouroboros.ConnectionId,
				start ocommon.Point,
				_ ocommon.Point,
			) error {
				requested <- start
				return nil
			},
		},
	}
	ls.publishSnapshotsLocked()
	t.Cleanup(func() {
		if ls.chainsyncBlockfetchTimeoutTimer != nil {
			ls.chainsyncBlockfetchTimeoutTimer.Stop()
		}
	})
	header := mockHeader{slot: 1000, blockNumber: 100}
	point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())

	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId,
		BlockHeader:  header,
		Point:        point,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				point.Slot+1,
				[]byte("unbound-tip"),
			),
			BlockNumber: header.BlockNumber() + 1,
		},
	})
	require.NoError(t, err)

	gotStart := testutil.RequireReceive(
		t,
		requested,
		testutil.AsyncWait,
		"empty nonce should start blockfetch for deferred verification",
	)
	assert.Equal(t, point, gotStart)
	testutil.RequireNoReceive(
		t,
		recycled,
		100*time.Millisecond,
		"empty nonce should defer header verification, not recycle peer",
	)
	assert.True(t, testChain.FirstHeaderMatchesPoint(point))
	assert.False(t, testChain.FirstVerifiedHeaderMatchesPoint(point))
	assert.Zero(t, ls.syncUpstreamTipSlot.Load())
}

func TestChainsyncHeaderVerificationMithrilCoverageAdvancesFrontier(
	t *testing.T,
) {
	t.Parallel()

	header := mockHeader{slot: 1000, blockNumber: 100}
	point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())
	testChain := &chain.Chain{}
	ls := &LedgerState{
		validationEnabled:            true,
		mithrilLedgerSlot:            point.Slot,
		chain:                        testChain,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	require.NoError(t, ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: testRecycleConnId(),
		BlockHeader:  header,
		Point:        point,
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(point.Slot+1, []byte("peer-tip")),
			BlockNumber: header.BlockNumber() + 1,
		},
	}))

	assert.True(t, testChain.FirstHeaderMatchesPoint(point))
	assert.False(t, testChain.FirstVerifiedHeaderMatchesPoint(point))
	assert.Equal(t, point.Slot, ls.syncUpstreamTipSlot.Load())
}

// TestBlockfetchHeaderVerificationFailurePublishesRecycleEvent verifies that
// a block header crypto verification failure on the blockfetch path publishes a
// ledger.ConnectionRecycleRequestedEvent with reason "block_header_verification_failure".
func TestBlockfetchHeaderVerificationFailurePublishesRecycleEvent(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	ls := &LedgerState{
		validationEnabled:            true,
		activeBlockfetchConnId:       connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		chain:                        &chain.Chain{},
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
		},
	}
	ls.publishSnapshotsLocked()

	ls.handleEventBlockfetch(event.NewEvent(
		BlockfetchEventType,
		BlockfetchEvent{
			ConnectionId: connId,
			Block:        &mockBabbageBlock{slot: 500},
			Point:        ocommon.Point{Slot: 500, Hash: []byte("fake-hash")},
		},
	))

	got := testutil.RequireReceive(
		t,
		recycled,
		testutil.AsyncWait,
		"recycle event not published",
	)
	assert.Equal(t, connId, got.ConnectionId)
	assert.Equal(t, "block_header_verification_failure", got.Reason)
}

// TestBlockfetchHeaderVerificationRunsRegardlessOfValidationEnabled is a
// regression test for a human-review finding: no test failed if
// handleEventBlockfetchBlockDeferred's Mithril-slot gate were reverted to
// the previous validationEnabled check. Issue #3528 made header crypto
// verification unconditional -- before it, an entire
// ValidateHistorical=false bulk-sync run skipped VRF/KES/opcert
// verification and stake-derived leader eligibility for every block. This
// proves the fail-closed behavior directly: with validationEnabled=false
// on a non-Mithril slot, a block with unverifiable header crypto still
// returns a definite (non-deferred) error instead of being silently
// accepted.
func TestBlockfetchHeaderVerificationRunsRegardlessOfValidationEnabled(
	t *testing.T,
) {
	t.Parallel()

	connId := testRecycleConnId()
	ls := &LedgerState{
		validationEnabled:            false,
		activeBlockfetchConnId:       connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		chain:                        &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	err := ls.handleEventBlockfetchBlockDeferred(BlockfetchEvent{
		ConnectionId: connId,
		Block:        &mockBabbageBlock{slot: 500},
		Point:        ocommon.Point{Slot: 500, Hash: []byte("fake-hash")},
	}, nil)

	require.Error(t, err)
	assert.False(t, IsHeaderVerificationDeferred(err))
	assert.Contains(t, err.Error(), "block header crypto verification failed")
}

// TestBlockfetchHeaderVerificationSkippedForMithrilCoveredSlot is the
// companion regression test to the one above: verification must still be
// skipped for a slot an imported Mithril snapshot already covers,
// regardless of validationEnabled -- that is the one exemption
// slotCoveredByMithril preserves. A block with unverifiable header crypto
// at a Mithril-covered slot must be accepted without error.
func TestBlockfetchHeaderVerificationSkippedForMithrilCoveredSlot(
	t *testing.T,
) {
	t.Parallel()

	const targetSlot = uint64(500)
	connId := testRecycleConnId()
	ls := &LedgerState{
		validationEnabled:            false,
		mithrilLedgerSlot:            targetSlot,
		activeBlockfetchConnId:       connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		chain:                        &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	err := ls.handleEventBlockfetchBlockDeferred(BlockfetchEvent{
		ConnectionId: connId,
		Block:        &mockBabbageBlock{slot: targetSlot},
		Point: ocommon.Point{
			Slot: targetSlot,
			Hash: []byte("fake-hash"),
		},
	}, nil)

	require.NoError(t, err)
	require.Len(t, ls.pendingBlockfetchEvents, 1)
}

func TestBlockfetchStatefulHeaderVerificationDefersUntilLedgerApply(
	t *testing.T,
) {
	t.Parallel()

	connId := testRecycleConnId()
	tb := createTestBlock(t, [32]byte{47}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.validationEnabled = true
	ls.activeBlockfetchConnId = connId
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.chain = &chain.Chain{}

	point := ocommon.NewPoint(tb.block.SlotNumber(), tb.block.Hash().Bytes())
	err := ls.handleEventBlockfetchBlockDeferred(BlockfetchEvent{
		ConnectionId: connId,
		Block:        tb.block,
		Point:        point,
	}, nil)
	require.NoError(t, err)
	require.Len(t, ls.pendingBlockfetchEvents, 1)
	assert.True(t, ls.consumeDeferredHeaderValidation(point))
	value, err := ls.db.GetSyncState(
		deferredHeaderValidationSyncStateKey(point),
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, deferredHeaderValidationSyncStateValue, value)
}

// TestBlockfetchHeaderVerificationEmptyEpochNonceDefersNotFails is a
// regression test for a human-review finding: handleEventBlockfetchBlockDeferred
// checked errors.Is(verifyErr, errHeaderVerificationDeferred) directly
// instead of the exported IsHeaderVerificationDeferred, so a covered epoch
// with no published nonce yet (errEpochNonceUnavailable, which
// IsHeaderVerificationDeferred was broadened to recognize) was still
// treated as a hard crypto failure at this call site. Unlike the chainsync
// admission gate (chainsyncHeaderCryptoPolicy), which skips calling verify
// entirely when the nonce isn't cached, this path only flushes pending
// blocks once and rechecks whether the header was verified elsewhere before
// falling through to verify regardless -- so it reaches this exact case in
// practice, and an honest peer's block would otherwise have its connection
// recycled over a transient local gap.
func TestBlockfetchHeaderVerificationEmptyEpochNonceDefersNotFails(
	t *testing.T,
) {
	t.Parallel()

	const targetSlot = uint64(1000)
	connId := testRecycleConnId()
	// A nil epoch nonce (covered epoch, nonce not yet published) rather
	// than a real one from createTestBlock: headerVerificationEpoch checks
	// epoch/nonce availability before ever touching the VRF proof, so the
	// block's own crypto content doesn't matter for this case.
	ls, _ := newEligibilityTestLedger(t, nil)
	ls.validationEnabled = true
	ls.activeBlockfetchConnId = connId
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.chain = &chain.Chain{}

	block := &mockBabbageBlock{slot: targetSlot}
	point := ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
	err := ls.handleEventBlockfetchBlockDeferred(BlockfetchEvent{
		ConnectionId: connId,
		Block:        block,
		Point:        point,
	}, nil)
	require.NoError(
		t,
		err,
		"an unpublished epoch nonce must defer, not hard-fail, block header verification",
	)
	require.Len(t, ls.pendingBlockfetchEvents, 1)
	assert.True(t, ls.consumeDeferredHeaderValidation(point))
}

// --- non-extending-block flood demotion (issue #4272) ---

// TestEvaluateNonExtendingBlockRejection exercises the pure threshold/window
// decision directly against synthetic timestamps, matching
// chainsyncrecycler.shouldRecycleLocalTipPlateau's test style.
func TestEvaluateNonExtendingBlockRejection(t *testing.T) {
	t.Parallel()

	now := time.Now()

	// A handful of rejections, well under the threshold, must never trigger
	// a recycle -- this is the legitimate brief-rollback-race shape: a peer
	// serves a few blocks that no longer fit while its view of a fork
	// converges with ours.
	var state nonExtendingBlockRejectionState
	for i := range nonExtendingBlockRejectionThreshold - 1 {
		var shouldRecycle bool
		state, shouldRecycle = evaluateNonExtendingBlockRejection(
			state,
			now.Add(time.Duration(i)*time.Millisecond),
		)
		require.Falsef(
			t,
			shouldRecycle,
			"rejection %d must not cross the threshold yet", i+1,
		)
	}
	require.Equal(t, nonExtendingBlockRejectionThreshold-1, state.count)

	// The threshold-th rejection, still well inside the window, must
	// trigger the recycle and the state must reset.
	state, shouldRecycle := evaluateNonExtendingBlockRejection(
		state,
		now.Add(time.Duration(nonExtendingBlockRejectionThreshold)*time.Millisecond),
	)
	assert.True(t, shouldRecycle, "threshold-th rejection must recycle")
	assert.Equal(
		t,
		nonExtendingBlockRejectionState{},
		state,
		"state must reset after a recycle decision",
	)
}

// TestEvaluateNonExtendingBlockRejectionWindowExpiry verifies that a gap
// longer than nonExtendingBlockRejectionWindow restarts the count instead of
// accumulating with it. Without this, rejections spread thinly over a very
// long-lived, otherwise healthy connection could eventually cross the bound
// even though none of them were ever part of a flood.
func TestEvaluateNonExtendingBlockRejectionWindowExpiry(t *testing.T) {
	t.Parallel()

	now := time.Now()
	var state nonExtendingBlockRejectionState
	for range nonExtendingBlockRejectionThreshold - 1 {
		var shouldRecycle bool
		state, shouldRecycle = evaluateNonExtendingBlockRejection(state, now)
		require.False(t, shouldRecycle)
	}
	require.Equal(t, nonExtendingBlockRejectionThreshold-1, state.count)

	// A rejection after the window has elapsed restarts the count at 1
	// rather than reaching the threshold.
	later := now.Add(nonExtendingBlockRejectionWindow + time.Second)
	state, shouldRecycle := evaluateNonExtendingBlockRejection(state, later)
	assert.False(
		t,
		shouldRecycle,
		"a rejection after the window expired must not inherit the prior count",
	)
	assert.Equal(t, 1, state.count)
	assert.Equal(t, later, state.windowStart)
}

// TestNoteNonExtendingBlockRejectionPublishesRecycleAtThreshold verifies the
// LedgerState-level wiring: exactly nonExtendingBlockRejectionThreshold calls
// for one connection publish exactly one ConnectionRecycleRequestedEvent with
// reason "non_extending_block_flood", and no earlier call does.
func TestNoteNonExtendingBlockRejectionPublishesRecycleAtThreshold(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
		},
	}

	point := ocommon.Point{Slot: 1000, Hash: []byte("not-fit-hash")}
	for i := range nonExtendingBlockRejectionThreshold - 1 {
		ls.noteNonExtendingBlockRejection(connId, point, nil)
		testutil.RequireNoReceive(
			t,
			recycled,
			20*time.Millisecond,
			fmt.Sprintf("unexpected recycle event after rejection %d", i+1),
		)
	}

	ls.noteNonExtendingBlockRejection(connId, point, nil)
	got := testutil.RequireReceive(
		t,
		recycled,
		2*time.Second,
		"recycle event not published at threshold",
	)
	assert.Equal(t, connId, got.ConnectionId)
	assert.Equal(t, "non_extending_block_flood", got.Reason)
}

// TestNoteNonExtendingBlockRejectionResetsOnAcceptedBlock is the "should NOT
// demote" regression: a connection that occasionally fails to extend the
// chain (a brief rollback/reorg race) but then successfully DOES extend it
// must have its count forgiven. Without noteBlockAcceptedFromConn resetting
// the count, two sub-threshold bursts split by a success would still sum
// past the threshold and wrongly recycle a peer that just proved it was
// healthy.
func TestNoteNonExtendingBlockRejectionResetsOnAcceptedBlock(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
		},
	}
	point := ocommon.Point{Slot: 1000, Hash: []byte("not-fit-hash")}

	// A burst just under the threshold: a brief race, not a flood.
	burst := nonExtendingBlockRejectionThreshold - 1
	for range burst {
		ls.noteNonExtendingBlockRejection(connId, point, nil)
	}
	require.Equal(t, burst, ls.nonExtendingBlockRejections[connIdKey(connId)].count)

	// The same connection then delivers a block that DOES extend the chain.
	ls.noteBlockAcceptedFromConn(connId)
	_, tracked := ls.nonExtendingBlockRejections[connIdKey(connId)]
	require.False(t, tracked, "acceptance must clear the tracked count")

	// A second sub-threshold burst must not combine with the forgiven one:
	// without the reset, burst+burst would have crossed the threshold.
	for i := range burst {
		ls.noteNonExtendingBlockRejection(connId, point, nil)
		testutil.RequireNoReceive(
			t,
			recycled,
			20*time.Millisecond,
			fmt.Sprintf(
				"unexpected recycle event after post-reset rejection %d",
				i+1,
			),
		)
	}
}

// TestNoteNonExtendingBlockRejectionPerConnectionIndependent verifies that
// one connection's rejection count cannot push a different, well-behaved
// connection over the threshold.
func TestNoteNonExtendingBlockRejectionPerConnectionIndependent(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	floodConn := testRecycleConnId()
	healthyConn := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6002},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3003},
	}

	recycled := make(chan ConnectionRecycleRequestedEvent, 2)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
		},
	}
	point := ocommon.Point{Slot: 1000, Hash: []byte("not-fit-hash")}

	// healthyConn stays well under the threshold throughout.
	for range nonExtendingBlockRejectionThreshold - 1 {
		ls.noteNonExtendingBlockRejection(healthyConn, point, nil)
	}
	for range nonExtendingBlockRejectionThreshold {
		ls.noteNonExtendingBlockRejection(floodConn, point, nil)
	}

	got := testutil.RequireReceive(
		t,
		recycled,
		2*time.Second,
		"flooding connection must be recycled",
	)
	assert.Equal(t, floodConn, got.ConnectionId)
	testutil.RequireNoReceive(
		t,
		recycled,
		50*time.Millisecond,
		"healthy connection must not be recycled by another connection's flood",
	)
}

// nonExtendingRejectionTestBlock is a minimal ledger.Block, modeled on
// spliceAuditBlock, with a settable BlockNumber so the same helper can build
// both a block that fits the fixture's chain tip (extending it, clearing the
// tracked count) and one that does not (BlockNotFitChainTipError).
type nonExtendingRejectionTestBlock struct {
	hash        lcommon.Blake2b256
	prevHash    lcommon.Blake2b256
	slot        uint64
	blockNumber uint64
}

func (b *nonExtendingRejectionTestBlock) Hash() lcommon.Blake2b256 { return b.hash }
func (b *nonExtendingRejectionTestBlock) PrevHash() lcommon.Blake2b256 {
	return b.prevHash
}
func (b *nonExtendingRejectionTestBlock) SlotNumber() uint64  { return b.slot }
func (b *nonExtendingRejectionTestBlock) BlockNumber() uint64 { return b.blockNumber }
func (b *nonExtendingRejectionTestBlock) IssuerVkey() lcommon.IssuerVkey {
	return lcommon.IssuerVkey{}
}
func (b *nonExtendingRejectionTestBlock) BlockBodySize() uint64 { return 0 }
func (b *nonExtendingRejectionTestBlock) Era() lcommon.Era      { return lcommon.Era{} }
func (b *nonExtendingRejectionTestBlock) Cbor() []byte          { return nil }
func (b *nonExtendingRejectionTestBlock) BlockBodyHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}
func (b *nonExtendingRejectionTestBlock) Header() lcommon.BlockHeader { return nil }
func (b *nonExtendingRejectionTestBlock) Type() int                   { return 0 }
func (b *nonExtendingRejectionTestBlock) Transactions() []lcommon.Transaction {
	return nil
}
func (b *nonExtendingRejectionTestBlock) Utxorpc() (*utxorpc.Block, error) {
	return nil, nil
}

// TestFlushPendingBlockfetchNonExtendingFloodRecyclesConnection is the
// end-to-end "should demote" regression: nonExtendingBlockRejectionThreshold
// blocks from the same connection, none of which fit the real chain tip,
// delivered through the actual flushPendingBlockfetchBlocksDeferred path
// (the code that logs "ignoring blockfetch block ... does not fit on
// current chain tip"), must publish exactly one recycle request for that
// connection.
func TestFlushPendingBlockfetchNonExtendingFloodRecyclesConnection(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	// None of these fit: their PrevHash matches neither the ancestor nor the
	// current tip, so each one hits BlockNotFitChainTipError. Each carries a
	// unique hash so none collides with another.
	events := make([]BlockfetchEvent, 0, nonExtendingBlockRejectionThreshold)
	for i := range nonExtendingBlockRejectionThreshold {
		hash := testHashBytes(fmt.Sprintf("flood-block-%d", i))
		block := &nonExtendingRejectionTestBlock{
			hash:        lcommon.NewBlake2b256(hash),
			prevHash:    lcommon.NewBlake2b256(testHashBytes("abandoned-parent")),
			slot:        fixture.currentTip.Point.Slot + uint64(i) + 1,
			blockNumber: fixture.currentTip.BlockNumber + 1,
		}
		events = append(events, BlockfetchEvent{
			ConnectionId: fixture.connId,
			Block:        block,
			Point:        ocommon.NewPoint(block.slot, hash),
		})
	}
	ls.pendingBlockfetchEvents = events

	err := ls.flushPendingBlockfetchBlocksDeferred(nil)
	require.NoError(
		t,
		err,
		"a non-extending block is ignored, not a hard processing error",
	)

	got := testutil.RequireReceive(
		t,
		recycled,
		2*time.Second,
		"flooding connection must be recycled",
	)
	assert.Equal(t, fixture.connId, got.ConnectionId)
	assert.Equal(t, "non_extending_block_flood", got.Reason)
	assert.Equal(
		t,
		fixture.currentTip.Point.Hash,
		ls.chain.Tip().Point.Hash,
		"the real chain tip must be completely unaffected by the flood",
	)
	assert.Equal(
		t,
		float64(nonExtendingBlockRejectionThreshold),
		promtestutil.ToFloat64(ls.metrics.nonExtendingBlockRejections),
		"every rejected block in the flood must be counted, not only the ones before threshold",
	)
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(ls.metrics.nonExtendingBlockFloodRecycles),
		"exactly one recycle event fired for this flood",
	)
}

// TestFlushPendingBlockfetchNonExtendingHandfulNotRecycled is the "should
// NOT demote" regression for a legitimate brief rollback/reorg race: a
// handful of blocks that momentarily do not fit the tip, well under
// nonExtendingBlockRejectionThreshold, must not trigger a recycle.
func TestFlushPendingBlockfetchNonExtendingHandfulNotRecycled(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	const handful = 3 // well under nonExtendingBlockRejectionThreshold
	events := make([]BlockfetchEvent, 0, handful)
	for i := range handful {
		hash := testHashBytes(fmt.Sprintf("race-block-%d", i))
		block := &nonExtendingRejectionTestBlock{
			hash:        lcommon.NewBlake2b256(hash),
			prevHash:    lcommon.NewBlake2b256(testHashBytes("abandoned-parent")),
			slot:        fixture.currentTip.Point.Slot + uint64(i) + 1,
			blockNumber: fixture.currentTip.BlockNumber + 1,
		}
		events = append(events, BlockfetchEvent{
			ConnectionId: fixture.connId,
			Block:        block,
			Point:        ocommon.NewPoint(block.slot, hash),
		})
	}
	ls.pendingBlockfetchEvents = events

	require.NoError(t, ls.flushPendingBlockfetchBlocksDeferred(nil))

	testutil.RequireNoReceive(
		t,
		recycled,
		100*time.Millisecond,
		"a handful of rejections from a brief rollback race must not recycle the peer",
	)
	assert.Equal(
		t,
		handful,
		ls.nonExtendingBlockRejections[connIdKey(fixture.connId)].count,
		"the rejections must still be tracked, just below threshold",
	)
	assert.Equal(
		t,
		float64(handful),
		promtestutil.ToFloat64(ls.metrics.nonExtendingBlockRejections),
		"individual rejections are still counted even when no flood is detected",
	)
	assert.Equal(
		t,
		float64(0),
		promtestutil.ToFloat64(ls.metrics.nonExtendingBlockFloodRecycles),
		"a brief rollback race must not increment the recycle counter",
	)
}
