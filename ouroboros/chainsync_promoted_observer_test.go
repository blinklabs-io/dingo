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

package ouroboros

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// A header numbered at or below the applied tip can never reach the ledger,
// so holding it for corroboration only stalls its peer.
func TestChainsyncHeaderAtOrBelowLedgerTipIsNotHeld(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         400,
		MinCorroboratingPeers: 1,
	})
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	require.True(t, state.AddClientConnId(conn))
	var holds atomic.Int32
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip: cs.PreparePeerTipAdmissionDisposition,
		ChainsyncResolvePeerTip: resolvePeerTipLikeNode(cs),
		ChainsyncApplyEligible:  cs.ShouldApplyIngress,
		ChainsyncAwaitCorroboration: func(
			ctx context.Context,
			connId ouroboros.ConnectionId,
		) (bool, error) {
			holds.Add(1)
			return cs.AwaitIngressCorroboration(ctx, connId)
		},
	})
	o.chainsyncState = state
	o.eventBus = bus
	o.corroborationHoldTimeout = 10 * time.Millisecond
	o.chainsyncScheduleAt = func(time.Time, func()) func() { return func() {} }
	const ledgerTipBlock = uint64(5)
	o.chainsyncLocalTipBlock = func() (uint64, uint64) {
		return ledgerTipBlock, 400
	}
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(10_000_000, []byte("advertised-tip")),
		BlockNumber: 500_000,
	}
	headers := newLinkedTestHeaders(int(ledgerTipBlock) + 1)
	for _, header := range headers[:ledgerTipBlock] {
		require.NoError(t, o.chainsyncClientRollForward(
			ochainsync.CallbackContext{ConnectionId: conn},
			0,
			header,
			tip,
		))
	}
	require.Zero(t, holds.Load(),
		"a header at or below the ledger tip must not be held")

	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		headers[ledgerTipBlock],
		tip,
	))
	require.Equal(t, int32(1), holds.Load(),
		"the header after the ledger tip is still held for corroboration")
}

// A Byron epoch boundary block shares its predecessor's block number, so one
// numbered as the ledger tip is the block after the tip and is still held.
func TestChainsyncByronEbbNumberedAsLedgerTipIsHeld(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         400,
		MinCorroboratingPeers: 1,
	})
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	require.True(t, state.AddClientConnId(conn))
	var holds atomic.Int32
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip: cs.PreparePeerTipAdmissionDisposition,
		ChainsyncResolvePeerTip: resolvePeerTipLikeNode(cs),
		ChainsyncApplyEligible:  cs.ShouldApplyIngress,
		ChainsyncAwaitCorroboration: func(
			ctx context.Context,
			connId ouroboros.ConnectionId,
		) (bool, error) {
			holds.Add(1)
			return cs.AwaitIngressCorroboration(ctx, connId)
		},
	})
	o.chainsyncState = state
	o.eventBus = bus
	o.corroborationHoldTimeout = 10 * time.Millisecond
	o.chainsyncScheduleAt = func(time.Time, func()) func() { return func() {} }
	const ledgerTipBlock = uint64(5)
	o.chainsyncLocalTipBlock = func() (uint64, uint64) {
		return ledgerTipBlock, 400
	}
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(10_000_000, []byte("advertised-tip")),
		BlockNumber: 500_000,
	}
	headers := newLinkedTestHeaders(int(ledgerTipBlock))
	for _, header := range headers {
		require.NoError(t, o.chainsyncClientRollForward(
			ochainsync.CallbackContext{ConnectionId: conn},
			0,
			header,
			tip,
		))
	}
	require.Zero(t, holds.Load())

	ebb := &byron.ByronEpochBoundaryBlockHeader{
		PrevBlock: headers[ledgerTipBlock-1].Hash(),
	}
	ebb.ConsensusData.Epoch = 1
	ebb.ConsensusData.Difficulty.Value = ledgerTipBlock
	require.Equal(t, ledgerTipBlock, ebb.BlockNumber())
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		gledger.BlockTypeByronEbb,
		ebb,
		tip,
	))
	require.Equal(t, int32(1), holds.Load(),
		"a boundary block numbered as the ledger tip must be held")
}

type promotedObserverFixture struct {
	forecastHorizonFixture
	resyncCh <-chan event.Event
}

func newPromotedObserverFixture(
	t *testing.T,
	observer bool,
) promotedObserverFixture {
	t.Helper()
	f := newForecastHorizonFixture(t, "10.0.0.30:3001")
	if observer {
		f.o.chainsyncState.RemoveClientConnId(f.conn)
		require.True(
			t,
			f.o.chainsyncState.TryAddObservedClientConnIdWithDirection(
				f.conn,
				false,
			),
		)
	}
	f.o.chainSelectionShouldVerifyHeaderCrypto = nil
	f.setLocalTip(10, 432)
	f.setBestPeer(true)
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	return promotedObserverFixture{
		forecastHorizonFixture: f,
		resyncCh:               resyncCh,
	}
}

// An observer streams headers without recording them, so promoting it at a
// cursor far above the ledger gives it a frontier that begins there. It is
// re-synced from the local tip, and the header is not recorded.
func TestChainsyncPromotedObserverAboveLedgerIsResynced(t *testing.T) {
	t.Parallel()

	f := newPromotedObserverFixture(t, true)
	f.rollForwardWithTip(t, newTestBlockHeader(500, 500, 0xa1), peerTipAt(900))

	resync := testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"a promoted observer above the ledger must be re-synced",
	)
	data, ok := resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(
		t,
		event.ChainsyncResyncReasonPromotedObserverAhead,
		data.Reason,
	)
	require.Equal(t, f.conn, data.ConnectionId)
	testutil.RequireNoReceive(
		t,
		f.tipCh,
		200*time.Millisecond,
		"the header that starts the frontier above the ledger must not be recorded",
	)
}

// A promoted observer whose cursor is the block after the ledger tip already
// contributes to the frontier from the tip.
func TestChainsyncPromotedObserverAtLedgerTipIsNotResynced(t *testing.T) {
	t.Parallel()

	f := newPromotedObserverFixture(t, true)
	f.rollForwardWithTip(t, newTestBlockHeader(11, 11, 0xa2), peerTipAt(900))

	testutil.RequireReceive(
		t,
		f.tipCh,
		testutil.AsyncWait,
		"the promoted observer's header must be recorded",
	)
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		200*time.Millisecond,
		"a promoted observer at the ledger tip must not be re-synced",
	)
}

// A connection that was ingress-eligible from its first header legitimately
// resumes above the ledger from the primary chain's points.
func TestChainsyncIngressConnectionAboveLedgerIsNotResynced(t *testing.T) {
	t.Parallel()

	f := newPromotedObserverFixture(t, false)
	f.rollForwardWithTip(t, newTestBlockHeader(500, 500, 0xa3), peerTipAt(900))

	testutil.RequireReceive(
		t,
		f.tipCh,
		testutil.AsyncWait,
		"the ingress connection's header must be recorded",
	)
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		200*time.Millisecond,
		"only a promoted observer is re-synced for starting above the ledger",
	)
}

// While Genesis corroboration is active a new session intersects at the
// applied ledger tip; the primary chain's header-ahead points would start the
// peer's frontier above the ledger again.
func TestBuildDefaultChainsyncIntersectPointsAtLedgerTipForCorroboration(
	t *testing.T,
) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()

	ls, _ := newTestLedgerStateWithChain(t, 5)
	o, _, connId := newOutboundStartTestOuroboros(t, logger, bus)
	o.ledgerState = ls
	ledgerTip := ocommon.NewPoint(2, bytes.Repeat([]byte{2}, 32))
	ls.SetTipForTesting(ochainsync.Tip{Point: ledgerTip, BlockNumber: 2})
	require.Equal(t, uint64(5), ls.PrimaryChainTip().Point.Slot)

	points, err := o.buildDefaultChainsyncIntersectPoints(
		context.Background(),
		connId,
	)
	require.NoError(t, err)
	require.NotEmpty(t, points)
	require.Equal(t, uint64(5), points[0].Slot,
		"without corroboration the primary chain's points lead")

	o.config.ChainsyncIntersectAtLedgerTip = func() bool { return true }
	points, err = o.buildDefaultChainsyncIntersectPoints(
		context.Background(),
		connId,
	)
	require.NoError(t, err)
	require.NotEmpty(t, points)
	require.Equal(t, ledgerTip.Slot, points[0].Slot,
		"the newest intersect point must be the ledger tip")
	require.Equal(t, ledgerTip.Hash, points[0].Hash)
}
