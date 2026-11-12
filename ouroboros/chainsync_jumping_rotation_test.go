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
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

func (h *csjHarness) closeConn(connId ouroboros.ConnectionId) {
	h.o.HandleConnClosedEvent(event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{ConnectionId: connId},
	))
}

// An objector refused a jump, so it is on another chain or behind the dynamo,
// and it holds no ingress slot. An agreed jumper vouched for the dynamo's
// fragment, so it replaces a departed dynamo ahead of the objector.
func TestChainsyncJumpingRotationPrefersAgreedJumperOverObjector(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	dynamo, jumpers := h.startDynamoAndJumpers(100, 100)
	agreed, objector := jumpers[0], jumpers[1]
	objector.tipBlock.Store(0)

	h.fragment.Store(20)
	h.deliver(dynamo.connId, 20)
	agreed.requireJump(t, "agreed jumper jump")
	objector.requireJump(t, "refused jump")
	h.requireAgreement(agreed.connId, dynamo.connId, csjPoint(20))
	require.Equal(
		t,
		objector.connId,
		testutil.RequireReceive(t, h.rejections, 5*time.Second, "rejection"),
	)
	objector.requireStreams(t, "objector")
	h.awaitRound()

	h.closeConn(dynamo.connId)

	require.True(
		t,
		h.o.csjIsDynamo(agreed.connId),
		"the agreed jumper must replace the dynamo, not the objector",
	)
	require.False(t, h.o.csjIsDynamo(objector.connId))
	agreed.requireStreams(t, "new dynamo")
	observer, tracked := h.o.chainsyncState.ClientObservabilityOnly(
		agreed.connId,
	)
	require.True(t, tracked)
	require.False(t, observer, "the new dynamo holds an ingress slot")
}

// With nothing better, an objector is elected, and it claims an ingress slot
// as it is elected: its header stream is the dynamo's, and a dynamo that is
// an observer extends no candidate fragment.
func TestChainsyncJumpingRotationObjectorIsLastResortAndClaimsIngress(
	t *testing.T,
) {
	t.Parallel()
	h := newCsjHarness(t, true)
	dynamo, jumpers := h.startDynamoAndJumpers(100)
	objector := jumpers[0]
	objector.tipBlock.Store(0)
	h.fragment.Store(20)
	h.deliver(dynamo.connId, 20)
	objector.requireJump(t, "refused jump")
	testutil.RequireReceive(t, h.rejections, 5*time.Second, "rejection")
	objector.requireStreams(t, "objector")
	h.awaitRound()
	observer, _ := h.o.chainsyncState.ClientObservabilityOnly(objector.connId)
	require.True(t, observer, "the objector streams without an ingress slot")

	h.closeConn(dynamo.connId)

	require.True(t, h.o.csjIsDynamo(objector.connId))
	observer, tracked := h.o.chainsyncState.ClientObservabilityOnly(
		objector.connId,
	)
	require.True(t, tracked)
	require.False(t, observer, "an elected dynamo holds an ingress slot")
}

// A promotion takes an observer's callback out of its wait: the header was
// held as an observer's, and the connection is no longer one. The wake does
// not depend on a ledger snapshot being published, which is the wait's only
// other exit besides its bound.
func TestChainsyncPromotionWakesObserverHorizonWait(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, false)
	f.o.catchUpBandWaitTimeout = time.Minute
	var promoted atomic.Bool
	f.o.config.ChainsyncIngressEligible = func(ouroboros.ConnectionId) bool {
		return promoted.Load()
	}
	entered := make(chan struct{})
	var enterOnce sync.Once
	f.o.chainsyncObserverHorizonWait = func(
		ctx context.Context, _ ledger.ChainsyncEvent,
	) error {
		enterOnce.Do(func() { close(entered) })
		<-ctx.Done()
		return ctx.Err()
	}
	f.o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	var verifyCalls atomic.Int32
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId, gledger.BlockHeader,
	) (uint64, error) {
		verifyCalls.Add(1)
		return 42, nil
	}
	f.o.config.ChainsyncRecordDeliveredPeerTip = func(
		chainselection.PeerTipUpdateEvent,
	) {
	}
	done := make(chan error, 1)
	go func() { done <- f.rollForwardError(newTestBlockHeader(100, 1, 0xaa)) }()
	testutil.RequireReceive(t, entered, testutil.AsyncWait,
		"observer header did not reach the horizon wait")
	testutil.RequireNoReceive(t, done, 50*time.Millisecond,
		"observer callback must stay parked until promoted")

	promoted.Store(true)
	require.True(t, f.o.reconcileChainsyncIngressAdmission(f.conn, true))

	require.NoError(t, testutil.RequireReceive(t, done, 10*time.Second,
		"a promoted connection's callback stayed parked in the observer wait"))
	require.Zero(t, verifyCalls.Load(),
		"a header held as an observer's must not be verified after promotion")
}

func newCsjStallHarness(
	t *testing.T,
	timeout time.Duration,
) (*csjHarness, <-chan event.Event) {
	t.Helper()
	h := newCsjHarness(t, true)
	h.o.csj.cfg.DynamoStallTimeout = timeout
	_, recycles := h.o.eventBus.Subscribe(
		ledger.ConnectionRecycleRequestedEventType,
	)
	return h, recycles
}

func requireStallRecycle(
	t *testing.T,
	recycles <-chan event.Event,
	connId ouroboros.ConnectionId,
) {
	t.Helper()
	evt := testutil.RequireReceive(t, recycles, 30*time.Second,
		"a dynamo that delivers no header was not recycled")
	data, ok := evt.Data.(ledger.ConnectionRecycleRequestedEvent)
	require.True(t, ok)
	require.Equal(t, connId, data.ConnectionId)
	require.Equal(t, csjDynamoStallReason, data.Reason)
}

// A dynamo that delivers no header is replaced whatever the reason, as the
// stall recycler cannot: it skips an observer, and waits out a grace period
// and cooldown meant for a node that is making progress.
func TestChainsyncJumpingStalledDynamoIsRecycled(t *testing.T) {
	t.Parallel()
	h, recycles := newCsjStallHarness(t, 50*time.Millisecond)
	dynamo := h.addPeer("dynamo", 100)
	h.promote(dynamo)
	dynamo.requireStreams(t, "dynamo")

	requireStallRecycle(t, recycles, dynamo.connId)
}

func TestChainsyncJumpingDynamoStallCheck(t *testing.T) {
	t.Parallel()
	const timeout = time.Minute
	start := time.Unix(1_700_000_000, 0)
	setup := func(
		t *testing.T,
		configure func(*ChainsyncJumpingConfig),
	) (*csjHarness, *csjPeer, *atomic.Pointer[time.Time], <-chan event.Event) {
		h, recycles := newCsjStallHarness(t, timeout)
		var clock atomic.Pointer[time.Time]
		clock.Store(&start)
		h.o.csj.now = func() time.Time { return *clock.Load() }
		if configure != nil {
			configure(&h.o.csj.cfg)
		}
		dynamo := h.addPeer("dynamo", 100)
		h.promote(dynamo)
		dynamo.requireStreams(t, "dynamo")
		return h, dynamo, &clock, recycles
	}
	at := func(clock *atomic.Pointer[time.Time], d time.Duration) time.Time {
		now := start.Add(d)
		clock.Store(&now)
		return now
	}
	t.Run("a delivery restarts the clock", func(t *testing.T) {
		t.Parallel()
		h, dynamo, clock, recycles := setup(t, nil)
		h.o.csjCheckDynamoStall(at(clock, timeout-time.Second))
		h.deliver(dynamo.connId, 1)
		h.o.csjCheckDynamoStall(at(clock, 2*timeout-2*time.Second))
		testutil.RequireNoReceive(t, recycles, 50*time.Millisecond,
			"a dynamo that delivered within the timeout was recycled")
		h.o.csjCheckDynamoStall(at(clock, 2*timeout))
		requireStallRecycle(t, recycles, dynamo.connId)
	})
	t.Run("local progress restarts the clock", func(t *testing.T) {
		t.Parallel()
		var progress atomic.Uint64
		h, dynamo, clock, recycles := setup(t, func(c *ChainsyncJumpingConfig) {
			c.LocalProgress = progress.Load
		})
		h.o.csjCheckDynamoStall(at(clock, timeout-time.Second))
		progress.Add(1)
		h.o.csjCheckDynamoStall(at(clock, timeout+time.Second))
		testutil.RequireNoReceive(t, recycles, 50*time.Millisecond,
			"a dynamo was recycled while the ledger applied blocks")
		h.o.csjCheckDynamoStall(at(clock, 2*timeout+2*time.Second))
		requireStallRecycle(t, recycles, dynamo.connId)
	})
	t.Run("a held dynamo is left alone", func(t *testing.T) {
		t.Parallel()
		var held atomic.Bool
		held.Store(true)
		h, dynamo, clock, recycles := setup(t, func(c *ChainsyncJumpingConfig) {
			c.Held = func(ouroboros.ConnectionId) bool { return held.Load() }
		})
		h.o.csjCheckDynamoStall(at(clock, 3*timeout))
		testutil.RequireNoReceive(t, recycles, 50*time.Millisecond,
			"a dynamo held at the Limit on Eagerness was recycled")
		held.Store(false)
		h.o.csjCheckDynamoStall(at(clock, 4*timeout+time.Second))
		requireStallRecycle(t, recycles, dynamo.connId)
	})
}
