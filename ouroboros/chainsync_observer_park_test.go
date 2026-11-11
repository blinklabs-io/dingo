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
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// parkObservers makes f's node treat observer deliveries as unused, as it does
// while Genesis selection runs without corroboration, and returns a channel
// that receives once the observer's callback is held.
func (f *promotionGapFixture) parkObservers(unused *atomic.Bool) <-chan struct{} {
	parked := make(chan struct{}, 1)
	f.o.config.ChainsyncObserverDeliveriesUnused = unused.Load
	f.o.observerParkPoll = time.Millisecond
	f.o.observerParkedHook = func() {
		select {
		case parked <- struct{}{}:
		default:
		}
	}
	return parked
}

// An observer's headers are dropped. While nothing counts them either, reading
// them only downloads the chain once more per connected peer, so the observer
// stops reading: its callback is held, the peer's pipelined replies back up,
// and nothing more is requested. A freed ingress slot releases it, and the
// promoted connection replays from a fresh intersection as any gapped observer
// does.
func TestObserverWithUnusedDeliveriesStopsReadingUntilPromoted(t *testing.T) {
	t.Parallel()
	f := newPromotionGapFixture(t)
	var unused atomic.Bool
	unused.Store(true)
	parked := f.parkObservers(&unused)

	done := make(chan error, 1)
	go func() { done <- f.header(100, 1, 0xaa) }()
	testutil.RequireReceive(
		t, parked, 5*time.Second, "the observer must stop reading",
	)
	select {
	case err := <-done:
		t.Fatalf("the observer's callback returned while held: %v", err)
	default:
	}
	require.EqualValues(t, 0, f.verified.Load(),
		"a header nobody counts must not be authenticated")
	require.True(t, f.o.chainsyncState.ClientObserverHeaderGap(f.conn),
		"a held observer falls behind and must replay on promotion")

	f.o.chainsyncState.RemoveClientConnId(f.fillers[0])
	require.NoError(t, testutil.RequireReceive(
		t, done, 5*time.Second, "a freed slot must release the observer",
	))
	observing, exists := f.o.chainsyncState.ClientObservabilityOnly(f.conn)
	require.True(t, exists)
	require.False(t, observing, "the released observer must hold the slot")
	timer := testutil.RequireReceive(
		t, f.timers, 5*time.Second, "the promoted observer must replay",
	)
	timer.fire()
	evt := testutil.RequireReceive(
		t, f.resyncCh, 5*time.Second, "the promoted observer must re-intersect",
	)
	require.Equal(
		t,
		event.ChainsyncResyncReasonIngressPromotion,
		evt.Data.(event.ChainsyncResyncEvent).Reason,
	)
	select {
	case evt := <-f.ledgerCh:
		if data, ok := evt.Data.(ledger.ChainsyncEvent); ok &&
			data.BlockHeader != nil {
			t.Fatal("the held header must not reach the ledger")
		}
	default:
	}
}

// Once observer deliveries are counted again the held observer resumes. Its
// cursor stopped where it was held, so it re-intersects rather than streaming
// everything applied since.
func TestHeldObserverResumesWhenDeliveriesAreCounted(t *testing.T) {
	t.Parallel()
	f := newPromotionGapFixture(t)
	var unused atomic.Bool
	unused.Store(true)
	parked := f.parkObservers(&unused)

	done := make(chan error, 1)
	go func() { done <- f.header(100, 1, 0xaa) }()
	testutil.RequireReceive(
		t, parked, 5*time.Second, "the observer must stop reading",
	)

	unused.Store(false)
	require.NoError(t, testutil.RequireReceive(
		t, done, 5*time.Second, "counted deliveries must release the observer",
	))
	observing, exists := f.o.chainsyncState.ClientObservabilityOnly(f.conn)
	require.True(t, exists)
	require.True(t, observing, "no slot was freed, so it stays an observer")
	timer := testutil.RequireReceive(
		t, f.timers, 5*time.Second, "the resumed observer must re-intersect",
	)
	timer.fire()
	evt := testutil.RequireReceive(
		t, f.resyncCh, 5*time.Second, "the resumed observer must re-intersect",
	)
	require.Equal(
		t,
		event.ChainsyncResyncReasonObserverResumed,
		evt.Data.(event.ChainsyncResyncEvent).Reason,
	)
}

// While observer deliveries are counted, as under Genesis corroboration, an
// observer keeps reading: its headers are the witness evidence.
func TestObserverWithCountedDeliveriesKeepsReading(t *testing.T) {
	t.Parallel()
	f := newPromotionGapFixture(t)
	var unused atomic.Bool
	parked := f.parkObservers(&unused)

	require.NoError(t, f.header(100, 1, 0xaa))
	select {
	case <-parked:
		t.Fatal("an observer whose deliveries are counted must not be held")
	default:
	}
}

// A held callback stops the protocol's receive loop, and the client fails a
// connection that consumes no reply within the protocol's CanAwait timer. The
// hold is therefore bounded like every other roll-forward wait: at the bound
// the header is dropped and the next one is held, so a held observer trickles
// one header per bound instead of streaming.
func TestObserverHoldIsBoundedBelowProtocolTimers(t *testing.T) {
	t.Parallel()
	f := newPromotionGapFixture(t)
	var unused atomic.Bool
	unused.Store(true)
	parked := f.parkObservers(&unused)
	f.o.observerParkPoll = time.Hour
	f.o.catchUpBandWaitTimeout = 20 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- f.header(100, 1, 0xaa) }()
	testutil.RequireReceive(
		t, parked, 5*time.Second, "the observer must stop reading",
	)
	require.NoError(t, testutil.RequireReceive(
		t, done, 5*time.Second, "the hold must end at its bound",
	))
	observing, exists := f.o.chainsyncState.ClientObservabilityOnly(f.conn)
	require.True(t, exists)
	require.True(t, observing)
	select {
	case <-f.timers:
		t.Fatal("a hold that reached its bound must not re-intersect")
	default:
	}
}

// setLocalTip makes f's node report a local tip at block and a security
// parameter k, and returns the variable that moves the local tip.
func (f *promotionGapFixture) setLocalTip(block, k uint64) *atomic.Uint64 {
	var local atomic.Uint64
	local.Store(block)
	f.o.chainsyncLocalTipBlock = func() (uint64, uint64) {
		return local.Load(), k
	}
	return &local
}

// Genesis selection is left only when every ChainSync connection, observers
// included, has sent MsgAwaitReply. A held observer reads one header per bound
// and never gets there, so an observer whose peer is within the security
// parameter of the local tip must read instead of being held.
func TestObserverNearLocalTipIsNotHeld(t *testing.T) {
	t.Parallel()
	f := newPromotionGapFixture(t)
	var unused atomic.Bool
	unused.Store(true)
	parked := f.parkObservers(&unused)
	f.o.catchUpBandWaitTimeout = 20 * time.Millisecond
	// header(100, 1, ...) advertises block 2.
	f.setLocalTip(2, 10)

	require.NoError(t, f.header(100, 1, 0xaa))
	select {
	case <-parked:
		t.Fatal("an observer whose peer is at the local tip must not be held")
	default:
	}
}

// A peer more than the security parameter ahead of the local tip is the
// catch-up case the hold exists for, and the exit rule cannot be met there.
func TestObserverFarAheadOfLocalTipIsHeld(t *testing.T) {
	t.Parallel()
	f := newPromotionGapFixture(t)
	var unused atomic.Bool
	unused.Store(true)
	parked := f.parkObservers(&unused)
	f.o.catchUpBandWaitTimeout = 20 * time.Millisecond
	// header(100, 5000, ...) advertises block 5001.
	f.setLocalTip(100, 10)

	done := make(chan error, 1)
	go func() { done <- f.header(100, 5000, 0xaa) }()
	testutil.RequireReceive(
		t, parked, 5*time.Second, "an observer far behind its peer must be held",
	)
	require.NoError(t, testutil.RequireReceive(
		t, done, 5*time.Second, "the hold must end at its bound",
	))
}

// A held observer is released once the local chain comes within the security
// parameter of its peer, so that it can reach MsgAwaitReply.
func TestHeldObserverResumesWhenLocalTipComesNear(t *testing.T) {
	t.Parallel()
	f := newPromotionGapFixture(t)
	var unused atomic.Bool
	unused.Store(true)
	parked := f.parkObservers(&unused)
	local := f.setLocalTip(100, 10)

	done := make(chan error, 1)
	go func() { done <- f.header(100, 5000, 0xaa) }()
	testutil.RequireReceive(
		t, parked, 5*time.Second, "the observer must stop reading",
	)

	local.Store(4995)
	require.NoError(t, testutil.RequireReceive(
		t, done, 5*time.Second, "a near local tip must release the observer",
	))
	timer := testutil.RequireReceive(
		t, f.timers, 5*time.Second, "the resumed observer must re-intersect",
	)
	timer.fire()
	evt := testutil.RequireReceive(
		t, f.resyncCh, 5*time.Second, "the resumed observer must re-intersect",
	)
	require.Equal(
		t,
		event.ChainsyncResyncReasonObserverResumed,
		evt.Data.(event.ChainsyncResyncEvent).Reason,
	)
}
