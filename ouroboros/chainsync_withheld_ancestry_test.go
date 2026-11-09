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
	"testing"
	"time"

	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

const withheldTipBlock = 10

type withheldAncestryFixture struct {
	o     *Ouroboros
	state *dchainsync.State
	conn  ouroboros.ConnectionId
	now   time.Time

	mu       sync.Mutex
	tipHash  []byte
	withheld bool
	armed    []time.Time
	timers   []func()
}

// newWithheldAncestryFixture wires an apply-eligible connection whose local
// chain is at block withheldTipBlock with an empty header queue. While
// withheld is set, the header verifier reports unavailable ancestry.
func newWithheldAncestryFixture(t *testing.T) *withheldAncestryFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	f := &withheldAncestryFixture{
		conn:    newTestConnId("127.0.0.1:6000", "10.0.0.1:3001"),
		now:     time.Unix(1_700_000_000, 0),
		tipHash: []byte("applied-tip-1"),
	}
	cfg := dchainsync.DefaultConfig()
	cfg.Now = func() time.Time {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.now
	}
	f.state = dchainsync.NewStateWithConfig(bus, nil, cfg)
	require.True(t, f.state.AddClientConnId(f.conn))
	f.o = newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	f.o.chainsyncState = f.state
	f.o.eventBus = bus
	f.o.futureHeaderResyncCtx = context.Background()
	f.o.chainsyncScheduleAt = func(onset time.Time, fn func()) func() {
		f.mu.Lock()
		f.armed = append(f.armed, onset)
		f.timers = append(f.timers, fn)
		f.mu.Unlock()
		return func() {}
	}
	f.o.chainsyncLocalTipBlock = func() (uint64, uint64) {
		return withheldTipBlock, 2160
	}
	f.o.chainsyncLocalChainTips = func() ([]byte, []byte) {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.tipHash, f.tipHash
	}
	f.o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.withheld {
			return 0, ledger.ErrHeaderVerificationWithheld
		}
		return 42, nil
	}
	return f
}

func (f *withheldAncestryFixture) setWithheld(v bool) {
	f.mu.Lock()
	f.withheld = v
	f.mu.Unlock()
}

func (f *withheldAncestryFixture) setAppliedTip(hash string) {
	f.mu.Lock()
	f.tipHash = []byte(hash)
	f.mu.Unlock()
}

// fireLast runs the most recently armed timer callback.
func (f *withheldAncestryFixture) fireLast(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	require.NotEmpty(t, f.timers)
	fn := f.timers[len(f.timers)-1]
	f.mu.Unlock()
	fn()
}

func (f *withheldAncestryFixture) armedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.armed)
}

// clearRepeatWindow ends the per-address spacing of deferred recoveries.
func (f *withheldAncestryFixture) clearRepeatWindow() {
	f.o.deferredRecoveryRecycleMu.Lock()
	clear(f.o.deferredRecoveryRecycleAt)
	f.o.deferredRecoveryRecycleMu.Unlock()
}

// advanceClock moves the tracked-client clock.
func (f *withheldAncestryFixture) advanceClock(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}

func (f *withheldAncestryFixture) lastActivity(t *testing.T) time.Time {
	t.Helper()
	tc := f.state.GetTrackedClient(f.conn)
	require.NotNil(t, tc)
	return tc.LastActivity
}

// deliver rolls forward a header at block number block whose parent is a hash
// that is neither on the local chain nor in the peer's history.
func (f *withheldAncestryFixture) deliver(
	t *testing.T,
	block uint64,
) {
	t.Helper()
	var hash, prev gledger.Blake2b256
	hash[0], hash[1] = 0xd0, byte(block)
	prev[0], prev[1] = 0xd0, byte(block-1)
	header := &testBlockHeader{
		hash:        hash,
		prevHash:    prev,
		blockNumber: block,
		slotNumber:  block * 20,
	}
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(1_000_000, []byte("advertised-tip")),
		BlockNumber: 1_000_000,
	}
	require.NoError(t, f.o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		0,
		header,
		tip,
		time.Now(),
	))
}

func (f *withheldAncestryFixture) scheduledReason() (string, bool) {
	f.o.futureHeaderResyncMu.Lock()
	defer f.o.futureHeaderResyncMu.Unlock()
	s := f.o.futureHeaderResyncs[f.conn]
	if s == nil {
		return "", false
	}
	return s.reason, true
}

// An apply-eligible connection whose streamed headers cannot be linked to its history above the
// applied tip is re-intersected from the local chain, once per applied tip.
func TestChainsyncWithheldAncestryReintersectsConnection(t *testing.T) {
	t.Parallel()
	f := newWithheldAncestryFixture(t)
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	f.setWithheld(true)

	for block := uint64(withheldTipBlock + 2); block < withheldTipBlock+6; block++ {
		f.deliver(t, block)
	}
	reason, ok := f.scheduledReason()
	require.True(t, ok, "withheld ancestry above the applied tip must re-intersect the connection")
	require.Equal(t, event.ChainsyncResyncReasonWithheldCandidateAncestry, reason)
	require.Equal(t, 1, f.armedCount())

	f.fireLast(t)
	evt := testutil.RequireReceive(
		t,
		resyncCh,
		time.Second,
		"the armed timer must publish the resync",
	)
	data := evt.Data.(event.ChainsyncResyncEvent)
	require.Equal(t, f.conn, data.ConnectionId)
	require.Equal(t, event.ChainsyncResyncReasonWithheldCandidateAncestry, data.Reason)

	// The resync handler restarted the protocol. A new applied tip inside the
	// repeat window still waits.
	f.o.cancelFutureHeaderResync(f.conn)
	f.setAppliedTip("applied-tip-2")
	f.deliver(t, withheldTipBlock+2)
	require.Equal(t, 1, f.armedCount(), "the repeat window spaces resyncs")

	// Redelivered headers withheld again at the tip already retried: no
	// second resync even once the window has passed.
	f.setAppliedTip("applied-tip-1")
	f.clearRepeatWindow()
	for block := uint64(withheldTipBlock + 2); block < withheldTipBlock+6; block++ {
		f.deliver(t, block)
	}
	require.Equal(t, 1, f.armedCount(), "one resync per applied tip")

	// A new applied tip after the window may re-intersect again.
	f.setAppliedTip("applied-tip-2")
	f.deliver(t, withheldTipBlock+2)
	require.Equal(t, 2, f.armedCount())
}

// A withheld header is not evidence that the peer is making progress, so it
// must not refresh the activity clock the stall recycler reads.
func TestChainsyncWithheldAncestryDoesNotRefreshLastActivity(t *testing.T) {
	t.Parallel()
	f := newWithheldAncestryFixture(t)
	f.setWithheld(true)
	before := f.lastActivity(t)
	f.advanceClock(time.Minute)
	f.deliver(t, withheldTipBlock+2)
	require.Equal(t, before, f.lastActivity(t))
	require.Zero(t, f.state.GetTrackedClient(f.conn).HeadersRecv)
}

// Linkable headers from a healthy connection are neither re-intersected nor
// stop refreshing the activity clock.
func TestChainsyncLinkableHeadersAreNotReintersected(t *testing.T) {
	t.Parallel()
	f := newWithheldAncestryFixture(t)
	before := f.lastActivity(t)
	f.advanceClock(time.Minute)
	for block := uint64(withheldTipBlock + 1); block < withheldTipBlock+5; block++ {
		f.deliver(t, block)
	}
	_, scheduled := f.scheduledReason()
	require.False(t, scheduled)
	require.Zero(t, f.armedCount())
	require.True(t, f.lastActivity(t).After(before))
}

// Only a withheld header above the applied tip can be the missing link, and
// only a connection that can feed the ledger is worth re-intersecting.
func TestChainsyncWithheldAncestryRecoveryScope(t *testing.T) {
	t.Parallel()
	t.Run("at or below the applied tip", func(t *testing.T) {
		t.Parallel()
		f := newWithheldAncestryFixture(t)
		f.setWithheld(true)
		f.deliver(t, withheldTipBlock)
		require.Zero(t, f.armedCount())
	})
	t.Run("not apply eligible", func(t *testing.T) {
		t.Parallel()
		f := newWithheldAncestryFixture(t)
		f.setWithheld(true)
		f.o.config.ChainsyncApplyEligible = func(ouroboros.ConnectionId) bool {
			return false
		}
		f.deliver(t, withheldTipBlock+2)
		require.Zero(t, f.armedCount())
	})
}

// A close that lands after the resync is scheduled must remove the retry
// marker, or a replacement connection reusing the ConnectionId is suppressed
// at the same applied tip. The close's marker cleanup runs from inside the
// scheduler; its timer cancel runs after delivery returns, because the
// scheduler holds the timer lock while arming.
func TestChainsyncWithheldAncestryCloseDuringSchedulingClearsMarker(t *testing.T) {
	t.Parallel()
	f := newWithheldAncestryFixture(t)
	f.setWithheld(true)
	closeOnArm := true
	f.o.chainsyncScheduleAt = func(onset time.Time, _ func()) func() {
		f.mu.Lock()
		f.armed = append(f.armed, onset)
		f.mu.Unlock()
		if closeOnArm {
			closeOnArm = false
			f.o.deferredHeaderRecoveryMu.Lock()
			delete(f.o.withheldAncestryRetry, f.conn)
			f.o.deferredHeaderRecoveryMu.Unlock()
		}
		return func() {}
	}
	f.deliver(t, withheldTipBlock+2)
	require.Equal(t, 1, f.armedCount())
	f.o.cancelFutureHeaderResync(f.conn)

	// The replacement connection, after the repeat window, at the same tip.
	f.clearRepeatWindow()
	f.deliver(t, withheldTipBlock+2)
	require.Equal(t, 2, f.armedCount(),
		"a marker written after the close must not suppress the replacement")
}

// A failed schedule drops the claimed marker, so the next withheld header at
// the same applied tip may retry.
func TestChainsyncWithheldAncestryFailedScheduleDropsMarker(t *testing.T) {
	t.Parallel()
	f := newWithheldAncestryFixture(t)
	f.setWithheld(true)
	f.o.futureHeaderResyncMu.Lock()
	f.o.futureHeaderResyncClosed = true
	f.o.futureHeaderResyncMu.Unlock()
	f.deliver(t, withheldTipBlock+2)
	require.Zero(t, f.armedCount())
	f.o.deferredHeaderRecoveryMu.Lock()
	_, marked := f.o.withheldAncestryRetry[f.conn]
	f.o.deferredHeaderRecoveryMu.Unlock()
	require.False(t, marked)

	f.o.futureHeaderResyncMu.Lock()
	f.o.futureHeaderResyncClosed = false
	f.o.futureHeaderResyncMu.Unlock()
	f.deliver(t, withheldTipBlock+2)
	require.Equal(t, 1, f.armedCount())
}
