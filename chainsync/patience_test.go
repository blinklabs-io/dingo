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

package chainsync_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// patienceHarness drives a chainsync State with a fake clock so patience can
// be measured over simulated hours without waiting.
type patienceHarness struct {
	t      *testing.T
	state  *chainsync.State
	now    time.Time
	active atomic.Bool
}

func newPatienceHarness(
	t *testing.T,
	patience chainsync.PatienceConfig,
) *patienceHarness {
	t.Helper()
	h := &patienceHarness{t: t, now: time.Unix(1_700_000_000, 0)}
	h.active.Store(true)
	h.state = chainsync.NewStateWithConfig(nil, nil, chainsync.Config{
		MaxClients:         5,
		StallTimeout:       chainsync.DefaultStallTimeout,
		Patience:           patience,
		PatienceActiveFunc: h.active.Load,
		Now:                func() time.Time { return h.now },
	})
	return h
}

func (h *patienceHarness) advance(d time.Duration) { h.now = h.now.Add(d) }

// deliver simulates one header from the peer: it arrives now, this node spends
// processing on it, and it is accepted against the peer's advertised tip.
func (h *patienceHarness) deliver(
	connId ouroboros.ConnectionId,
	blockNumber uint64,
	tip ochainsync.Tip,
	processing time.Duration,
) {
	h.t.Helper()
	point := ocommon.NewPoint(
		blockNumber,
		[]byte(fmt.Sprintf("h%d", blockNumber)),
	)
	require.True(h.t, h.state.PatienceMessageArrived(connId, h.now))
	h.advance(processing)
	require.True(h.t, h.state.UpdateClientTipWithoutDedup(connId, point, tip))
	h.state.PatienceHeaderAccepted(
		connId,
		blockNumber,
		point.Slot == tip.Point.Slot,
	)
}

func farTip() ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(50_000_000, []byte("far")),
		BlockNumber: 50_000_000,
	}
}

func patience(capacity, rate uint64) chainsync.PatienceConfig {
	return chainsync.PatienceConfig{
		Enabled:  true,
		Capacity: capacity,
		Rate:     rate,
	}
}

// TestPatienceDisconnectsSlowDripPeer is the #4348 regression: a peer that
// advertises a far better tip and delivers one valid header every 110 seconds
// refreshes LastActivity each time, so the two-minute stall watchdog never
// fires. The Limit on Patience must exhaust it anyway.
func TestPatienceDisconnectsSlowDripPeer(t *testing.T) {
	t.Parallel()
	h := newPatienceHarness(t, chainsync.DefaultPatienceConfig())
	conn := newTestConnId(1)
	require.True(t, h.state.AddClientConnId(conn))

	// The first accepted header starts the leak. From then on 1000 tokens
	// leak at 5/s: 550 are gone after 110s, and one is earned back.
	h.deliver(conn, 1, farTip(), 0)
	h.advance(110 * time.Second)
	h.deliver(conn, 2, farTip(), 0)
	assert.Empty(t, h.state.CheckPatienceExhausted())
	assert.InDelta(t, 451, h.state.GetTrackedClient(conn).Patience.Tokens, 1e-9)

	// 451 tokens last 90.2s. The periodic check must catch the exhaustion
	// between drips, without waiting for the peer's next header.
	h.advance(90 * time.Second)
	assert.Empty(t, h.state.CheckPatienceExhausted())
	h.advance(time.Second)
	assert.Empty(
		t,
		h.state.CheckStalledClients(),
		"the stall watchdog must not be what disconnects the peer",
	)
	assert.Equal(t, []ouroboros.ConnectionId{conn},
		h.state.CheckPatienceExhausted())

	tc := h.state.GetTrackedClient(conn)
	require.NotNil(t, tc)
	assert.True(t, tc.Patience.Exhausted)
	assert.NotEqual(t, chainsync.ClientStatusStalled, tc.Status)

	// Exhaustion latches and is reported once, even if the peer keeps
	// dripping headers.
	h.advance(19 * time.Second)
	h.deliver(conn, 3, farTip(), 0)
	assert.True(t, h.state.GetTrackedClient(conn).Patience.Exhausted)
	assert.Empty(t, h.state.CheckPatienceExhausted())
}

func TestPatienceSlowDripPublishesDistinctEvent(t *testing.T) {
	t.Parallel()
	bus := newTestEventBus(t)
	_, patienceCh := bus.Subscribe(chainsync.ClientPatienceExhaustedEventType)
	now := time.Unix(1_700_000_000, 0)
	s := chainsync.NewStateWithConfig(bus, nil, chainsync.Config{
		Patience:           patience(10, 1),
		PatienceActiveFunc: func() bool { return true },
		Now:                func() time.Time { return now },
	})
	conn := newTestConnId(1)
	require.True(t, s.AddClientConnId(conn))
	s.UpdateClientTip(conn, ocommon.NewPoint(5, []byte("h5")), farTip())
	s.PatienceHeaderAccepted(conn, 5, false)
	now = now.Add(12 * time.Second)

	require.Equal(t, []ouroboros.ConnectionId{conn}, s.CheckPatienceExhausted())
	evt := testutil.RequireReceive(t, patienceCh, 2*time.Second, "patience event")
	data, ok := evt.Data.(chainsync.ClientPatienceExhaustedEvent)
	require.True(t, ok)
	assert.Equal(t, conn, data.ConnId)
	assert.Equal(t, uint64(50_000_000), data.TipBlockNumber)
	assert.Equal(t, uint64(1), data.HeadersDelivered)
}

// TestPatienceKeepsPeerDeliveringAtAcceptableRate is the negative case: a peer
// delivering faster than the leak rate keeps a full bucket indefinitely.
func TestPatienceKeepsPeerDeliveringAtAcceptableRate(t *testing.T) {
	t.Parallel()
	h := newPatienceHarness(t, chainsync.DefaultPatienceConfig())
	conn := newTestConnId(1)
	require.True(t, h.state.AddClientConnId(conn))

	// Ten headers per second against a five-token leak, for one simulated
	// hour, with a check every simulated 30 seconds like the recycler's.
	for block := uint64(1); block <= 36_000; block++ {
		h.advance(100 * time.Millisecond)
		h.deliver(conn, block, farTip(), 0)
		if block%300 == 0 {
			require.Empty(t, h.state.CheckPatienceExhausted())
		}
	}
	tc := h.state.GetTrackedClient(conn)
	assert.False(t, tc.Patience.Exhausted)
	assert.InDelta(t, chainsync.DefaultPatienceCapacity, tc.Patience.Tokens, 1)
}

// TestPatienceDoesNotChargeLocalProcessing pins that time this node spends
// processing a header (verification, admission waits, ledger backpressure) is
// not charged to the peer.
func TestPatienceDoesNotChargeLocalProcessing(t *testing.T) {
	t.Parallel()
	h := newPatienceHarness(t, chainsync.DefaultPatienceConfig())
	conn := newTestConnId(1)
	require.True(t, h.state.AddClientConnId(conn))

	for block := uint64(1); block <= 10; block++ {
		h.advance(10 * time.Millisecond)
		h.deliver(conn, block, farTip(), 10*time.Minute)
	}
	assert.Empty(t, h.state.CheckPatienceExhausted())
	assert.False(t, h.state.GetTrackedClient(conn).Patience.Exhausted)
}

func TestPatiencePausesWhenPeerHasDeliveredItsTip(t *testing.T) {
	t.Parallel()
	h := newPatienceHarness(t, chainsync.DefaultPatienceConfig())
	conn := newTestConnId(1)
	require.True(t, h.state.AddClientConnId(conn))
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(7, []byte("h7")),
		BlockNumber: 7,
	}
	h.deliver(conn, 7, tip, 0)

	// A caught-up peer is in MsgAwaitReply and owes nothing.
	h.advance(24 * time.Hour)
	assert.Empty(t, h.state.CheckPatienceExhausted())

	// A rollback away from the tip resumes the leak.
	require.True(t, h.state.UpdateClientRollback(
		conn,
		ocommon.NewPoint(3, []byte("h3")),
		farTip(),
	))
	h.advance(201 * time.Second)
	assert.Equal(t, []ouroboros.ConnectionId{conn},
		h.state.CheckPatienceExhausted())
}

func TestPatienceRedeliveryAfterRollbackEarnsNoTokens(t *testing.T) {
	t.Parallel()
	h := newPatienceHarness(t, patience(100, 1))
	conn := newTestConnId(1)
	require.True(t, h.state.AddClientConnId(conn))
	h.deliver(conn, 9, farTip(), 0)
	h.advance(50 * time.Second)
	h.deliver(conn, 10, farTip(), 0)
	require.InDelta(t, 51, h.state.GetTrackedClient(conn).Patience.Tokens, 1e-9)

	require.True(t, h.state.UpdateClientRollback(
		conn,
		ocommon.NewPoint(5, []byte("h5")),
		farTip(),
	))
	for block := uint64(6); block <= 10; block++ {
		h.deliver(conn, block, farTip(), 0)
	}
	assert.InDelta(t, 51, h.state.GetTrackedClient(conn).Patience.Tokens, 1e-9)
	h.deliver(conn, 11, farTip(), 0)
	assert.InDelta(t, 52, h.state.GetTrackedClient(conn).Patience.Tokens, 1e-9)
}

func TestPatienceAppliesOnlyWhileGenesisIsSyncing(t *testing.T) {
	t.Parallel()
	h := newPatienceHarness(t, chainsync.DefaultPatienceConfig())
	conn := newTestConnId(1)
	require.True(t, h.state.AddClientConnId(conn))
	h.deliver(conn, 1, farTip(), 0)
	h.advance(100 * time.Second)
	h.deliver(conn, 2, farTip(), 0)
	require.InDelta(t, 501, h.state.GetTrackedClient(conn).Patience.Tokens, 1e-9)

	// Outside Genesis syncing the bucket is held full, so a later return to
	// Genesis selection starts the peer with its whole budget.
	h.active.Store(false)
	h.advance(24 * time.Hour)
	assert.Empty(t, h.state.CheckPatienceExhausted())
	assert.InDelta(t, chainsync.DefaultPatienceCapacity,
		h.state.GetTrackedClient(conn).Patience.Tokens, 1e-9)

	h.active.Store(true)
	h.advance(199 * time.Second)
	assert.Empty(t, h.state.CheckPatienceExhausted())
	h.advance(2 * time.Second)
	assert.Equal(t, []ouroboros.ConnectionId{conn},
		h.state.CheckPatienceExhausted())
}

func TestPatienceIgnoresDisabledConfigAndObservabilityClients(t *testing.T) {
	t.Parallel()
	disabled := newPatienceHarness(t, chainsync.PatienceConfig{})
	conn := newTestConnId(1)
	require.True(t, disabled.state.AddClientConnId(conn))
	disabled.deliver(conn, 1, farTip(), 0)
	disabled.advance(24 * time.Hour)
	assert.Empty(t, disabled.state.CheckPatienceExhausted())

	h := newPatienceHarness(t, chainsync.DefaultPatienceConfig())
	observed := newTestConnId(2)
	require.True(t, h.state.TryAddObservedClientConnId(observed))
	h.deliver(observed, 1, farTip(), 0)
	h.advance(24 * time.Hour)
	assert.Empty(t, h.state.CheckPatienceExhausted())
}

func TestPatiencePauseStopsLeakUntilNextMessage(t *testing.T) {
	t.Parallel()
	h := newPatienceHarness(t, chainsync.DefaultPatienceConfig())
	conn := newTestConnId(1)
	require.True(t, h.state.AddClientConnId(conn))
	h.deliver(conn, 1, farTip(), 0)
	h.advance(100 * time.Second)
	h.state.PatiencePause(conn)
	h.advance(time.Hour)
	assert.Empty(t, h.state.CheckPatienceExhausted())
	assert.InDelta(t, 500, h.state.GetTrackedClient(conn).Patience.Tokens, 1e-9)
}

// TestPatienceStartsPausedUntilFirstAcceptedHeader pins that registration
// alone does not start the leak: clients are registered inside their first
// ChainSync callback, whose local processing the peer must not pay for.
func TestPatienceStartsPausedUntilFirstAcceptedHeader(t *testing.T) {
	t.Parallel()
	h := newPatienceHarness(t, chainsync.DefaultPatienceConfig())
	conn := newTestConnId(1)
	require.True(t, h.state.AddClientConnId(conn))
	h.advance(time.Hour)
	assert.Empty(t, h.state.CheckPatienceExhausted())
	h.deliver(conn, 1, farTip(), 0)
	assert.InDelta(t, chainsync.DefaultPatienceCapacity,
		h.state.GetTrackedClient(conn).Patience.Tokens, 1e-9)
}

// TestPatienceExhaustionClearsWhenGenesisEnds pins that a bucket which
// exhausted while Genesis selection was active is not reported for disconnect
// once selection has stopped before the recycler's tick: the Limit on Patience
// no longer applies, and the bucket is refilled for any later Genesis period.
func TestPatienceExhaustionClearsWhenGenesisEnds(t *testing.T) {
	t.Parallel()
	h := newPatienceHarness(t, chainsync.DefaultPatienceConfig())
	conn := newTestConnId(1)
	require.True(t, h.state.AddClientConnId(conn))
	h.deliver(conn, 1, farTip(), 0)

	// The next header's arrival latches exhaustion without a recycler tick.
	h.advance(201 * time.Second)
	require.True(t, h.state.PatienceMessageArrived(conn, h.now))
	require.True(t, h.state.GetTrackedClient(conn).Patience.Exhausted)

	h.active.Store(false)
	assert.Empty(t, h.state.CheckPatienceExhausted())
	tc := h.state.GetTrackedClient(conn)
	assert.False(t, tc.Patience.Exhausted)
	assert.InDelta(t, chainsync.DefaultPatienceCapacity, tc.Patience.Tokens, 1e-9)

	// A later Genesis period starts from a full bucket and can exhaust, and
	// be reported, again.
	h.active.Store(true)
	h.state.PatienceHeaderAccepted(conn, 2, false)
	h.advance(199 * time.Second)
	assert.Empty(t, h.state.CheckPatienceExhausted())
	h.advance(2 * time.Second)
	assert.Equal(t, []ouroboros.ConnectionId{conn},
		h.state.CheckPatienceExhausted())
}

// TestPatienceExhaustionClearsWhenClientBecomesObservabilityOnly pins that a
// client demoted to observability-only before the recycler's tick is not
// disconnected by the Limit on Patience, which does not apply to it.
func TestPatienceExhaustionClearsWhenClientBecomesObservabilityOnly(
	t *testing.T,
) {
	t.Parallel()
	h := newPatienceHarness(t, chainsync.DefaultPatienceConfig())
	conn := newTestConnId(1)
	require.True(t, h.state.AddClientConnId(conn))
	h.deliver(conn, 1, farTip(), 0)
	h.advance(201 * time.Second)
	require.True(t, h.state.PatienceMessageArrived(conn, h.now))
	require.True(t, h.state.GetTrackedClient(conn).Patience.Exhausted)

	require.True(t, h.state.SetClientObservabilityOnly(conn, true))
	assert.Empty(t, h.state.CheckPatienceExhausted())
	assert.False(t, h.state.GetTrackedClient(conn).Patience.Exhausted)
}
