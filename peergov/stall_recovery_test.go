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

package peergov

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errChainsyncStall = errors.New(
	"protocol error: chain-sync: timeout waiting on transition from " +
		"protocol state MustReply",
)

func closeStalledPeer(pg *PeerGovernor, peer *Peer) {
	connId := outboundTestConnId()
	pg.mu.Lock()
	peer.Connection = &PeerConnection{Id: connId, IsClient: true}
	peer.State = PeerStateHot
	// The session outlived the stable-connection threshold: only the stall,
	// not a short session, can explain a penalty.
	peer.ConnectedAt = time.Now().Add(-10 * minStableConnectionDuration)
	pg.mu.Unlock()
	pg.handleConnectionClosedEvent(event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{
			ConnectionId: connId,
			Error:        errChainsyncStall,
		},
	))
}

// A peer that keeps stalling ChainSync is penalised with an escalating
// reconnect delay even though each session was long enough to count as stable.
func TestHandleConnectionClosedEvent_ChainsyncStallEscalatesReconnectDelay(
	t *testing.T,
) {
	t.Parallel()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	peer := &Peer{
		Address:           "192.168.12.101:3003",
		NormalizedAddress: "192.168.12.101:3003",
		Source:            PeerSourceP2PGossip,
		// Suppress the reconnect goroutine; only the accounting is checked.
		Reconnecting: true,
	}
	// Hot fillers keep the pool above the critically-low cap so the
	// escalation is visible.
	pg.mu.Lock()
	pg.peers = []*Peer{
		peer,
		{Address: "10.0.0.1:3001", State: PeerStateHot},
		{Address: "10.0.0.2:3001", State: PeerStateHot},
		{Address: "10.0.0.3:3001", State: PeerStateHot},
	}
	pg.mu.Unlock()

	for _, want := range []time.Duration{
		1 * time.Second, 2 * time.Second, 4 * time.Second,
	} {
		pg.mu.Lock()
		peer.ReconnectDelay = 0
		pg.mu.Unlock()
		closeStalledPeer(pg, peer)
		pg.mu.Lock()
		got := peer.ReconnectDelay
		pg.mu.Unlock()
		assert.Equal(t, want, got, "stall must back the peer off")
	}
}

// When the only hot peer stalls, a known cold alternate is dialed at once
// instead of waiting for the next reconcile.
func TestHandleConnectionClosedEvent_ChainsyncStallDialsColdAlternate(
	t *testing.T,
) {
	t.Parallel()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: logger,
		ConnManager: connmanager.NewConnectionManager(
			connmanager.ConnectionManagerConfig{Logger: logger},
		),
	})
	stalled := &Peer{
		Address:           "192.168.12.101:3003",
		NormalizedAddress: "192.168.12.101:3003",
		Source:            PeerSourceP2PGossip,
		Reconnecting:      true,
	}
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.peers = []*Peer{
		stalled,
		{
			// Nothing listens here; the dial fails fast and the
			// reconnect loop records the attempt.
			Address:           "127.0.0.1:1",
			NormalizedAddress: "127.0.0.1:1",
			Source:            PeerSourceP2PGossip,
			State:             PeerStateCold,
		},
	}
	pg.mu.Unlock()
	t.Cleanup(func() { _ = pg.Stop(context.Background()) })

	closeStalledPeer(pg, stalled)

	require.Eventually(
		t,
		func() bool {
			pg.mu.Lock()
			defer pg.mu.Unlock()
			if idx := pg.peerIndexByAddress("127.0.0.1:1"); idx != -1 {
				alternate := pg.peers[idx]
				return alternate.Reconnecting || alternate.ReconnectCount > 0
			}
			_, denied := pg.denyList["127.0.0.1:1"]
			return denied
		},
		5*time.Second,
		10*time.Millisecond,
		"a cold alternate must be dialed when the hot peer stalls",
	)
}

// Redial order puts a peer that recently stalled ChainSync behind every
// alternate, whatever its score.
func TestRedialRankCompare_RecentChainsyncStallRanksLast(t *testing.T) {
	t.Parallel()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	stalled := &Peer{
		Address:            "stalled:3001",
		PerformanceScore:   0.9,
		LastChainsyncStall: time.Now(),
	}
	alternate := &Peer{Address: "alternate:3001", PerformanceScore: 0.4}

	assert.Positive(t, pg.redialRankCompare(stalled, alternate))
	assert.Negative(t, pg.redialRankCompare(alternate, stalled))
}
