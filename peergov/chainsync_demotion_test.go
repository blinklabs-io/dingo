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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var chainsyncDemotionReasons = []string{
	event.ConnectionCloseReasonPatienceExhausted,
	event.ConnectionCloseReasonEagernessStandoffLost,
}

func closePeerWithReason(pg *PeerGovernor, peer *Peer, reason string) {
	connId := outboundTestConnId()
	pg.mu.Lock()
	peer.Connection = &PeerConnection{Id: connId, IsClient: true}
	peer.State = PeerStateHot
	// The session outlived the stable-connection threshold, which on its own
	// resets the backoff to zero.
	peer.ConnectedAt = time.Now().Add(-10 * minStableConnectionDuration)
	pg.mu.Unlock()
	pg.handleConnectionClosedEvent(event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{
			ConnectionId:     connId,
			LocalCloseReason: reason,
		},
	))
}

// A peer closed for exhausting the Limit on Patience or losing a Limit on
// Eagerness standoff is not redialed at once after a long session, even when
// the hot pool is critically low: an immediate redial lets it pin the limit
// again with its first fork header.
func TestHandleConnectionClosedEvent_ChainsyncDemotionDelaysRedial(
	t *testing.T,
) {
	t.Parallel()
	for _, reason := range chainsyncDemotionReasons {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			pg := NewPeerGovernor(PeerGovernorConfig{
				Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			})
			peer := &Peer{
				Address:           "192.168.12.101:3003",
				NormalizedAddress: "192.168.12.101:3003",
				Source:            PeerSourceP2PGossip,
				// Suppress the reconnect goroutine; only the accounting is
				// checked.
				Reconnecting: true,
			}
			pg.mu.Lock()
			pg.peers = []*Peer{peer}
			pg.mu.Unlock()

			closePeerWithReason(pg, peer, reason)

			pg.mu.Lock()
			got := peer.ReconnectDelay
			stalledAt := peer.LastChainsyncStall
			pg.mu.Unlock()
			assert.GreaterOrEqual(t, got,
				chainsyncDemotionReconnectDelay+chainsyncDemotionMinJitter)
			assert.Less(t, got,
				chainsyncDemotionReconnectDelay+chainsyncDemotionMaxJitter)
			assert.False(t, stalledAt.IsZero(),
				"the demoted peer must rank behind alternates")
		})
	}
}

// An owner-requested close for any other reason after a long session keeps
// the stable-session reset.
func TestHandleConnectionClosedEvent_UnclassifiedLocalCloseResetsBackoff(
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
		Reconnecting:      true,
		ReconnectDelay:    8 * time.Second,
	}
	pg.mu.Lock()
	pg.peers = []*Peer{peer}
	pg.mu.Unlock()

	closePeerWithReason(pg, peer, "stalled_active_connection")

	pg.mu.Lock()
	defer pg.mu.Unlock()
	assert.Zero(t, peer.ReconnectDelay)
}

// A demoted peer's slot goes to a known alternate at once instead of waiting
// for the next reconcile.
func TestHandleConnectionClosedEvent_ChainsyncDemotionDialsColdAlternate(
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
	demoted := &Peer{
		Address:           "192.168.12.101:3003",
		NormalizedAddress: "192.168.12.101:3003",
		Source:            PeerSourceP2PGossip,
		Reconnecting:      true,
	}
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.peers = []*Peer{
		demoted,
		{
			// Nothing listens here; the dial fails fast and the reconnect
			// loop records the attempt.
			Address:           "127.0.0.1:1",
			NormalizedAddress: "127.0.0.1:1",
			Source:            PeerSourceP2PGossip,
			State:             PeerStateCold,
		},
	}
	pg.mu.Unlock()
	t.Cleanup(func() { _ = pg.Stop(context.Background()) })

	closePeerWithReason(
		pg,
		demoted,
		event.ConnectionCloseReasonEagernessStandoffLost,
	)

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
		"a cold alternate must be dialed when the hot peer is demoted",
	)
}
