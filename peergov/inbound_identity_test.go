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
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newInboundIdentityGovernor(t *testing.T) *PeerGovernor {
	t.Helper()
	return NewPeerGovernor(PeerGovernorConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		PromRegistry:    prometheus.NewRegistry(),
		InboundCooldown: 5 * time.Minute,
		DenyDuration:    time.Minute,
	})
}

func inboundArrival(t *testing.T, remote string) event.Event {
	t.Helper()
	localAddr, err := net.ResolveTCPAddr("tcp", "44.0.0.9:3001")
	require.NoError(t, err)
	remoteAddr, err := net.ResolveTCPAddr("tcp", remote)
	require.NoError(t, err)
	return event.Event{
		Type: connmanager.InboundConnectionEventType,
		Data: connmanager.InboundConnectionEvent{
			ConnectionId: ouroboros.ConnectionId{
				LocalAddr:  localAddr,
				RemoteAddr: remoteAddr,
			},
			LocalAddr:  localAddr,
			RemoteAddr: remoteAddr,
			NormalizedRemoteAddr: connmanager.NormalizePeerAddr(
				remoteAddr.String(),
			),
		},
	}
}

func deniedInboundCount(pg *PeerGovernor) float64 {
	return promtestutil.ToFloat64(
		pg.metrics.inboundLifecycle.WithLabelValues("denied"),
	)
}

// A peer denied on one source port stays denied when it reconnects from
// another: inbound source ports are ephemeral, so the host is the identity.
func TestInboundDenialSurvivesSourcePortRotation(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	first := inboundArrival(t, "44.0.0.1:51000")
	pg.handleInboundConnectionEvent(first)
	require.Equal(t, 1, len(pg.GetPeers()))
	// The first connection is still open, so its entry is not reused by the
	// arrivals below and only the host-wide denial can refuse them.
	pg.mu.Lock()
	pg.peers[0].Connection = &PeerConnection{
		Id:       first.Data.(connmanager.InboundConnectionEvent).ConnectionId,
		IsClient: true,
	}
	pg.mu.Unlock()

	pg.DenyPeer("44.0.0.1:51000", time.Minute)

	for port := 51001; port < 51006; port++ {
		pg.handleInboundConnectionEvent(
			inboundArrival(t, fmt.Sprintf("44.0.0.1:%d", port)),
		)
	}
	assert.Equal(
		t,
		1,
		len(pg.GetPeers()),
		"a denied host must not obtain fresh records from new ports",
	)
	assert.Equal(t, float64(5), deniedInboundCount(pg))

	pg.handleInboundConnectionEvent(inboundArrival(t, "44.0.0.2:51000"))
	assert.Equal(t, 2, len(pg.GetPeers()), "other hosts are unaffected")
}

// Denying a connection that an inbound arrival was matched to a configured
// topology peer applies to that topology peer, not to the source port.
func TestInboundTopologyDenialSurvivesSourcePortRotation(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	seedTopologyPeer(
		pg, "44.0.0.1:3001", "44.0.0.1:3001",
		"local-root-0", PeerSourceTopologyLocalRoot,
	)
	remote, err := net.ResolveTCPAddr("tcp", "44.0.0.1:51000")
	require.NoError(t, err)
	pg.mu.Lock()
	pg.peers[0].Connection = &PeerConnection{
		Id:       ouroboros.ConnectionId{RemoteAddr: remote},
		IsClient: true,
	}
	pg.mu.Unlock()

	pg.DenyPeer("44.0.0.1:51000", time.Minute)
	pg.handleInboundConnectionEvent(inboundArrival(t, "44.0.0.1:51001"))

	assert.Equal(t, float64(1), deniedInboundCount(pg))
}

// Short-lived sessions are counted against the host across source ports, and
// a host already flapping is refused rather than given a fresh record.
func TestInboundFlappingSurvivesSourcePortRotation(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address:                "44.0.0.1:51000",
		NormalizedAddress:      "44.0.0.1:51000",
		Source:                 PeerSourceInboundConn,
		State:                  PeerStateCold,
		FirstSeen:              time.Now().Add(-time.Hour),
		LastInboundDisconnect:  time.Now().Add(-5 * time.Second),
		InboundShortLivedCount: 1,
	})
	pg.mu.Unlock()

	// One short session so far: the new port inherits the record and its
	// history instead of starting from zero.
	pg.handleInboundConnectionEvent(inboundArrival(t, "44.0.0.1:51001"))
	peers := pg.GetPeers()
	require.Equal(t, 1, len(peers))
	assert.Equal(t, uint32(1), peers[0].InboundShortLivedCount)

	// Two short sessions inside the cooldown: every further arrival from the
	// host is refused, whichever port it uses.
	pg.mu.Lock()
	pg.peers[0].InboundShortLivedCount = 2
	pg.peers[0].LastInboundDisconnect = time.Now()
	pg.mu.Unlock()
	for port := 51002; port < 51007; port++ {
		pg.handleInboundConnectionEvent(
			inboundArrival(t, fmt.Sprintf("44.0.0.1:%d", port)),
		)
	}
	assert.Equal(t, 1, len(pg.GetPeers()))
	assert.Equal(t, float64(5), deniedInboundCount(pg))
}

// The flapping cooldown applied when a warm inbound peer is pruned must also
// hold for the host on a new source port.
func TestInboundFlappingPruneCooldownCoversHost(t *testing.T) {
	t.Parallel()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:          event.NewEventBus(nil, nil),
		PromRegistry:      prometheus.NewRegistry(),
		InboundPruneAfter: time.Minute,
		InboundCooldown:   2 * time.Minute,
		DenyDuration:      30 * time.Second,
	})
	now := time.Now()
	pg.mu.Lock()
	pg.peers = []*Peer{{
		Address:                "44.0.0.1:51000",
		NormalizedAddress:      "44.0.0.1:51000",
		Source:                 PeerSourceInboundConn,
		State:                  PeerStateWarm,
		FirstSeen:              now.Add(-time.Hour),
		LastActivity:           now.Add(-time.Hour),
		LastInboundDisconnect:  now.Add(-30 * time.Second),
		InboundShortLivedCount: 4,
	}}
	pg.mu.Unlock()
	pg.reconcile(t.Context())
	require.Equal(t, 0, len(pg.GetPeers()), "flapping peer is pruned")

	pg.handleInboundConnectionEvent(inboundArrival(t, "44.0.0.1:51001"))

	assert.Equal(t, 0, len(pg.GetPeers()))
	assert.Equal(t, float64(1), deniedInboundCount(pg))
}
