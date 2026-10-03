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
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const withholdTestNetworkMagic = 764824073

// withholdFixture is a governor whose connection manager holds one real
// full-duplex inbound connection from a seeded topology peer.
type withholdFixture struct {
	pg      *PeerGovernor
	eventCh <-chan event.Event
	connId  ouroboros.ConnectionId
	arrival event.Event
}

func newWithholdFixture(
	t *testing.T,
	source PeerSource,
) *withholdFixture {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	type result struct {
		conn *ouroboros.Connection
		err  error
	}
	serverCh := make(chan result, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			serverCh <- result{err: err}
			return
		}
		conn, err := ouroboros.New(
			ouroboros.WithConnection(raw),
			ouroboros.WithServer(true),
			ouroboros.WithNetworkMagic(withholdTestNetworkMagic),
			ouroboros.WithNodeToNode(true),
			ouroboros.WithFullDuplex(true),
		)
		serverCh <- result{conn: conn, err: err}
	}()
	raw, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	client, err := ouroboros.New(
		ouroboros.WithConnection(raw),
		ouroboros.WithNetworkMagic(withholdTestNetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithFullDuplex(true),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	var server result
	select {
	case server = <-serverCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the inbound handshake")
	}
	require.NoError(t, server.err)
	t.Cleanup(func() { _ = server.conn.Close() })

	connMgr := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	)
	require.True(t, connMgr.AddConnection(
		server.conn,
		true,
		server.conn.Id().RemoteAddr.String(),
	))
	eventBus := event.NewEventBus(nil, nil)
	t.Cleanup(eventBus.Stop)
	_, eventCh := eventBus.Subscribe(PeerEligibilityChangedEventType)
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:                  eventBus,
		PromRegistry:              prometheus.NewRegistry(),
		ConnManager:               connMgr,
		InboundCooldown:           5 * time.Minute,
		DenyDuration:              time.Minute,
		MinHotPeers:               1,
		TargetNumberOfActivePeers: 1,
	})
	host, _, err := net.SplitHostPort(
		server.conn.Id().RemoteAddr.String(),
	)
	require.NoError(t, err)
	seedTopologyPeer(
		pg, net.JoinHostPort(host, "3001"), net.JoinHostPort(host, "3001"),
		"group-0", source,
	)
	id := server.conn.Id()
	return &withholdFixture{
		pg:      pg,
		eventCh: eventCh,
		connId:  id,
		arrival: event.Event{
			Type: connmanager.InboundConnectionEventType,
			Data: connmanager.InboundConnectionEvent{
				ConnectionId: id,
				LocalAddr:    id.LocalAddr,
				RemoteAddr:   id.RemoteAddr,
				NormalizedRemoteAddr: connmanager.NormalizePeerAddr(
					id.RemoteAddr.String(),
				),
			},
		},
	}
}

func (f *withholdFixture) peerAddr() string {
	f.pg.mu.Lock()
	defer f.pg.mu.Unlock()
	return f.pg.peers[0].Address
}

// expireDenials moves every deny entry into the past.
func (f *withholdFixture) expireDenials() {
	f.pg.mu.Lock()
	defer f.pg.mu.Unlock()
	for k := range f.pg.denyList {
		f.pg.denyList[k] = time.Now().Add(-time.Second)
	}
}

// nextEligibility returns the next PeerEligibilityChanged event for the
// fixture's connection, blocking until one arrives.
func (f *withholdFixture) nextEligibility(
	t *testing.T,
) PeerEligibilityChangedEvent {
	t.Helper()
	select {
	case evt := <-f.eventCh:
		return evt.Data.(PeerEligibilityChangedEvent)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a PeerEligibilityChanged event")
	}
	return PeerEligibilityChangedEvent{}
}

// A denied topology peer that arrives full-duplex keeps its connection but is
// not a chain selection source. The handler is driven end to end so removing
// the withhold at the arrival call site fails this test.
func TestInboundArrivalOfDeniedTopologyPeerIsWithheld(t *testing.T) {
	t.Parallel()
	f := newWithholdFixture(t, PeerSourceTopologyLocalRoot)
	f.pg.DenyPeer(f.peerAddr(), time.Minute)

	f.pg.handleInboundConnectionEvent(f.arrival)

	require.NotNil(t, f.pg.GetPeers()[0].Connection, "connection stays open")
	assert.False(t, f.pg.IsChainSelectionEligible(f.connId))
	evt := f.nextEligibility(t)
	assert.False(t, evt.Eligible)
}

// Control: the same arrival without a denial is eligible.
func TestInboundArrivalOfUndeniedTopologyPeerIsEligible(t *testing.T) {
	t.Parallel()
	f := newWithholdFixture(t, PeerSourceTopologyLocalRoot)

	f.pg.handleInboundConnectionEvent(f.arrival)

	assert.True(t, f.pg.IsChainSelectionEligible(f.connId))
	assert.True(t, f.nextEligibility(t).Eligible)
}

// A withhold lasts only as long as the denial: once the denial expires the
// open connection is a chain selection source again, and reconcile announces
// the change.
func TestWithheldUpstreamRecoversWhenDenialExpires(t *testing.T) {
	t.Parallel()
	f := newWithholdFixture(t, PeerSourceTopologyLocalRoot)
	f.pg.DenyPeer(f.peerAddr(), time.Minute)
	f.pg.handleInboundConnectionEvent(f.arrival)
	require.False(t, f.nextEligibility(t).Eligible)
	require.False(t, f.pg.IsChainSelectionEligible(f.connId))

	f.expireDenials()

	assert.True(
		t,
		f.pg.IsChainSelectionEligible(f.connId),
		"eligibility is read from the current denial state",
	)
	f.pg.reconcile(t.Context())
	evt := f.nextEligibility(t)
	assert.True(t, evt.Eligible)
	assert.True(t, sameConnectionId(f.connId, evt.ConnectionId))
	assert.True(t, f.pg.IsChainSelectionEligible(f.connId))
}

// A denial that starts on an already-open connection withdraws eligibility
// immediately and says so.
func TestDenialOnOpenConnectionEmitsIneligible(t *testing.T) {
	t.Parallel()
	f := newWithholdFixture(t, PeerSourceTopologyLocalRoot)
	f.pg.handleInboundConnectionEvent(f.arrival)
	require.True(t, f.nextEligibility(t).Eligible)

	f.pg.DenyPeer(f.peerAddr(), time.Minute)

	assert.False(t, f.pg.IsChainSelectionEligible(f.connId))
	evt := f.nextEligibility(t)
	assert.False(t, evt.Eligible)
	assert.True(t, sameConnectionId(f.connId, evt.ConnectionId))
}

// A withheld connection is not reusable inbound topology demand, so the
// governor still dials the peer outbound; once the denial expires it is.
func TestWithheldConnectionIsNotReusableInboundTopology(t *testing.T) {
	t.Parallel()
	f := newWithholdFixture(t, PeerSourceTopologyLocalRoot)
	f.pg.DenyPeer(f.peerAddr(), time.Minute)
	f.pg.handleInboundConnectionEvent(f.arrival)

	f.pg.mu.Lock()
	withheld := f.pg.isReusableInboundTopologyConnectionLocked(f.pg.peers[0])
	f.pg.mu.Unlock()
	assert.False(t, withheld)

	f.expireDenials()
	f.pg.mu.Lock()
	recovered := f.pg.isReusableInboundTopologyConnectionLocked(f.pg.peers[0])
	f.pg.mu.Unlock()
	assert.True(t, recovered, "control: reusable once the denial expires")
}

// A withheld hot peer is not an upstream, so bootstrap recovery counts it as
// missing.
func TestBootstrapRecoveryDoesNotCountWithheldHotPeer(t *testing.T) {
	t.Parallel()
	f := newWithholdFixture(t, PeerSourceTopologyLocalRoot)
	f.pg.DenyPeer(f.peerAddr(), time.Minute)
	f.pg.handleInboundConnectionEvent(f.arrival)
	f.pg.mu.Lock()
	f.pg.peers[0].State = PeerStateHot
	f.pg.peers = append(f.pg.peers, &Peer{
		Address:           "45.0.0.1:3001",
		NormalizedAddress: "45.0.0.1:3001",
		Source:            PeerSourceTopologyBootstrapPeer,
		State:             PeerStateCold,
	})
	f.pg.bootstrapExited = true
	f.pg.mu.Unlock()

	f.pg.mu.Lock()
	f.pg.checkBootstrapRecoveryLocked()
	recovered := !f.pg.bootstrapExited
	f.pg.mu.Unlock()
	assert.True(t, recovered, "withheld hot peer must not satisfy MinHotPeers")
}

// Control for the test above: an eligible hot peer satisfies MinHotPeers.
func TestBootstrapRecoveryCountsEligibleHotPeer(t *testing.T) {
	t.Parallel()
	f := newWithholdFixture(t, PeerSourceTopologyLocalRoot)
	f.pg.handleInboundConnectionEvent(f.arrival)
	f.pg.mu.Lock()
	f.pg.peers[0].State = PeerStateHot
	f.pg.peers = append(f.pg.peers, &Peer{
		Address:           "45.0.0.1:3001",
		NormalizedAddress: "45.0.0.1:3001",
		Source:            PeerSourceTopologyBootstrapPeer,
		State:             PeerStateCold,
	})
	f.pg.bootstrapExited = true
	f.pg.checkBootstrapRecoveryLocked()
	stillExited := f.pg.bootstrapExited
	f.pg.mu.Unlock()
	assert.True(t, stillExited)
}

// A withheld warm peer is not promoted to hot; an eligible one is.
func TestWithheldWarmPeerIsNotPromoted(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		deny    bool
		wantHot bool
	}{
		{"withheld", true, false},
		{"eligible", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newWithholdFixture(t, PeerSourceTopologyLocalRoot)
			if tc.deny {
				f.pg.DenyPeer(f.peerAddr(), time.Minute)
			}
			f.pg.handleInboundConnectionEvent(f.arrival)

			f.pg.reconcile(t.Context())

			peers := f.pg.GetPeers()
			require.Equal(t, 1, len(peers))
			assert.Equal(t, tc.wantHot, peers[0].State == PeerStateHot)
		})
	}
}
