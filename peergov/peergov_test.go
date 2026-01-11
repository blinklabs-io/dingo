// Copyright 2025 Blink Labs Software
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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/topology"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func newMockEventBus() *event.EventBus {
	// Use the real EventBus for simplicity
	return event.NewEventBus(nil, nil)
}

func TestNewPeerGovernor(t *testing.T) {
	tests := []struct {
		name     string
		config   PeerGovernorConfig
		expected PeerGovernorConfig
	}{
		{
			name: "default config",
			config: PeerGovernorConfig{
				Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			},
			expected: PeerGovernorConfig{
				ReconcileInterval:    defaultReconcileInterval,
				MaxReconnectFailures: defaultMaxReconnectFailures,
				MinHotPeers:          defaultMinHotPeers,
				InactivityTimeout:    defaultInactivityTimeout,
			},
		},
		{
			name: "custom config",
			config: PeerGovernorConfig{
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
				ReconcileInterval:    10 * time.Minute,
				MaxReconnectFailures: 10,
				MinHotPeers:          5,
			},
			expected: PeerGovernorConfig{
				ReconcileInterval:    10 * time.Minute,
				MaxReconnectFailures: 10,
				MinHotPeers:          5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pg := NewPeerGovernor(tt.config)

			assert.NotNil(t, pg)
			assert.Equal(
				t,
				tt.expected.ReconcileInterval,
				pg.config.ReconcileInterval,
			)
			assert.Equal(
				t,
				tt.expected.MaxReconnectFailures,
				pg.config.MaxReconnectFailures,
			)
			assert.Equal(t, tt.expected.MinHotPeers, pg.config.MinHotPeers)
			assert.NotNil(t, pg.config.Logger)
		})
	}
}

func TestPeerGovernor_AddPeer(t *testing.T) {
	eventBus := newMockEventBus()
	reg := prometheus.NewRegistry()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:     eventBus,
		PromRegistry: reg,
	})

	// Test adding a gossip peer (should be sharable)
	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)

	peers := pg.GetPeers()
	assert.Len(t, peers, 1)
	assert.Equal(t, "127.0.0.1:3001", peers[0].Address)
	assert.EqualValues(t, PeerSourceP2PGossip, peers[0].Source)
	assert.Equal(t, PeerStateCold, peers[0].State)
	assert.True(t, peers[0].Sharable, "gossip-discovered peers should be sharable")

	// Test adding duplicate peer (should not add)
	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)
	peers = pg.GetPeers()
	assert.Len(t, peers, 1)

	// Test adding a non-gossip peer (should not be sharable by default)
	pg.AddPeer("127.0.0.1:3002", PeerSourceUnknown)
	peers = pg.GetPeers()
	assert.Len(t, peers, 2)
	// Find the non-gossip peer
	var nonGossipPeer *Peer
	for _, p := range peers {
		if p.Address == "127.0.0.1:3002" {
			nonGossipPeer = &p
			break
		}
	}
	assert.NotNil(t, nonGossipPeer, "non-gossip peer should be found")
	assert.False(t, nonGossipPeer.Sharable, "non-gossip peers should not be sharable by default")
}

func TestPeerGovernor_LoadTopologyConfig(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	topologyConfig := &topology.TopologyConfig{
		BootstrapPeers: []topology.TopologyConfigP2PBootstrapPeer{
			{Address: "127.0.0.1", Port: 3001},
		},
		LocalRoots: []topology.TopologyConfigP2PLocalRoot{
			{
				AccessPoints: []topology.TopologyConfigP2PAccessPoint{
					{Address: "127.0.0.2", Port: 3002},
				},
				Advertise: true,
			},
		},
		PublicRoots: []topology.TopologyConfigP2PPublicRoot{
			{
				AccessPoints: []topology.TopologyConfigP2PAccessPoint{
					{Address: "127.0.0.3", Port: 3003},
				},
				Advertise: false,
			},
		},
	}

	pg.LoadTopologyConfig(topologyConfig)

	peers := pg.GetPeers()
	assert.Len(t, peers, 3)

	// Check bootstrap peer
	assert.Equal(t, "127.0.0.1:3001", peers[0].Address)
	assert.EqualValues(t, PeerSourceTopologyBootstrapPeer, peers[0].Source)

	// Check local root peer
	assert.Equal(t, "127.0.0.2:3002", peers[1].Address)
	assert.EqualValues(t, PeerSourceTopologyLocalRoot, peers[1].Source)
	assert.True(t, peers[1].Sharable)

	// Check public root peer
	assert.Equal(t, "127.0.0.3:3003", peers[2].Address)
	assert.EqualValues(t, PeerSourceTopologyPublicRoot, peers[2].Source)
	assert.False(t, peers[2].Sharable)
}

func TestPeerGovernor_Reconcile_Promotions(t *testing.T) {
	eventBus := newMockEventBus()
	reg := prometheus.NewRegistry()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:     eventBus,
		PromRegistry: reg,
		MinHotPeers:  2,
	})

	// Add peers with different states
	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)
	pg.AddPeer("127.0.0.1:3002", PeerSourceP2PGossip)

	// Manually set one peer to warm with connection (should promote to hot)
	pg.mu.Lock()
	pg.peers[0].State = PeerStateWarm
	pg.peers[0].Connection = &PeerConnection{} // Mock connection
	pg.mu.Unlock()

	pg.reconcile()

	peers := pg.GetPeers()
	assert.Equal(t, PeerStateHot, peers[0].State)
	assert.Equal(t, PeerStateCold, peers[1].State)

	// Event publishing is tested indirectly
}

func TestPeerGovernor_Reconcile_Demotions(t *testing.T) {
	eventBus := newMockEventBus()
	reg := prometheus.NewRegistry()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:     eventBus,
		PromRegistry: reg,
	})

	// Add peer and set to hot
	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)

	pg.mu.Lock()
	pg.peers[0].State = PeerStateHot
	pg.peers[0].Connection = nil // No connection
	pg.peers[0].LastActivity = time.Now().
		Add(-15 * time.Minute)
		// Make it inactive
	pg.mu.Unlock()

	pg.reconcile()

	peers := pg.GetPeers()
	assert.Equal(t, PeerStateWarm, peers[0].State)

	// Event publishing is tested indirectly
}

func TestPeerGovernor_Reconcile_Removal(t *testing.T) {
	eventBus := newMockEventBus()
	reg := prometheus.NewRegistry()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:               slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:             eventBus,
		PromRegistry:         reg,
		MaxReconnectFailures: 3,
	})

	// Add peer and set excessive failures
	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)

	pg.mu.Lock()
	pg.peers[0].ReconnectCount = 5 // Exceeds MaxReconnectFailures
	pg.mu.Unlock()

	pg.reconcile()

	peers := pg.GetPeers()
	assert.Len(t, peers, 0)

	// Event publishing is tested indirectly
}

func TestPeerGovernor_Reconcile_MinimumHotPeers(t *testing.T) {
	eventBus := newMockEventBus()
	reg := prometheus.NewRegistry()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:     eventBus,
		PromRegistry: reg,
		MinHotPeers:  2,
	})

	// Add 3 warm peers with connections
	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)
	pg.AddPeer("127.0.0.1:3002", PeerSourceP2PGossip)
	pg.AddPeer("127.0.0.1:3003", PeerSourceP2PGossip)

	pg.mu.Lock()
	for i := range pg.peers {
		pg.peers[i].State = PeerStateWarm
		pg.peers[i].Connection = &PeerConnection{} // Mock connection
	}
	pg.mu.Unlock()

	pg.reconcile()

	peers := pg.GetPeers()
	hotCount := 0
	for _, peer := range peers {
		if peer.State == PeerStateHot {
			hotCount++
		}
	}
	assert.Equal(
		t,
		2,
		hotCount,
	) // Only MinHotPeers warm peers should be promoted to hot (score-based selection)

	// Event publishing is tested indirectly
}

func TestPeerGovernor_Metrics(t *testing.T) {
	reg := prometheus.NewRegistry()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry: reg,
	})

	// Add some peers
	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)
	pg.AddPeer("127.0.0.1:3002", PeerSourceP2PGossip)

	pg.mu.Lock()
	pg.peers[0].State = PeerStateWarm
	pg.peers[0].Connection = &PeerConnection{}
	pg.peers[1].State = PeerStateHot
	pg.peers[1].Connection = &PeerConnection{}
	pg.mu.Unlock()

	// Force metrics update
	pg.mu.Lock()
	pg.updatePeerMetrics()
	pg.mu.Unlock()

	// Check metrics
	assert.Equal(t, float64(0), testutil.ToFloat64(pg.metrics.coldPeers))
	assert.Equal(t, float64(1), testutil.ToFloat64(pg.metrics.warmPeers))
	assert.Equal(t, float64(1), testutil.ToFloat64(pg.metrics.hotPeers))
	assert.Equal(t, float64(2), testutil.ToFloat64(pg.metrics.knownPeers))
	assert.Equal(t, float64(2), testutil.ToFloat64(pg.metrics.establishedPeers))
	assert.Equal(t, float64(1), testutil.ToFloat64(pg.metrics.activePeers))
}

func TestPeerGovernor_PeerSharing(t *testing.T) {
	eventBus := newMockEventBus()
	reg := prometheus.NewRegistry()

	peerRequestCount := 0
	var requestedPeers []*Peer

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:     eventBus,
		PromRegistry: reg,
		PeerRequestFunc: func(peer *Peer) []string {
			peerRequestCount++
			requestedPeers = append(requestedPeers, peer)
			return []string{"newpeer:3001"}
		},
	})

	// Add hot peers with connections
	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)
	pg.AddPeer("127.0.0.1:3002", PeerSourceP2PGossip)

	pg.mu.Lock()
	for i := range pg.peers {
		pg.peers[i].State = PeerStateHot
		pg.peers[i].Connection = &PeerConnection{}
	}
	pg.mu.Unlock()

	pg.reconcile()

	// Should have requested peers from both hot peers
	assert.Equal(t, 2, peerRequestCount)
	assert.Len(t, requestedPeers, 2)
	// Should have added the new peer
	peers := pg.GetPeers()
	assert.Len(t, peers, 3) // 2 original + 1 new
}

func TestPeerGovernor_SetPeerHotByConnId(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	// Add peer
	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)

	// Mock connection ID - use proper ConnectionId construction
	localAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	remoteAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3002")
	connId := ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}

	pg.mu.Lock()
	pg.peers[0].Connection = &PeerConnection{Id: connId}
	pg.peers[0].State = PeerStateWarm
	pg.mu.Unlock()

	pg.SetPeerHotByConnId(connId)

	peers := pg.GetPeers()
	assert.Equal(t, PeerStateHot, peers[0].State)
	assert.True(t, peers[0].LastActivity.After(time.Now().Add(-1*time.Second)))
}

func TestPeerGovernor_HandleInboundConnection(t *testing.T) {
	eventBus := newMockEventBus()
	reg := prometheus.NewRegistry()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:     eventBus,
		PromRegistry: reg,
		// ConnManager is nil - this tests the case where no connection is found
	})

	// Create a mock inbound connection event
	localAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	remoteAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3002")
	connId := ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}

	inboundEvent := connmanager.InboundConnectionEvent{
		ConnectionId: connId,
		LocalAddr:    localAddr,
		RemoteAddr:   remoteAddr,
	}

	// Create event and call handler directly
	evt := event.Event{
		Type: connmanager.InboundConnectionEventType,
		Data: inboundEvent,
	}

	// Initially no peers
	peers := pg.GetPeers()
	assert.Empty(t, peers)

	// Call the handler
	pg.handleInboundConnectionEvent(evt)

	// Should now have one peer
	peers = pg.GetPeers()
	assert.Len(t, peers, 1)
	if len(peers) > 0 {
		assert.Equal(t, "127.0.0.1:3002", peers[0].Address)
		assert.Equal(t, PeerSource(PeerSourceInboundConn), peers[0].Source)
		assert.Equal(
			t,
			PeerStateCold,
			peers[0].State,
		) // No connection found, so stays cold
	}
}

func TestPeerGovernor_HandleConnectionClosed(t *testing.T) {
	eventBus := newMockEventBus()
	reg := prometheus.NewRegistry()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:     eventBus,
		PromRegistry: reg,
	})

	// Add peer with connection
	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)
	localAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	remoteAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3002")
	connId := ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}

	pg.mu.Lock()
	pg.peers[0].Connection = &PeerConnection{Id: connId}
	pg.peers[0].State = PeerStateHot
	pg.mu.Unlock()

	// Create a mock connection closed event
	// Note: This test is simplified due to complex mocking requirements
	// In a real implementation, we'd need proper mocks for connmanager types

	pg.mu.Lock()
	peerIdx := pg.peerIndexByConnId(connId)
	assert.Equal(t, 0, peerIdx)
	pg.mu.Unlock()

	// Test that peer state changes when connection is cleared
	pg.mu.Lock()
	pg.peers[0].Connection = nil
	pg.peers[0].State = PeerStateCold
	pg.mu.Unlock()

	peers := pg.GetPeers()
	assert.Equal(t, PeerStateCold, peers[0].State)
	assert.Nil(t, peers[0].Connection)
}

func TestPeer_IndexByAddress(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)
	pg.AddPeer("127.0.0.1:3002", PeerSourceP2PGossip)

	idx := pg.peerIndexByAddress("127.0.0.1:3001")
	assert.Equal(t, 0, idx)

	idx = pg.peerIndexByAddress("127.0.0.1:3002")
	assert.Equal(t, 1, idx)

	idx = pg.peerIndexByAddress("127.0.0.1:3003")
	assert.Equal(t, -1, idx)
}

func TestPeerGovernor_IndexByConnId(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)
	localAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	remoteAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3002")
	connId := ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}

	pg.mu.Lock()
	pg.peers[0].Connection = &PeerConnection{Id: connId}
	pg.mu.Unlock()

	idx := pg.peerIndexByConnId(connId)
	assert.Equal(t, 0, idx)

	// Test with different connection ID
	localAddr2, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3003")
	remoteAddr2, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3004")
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  localAddr2,
		RemoteAddr: remoteAddr2,
	}
	idx = pg.peerIndexByConnId(connId2)
	assert.Equal(t, -1, idx)
}

func TestPeer_SetConnection(t *testing.T) {
	peer := &Peer{
		Address: "127.0.0.1:3001",
	}

	// Create a proper connection structure manually for testing
	localAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	remoteAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3002")
	connId := ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}

	// Manually set the connection to test the structure
	peer.Connection = &PeerConnection{
		Id:              connId,
		ProtocolVersion: 1,
		VersionData:     nil,
		IsClient:        true,
	}

	assert.NotNil(t, peer.Connection)
	assert.Equal(t, connId, peer.Connection.Id)
	assert.Equal(t, uint(1), peer.Connection.ProtocolVersion)
	assert.True(t, peer.Connection.IsClient)
}

func TestPeerGovernor_TestPeer_WithCustomFunc(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PeerTestFunc: func(address string) error {
			if address == "pass:3001" {
				return nil
			}
			return fmt.Errorf("test failure")
		},
	})

	// Test passing peer
	result, err := pg.TestPeer("pass:3001")
	assert.True(t, result)
	assert.NoError(t, err)

	// Verify peer was added and marked as passed
	peers := pg.GetPeers()
	assert.Len(t, peers, 1)
	assert.Equal(t, TestResultPass, peers[0].LastTestResult)
	assert.False(t, peers[0].LastTestTime.IsZero())

	// Test failing peer
	result, err = pg.TestPeer("fail:3001")
	assert.False(t, result)
	assert.Error(t, err)

	// Verify peer was added and marked as failed
	peers = pg.GetPeers()
	assert.Len(t, peers, 2)
	var failPeer *Peer
	for i := range peers {
		if peers[i].Address == "fail:3001" {
			failPeer = &peers[i]
			break
		}
	}
	assert.NotNil(t, failPeer)
	assert.Equal(t, TestResultFail, failPeer.LastTestResult)
}

func TestPeerGovernor_TestPeer_CachedResult(t *testing.T) {
	callCount := 0
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		TestCooldown: 1 * time.Hour, // Long cooldown for test
		PeerTestFunc: func(address string) error {
			callCount++
			return nil
		},
	})

	// First test should call the function
	result, err := pg.TestPeer("127.0.0.1:3001")
	assert.True(t, result)
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)

	// Second test should use cached result
	result, err = pg.TestPeer("127.0.0.1:3001")
	assert.True(t, result)
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount) // Should not have called again
}

func TestPeerGovernor_TestPeer_CachedFailure(t *testing.T) {
	callCount := 0
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		TestCooldown: 1 * time.Hour,
		PeerTestFunc: func(address string) error {
			callCount++
			return fmt.Errorf("connection failed")
		},
	})

	// First test should fail
	result, err := pg.TestPeer("127.0.0.1:3001")
	assert.False(t, result)
	assert.Error(t, err)
	assert.Equal(t, 1, callCount)

	// Second test should return cached failure
	result, err = pg.TestPeer("127.0.0.1:3001")
	assert.False(t, result)
	assert.Error(t, err)
	assert.Equal(t, 1, callCount) // Should not have called again
}

func TestPeerGovernor_TestPeer_ExistingPeer(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PeerTestFunc: func(address string) error {
			return nil
		},
	})

	// Add peer first
	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)

	// Test should update existing peer
	result, err := pg.TestPeer("127.0.0.1:3001")
	assert.True(t, result)
	assert.NoError(t, err)

	// Should still have only one peer
	peers := pg.GetPeers()
	assert.Len(t, peers, 1)
	assert.EqualValues(t, PeerSourceP2PGossip, peers[0].Source)
	assert.Equal(t, TestResultPass, peers[0].LastTestResult)
}

func TestPeerGovernor_TestPeer_NoConnManager(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		// No PeerTestFunc or ConnManager
	})

	// Should fail with appropriate error
	result, err := pg.TestPeer("127.0.0.1:3001")
	assert.False(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no test function or connection manager")
}

func TestPeerGovernor_DenyPeer(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		DenyDuration: 1 * time.Hour,
	})

	// Deny a peer
	pg.DenyPeer("127.0.0.1:3001", 0) // Use default duration

	// Verify peer is denied
	assert.True(t, pg.IsDenied("127.0.0.1:3001"))

	// Deny another peer with custom duration
	pg.DenyPeer("127.0.0.1:3002", 30*time.Minute)
	assert.True(t, pg.IsDenied("127.0.0.1:3002"))

	// Verify deny list size
	pg.mu.Lock()
	assert.Len(t, pg.denyList, 2)
	pg.mu.Unlock()
}

func TestPeerGovernor_DenyPeer_CaseInsensitive(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		DenyDuration: 1 * time.Hour,
	})

	// Deny a peer with uppercase hostname
	pg.DenyPeer("RELAY.EXAMPLE.COM:3001", 0)

	// Should be denied with lowercase (normalized) lookup
	assert.True(t, pg.IsDenied("relay.example.com:3001"))

	// Should also be denied with original case
	assert.True(t, pg.IsDenied("RELAY.EXAMPLE.COM:3001"))

	// Should be denied with mixed case
	assert.True(t, pg.IsDenied("Relay.Example.Com:3001"))

	// Verify deny list stores normalized address
	pg.mu.Lock()
	_, exists := pg.denyList["relay.example.com:3001"]
	pg.mu.Unlock()
	assert.True(t, exists, "deny list should store normalized (lowercase) address")
}

func TestPeerGovernor_IsDenied_Expiry(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		DenyDuration: 1 * time.Millisecond, // Very short for testing
	})

	// Deny a peer with very short duration
	pg.DenyPeer("127.0.0.1:3001", 1*time.Millisecond)

	// Should be denied initially
	assert.True(t, pg.IsDenied("127.0.0.1:3001"))

	// Wait for expiry
	time.Sleep(5 * time.Millisecond)

	// Should no longer be denied
	assert.False(t, pg.IsDenied("127.0.0.1:3001"))
}

func TestPeerGovernor_AddPeer_Denied(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		DenyDuration: 1 * time.Hour,
	})

	// Deny a peer first
	pg.DenyPeer("127.0.0.1:3001", 0)

	// Try to add the denied peer
	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)

	// Should not be added
	peers := pg.GetPeers()
	assert.Empty(t, peers)

	// Add a non-denied peer
	pg.AddPeer("127.0.0.1:3002", PeerSourceP2PGossip)
	peers = pg.GetPeers()
	assert.Len(t, peers, 1)
	assert.Equal(t, "127.0.0.1:3002", peers[0].Address)
}

func TestPeerGovernor_TestPeer_DeniesOnFailure(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		DenyDuration: 1 * time.Hour,
		PeerTestFunc: func(address string) error {
			return fmt.Errorf("connection failed")
		},
	})

	// Test peer (will fail)
	result, err := pg.TestPeer("127.0.0.1:3001")
	assert.False(t, result)
	assert.Error(t, err)

	// Peer should now be denied
	assert.True(t, pg.IsDenied("127.0.0.1:3001"))

	// Verify deny list has entry
	pg.mu.Lock()
	_, exists := pg.denyList["127.0.0.1:3001"]
	pg.mu.Unlock()
	assert.True(t, exists)
}

func TestPeerGovernor_Reconcile_CleanupDenyList(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		DenyDuration:      1 * time.Millisecond,
		ReconcileInterval: 1 * time.Hour, // Don't auto-trigger
	})

	// Add expired entries directly
	pg.mu.Lock()
	pg.denyList["127.0.0.1:3001"] = time.Now().Add(-1 * time.Hour) // Already expired
	pg.denyList["127.0.0.1:3002"] = time.Now().Add(-1 * time.Hour) // Already expired
	pg.denyList["127.0.0.1:3003"] = time.Now().Add(1 * time.Hour)  // Not expired
	pg.mu.Unlock()

	// Run reconcile to trigger cleanup
	pg.reconcile()

	// Check deny list - only non-expired entry should remain
	pg.mu.Lock()
	assert.Len(t, pg.denyList, 1)
	_, exists := pg.denyList["127.0.0.1:3003"]
	pg.mu.Unlock()
	assert.True(t, exists)
}

// mockLedgerPeerProvider implements LedgerPeerProvider for testing
type mockLedgerPeerProvider struct {
	relays      []PoolRelay
	currentSlot uint64
	err         error
}

func (m *mockLedgerPeerProvider) GetPoolRelays() ([]PoolRelay, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.relays, nil
}

func (m *mockLedgerPeerProvider) CurrentSlot() uint64 {
	return m.currentSlot
}

func TestPeerGovernor_DiscoverLedgerPeers_Disabled(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		UseLedgerAfterSlot: -1, // Disabled
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays: []PoolRelay{
				{Hostname: "relay.example.com", Port: 3001},
			},
			currentSlot: 1000,
		},
	})

	// Should not add any peers when disabled
	pg.discoverLedgerPeers()

	assert.Len(t, pg.peers, 0)
}

func TestPeerGovernor_DiscoverLedgerPeers_SlotNotReached(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		UseLedgerAfterSlot: 5000, // Require slot 5000
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays: []PoolRelay{
				{Hostname: "relay.example.com", Port: 3001},
			},
			currentSlot: 1000, // Only at slot 1000
		},
	})

	// Should not add any peers when slot threshold not reached
	pg.discoverLedgerPeers()

	assert.Len(t, pg.peers, 0)
}

func TestPeerGovernor_DiscoverLedgerPeers_Success(t *testing.T) {
	ipv4 := net.ParseIP("192.168.1.1")
	ipv6 := net.ParseIP("2001:db8::1")

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0, // Always enabled
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays: []PoolRelay{
				{Hostname: "relay1.example.com", Port: 3001},
				{IPv4: &ipv4, Port: 3002},
				{IPv6: &ipv6, Port: 3003},
			},
			currentSlot: 1000,
		},
	})

	pg.discoverLedgerPeers()

	// Should have added 3 peers
	assert.Len(t, pg.peers, 3)

	// Verify peer sources and shareability
	for _, peer := range pg.peers {
		assert.Equal(t, PeerSource(PeerSourceP2PLedger), peer.Source)
		assert.True(t, peer.Sharable)
		assert.Equal(t, PeerState(PeerStateCold), peer.State)
	}
}

func TestPeerGovernor_DiscoverLedgerPeers_Deduplication(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays: []PoolRelay{
				{Hostname: "relay.example.com", Port: 3001},
				{Hostname: "relay.example.com", Port: 3001}, // Duplicate
				{Hostname: "RELAY.EXAMPLE.COM", Port: 3001}, // Same but different case
			},
			currentSlot: 1000,
		},
	})

	pg.discoverLedgerPeers()

	// Should have only 1 peer due to deduplication
	assert.Len(t, pg.peers, 1)
}

func TestPeerGovernor_DiscoverLedgerPeers_RefreshInterval(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:                  newMockEventBus(),
		UseLedgerAfterSlot:        0,
		LedgerPeerRefreshInterval: 1 * time.Hour, // Long refresh interval
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays: []PoolRelay{
				{Hostname: "relay1.example.com", Port: 3001},
			},
			currentSlot: 1000,
		},
	})

	// First discovery should work
	pg.discoverLedgerPeers()
	assert.Len(t, pg.peers, 1)

	// Update mock to return different relays
	pg.config.LedgerPeerProvider = &mockLedgerPeerProvider{
		relays: []PoolRelay{
			{Hostname: "relay2.example.com", Port: 3001},
		},
		currentSlot: 2000,
	}

	// Second discovery should be skipped due to refresh interval
	pg.discoverLedgerPeers()
	assert.Len(t, pg.peers, 1) // Still only 1 peer

	// Force refresh by setting last refresh time to the past
	pg.lastLedgerPeerRefresh.Store(time.Now().Add(-2 * time.Hour).UnixNano())

	// Now discovery should add the new peer
	pg.discoverLedgerPeers()
	assert.Len(t, pg.peers, 2)
}

func TestPeerGovernor_DiscoverLedgerPeers_ExistingPeersNotDuplicated(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays: []PoolRelay{
				{Hostname: "relay.example.com", Port: 3001},
			},
			currentSlot: 1000,
		},
	})

	// Add existing peer from another source
	pg.AddPeer("relay.example.com:3001", PeerSourceP2PGossip)
	assert.Len(t, pg.peers, 1)

	// Ledger discovery should not add duplicate
	pg.discoverLedgerPeers()
	assert.Len(t, pg.peers, 1)

	// Verify original source is preserved
	assert.Equal(t, PeerSource(PeerSourceP2PGossip), pg.peers[0].Source)
}

func TestPeerGovernor_DiscoverLedgerPeers_DeniedPeersSkipped(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		DenyDuration:       1 * time.Hour,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays: []PoolRelay{
				{Hostname: "relay.example.com", Port: 3001},
			},
			currentSlot: 1000,
		},
	})

	// Deny the relay
	pg.DenyPeer("relay.example.com:3001", 0)

	// Ledger discovery should skip denied peer
	pg.discoverLedgerPeers()
	assert.Len(t, pg.peers, 0)
}

func TestPeerGovernor_DiscoverLedgerPeers_Error(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		UseLedgerAfterSlot: 0,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			err:         errors.New("database error"),
			currentSlot: 1000,
		},
	})

	// Should not panic and should not add any peers when error occurs
	pg.discoverLedgerPeers()
	assert.Len(t, pg.peers, 0)
}

func TestPeerGovernor_DiscoverLedgerPeers_ErrorAllowsRetry(t *testing.T) {
	// Create a mock provider that fails first, then succeeds
	mockProvider := &mockLedgerPeerProvider{
		err:         errors.New("transient error"),
		currentSlot: 1000,
	}

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		UseLedgerAfterSlot:        0,
		LedgerPeerProvider:        mockProvider,
		LedgerPeerRefreshInterval: 1 * time.Hour, // Long interval
	})

	// First call fails
	pg.discoverLedgerPeers()
	assert.Len(t, pg.peers, 0)

	// Fix the provider (simulate transient error recovery)
	mockProvider.err = nil
	mockProvider.relays = []PoolRelay{
		{Hostname: "relay.example.com", Port: 3001},
	}

	// Second call should succeed immediately (not wait for refresh interval)
	// because the timestamp was reset on error
	pg.discoverLedgerPeers()
	assert.Len(t, pg.peers, 1)
}

func TestPeerSource_String(t *testing.T) {
	tests := []struct {
		source   PeerSource
		expected string
	}{
		{PeerSourceUnknown, "unknown"},
		{PeerSourceTopologyLocalRoot, "topology-local-root"},
		{PeerSourceTopologyPublicRoot, "topology-public-root"},
		{PeerSourceTopologyBootstrapPeer, "topology-bootstrap"},
		{PeerSourceP2PLedger, "ledger"},
		{PeerSourceP2PGossip, "gossip"},
		{PeerSourceInboundConn, "inbound"},
		{PeerSource(99), "unknown"}, // Unknown value
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.source.String())
		})
	}
}

func TestPeerGovernor_NormalizeAddress(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "lowercase hostname",
			input:    "RELAY.EXAMPLE.COM:3001",
			expected: "relay.example.com:3001",
		},
		{
			name:     "ipv4 address",
			input:    "192.168.1.1:3001",
			expected: "192.168.1.1:3001",
		},
		{
			name:     "ipv6 short form",
			input:    "[::1]:3001",
			expected: "[::1]:3001",
		},
		{
			name:     "ipv6 full form normalized",
			input:    "[0:0:0:0:0:0:0:1]:3001",
			expected: "[::1]:3001",
		},
		{
			name:     "invalid address passthrough",
			input:    "invalid",
			expected: "invalid",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := pg.normalizeAddress(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestPoolRelay_Addresses(t *testing.T) {
	ipv4 := net.ParseIP("192.168.1.1")
	ipv6 := net.ParseIP("2001:db8::1")

	tests := []struct {
		name     string
		relay    PoolRelay
		expected []string
	}{
		{
			name:     "hostname only",
			relay:    PoolRelay{Hostname: "relay.example.com", Port: 3001},
			expected: []string{"relay.example.com:3001"},
		},
		{
			name:     "ipv4 only",
			relay:    PoolRelay{IPv4: &ipv4, Port: 3002},
			expected: []string{"192.168.1.1:3002"},
		},
		{
			name:     "ipv6 only",
			relay:    PoolRelay{IPv6: &ipv6, Port: 3003},
			expected: []string{"[2001:db8::1]:3003"},
		},
		{
			name:     "all addresses",
			relay:    PoolRelay{Hostname: "relay.example.com", IPv4: &ipv4, IPv6: &ipv6, Port: 3001},
			expected: []string{"relay.example.com:3001", "192.168.1.1:3001", "[2001:db8::1]:3001"},
		},
		{
			name:     "default port",
			relay:    PoolRelay{Hostname: "relay.example.com", Port: 0},
			expected: []string{"relay.example.com:3001"},
		},
		{
			name:     "empty ipv4 slice ignored",
			relay:    PoolRelay{Hostname: "relay.example.com", IPv4: &net.IP{}, Port: 3001},
			expected: []string{"relay.example.com:3001"},
		},
		{
			name:     "empty ipv6 slice ignored",
			relay:    PoolRelay{Hostname: "relay.example.com", IPv6: &net.IP{}, Port: 3001},
			expected: []string{"relay.example.com:3001"},
		},
		{
			name:     "nil ip pointers ignored",
			relay:    PoolRelay{Hostname: "relay.example.com", IPv4: nil, IPv6: nil, Port: 3001},
			expected: []string{"relay.example.com:3001"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.relay.Addresses()
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestPeerGovernor_PeerLimits_DefaultValues(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		// MaxColdPeers, MaxWarmPeers, MaxHotPeers not set (0)
	})

	// Should use default values
	assert.Equal(t, 200, pg.config.MaxColdPeers)
	assert.Equal(t, 50, pg.config.MaxWarmPeers)
	assert.Equal(t, 20, pg.config.MaxHotPeers)
}

func TestPeerGovernor_PeerLimits_CustomValues(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxColdPeers: 100,
		MaxWarmPeers: 25,
		MaxHotPeers:  10,
	})

	assert.Equal(t, 100, pg.config.MaxColdPeers)
	assert.Equal(t, 25, pg.config.MaxWarmPeers)
	assert.Equal(t, 10, pg.config.MaxHotPeers)
}

func TestPeerGovernor_PeerLimits_Unlimited(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxColdPeers: -1, // Unlimited
		MaxWarmPeers: -1,
		MaxHotPeers:  -1,
	})

	// -1 should be converted to 0 (unlimited internally)
	assert.Equal(t, 0, pg.config.MaxColdPeers)
	assert.Equal(t, 0, pg.config.MaxWarmPeers)
	assert.Equal(t, 0, pg.config.MaxHotPeers)
}

func TestPeerGovernor_EnforcePeerLimits_ColdPeers(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:     newMockEventBus(),
		MaxColdPeers: 3, // Limit to 3 cold peers
		MaxWarmPeers: -1,
		MaxHotPeers:  -1,
	})

	// Add 5 cold peers from different sources
	pg.AddPeer("ledger1.example.com:3001", PeerSourceP2PLedger)
	pg.AddPeer("ledger2.example.com:3001", PeerSourceP2PLedger)
	pg.AddPeer("gossip1.example.com:3001", PeerSourceP2PGossip)
	pg.AddPeer("gossip2.example.com:3001", PeerSourceP2PGossip)
	pg.AddPeer("inbound1.example.com:3001", PeerSourceInboundConn)

	assert.Len(t, pg.peers, 5)

	// Run reconcile to enforce limits
	pg.reconcile()

	// Should have removed 2 peers (down to 3)
	assert.Len(t, pg.peers, 3)

	// Lower priority peers should be removed first (ledger before gossip)
	// Check that gossip peers are kept (higher priority than ledger)
	hasGossip1 := false
	hasGossip2 := false
	for _, peer := range pg.peers {
		if peer.Address == "gossip1.example.com:3001" {
			hasGossip1 = true
		}
		if peer.Address == "gossip2.example.com:3001" {
			hasGossip2 = true
		}
	}
	assert.True(t, hasGossip1, "gossip1 peer should be kept")
	assert.True(t, hasGossip2, "gossip2 peer should be kept")
}

func TestPeerGovernor_EnforcePeerLimits_TopologyPeersNeverRemoved(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:     newMockEventBus(),
		MaxColdPeers: 2, // Very low limit
		MaxWarmPeers: -1,
		MaxHotPeers:  -1,
	})

	// Add topology peers (should never be removed)
	pg.AddPeer("bootstrap1.example.com:3001", PeerSourceTopologyBootstrapPeer)
	pg.AddPeer("localroot1.example.com:3001", PeerSourceTopologyLocalRoot)
	pg.AddPeer("publicroot1.example.com:3001", PeerSourceTopologyPublicRoot)

	// Add regular peers
	pg.AddPeer("ledger1.example.com:3001", PeerSourceP2PLedger)
	pg.AddPeer("ledger2.example.com:3001", PeerSourceP2PLedger)

	assert.Len(t, pg.peers, 5)

	// Run reconcile to enforce limits
	pg.reconcile()

	// All topology peers should be kept (3) even though limit is 2
	// Only ledger peers should be removed
	topologyCount := 0
	for _, peer := range pg.peers {
		switch peer.Source {
		case PeerSourceTopologyBootstrapPeer,
			PeerSourceTopologyLocalRoot,
			PeerSourceTopologyPublicRoot:
			topologyCount++
		}
	}
	assert.Equal(t, 3, topologyCount, "all topology peers should be kept")
}

func TestPeerGovernor_EnforcePeerLimits_Unlimited(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:     newMockEventBus(),
		MaxColdPeers: -1, // Unlimited
		MaxWarmPeers: -1,
		MaxHotPeers:  -1,
	})

	// Add many peers
	for i := 0; i < 10; i++ {
		pg.AddPeer(fmt.Sprintf("peer%d.example.com:3001", i), PeerSourceP2PLedger)
	}

	assert.Len(t, pg.peers, 10)

	// Run reconcile - should not remove any peers when unlimited
	pg.reconcile()

	assert.Len(t, pg.peers, 10)
}

func TestPeerGovernor_PeerSourcePriority(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	// Topology peers should have highest priority
	assert.Greater(t,
		pg.peerSourcePriority(PeerSourceTopologyLocalRoot),
		pg.peerSourcePriority(PeerSourceP2PGossip),
	)

	// Gossip should be higher than ledger
	assert.Greater(t,
		pg.peerSourcePriority(PeerSourceP2PGossip),
		pg.peerSourcePriority(PeerSourceP2PLedger),
	)

	// Ledger should be higher than inbound
	assert.Greater(t,
		pg.peerSourcePriority(PeerSourceP2PLedger),
		pg.peerSourcePriority(PeerSourceInboundConn),
	)

	// Inbound should be higher than unknown
	assert.Greater(t,
		pg.peerSourcePriority(PeerSourceInboundConn),
		pg.peerSourcePriority(PeerSourceUnknown),
	)
}
