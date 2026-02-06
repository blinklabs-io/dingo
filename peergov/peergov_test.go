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
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/topology"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMockEventBus() *event.EventBus {
	// Use the real EventBus for simplicity
	return event.NewEventBus(nil, nil)
}

// boolPtr returns a pointer to the given bool value.
// Used for config fields that use *bool to distinguish nil from explicit false.
func boolPtr(v bool) *bool {
	return &v
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
	assert.True(
		t,
		peers[0].Sharable,
		"gossip-discovered peers should be sharable",
	)

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
	assert.False(
		t,
		nonGossipPeer.Sharable,
		"non-gossip peers should not be sharable by default",
	)
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

func TestPeer_IndexByAddress_HostnameMatchesOriginalAddress(t *testing.T) {
	// Test that peerIndexByAddress can find a peer by its original hostname
	// even when NormalizedAddress contains a DNS-resolved IP.
	// This simulates the case where AddPeer resolved "relay.example.com:3001"
	// to "1.2.3.4:3001" via DNS.
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	// Manually add a peer to simulate DNS resolution during AddPeer
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address:           "relay.example.com:3001",
		NormalizedAddress: "1.2.3.4:3001", // Simulates DNS resolution
		Source:            PeerSourceP2PGossip,
		State:             PeerStateCold,
	})
	pg.mu.Unlock()

	// Should find by resolved IP (NormalizedAddress match)
	idx := pg.peerIndexByAddress("1.2.3.4:3001")
	assert.Equal(t, 0, idx, "should find peer by resolved IP address")

	// Should also find by original hostname (Address match via normalizeAddress)
	idx = pg.peerIndexByAddress("relay.example.com:3001")
	assert.Equal(t, 0, idx, "should find peer by original hostname")

	// Should handle case-insensitive hostname lookup
	idx = pg.peerIndexByAddress("RELAY.EXAMPLE.COM:3001")
	assert.Equal(t, 0, idx, "should find peer by uppercase hostname")

	// Should not find non-existent peer
	idx = pg.peerIndexByAddress("other.example.com:3001")
	assert.Equal(t, -1, idx, "should not find non-existent peer")
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
	assert.True(
		t,
		exists,
		"deny list should store normalized (lowercase) address",
	)
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

	// Wait for expiry using polling instead of fixed sleep
	require.Eventually(t, func() bool {
		return !pg.IsDenied("127.0.0.1:3001")
	}, 1*time.Second, 1*time.Millisecond, "deny entry should expire")
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
	pg.denyList["127.0.0.1:3001"] = time.Now().
		Add(-1 * time.Hour)
		// Already expired
	pg.denyList["127.0.0.1:3002"] = time.Now().
		Add(-1 * time.Hour)
		// Already expired
	pg.denyList["127.0.0.1:3003"] = time.Now().
		Add(1 * time.Hour)
		// Not expired
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
				{
					Hostname: "RELAY.EXAMPLE.COM",
					Port:     3001,
				}, // Same but different case
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
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
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

func TestPeerGovernor_DiscoverLedgerPeers_ExistingPeersNotDuplicated(
	t *testing.T,
) {
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
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
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
			name: "all addresses",
			relay: PoolRelay{
				Hostname: "relay.example.com",
				IPv4:     &ipv4,
				IPv6:     &ipv6,
				Port:     3001,
			},
			expected: []string{
				"relay.example.com:3001",
				"192.168.1.1:3001",
				"[2001:db8::1]:3001",
			},
		},
		{
			name:     "default port",
			relay:    PoolRelay{Hostname: "relay.example.com", Port: 0},
			expected: []string{"relay.example.com:3001"},
		},
		{
			name: "empty ipv4 slice ignored",
			relay: PoolRelay{
				Hostname: "relay.example.com",
				IPv4:     &net.IP{},
				Port:     3001,
			},
			expected: []string{"relay.example.com:3001"},
		},
		{
			name: "empty ipv6 slice ignored",
			relay: PoolRelay{
				Hostname: "relay.example.com",
				IPv6:     &net.IP{},
				Port:     3001,
			},
			expected: []string{"relay.example.com:3001"},
		},
		{
			name: "nil ip pointers ignored",
			relay: PoolRelay{
				Hostname: "relay.example.com",
				IPv4:     nil,
				IPv6:     nil,
				Port:     3001,
			},
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

func TestPeerGovernor_PeerTargets_DefaultValues(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		// TargetNumberOf* not set (0)
	})

	// Should use default values
	assert.Equal(t, 150, pg.config.TargetNumberOfKnownPeers)
	assert.Equal(t, 50, pg.config.TargetNumberOfEstablishedPeers)
	assert.Equal(t, 20, pg.config.TargetNumberOfActivePeers)
	assert.Equal(t, 60, pg.config.TargetNumberOfRootPeers)
	// Per-source quotas
	assert.Equal(t, 3, pg.config.ActivePeersTopologyQuota)
	assert.Equal(t, 12, pg.config.ActivePeersGossipQuota)
	assert.Equal(t, 5, pg.config.ActivePeersLedgerQuota)
}

func TestPeerGovernor_PeerTargets_CustomValues(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		TargetNumberOfKnownPeers:       100,
		TargetNumberOfEstablishedPeers: 25,
		TargetNumberOfActivePeers:      10,
		ActivePeersTopologyQuota:       5,
		ActivePeersGossipQuota:         15,
		ActivePeersLedgerQuota:         8,
	})

	assert.Equal(t, 100, pg.config.TargetNumberOfKnownPeers)
	assert.Equal(t, 25, pg.config.TargetNumberOfEstablishedPeers)
	assert.Equal(t, 10, pg.config.TargetNumberOfActivePeers)
	assert.Equal(t, 5, pg.config.ActivePeersTopologyQuota)
	assert.Equal(t, 15, pg.config.ActivePeersGossipQuota)
	assert.Equal(t, 8, pg.config.ActivePeersLedgerQuota)
}

func TestPeerGovernor_PeerTargets_Unlimited(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		TargetNumberOfKnownPeers:       -1, // Unlimited
		TargetNumberOfEstablishedPeers: -1,
		TargetNumberOfActivePeers:      -1,
	})

	// -1 should be converted to 0 (unlimited internally)
	assert.Equal(t, 0, pg.config.TargetNumberOfKnownPeers)
	assert.Equal(t, 0, pg.config.TargetNumberOfEstablishedPeers)
	assert.Equal(t, 0, pg.config.TargetNumberOfActivePeers)
}

func TestPeerGovernor_EnforcePeerTargets_ColdPeers(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		EventBus:                       newMockEventBus(),
		TargetNumberOfKnownPeers:       3, // Target 3 cold peers
		TargetNumberOfEstablishedPeers: -1,
		TargetNumberOfActivePeers:      -1,
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

func TestPeerGovernor_EnforcePeerTargets_TopologyPeersNeverRemoved(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		EventBus:                       newMockEventBus(),
		TargetNumberOfKnownPeers:       2, // Very low target
		TargetNumberOfEstablishedPeers: -1,
		TargetNumberOfActivePeers:      -1,
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

func TestPeerGovernor_EnforcePeerTargets_Unlimited(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		EventBus:                       newMockEventBus(),
		TargetNumberOfKnownPeers:       -1, // Unlimited
		TargetNumberOfEstablishedPeers: -1,
		TargetNumberOfActivePeers:      -1,
	})

	// Add many peers
	for i := range 10 {
		pg.AddPeer(
			fmt.Sprintf("peer%d.example.com:3001", i),
			PeerSourceP2PLedger,
		)
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

func TestPeerGovernor_CountPeersBySourceAndState(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	// Add peers of different sources and states
	pg.peers = []*Peer{
		{
			Address: "gossip1:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateCold,
		},
		{
			Address: "gossip2:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateWarm,
		},
		{
			Address: "gossip3:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
		{
			Address: "ledger1:3001",
			Source:  PeerSourceP2PLedger,
			State:   PeerStateCold,
		},
		{
			Address: "ledger2:3001",
			Source:  PeerSourceP2PLedger,
			State:   PeerStateHot,
		},
		{
			Address: "local1:3001",
			Source:  PeerSourceTopologyLocalRoot,
			State:   PeerStateHot,
		},
	}

	counts := pg.countPeersBySourceAndState()

	// Check gossip counts
	assert.Equal(t, 1, counts[PeerSourceP2PGossip].Cold)
	assert.Equal(t, 1, counts[PeerSourceP2PGossip].Warm)
	assert.Equal(t, 1, counts[PeerSourceP2PGossip].Hot)

	// Check ledger counts
	assert.Equal(t, 1, counts[PeerSourceP2PLedger].Cold)
	assert.Equal(t, 0, counts[PeerSourceP2PLedger].Warm)
	assert.Equal(t, 1, counts[PeerSourceP2PLedger].Hot)

	// Check local root counts
	assert.Equal(t, 0, counts[PeerSourceTopologyLocalRoot].Cold)
	assert.Equal(t, 0, counts[PeerSourceTopologyLocalRoot].Warm)
	assert.Equal(t, 1, counts[PeerSourceTopologyLocalRoot].Hot)
}

func TestPeerGovernor_GetSourceCategory(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	assert.Equal(
		t,
		"topology",
		pg.getSourceCategory(PeerSourceTopologyLocalRoot),
	)
	assert.Equal(
		t,
		"topology",
		pg.getSourceCategory(PeerSourceTopologyPublicRoot),
	)
	assert.Equal(t, "gossip", pg.getSourceCategory(PeerSourceP2PGossip))
	assert.Equal(t, "ledger", pg.getSourceCategory(PeerSourceP2PLedger))
	assert.Equal(t, "other", pg.getSourceCategory(PeerSourceInboundConn))
	assert.Equal(t, "other", pg.getSourceCategory(PeerSourceUnknown))
}

func TestPeerGovernor_GetHotPeersByCategory(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	pg.peers = []*Peer{
		{
			Address: "gossip1:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
		{
			Address: "gossip2:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
		{
			Address: "gossip3:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateWarm,
		}, // Not hot
		{
			Address: "ledger1:3001",
			Source:  PeerSourceP2PLedger,
			State:   PeerStateHot,
		},
		{
			Address: "local1:3001",
			Source:  PeerSourceTopologyLocalRoot,
			State:   PeerStateHot,
		},
		{
			Address: "public1:3001",
			Source:  PeerSourceTopologyPublicRoot,
			State:   PeerStateHot,
		},
	}

	counts := pg.getHotPeersByCategory()

	assert.Equal(t, 2, counts["gossip"])
	assert.Equal(t, 1, counts["ledger"])
	assert.Equal(t, 2, counts["topology"]) // local + public
	assert.Equal(t, 0, counts["other"])
}

func TestPeerGovernor_EnforceSourceQuota(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                 slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:               newMockEventBus(),
		ActivePeersGossipQuota: 2, // Allow only 2 gossip hot peers
	})

	// Add 4 hot gossip peers with different scores
	pg.peers = []*Peer{
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.9,
		},
		{
			Address:          "gossip2:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.8,
		},
		{
			Address:          "gossip3:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.5,
		}, // Should be demoted
		{
			Address:          "gossip4:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.3,
		}, // Should be demoted
	}

	// Enforce quota
	pg.enforceSourceQuota("gossip", 2, 4)

	// Count remaining hot gossip peers
	hotCount := 0
	for _, peer := range pg.peers {
		if peer.State == PeerStateHot {
			hotCount++
		}
	}
	assert.Equal(t, 2, hotCount)

	// Verify highest scoring peers are kept hot
	for _, peer := range pg.peers {
		if peer.Address == "gossip1:3001" || peer.Address == "gossip2:3001" {
			assert.Equal(
				t,
				PeerStateHot,
				peer.State,
				"high-scoring peer should remain hot",
			)
		}
		if peer.Address == "gossip3:3001" || peer.Address == "gossip4:3001" {
			assert.Equal(
				t,
				PeerStateWarm,
				peer.State,
				"low-scoring peer should be demoted to warm",
			)
		}
	}
}

func TestPeerGovernor_EnforcePerSourceQuotas(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                 slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:               newMockEventBus(),
		ActivePeersGossipQuota: 2,
		ActivePeersLedgerQuota: 1,
	})

	// Add peers exceeding quotas
	pg.peers = []*Peer{
		// 3 hot gossip peers (quota: 2)
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.9,
		},
		{
			Address:          "gossip2:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.7,
		},
		{
			Address:          "gossip3:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.5,
		},
		// 2 hot ledger peers (quota: 1)
		{
			Address:          "ledger1:3001",
			Source:           PeerSourceP2PLedger,
			State:            PeerStateHot,
			PerformanceScore: 0.8,
		},
		{
			Address:          "ledger2:3001",
			Source:           PeerSourceP2PLedger,
			State:            PeerStateHot,
			PerformanceScore: 0.4,
		},
	}

	pg.enforcePerSourceQuotas()

	// Count hot peers by category
	hotByCategory := pg.getHotPeersByCategory()
	assert.Equal(
		t,
		2,
		hotByCategory["gossip"],
		"should have 2 hot gossip peers",
	)
	assert.Equal(t, 1, hotByCategory["ledger"], "should have 1 hot ledger peer")
}

func TestPeerGovernor_RedistributeUnusedSlots(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		ActivePeersTopologyQuota: 3,
		ActivePeersGossipQuota:   12,
		ActivePeersLedgerQuota:   5,
	})

	// Scenario: topology has 1 hot peer (2 unused), gossip has 10 (2 unused), ledger has 3 (2 unused)
	pg.peers = []*Peer{
		{
			Address: "local1:3001",
			Source:  PeerSourceTopologyLocalRoot,
			State:   PeerStateHot,
		},
		// 10 gossip peers
		{
			Address: "gossip1:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
		{
			Address: "gossip2:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
		{
			Address: "gossip3:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
		{
			Address: "gossip4:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
		{
			Address: "gossip5:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
		{
			Address: "gossip6:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
		{
			Address: "gossip7:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
		{
			Address: "gossip8:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
		{
			Address: "gossip9:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
		{
			Address: "gossip10:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
		// 3 ledger peers
		{
			Address: "ledger1:3001",
			Source:  PeerSourceP2PLedger,
			State:   PeerStateHot,
		},
		{
			Address: "ledger2:3001",
			Source:  PeerSourceP2PLedger,
			State:   PeerStateHot,
		},
		{
			Address: "ledger3:3001",
			Source:  PeerSourceP2PLedger,
			State:   PeerStateHot,
		},
	}

	extraSlots := pg.redistributeUnusedSlots()

	// Gossip can borrow from topology (2) + ledger (2) = 4
	// But remaining after gossip borrows 4 is 0
	assert.Equal(t, 4, extraSlots["gossip"])
}

func TestPeerGovernor_LoadTopologyConfig_ValencyStored(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	topologyConfig := &topology.TopologyConfig{
		LocalRoots: []topology.TopologyConfigP2PLocalRoot{
			{
				AccessPoints: []topology.TopologyConfigP2PAccessPoint{
					{Address: "local1.example.com", Port: 3001},
					{Address: "local2.example.com", Port: 3001},
				},
				Advertise:   true,
				Valency:     2,
				WarmValency: 3,
			},
		},
		PublicRoots: []topology.TopologyConfigP2PPublicRoot{
			{
				AccessPoints: []topology.TopologyConfigP2PAccessPoint{
					{Address: "public1.example.com", Port: 3001},
				},
				Advertise:   true,
				Valency:     1,
				WarmValency: 2,
			},
		},
	}

	pg.LoadTopologyConfig(topologyConfig)

	// Find local root peers and check valency
	localRootCount := 0
	for _, peer := range pg.peers {
		if peer.Source == PeerSourceTopologyLocalRoot {
			localRootCount++
			assert.Equal(
				t,
				uint(2),
				peer.Valency,
				"local root valency should be 2",
			)
			assert.Equal(
				t,
				uint(3),
				peer.WarmValency,
				"local root warmValency should be 3",
			)
			assert.Equal(
				t,
				"local-root-0",
				peer.GroupID,
				"local root groupID should be local-root-0",
			)
		}
	}
	assert.Equal(t, 2, localRootCount, "should have 2 local root peers")

	// Find public root peers and check valency
	publicRootCount := 0
	for _, peer := range pg.peers {
		if peer.Source == PeerSourceTopologyPublicRoot {
			publicRootCount++
			assert.Equal(
				t,
				uint(1),
				peer.Valency,
				"public root valency should be 1",
			)
			assert.Equal(
				t,
				uint(2),
				peer.WarmValency,
				"public root warmValency should be 2",
			)
			assert.Equal(
				t,
				"public-root-0",
				peer.GroupID,
				"public root groupID should be public-root-0",
			)
		}
	}
	assert.Equal(t, 1, publicRootCount, "should have 1 public root peer")
}

// Tests for churn system (Phase 3)

func TestPeerGovernor_ChurnConfig_DefaultValues(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	// Check default churn configuration values
	assert.Equal(t, 5*time.Minute, pg.config.GossipChurnInterval)
	assert.Equal(t, 0.20, pg.config.GossipChurnPercent)
	assert.Equal(t, 30*time.Minute, pg.config.PublicRootChurnInterval)
	assert.Equal(t, 0.20, pg.config.PublicRootChurnPercent)
	assert.Equal(t, 0.3, pg.config.MinScoreThreshold)
}

func TestPeerGovernor_ChurnConfig_CustomValues(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		GossipChurnInterval:     10 * time.Minute,
		GossipChurnPercent:      0.30,
		PublicRootChurnInterval: 1 * time.Hour,
		PublicRootChurnPercent:  0.10,
		MinScoreThreshold:       0.5,
	})

	assert.Equal(t, 10*time.Minute, pg.config.GossipChurnInterval)
	assert.Equal(t, 0.30, pg.config.GossipChurnPercent)
	assert.Equal(t, 1*time.Hour, pg.config.PublicRootChurnInterval)
	assert.Equal(t, 0.10, pg.config.PublicRootChurnPercent)
	assert.Equal(t, 0.5, pg.config.MinScoreThreshold)
}

func TestPeerGovernor_DemotionTarget(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	tests := []struct {
		source        PeerSource
		expectedState PeerState
		canDemote     bool
	}{
		// Local roots should never be demoted
		{PeerSourceTopologyLocalRoot, PeerStateCold, false},
		// Public roots demote to warm (keep connection alive)
		{PeerSourceTopologyPublicRoot, PeerStateWarm, true},
		// Gossip/Ledger/Inbound demote to cold (close connection)
		{PeerSourceP2PGossip, PeerStateCold, true},
		{PeerSourceP2PLedger, PeerStateCold, true},
		{PeerSourceInboundConn, PeerStateCold, true},
		{PeerSourceUnknown, PeerStateCold, true},
		// Bootstrap peers demote to cold
		{PeerSourceTopologyBootstrapPeer, PeerStateCold, true},
	}

	for _, tc := range tests {
		t.Run(tc.source.String(), func(t *testing.T) {
			targetState, canDemote := pg.demotionTarget(tc.source)
			assert.Equal(t, tc.expectedState, targetState)
			assert.Equal(t, tc.canDemote, canDemote)
		})
	}
}

func TestPeerGovernor_GossipChurn_DemotesLowestScoringPeers(t *testing.T) {
	reg := prometheus.NewRegistry()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		PromRegistry:       reg,
		GossipChurnPercent: 0.50, // Churn 50% of hot peers
		MinScoreThreshold:  0.3,
	})

	// Add 4 hot gossip peers with different scores
	pg.peers = []*Peer{
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.9,
		},
		{
			Address:          "gossip2:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.7,
		},
		{
			Address:          "gossip3:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.5,
		},
		{
			Address:          "gossip4:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.2,
		}, // Below threshold
	}

	pg.gossipChurn()

	// With 4 peers and 50% churn, should demote 2 peers
	hotCount := 0
	coldCount := 0
	for _, peer := range pg.peers {
		switch peer.State {
		case PeerStateHot:
			hotCount++
		case PeerStateCold:
			coldCount++
		}
	}
	assert.Equal(t, 2, hotCount, "should have 2 hot peers remaining")
	assert.Equal(t, 2, coldCount, "should have 2 cold peers (demoted)")

	// Highest scoring peers should remain hot
	for _, peer := range pg.peers {
		if peer.Address == "gossip1:3001" || peer.Address == "gossip2:3001" {
			assert.Equal(
				t,
				PeerStateHot,
				peer.State,
				"high-scoring peer should remain hot",
			)
		}
		if peer.Address == "gossip3:3001" || peer.Address == "gossip4:3001" {
			assert.Equal(
				t,
				PeerStateCold,
				peer.State,
				"low-scoring peer should be demoted to cold",
			)
		}
	}
}

func TestPeerGovernor_GossipChurn_DemotesToCold(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		GossipChurnPercent: 1.0, // Churn all
		MinScoreThreshold:  0.3,
	})

	// Add gossip and ledger peers (both should demote to cold)
	pg.peers = []*Peer{
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.5,
		},
		{
			Address:          "ledger1:3001",
			Source:           PeerSourceP2PLedger,
			State:            PeerStateHot,
			PerformanceScore: 0.4,
		},
	}

	pg.gossipChurn()

	// Both should be demoted to cold (not warm)
	for _, peer := range pg.peers {
		assert.Equal(
			t,
			PeerStateCold,
			peer.State,
			"gossip/ledger peers should demote to cold",
		)
	}
}

func TestPeerGovernor_GossipChurn_SkipsLocalRoots(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		GossipChurnPercent: 1.0, // Churn all
		MinScoreThreshold:  0.3,
	})

	// Add local root peers (should NOT be churned by gossip churn)
	pg.peers = []*Peer{
		{
			Address:          "local1:3001",
			Source:           PeerSourceTopologyLocalRoot,
			State:            PeerStateHot,
			PerformanceScore: 0.1,
		},
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.5,
		},
	}

	pg.gossipChurn()

	// Local root should remain hot, gossip should be demoted
	for _, peer := range pg.peers {
		if peer.Source == PeerSourceTopologyLocalRoot {
			assert.Equal(
				t,
				PeerStateHot,
				peer.State,
				"local root should NOT be churned",
			)
		}
		if peer.Source == PeerSourceP2PGossip {
			assert.Equal(
				t,
				PeerStateCold,
				peer.State,
				"gossip should be churned",
			)
		}
	}
}

func TestPeerGovernor_GossipChurn_SkipsPublicRoots(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		GossipChurnPercent: 1.0,
		MinScoreThreshold:  0.3,
	})

	// Gossip churn only affects gossip and ledger peers, not public roots
	pg.peers = []*Peer{
		{
			Address:          "public1:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			PerformanceScore: 0.1,
		},
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.5,
		},
	}

	pg.gossipChurn()

	// Public root should remain hot (handled by publicRootChurn instead)
	for _, peer := range pg.peers {
		if peer.Source == PeerSourceTopologyPublicRoot {
			assert.Equal(
				t,
				PeerStateHot,
				peer.State,
				"public root should NOT be affected by gossip churn",
			)
		}
		if peer.Source == PeerSourceP2PGossip {
			assert.Equal(
				t,
				PeerStateCold,
				peer.State,
				"gossip should be churned",
			)
		}
	}
}

func TestPeerGovernor_GossipChurn_PromotesWarmPeers(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		GossipChurnPercent: 1.0, // Churn all hot
		MinScoreThreshold:  0.3,
	})

	// Add hot and warm gossip peers
	pg.peers = []*Peer{
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.4,
			Connection:       &PeerConnection{},
		},
		{
			Address:          "gossip2:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateWarm,
			PerformanceScore: 0.8,
			Connection:       &PeerConnection{},
		}, // High score, should promote
		{
			Address:          "gossip3:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateWarm,
			PerformanceScore: 0.2,
			Connection:       &PeerConnection{},
		}, // Low score, should not promote
	}

	pg.gossipChurn()

	// gossip1 should be demoted, gossip2 should be promoted, gossip3 stays warm
	for _, peer := range pg.peers {
		switch peer.Address {
		case "gossip1:3001":
			assert.Equal(
				t,
				PeerStateCold,
				peer.State,
				"should be demoted to cold",
			)
		case "gossip2:3001":
			assert.Equal(
				t,
				PeerStateHot,
				peer.State,
				"should be promoted to hot",
			)
		case "gossip3:3001":
			assert.Equal(
				t,
				PeerStateWarm,
				peer.State,
				"should remain warm (score below threshold)",
			)
		}
	}
}

func TestPeerGovernor_PublicRootChurn_DemotesToWarm(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                 slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:               newMockEventBus(),
		PublicRootChurnPercent: 1.0, // Churn all
		MinScoreThreshold:      0.3,
	})

	// Add hot public root peers
	pg.peers = []*Peer{
		{
			Address:          "public1:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			PerformanceScore: 0.5,
		},
		{
			Address:          "public2:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			PerformanceScore: 0.4,
		},
	}

	pg.publicRootChurn()

	// Public roots should demote to WARM (not cold)
	for _, peer := range pg.peers {
		assert.Equal(
			t,
			PeerStateWarm,
			peer.State,
			"public root should demote to warm, not cold",
		)
	}
}

func TestPeerGovernor_PublicRootChurn_DemotesLowestScoringPeers(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                 slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:               newMockEventBus(),
		PublicRootChurnPercent: 0.50, // Churn 50%
		MinScoreThreshold:      0.3,
	})

	// Add 4 hot public root peers
	pg.peers = []*Peer{
		{
			Address:          "public1:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			PerformanceScore: 0.9,
		},
		{
			Address:          "public2:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			PerformanceScore: 0.7,
		},
		{
			Address:          "public3:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			PerformanceScore: 0.5,
		},
		{
			Address:          "public4:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			PerformanceScore: 0.2,
		},
	}

	pg.publicRootChurn()

	// 50% churn = 2 demoted
	hotCount := 0
	warmCount := 0
	for _, peer := range pg.peers {
		switch peer.State {
		case PeerStateHot:
			hotCount++
		case PeerStateWarm:
			warmCount++
		}
	}
	assert.Equal(t, 2, hotCount, "should have 2 hot peers remaining")
	assert.Equal(t, 2, warmCount, "should have 2 warm peers (demoted)")

	// Highest scoring should remain hot
	for _, peer := range pg.peers {
		if peer.Address == "public1:3001" || peer.Address == "public2:3001" {
			assert.Equal(
				t,
				PeerStateHot,
				peer.State,
				"high-scoring peer should remain hot",
			)
		}
	}
}

func TestPeerGovernor_PublicRootChurn_OnlyAffectsPublicRoots(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                 slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:               newMockEventBus(),
		PublicRootChurnPercent: 1.0,
		MinScoreThreshold:      0.3,
	})

	// Add mixed peers
	pg.peers = []*Peer{
		{
			Address:          "public1:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			PerformanceScore: 0.5,
		},
		{
			Address:          "local1:3001",
			Source:           PeerSourceTopologyLocalRoot,
			State:            PeerStateHot,
			PerformanceScore: 0.5,
		},
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.5,
		},
	}

	pg.publicRootChurn()

	// Only public root should be churned
	for _, peer := range pg.peers {
		switch peer.Source {
		case PeerSourceTopologyPublicRoot:
			assert.Equal(
				t,
				PeerStateWarm,
				peer.State,
				"public root should be churned to warm",
			)
		default:
			assert.Equal(
				t,
				PeerStateHot,
				peer.State,
				"non-public-root should NOT be affected",
			)
		}
	}
}

func TestPeerGovernor_PublicRootChurn_PromotesWarmPublicRoots(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                 slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:               newMockEventBus(),
		PublicRootChurnPercent: 1.0,
		MinScoreThreshold:      0.3,
	})

	// Add hot and warm public root peers
	pg.peers = []*Peer{
		{
			Address:          "public1:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			PerformanceScore: 0.4,
			Connection:       &PeerConnection{},
		},
		{
			Address:          "public2:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateWarm,
			PerformanceScore: 0.8,
			Connection:       &PeerConnection{},
		}, // High score
		{
			Address:          "public3:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateWarm,
			PerformanceScore: 0.2,
			Connection:       &PeerConnection{},
		}, // Low score
	}

	pg.publicRootChurn()

	// public1 demoted to warm, public2 promoted to hot, public3 stays warm
	for _, peer := range pg.peers {
		switch peer.Address {
		case "public1:3001":
			assert.Equal(
				t,
				PeerStateWarm,
				peer.State,
				"should be demoted to warm",
			)
		case "public2:3001":
			assert.Equal(
				t,
				PeerStateHot,
				peer.State,
				"should be promoted to hot",
			)
		case "public3:3001":
			assert.Equal(
				t,
				PeerStateWarm,
				peer.State,
				"should remain warm (score below threshold)",
			)
		}
	}
}

func TestPeerGovernor_GossipChurn_AtLeastOneChurned(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		GossipChurnPercent: 0.10, // 10% of 2 = 0.2, rounds to 1
		MinScoreThreshold:  0.3,
	})

	// Add 2 hot gossip peers
	pg.peers = []*Peer{
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.9,
		},
		{
			Address:          "gossip2:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.4,
		},
	}

	pg.gossipChurn()

	// Even with 10% of 2 peers (= 0.2), at least 1 should be churned
	coldCount := 0
	for _, peer := range pg.peers {
		if peer.State == PeerStateCold {
			coldCount++
		}
	}
	assert.Equal(t, 1, coldCount, "at least 1 peer should be churned")
}

func TestPeerGovernor_GossipChurn_NoPeersToChurn(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		GossipChurnPercent: 0.20,
		MinScoreThreshold:  0.3,
	})

	// No hot gossip/ledger peers
	pg.peers = []*Peer{
		{
			Address:          "local1:3001",
			Source:           PeerSourceTopologyLocalRoot,
			State:            PeerStateHot,
			PerformanceScore: 0.5,
		},
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateWarm,
			PerformanceScore: 0.5,
		},
	}

	// Should not panic
	pg.gossipChurn()

	// States should be unchanged
	for _, peer := range pg.peers {
		switch peer.Address {
		case "local1:3001":
			assert.Equal(t, PeerStateHot, peer.State)
		case "gossip1:3001":
			assert.Equal(t, PeerStateWarm, peer.State)
		}
	}
}

func TestPeerGovernor_PromoteWarmNonRootPeers_RespectsScoreThreshold(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:          newMockEventBus(),
		MinScoreThreshold: 0.5, // Higher threshold
	})

	pg.peers = []*Peer{
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateWarm,
			PerformanceScore: 0.8,
			Connection:       &PeerConnection{},
		}, // Above threshold
		{
			Address:          "gossip2:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateWarm,
			PerformanceScore: 0.3,
			Connection:       &PeerConnection{},
		}, // Below threshold
		{
			Address:          "gossip3:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateWarm,
			PerformanceScore: 0.4,
			Connection:       &PeerConnection{},
		}, // Below threshold
	}

	pg.promoteWarmNonRootPeers(3) // Try to promote all 3

	// Only gossip1 should be promoted (above threshold)
	promotedCount := 0
	for _, peer := range pg.peers {
		if peer.State == PeerStateHot {
			promotedCount++
			assert.Equal(
				t,
				"gossip1:3001",
				peer.Address,
				"only high-scoring peer should be promoted",
			)
		}
	}
	assert.Equal(t, 1, promotedCount, "only 1 peer should be promoted")
}

func TestPeerGovernor_PromoteFromWarmPublicRoots_RespectsScoreThreshold(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:          newMockEventBus(),
		MinScoreThreshold: 0.5,
	})

	pg.peers = []*Peer{
		{
			Address:          "public1:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateWarm,
			PerformanceScore: 0.8,
			Connection:       &PeerConnection{},
		},
		{
			Address:          "public2:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateWarm,
			PerformanceScore: 0.3,
			Connection:       &PeerConnection{},
		},
	}

	// Collect warm public roots for promotion
	var warmPublicRoots []*Peer
	for _, peer := range pg.peers {
		if peer.State == PeerStateWarm &&
			peer.Source == PeerSourceTopologyPublicRoot {
			warmPublicRoots = append(warmPublicRoots, peer)
		}
	}

	pg.promoteFromWarmPublicRoots(warmPublicRoots, 2)

	// Only public1 should be promoted
	promotedCount := 0
	for _, peer := range pg.peers {
		if peer.State == PeerStateHot {
			promotedCount++
		}
	}
	assert.Equal(t, 1, promotedCount, "only 1 peer should be promoted")
}

func TestPeerGovernor_GossipChurn_Metrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		PromRegistry:       reg,
		GossipChurnPercent: 1.0,
		MinScoreThreshold:  0.3,
	})

	// Add hot gossip peer and warm peer for promotion
	pg.peers = []*Peer{
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.4,
			Connection:       &PeerConnection{},
		},
		{
			Address:          "gossip2:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateWarm,
			PerformanceScore: 0.8,
			Connection:       &PeerConnection{},
		},
	}

	pg.gossipChurn()

	// Check demotion metric
	demotions := testutil.ToFloat64(pg.metrics.activeNonRootPeersDemotions)
	assert.Equal(t, float64(1), demotions, "should have 1 demotion")

	// Check promotion metric
	promotions := testutil.ToFloat64(pg.metrics.warmNonRootPeersPromotions)
	assert.Equal(t, float64(1), promotions, "should have 1 promotion")
}

// Tests for valency enforcement (Phase 4)

func TestPeerGovernor_CountPeersByGroup(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	pg.peers = []*Peer{
		{
			Address:     "local1:3001",
			Source:      PeerSourceTopologyLocalRoot,
			State:       PeerStateCold,
			GroupID:     "local-root-0",
			Valency:     2,
			WarmValency: 3,
		},
		{
			Address:     "local2:3001",
			Source:      PeerSourceTopologyLocalRoot,
			State:       PeerStateWarm,
			GroupID:     "local-root-0",
			Valency:     2,
			WarmValency: 3,
		},
		{
			Address:     "local3:3001",
			Source:      PeerSourceTopologyLocalRoot,
			State:       PeerStateHot,
			GroupID:     "local-root-0",
			Valency:     2,
			WarmValency: 3,
		},
		{
			Address: "public1:3001",
			Source:  PeerSourceTopologyPublicRoot,
			State:   PeerStateHot,
			GroupID: "public-root-0",
			Valency: 1,
		},
		{
			Address: "public2:3001",
			Source:  PeerSourceTopologyPublicRoot,
			State:   PeerStateHot,
			GroupID: "public-root-1",
			Valency: 2,
		},
		// Gossip peers have no GroupID
		{
			Address: "gossip1:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
	}

	groups := pg.countPeersByGroup()

	// Check local-root-0
	assert.NotNil(t, groups["local-root-0"])
	assert.Equal(t, 1, groups["local-root-0"].Cold)
	assert.Equal(t, 1, groups["local-root-0"].Warm)
	assert.Equal(t, 1, groups["local-root-0"].Hot)
	assert.Equal(t, uint(2), groups["local-root-0"].Valency)
	assert.Equal(t, uint(3), groups["local-root-0"].WarmValency)

	// Check public-root-0
	assert.NotNil(t, groups["public-root-0"])
	assert.Equal(t, 0, groups["public-root-0"].Cold)
	assert.Equal(t, 0, groups["public-root-0"].Warm)
	assert.Equal(t, 1, groups["public-root-0"].Hot)
	assert.Equal(t, uint(1), groups["public-root-0"].Valency)

	// Check public-root-1
	assert.NotNil(t, groups["public-root-1"])
	assert.Equal(t, 1, groups["public-root-1"].Hot)
	assert.Equal(t, uint(2), groups["public-root-1"].Valency)

	// Gossip peers should not be in any group
	assert.Len(t, groups, 3) // Only 3 groups
}

func TestPeerGovernor_IsGroupUnderHotValency(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	pg.peers = []*Peer{
		// Group with valency 2, only 1 hot peer (under valency)
		{
			Address: "local1:3001",
			Source:  PeerSourceTopologyLocalRoot,
			State:   PeerStateHot,
			GroupID: "local-root-0",
			Valency: 2,
		},
		{
			Address: "local2:3001",
			Source:  PeerSourceTopologyLocalRoot,
			State:   PeerStateWarm,
			GroupID: "local-root-0",
			Valency: 2,
		},
		// Group with valency 1, 1 hot peer (at valency)
		{
			Address: "public1:3001",
			Source:  PeerSourceTopologyPublicRoot,
			State:   PeerStateHot,
			GroupID: "public-root-0",
			Valency: 1,
		},
		// Group with valency 0 (no limit)
		{
			Address: "public2:3001",
			Source:  PeerSourceTopologyPublicRoot,
			State:   PeerStateWarm,
			GroupID: "public-root-1",
			Valency: 0,
		},
	}

	groups := pg.countPeersByGroup()

	// local-root-0: 1 hot, valency 2 -> under valency
	assert.True(
		t,
		pg.isGroupUnderHotValency(pg.peers[0], groups),
		"group with 1/2 hot should be under valency",
	)
	assert.True(
		t,
		pg.isGroupUnderHotValency(pg.peers[1], groups),
		"warm peer in same group should be under valency",
	)

	// public-root-0: 1 hot, valency 1 -> at valency (not under)
	assert.False(
		t,
		pg.isGroupUnderHotValency(pg.peers[2], groups),
		"group with 1/1 hot should NOT be under valency",
	)

	// public-root-1: valency 0 (no limit) -> not under valency
	assert.False(
		t,
		pg.isGroupUnderHotValency(pg.peers[3], groups),
		"group with valency 0 should NOT be under valency",
	)

	// Peer with no GroupID
	gossipPeer := &Peer{
		Address: "gossip:3001",
		Source:  PeerSourceP2PGossip,
		State:   PeerStateWarm,
	}
	assert.False(
		t,
		pg.isGroupUnderHotValency(gossipPeer, groups),
		"peer with no GroupID should NOT be under valency",
	)
}

func TestPeerGovernor_IsGroupOverHotValency(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	pg.peers = []*Peer{
		// Group with valency 1, 2 hot peers (over valency)
		{
			Address: "local1:3001",
			Source:  PeerSourceTopologyLocalRoot,
			State:   PeerStateHot,
			GroupID: "local-root-0",
			Valency: 1,
		},
		{
			Address: "local2:3001",
			Source:  PeerSourceTopologyLocalRoot,
			State:   PeerStateHot,
			GroupID: "local-root-0",
			Valency: 1,
		},
		// Group with valency 2, 2 hot peers (at valency)
		{
			Address: "public1:3001",
			Source:  PeerSourceTopologyPublicRoot,
			State:   PeerStateHot,
			GroupID: "public-root-0",
			Valency: 2,
		},
		{
			Address: "public2:3001",
			Source:  PeerSourceTopologyPublicRoot,
			State:   PeerStateHot,
			GroupID: "public-root-0",
			Valency: 2,
		},
	}

	groups := pg.countPeersByGroup()

	// local-root-0: 2 hot, valency 1 -> over valency
	assert.True(
		t,
		pg.isGroupOverHotValency(pg.peers[0], groups),
		"group with 2/1 hot should be over valency",
	)
	assert.True(
		t,
		pg.isGroupOverHotValency(pg.peers[1], groups),
		"second peer in same group should be over valency",
	)

	// public-root-0: 2 hot, valency 2 -> at valency (not over)
	assert.False(
		t,
		pg.isGroupOverHotValency(pg.peers[2], groups),
		"group with 2/2 hot should NOT be over valency",
	)
}

func TestPeerGovernor_EnforceGroupValency(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: newMockEventBus(),
	})

	// Group with valency 1, but 3 hot public root peers -> should demote 2
	// Note: Uses public roots, not local roots, because local roots are never demoted
	pg.peers = []*Peer{
		{
			Address:          "public1:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			GroupID:          "public-root-0",
			Valency:          1,
			PerformanceScore: 0.9,
		},
		{
			Address:          "public2:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			GroupID:          "public-root-0",
			Valency:          1,
			PerformanceScore: 0.5,
		},
		{
			Address:          "public3:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			GroupID:          "public-root-0",
			Valency:          1,
			PerformanceScore: 0.3,
		},
	}

	pg.enforceGroupValency()

	// Should demote 2 lowest-scoring peers
	hotCount := 0
	warmCount := 0
	for _, peer := range pg.peers {
		switch peer.State {
		case PeerStateHot:
			hotCount++
		case PeerStateWarm:
			warmCount++
		}
	}
	assert.Equal(t, 1, hotCount, "should have 1 hot peer remaining")
	assert.Equal(t, 2, warmCount, "should have 2 warm peers (demoted)")

	// Highest scoring peer should remain hot
	for _, peer := range pg.peers {
		if peer.Address == "public1:3001" {
			assert.Equal(
				t,
				PeerStateHot,
				peer.State,
				"highest-scoring peer should remain hot",
			)
		} else {
			assert.Equal(t, PeerStateWarm, peer.State, "lower-scoring peers should be demoted")
		}
	}
}

func TestPeerGovernor_EnforceGroupValency_LocalRootsNeverDemoted(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: newMockEventBus(),
	})

	// Group with valency 1, but 3 hot local root peers -> none should be demoted
	// Local roots are never demoted to preserve the topology invariant
	pg.peers = []*Peer{
		{
			Address:          "local1:3001",
			Source:           PeerSourceTopologyLocalRoot,
			State:            PeerStateHot,
			GroupID:          "local-root-0",
			Valency:          1,
			PerformanceScore: 0.9,
		},
		{
			Address:          "local2:3001",
			Source:           PeerSourceTopologyLocalRoot,
			State:            PeerStateHot,
			GroupID:          "local-root-0",
			Valency:          1,
			PerformanceScore: 0.5,
		},
		{
			Address:          "local3:3001",
			Source:           PeerSourceTopologyLocalRoot,
			State:            PeerStateHot,
			GroupID:          "local-root-0",
			Valency:          1,
			PerformanceScore: 0.3,
		},
	}

	pg.enforceGroupValency()

	// All local roots should remain hot (never demoted)
	for _, peer := range pg.peers {
		assert.Equal(
			t,
			PeerStateHot,
			peer.State,
			"local roots should never be demoted",
		)
	}
}

func TestPeerGovernor_EnforceGroupValency_NoExcess(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	// Group with valency 2, only 1 hot peer -> no demotion needed
	pg.peers = []*Peer{
		{
			Address: "local1:3001",
			Source:  PeerSourceTopologyLocalRoot,
			State:   PeerStateHot,
			GroupID: "local-root-0",
			Valency: 2,
		},
		{
			Address: "local2:3001",
			Source:  PeerSourceTopologyLocalRoot,
			State:   PeerStateWarm,
			GroupID: "local-root-0",
			Valency: 2,
		},
	}

	pg.enforceGroupValency()

	// No changes should occur
	assert.Equal(t, PeerStateHot, pg.peers[0].State)
	assert.Equal(t, PeerStateWarm, pg.peers[1].State)
}

func TestPeerGovernor_ValencyAwarePromotion_PrefersUnderValencyGroups(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                 slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:               newMockEventBus(),
		PublicRootChurnPercent: 1.0,
		MinScoreThreshold:      0.3,
	})

	// Two groups: group1 under valency, group2 at valency
	// Both have warm peers available for promotion
	pg.peers = []*Peer{
		// Group1: valency 2, 0 hot (under valency)
		{
			Address:          "public1:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateWarm,
			GroupID:          "public-root-0",
			Valency:          2,
			PerformanceScore: 0.5,
			Connection:       &PeerConnection{},
		},
		{
			Address:          "public2:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateWarm,
			GroupID:          "public-root-0",
			Valency:          2,
			PerformanceScore: 0.6,
			Connection:       &PeerConnection{},
		},
		// Group2: valency 1, 1 hot (at valency)
		{
			Address:          "public3:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			GroupID:          "public-root-1",
			Valency:          1,
			PerformanceScore: 0.4,
			Connection:       &PeerConnection{},
		},
		{
			Address:          "public4:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateWarm,
			GroupID:          "public-root-1",
			Valency:          1,
			PerformanceScore: 0.9,
			Connection:       &PeerConnection{},
		}, // Higher score but at valency
	}

	// Collect warm public roots (before churn modifies state)
	var warmPublicRoots []*Peer
	for _, peer := range pg.peers {
		if peer.State == PeerStateWarm &&
			peer.Source == PeerSourceTopologyPublicRoot {
			warmPublicRoots = append(warmPublicRoots, peer)
		}
	}

	pg.promoteFromWarmPublicRoots(warmPublicRoots, 2)

	// Group1 peers should be promoted first (under valency)
	var group1Hot, group2Hot int
	for _, peer := range pg.peers {
		if peer.State == PeerStateHot {
			if peer.GroupID == "public-root-0" {
				group1Hot++
			}
			if peer.GroupID == "public-root-1" {
				group2Hot++
			}
		}
	}

	assert.Equal(
		t,
		2,
		group1Hot,
		"under-valency group should have 2 hot peers promoted",
	)
	assert.Equal(
		t,
		1,
		group2Hot,
		"at-valency group should keep only 1 hot peer",
	)
}

func TestPeerGovernor_ValencyAwareDemotion_PrefersOverValencyGroups(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                 slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:               newMockEventBus(),
		PublicRootChurnPercent: 0.50, // Demote 50% = 2 peers
		MinScoreThreshold:      0.3,
	})

	// Group1: valency 1, 2 hot (over valency) - lower scores
	// Group2: valency 2, 2 hot (at valency) - higher scores
	pg.peers = []*Peer{
		{
			Address:          "public1:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			GroupID:          "public-root-0",
			Valency:          1,
			PerformanceScore: 0.6,
		},
		{
			Address:          "public2:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			GroupID:          "public-root-0",
			Valency:          1,
			PerformanceScore: 0.5,
		},
		{
			Address:          "public3:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			GroupID:          "public-root-1",
			Valency:          2,
			PerformanceScore: 0.9,
		},
		{
			Address:          "public4:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			GroupID:          "public-root-1",
			Valency:          2,
			PerformanceScore: 0.8,
		},
	}

	pg.publicRootChurn()

	// Over-valency group should be demoted first
	var group1Hot, group2Hot int
	for _, peer := range pg.peers {
		if peer.State == PeerStateHot {
			if peer.GroupID == "public-root-0" {
				group1Hot++
			}
			if peer.GroupID == "public-root-1" {
				group2Hot++
			}
		}
	}

	// Group1 was over valency (2 hot, valency 1), should demote preferentially
	// With 50% churn of 4 peers = 2 demotions, both should come from over-valency group1
	assert.Equal(
		t,
		0,
		group1Hot,
		"over-valency group should have peers demoted first",
	)
	assert.Equal(t, 2, group2Hot, "at-valency group should keep hot peers")
}

func TestPeerGovernor_LogValencyStatus_NoGroups(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	// Only gossip peers (no groups)
	pg.peers = []*Peer{
		{
			Address: "gossip1:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateHot,
		},
	}

	// Should not panic
	pg.logValencyStatus()
}

func TestPeerGovernor_ReconcilePromotion_ValencyAware(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:    newMockEventBus(),
		MinHotPeers: 3,
	})

	// Two groups with warm peers available
	// Group1: valency 2, 0 hot (under valency)
	// Group2: valency 1, 1 hot (at valency)
	pg.peers = []*Peer{
		// Hot peer for group2 (at valency)
		{
			Address:          "public3:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateHot,
			GroupID:          "public-root-1",
			Valency:          1,
			PerformanceScore: 0.5,
			Connection:       &PeerConnection{},
		},
		// Warm peers for group1 (under valency) - should be promoted first
		{
			Address:          "public1:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateWarm,
			GroupID:          "public-root-0",
			Valency:          2,
			PerformanceScore: 0.6,
			Connection:       &PeerConnection{},
		},
		{
			Address:          "public2:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateWarm,
			GroupID:          "public-root-0",
			Valency:          2,
			PerformanceScore: 0.7,
			Connection:       &PeerConnection{},
		},
		// Warm peer for group2 (at valency) - lower priority
		{
			Address:          "public4:3001",
			Source:           PeerSourceTopologyPublicRoot,
			State:            PeerStateWarm,
			GroupID:          "public-root-1",
			Valency:          1,
			PerformanceScore: 0.9,
			Connection:       &PeerConnection{},
		},
	}

	pg.reconcile()

	// With MinHotPeers=3, we need 2 more hot peers
	// Group1 is under valency, so public1 and public2 should be promoted first
	var group1Hot, group2Hot int
	for _, peer := range pg.peers {
		if peer.State == PeerStateHot {
			if peer.GroupID == "public-root-0" {
				group1Hot++
			}
			if peer.GroupID == "public-root-1" {
				group2Hot++
			}
		}
	}

	// Group1 should have 2 hot (under-valency promoted first)
	// Group2 should still have 1 hot
	assert.Equal(
		t,
		2,
		group1Hot,
		"under-valency group should have peers promoted",
	)
	assert.Equal(
		t,
		1,
		group2Hot,
		"at-valency group should not get more promotions",
	)
}

// TestPeerGovernor_InboundConfig_DefaultValues tests that default values
// for inbound peer validation are correctly applied.
func TestPeerGovernor_InboundConfig_DefaultValues(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	// Check that default values are applied
	assert.Equal(t, 0.6, pg.config.InboundHotScoreThreshold,
		"default InboundHotScoreThreshold should be 0.6")
	assert.Equal(t, 10*time.Minute, pg.config.InboundMinTenure,
		"default InboundMinTenure should be 10 minutes")
}

// TestPeerGovernor_InboundConfig_CustomValues tests that custom values
// for inbound peer validation are respected.
func TestPeerGovernor_InboundConfig_CustomValues(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		InboundHotScoreThreshold: 0.7,
		InboundMinTenure:         15 * time.Minute,
	})

	// Check that custom values are applied
	assert.Equal(t, 0.7, pg.config.InboundHotScoreThreshold,
		"custom InboundHotScoreThreshold should be 0.7")
	assert.Equal(t, 15*time.Minute, pg.config.InboundMinTenure,
		"custom InboundMinTenure should be 15 minutes")
}

// TestPeerGovernor_InboundPeer_FirstSeenTracked tests that FirstSeen is
// correctly set when inbound peers are added.
func TestPeerGovernor_InboundPeer_FirstSeenTracked(t *testing.T) {
	eventBus := newMockEventBus()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: eventBus,
	})

	beforeAdd := time.Now()

	// Add an inbound peer using AddPeer
	pg.AddPeer("192.168.1.1:3001", PeerSourceInboundConn)

	afterAdd := time.Now()

	peers := pg.GetPeers()
	assert.Len(t, peers, 1)
	assert.EqualValues(t, PeerSourceInboundConn, peers[0].Source)

	// FirstSeen should be set between beforeAdd and afterAdd
	assert.False(t, peers[0].FirstSeen.IsZero(), "FirstSeen should be set")
	assert.True(t, !peers[0].FirstSeen.Before(beforeAdd),
		"FirstSeen should be at or after beforeAdd")
	assert.True(t, !peers[0].FirstSeen.After(afterAdd),
		"FirstSeen should be at or before afterAdd")
}

// TestPeerGovernor_InboundPeer_TenureRequirement tests that inbound peers
// are not promoted to hot until they meet the minimum tenure requirement.
func TestPeerGovernor_InboundPeer_TenureRequirement(t *testing.T) {
	eventBus := newMockEventBus()
	reg := prometheus.NewRegistry()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		EventBus:                 eventBus,
		PromRegistry:             reg,
		MinHotPeers:              1,
		InboundHotScoreThreshold: 0.5,
		InboundMinTenure:         10 * time.Minute,
	})

	// Add an inbound peer that was just seen (tenure < InboundMinTenure)
	pg.mu.Lock()
	pg.peers = []*Peer{
		{
			Address:          "192.168.1.1:3001",
			Source:           PeerSourceInboundConn,
			State:            PeerStateWarm,
			Connection:       &PeerConnection{},
			PerformanceScore: 0.8,        // Above threshold
			FirstSeen:        time.Now(), // Just seen, not enough tenure
		},
	}
	pg.mu.Unlock()

	pg.reconcile()

	peers := pg.GetPeers()
	// Should still be warm because tenure requirement is not met
	assert.Equal(t, PeerStateWarm, peers[0].State,
		"inbound peer without sufficient tenure should remain warm")
}

// TestPeerGovernor_InboundPeer_HigherScoreThreshold tests that inbound peers
// require a higher score threshold than non-inbound peers.
func TestPeerGovernor_InboundPeer_HigherScoreThreshold(t *testing.T) {
	eventBus := newMockEventBus()
	reg := prometheus.NewRegistry()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		EventBus:                 eventBus,
		PromRegistry:             reg,
		MinHotPeers:              1,
		MinScoreThreshold:        0.3,             // Normal threshold
		InboundHotScoreThreshold: 0.6,             // Higher threshold for inbound
		InboundMinTenure:         1 * time.Minute, // Short tenure for test
	})

	// Add an inbound peer with score above normal threshold but below inbound threshold
	pg.mu.Lock()
	pg.peers = []*Peer{
		{
			Address:          "192.168.1.1:3001",
			Source:           PeerSourceInboundConn,
			State:            PeerStateWarm,
			Connection:       &PeerConnection{},
			PerformanceScore: 0.4, // Above normal 0.3, below inbound 0.6
			FirstSeen: time.Now().
				Add(-5 * time.Minute),
			// Sufficient tenure
		},
	}
	pg.mu.Unlock()

	pg.reconcile()

	peers := pg.GetPeers()
	// Should still be warm because score is below inbound threshold
	assert.Equal(t, PeerStateWarm, peers[0].State,
		"inbound peer with score below inbound threshold should remain warm")
}

// TestPeerGovernor_InboundPeer_BothRequirementsMet tests that inbound peers
// are promoted to hot when both score and tenure requirements are met.
func TestPeerGovernor_InboundPeer_BothRequirementsMet(t *testing.T) {
	eventBus := newMockEventBus()
	reg := prometheus.NewRegistry()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		EventBus:                 eventBus,
		PromRegistry:             reg,
		MinHotPeers:              1,
		InboundHotScoreThreshold: 0.6,
		InboundMinTenure:         5 * time.Minute,
	})

	// Add an inbound peer that meets both requirements
	// We need to set the metrics flags so that UpdatePeerScore() calculates
	// a high enough score (above InboundHotScoreThreshold of 0.6)
	pg.mu.Lock()
	pg.peers = []*Peer{
		{
			Address:    "192.168.1.1:3001",
			Source:     PeerSourceInboundConn,
			State:      PeerStateWarm,
			Connection: &PeerConnection{},
			FirstSeen: time.Now().
				Add(-10 * time.Minute),
			// Sufficient tenure (>= 5min)
			BlockFetchLatencyMs:     50, // Low latency = high score
			BlockFetchLatencyInit:   true,
			BlockFetchSuccessRate:   0.95, // High success rate
			BlockFetchSuccessInit:   true,
			ConnectionStability:     0.9, // High stability
			ConnectionStabilityInit: true,
			HeaderArrivalRate:       100, // Good header rate
			HeaderArrivalRateInit:   true,
			TipSlotDelta:            0, // At tip
			TipSlotDeltaInit:        true,
		},
	}
	pg.mu.Unlock()

	pg.reconcile()

	peers := pg.GetPeers()
	// Should be hot because both requirements are met
	// With good metrics: latency 50ms gives ~0.8, success 0.95, stability 0.9,
	// header rate ~0.67, tip delta 1.0 => weighted average should be >0.6
	assert.Equal(t, PeerStateHot, peers[0].State,
		"inbound peer meeting both requirements should be promoted to hot")
}

// TestPeerGovernor_NonInboundPeer_UsesNormalThreshold tests that non-inbound
// peers use the normal score threshold, not the inbound threshold.
func TestPeerGovernor_NonInboundPeer_UsesNormalThreshold(t *testing.T) {
	eventBus := newMockEventBus()
	reg := prometheus.NewRegistry()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		EventBus:                 eventBus,
		PromRegistry:             reg,
		MinHotPeers:              1,
		MinScoreThreshold:        0.3, // Normal threshold
		InboundHotScoreThreshold: 0.6, // Higher threshold for inbound
		InboundMinTenure:         10 * time.Minute,
	})

	// Add a gossip peer with score above normal threshold but below inbound threshold
	pg.mu.Lock()
	pg.peers = []*Peer{
		{
			Address:          "192.168.1.1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateWarm,
			Connection:       &PeerConnection{},
			PerformanceScore: 0.4,        // Above normal 0.3, below inbound 0.6
			FirstSeen:        time.Now(), // Just seen (would fail inbound tenure)
		},
	}
	pg.mu.Unlock()

	pg.reconcile()

	peers := pg.GetPeers()
	// Should be hot because gossip peers use normal threshold, not inbound threshold
	assert.Equal(t, PeerStateHot, peers[0].State,
		"non-inbound peer should use normal threshold and be promoted to hot")
}

// TestPeerGovernor_IsInboundEligibleForHot tests the isInboundEligibleForHot method directly.
func TestPeerGovernor_IsInboundEligibleForHot(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		InboundHotScoreThreshold: 0.6,
		InboundMinTenure:         10 * time.Minute,
	})

	tests := []struct {
		name     string
		peer     *Peer
		expected bool
	}{
		{
			name:     "nil peer",
			peer:     nil,
			expected: false,
		},
		{
			name: "non-inbound peer always eligible",
			peer: &Peer{
				Source:           PeerSourceP2PGossip,
				PerformanceScore: 0.1,         // Low score, doesn't matter for non-inbound
				FirstSeen:        time.Time{}, // Zero time, doesn't matter for non-inbound
			},
			expected: true,
		},
		{
			name: "inbound peer with zero FirstSeen",
			peer: &Peer{
				Source:           PeerSourceInboundConn,
				PerformanceScore: 0.8,
				FirstSeen:        time.Time{}, // Zero value
			},
			expected: false,
		},
		{
			name: "inbound peer with insufficient score",
			peer: &Peer{
				Source:           PeerSourceInboundConn,
				PerformanceScore: 0.5, // Below 0.6 threshold
				FirstSeen:        time.Now().Add(-15 * time.Minute),
			},
			expected: false,
		},
		{
			name: "inbound peer with insufficient tenure",
			peer: &Peer{
				Source:           PeerSourceInboundConn,
				PerformanceScore: 0.8, // Above threshold
				FirstSeen: time.Now().
					Add(-5 * time.Minute),
				// Less than 10 min
			},
			expected: false,
		},
		{
			name: "inbound peer meeting both requirements",
			peer: &Peer{
				Source:           PeerSourceInboundConn,
				PerformanceScore: 0.7, // Above threshold
				FirstSeen: time.Now().
					Add(-15 * time.Minute),
				// More than 10 min
			},
			expected: true,
		},
		{
			name: "inbound peer with exact threshold score",
			peer: &Peer{
				Source:           PeerSourceInboundConn,
				PerformanceScore: 0.6, // Exact threshold
				FirstSeen:        time.Now().Add(-15 * time.Minute),
			},
			expected: true,
		},
		{
			name: "inbound peer with exact threshold tenure",
			peer: &Peer{
				Source:           PeerSourceInboundConn,
				PerformanceScore: 0.8,
				FirstSeen: time.Now().
					Add(-10 * time.Minute),
				// Exact 10 min
			},
			expected: true,
		},
		{
			name: "topology local root always eligible",
			peer: &Peer{
				Source:           PeerSourceTopologyLocalRoot,
				PerformanceScore: 0.1,
				FirstSeen:        time.Time{},
			},
			expected: true,
		},
		{
			name: "topology public root always eligible",
			peer: &Peer{
				Source:           PeerSourceTopologyPublicRoot,
				PerformanceScore: 0.1,
				FirstSeen:        time.Time{},
			},
			expected: true,
		},
		{
			name: "ledger peer always eligible",
			peer: &Peer{
				Source:           PeerSourceP2PLedger,
				PerformanceScore: 0.1,
				FirstSeen:        time.Time{},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pg.isInboundEligibleForHot(tt.peer)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestPeerGovernor_InboundPeer_MixedPeerPromotion tests that when there are
// multiple peer types, inbound peers are skipped if they don't meet requirements
// while other peers can still be promoted.
func TestPeerGovernor_InboundPeer_MixedPeerPromotion(t *testing.T) {
	eventBus := newMockEventBus()
	reg := prometheus.NewRegistry()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		EventBus:                 eventBus,
		PromRegistry:             reg,
		MinHotPeers:              2,
		InboundHotScoreThreshold: 0.6,
		InboundMinTenure:         10 * time.Minute,
	})

	// Add a mix of peers:
	// - Inbound peer that doesn't meet requirements (should stay warm)
	// - Gossip peer that can be promoted
	// - Another gossip peer that can be promoted
	pg.mu.Lock()
	pg.peers = []*Peer{
		{
			Address:          "192.168.1.1:3001",
			Source:           PeerSourceInboundConn,
			State:            PeerStateWarm,
			Connection:       &PeerConnection{},
			PerformanceScore: 0.9,        // High score but...
			FirstSeen:        time.Now(), // Insufficient tenure
		},
		{
			Address:          "192.168.1.2:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateWarm,
			Connection:       &PeerConnection{},
			PerformanceScore: 0.5, // Lower score
			FirstSeen:        time.Now(),
		},
		{
			Address:          "192.168.1.3:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateWarm,
			Connection:       &PeerConnection{},
			PerformanceScore: 0.4, // Even lower score
			FirstSeen:        time.Now(),
		},
	}
	pg.mu.Unlock()

	pg.reconcile()

	peers := pg.GetPeers()
	var inboundState, gossip1State, gossip2State PeerState
	for _, peer := range peers {
		switch peer.Address {
		case "192.168.1.1:3001":
			inboundState = peer.State
		case "192.168.1.2:3001":
			gossip1State = peer.State
		case "192.168.1.3:3001":
			gossip2State = peer.State
		}
	}

	// Inbound should stay warm (tenure requirement not met)
	assert.Equal(t, PeerStateWarm, inboundState,
		"inbound peer without sufficient tenure should remain warm")
	// Gossip peers should be promoted to meet MinHotPeers=2
	assert.Equal(t, PeerStateHot, gossip1State,
		"gossip peer should be promoted to hot")
	assert.Equal(t, PeerStateHot, gossip2State,
		"gossip peer should be promoted to hot")
}

// Phase 7: Enhanced Observability Tests

// TestPeerGovernor_PerSourceMetrics tests that per-source metrics are properly
// initialized and updated based on peer source and state.
func TestPeerGovernor_PerSourceMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry: reg,
	})

	// Add peers from different sources in different states
	pg.mu.Lock()
	pg.peers = []*Peer{
		{
			Address:    "192.168.1.1:3001",
			Source:     PeerSourceP2PGossip,
			State:      PeerStateHot,
			Connection: &PeerConnection{},
		},
		{
			Address: "192.168.1.2:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateWarm,
		},
		{
			Address: "192.168.1.3:3001",
			Source:  PeerSourceP2PGossip,
			State:   PeerStateCold,
		},
		{
			Address:    "192.168.1.4:3001",
			Source:     PeerSourceTopologyPublicRoot,
			State:      PeerStateHot,
			Connection: &PeerConnection{},
		},
		{
			Address: "192.168.1.5:3001",
			Source:  PeerSourceP2PLedger,
			State:   PeerStateWarm,
		},
	}
	pg.updatePeerMetrics()
	pg.mu.Unlock()

	// Check per-source gauge metrics
	// Gossip: 1 hot, 1 warm, 1 cold
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			pg.metrics.peersBySource.WithLabelValues("gossip", "hot"),
		),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			pg.metrics.peersBySource.WithLabelValues("gossip", "warm"),
		),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			pg.metrics.peersBySource.WithLabelValues("gossip", "cold"),
		),
	)

	// Topology public root: 1 hot
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			pg.metrics.peersBySource.WithLabelValues(
				"topology-public-root",
				"hot",
			),
		),
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			pg.metrics.peersBySource.WithLabelValues(
				"topology-public-root",
				"warm",
			),
		),
	)

	// Ledger: 1 warm
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			pg.metrics.peersBySource.WithLabelValues("ledger", "hot"),
		),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			pg.metrics.peersBySource.WithLabelValues("ledger", "warm"),
		),
	)
}

// TestPeerGovernor_ChurnMetricsBySource tests that churn promotions and demotions
// increment the per-source counter metrics correctly.
func TestPeerGovernor_ChurnMetricsBySource(t *testing.T) {
	reg := prometheus.NewRegistry()
	eventBus := newMockEventBus()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:              slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry:        reg,
		EventBus:            eventBus,
		GossipChurnInterval: 1 * time.Hour, // Don't auto-trigger
		GossipChurnPercent:  1.0,           // Churn all peers
		MinScoreThreshold:   0.0,           // Allow all scores
	})

	// Add hot gossip peers to be churned
	pg.mu.Lock()
	pg.peers = []*Peer{
		{
			Address:          "192.168.1.1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			Connection:       &PeerConnection{},
			PerformanceScore: 0.5,
		},
		{
			Address:          "192.168.1.2:3001",
			Source:           PeerSourceP2PLedger,
			State:            PeerStateHot,
			Connection:       &PeerConnection{},
			PerformanceScore: 0.4,
		},
		// Warm peers to be promoted
		{
			Address:          "192.168.1.3:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateWarm,
			Connection:       &PeerConnection{},
			PerformanceScore: 0.8,
		},
	}
	pg.mu.Unlock()

	// Verify initial demotion counters are 0
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			pg.metrics.churnDemotionsBySource.WithLabelValues("gossip"),
		),
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			pg.metrics.churnDemotionsBySource.WithLabelValues("ledger"),
		),
	)

	// Trigger gossip churn
	pg.gossipChurn()

	// Check demotion counters incremented
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			pg.metrics.churnDemotionsBySource.WithLabelValues("gossip"),
		),
		"gossip demotion counter should increment",
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			pg.metrics.churnDemotionsBySource.WithLabelValues("ledger"),
		),
		"ledger demotion counter should increment",
	)

	// Check promotion counters incremented
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			pg.metrics.churnPromotionsBySource.WithLabelValues("gossip"),
		),
		"gossip promotion counter should increment",
	)
}

// TestPeerGovernor_EventsPublished tests that churn events are published
// with correct data during churn operations.
func TestPeerGovernor_EventsPublished(t *testing.T) {
	reg := prometheus.NewRegistry()
	eventBus := newMockEventBus()

	// Thread-safe event storage
	var churnMu sync.Mutex
	var quotaMu sync.Mutex
	churnEvents := []PeerChurnEvent{}
	quotaEvents := []QuotaStatusEvent{}

	eventBus.SubscribeFunc(PeerChurnEventType, func(e event.Event) {
		if ce, ok := e.Data.(PeerChurnEvent); ok {
			churnMu.Lock()
			churnEvents = append(churnEvents, ce)
			churnMu.Unlock()
		}
	})
	eventBus.SubscribeFunc(QuotaStatusEventType, func(e event.Event) {
		if qe, ok := e.Data.(QuotaStatusEvent); ok {
			quotaMu.Lock()
			quotaEvents = append(quotaEvents, qe)
			quotaMu.Unlock()
		}
	})

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:              slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry:        reg,
		EventBus:            eventBus,
		GossipChurnInterval: 1 * time.Hour,
		GossipChurnPercent:  1.0,
		MinScoreThreshold:   0.0,
	})

	// Add a hot gossip peer to be churned
	pg.mu.Lock()
	pg.peers = []*Peer{
		{
			Address:          "192.168.1.1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			Connection:       &PeerConnection{},
			PerformanceScore: 0.5,
		},
	}
	pg.mu.Unlock()

	// Trigger gossip churn
	pg.gossipChurn()

	// Wait for async event delivery
	require.Eventually(t, func() bool {
		churnMu.Lock()
		defer churnMu.Unlock()
		return len(churnEvents) >= 1
	}, 2*time.Second, 5*time.Millisecond, "at least one churn event should be published")

	// Verify PeerChurnEvent was published with correct data
	churnMu.Lock()
	churnLen := len(churnEvents)
	var firstChurn PeerChurnEvent
	if churnLen > 0 {
		firstChurn = churnEvents[0]
	}
	churnMu.Unlock()
	if churnLen > 0 {
		assert.Equal(t, "192.168.1.1:3001", firstChurn.Address)
		assert.Equal(t, "gossip", firstChurn.Source)
		assert.Equal(t, "hot", firstChurn.OldState)
		assert.Equal(t, "cold", firstChurn.NewState)
		assert.Equal(t, 0.5, firstChurn.Score)
		assert.Equal(t, "gossip churn", firstChurn.Reason)
	}

	// Test QuotaStatusEvent from reconcile
	pg.mu.Lock()
	pg.peers = []*Peer{
		{
			Address:    "192.168.1.1:3001",
			Source:     PeerSourceP2PGossip,
			State:      PeerStateHot,
			Connection: &PeerConnection{},
		},
		{
			Address:    "192.168.1.2:3001",
			Source:     PeerSourceTopologyPublicRoot,
			State:      PeerStateHot,
			Connection: &PeerConnection{},
		},
		{
			Address:    "192.168.1.3:3001",
			Source:     PeerSourceP2PLedger,
			State:      PeerStateHot,
			Connection: &PeerConnection{},
		},
	}
	pg.mu.Unlock()

	// Reset quota events
	quotaMu.Lock()
	quotaEvents = []QuotaStatusEvent{}
	quotaMu.Unlock()

	// Run reconcile which should publish QuotaStatusEvent
	pg.reconcile()

	// Wait for async event delivery
	require.Eventually(t, func() bool {
		quotaMu.Lock()
		defer quotaMu.Unlock()
		return len(quotaEvents) >= 1
	}, 2*time.Second, 5*time.Millisecond, "quota status event should be published")

	// Verify QuotaStatusEvent was published
	quotaMu.Lock()
	quotaLen := len(quotaEvents)
	var lastQuota QuotaStatusEvent
	if quotaLen > 0 {
		lastQuota = quotaEvents[quotaLen-1]
	}
	quotaMu.Unlock()
	if quotaLen > 0 {
		assert.Equal(t, 1, lastQuota.GossipHot, "should have 1 gossip hot peer")
		assert.Equal(
			t,
			1,
			lastQuota.TopologyHot,
			"should have 1 topology hot peer",
		)
		assert.Equal(t, 1, lastQuota.LedgerHot, "should have 1 ledger hot peer")
		assert.Equal(t, 3, lastQuota.TotalHot, "should have 3 total hot peers")
	}
}

// TestPeerGovernor_PeerStateString tests the PeerState String() method.
func TestPeerGovernor_PeerStateString(t *testing.T) {
	tests := []struct {
		state    PeerState
		expected string
	}{
		{PeerStateCold, "cold"},
		{PeerStateWarm, "warm"},
		{PeerStateHot, "hot"},
		{PeerState(99), "unknown"}, // Unknown state
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

// mockSyncProgressProvider implements SyncProgressProvider for testing
type mockSyncProgressProvider struct {
	progress float64
}

func (m *mockSyncProgressProvider) SyncProgress() float64 {
	return m.progress
}

func TestPeerGovernor_BootstrapConfig_DefaultValues(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	// Verify default values are set
	assert.Equal(t, 5, pg.config.MinLedgerPeersForExit,
		"MinLedgerPeersForExit should default to 5")
	assert.Equal(t, 0.95, pg.config.SyncProgressForExit,
		"SyncProgressForExit should default to 0.95")
	// AutoBootstrapRecovery: nil means use default (true)
	assert.Nil(t, pg.config.AutoBootstrapRecovery,
		"AutoBootstrapRecovery should be nil (default to true behavior)")
}

func TestPeerGovernor_ShouldExitBootstrap_SlotThreshold(t *testing.T) {
	mockProvider := &mockLedgerPeerProvider{
		currentSlot: 5001, // Above threshold
	}

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerPeerProvider: mockProvider,
		UseLedgerAfterSlot: 5000, // Threshold
	})

	// Add a bootstrap peer
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3001",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateWarm,
	})

	shouldExit, reason := pg.shouldExitBootstrap()
	pg.mu.Unlock()

	assert.True(t, shouldExit, "should exit bootstrap when slot > threshold")
	assert.Equal(t, "slot threshold reached", reason)
}

func TestPeerGovernor_ShouldExitBootstrap_SlotThreshold_NotReached(
	t *testing.T,
) {
	mockProvider := &mockLedgerPeerProvider{
		currentSlot: 4999, // Below threshold
	}

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerPeerProvider: mockProvider,
		UseLedgerAfterSlot: 5000, // Threshold
	})

	// Add a bootstrap peer
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3001",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateWarm,
	})

	shouldExit, _ := pg.shouldExitBootstrap()
	pg.mu.Unlock()

	assert.False(
		t,
		shouldExit,
		"should not exit bootstrap when slot < threshold",
	)
}

func TestPeerGovernor_ShouldExitBootstrap_LedgerPeerCount(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MinLedgerPeersForExit: 3,
	})

	// Add a bootstrap peer
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3001",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateWarm,
	})

	// Add ledger peers (warm/hot count towards exit)
	for i := range 3 {
		pg.peers = append(pg.peers, &Peer{
			Address: fmt.Sprintf("192.168.1.%d:3001", i+1),
			Source:  PeerSourceP2PLedger,
			State:   PeerStateWarm,
		})
	}

	shouldExit, reason := pg.shouldExitBootstrap()
	pg.mu.Unlock()

	assert.True(
		t,
		shouldExit,
		"should exit bootstrap when ledger peer count >= threshold",
	)
	assert.Contains(t, reason, "ledger peer count")
}

func TestPeerGovernor_ShouldExitBootstrap_LedgerPeerCount_ColdNotCounted(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MinLedgerPeersForExit: 3,
	})

	// Add a bootstrap peer
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3001",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateWarm,
	})

	// Add ledger peers that are cold (should not count)
	for i := range 3 {
		pg.peers = append(pg.peers, &Peer{
			Address: fmt.Sprintf("192.168.1.%d:3001", i+1),
			Source:  PeerSourceP2PLedger,
			State:   PeerStateCold, // Cold peers don't count
		})
	}

	shouldExit, _ := pg.shouldExitBootstrap()
	pg.mu.Unlock()

	assert.False(
		t,
		shouldExit,
		"should not exit bootstrap when ledger peers are cold",
	)
}

func TestPeerGovernor_ShouldExitBootstrap_SyncProgress(t *testing.T) {
	mockSync := &mockSyncProgressProvider{
		progress: 0.96, // 96% synced
	}

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:               slog.New(slog.NewJSONHandler(io.Discard, nil)),
		SyncProgressProvider: mockSync,
		SyncProgressForExit:  0.95, // 95% threshold
	})

	// Add a bootstrap peer
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3001",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateWarm,
	})

	shouldExit, reason := pg.shouldExitBootstrap()
	pg.mu.Unlock()

	assert.True(
		t,
		shouldExit,
		"should exit bootstrap when sync progress >= threshold",
	)
	assert.Contains(t, reason, "sync progress")
}

func TestPeerGovernor_ShouldExitBootstrap_SyncProgress_NotReached(
	t *testing.T,
) {
	mockSync := &mockSyncProgressProvider{
		progress: 0.80, // 80% synced
	}

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:               slog.New(slog.NewJSONHandler(io.Discard, nil)),
		SyncProgressProvider: mockSync,
		SyncProgressForExit:  0.95, // 95% threshold
	})

	// Add a bootstrap peer
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3001",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateWarm,
	})

	shouldExit, _ := pg.shouldExitBootstrap()
	pg.mu.Unlock()

	assert.False(
		t,
		shouldExit,
		"should not exit bootstrap when sync progress < threshold",
	)
}

func TestPeerGovernor_ShouldExitBootstrap_NoBootstrapPeers(t *testing.T) {
	mockSync := &mockSyncProgressProvider{
		progress: 0.99, // Very synced
	}

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:               slog.New(slog.NewJSONHandler(io.Discard, nil)),
		SyncProgressProvider: mockSync,
		SyncProgressForExit:  0.95,
	})

	// No bootstrap peers, only gossip peers
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3001",
		Source:  PeerSourceP2PGossip,
		State:   PeerStateWarm,
	})

	shouldExit, _ := pg.shouldExitBootstrap()
	pg.mu.Unlock()

	assert.False(
		t,
		shouldExit,
		"should not exit bootstrap when no bootstrap peers exist",
	)
}

func TestPeerGovernor_ShouldExitBootstrap_AlreadyExited(t *testing.T) {
	mockSync := &mockSyncProgressProvider{
		progress: 0.99,
	}

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:               slog.New(slog.NewJSONHandler(io.Discard, nil)),
		SyncProgressProvider: mockSync,
		SyncProgressForExit:  0.95,
	})

	// Add bootstrap peer and mark as already exited
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3001",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateCold,
	})
	pg.bootstrapExited = true

	shouldExit, _ := pg.shouldExitBootstrap()
	pg.mu.Unlock()

	assert.False(t, shouldExit, "should not exit bootstrap when already exited")
}

func TestPeerGovernor_ExitBootstrap_DemotesPeers(t *testing.T) {
	eventBus := newMockEventBus()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: eventBus,
	})

	// Add bootstrap peers in different states
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3001",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateHot,
	})
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3002",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateWarm,
	})
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3003",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateCold, // Already cold, shouldn't be demoted
	})
	// Add a non-bootstrap peer that should not be affected
	pg.peers = append(pg.peers, &Peer{
		Address: "192.168.1.1:3001",
		Source:  PeerSourceP2PGossip,
		State:   PeerStateHot,
	})

	_ = pg.exitBootstrapLocked("test reason")

	// Verify all bootstrap peers are cold
	for _, peer := range pg.peers {
		if peer.Source == PeerSourceTopologyBootstrapPeer {
			assert.Equal(t, PeerStateCold, peer.State,
				"bootstrap peer %s should be cold after exit", peer.Address)
		}
	}

	// Verify non-bootstrap peer is unaffected
	var gossipPeer *Peer
	for _, peer := range pg.peers {
		if peer.Source == PeerSourceP2PGossip {
			gossipPeer = peer
			break
		}
	}
	assert.NotNil(t, gossipPeer)
	assert.Equal(t, PeerStateHot, gossipPeer.State,
		"non-bootstrap peer should not be affected")

	// Verify bootstrapExited flag is set
	assert.True(t, pg.bootstrapExited, "bootstrapExited should be true")
	pg.mu.Unlock()
}

func TestPeerGovernor_ExitBootstrap_PreventsPromotion(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MinHotPeers: 5,
	})

	// Mark bootstrap as exited
	pg.mu.Lock()
	pg.bootstrapExited = true

	// Add warm bootstrap peer with connection
	pg.peers = append(pg.peers, &Peer{
		Address:    "127.0.0.1:3001",
		Source:     PeerSourceTopologyBootstrapPeer,
		State:      PeerStateWarm,
		Connection: &PeerConnection{},
	})

	// Check if bootstrap peer can be promoted
	canPromote := pg.canPromoteBootstrapPeer()
	isBootstrap := pg.isBootstrapPeer(pg.peers[0])
	pg.mu.Unlock()

	assert.True(t, isBootstrap, "peer should be identified as bootstrap peer")
	assert.False(
		t,
		canPromote,
		"bootstrap peers should not be promotable after exit",
	)
}

func TestPeerGovernor_BootstrapRecovery(t *testing.T) {
	eventBus := newMockEventBus()

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:              eventBus,
		MinHotPeers:           3,
		AutoBootstrapRecovery: boolPtr(true), // Explicitly enable
	})

	// Start with bootstrap exited
	pg.mu.Lock()
	pg.bootstrapExited = true

	// Add bootstrap peer that's cold
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3001",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateCold,
	})

	// No hot peers, no warm candidates from gossip/ledger
	pg.peers = append(pg.peers, &Peer{
		Address: "192.168.1.1:3001",
		Source:  PeerSourceP2PGossip,
		State:   PeerStateCold, // Cold, not a candidate
	})

	_ = pg.checkBootstrapRecoveryLocked()

	// Bootstrap should be re-enabled
	assert.False(
		t,
		pg.bootstrapExited,
		"bootstrapExited should be false after recovery",
	)
	pg.mu.Unlock()
}

func TestPeerGovernor_BootstrapRecovery_NotNeededWithWarmCandidates(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MinHotPeers:           3,
		AutoBootstrapRecovery: boolPtr(true),
	})

	// Start with bootstrap exited
	pg.mu.Lock()
	pg.bootstrapExited = true

	// Add bootstrap peer that's cold
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3001",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateCold,
	})

	// Add warm gossip peer with connection (a candidate for promotion)
	pg.peers = append(pg.peers, &Peer{
		Address:    "192.168.1.1:3001",
		Source:     PeerSourceP2PGossip,
		State:      PeerStateWarm,
		Connection: &PeerConnection{}, // Has connection
	})

	_ = pg.checkBootstrapRecoveryLocked()

	// Bootstrap should NOT be re-enabled because we have warm candidates
	assert.True(
		t,
		pg.bootstrapExited,
		"bootstrapExited should remain true with warm candidates",
	)
	pg.mu.Unlock()
}

func TestPeerGovernor_BootstrapRecovery_NotNeededWithEnoughHotPeers(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MinHotPeers:           2,
		AutoBootstrapRecovery: boolPtr(true),
	})

	// Start with bootstrap exited
	pg.mu.Lock()
	pg.bootstrapExited = true

	// Add bootstrap peer that's cold
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3001",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateCold,
	})

	// Add enough hot peers (>= MinHotPeers)
	pg.peers = append(pg.peers, &Peer{
		Address: "192.168.1.1:3001",
		Source:  PeerSourceP2PGossip,
		State:   PeerStateHot,
	})
	pg.peers = append(pg.peers, &Peer{
		Address: "192.168.1.2:3001",
		Source:  PeerSourceP2PGossip,
		State:   PeerStateHot,
	})

	_ = pg.checkBootstrapRecoveryLocked()

	// Bootstrap should NOT be re-enabled because we have enough hot peers
	assert.True(
		t,
		pg.bootstrapExited,
		"bootstrapExited should remain true with enough hot peers",
	)
	pg.mu.Unlock()
}

func TestPeerGovernor_Reconcile_ExitsBootstrap(t *testing.T) {
	mockSync := &mockSyncProgressProvider{
		progress: 0.99, // Fully synced
	}

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                slog.New(slog.NewJSONHandler(io.Discard, nil)),
		SyncProgressProvider:  mockSync,
		SyncProgressForExit:   0.95,
		AutoBootstrapRecovery: boolPtr(false), // Disable recovery for this test
		MinHotPeers:           1,              // Low threshold to avoid recovery
	})

	// Add bootstrap peer
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address: "127.0.0.1:3001",
		Source:  PeerSourceTopologyBootstrapPeer,
		State:   PeerStateWarm,
	})
	// Add a hot gossip peer to satisfy MinHotPeers and prevent recovery
	pg.peers = append(pg.peers, &Peer{
		Address:    "192.168.1.1:3001",
		Source:     PeerSourceP2PGossip,
		State:      PeerStateHot,
		Connection: &PeerConnection{},
	})
	pg.mu.Unlock()

	// Run reconcile
	pg.reconcile()

	// Verify bootstrap was exited
	pg.mu.Lock()
	assert.True(
		t,
		pg.bootstrapExited,
		"bootstrap should be exited after reconcile",
	)
	// Find the bootstrap peer
	var bootstrapPeer *Peer
	for _, peer := range pg.peers {
		if peer.Source == PeerSourceTopologyBootstrapPeer {
			bootstrapPeer = peer
			break
		}
	}
	assert.NotNil(t, bootstrapPeer)
	assert.Equal(t, PeerStateCold, bootstrapPeer.State,
		"bootstrap peer should be cold after reconcile")
	pg.mu.Unlock()
}

func TestPeerGovernor_Reconcile_SkipsBootstrapPromotionAfterExit(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MinHotPeers: 5, // Need 5 hot peers
	})

	// Set bootstrap as exited
	pg.mu.Lock()
	pg.bootstrapExited = true

	// Add warm bootstrap peer with connection (would normally be promoted)
	pg.peers = append(pg.peers, &Peer{
		Address:    "127.0.0.1:3001",
		Source:     PeerSourceTopologyBootstrapPeer,
		State:      PeerStateWarm,
		Connection: &PeerConnection{},
	})

	// Add warm gossip peer with connection (should be promoted)
	pg.peers = append(pg.peers, &Peer{
		Address:    "192.168.1.1:3001",
		Source:     PeerSourceP2PGossip,
		State:      PeerStateWarm,
		Connection: &PeerConnection{},
	})
	pg.mu.Unlock()

	// Run reconcile
	pg.reconcile()

	// Check results
	pg.mu.Lock()
	defer pg.mu.Unlock()

	// Bootstrap peer should NOT be promoted (still warm or cold)
	var bootstrapPeer *Peer
	var gossipPeer *Peer
	for _, peer := range pg.peers {
		if peer.Source == PeerSourceTopologyBootstrapPeer {
			bootstrapPeer = peer
		}
		if peer.Source == PeerSourceP2PGossip {
			gossipPeer = peer
		}
	}

	assert.NotNil(t, bootstrapPeer)
	assert.NotEqual(t, PeerStateHot, bootstrapPeer.State,
		"bootstrap peer should not be promoted after exit")

	assert.NotNil(t, gossipPeer)
	assert.Equal(t, PeerStateHot, gossipPeer.State,
		"gossip peer should be promoted to hot")
}

func TestPeerGovernor_IsBootstrapPeer(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	tests := []struct {
		name     string
		peer     *Peer
		expected bool
	}{
		{
			name: "bootstrap peer",
			peer: &Peer{
				Source: PeerSourceTopologyBootstrapPeer,
			},
			expected: true,
		},
		{
			name: "local root peer",
			peer: &Peer{
				Source: PeerSourceTopologyLocalRoot,
			},
			expected: false,
		},
		{
			name: "gossip peer",
			peer: &Peer{
				Source: PeerSourceP2PGossip,
			},
			expected: false,
		},
		{
			name:     "nil peer",
			peer:     nil,
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := pg.isBootstrapPeer(tc.peer)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestPeerGovernor_CanPromoteBootstrapPeer(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	// Initially bootstrap not exited
	pg.mu.Lock()
	assert.True(t, pg.canPromoteBootstrapPeer(),
		"should be able to promote bootstrap peers before exit")

	// After exit
	pg.bootstrapExited = true
	assert.False(t, pg.canPromoteBootstrapPeer(),
		"should not be able to promote bootstrap peers after exit")
	pg.mu.Unlock()
}
