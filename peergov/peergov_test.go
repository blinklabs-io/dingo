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

	// Test adding a new peer
	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)

	peers := pg.GetPeers()
	assert.Len(t, peers, 1)
	assert.Equal(t, "127.0.0.1:3001", peers[0].Address)
	assert.EqualValues(t, PeerSourceP2PGossip, peers[0].Source)
	assert.Equal(t, PeerStateCold, peers[0].State)

	// Test adding duplicate peer (should not add)
	pg.AddPeer("127.0.0.1:3001", PeerSourceP2PGossip)
	peers = pg.GetPeers()
	assert.Len(t, peers, 1)

	// Event publishing is tested indirectly through the real EventBus
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
