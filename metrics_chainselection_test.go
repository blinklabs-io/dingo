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

package dingo

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/promutil"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMetricsTestNode(t *testing.T) (*Node, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	n := &Node{
		config: Config{
			logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
			promRegistry: registry,
		},
	}
	n.registerChainSelectionMetrics(promutil.NewRegistration(registry))
	require.NotNil(t, n.chainSelectionMetrics)
	return n, registry
}

// counterValues returns label value -> counter value for the named metric.
func counterValues(
	t *testing.T,
	registry *prometheus.Registry,
	name string,
) map[string]float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	values := map[string]float64{}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			var label string
			for _, pair := range metric.GetLabel() {
				label = pair.GetValue()
			}
			values[label] = metric.GetCounter().GetValue()
		}
	}
	return values
}

// Both counters materialize every label value at registration, so a scrape
// before the first occurrence reports an explicit 0 rather than a missing
// series. An alert on a stall counter that only appears after the first stall
// cannot distinguish "healthy" from "not reporting".
func TestChainSelectionMetricsPreMaterializeLabels(t *testing.T) {
	_, registry := newMetricsTestNode(t)

	stalls := counterValues(t, registry, "dingo_chainselection_stalled_total")
	assert.Equal(t, map[string]float64{
		chainSelectionStallNoSelectablePeer:     0,
		chainSelectionStallGenesisCorroboration: 0,
	}, stalls)

	registrations := counterValues(
		t,
		registry,
		"dingo_chainselection_rollback_registrations_total",
	)
	assert.Equal(t, map[string]float64{
		string(chainselection.RollbackRegistrationRegistered):       0,
		string(chainselection.RollbackRegistrationClosedConnection): 0,
		string(chainselection.RollbackRegistrationImplausibleTip):   0,
		string(chainselection.RollbackRegistrationAtCapacity):       0,
	}, registrations)
}

// The selected-to-none handler that logs "chain selection stalled" also counts
// the stall, labelled by whether the Genesis corroboration gate caused it.
func TestHandleChainSelectedNoneEventCountsStall(t *testing.T) {
	n, registry := newMetricsTestNode(t)
	n.chainsyncState = chainsync.NewStateWithConfig(
		nil,
		nil,
		chainsync.DefaultConfig(),
	)
	conn := newNodeTestConnId(3301)

	n.handleChainSelectedNoneEvent(event.NewEvent(
		chainselection.ChainSelectedNoneEventType,
		chainselection.ChainSelectedNoneEvent{PreviousConnectionId: conn},
	))
	n.handleChainSelectedNoneEvent(event.NewEvent(
		chainselection.ChainSelectedNoneEventType,
		chainselection.ChainSelectedNoneEvent{
			PreviousConnectionId: conn,
			GenesisCorroboration: true,
		},
	))

	assert.Equal(t, map[string]float64{
		chainSelectionStallNoSelectablePeer:     1,
		chainSelectionStallGenesisCorroboration: 1,
	}, counterValues(t, registry, "dingo_chainselection_stalled_total"))
}

// buildChainSelectorConfig is the composition site the running node uses, so
// the rollback-registration hook is verified end to end from there: build the
// config the binary builds, hand it to a real ChainSelector, drive the rollback
// a recycled connection produces, and require the node's counter to move. A
// hook that is defined in chainselection but never set here is invisible at
// runtime.
func TestBuildChainSelectorConfigWiresRollbackRegistrationCounter(
	t *testing.T,
) {
	n, registry := newMetricsTestNode(t)
	conn := newNodeTestConnId(3302)
	rollback := event.NewEvent(
		chainselection.PeerRollbackEventType,
		chainselection.PeerRollbackEvent{
			ConnectionId: conn,
			Point:        ocommon.NewPoint(2614270, []byte("intersect")),
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(2614276, []byte("peer-tip")),
				BlockNumber: 2614276,
			},
		},
	)
	registrations := func() map[string]float64 {
		return counterValues(
			t,
			registry,
			"dingo_chainselection_rollback_registrations_total",
		)
	}

	// As composed: this node has no connection manager, so the config's own
	// ConnectionLive hook reports the connection as dead and registration is
	// refused. The refusal is still reported through OnRollbackRegistration.
	refusing := n.buildChainSelectorConfig(2160, false, 0)
	require.NotNil(t, refusing.OnRollbackRegistration)
	refusing.DisableEventSubscriptions = true
	refusingSelector := chainselection.NewChainSelector(refusing)
	refusingSelector.HandlePeerRollbackEvent(rollback)
	require.Equal(t, 0, refusingSelector.PeerCount())
	assert.Equal(
		t,
		float64(1),
		registrations()[string(
			chainselection.RollbackRegistrationClosedConnection,
		)],
	)

	// With a live connection, the same composed config registers the peer and
	// counts it. Only ConnectionLive is stubbed (this node has no connManager);
	// the hook under test is the one buildChainSelectorConfig installed.
	live := n.buildChainSelectorConfig(2160, false, 0)
	live.DisableEventSubscriptions = true
	live.ConnectionLive = func(ouroboros.ConnectionId) bool { return true }
	liveSelector := chainselection.NewChainSelector(live)
	liveSelector.HandlePeerRollbackEvent(rollback)

	require.Equal(t, 1, liveSelector.PeerCount())
	best := liveSelector.GetBestPeer()
	require.NotNil(t, best)
	assert.Equal(t, conn, *best)
	assert.Equal(
		t,
		float64(1),
		registrations()[string(chainselection.RollbackRegistrationRegistered)],
	)
}

// The corroboration identity hook is installed by the composition site; a
// hook left unset silently falls back to grouping witnesses by remote host.
func TestBuildChainSelectorConfigWiresPeerIdentity(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Stop)
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger, EventBus: bus},
	)
	t.Cleanup(func() {
		assert.NoError(t, connManager.Stop(context.Background()))
	})

	localAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6000}
	remoteAddr := &net.TCPAddr{IP: net.IPv4(44, 0, 0, 1), Port: 3001}
	localWire, peerWire := newLeiosNotifyTestConnPair(localAddr, remoteAddr)
	t.Cleanup(func() {
		_ = localWire.Close()
		_ = peerWire.Close()
	})
	type connectionResult struct {
		conn *ouroboros.Connection
		err  error
	}
	localResult := make(chan connectionResult, 1)
	peerResult := make(chan connectionResult, 1)
	go func() {
		conn, err := ouroboros.NewConnection(
			ouroboros.WithConnection(localWire),
			ouroboros.WithNetworkMagic(42),
			ouroboros.WithNodeToNode(true),
			ouroboros.WithServer(true),
		)
		localResult <- connectionResult{conn: conn, err: err}
	}()
	go func() {
		conn, err := ouroboros.NewConnection(
			ouroboros.WithConnection(peerWire),
			ouroboros.WithNetworkMagic(42),
			ouroboros.WithNodeToNode(true),
		)
		peerResult <- connectionResult{conn: conn, err: err}
	}()
	receiveConnection := func(ch <-chan connectionResult) connectionResult {
		t.Helper()
		select {
		case result := <-ch:
			return result
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for local Ouroboros handshake")
			return connectionResult{}
		}
	}
	local := receiveConnection(localResult)
	peer := receiveConnection(peerResult)
	require.NoError(t, local.err)
	require.NoError(t, peer.err)
	t.Cleanup(func() {
		_ = local.conn.Close()
		_ = peer.conn.Close()
	})

	conn := local.conn.Id()
	require.Equal(t, localAddr.String(), conn.LocalAddr.String())
	require.Equal(t, remoteAddr.String(), conn.RemoteAddr.String())
	require.True(t, connManager.AddConnection(local.conn, true, remoteAddr.String()))

	n, _ := newMetricsTestNode(t)
	n.eventBus = bus
	n.connManager = connManager
	cfg := n.buildChainSelectorConfig(2160, true, 0)
	require.NotNil(t, cfg.PeerIdentity)
	peerGovs := []*peergov.PeerGovernor{
		peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
			Logger: logger, EventBus: bus, ConnManager: connManager,
			DisableOutbound: true,
		}),
		peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
			Logger: logger, EventBus: bus, ConnManager: connManager,
			DisableOutbound: true,
		}),
	}
	const expectedGroup = "ipv4:44.0.0.0/24"
	for _, peerGov := range peerGovs {
		t.Cleanup(func() {
			assert.NoError(t, peerGov.Stop(context.Background()))
		})
		require.NoError(t, peerGov.AddPeer(
			remoteAddr.String(),
			peergov.PeerSourceTopologyLocalRoot,
		))
		require.NoError(t, peerGov.Start(context.Background()))
		n.setPeerGovernor(peerGov)
		bus.Publish(
			connmanager.InboundConnectionEventType,
			event.NewEvent(
				connmanager.InboundConnectionEventType,
				connmanager.InboundConnectionEvent{
					ConnectionId:         conn,
					LocalAddr:            conn.LocalAddr,
					RemoteAddr:           conn.RemoteAddr,
					NormalizedRemoteAddr: connmanager.NormalizePeerAddr(remoteAddr.String()),
				},
			),
		)
		require.Eventually(t, func() bool {
			return peerGov.DiversityGroupByConnId(conn) == expectedGroup &&
				cfg.PeerIdentity(conn) == expectedGroup
		}, time.Second, time.Millisecond)
		require.NoError(t, peerGov.Stop(context.Background()))
	}
	n.setPeerGovernor(peerGovs[0])
	assert.Equal(t, expectedGroup, cfg.PeerIdentity(conn))

	// Live restore replaces this pointer while the retained selector remains
	// active. Exercise the same synchronized read alongside repeated replacement.
	start := make(chan struct{})
	groups := make(chan string, 64)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for range 64 {
			groups <- cfg.PeerIdentity(conn)
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := range 64 {
			n.setPeerGovernor(peerGovs[i%len(peerGovs)])
		}
	}()
	close(start)
	workers.Wait()
	close(groups)
	for got := range groups {
		assert.Equal(t, expectedGroup, got)
	}
}

// New() registers the chain-selection counters, so they exist for the node's
// whole lifetime rather than only after a component that happens to touch them
// is built. Registration must happen against the pre-wrap registerer (see
// registerChainSelectionMetrics), because a live database restore unregisters
// everything registered through the rebuildable wrapper and nothing rebuilds
// the ChainSelector.
func TestNewRegistersChainSelectionMetrics(t *testing.T) {
	cardanoCfg := newNodeTestCardanoNodeCfg(t)
	registry := prometheus.NewRegistry()
	n, err := New(NewConfig(
		WithDatabasePath(t.TempDir()),
		WithCardanoNodeConfig(cardanoCfg),
		WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		WithPrometheusRegistry(registry),
		WithStorageMode(StorageModeAPI),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		WithShutdownTimeout(5*time.Second),
	))
	require.NoError(t, err)
	t.Cleanup(func() { _ = n.Stop() })

	assert.Contains(
		t,
		counterValues(t, registry, "dingo_chainselection_stalled_total"),
		chainSelectionStallNoSelectablePeer,
	)
	assert.Contains(
		t,
		counterValues(
			t,
			registry,
			"dingo_chainselection_rollback_registrations_total",
		),
		string(chainselection.RollbackRegistrationRegistered),
	)
}

// The composed selector config installs the Genesis Density Disconnector
// callback; a reported peer is counted and put on peer governance's deny list
// so it is not redialed straight away.
func TestBuildChainSelectorConfigWiresGenesisDensityDisconnect(t *testing.T) {
	t.Parallel()
	n, registry := newMetricsTestNode(t)
	n.peerGov = peergov.NewPeerGovernor(peergov.PeerGovernorConfig{})
	conn := newNodeTestConnId(3303)

	cfg := n.buildChainSelectorConfig(2160, true, 0)
	require.NotNil(t, cfg.OnGenesisDensityDisconnect)
	require.False(t, n.peerGov.IsDenied(conn.RemoteAddr.String()))

	cfg.OnGenesisDensityDisconnect(chainselection.GenesisDensityDisconnect{
		ConnectionId: conn,
	})

	assert.Equal(
		t,
		map[string]float64{"": 1},
		counterValues(
			t,
			registry,
			"dingo_chainselection_gdd_disconnects_total",
		),
	)
	assert.True(t, n.peerGov.IsDenied(conn.RemoteAddr.String()))
}

// The disconnect log reports whether the peer was actually denied: a
// connection ID with no remote address cannot be put on the deny list, and a
// log that only carries the deny duration would claim a denial that did not
// happen.
func TestGenesisDensityDisconnectLogReportsDenial(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	n, _ := newMetricsTestNode(t)
	n.config.logger = slog.New(slog.NewTextHandler(&logs, nil))
	n.peerGov = peergov.NewPeerGovernor(peergov.PeerGovernorConfig{})

	withAddr := newNodeTestConnId(3304)
	n.onGenesisDensityDisconnect(chainselection.GenesisDensityDisconnect{
		ConnectionId: withAddr,
	})
	require.Contains(t, logs.String(), "denied=true")
	require.True(t, n.peerGov.IsDenied(withAddr.RemoteAddr.String()))

	logs.Reset()
	noAddr := newNodeTestConnId(3305)
	noAddr.RemoteAddr = nil
	n.onGenesisDensityDisconnect(chainselection.GenesisDensityDisconnect{
		ConnectionId: noAddr,
	})
	require.Contains(t, logs.String(), "denied=false")
}
