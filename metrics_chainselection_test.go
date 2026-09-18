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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
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
	n.registerChainSelectionMetrics()
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
