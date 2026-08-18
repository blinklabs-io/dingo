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

package ouroboros

import (
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// wiringTestConnId builds a throwaway ConnectionId for wiring tests.
func wiringTestConnId(t *testing.T) ouroboros.ConnectionId {
	t.Helper()
	localAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	require.NoError(t, err)
	remoteAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:3002")
	require.NoError(t, err)
	return ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}
}

func newUnwiredOuroboros() *Ouroboros {
	return newOuroboros(OuroborosConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
}

// TestUnwiredHandleOutboundConnEventDoesNotPanic pins the core defect behind
// this refactor: the dependencies are plain exported fields populated after
// construction, so an event delivered before wiring completes reaches a
// handler that dereferences a nil dependency. HandleOutboundConnEvent guards
// o.peerGov but then calls o.connManager.GetConnectionById unguarded, and
// GetConnectionById locks a mutex on its receiver, so a nil ConnManager
// panics rather than producing a diagnosable error.
func TestUnwiredHandleOutboundConnEventDoesNotPanic(t *testing.T) {
	o := newUnwiredOuroboros()
	evt := event.NewEvent(
		peergov.OutboundConnectionEventType,
		peergov.OutboundConnectionEvent{
			ConnectionId: wiringTestConnId(t),
		},
	)
	require.NotPanics(t, func() { o.HandleOutboundConnEvent(evt) })
}

// TestUnwiredHandleInboundConnEventDoesNotPanic covers the inbound half of the
// same hazard: node.go subscribes this handler to the EventBus before the
// connection manager is wired, so it must tolerate an unwired instance.
func TestUnwiredHandleInboundConnEventDoesNotPanic(t *testing.T) {
	o := newUnwiredOuroboros()
	evt := event.NewEvent(
		connmanager.InboundConnectionEventType,
		connmanager.InboundConnectionEvent{
			ConnectionId: wiringTestConnId(t),
		},
	)
	require.NotPanics(t, func() { o.HandleInboundConnEvent(evt) })
}

// newWiringTestDeps builds a config carrying a complete, valid dependency set.
// Each dependency only has to be non-nil and distinguishable; constructor
// validation never calls into them.
func newWiringTestDeps(t *testing.T) OuroborosConfig {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	ls := newTestLedgerState(t)
	m, err := mempool.NewMempool(mempool.MempoolConfig{
		Logger:          logger,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       ls,
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger},
	)
	return OuroborosConfig{
		Logger:         logger,
		EventBus:       bus,
		LedgerState:    ls,
		Mempool:        &mempool.FIFO{Mempool: m},
		ChainsyncState: chainsync.NewState(bus, ls),
		ConnManager:    connManager,
		PeerGov: peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
			Logger:      logger,
			EventBus:    bus,
			ConnManager: connManager,
		}),
	}
}

func newWiringTestOuroboros(t *testing.T) *Ouroboros {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
}

// TestNewOuroborosRejectsMissingRequiredDependency is the fail-fast contract:
// constructing without a required dependency must fail, naming the field,
// rather than returning an instance whose only symptom is a nil dereference at
// first protocol use.
func TestNewOuroborosRejectsMissingRequiredDependency(t *testing.T) {
	for _, tc := range []struct {
		name  string
		clear func(*OuroborosConfig)
	}{
		{"EventBus", func(c *OuroborosConfig) { c.EventBus = nil }},
		{"LedgerState", func(c *OuroborosConfig) { c.LedgerState = nil }},
		{"Mempool", func(c *OuroborosConfig) { c.Mempool = nil }},
		{"ChainsyncState", func(c *OuroborosConfig) { c.ChainsyncState = nil }},
		{"ConnManager", func(c *OuroborosConfig) { c.ConnManager = nil }},
		{"PeerGov", func(c *OuroborosConfig) { c.PeerGov = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newWiringTestDeps(t)
			tc.clear(&cfg)
			o, err := NewOuroboros(cfg)
			require.Nil(t, o)
			require.ErrorIs(t, err, ErrMissingDependency)
			require.Contains(t, err.Error(), tc.name)
		})
	}
}

// TestNewOuroborosExposesDependencies checks the happy path, and that the
// optional Leios handlers start unset.
func TestNewOuroborosExposesDependencies(t *testing.T) {
	cfg := newWiringTestDeps(t)
	o, err := NewOuroboros(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	require.Same(t, cfg.LedgerState, o.LedgerState())
	require.Same(t, cfg.ChainsyncState, o.ChainsyncState())
	require.Same(t, cfg.ConnManager, o.ConnManager())
	require.Same(t, cfg.PeerGov, o.PeerGov())
	require.Same(t, cfg.EventBus, o.EventBus())
	require.Equal(t, cfg.Mempool, o.Mempool())
	require.Nil(t, o.LeiosVotes())
	require.Nil(t, o.LeiosPipeline())
}

// TestNewOuroborosIsFullyWired pins the property the refactor exists to
// provide: any instance the exported constructor hands back already satisfies
// the dependency invariant, so no protocol handler can observe a half-built
// one.
func TestNewOuroborosIsFullyWired(t *testing.T) {
	o, err := NewOuroboros(newWiringTestDeps(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	require.True(t, o.hasDependencies())
}

// TestSetLeiosHandlersRejectNil guards the optional handlers against being
// silently cleared: node_leios.go wires them from a separate path that reruns
// across live restore cycles, so a nil there is a wiring bug, not a request
// to disable Leios.
func TestSetLeiosHandlersRejectNil(t *testing.T) {
	o := newWiringTestOuroboros(t)
	require.Error(t, o.SetLeiosVotes(nil))
	require.Error(t, o.SetLeiosPipeline(nil))
}
