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
	return NewOuroboros(OuroborosConfig{
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

// newWiringTestDeps builds a complete, valid dependency set. Each dependency
// only has to be non-nil and distinguishable; wiring validation never calls
// into them.
func newWiringTestDeps(t *testing.T) Deps {
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
	return Deps{
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
	return NewOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
}

// TestWireRejectsMissingRequiredDependency is the fail-fast contract: wiring
// with any required dependency absent must return an error naming the field
// rather than leaving the instance in a partially-wired state that only fails
// later, at first protocol use, with a nil dereference.
func TestWireRejectsMissingRequiredDependency(t *testing.T) {
	for _, tc := range []struct {
		name  string
		clear func(*Deps)
	}{
		{"LedgerState", func(d *Deps) { d.LedgerState = nil }},
		{"Mempool", func(d *Deps) { d.Mempool = nil }},
		{"ChainsyncState", func(d *Deps) { d.ChainsyncState = nil }},
		{"ConnManager", func(d *Deps) { d.ConnManager = nil }},
		{"PeerGov", func(d *Deps) { d.PeerGov = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := newWiringTestOuroboros(t)
			deps := newWiringTestDeps(t)
			tc.clear(&deps)
			err := o.Wire(deps)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.name)
		})
	}
}

// TestWireRejectsMissingEventBus covers the one required dependency that
// arrives through OuroborosConfig rather than Deps. NewOuroboros cannot
// report it (it has no error return and 100+ call sites), so Wire is the
// explicit setup-time gate that catches it.
func TestWireRejectsMissingEventBus(t *testing.T) {
	o := newUnwiredOuroboros() // built without an EventBus
	err := o.Wire(newWiringTestDeps(t))
	require.Error(t, err)
	require.Contains(t, err.Error(), "EventBus")
}

// TestWireSucceedsWithRequiredDeps checks the happy path, and that the
// optional Leios handlers may be left unset.
func TestWireSucceedsWithRequiredDeps(t *testing.T) {
	o := newWiringTestOuroboros(t)
	deps := newWiringTestDeps(t)
	require.NoError(t, o.Wire(deps))
	require.Same(t, deps.LedgerState, o.LedgerState())
	require.Same(t, deps.ChainsyncState, o.ChainsyncState())
	require.Same(t, deps.ConnManager, o.ConnManager())
	require.Same(t, deps.PeerGov, o.PeerGov())
	require.Equal(t, deps.Mempool, o.Mempool())
	require.Nil(t, o.LeiosVotes())
	require.Nil(t, o.LeiosPipeline())
}

// TestWireIsRepeatable models the live snapshot/restore path in
// node_lifecycle.go, which tears down and rebuilds the ledger state, mempool,
// chainsync state, connection manager and peer governor while the node is
// running, then rewires the same Ouroboros instance with the replacements.
func TestWireIsRepeatable(t *testing.T) {
	o := newWiringTestOuroboros(t)
	require.NoError(t, o.Wire(newWiringTestDeps(t)))
	rebuilt := newWiringTestDeps(t)
	require.NoError(t, o.Wire(rebuilt))
	require.Same(t, rebuilt.LedgerState, o.LedgerState())
	require.Same(t, rebuilt.ChainsyncState, o.ChainsyncState())
	require.Same(t, rebuilt.ConnManager, o.ConnManager())
	require.Same(t, rebuilt.PeerGov, o.PeerGov())
	require.Equal(t, rebuilt.Mempool, o.Mempool())
}

// TestWireRejectsNilLeiosHandlers guards the optional handlers against being
// silently cleared: node_leios.go wires them from a separate path that reruns
// across live restore cycles, so a nil there is a wiring bug, not a request
// to disable Leios.
func TestWireRejectsNilLeiosHandlers(t *testing.T) {
	o := newWiringTestOuroboros(t)
	require.Error(t, o.SetLeiosVotes(nil))
	require.Error(t, o.SetLeiosPipeline(nil))
}
