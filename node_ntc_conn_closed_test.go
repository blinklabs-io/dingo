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
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	ouroborosPkg "github.com/blinklabs-io/dingo/ouroboros"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// nilIterChainProvider satisfies chainsync.ChainProvider without requiring a
// real database-backed chain. It hands AddClient a nil *chain.ChainIterator,
// which is enough to register server-side (N2C) client state -- the object
// under test here is whether that state is released, not the iterator's own
// Cancel behavior.
type nilIterChainProvider struct{}

func (nilIterChainProvider) GetChainFromPoint(
	_ ocommon.Point,
	_ bool,
) (*chain.ChainIterator, error) {
	return nil, nil
}

func (nilIterChainProvider) StabilityWindow() uint64 { return 0 }

func newNtCTestConnId(port int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: 3001,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: port,
		},
	}
}

func newHandleConnManagerClosedTestNode(t *testing.T) *Node {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	return &Node{
		chainsyncState: chainsync.NewStateWithConfig(
			bus,
			nilIterChainProvider{},
			chainsync.DefaultConfig(),
		),
	}
}

// TestHandleConnManagerClosed_NtC_ReleasesChainsyncClientState reproduces
// issue #3508: NtC connections never received any close notification (the
// EventBus's ConnectionClosedEventType is intentionally NtN-only), so
// chainsync.State.RemoveClient -- which cancels the live chain iterator and
// deletes the per-connection client state -- was never invoked for a closed
// NtC connection. Without handleConnManagerClosed wired as the connection
// manager's ConnClosedFunc, this assertion fails: the client state
// registered by AddClient is still present after the simulated close.
func TestHandleConnManagerClosed_NtC_ReleasesChainsyncClientState(t *testing.T) {
	n := newHandleConnManagerClosedTestNode(t)
	connId := newNtCTestConnId(1)

	_, err := n.chainsyncState.AddClient(connId, ocommon.Point{})
	require.NoError(t, err)
	_, ok := n.chainsyncState.LookupClient(connId)
	require.True(t, ok, "precondition: server-side client state registered")

	n.handleConnManagerClosed(connId, true, nil)

	_, ok = n.chainsyncState.LookupClient(connId)
	require.False(
		t,
		ok,
		"NtC close must release the chainsync server-side client state and its chain iterator",
	)
}

// TestHandleConnManagerClosed_NtN_LeavesStateForEventBusPath guards the
// "exactly once" half of the fix: NtN connections are already cleaned up via
// Ouroboros.HandleConnClosedEvent, subscribed to the EventBus's
// ConnectionClosedEventType. If handleConnManagerClosed also released state
// for isNtC=false, an NtN close would race two independent RemoveClient
// calls instead of exactly one.
func TestHandleConnManagerClosed_NtN_LeavesStateForEventBusPath(t *testing.T) {
	n := newHandleConnManagerClosedTestNode(t)
	connId := newNtCTestConnId(2)

	_, err := n.chainsyncState.AddClient(connId, ocommon.Point{})
	require.NoError(t, err)

	n.handleConnManagerClosed(connId, false, nil)

	_, ok := n.chainsyncState.LookupClient(connId)
	require.True(
		t,
		ok,
		"NtN close must not be released through the NtC-only callback",
	)
}

// TestHandleConnManagerClosed_NilChainsyncState guards the shutdown/restore
// window (node_lifecycle.go nils n.chainsyncState while rebuilding it) so a
// late NtC close callback cannot panic.
func TestHandleConnManagerClosed_NilChainsyncState(t *testing.T) {
	n := &Node{}
	require.NotPanics(t, func() {
		n.handleConnManagerClosed(newNtCTestConnId(3), true, nil)
	})
}

// TestHandleConnManagerClosed_NtC_ReleasesLeiosServeWaiters covers the node
// half of the issue #3514 wiring. The connection manager's ConnClosedFunc is
// the only close notification an NtC connection gets, and it is what wakes a
// chainsync server callback parked waiting for a certified endorser closure --
// the protocol's own done channel cannot close while that callback is running.
// Without the ReleaseLeiosServeWaiters call in handleConnManagerClosed the
// registered waiter survives the close and this fails.
//
// The Ouroboros instance is built through the validating constructor with the
// full dependency set, and the connection is registered with its connection
// manager, so the waiter passes the liveness check the same way a live serve
// does.
func TestHandleConnManagerClosed_NtC_ReleasesLeiosServeWaiters(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	n := newHandleConnManagerClosedTestNode(t)
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Stop)

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })
	chainManager, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	ledgerState, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
		Logger:       logger,
	})
	require.NoError(t, err)
	harnessMempool, err := mempool.NewMempool(mempool.MempoolConfig{
		Logger:          logger,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       ledgerState,
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger},
	)
	o, err := ouroborosPkg.NewOuroboros(ouroborosPkg.OuroborosConfig{
		Logger:         logger,
		EventBus:       bus,
		LedgerState:    ledgerState,
		Mempool:        &mempool.FIFO{Mempool: harnessMempool},
		ChainsyncState: chainsync.NewState(bus, ledgerState),
		ConnManager:    connManager,
		PeerGov: peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
			Logger:      logger,
			EventBus:    bus,
			ConnManager: connManager,
		}),
	})
	require.NoError(t, err)
	n.ouroborosRef.Store(o)

	// Register a connection so the waiter is not short-circuited by the
	// already-closed liveness check.
	conn, err := ouroboros.NewConnection()
	require.NoError(t, err)
	require.True(t, connManager.AddConnection(conn, true, "127.0.0.1:3002"))
	connId := conn.Id()

	done, cancel := o.RegisterLeiosServeWaiterForTesting(connId)
	t.Cleanup(cancel)

	select {
	case <-done:
		t.Fatal("precondition: waiter must not be released before the close")
	default:
	}

	n.handleConnManagerClosed(connId, true, nil)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal(
			"NtC close must release the parked Leios endorser-closure serving wait",
		)
	}
}

// TestHandleConnManagerClosed_NilOuroboros guards the same restore window as
// TestHandleConnManagerClosed_NilChainsyncState for the added ouroboros
// dereference: n.ouroboros() is nil before Run wires it.
func TestHandleConnManagerClosed_NilOuroboros(t *testing.T) {
	n := newHandleConnManagerClosedTestNode(t)
	require.Nil(t, n.ouroboros())
	require.NotPanics(t, func() {
		n.handleConnManagerClosed(newNtCTestConnId(5), true, nil)
	})
}
