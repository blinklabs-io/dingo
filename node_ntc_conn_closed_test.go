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
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	ouroborosPkg "github.com/blinklabs-io/dingo/ouroboros"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
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

func newHandleConnManagerClosedOwnerConn(
	t *testing.T,
	o *ouroborosPkg.Ouroboros,
) *ouroboros.Connection {
	t.Helper()
	listener := o.ConfigureListeners([]connmanager.ListenerConfig{{UseNtC: true}})[0]
	localWire, peerWire := newLeiosNotifyTestConnPair(
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	)
	t.Cleanup(func() {
		_ = localWire.Close()
		_ = peerWire.Close()
	})
	type result struct {
		conn *ouroboros.Connection
		err  error
	}
	localResult, peerResult := make(chan result, 1), make(chan result, 1)
	go func() {
		conn, err := ouroboros.NewConnection(append(
			[]ouroboros.ConnectionOptionFunc{
				ouroboros.WithConnection(localWire),
			},
			listener.ConnectionOpts...,
		)...)
		localResult <- result{conn: conn, err: err}
	}()
	go func() {
		conn, err := ouroboros.NewConnection(append(
			[]ouroboros.ConnectionOptionFunc{
				ouroboros.WithConnection(peerWire),
				ouroboros.WithServer(true),
			},
			listener.ConnectionOpts...,
		)...)
		peerResult <- result{conn: conn, err: err}
	}()
	local := testutil.RequireReceive(
		t,
		localResult,
		10*time.Second,
		"owner test local handshake",
	)
	peer := testutil.RequireReceive(
		t,
		peerResult,
		10*time.Second,
		"owner test peer handshake",
	)
	t.Cleanup(func() {
		if local.conn != nil {
			_ = local.conn.Close()
		}
		if peer.conn != nil {
			_ = peer.conn.Close()
		}
	})
	require.NoError(t, local.err)
	require.NoError(t, peer.err)
	require.NotNil(t, peer.conn.ChainSync())
	require.NotNil(t, peer.conn.ChainSync().Server)
	return peer.conn
}

// TestHandleConnManagerClosedOwner_NtC_ReleasesChainsyncClientState reproduces
// issue #3508: NtC connections never received any close notification (the
// EventBus's ConnectionClosedEventType is intentionally NtN-only), so
// chainsync.State.RemoveClient -- which cancels the live chain iterator and
// deletes the per-connection client state -- was never invoked for a closed
// NtC connection. Without handleConnManagerClosedOwner wired as the connection
// manager's ConnClosedOwnerFunc, this assertion fails: the client state
// registered by AddClient is still present after the simulated close.
func TestHandleConnManagerClosedOwner_NtC_ReleasesChainsyncClientState(
	t *testing.T,
) {
	t.Parallel()

	n := newHandleConnManagerClosedTestNode(t)
	conn, err := ouroboros.NewConnection()
	require.NoError(t, err)
	connId := conn.Id()

	_, err = n.chainsyncState.AddClient(connId, ocommon.Point{})
	require.NoError(t, err)
	_, ok := n.chainsyncState.LookupClient(connId)
	require.True(t, ok, "precondition: server-side client state registered")

	n.handleConnManagerClosedOwner(conn, true, nil)

	_, ok = n.chainsyncState.LookupClient(connId)
	require.False(
		t,
		ok,
		"NtC close must release the chainsync server-side client state and its chain iterator",
	)
}

// TestHandleConnManagerClosedOwner_NtN_ReleasesState covers the owner-aware
// connmanager path used for both NtC and NtN. The EventBus path deliberately no
// longer removes server-side state by connection ID because a delayed event
// could delete a replacement connection's state.
func TestHandleConnManagerClosedOwner_NtN_ReleasesState(t *testing.T) {
	t.Parallel()

	n := newHandleConnManagerClosedTestNode(t)
	conn, err := ouroboros.NewConnection()
	require.NoError(t, err)
	connId := conn.Id()

	_, err = n.chainsyncState.AddClient(connId, ocommon.Point{})
	require.NoError(t, err)

	n.handleConnManagerClosedOwner(conn, false, nil)

	_, ok := n.chainsyncState.LookupClient(connId)
	require.False(
		t,
		ok,
		"NtN close must release the chainsync server-side client state",
	)
}

// TestHandleConnManagerClosed_NilChainsyncState guards the shutdown/restore
// window (node_lifecycle.go nils n.chainsyncState while rebuilding it) so a
// late NtC close callback cannot panic.
func TestHandleConnManagerClosedOwner_NilChainsyncState(t *testing.T) {
	t.Parallel()

	n := &Node{}
	require.NotPanics(t, func() {
		conn, err := ouroboros.NewConnection()
		require.NoError(t, err)
		n.handleConnManagerClosedOwner(conn, true, nil)
	})
}

// TestHandleConnManagerClosedOwner_NtC_ReleasesLeiosServeWaiters covers the node
// half of the issue #3514 wiring. The connection manager's ConnClosedOwnerFunc is
// the only close notification an NtC connection gets, and it is what wakes a
// chainsync server callback parked waiting for a certified endorser closure --
// the protocol's own done channel cannot close while that callback is running.
// Without the owner-aware release in handleConnManagerClosedOwner the
// registered waiter survives the close and this fails.
//
// The Ouroboros instance is built through the validating constructor with the
// full dependency set, and the connection is registered with its connection
// manager, so the waiter passes the liveness check the same way a live serve
// does.
func TestHandleConnManagerClosedOwner_NtC_ReleasesLeiosServeWaiters(
	t *testing.T,
) {
	t.Parallel()
	testHandleConnManagerClosedReleasesLeiosServeWaiters(t, true)
}

func TestHandleConnManagerClosedOwner_NtN_ReleasesLeiosServeWaiters(
	t *testing.T,
) {
	t.Parallel()
	testHandleConnManagerClosedReleasesLeiosServeWaiters(t, false)
}

func testHandleConnManagerClosedReleasesLeiosServeWaiters(
	t *testing.T,
	isNtC bool,
) {
	t.Helper()
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
		NetworkMagic:   ouroboros_mock.MockNetworkMagic,
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

	// Register a real node-to-client connection so the waiter carries its
	// chainsync server owner, as it does in production.
	conn := newHandleConnManagerClosedOwnerConn(t, o)
	require.True(t, connManager.AddConnection(conn, isNtC, "127.0.0.1:3002"))
	connId := conn.Id()

	done, cancel := o.RegisterLeiosServeWaiterForTesting(connId)
	t.Cleanup(cancel)

	testutil.RequireNoReceive(
		t,
		done,
		50*time.Millisecond,
		"waiter must not be released before the close",
	)

	n.handleConnManagerClosedOwner(conn, isNtC, nil)

	testutil.RequireReceive(
		t,
		done,
		time.Second,
		"connection close must release the parked Leios serving wait",
	)
}

// TestHandleConnManagerClosedOwner_NtC_ReleasesLocalStateQueryAcquiredPoint covers
// the NtC-close half of blinklabs-io/dingo#382's point-pinning: a client
// that pins a point and then disconnects without a clean Release must not
// leak its map entry, since NtC closes never reach
// Ouroboros.HandleConnClosedEvent (the EventBus's ConnectionClosedEventType
// is intentionally NtN-only) and localstatequeryServerRelease is therefore
// never invoked for it. Without ReleaseLocalStateQueryAcquiredPoint wired
// into handleConnManagerClosedOwner, this assertion fails: the entry
// SetLocalStateQueryAcquiredPointForTesting seeded is still present after
// the simulated close.
func TestHandleConnManagerClosedOwner_NtC_ReleasesLocalStateQueryAcquiredPoint(
	t *testing.T,
) {
	t.Parallel()

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

	conn, err := ouroboros.NewConnection()
	require.NoError(t, err)
	connId := conn.Id()
	o.SetLocalStateQueryAcquiredPointForTesting(connId, ledger.QueryPoint{
		Slot: 100,
		Hash: []byte{0xAB},
	})
	require.True(
		t,
		o.HasLocalStateQueryAcquiredPointForTesting(connId),
		"precondition: pinned point recorded",
	)

	n.handleConnManagerClosedOwner(conn, true, nil)

	require.False(
		t,
		o.HasLocalStateQueryAcquiredPointForTesting(connId),
		"NtC close must release the pinned LocalStateQuery point",
	)
}

// TestHandleConnManagerClosedOwner_NilOuroboros guards the same restore window as
// TestHandleConnManagerClosedOwner_NilChainsyncState for the added ouroboros
// dereference: n.ouroboros() is nil before Run wires it.
func TestHandleConnManagerClosedOwner_NilOuroboros(t *testing.T) {
	t.Parallel()

	n := newHandleConnManagerClosedTestNode(t)
	require.Nil(t, n.ouroboros())
	require.NotPanics(t, func() {
		conn, err := ouroboros.NewConnection()
		require.NoError(t, err)
		n.handleConnManagerClosedOwner(conn, true, nil)
	})
}
