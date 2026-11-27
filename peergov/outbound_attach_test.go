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
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/stretchr/testify/require"
)

// addrConn gives a mock connection its own addresses. net.Pipe reports one
// shared address value, so two mock connections would otherwise collide on
// their ConnectionId in the connection manager.
type addrConn struct {
	net.Conn
	local  net.Addr
	remote net.Addr
}

func (c *addrConn) LocalAddr() net.Addr  { return c.local }
func (c *addrConn) RemoteAddr() net.Addr { return c.remote }

var addrConnPort atomic.Int32

func newAddrConn() *addrConn {
	port := int(addrConnPort.Add(1))
	return &addrConn{
		Conn:   newMockNtNHandshakeConn(false),
		local:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40000 + port},
		remote: &net.TCPAddr{IP: net.IPv4(44, 0, 0, 1), Port: 3001},
	}
}

func (c *addrConn) id() ouroboros.ConnectionId {
	return ouroboros.ConnectionId{LocalAddr: c.local, RemoteAddr: c.remote}
}

func newAttachTestConnManager(
	t *testing.T,
	dialer func(context.Context, string) (net.Conn, error),
	closed func(ouroboros.ConnectionId),
) *connmanager.ConnectionManager {
	t.Helper()
	connMgr := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			OutboundConnOpts: []ouroboros.ConnectionOptionFunc{
				ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
				ouroboros.WithNodeToNode(true),
				ouroboros.WithKeepAlive(false),
			},
			OutboundDialer: dialer,
			ConnClosedFunc: func(
				connId ouroboros.ConnectionId,
				_ bool,
				_ error,
			) {
				if closed != nil {
					closed(connId)
				}
			},
		},
	)
	t.Cleanup(func() { _ = connMgr.Stop(context.Background()) })
	return connMgr
}

func waitDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for " + what)
	}
}

// An outbound connection that closes before the dial worker records it must
// not be recorded: its close event already ran and found no peer holding it,
// so a peer recording it afterwards keeps a dead connection for good. Every
// connection a peer holds is open or still has its close event pending.
func TestCreateOutboundConnection_DoesNotRecordConnectionClosedBeforeAttach(
	t *testing.T,
) {
	t.Parallel()

	var pg *PeerGovernor
	dialed := make(chan *addrConn, 1)
	removed := make(chan struct{})
	var removeOnce sync.Once
	var dials atomic.Int32
	connMgr := newAttachTestConnManager(
		t,
		func(context.Context, string) (net.Conn, error) {
			if dials.Add(1) > 1 {
				return nil, io.ErrUnexpectedEOF
			}
			// Take the governor lock for the test, so the worker
			// blocks on it after the connection manager has added
			// the connection and before it records it.
			pg.mu.Lock()
			c := newAddrConn()
			dialed <- c
			return c, nil
		},
		func(ouroboros.ConnectionId) {
			removeOnce.Do(func() { close(removed) })
		},
	)
	pg = NewPeerGovernor(PeerGovernorConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		ConnManager: connMgr,
	})
	target := &Peer{
		Address:           "44.0.0.1:3001",
		NormalizedAddress: "44.0.0.1:3001",
		Source:            PeerSourceP2PGossip,
		State:             PeerStateCold,
	}
	upstream := &Peer{
		Address:           "203.0.113.10:3001",
		NormalizedAddress: "203.0.113.10:3001",
		Source:            PeerSourceTopologyLocalRoot,
		State:             PeerStateHot,
		Connection:        &PeerConnection{IsClient: true},
	}
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.peers = []*Peer{target, upstream}
	pg.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		pg.createOutboundConnection(target, false)
	}()

	c := <-dialed
	require.Eventually(t, func() bool {
		return connMgr.GetConnectionById(c.id()) != nil
	}, 10*time.Second, time.Millisecond)
	// The remote closes the connection. Its close event finds no peer
	// holding it; the governor has nothing to clear.
	require.NoError(t, c.Close())
	waitDone(t, removed, "the connection manager to remove the connection")
	require.Nil(t, connMgr.GetConnectionById(c.id()))
	pg.mu.Unlock()

	waitDone(t, done, "the dial worker")
	pg.mu.Lock()
	defer pg.mu.Unlock()
	if idx := pg.peerIndexByAddress("44.0.0.1:3001"); idx != -1 &&
		pg.peers[idx].Connection != nil {
		require.NotNil(
			t,
			connMgr.GetConnectionById(pg.peers[idx].Connection.Id),
			"the peer holds a connection the manager already removed",
		)
	}
}

// A dial worker serves only the peer entry it reserved. When the entry is
// removed while the worker dials and the address is added again with a worker
// of its own, the first worker's connection is closed rather than recorded
// over the second's, which would leave the second open with no peer holding
// it: the governor never closes or counts it again.
func TestCreateOutboundConnection_StaleWorkerDoesNotReplaceNewEntry(
	t *testing.T,
) {
	t.Parallel()

	firstDialing := make(chan struct{})
	releaseFirst := make(chan struct{})
	conns := make(chan *addrConn, 2)
	var dials atomic.Int32
	connMgr := newAttachTestConnManager(
		t,
		func(context.Context, string) (net.Conn, error) {
			n := dials.Add(1)
			if n > 2 {
				return nil, io.ErrUnexpectedEOF
			}
			c := newAddrConn()
			if n == 1 {
				close(firstDialing)
				<-releaseFirst
			}
			conns <- c
			return c, nil
		},
		nil,
	)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	attached := make(chan ouroboros.ConnectionId, 2)
	bus.SubscribeFunc(OutboundConnectionEventType, func(evt event.Event) {
		if e, ok := evt.Data.(OutboundConnectionEvent); ok {
			attached <- e.ConnectionId
		}
	})
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:    bus,
		ConnManager: connMgr,
	})
	first := &Peer{
		Address:           "44.0.0.1:3001",
		NormalizedAddress: "44.0.0.1:3001",
		Source:            PeerSourceP2PGossip,
		State:             PeerStateCold,
	}
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.peers = []*Peer{first}
	pg.mu.Unlock()

	pg.mu.Lock()
	pg.spawnOutboundConnectionLocked(first)
	pg.mu.Unlock()
	waitDone(t, firstDialing, "the first dial")

	// The known-peer limit forgets the cold entry while its worker dials,
	// and peer sharing adds the address again with a worker of its own.
	pg.mu.Lock()
	removed := 0
	pg.enforceStateLimit(
		PeerStateCold, 0, false, &removed, new([]deferredClose),
	)
	pg.mu.Unlock()
	require.Equal(t, 1, removed)
	require.NoError(t, pg.AddPeer("44.0.0.1:3001", PeerSourceP2PGossip))
	second := <-conns
	select {
	case id := <-attached:
		require.True(t, sameConnectionId(id, second.id()))
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the new entry's connection")
	}

	close(releaseFirst)
	stale := <-conns
	workersDone := make(chan struct{})
	go func() {
		pg.wg.Wait()
		close(workersDone)
	}()
	waitDone(t, workersDone, "the dial workers")

	pg.mu.Lock()
	idx := pg.peerIndexByAddress("44.0.0.1:3001")
	require.NotEqual(t, -1, idx)
	held := pg.peers[idx].Connection
	pg.mu.Unlock()
	require.NotNil(t, held)
	require.True(
		t,
		sameConnectionId(held.Id, second.id()),
		"the new entry must keep its own connection",
	)
	require.NotNil(t, connMgr.GetConnectionById(second.id()))
	if staleConn, done := connMgr.GetConnectionWithDone(
		stale.id(),
	); staleConn != nil {
		waitDone(t, done, "the stale worker's connection to close")
	}
}

// A dial failure of a worker whose entry was replaced belongs to the removed
// entry. Charging it to the replacement would back off, deny or remove an
// address whose own worker has just connected.
func TestCreateOutboundConnection_StaleWorkerFailureLeavesNewEntry(
	t *testing.T,
) {
	t.Parallel()

	firstDialing := make(chan struct{})
	releaseFirst := make(chan struct{})
	var dials atomic.Int32
	connMgr := newAttachTestConnManager(
		t,
		func(context.Context, string) (net.Conn, error) {
			switch dials.Add(1) {
			case 1:
				close(firstDialing)
				<-releaseFirst
				return nil, io.ErrUnexpectedEOF
			case 2:
				return newAddrConn(), nil
			default:
				return nil, io.ErrUnexpectedEOF
			}
		},
		nil,
	)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	attached := make(chan struct{}, 1)
	bus.SubscribeFunc(OutboundConnectionEventType, func(event.Event) {
		attached <- struct{}{}
	})
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:    bus,
		ConnManager: connMgr,
	})
	first := &Peer{
		Address:           "44.0.0.1:3001",
		NormalizedAddress: "44.0.0.1:3001",
		Source:            PeerSourceP2PGossip,
		State:             PeerStateCold,
	}
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.peers = []*Peer{first}
	pg.mu.Unlock()

	pg.mu.Lock()
	pg.spawnOutboundConnectionLocked(first)
	pg.mu.Unlock()
	waitDone(t, firstDialing, "the first dial")
	pg.mu.Lock()
	removed := 0
	pg.enforceStateLimit(
		PeerStateCold, 0, false, &removed, new([]deferredClose),
	)
	pg.mu.Unlock()
	require.Equal(t, 1, removed)
	require.NoError(t, pg.AddPeer("44.0.0.1:3001", PeerSourceP2PGossip))
	select {
	case <-attached:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the new entry's connection")
	}

	close(releaseFirst)
	workersDone := make(chan struct{})
	go func() {
		pg.wg.Wait()
		close(workersDone)
	}()
	waitDone(t, workersDone, "the dial workers")

	pg.mu.Lock()
	defer pg.mu.Unlock()
	idx := pg.peerIndexByAddress("44.0.0.1:3001")
	require.NotEqual(t, -1, idx, "the new entry must not be removed")
	require.Zero(t, pg.peers[idx].ReconnectCount)
	require.Zero(t, pg.peers[idx].ReconnectDelay)
	require.False(t, pg.isPeerDeniedLocked(pg.peers[idx]))
}

// A duplex inbound connection recorded while the worker dials already serves
// the peer. The dialled connection is closed rather than recorded over it,
// which would leave the inbound connection open with no peer holding it.
func TestCreateOutboundConnection_KeepsClientConnectionRecordedDuringDial(
	t *testing.T,
) {
	t.Parallel()

	dialing := make(chan struct{})
	release := make(chan struct{})
	dialed := make(chan *addrConn, 1)
	var dials atomic.Int32
	connMgr := newAttachTestConnManager(
		t,
		func(context.Context, string) (net.Conn, error) {
			if dials.Add(1) > 1 {
				return nil, io.ErrUnexpectedEOF
			}
			close(dialing)
			<-release
			c := newAddrConn()
			dialed <- c
			return c, nil
		},
		nil,
	)
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		ConnManager: connMgr,
	})
	target := &Peer{
		Address:           "44.0.0.1:3001",
		NormalizedAddress: "44.0.0.1:3001",
		Source:            PeerSourceP2PGossip,
		State:             PeerStateCold,
	}
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.peers = []*Peer{target}
	pg.mu.Unlock()

	pg.mu.Lock()
	pg.spawnOutboundConnectionLocked(target)
	pg.mu.Unlock()
	waitDone(t, dialing, "the dial")
	inboundId := closeLaneConnId(1)
	pg.mu.Lock()
	target.Connection = &PeerConnection{Id: inboundId, IsClient: true}
	target.State = PeerStateWarm
	pg.mu.Unlock()
	close(release)
	c := <-dialed

	workersDone := make(chan struct{})
	go func() {
		pg.wg.Wait()
		close(workersDone)
	}()
	waitDone(t, workersDone, "the dial worker")

	pg.mu.Lock()
	held := target.Connection
	pg.mu.Unlock()
	require.NotNil(t, held)
	require.True(
		t,
		sameConnectionId(held.Id, inboundId),
		"the inbound connection must stay recorded",
	)
	if conn, done := connMgr.GetConnectionWithDone(c.id()); conn != nil {
		waitDone(t, done, "the dialled connection to close")
	}
}
