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
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/muxer"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	csmock "github.com/blinklabs-io/ouroboros-mock/chainsync"
	"github.com/stretchr/testify/require"
)

func TestLeiosNotifyStartedUnregisteredConnectionCloses(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{})
	o.connManager = connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	connID := gouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
	local, peer := net.Pipe()
	m := muxer.New(local)
	connectionDone := make(chan any)
	var closeOnce sync.Once
	closeConnection := func() { closeOnce.Do(func() { close(connectionDone) }) }
	finished := make(chan struct{})
	cfg := leiosnotify.NewConfig(leiosnotify.WithRequestNextFunc(
		func(ctx leiosnotify.CallbackContext) (protocol.Message, error) {
			defer close(finished)
			return o.leiosnotifyServerRequestNext(ctx)
		},
	))
	server := leiosnotify.NewServer(protocol.ProtocolOptions{
		ConnectionId:       connID,
		ConnectionDoneChan: connectionDone,
		Muxer:              m,
		ErrorChan:          make(chan error, 4),
	}, &cfg)
	t.Cleanup(func() {
		closeConnection()
		// Release the old implementation after a failed assertion, too.
		o.ReleaseLeiosServeWaiters(connID)
		server.Stop()
		m.Stop()
		_ = local.Close()
		_ = peer.Close()
	})
	server.Start()
	m.Start()
	request, err := cbor.Encode(leiosnotify.NewMsgNotificationRequestNext())
	require.NoError(t, err)
	segment := muxer.NewSegment(leiosnotify.ProtocolId, request, false)
	require.NoError(t, peer.SetWriteDeadline(time.Now().Add(5*time.Second)))
	require.NoError(
		t,
		binary.Write(peer, binary.BigEndian, segment.SegmentHeader),
	)
	_, err = peer.Write(segment.Payload)
	require.NoError(t, err)
	key := leiosConnectionIdString(connID)
	testutil.WaitForCondition(t, func() bool {
		o.leiosEBLog.mu.Lock()
		defer o.leiosEBLog.mu.Unlock()
		_, waiting := o.leiosEBLog.cursors[key]
		return waiting
	}, 5*time.Second, "started callback to wait before manager publication")
	require.Nil(t, o.connManager.GetConnectionById(connID))
	closeConnection()
	server.Stop()
	testutil.RequireReceive(
		t,
		finished,
		time.Second,
		"connection closure must release the started callback without manager notification",
	)
	testutil.RequireReceive(t, server.DoneChan(), time.Second,
		"protocol completion must follow callback return")
	o.leiosEBLog.mu.Lock()
	_, retained := o.leiosEBLog.cursors[key]
	o.leiosEBLog.mu.Unlock()
	require.False(
		t,
		retained,
		"unregistered closed connection retained its cursor",
	)
}

func TestLeiosNotifyConnectionCleanupPreservesAnotherOwner(t *testing.T) {
	t.Parallel()
	log := newLeiosForgedEBLog()
	first := new(leiosnotify.Server)
	second := new(leiosnotify.Server)
	log.registerConn("peer", first, nil)
	log.append(
		leiosForgedEBEntry{point: &ocommon.Point{Slot: 1, Hash: []byte{1}}},
	)
	entry, _ := log.nextWhileConnected("peer", second, nil)
	require.Nil(
		t,
		entry,
		"an unpublished owner must not take over a live cursor",
	)
	log.removeConnOwned("peer", second)
	require.Contains(t, log.cursors, "peer")
	require.Same(t, first, log.owners["peer"])
	log.removeConnOwned("peer", first)
	require.Empty(t, log.cursors)
	require.Empty(t, log.owners)
}

func TestLeiosNotifyStaleResponseDoesNotCompleteReplacement(t *testing.T) {
	t.Parallel()
	log := newLeiosForgedEBLog()
	o := &Ouroboros{leiosEBLog: log}
	first := new(leiosnotify.Server)
	second := new(leiosnotify.Server)
	connID := gouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
	key := leiosConnectionIdString(connID)
	const holder = "holder"
	point := ocommon.Point{Slot: 1, Hash: []byte{1}}
	log.registerConn(holder, nil, nil)
	log.registerConn(key, first, nil)
	log.append(leiosForgedEBEntry{point: &point})
	entry, _ := log.nextWhileConnected(key, first, nil)
	require.NotNil(t, entry)
	// The manager has removed the old connection, but its close callback
	// has not run when the replacement registers.
	log.registerConn(key, second, nil)
	entry, _ = log.nextWhileConnected(key, second, nil)
	require.NotNil(t, entry)
	require.True(t, log.reservations[key].retry)
	log.removeConnOwned(key, first)
	require.Same(t, second, log.owners[key])

	o.leiosnotifyServerResponseSent(
		leiosnotify.CallbackContext{ConnectionId: connID, Server: first},
		nil,
		nil,
	)
	_, reserved := log.reservations[key]
	require.True(t, reserved,
		"stale response must not consume replacement reservation",
	)
	o.leiosnotifyServerResponseSent(
		leiosnotify.CallbackContext{ConnectionId: connID, Server: second},
		nil,
		nil,
	)
	_, reserved = log.reservations[key]
	require.False(t, reserved)
	secondPoint := ocommon.Point{Slot: 2, Hash: []byte{2}}
	log.append(leiosForgedEBEntry{point: &secondPoint})
	entry, _ = log.nextWhileConnected(key, second, nil)
	require.NotNil(t, entry)
	o.leiosnotifyServerResponseSent(
		leiosnotify.CallbackContext{ConnectionId: connID, Server: first},
		nil,
		errors.New("stale response"),
	)
	_, reserved = log.reservations[key]
	require.True(t, reserved,
		"stale failed response must not alter replacement reservation",
	)
	log.complete(key, second, false)
	log.removeConn(holder)
}

func TestLeiosNotifyClientStartBindsRealServerOwner(t *testing.T) {
	t.Parallel()
	left, right := net.Pipe()
	clientCfg := leiosnotify.NewConfig(
		leiosnotify.WithNotificationFunc(func(
			leiosnotify.CallbackContext,
			protocol.Message,
		) error {
			return nil
		}),
	)
	serverCfg := leiosnotify.NewConfig()
	type result struct {
		conn *gouroboros.Connection
		err  error
	}
	clientCh := make(chan result, 1)
	serverCh := make(chan result, 1)
	var client, server *gouroboros.Connection
	t.Cleanup(func() {
		if client != nil {
			_ = client.Close()
		}
		if server != nil {
			_ = server.Close()
		}
		_ = left.Close()
		_ = right.Close()
	})
	go func() {
		conn, err := gouroboros.NewConnection(
			gouroboros.WithConnection(left),
			gouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
			gouroboros.WithNodeToNode(true),
			gouroboros.WithLeiosNotifyConfig(clientCfg),
		)
		clientCh <- result{conn: conn, err: err}
	}()
	go func() {
		conn, err := gouroboros.NewConnection(
			gouroboros.WithConnection(right),
			gouroboros.WithServer(true),
			gouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
			gouroboros.WithNodeToNode(true),
			gouroboros.WithLeiosNotifyConfig(serverCfg),
		)
		serverCh <- result{conn: conn, err: err}
	}()
	clientResult := testutil.RequireReceive(
		t,
		clientCh,
		5*time.Second,
		"real LeiosNotify client handshake",
	)
	serverResult := testutil.RequireReceive(
		t,
		serverCh,
		5*time.Second,
		"real LeiosNotify server handshake",
	)
	require.NoError(t, clientResult.err)
	require.NoError(t, serverResult.err)
	client = clientResult.conn
	server = serverResult.conn
	require.NotNil(t, client)
	require.NotNil(t, server)
	require.NotNil(t, client.LeiosNotify())
	require.NotNil(t, client.LeiosNotify().Server)
	require.NotNil(t, server.LeiosNotify())
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	o := newOuroboros(OuroborosConfig{ConnManager: cm})
	require.True(t, cm.AddConnection(client, false, ""))
	require.NoError(t, o.leiosnotifyClientStart(client.Id()))
	key := leiosConnectionIdString(client.Id())
	o.leiosEBLog.mu.Lock()
	defer o.leiosEBLog.mu.Unlock()
	require.Same(t, client.LeiosNotify().Server, o.leiosEBLog.owners[key])
}

func TestLeiosNotifyClientContinuesAfterFirstNotification(t *testing.T) {
	t.Parallel()

	sender := newOuroboros(OuroborosConfig{})
	receiverConnections := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	receiver := newOuroboros(OuroborosConfig{
		ConnManager: receiverConnections,
	})
	clientCfg := leiosnotify.NewConfig(receiver.leiosnotifyClientConnOpts()...)
	serverCfg := leiosnotify.NewConfig(sender.leiosnotifyServerConnOpts()...)
	left, right := net.Pipe()
	type result struct {
		conn *gouroboros.Connection
		err  error
	}
	clientCh := make(chan result, 1)
	serverCh := make(chan result, 1)
	var client, server *gouroboros.Connection
	t.Cleanup(func() {
		if client != nil {
			_ = client.Close()
		}
		if server != nil {
			_ = server.Close()
		}
		_ = left.Close()
		_ = right.Close()
	})
	go func() {
		conn, err := gouroboros.NewConnection(
			gouroboros.WithConnection(left),
			gouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
			gouroboros.WithNodeToNode(true),
			gouroboros.WithLeiosNotifyConfig(clientCfg),
		)
		clientCh <- result{conn: conn, err: err}
	}()
	go func() {
		conn, err := gouroboros.NewConnection(
			gouroboros.WithConnection(right),
			gouroboros.WithServer(true),
			gouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
			gouroboros.WithNodeToNode(true),
			gouroboros.WithLeiosNotifyConfig(serverCfg),
		)
		serverCh <- result{conn: conn, err: err}
	}()
	clientResult := testutil.RequireReceive(
		t, clientCh, 5*time.Second, "real LeiosNotify client handshake",
	)
	serverResult := testutil.RequireReceive(
		t, serverCh, 5*time.Second, "real LeiosNotify server handshake",
	)
	require.NoError(t, clientResult.err)
	require.NoError(t, serverResult.err)
	client = clientResult.conn
	server = serverResult.conn
	require.True(t, receiverConnections.AddConnection(client, false, ""))
	require.NoError(t, receiver.leiosnotifyClientStart(client.Id()))

	serverKey := leiosConnectionIdString(server.Id())
	testutil.WaitForCondition(t, func() bool {
		sender.leiosEBLog.mu.Lock()
		defer sender.leiosEBLog.mu.Unlock()
		_, registered := sender.leiosEBLog.cursors[serverKey]
		return registered
	}, time.Second, "notification request to register the server cursor")

	for voterID := range uint64(4) {
		vote := lcommon.LeiosPrototypeVote{
			VoterId:       voterID,
			VoteSignature: make([]byte, lcommon.LeiosBlsSignatureSize),
		}
		sender.EnqueueLeiosPrototypeVote(vote)
	}
	testutil.WaitForCondition(t, func() bool {
		sender.leiosEBLog.mu.Lock()
		defer sender.leiosEBLog.mu.Unlock()
		return sender.leiosEBLog.base == 4 && len(sender.leiosEBLog.items) == 0
	}, 2*time.Second, "client to consume notifications after the first response")
}

func TestLeiosNotifyClosedOwnerCannotRecreateCursor(t *testing.T) {
	t.Parallel()
	log := newLeiosForgedEBLog()
	owner := new(leiosnotify.Server)
	connectionDone := make(chan any)
	close(connectionDone)
	entry, _ := log.nextWhileConnected("peer", owner, connectionDone)
	require.Nil(t, entry)
	require.Empty(t, log.cursors)
	require.Empty(t, log.owners)
}

func TestLeiosNotifyStaleRegistrationCannotReplaceLiveOwner(t *testing.T) {
	t.Parallel()
	log := newLeiosForgedEBLog()
	oldOwner := new(leiosnotify.Server)
	liveOwner := new(leiosnotify.Server)
	log.registerConn("peer", liveOwner, nil)
	log.append(
		leiosForgedEBEntry{point: &ocommon.Point{Slot: 1, Hash: []byte{1}}},
	)
	entry, _ := log.nextWhileConnected("peer", liveOwner, nil)
	require.NotNil(t, entry)
	reservation := log.reservations["peer"]
	log.registerConn("peer", oldOwner, func() bool { return false })
	require.Same(t, liveOwner, log.owners["peer"])
	require.Equal(t, reservation, log.reservations["peer"])
	log.removeConnOwned("peer", oldOwner)
	require.Same(t, liveOwner, log.owners["peer"])
}

func TestLeiosNotifyIdleRequestReleasedByDisconnect(t *testing.T) {
	f := newChainsyncServerFixtureWithConfig(
		t,
		csmock.ModeNtN,
		OuroborosConfig{EnableLeios: true},
	)
	// Exercise the production callback with a real managed connection. The
	// standalone protocol's completion remains open throughout the disconnect.
	server := leiosnotify.NewServer(protocol.ProtocolOptions{}, nil)
	t.Cleanup(server.Stop)
	finished := make(chan struct{})
	connectionDone := make(chan any)
	go func() {
		defer close(finished)
		_, _ = f.o.leiosnotifyServerRequestNext(
			leiosnotify.CallbackContext{
				Server:             server,
				ConnectionId:       f.conn.Id(),
				ConnectionDoneChan: connectionDone,
			},
		)
	}()
	key := leiosConnectionIdString(f.conn.Id())
	testutil.WaitForCondition(t, func() bool {
		f.o.leiosEBLog.mu.Lock()
		defer f.o.leiosEBLog.mu.Unlock()
		_, ok := f.o.leiosEBLog.cursors[key]
		return ok
	}, 5*time.Second, "idle notification request to register its cursor")
	require.NoError(t, f.h.Disconnect())
	close(connectionDone)
	testutil.RequireReceive(
		t,
		finished,
		5*time.Second,
		"idle callback to exit on connection closure",
	)
	// Callback completion removes the owner-specific cursor.
	testutil.WaitForCondition(t, func() bool {
		f.o.leiosEBLog.mu.Lock()
		defer f.o.leiosEBLog.mu.Unlock()
		_, retained := f.o.leiosEBLog.cursors[key]
		return !retained
	}, 5*time.Second, "closed connection cursor to be removed")
	select {
	case <-server.DoneChan():
		t.Fatal("test must not close protocol completion to release callback")
	default:
	}
}

func TestLeiosNotifyClosedReservationDoesNotRecreateCursor(t *testing.T) {
	o := newOuroboros(OuroborosConfig{})
	key := "peer"
	o.leiosEBLog.registerConn(key, nil, nil)
	done := make(chan any)
	close(done)
	o.leiosEBLog.removeConn(key)
	entry, _ := o.leiosEBLog.nextWhileConnected(key, nil, done)
	require.Nil(t, entry)
	o.leiosEBLog.mu.Lock()
	_, retained := o.leiosEBLog.cursors[key]
	o.leiosEBLog.mu.Unlock()
	require.False(t, retained)
}

func TestLeiosNotifyDisconnectAfterOfferReturnReleasesCursor(t *testing.T) {
	o := newOuroboros(OuroborosConfig{})
	connID := gouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
	key := leiosConnectionIdString(connID)
	o.leiosEBLog.registerConn(key, nil, nil)
	point := ocommon.NewPoint(1, []byte{1})
	o.leiosEBLog.append(leiosForgedEBEntry{point: &point})
	entry, _ := o.leiosEBLog.nextWhileConnected(key, nil, nil)
	require.NotNil(t, entry)
	// The callback returned an offer, but its send has not completed.
	o.leiosEBLog.removeConn(key)
	require.Empty(t, o.leiosEBLog.cursors)
	require.Empty(t, o.leiosEBLog.reservations)
	require.Len(
		t,
		o.leiosEBLog.retries,
		1,
		"undelivered offer must remain retryable",
	)
}

func TestLeiosNotifyOfferKeepsConnectionClosureObserver(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{})
	server := leiosnotify.NewServer(protocol.ProtocolOptions{}, nil)
	t.Cleanup(server.Stop)
	connectionDone := make(chan any)
	connID := gouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
	key := leiosConnectionIdString(connID)
	type result struct {
		msg protocol.Message
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		msg, err := o.leiosnotifyServerRequestNext(leiosnotify.CallbackContext{
			Server:             server,
			ConnectionId:       connID,
			ConnectionDoneChan: connectionDone,
		})
		resultCh <- result{msg: msg, err: err}
	}()
	testutil.WaitForCondition(t, func() bool {
		o.leiosEBLog.mu.Lock()
		defer o.leiosEBLog.mu.Unlock()
		_, registered := o.leiosEBLog.cursors[key]
		return registered
	}, time.Second, "request-next callback to register its cursor")
	point := ocommon.NewPoint(1, []byte{1})
	o.leiosEBLog.append(leiosForgedEBEntry{point: &point})
	got := testutil.RequireReceive(
		t,
		resultCh,
		time.Second,
		"request-next callback to return an offer",
	)
	require.NoError(t, got.err)
	require.NotNil(t, got.msg)
	require.Contains(t, o.leiosEBLog.cursors, key)

	close(connectionDone)
	testutil.WaitForCondition(t, func() bool {
		o.leiosEBLog.mu.Lock()
		defer o.leiosEBLog.mu.Unlock()
		_, retained := o.leiosEBLog.cursors[key]
		return !retained
	}, time.Second, "connection observer to remove returned offer cursor")
}

func TestLeiosNotifyCompletedProtocolReturnsWithoutCursor(t *testing.T) {
	o := newOuroboros(OuroborosConfig{})
	server := leiosnotify.NewServer(protocol.ProtocolOptions{}, nil)
	server.Stop()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, _ = o.leiosnotifyServerRequestNext(
			leiosnotify.CallbackContext{Server: server},
		)
	}()
	testutil.RequireReceive(
		t,
		finished,
		time.Second,
		"completed protocol callback to return",
	)
	require.Empty(t, o.leiosEBLog.cursors)
	require.Empty(t, o.leiosServeWaiters)
}

// removeConn unregisters a connection cursor and prunes newly freed entries.
func (l *leiosForgedEBLog) removeConn(connKey string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.removeConnLocked(connKey)
}

func TestLeiosNotifyUnregisteredConnectionWaitsForProtocolCompletion(
	t *testing.T,
) {
	o := newOuroboros(OuroborosConfig{})
	o.connManager = connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	server := leiosnotify.NewServer(protocol.ProtocolOptions{}, nil)
	finished := make(chan error, 1)
	go func() {
		_, err := o.leiosnotifyServerRequestNext(leiosnotify.CallbackContext{
			Server:       server,
			ConnectionId: gouroboros.ConnectionId{},
		})
		finished <- err
	}()
	t.Cleanup(server.Stop)
	testutil.RequireNoReceive(
		t,
		finished,
		50*time.Millisecond,
		"an unregistered connection must not be treated as a live-protocol disconnect",
	)
	server.Stop()
	err := testutil.RequireReceive(t, finished, time.Second,
		"protocol completion must release the callback")
	require.Error(t, err)
}

// next lets log-only tests reserve entries without a connection lifecycle.
func (l *leiosForgedEBLog) next(
	connKey string,
) (*leiosForgedEBEntry, chan struct{}) {
	return l.nextWhileConnected(connKey, nil, nil)
}

// A peer issues its next RequestNext as soon as it has received a response,
// which can be before the server goroutine that sent that response has
// reported the send. That request must wait for the in-flight entry to be
// committed and then serve the following entry, not offer the in-flight entry
// a second time.
func TestLeiosNotifyRequestDuringInFlightSendServesNextEntry(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{})
	server := leiosnotify.NewServer(protocol.ProtocolOptions{}, nil)
	t.Cleanup(server.Stop)
	connectionDone := make(chan any)
	t.Cleanup(func() { close(connectionDone) })
	ctx := leiosnotify.CallbackContext{
		Server: server,
		ConnectionId: gouroboros.ConnectionId{
			LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
			RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
		},
		ConnectionDoneChan: connectionDone,
	}
	o.leiosEBLog.registerConn(
		leiosConnectionIdString(ctx.ConnectionId),
		server,
		nil,
	)
	first := lcommon.LeiosPrototypeVote{
		AnnouncingRbHash: lcommon.NewBlake2b256([]byte("first")),
		VoterId:          1,
		VoteSignature:    make([]byte, lcommon.LeiosBlsSignatureSize),
	}
	second := lcommon.LeiosPrototypeVote{
		AnnouncingRbHash: lcommon.NewBlake2b256([]byte("second")),
		VoterId:          2,
		VoteSignature:    make([]byte, lcommon.LeiosBlsSignatureSize),
	}
	o.EnqueueLeiosPrototypeVote(first)
	o.EnqueueLeiosPrototypeVote(second)

	sent, err := o.leiosnotifyServerRequestNext(ctx)
	require.NoError(t, err)
	require.Equal(
		t,
		[]lcommon.LeiosPrototypeVote{first},
		sent.(*leiosnotify.MsgVotesOffer).PrototypeVotes,
	)

	type result struct {
		msg protocol.Message
		err error
	}
	next := make(chan result, 1)
	go func() {
		msg, err := o.leiosnotifyServerRequestNext(ctx)
		next <- result{msg, err}
	}()
	testutil.RequireNoReceive(
		t,
		next,
		50*time.Millisecond,
		"a request must not be answered while the previous response is unreported",
	)
	o.leiosnotifyServerResponseSent(ctx, sent, nil)
	got := testutil.RequireReceive(
		t,
		next,
		2*time.Second,
		"reporting the send must release the waiting request",
	)
	require.NoError(t, got.err)
	require.Equal(
		t,
		[]lcommon.LeiosPrototypeVote{second},
		got.msg.(*leiosnotify.MsgVotesOffer).PrototypeVotes,
	)
}
