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
	"log/slog"
	"net"
	"sync"
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
	gouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type leiosNotifyTestConn struct {
	net.Conn
	local, remote net.Addr
}

type leiosNotifyTestLogWriter struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *leiosNotifyTestLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *leiosNotifyTestLogWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

func (c *leiosNotifyTestConn) LocalAddr() net.Addr  { return c.local }
func (c *leiosNotifyTestConn) RemoteAddr() net.Addr { return c.remote }

func newLeiosNotifyTestConnPair(local, remote net.Addr) (net.Conn, net.Conn) {
	left, right := net.Pipe()
	return &leiosNotifyTestConn{Conn: left, local: local, remote: remote},
		&leiosNotifyTestConn{Conn: right, local: remote, remote: local}
}

// TestHandleConnManagerClosedOwnerKeepsReplacementLeiosNotifyDelivery proves
// the owner-specific close path across a real mini-protocol boundary. An old
// inbound connection is replaced by a new connection with the same ID; the
// delayed old owner callback must not remove the replacement's cursor. The
// queued vote is then observed by the replacement peer through LeiosNotify.
func TestHandleConnManagerClosedOwnerKeepsReplacementLeiosNotifyDelivery(t *testing.T) {
	t.Parallel()
	logWriter := new(leiosNotifyTestLogWriter)
	logger := slog.New(slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: slog.LevelDebug}))
	t.Cleanup(func() {
		if logs := logWriter.String(); logs != "" {
			t.Log(logs)
		}
	})
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Stop)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })
	chainManager, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	ledgerState, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database: db, ChainManager: chainManager, Logger: logger,
	})
	require.NoError(t, err)
	harnessMempool, err := mempool.NewMempool(mempool.MempoolConfig{
		Logger: logger, PromRegistry: prometheus.NewRegistry(),
		Validator: ledgerState, MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	n := &Node{}
	ownerClosed := make(chan *gouroboros.Connection, 2)
	cm := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{
		Logger: logger,
		ConnClosedOwnerFunc: func(conn *gouroboros.Connection, isNtC bool, err error) {
			ownerClosed <- conn
			n.handleConnManagerClosedOwner(conn, isNtC, err)
		},
	})
	t.Cleanup(func() { require.NoError(t, cm.Stop(context.Background())) })
	o, err := ouroborosPkg.NewOuroboros(ouroborosPkg.OuroborosConfig{
		Logger: logger, EventBus: bus, LedgerState: ledgerState,
		LeiosAnnouncementLedger: ledgerState,
		Mempool:                 &mempool.FIFO{Mempool: harnessMempool},
		ChainsyncState:          chainsync.NewState(bus, ledgerState), ConnManager: cm,
		PeerGov: peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
			Logger: logger, EventBus: bus, ConnManager: cm,
		}),
		EnableLeios:  true,
		NetworkMagic: ouroboros_mock.MockNetworkMagic,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	n.ouroborosRef.Store(o)

	listener := o.ConfigureListeners([]connmanager.ListenerConfig{{}})[0]
	listener.ConnectionOpts = append(listener.ConnectionOpts, gouroboros.WithLogger(logger))
	received := make(chan protocol.Message, 1)
	peerOpts := append([]gouroboros.ConnectionOptionFunc{}, listener.ConnectionOpts...)
	peerOpts = append(peerOpts, gouroboros.WithLeiosNotifyConfig(
		leiosnotify.NewConfig(
			leiosnotify.WithNotificationFunc(func(_ leiosnotify.CallbackContext, msg protocol.Message) error {
				received <- msg
				return nil
			}),
			leiosnotify.WithRequestNextFunc(func(ctx leiosnotify.CallbackContext) (protocol.Message, error) {
				// Keep the node's client request benignly parked while the
				// peer's client exercises the node's server direction.
				<-ctx.ConnectionDoneChan
				return leiosnotify.NewMsgDone(), nil
			}),
		),
	))

	localAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001}
	remoteAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002}
	makeConn := func() (*gouroboros.Connection, *gouroboros.Connection) {
		local, peer := newLeiosNotifyTestConnPair(localAddr, remoteAddr)
		t.Cleanup(func() { _ = local.Close(); _ = peer.Close() })
		type result struct {
			conn *gouroboros.Connection
			err  error
		}
		localCh, peerCh := make(chan result, 1), make(chan result, 1)
		localErrors := make(chan error, 16)
		peerErrors := make(chan error, 16)
		go func() {
			conn, err := gouroboros.NewConnection(append([]gouroboros.ConnectionOptionFunc{
				gouroboros.WithConnection(local), gouroboros.WithNodeToNode(true),
				gouroboros.WithErrorChan(localErrors),
			}, listener.ConnectionOpts...)...)
			localCh <- result{conn, err}
		}()
		go func() {
			conn, err := gouroboros.NewConnection(append([]gouroboros.ConnectionOptionFunc{
				gouroboros.WithConnection(peer), gouroboros.WithServer(true), gouroboros.WithNodeToNode(true),
				gouroboros.WithErrorChan(peerErrors),
			}, peerOpts...)...)
			peerCh <- result{conn, err}
		}()
		localResult := testutil.RequireReceive(t, localCh, 10*time.Second, "local Ouroboros handshake")
		peerResult := testutil.RequireReceive(t, peerCh, 10*time.Second, "peer Ouroboros handshake")
		// Supplied error channels remain caller-owned. Close waits for the
		// connection's senders before the fixture can close those channels and
		// release the connection manager's error watchers.
		t.Cleanup(func() {
			if localResult.conn != nil {
				_ = localResult.conn.Close()
				close(localErrors)
			}
			if peerResult.conn != nil {
				_ = peerResult.conn.Close()
				close(peerErrors)
			}
		})
		assert.NoError(t, localResult.err, "local Ouroboros connection")
		assert.NoError(t, peerResult.err, "peer Ouroboros connection")
		for name, errors := range map[string]chan error{
			"local": localErrors,
			"peer":  peerErrors,
		} {
			for {
				select {
				case err, ok := <-errors:
					if !ok {
						goto nextErrorSource
					}
					t.Logf("%s connection error: %v", name, err)
				default:
					goto nextErrorSource
				}
			}
		nextErrorSource:
		}
		if localResult.err != nil || peerResult.err != nil {
			return nil, nil
		}
		localConn, peerConn := localResult.conn, peerResult.conn
		return localConn, peerConn
	}

	oldConn, oldPeer := makeConn()
	if oldConn == nil || oldPeer == nil {
		return
	}
	require.True(t, cm.AddConnection(oldConn, true, "127.0.0.1:3002"))
	require.Same(t, oldConn, cm.GetConnectionById(oldConn.Id()))
	o.HandleInboundConnEvent(event.NewEvent(connmanager.InboundConnectionEventType,
		connmanager.InboundConnectionEvent{ConnectionId: oldConn.Id()}))
	require.NotNil(t, oldConn.LeiosNotify())
	_ = oldPeer

	newConn, newPeer := makeConn()
	if newConn == nil || newPeer == nil {
		return
	}
	_ = newPeer
	require.True(t, oldConn.Id().LocalAddr == newConn.Id().LocalAddr)
	require.True(t, oldConn.Id().RemoteAddr == newConn.Id().RemoteAddr)
	require.True(t, cm.AddConnection(newConn, true, "127.0.0.1:3002"))
	evicted := testutil.RequireReceive(t, ownerClosed, time.Second, "connection manager owner callback for evicted old connection")
	require.Same(t, oldConn, evicted)
	o.HandleInboundConnEvent(event.NewEvent(connmanager.InboundConnectionEventType,
		connmanager.InboundConnectionEvent{ConnectionId: newConn.Id()}))
	_, versionData := newConn.ProtocolVersion()
	require.NotNil(t, versionData)
	require.Same(t, newConn, cm.GetConnectionById(newConn.Id()))
	require.NoError(t, newPeer.LeiosNotify().Client.Sync())

	// Control delivery establishes that the replacement's client is running
	// and its server owner is registered before the stale close callback.
	o.EnqueueLeiosPrototypeVote(lcommon.LeiosPrototypeVote{
		AnnouncingRbHash: lcommon.NewBlake2b256([]byte("control")),
		VoterId:          6,
		VoteSignature:    make([]byte, lcommon.LeiosBlsSignatureSize),
	})
	control := testutil.RequireReceive(t, received, 5*time.Second, "replacement control notification")
	require.Equal(t, uint8(leiosnotify.MessageTypeVotesOffer), control.Type())
	controlOffer, ok := control.(*leiosnotify.MsgVotesOffer)
	require.True(t, ok)
	require.Equal(t, []lcommon.LeiosPrototypeVote{{
		AnnouncingRbHash: lcommon.NewBlake2b256([]byte("control")),
		VoterId:          6,
		VoteSignature:    make([]byte, lcommon.LeiosBlsSignatureSize),
	}}, controlOffer.PrototypeVotes)

	n.handleConnManagerClosedOwner(oldConn, false, nil)
	vote := lcommon.LeiosPrototypeVote{
		AnnouncingRbHash: lcommon.NewBlake2b256([]byte("after-cleanup")),
		VoterId:          7,
		VoteSignature:    make([]byte, lcommon.LeiosBlsSignatureSize),
	}
	o.EnqueueLeiosPrototypeVote(vote)
	msg := testutil.RequireReceive(t, received, 5*time.Second, "replacement peer receives LeiosNotify vote")
	require.Equal(t, uint8(leiosnotify.MessageTypeVotesOffer), msg.Type())
	msgOffer, ok := msg.(*leiosnotify.MsgVotesOffer)
	require.True(t, ok)
	require.Equal(t, []lcommon.LeiosPrototypeVote{vote}, msgOffer.PrototypeVotes)
}
