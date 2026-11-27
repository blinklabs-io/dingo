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
	"os"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/topology"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

// callbackJoiningConn stands in for a connection whose Close waits for its
// running protocol callbacks. Its one callback reports a chain-sync
// observation, which takes the governor lock, once Close has begun; Close
// returns only after that callback has returned. It also records whether
// Close was entered with the governor lock held, which deadlocks without a
// bound on the wait and stalls the governor for the bound with one.
type callbackJoiningConn struct {
	pg           *PeerGovernor
	underLock    atomic.Bool
	closeBegun   chan struct{}
	callbackDone chan struct{}
	abort        chan struct{}
	once         sync.Once
}

func newCallbackJoiningConn(
	t *testing.T,
	pg *PeerGovernor,
	id ouroboros.ConnectionId,
) *callbackJoiningConn {
	c := &callbackJoiningConn{
		pg:           pg,
		closeBegun:   make(chan struct{}),
		callbackDone: make(chan struct{}),
		abort:        make(chan struct{}),
	}
	// Releases a Close stuck behind a deadlocked pass so the test's
	// goroutines unwind after a failure.
	t.Cleanup(func() { close(c.abort) })
	go func() {
		defer close(c.callbackDone)
		select {
		case <-c.closeBegun:
		case <-c.abort:
			return
		}
		pg.UpdatePeerChainSyncObservation(id, 1, 0)
	}()
	return c
}

func (c *callbackJoiningConn) Close() error {
	// Nothing else in the test takes p.mu before closeBegun is closed, so a
	// failed TryLock means the closing pass holds it.
	if c.pg.mu.TryLock() {
		c.pg.mu.Unlock()
	} else {
		c.underLock.Store(true)
	}
	c.once.Do(func() { close(c.closeBegun) })
	select {
	case <-c.callbackDone:
	case <-c.abort:
	}
	return nil
}

func closeLockConnId(n int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 7000 + n},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("44.0.1.1"), Port: 3001 + n},
	}
}

// requirePassCompletes runs a governor pass and fails, with a goroutine
// dump, if it has not returned within the bound.
func requirePassCompletes(t *testing.T, pass func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pass()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
		t.Fatal(
			"governor pass deadlocked: it closed a connection with p.mu " +
				"held while that connection's callback waited for p.mu",
		)
	}
}

// A governor pass that drops a peer's connection must call Close with p.mu
// released: Close may wait for the connection's running callbacks, and a
// chain-sync or keep-alive callback takes p.mu. Each case drives one pass
// that closes a connection and requires it to finish, the connection to have
// been closed, and the callback to have run.
func TestGovernorPassesCloseConnectionsOutsideLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// setup returns the governor, the id of the connection the pass
		// closes, and the pass.
		setup func(t *testing.T) (*PeerGovernor, ouroboros.ConnectionId, func())
	}{
		{
			name: "gossip churn demotes a hot peer",
			setup: func(t *testing.T) (*PeerGovernor, ouroboros.ConnectionId, func()) {
				pg := NewPeerGovernor(PeerGovernorConfig{
					Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
					EventBus:           newMockEventBus(),
					GossipChurnPercent: 1.0,
					MinScoreThreshold:  0.3,
				})
				id := closeLockConnId(1)
				pg.peers = []*Peer{
					{
						Address:          "44.0.1.1:3002",
						Source:           PeerSourceP2PGossip,
						State:            PeerStateHot,
						PerformanceScore: 0.2,
						Connection:       &PeerConnection{Id: id, IsClient: true},
					},
					{
						Address:          "44.0.1.2:3001",
						Source:           PeerSourceP2PGossip,
						State:            PeerStateHot,
						PerformanceScore: 0.9,
						Connection:       &PeerConnection{Id: closeLockConnId(2), IsClient: true},
					},
					{
						Address:          "44.0.1.3:3001",
						Source:           PeerSourceP2PLedger,
						State:            PeerStateWarm,
						PerformanceScore: 0.5,
						Connection:       &PeerConnection{Id: closeLockConnId(3), IsClient: true},
					},
				}
				return pg, id, pg.gossipChurn
			},
		},
		{
			name: "reconcile closes a bootstrap peer after bootstrap exit",
			setup: func(t *testing.T) (*PeerGovernor, ouroboros.ConnectionId, func()) {
				noRecovery := false
				pg := NewPeerGovernor(PeerGovernorConfig{
					Logger:                slog.New(slog.NewJSONHandler(io.Discard, nil)),
					EventBus:              newMockEventBus(),
					DisableOutbound:       true,
					AutoBootstrapRecovery: &noRecovery,
				})
				id := closeLockConnId(4)
				pg.bootstrapExited = true
				pg.peers = []*Peer{
					{
						Address:    "44.0.1.4:3001",
						Source:     PeerSourceTopologyBootstrapPeer,
						State:      PeerStateCold,
						Connection: &PeerConnection{Id: id, IsClient: true},
					},
				}
				return pg, id, func() { pg.reconcile(context.Background()) }
			},
		},
		{
			name: "reconcile removes a hot peer over the active target",
			setup: func(t *testing.T) (*PeerGovernor, ouroboros.ConnectionId, func()) {
				pg := NewPeerGovernor(PeerGovernorConfig{
					Logger:                    slog.New(slog.NewJSONHandler(io.Discard, nil)),
					EventBus:                  newMockEventBus(),
					DisableOutbound:           true,
					TargetNumberOfActivePeers: 1,
				})
				id := closeLockConnId(5)
				pg.peers = []*Peer{
					{
						Address:          "44.0.1.5:3001",
						Source:           PeerSourceP2PGossip,
						State:            PeerStateHot,
						PerformanceScore: 0.1,
						LastActivity:     time.Now(),
						Connection:       &PeerConnection{Id: id, IsClient: true},
					},
					{
						Address:          "44.0.1.6:3001",
						Source:           PeerSourceP2PGossip,
						State:            PeerStateHot,
						PerformanceScore: 0.9,
						LastActivity:     time.Now(),
						Connection:       &PeerConnection{Id: closeLockConnId(6), IsClient: true},
					},
				}
				return pg, id, func() { pg.reconcile(context.Background()) }
			},
		},
		{
			name: "topology reload removes a local root",
			setup: func(t *testing.T) (*PeerGovernor, ouroboros.ConnectionId, func()) {
				pg := NewPeerGovernor(PeerGovernorConfig{
					Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
					EventBus: newMockEventBus(),
				})
				id := closeLockConnId(7)
				pg.peers = []*Peer{
					{
						Address:    "44.0.1.7:3001",
						Source:     PeerSourceTopologyLocalRoot,
						State:      PeerStateHot,
						Connection: &PeerConnection{Id: id, IsClient: true},
					},
				}
				return pg, id, func() {
					pg.LoadTopologyConfig(&topology.TopologyConfig{})
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pg, id, pass := tc.setup(t)
			conn := newCallbackJoiningConn(t, pg, id)
			pg.connCloser = func(got ouroboros.ConnectionId) io.Closer {
				if sameConnectionId(got, id) {
					return conn
				}
				return nil
			}

			requirePassCompletes(t, pass)

			select {
			case <-conn.closeBegun:
			default:
				t.Fatal("the pass did not close the connection")
			}
			select {
			case <-conn.callbackDone:
			default:
				t.Fatal("Close returned before the callback finished")
			}
			require.False(t, conn.underLock.Load(), "Close ran with p.mu held")
			pg.mu.Lock()
			defer pg.mu.Unlock()
			idx := pg.peerIndexByConnId(id)
			require.Equal(t, -1, idx, "a peer still holds the closed connection")
		})
	}
}
