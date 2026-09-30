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

package connmanager

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func FuzzNormalizePeerAddr(f *testing.F) {
	f.Add("")
	f.Add("Example.COM:3001")
	f.Add("127.000.000.001:3001")
	f.Add("[0:0:0:0:0:0:0:1]:3001")
	f.Add("malformed:address:with:colons")

	f.Fuzz(func(t *testing.T, peerAddr string) {
		normalized := NormalizePeerAddr(peerAddr)
		if NormalizePeerAddr(normalized) != normalized {
			t.Fatalf("NormalizePeerAddr is not idempotent: %q -> %q",
				normalized,
				NormalizePeerAddr(normalized),
			)
		}

		inputHost, inputPort, err := net.SplitHostPort(peerAddr)
		if err != nil {
			if normalized != strings.ToLower(peerAddr) {
				t.Fatalf(
					"malformed address normalized to %q, want lowercase %q",
					normalized,
					strings.ToLower(peerAddr),
				)
			}
			return
		}

		host, port, err := net.SplitHostPort(normalized)
		if err != nil {
			t.Fatalf("normalized address is not host:port: %q", normalized)
		}
		if port != inputPort {
			t.Fatalf("normalized port = %q, want %q", port, inputPort)
		}
		if net.ParseIP(inputHost) == nil && host != strings.ToLower(inputHost) {
			t.Fatalf(
				"normalized hostname = %q, want %q",
				host,
				strings.ToLower(inputHost),
			)
		}
	})
}

func BenchmarkUpdateConnectionMetrics(b *testing.B) {
	for _, connections := range []int{10, 100, 500, 1000} {
		b.Run(strconv.Itoa(connections), func(b *testing.B) {
			cm := NewConnectionManager(ConnectionManagerConfig{
				PromRegistry: prometheus.NewRegistry(),
			})
			for i := range connections {
				addr := net.JoinHostPort(
					"198.51.100."+strconv.Itoa(i%250+1),
					strconv.Itoa(3000+i),
				)
				cm.connections[connectionIDForBench(i)] = &connectionInfo{
					peerAddr:  addr,
					isInbound: i%2 == 0,
				}
				if i%2 == 0 {
					cm.inboundCount++
				}
			}
			b.ReportAllocs()
			cm.updateConnectionMetrics()
			b.ResetTimer()
			for range b.N {
				cm.updateConnectionMetrics()
			}
		})
	}
}

func BenchmarkTryReserveInboundSlotParallel(b *testing.B) {
	for _, connections := range []int{10, 100, 500} {
		b.Run(strconv.Itoa(connections), func(b *testing.B) {
			cm := NewConnectionManager(ConnectionManagerConfig{
				MaxInboundConns: connections * 2,
			})
			for i := range connections {
				addr := net.JoinHostPort(
					"198.51.100."+strconv.Itoa(i%250+1),
					strconv.Itoa(3000+i),
				)
				cm.connections[connectionIDForBench(i)] = &connectionInfo{
					peerAddr:  addr,
					isInbound: true,
				}
			}
			cm.inboundCount = connections
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if cm.tryReserveInboundSlot() {
						cm.releaseInboundSlot()
					}
				}
			})
		})
	}
}

func BenchmarkHasInboundPeerAddress(b *testing.B) {
	for _, connections := range []int{10, 100, 500, 1000} {
		b.Run(strconv.Itoa(connections), func(b *testing.B) {
			cm := NewConnectionManager(ConnectionManagerConfig{
				OutboundSourcePort: 3001,
			})
			for i := range connections {
				addr := net.JoinHostPort(
					"198.51.100."+strconv.Itoa(i%250+1),
					strconv.Itoa(3000+i),
				)
				cm.connections[connectionIDForBench(i)] = &connectionInfo{
					peerAddr:  addr,
					isInbound: true,
					conn:      &ouroboros.Connection{},
				}
				cm.inboundCount++
				peerKey := NormalizePeerAddr(addr)
				cm.inboundPeerAddrs[peerKey]++
			}
			targetAddr := net.JoinHostPort("203.0.113.10", "4000")
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if cm.HasInboundPeerAddress(targetAddr) {
					b.Fatal("unexpected inbound peer-address match")
				}
			}
		})
	}
}

func connectionIDForBench(i int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: 3001 + i,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.IPv4(198, 51, 100, byte(i%250+1)),
			Port: 4001 + i,
		},
	}
}

func newUnstartedConnection(t *testing.T) *ouroboros.Connection {
	t.Helper()
	conn, err := ouroboros.NewConnection()
	require.NoError(t, err)
	return conn
}

func waitForConnectionManagerWatchers(
	t *testing.T,
	cm *ConnectionManager,
) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		cm.goroutineWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for connection manager watchers")
	}
}

func TestAddConnectionRejectsInboundCollisionWithOutbound(t *testing.T) {
	t.Parallel()

	cm := NewConnectionManager(ConnectionManagerConfig{})
	outbound := newUnstartedConnection(t)
	inbound := newUnstartedConnection(t)

	require.True(t, cm.AddConnection(outbound, false, "1.2.3.4:3001"))
	require.False(t, cm.AddConnection(inbound, true, "1.2.3.4:3001"))
	require.Same(t, outbound, cm.GetConnectionById(outbound.Id()))
	require.False(t, cm.IsInboundConnection(outbound.Id()))

	outbound.ErrorChan() <- nil
	waitForConnectionManagerWatchers(t, cm)
}

func TestReplacedConnectionCloseDoesNotPublishStaleEvent(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	_, closeEvents := bus.Subscribe(ConnectionClosedEventType)

	cm := NewConnectionManager(ConnectionManagerConfig{EventBus: bus})
	inbound := newUnstartedConnection(t)
	outbound := newUnstartedConnection(t)
	staleErr := errors.New("stale connection closed")
	liveErr := errors.New("live connection closed")

	require.True(t, cm.AddConnection(inbound, true, "1.2.3.4:3001"))
	require.True(t, cm.AddConnection(outbound, false, "1.2.3.4:3001"))
	require.Same(t, outbound, cm.GetConnectionById(outbound.Id()))

	inbound.ErrorChan() <- staleErr
	select {
	case evt := <-closeEvents:
		data, ok := evt.Data.(ConnectionClosedEvent)
		require.True(t, ok)
		t.Fatalf("received stale close event: %v", data.Error)
	case <-time.After(100 * time.Millisecond):
	}

	outbound.ErrorChan() <- liveErr
	select {
	case evt := <-closeEvents:
		data, ok := evt.Data.(ConnectionClosedEvent)
		require.True(t, ok)
		require.ErrorIs(t, data.Error, liveErr)
	case <-time.After(time.Second):
		t.Fatal("expected close event for live connection")
	}

	select {
	case evt := <-closeEvents:
		data, ok := evt.Data.(ConnectionClosedEvent)
		require.True(t, ok)
		t.Fatalf("received unexpected extra close event: %v", data.Error)
	case <-time.After(100 * time.Millisecond):
	}

	waitForConnectionManagerWatchers(t, cm)
}

// TestSameDirectionCollisionNotifiesEvictedConnection verifies that a
// connection evicted by a same-direction ConnectionId collision is closed
// directly by
// addConnectionImpl, so its own error-watcher goroutine never observes a
// live ErrorChan send -- and even if it did, RemoveConnection would already
// find the replacement's entry under connId and reject it. Without an
// explicit notification at the eviction site, ConnClosedFunc never fires for
// the evicted connection, so an evicted NtC connection's chainsync
// server-side client state (and its live chain iterator) would never be
// released.
func TestSameDirectionCollisionNotifiesEvictedConnection(t *testing.T) {
	t.Parallel()

	type call struct {
		isNtC bool
		err   error
	}
	calls := make(chan call, 2)
	cm := NewConnectionManager(ConnectionManagerConfig{
		ConnClosedFunc: func(_ ouroboros.ConnectionId, isNtC bool, err error) {
			calls <- call{isNtC: isNtC, err: err}
		},
	})
	first := newUnstartedConnection(t)
	second := newUnstartedConnection(t)

	require.True(
		t,
		cm.addConnectionImpl(first, true, true, "127.0.0.1:3002", "", nil),
	)
	require.True(
		t,
		cm.addConnectionImpl(second, true, true, "127.0.0.1:3002", "", nil),
	)
	require.Same(t, second, cm.GetConnectionById(second.Id()))

	c := testutil.RequireReceive(
		t,
		calls,
		time.Second,
		"expected a ConnClosedFunc call for the evicted connection",
	)
	require.True(t, c.isNtC, "evicted NtC connection must report isNtC=true")
	require.ErrorIs(t, c.err, errConnectionReplaced)

	// The evicted connection's own ErrorChan send must not produce a second,
	// stale call once it is actually closed.
	first.ErrorChan() <- errors.New("evicted connection closed")
	testutil.RequireNoReceive(
		t,
		calls,
		200*time.Millisecond,
		"evicted connection's own close must not re-trigger ConnClosedFunc",
	)

	second.ErrorChan() <- nil
	waitForConnectionManagerWatchers(t, cm)
}

func TestConnectionClosedOwnerCallbackPreservesConnectionLifetime(t *testing.T) {
	t.Parallel()
	type call struct {
		conn *ouroboros.Connection
		err  error
	}
	calls := make(chan call, 2)
	cm := NewConnectionManager(ConnectionManagerConfig{
		ConnClosedOwnerFunc: func(conn *ouroboros.Connection, _ bool, err error) {
			calls <- call{conn: conn, err: err}
		},
	})
	first := newUnstartedConnection(t)
	second := newUnstartedConnection(t)
	require.True(t, cm.addConnectionImpl(first, true, true, "127.0.0.1:3002", "", nil))
	require.True(t, cm.addConnectionImpl(second, true, true, "127.0.0.1:3002", "", nil))
	evicted := testutil.RequireReceive(t, calls, time.Second, "expected owner callback for collision eviction")
	require.Same(t, first, evicted.conn)
	require.ErrorIs(t, evicted.err, errConnectionReplaced)
	second.ErrorChan() <- errors.New("normal close")
	closed := testutil.RequireReceive(t, calls, time.Second, "expected owner callback for normal close")
	require.Same(t, second, closed.conn)
	first.ErrorChan() <- nil
	testutil.RequireNoReceive(
		t,
		calls,
		200*time.Millisecond,
		"evicted connection's late close must not trigger a second owner callback",
	)
	waitForConnectionManagerWatchers(t, cm)
}

func TestInboundCollisionOwnerCallbackUsesEvictedConnection(t *testing.T) {
	t.Parallel()

	type call struct {
		conn  *ouroboros.Connection
		isNtC bool
		err   error
	}
	calls := make(chan call, 2)
	cm := NewConnectionManager(ConnectionManagerConfig{
		ConnClosedOwnerFunc: func(conn *ouroboros.Connection, isNtC bool, err error) {
			calls <- call{conn: conn, isNtC: isNtC, err: err}
		},
	})
	inbound := newUnstartedConnection(t)
	outbound := newUnstartedConnection(t)

	require.True(t, cm.addConnectionImpl(
		inbound, true, true, "127.0.0.1:3002", "", nil,
	))
	require.True(t, cm.addConnectionImpl(
		outbound, false, false, "127.0.0.1:3002", "", nil,
	))
	require.Same(t, outbound, cm.GetConnectionById(outbound.Id()))

	evicted := testutil.RequireReceive(
		t,
		calls,
		time.Second,
		"expected owner callback for inbound collision eviction",
	)
	require.Same(t, inbound, evicted.conn)
	require.True(t, evicted.isNtC)
	require.ErrorIs(t, evicted.err, errConnectionReplaced)

	// The evicted transport may report its own close after the synchronous
	// collision notification, but that stale report must not invoke the owner
	// callback again or remove the replacement's ownership.
	inbound.ErrorChan() <- errors.New("late evicted close")
	testutil.RequireNoReceive(
		t,
		calls,
		200*time.Millisecond,
		"evicted connection's late close must not trigger a duplicate callback",
	)

	outbound.ErrorChan() <- errors.New("live connection closed")
	closed := testutil.RequireReceive(
		t,
		calls,
		time.Second,
		"expected owner callback for replacement close",
	)
	require.Same(t, outbound, closed.conn)
	require.False(t, closed.isNtC)
	require.EqualError(t, closed.err, "live connection closed")
	waitForConnectionManagerWatchers(t, cm)
}

func requireNtCCleanupCounts(
	t *testing.T,
	manager *ConnectionManager,
	want int,
) {
	t.Helper()
	manager.ntcAdmissionMutex.Lock()
	defer manager.ntcAdmissionMutex.Unlock()
	require.Equal(t, want, manager.ntcCount, "total NtC reservations")
	require.Equal(t, want, manager.ntcIPConns["127.0.0.1"],
		"per-IP NtC reservations")
	if want == 0 {
		require.Empty(t, manager.ntcIPConns)
	}
}

func TestNtCCleanupNormalCloseBeforeBlockedCallback(t *testing.T) {
	entered := make(chan struct{})
	unblock := make(chan struct{})
	releaseCallback := sync.OnceFunc(func() { close(unblock) })
	var calls atomic.Int32
	manager := NewConnectionManager(ConnectionManagerConfig{
		MaxNtCConns:            1,
		MaxNtCConnectionsPerIP: 1,
		ConnClosedFunc: func(_ ouroboros.ConnectionId, _ bool, _ error) {
			calls.Add(1)
			close(entered)
			<-unblock
		},
	})
	connection := newUnstartedConnection(t)
	finishConnection := sync.OnceFunc(func() { connection.ErrorChan() <- nil })
	t.Cleanup(func() {
		releaseCallback()
		finishConnection()
		waitForConnectionManagerWatchers(t, manager)
	})
	address := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002}
	release := manager.reserveNtCSlot(address, false)
	require.NotNil(t, release)
	var releases atomic.Int32
	require.True(t, manager.addConnectionImpl(
		connection, true, true, address.String(), "", func() {
			releases.Add(1)
			release()
		},
	))
	requireNtCCleanupCounts(t, manager, 1)
	finishConnection()
	testutil.RequireReceive(t, entered, time.Second, "close callback")
	requireNtCCleanupCounts(t, manager, 0)
	require.Nil(t, manager.GetConnectionById(connection.Id()))
	probeRelease := manager.reserveNtCSlot(address, false)
	require.NotNil(t, probeRelease, "blocked callback must allow a new client")
	t.Cleanup(probeRelease)
	releaseCallback()
	waitForConnectionManagerWatchers(t, manager)
	require.Equal(t, int32(1), releases.Load())
	require.Equal(t, int32(1), calls.Load())
	requireNtCCleanupCounts(t, manager, 1)
	probeRelease()
	requireNtCCleanupCounts(t, manager, 0)
}

func TestNtCCleanupCollisionReleasesBeforeBlockedCallback(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		inbound   bool
		remaining int
	}{
		{name: "same_direction", inbound: true, remaining: 1},
		{name: "outbound_evicts_inbound", inbound: false, remaining: 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			entered := make(chan struct{})
			unblock := make(chan struct{})
			releaseCallback := sync.OnceFunc(func() { close(unblock) })
			var calls atomic.Int32
			manager := NewConnectionManager(ConnectionManagerConfig{
				MaxNtCConns:            2,
				MaxNtCConnectionsPerIP: 2,
				ConnClosedFunc: func(_ ouroboros.ConnectionId, _ bool, err error) {
					calls.Add(1)
					if errors.Is(err, errConnectionReplaced) {
						close(entered)
						<-unblock
					}
				},
			})
			first := newUnstartedConnection(t)
			second := newUnstartedConnection(t)
			require.Equal(t, first.Id(), second.Id())
			finishFirst := sync.OnceFunc(func() { first.ErrorChan() <- nil })
			finishSecond := sync.OnceFunc(func() { second.ErrorChan() <- nil })
			added := make(chan bool, 1)
			addDone := make(chan struct{})
			t.Cleanup(func() {
				releaseCallback()
				finishFirst()
				select {
				case <-addDone:
					finishSecond()
				case <-time.After(time.Second):
				}
				waitForConnectionManagerWatchers(t, manager)
			})
			address := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002}
			firstRelease := manager.reserveNtCSlot(address, false)
			require.NotNil(t, firstRelease)
			var releases atomic.Int32
			require.True(t, manager.addConnectionImpl(
				first, true, true, address.String(), "", func() {
					releases.Add(1)
					firstRelease()
				},
			))
			var secondRelease func()
			if testCase.inbound {
				secondRelease = manager.reserveNtCSlot(address, false)
				require.NotNil(t, secondRelease)
			}
			requireNtCCleanupCounts(t, manager, testCase.remaining+1)
			go func() {
				added <- manager.addConnectionImpl(
					second, testCase.inbound, testCase.inbound,
					address.String(), "", secondRelease,
				)
				close(addDone)
			}()
			testutil.RequireReceive(
				t,
				entered,
				time.Second,
				"replacement callback",
			)
			requireNtCCleanupCounts(t, manager, testCase.remaining)
			require.Equal(t, int32(1), releases.Load())
			require.Nil(t, manager.GetConnectionById(first.Id()))
			releaseCallback()
			require.True(t, testutil.RequireReceive(
				t, added, time.Second, "replacement registration",
			))
			require.Same(t, second, manager.GetConnectionById(second.Id()))
			require.False(t, manager.RemoveConnection(first.Id(), first))
			probeRelease := manager.reserveNtCSlot(address, false)
			require.NotNil(
				t,
				probeRelease,
				"replacement leaked admission capacity",
			)
			t.Cleanup(probeRelease)
			finishFirst()
			finishSecond()
			waitForConnectionManagerWatchers(t, manager)
			require.Equal(
				t,
				int32(1),
				releases.Load(),
				"stale watcher double release",
			)
			require.Equal(
				t,
				int32(2),
				calls.Load(),
				"stale watcher close callback",
			)
			requireNtCCleanupCounts(t, manager, 1)
			probeRelease()
			requireNtCCleanupCounts(t, manager, 0)
		})
	}
}

// ConnClosedFunc is the only close notification an NtC connection gets --
// ConnectionClosedEventType is published for NtN closes only (see
// ntc_conn_closed_test.go). This pair of tests proves the callback
// distinguishes the two: before issue #3508's fix, ConnClosedFunc carried no
// isNtC parameter at all, so nothing downstream could tell an NtC close from
// an NtN one and wire NtC-specific chainsync teardown to it.
//
// Each test gets its own ConnectionManager: newUnstartedConnection returns
// connections whose ConnectionId is the zero value, so two of them
// registered with one manager would collide (see ntc_conn_closed_test.go).

func TestConnClosedFunc_ReceivesIsNtCTrueForNtCClose(t *testing.T) {
	t.Parallel()

	type call struct {
		isNtC bool
		err   error
	}
	calls := make(chan call, 1)
	cm := NewConnectionManager(ConnectionManagerConfig{
		ConnClosedFunc: func(_ ouroboros.ConnectionId, isNtC bool, err error) {
			calls <- call{isNtC: isNtC, err: err}
		},
	})
	conn := newUnstartedConnection(t)
	require.True(
		t,
		cm.addConnectionImpl(conn, true, true, "127.0.0.1:3002", "", nil),
	)

	closeErr := errors.New("ntc connection closed")
	conn.ErrorChan() <- closeErr
	waitForConnectionManagerWatchers(t, cm)

	c := testutil.RequireReceive(
		t,
		calls,
		time.Second,
		"expected a ConnClosedFunc call for the NtC connection",
	)
	require.True(t, c.isNtC, "NtC connection close must report isNtC=true")
	require.ErrorIs(t, c.err, closeErr)
}

func TestConnClosedFunc_ReceivesIsNtCFalseForNtNClose(t *testing.T) {
	t.Parallel()

	type call struct {
		isNtC bool
		err   error
	}
	calls := make(chan call, 1)
	cm := NewConnectionManager(ConnectionManagerConfig{
		ConnClosedFunc: func(_ ouroboros.ConnectionId, isNtC bool, err error) {
			calls <- call{isNtC: isNtC, err: err}
		},
	})
	conn := newUnstartedConnection(t)
	require.True(t, cm.AddConnection(conn, false, "1.2.3.4:3001"))

	closeErr := errors.New("ntn connection closed")
	conn.ErrorChan() <- closeErr
	waitForConnectionManagerWatchers(t, cm)

	c := testutil.RequireReceive(
		t,
		calls,
		time.Second,
		"expected a ConnClosedFunc call for the NtN connection",
	)
	require.False(t, c.isNtC, "NtN connection close must report isNtC=false")
	require.ErrorIs(t, c.err, closeErr)
}

// Each connection gets its own ConnectionManager: newUnstartedConnection
// returns connections whose ConnectionId is the zero value, so two of them
// registered with one manager collide and the second replaces the first. A
// replaced connection's close is already suppressed as stale, which would make
// these assertions pass for the wrong reason.
func newConnManagerWithCloseEvents(
	t *testing.T,
) (*ConnectionManager, <-chan event.Event) {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, closeEvents := bus.Subscribe(ConnectionClosedEventType)
	return NewConnectionManager(ConnectionManagerConfig{EventBus: bus}),
		closeEvents
}

// ConnectionClosedEventType exists to drive node-to-node peer management: its
// subscribers remove the connection from chain selection, peer governance,
// chainsync client state, and the mempool consumer set. None of that applies
// to a node-to-client connection, and the event payload carries no way for a
// subscriber to tell the two apart.
//
// Publishing it for NtC closes is not merely redundant. A local client that
// reconnects rapidly -- devnet's txpump opens and closes a connection roughly
// 90 times a second -- floods the topic, fills the 1024-slot delivery buffer,
// and wedges the subscriber permanently ("event delivery stalled: subscriber
// not draining"). Once that happens the NtN chainsync recovery driven by these
// events never runs again: the node quietly stops following the chain while
// continuing to forge, and only a restart clears it.
func TestNtCConnectionCloseDoesNotPublishPeerEvent(t *testing.T) {
	t.Parallel()

	cm, closeEvents := newConnManagerWithCloseEvents(t)
	conn := newUnstartedConnection(t)

	require.True(
		t,
		cm.addConnectionImpl(conn, true, true, "127.0.0.1:3002", "", nil),
	)

	conn.ErrorChan() <- errors.New("ntc connection closed")
	waitForConnectionManagerWatchers(t, cm)

	testutil.RequireNoReceive(
		t,
		closeEvents,
		200*time.Millisecond,
		"NtC close must not publish a peer connection-closed event",
	)
}

// The control for the test above: the same harness must still observe the
// event for a node-to-node connection, or NtN peer cleanup never happens.
func TestNtNConnectionClosePublishesPeerEvent(t *testing.T) {
	t.Parallel()

	cm, closeEvents := newConnManagerWithCloseEvents(t)
	conn := newUnstartedConnection(t)

	require.True(t, cm.AddConnection(conn, false, "1.2.3.4:3001"))

	connErr := errors.New("ntn connection closed")
	conn.ErrorChan() <- connErr

	evt := testutil.RequireReceive(
		t,
		closeEvents,
		time.Second,
		"expected close event for the NtN connection",
	)
	data, ok := evt.Data.(ConnectionClosedEvent)
	require.True(t, ok)
	require.ErrorIs(t, data.Error, connErr)

	waitForConnectionManagerWatchers(t, cm)
}

// Suppressing the event must not suppress the connection manager's own
// cleanup: the NtC connection still has to leave the connection table.
func TestNtCConnectionCloseStillRemovesConnection(t *testing.T) {
	t.Parallel()

	cm, _ := newConnManagerWithCloseEvents(t)
	conn := newUnstartedConnection(t)

	require.True(
		t,
		cm.addConnectionImpl(conn, true, true, "127.0.0.1:3002", "", nil),
	)
	require.NotNil(t, cm.GetConnectionById(conn.Id()))

	conn.ErrorChan() <- errors.New("ntc connection closed")
	waitForConnectionManagerWatchers(t, cm)

	require.Nil(
		t,
		cm.GetConnectionById(conn.Id()),
		"NtC connection was not removed from the connection manager",
	)
}

func TestNormalizePeerAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "ipv4 preserved",
			input:    "1.2.3.4:3001",
			expected: "1.2.3.4:3001",
		},
		{
			name:     "ipv6 canonicalized",
			input:    "[2001:0db8::1]:3001",
			expected: "[2001:db8::1]:3001",
		},
		{
			name:     "hostname lowercased",
			input:    "Relay.Example.Com:3001",
			expected: "relay.example.com:3001",
		},
		{
			name:     "invalid hostport lowercased",
			input:    "Relay.Example.Com",
			expected: "relay.example.com",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, NormalizePeerAddr(test.input))
		})
	}
}

func TestHasInboundPeerAddress(t *testing.T) {
	t.Parallel()

	cm := NewConnectionManager(ConnectionManagerConfig{
		OutboundSourcePort: 3001,
	})
	cm.connections[ouroboros.ConnectionId{}] = &connectionInfo{
		conn:      &ouroboros.Connection{},
		isInbound: true,
		peerAddr:  "1.2.3.4:3001",
	}
	cm.inboundPeerAddrs[NormalizePeerAddr("1.2.3.4:3001")]++
	cm.connections[ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 3002},
	}] = &connectionInfo{
		conn:      &ouroboros.Connection{},
		isInbound: true,
		peerAddr:  "1.2.3.4:3002",
	}
	cm.inboundPeerAddrs[NormalizePeerAddr("1.2.3.4:3002")]++
	cm.connections[ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 3999},
	}] = &connectionInfo{
		conn:      &ouroboros.Connection{},
		isInbound: false,
		peerAddr:  "1.2.3.4:3001",
	}
	cm.connections[ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 3003},
	}] = &connectionInfo{
		conn:      nil,
		isInbound: true,
		peerAddr:  "1.2.3.4:3003",
	}

	assert.True(t, cm.HasInboundPeerAddress("1.2.3.4:3001"))
	assert.True(t, cm.HasInboundPeerAddress("1.2.3.4:3002"))
	assert.False(t, cm.HasInboundPeerAddress("1.2.3.4:3999"))
	assert.False(t, cm.HasInboundPeerAddress("1.2.3.4:3003"))
}

func TestHasInboundPeerAddressDisabledWithoutPortReuse(t *testing.T) {
	t.Parallel()

	cm := NewConnectionManager(ConnectionManagerConfig{})
	cm.connections[ouroboros.ConnectionId{}] = &connectionInfo{
		conn:      &ouroboros.Connection{},
		isInbound: true,
		peerAddr:  "1.2.3.4:3001",
	}
	cm.inboundPeerAddrs[NormalizePeerAddr("1.2.3.4:3001")]++

	assert.False(t, cm.HasInboundPeerAddress("1.2.3.4:3001"))
}
