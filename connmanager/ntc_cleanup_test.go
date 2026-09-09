package connmanager

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

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
	release := manager.reserveNtCSlot(address)
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
	probeRelease := manager.reserveNtCSlot(address)
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
			firstRelease := manager.reserveNtCSlot(address)
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
				secondRelease = manager.reserveNtCSlot(address)
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
			testutil.RequireReceive(t, entered, time.Second, "replacement callback")
			requireNtCCleanupCounts(t, manager, testCase.remaining)
			require.Equal(t, int32(1), releases.Load())
			require.Nil(t, manager.GetConnectionById(first.Id()))
			releaseCallback()
			require.True(t, testutil.RequireReceive(
				t, added, time.Second, "replacement registration",
			))
			require.Same(t, second, manager.GetConnectionById(second.Id()))
			require.False(t, manager.RemoveConnection(first.Id(), first))
			probeRelease := manager.reserveNtCSlot(address)
			require.NotNil(t, probeRelease, "replacement leaked admission capacity")
			t.Cleanup(probeRelease)
			finishFirst()
			finishSecond()
			waitForConnectionManagerWatchers(t, manager)
			require.Equal(t, int32(1), releases.Load(), "stale watcher double release")
			require.Equal(t, int32(2), calls.Load(), "stale watcher close callback")
			requireNtCCleanupCounts(t, manager, 1)
			probeRelease()
			requireNtCCleanupCounts(t, manager, 0)
		})
	}
}
