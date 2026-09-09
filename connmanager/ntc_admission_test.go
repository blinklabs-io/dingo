package connmanager

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestNtCAdmissionSlotReleasedBeforeCloseCallback(t *testing.T) {
	closeErr := errors.New("closed")
	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	var releaseCallbackOnce sync.Once
	releaseCallbackFunc := func() {
		releaseCallbackOnce.Do(func() { close(releaseCallback) })
	}
	defer releaseCallbackFunc()
	manager := NewConnectionManager(ConnectionManagerConfig{
		MaxNtCConns: 1,
		ConnClosedFunc: func(_ ouroboros.ConnectionId, _ bool, _ error) {
			close(callbackEntered)
			<-releaseCallback
		},
	})
	conn := newUnstartedConnection(t)
	release := manager.reserveNtCSlot(nil)
	require.NotNil(t, release)
	require.True(t, manager.addConnectionImpl(conn, true, true, "local", "", release))

	conn.ErrorChan() <- closeErr
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("close callback was not invoked")
	}
	manager.ntcAdmissionMutex.Lock()
	count := manager.ntcCount
	manager.ntcAdmissionMutex.Unlock()
	require.Zero(t, count, "NtC admission slot remained held by a blocking callback")
	releaseCallbackFunc()
	waitForConnectionManagerWatchers(t, manager)
}

func startNtCAdmissionManager(
	t *testing.T,
	cfg ConnectionManagerConfig,
	listeners ...net.Listener,
) *ConnectionManager {
	t.Helper()
	cfg.PromRegistry = prometheus.NewRegistry()
	for _, listener := range listeners {
		t.Cleanup(func() { _ = listener.Close() })
		cfg.Listeners = append(cfg.Listeners, ListenerConfig{
			Listener: listener,
			UseNtC:   true,
			ConnectionOpts: []ouroboros.ConnectionOptionFunc{
				ouroboros.WithNetworkMagic(1),
			},
		})
	}
	manager := NewConnectionManager(cfg)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require.NoError(t, manager.Stop(ctx))
	})
	require.NoError(t, manager.Start(t.Context()))
	return manager
}

func dialNtCAdmissionClient(t *testing.T, listener net.Listener) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout(
		listener.Addr().Network(), listener.Addr().String(), 2*time.Second,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func connectNtCAdmissionClient(
	t *testing.T,
	listener net.Listener,
) *ouroboros.Connection {
	t.Helper()
	conn := dialNtCAdmissionClient(t, listener)
	require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))
	client, err := ouroboros.NewConnection(
		ouroboros.WithConnection(conn),
		ouroboros.WithNetworkMagic(1),
		ouroboros.WithNodeToNode(false),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, conn.SetDeadline(time.Time{}))
	return client
}

func requireNtCAdmissionCount(
	t *testing.T,
	manager *ConnectionManager,
	count int,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		manager.ntcAdmissionMutex.Lock()
		defer manager.ntcAdmissionMutex.Unlock()
		return manager.ntcCount == count
	}, 2*time.Second, time.Millisecond, "unexpected NtC admission count")
}

func requireNtCAdmissionRejected(t *testing.T, listener net.Listener) {
	t.Helper()
	conn := dialNtCAdmissionClient(t, listener)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	var data [1]byte
	_, err := conn.Read(data[:])
	require.ErrorIs(
		t,
		err,
		io.EOF,
		"over-limit NtC connection must close before handshake",
	)
}

func TestNtCAdmissionPendingAndEstablished(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		total int
		perIP int
	}{
		{name: "total_limit", total: 2, perIP: 3},
		{name: "per_ip_limit", total: 3, perIP: 2},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			firstListener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { _ = firstListener.Close() })
			secondListener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			manager := startNtCAdmissionManager(t, ConnectionManagerConfig{
				MaxNtCConns:            testCase.total,
				MaxNtCConnectionsPerIP: testCase.perIP,
				MaxInboundConns:        1,
				MaxConnectionsPerIP:    1,
			}, firstListener, secondListener)
			first := connectNtCAdmissionClient(t, firstListener)
			requireNtCAdmissionCount(t, manager, 1)
			require.Eventually(t, func() bool {
				manager.connectionsMutex.Lock()
				defer manager.connectionsMutex.Unlock()
				return len(manager.connections) == 1
			}, 2*time.Second, time.Millisecond)
			silent := dialNtCAdmissionClient(t, secondListener)
			requireNtCAdmissionCount(t, manager, 2)
			requireNtCAdmissionRejected(t, firstListener)
			require.Equal(t, float64(1), testutil.ToFloat64(
				manager.metrics.ntcRejectedConns.WithLabelValues(testCase.name),
			))
			require.Eventually(t, func() bool {
				manager.listenersMutex.Lock()
				defer manager.listenersMutex.Unlock()
				return len(manager.pendingConns) == 1
			}, 2*time.Second, time.Millisecond, "rejected NtC connection remained pending")

			require.Zero(t, manager.InboundCount())
			require.True(t, manager.tryReserveInboundSlot())
			t.Cleanup(manager.releaseInboundSlot)
			require.True(t, manager.acquireIPSlot("127.0.0.1"))
			t.Cleanup(func() { manager.releaseIPSlot("127.0.0.1") })

			require.NoError(t, first.Close())
			requireNtCAdmissionCount(t, manager, 1)
			connectNtCAdmissionClient(t, secondListener)
			requireNtCAdmissionCount(t, manager, 2)
			require.NoError(t, silent.Close())
			requireNtCAdmissionCount(t, manager, 1)
			silent = dialNtCAdmissionClient(t, firstListener)
			requireNtCAdmissionCount(t, manager, 2)

			stopCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			require.NoError(t, manager.Stop(stopCtx))
			manager.ntcAdmissionMutex.Lock()
			count, sourceCount := manager.ntcCount, len(manager.ntcIPConns)
			manager.ntcAdmissionMutex.Unlock()
			require.Zero(t, count, "shutdown leaked NtC slots")
			require.Zero(t, sourceCount, "shutdown leaked NtC source slots")
			require.NoError(
				t,
				silent.SetReadDeadline(time.Now().Add(time.Second)),
			)
			var data [1]byte
			_, err = silent.Read(data[:])
			require.ErrorIs(t, err, io.EOF)
		})
	}
}

func TestNtCAdmissionUnix(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-domain sockets are not the Windows NtC transport")
	}
	listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "ntc.sock"))
	require.NoError(t, err)
	manager := startNtCAdmissionManager(t, ConnectionManagerConfig{
		MaxNtCConns: 2, MaxNtCConnectionsPerIP: 1,
	}, listener)
	first := connectNtCAdmissionClient(t, listener)
	connectNtCAdmissionClient(t, listener)
	requireNtCAdmissionCount(t, manager, 2)
	requireNtCAdmissionRejected(t, listener)
	manager.ntcAdmissionMutex.Lock()
	sources := len(manager.ntcIPConns)
	manager.ntcAdmissionMutex.Unlock()
	require.Zero(t, sources, "Unix sockets must not consume IP slots")
	require.NoError(t, first.Close())
	requireNtCAdmissionCount(t, manager, 1)
	connectNtCAdmissionClient(t, listener)
	requireNtCAdmissionCount(t, manager, 2)
}

func TestNtCAdmissionConcurrentReservations(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		total       int
		perIP       int
		addressByte int
	}{
		{name: "total_limit", total: 2, perIP: 3, addressByte: 7},
		{name: "per_ip_limit", total: 3, perIP: 2, addressByte: 15},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager := NewConnectionManager(ConnectionManagerConfig{
				MaxNtCConns: testCase.total, MaxNtCConnectionsPerIP: testCase.perIP,
			})
			var workers sync.WaitGroup
			var accepted atomic.Int32
			start := make(chan struct{})
			releases := make(chan func(), 64)
			for index := range 64 {
				workers.Go(func() {
					<-start
					address := net.ParseIP("2001:db8::").To16()
					address[testCase.addressByte] = byte(index)
					release := manager.reserveNtCSlot(&net.TCPAddr{
						IP: address, Port: index + 1000,
					})
					if release != nil {
						accepted.Add(1)
						releases <- release
					}
				})
			}
			close(start)
			workers.Wait()
			close(releases)
			require.Equal(
				t,
				int32(2),
				accepted.Load(),
				"concurrent sources exceeded their NtC budget",
			)
			for release := range releases {
				release()
				release()
			}
			requireNtCAdmissionCount(t, manager, 0)
			manager.ntcAdmissionMutex.Lock()
			sources := len(manager.ntcIPConns)
			manager.ntcAdmissionMutex.Unlock()
			require.Zero(t, sources)
		})
	}
}
