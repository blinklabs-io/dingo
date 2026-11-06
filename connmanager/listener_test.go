// Copyright 2025 Blink Labs Software
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
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestCalculateAcceptBackoff(t *testing.T) {
	t.Parallel()

	cm := NewConnectionManager(ConnectionManagerConfig{})

	tests := []struct {
		name              string
		consecutiveErrors int
		expectedBackoff   time.Duration
	}{
		{
			name:              "zero errors returns min backoff",
			consecutiveErrors: 0,
			expectedBackoff:   acceptBackoffMin,
		},
		{
			name:              "negative errors returns min backoff",
			consecutiveErrors: -1,
			expectedBackoff:   acceptBackoffMin,
		},
		{
			name:              "1 error returns min backoff",
			consecutiveErrors: 1,
			expectedBackoff:   10 * time.Millisecond, // 10ms * 2^0
		},
		{
			name:              "2 errors doubles min",
			consecutiveErrors: 2,
			expectedBackoff:   20 * time.Millisecond, // 10ms * 2^1
		},
		{
			name:              "3 errors",
			consecutiveErrors: 3,
			expectedBackoff:   40 * time.Millisecond, // 10ms * 2^2
		},
		{
			name:              "6 errors",
			consecutiveErrors: 6,
			expectedBackoff:   320 * time.Millisecond, // 10ms * 2^5
		},
		{
			name:              "7 errors (at cap)",
			consecutiveErrors: 7,
			expectedBackoff:   640 * time.Millisecond, // 10ms * 2^6 (capped)
		},
		{
			name:              "100 errors capped by exponent cap",
			consecutiveErrors: 100,
			expectedBackoff:   640 * time.Millisecond, // 10ms * 2^6 (capped)
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backoff := cm.calculateAcceptBackoff(tc.consecutiveErrors)
			assert.Equal(t, tc.expectedBackoff, backoff)
		})
	}
}

// mockListener implements net.Listener for testing
type mockListener struct {
	acceptCalls atomic.Int32
	acceptErr   error
	closed      atomic.Bool
	closeCh     chan struct{}
}

func newMockListener() *mockListener {
	return &mockListener{
		closeCh: make(chan struct{}),
	}
}

func (m *mockListener) Accept() (net.Conn, error) {
	m.acceptCalls.Add(1)
	if m.closed.Load() {
		return nil, net.ErrClosed
	}
	if m.acceptErr != nil {
		return nil, m.acceptErr
	}
	// Block until closed
	<-m.closeCh
	return nil, net.ErrClosed
}

func (m *mockListener) Close() error {
	// Use atomic swap to ensure idempotent close
	if m.closed.Swap(true) {
		return nil // Already closed
	}
	close(m.closeCh)
	return nil
}

func (m *mockListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
}

func (m *mockListener) AcceptCount() int {
	return int(m.acceptCalls.Load())
}

// toggleMockListener is a net.Listener that can toggle between errors and success
type toggleMockListener struct {
	closed        atomic.Bool
	closeCh       chan struct{}
	errorEnabled  atomic.Bool
	acceptErr     error
	connCh        chan net.Conn // channel to provide mock connections
	acceptEntered chan struct{} // signaled when Accept() is entered
}

func newToggleMockListener() *toggleMockListener {
	return &toggleMockListener{
		closeCh:       make(chan struct{}),
		acceptErr:     errors.New("simulated accept error"),
		connCh:        make(chan net.Conn, 100),
		acceptEntered: make(chan struct{}, 100),
	}
}

func (m *toggleMockListener) Accept() (net.Conn, error) {
	// Signal that Accept() has been entered (non-blocking)
	select {
	case m.acceptEntered <- struct{}{}:
	default:
	}

	if m.closed.Load() {
		return nil, net.ErrClosed
	}
	if m.errorEnabled.Load() {
		return nil, m.acceptErr
	}
	// Try to get a connection from the channel, or wait for close
	select {
	case conn := <-m.connCh:
		return conn, nil
	case <-m.closeCh:
		return nil, net.ErrClosed
	}
}

func (m *toggleMockListener) Close() error {
	if m.closed.Swap(true) {
		return nil
	}
	close(m.closeCh)
	return nil
}

func (m *toggleMockListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
}

func (m *toggleMockListener) SetErrorEnabled(enabled bool) {
	m.errorEnabled.Store(enabled)
}

func (m *toggleMockListener) ProvideConnection(conn net.Conn) {
	m.connCh <- conn
}

// WaitForAcceptEntered waits for Accept() to be called, or until timeout expires.
// Returns true if Accept() was entered, false on timeout.
func (m *toggleMockListener) WaitForAcceptEntered(timeout time.Duration) bool {
	select {
	case <-m.acceptEntered:
		return true
	case <-time.After(timeout):
		return false
	}
}

// mockConn implements net.Conn for testing
type mockConn struct {
	localAddr  net.Addr
	remoteAddr net.Addr
	closed     atomic.Bool
}

func (c *mockConn) Read(_ []byte) (int, error) {
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	return 0, io.EOF
}

func (c *mockConn) Write(_ []byte) (int, error) {
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	return 0, io.EOF
}

func (c *mockConn) Close() error {
	c.closed.Store(true)
	return nil
}

func (c *mockConn) LocalAddr() net.Addr  { return c.localAddr }
func (c *mockConn) RemoteAddr() net.Addr { return c.remoteAddr }

func (c *mockConn) SetDeadline(_ time.Time) error      { return nil }
func (c *mockConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *mockConn) SetWriteDeadline(_ time.Time) error { return nil }

// trackingMockConn is like mockConn but records when Close is called.
type trackingMockConn struct {
	mockConn
	closeCalled atomic.Bool
}

func newTrackingMockConn(remotePort int) *trackingMockConn {
	tc := &trackingMockConn{}
	tc.localAddr = &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
	tc.remoteAddr = &net.TCPAddr{
		IP:   net.ParseIP("127.0.0.2"),
		Port: remotePort,
	}
	// Do NOT pre-close so we can detect the limit-rejection Close call
	return tc
}

func (c *trackingMockConn) Close() error {
	c.closeCalled.Store(true)
	c.closed.Store(true)
	return nil
}

func (c *trackingMockConn) WasClosed() bool {
	return c.closeCalled.Load()
}

func TestInboundConnectionLimit_RejectsOverLimit(t *testing.T) {
	defer goleak.VerifyNone(t)

	const maxInbound = 3

	mockLn := newToggleMockListener()
	mockLn.SetErrorEnabled(false) // Will provide connections via channel

	cfg := ConnectionManagerConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry:    prometheus.NewRegistry(),
		MaxInboundConns: maxInbound,
		Listeners: []ListenerConfig{
			{
				Listener: mockLn,
			},
		},
	}

	cm := NewConnectionManager(cfg)

	// Pre-populate the connections map with fake inbound connections
	// to simulate being at the limit. We use fake ConnectionIds.
	cm.connectionsMutex.Lock()
	for i := range maxInbound {
		fakeId := ouroboros.ConnectionId{
			LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
			RemoteAddr: &net.TCPAddr{
				IP:   net.ParseIP("10.0.0.1"),
				Port: 40000 + i,
			},
		}
		cm.connections[fakeId] = &connectionInfo{
			conn:      nil,
			peerAddr:  "192.168.1.1:9999",
			isInbound: true,
		}
		cm.inboundCount++
	}
	cm.connectionsMutex.Unlock()

	err := cm.Start(context.Background())
	require.NoError(t, err)

	// Provide a connection that should be rejected
	rejected := newTrackingMockConn(60000)
	mockLn.ProvideConnection(rejected)

	// Wait for the accept loop to process it and return to Accept()
	entered := mockLn.WaitForAcceptEntered(5 * time.Second)
	require.True(
		t,
		entered,
		"accept loop should return to Accept() after rejecting connection",
	)

	// The rejected connection should have been closed
	require.Eventually(t, func() bool {
		return rejected.WasClosed()
	}, 2*time.Second, 5*time.Millisecond,
		"connection over limit should be closed")

	// The connection should NOT have been added to the connections map
	cm.connectionsMutex.Lock()
	connCount := len(cm.connections)
	cm.connectionsMutex.Unlock()
	assert.Equal(t, maxInbound, connCount,
		"no new connection should be added when at the limit")

	// Clean up fake connections before stopping
	cm.connectionsMutex.Lock()
	for k := range cm.connections {
		delete(cm.connections, k)
	}
	cm.inboundCount = 0
	cm.connectionsMutex.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = cm.Stop(ctx)
	require.NoError(t, err)
}

func TestAcceptLoopDoesNotBlockOnSilentHandshake(t *testing.T) {
	defer goleak.VerifyNone(t)

	mockLn := newToggleMockListener()
	cfg := ConnectionManagerConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry:    prometheus.NewRegistry(),
		MaxInboundConns: 2,
		Listeners: []ListenerConfig{{
			Listener: mockLn,
			ConnectionOpts: []ouroboros.ConnectionOptionFunc{
				ouroboros.WithNetworkMagic(1),
			},
		}},
	}

	cm := NewConnectionManager(cfg)
	ctx := t.Context()
	require.NoError(t, cm.Start(ctx))
	// Consume the signal for the initial Accept call. The next signal must
	// come after the silent peer has been accepted and setup was dispatched.
	require.True(t, mockLn.WaitForAcceptEntered(2*time.Second))

	silent, peer := net.Pipe()
	defer peer.Close()
	mockLn.ProvideConnection(silent)
	require.True(
		t,
		mockLn.WaitForAcceptEntered(2*time.Second),
		"silent handshake must not serialize the listener accept loop",
	)

	// Stop must close pending handshakes instead of waiting for the production
	// timeout. The accept worker is tracked and must exit cleanly.
	stopCtx, stopCancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer stopCancel()
	require.NoError(t, cm.Stop(stopCtx))
}

func TestInboundLimitRejectionUntracksPendingConnection(t *testing.T) {
	mockLn := newToggleMockListener()
	cm := NewConnectionManager(ConnectionManagerConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry:    prometheus.NewRegistry(),
		MaxInboundConns: 1,
		Listeners: []ListenerConfig{{
			Listener: mockLn,
		}},
	})
	t.Cleanup(func() { goleak.VerifyNone(t) })

	// Occupy the only inbound slot so the accept loop rejects the next bearer
	// before dispatching its handshake goroutine.
	require.True(t, cm.tryReserveInboundSlot())
	t.Cleanup(cm.releaseInboundSlot)
	require.NoError(t, cm.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})
	require.True(t, mockLn.WaitForAcceptEntered(2*time.Second))

	rejected, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	mockLn.ProvideConnection(rejected)
	require.True(
		t,
		mockLn.WaitForAcceptEntered(2*time.Second),
		"accept loop must continue after the inbound limit rejection",
	)

	cm.listenersMutex.Lock()
	pending := len(cm.pendingConns)
	cm.listenersMutex.Unlock()
	require.Zero(
		t,
		pending,
		"a connection closed during admission must not remain pending",
	)
}

func TestAcceptLoopBackoffOnError(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Create a mock listener that always returns an error
	mockLn := newMockListener()
	mockLn.acceptErr = errors.New("simulated accept error")

	cfg := ConnectionManagerConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry: prometheus.NewRegistry(),
		Listeners: []ListenerConfig{
			{
				Listener: mockLn,
			},
		},
	}

	start := time.Now()
	cm := NewConnectionManager(cfg)
	err := cm.Start(context.Background())
	require.NoError(t, err)

	// Wait for at least one accept call before measuring backoff behavior
	require.Eventually(t, func() bool {
		return mockLn.AcceptCount() >= 1
	}, 2*time.Second, 5*time.Millisecond, "accept should be called at least once")

	// Reaching the fourth Accept takes at least the first three backoffs
	// (10ms + 20ms + 40ms); a tight retry loop would get there immediately.
	// Load can only lengthen the wait, so the lower bound cannot flake.
	testutil.WaitForCondition(t, func() bool {
		return mockLn.AcceptCount() >= 4
	}, 5*time.Second, "accept should be retried with backoff")
	assert.GreaterOrEqual(
		t,
		time.Since(start),
		50*time.Millisecond,
		"retries should be spaced by the exponential backoff",
	)

	// Stop the connection manager
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = cm.Stop(ctx)
	require.NoError(t, err)

	// Verify backoff worked - should have far fewer than 1000 calls
	// Without backoff, a tight loop would have many more
	acceptCount := mockLn.AcceptCount()
	t.Logf("Accept was called %d times", acceptCount)

	// The loop is stopped right after the fourth Accept, so a few more calls
	// at most; a tight loop would have run far past this bound by then.
	assert.Less(t, acceptCount, 20, "backoff should limit accept calls")
	assert.Greater(
		t,
		acceptCount,
		0,
		"accept should have been called at least once",
	)
}

func TestTryReserveInboundSlot_Concurrent(t *testing.T) {
	t.Parallel()

	// This test verifies that tryReserveInboundSlot is atomic:
	// many goroutines racing to reserve slots should never exceed
	// MaxInboundConns total (existing connections + reservations).
	const maxInbound = 5
	const goroutines = 50

	cm := NewConnectionManager(ConnectionManagerConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxInboundConns: maxInbound,
	})

	// Pre-populate with 3 existing inbound connections, leaving 2 slots
	cm.connectionsMutex.Lock()
	for i := range 3 {
		fakeId := ouroboros.ConnectionId{
			LocalAddr: &net.TCPAddr{
				IP:   net.ParseIP("127.0.0.1"),
				Port: 3001,
			},
			RemoteAddr: &net.TCPAddr{
				IP:   net.ParseIP("10.0.0.1"),
				Port: 40000 + i,
			},
		}
		cm.connections[fakeId] = &connectionInfo{
			conn:      nil,
			peerAddr:  "192.168.1.1:9999",
			isInbound: true,
		}
		cm.inboundCount++
	}
	cm.connectionsMutex.Unlock()

	// Race: 50 goroutines all try to reserve a slot concurrently.
	// Only 2 should succeed (maxInbound=5, existing=3).
	var reserved atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			<-start // synchronize all goroutines
			if cm.tryReserveInboundSlot() {
				reserved.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(
		t,
		int32(2),
		reserved.Load(),
		"exactly 2 slots should be reserved (maxInbound=5, existing=3)",
	)

	// Verify internal state is consistent
	cm.connectionsMutex.Lock()
	assert.Equal(t, 2, cm.inboundReserved,
		"inboundReserved should be 2")
	cm.connectionsMutex.Unlock()

	// Release the slots and verify they can be re-reserved
	cm.releaseInboundSlot()
	cm.releaseInboundSlot()

	cm.connectionsMutex.Lock()
	assert.Equal(t, 0, cm.inboundReserved,
		"inboundReserved should be 0 after releasing")
	cm.connectionsMutex.Unlock()

	// Now 2 more should be reservable again
	ok1 := cm.tryReserveInboundSlot()
	ok2 := cm.tryReserveInboundSlot()
	ok3 := cm.tryReserveInboundSlot()
	assert.True(t, ok1, "first re-reservation should succeed")
	assert.True(t, ok2, "second re-reservation should succeed")
	assert.False(t, ok3, "third re-reservation should fail")

	// Clean up
	cm.releaseInboundSlot()
	cm.releaseInboundSlot()
}

func TestTryReserveInboundSlot_ConsumeAndRelease(t *testing.T) {
	t.Parallel()

	// Verify that dropping a reservation lets the slot be tracked by the
	// actual connection in the connections map instead.
	const maxInbound = 2

	cm := NewConnectionManager(ConnectionManagerConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxInboundConns: maxInbound,
	})

	// Reserve both slots
	ok1 := cm.tryReserveInboundSlot()
	ok2 := cm.tryReserveInboundSlot()
	ok3 := cm.tryReserveInboundSlot()
	require.True(t, ok1)
	require.True(t, ok2)
	require.False(t, ok3, "should not reserve beyond limit")

	// Drop one reservation (simulating successful AddConnection)
	cm.releaseInboundSlot()

	// Add a fake connection to represent the consumed slot
	cm.connectionsMutex.Lock()
	fakeId := ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 3001,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.ParseIP("10.0.0.1"),
			Port: 40000,
		},
	}
	cm.connections[fakeId] = &connectionInfo{
		conn:      nil,
		peerAddr:  "10.0.0.1:40000",
		isInbound: true,
	}
	cm.inboundCount++
	cm.connectionsMutex.Unlock()

	// Should still not be able to reserve (1 connection + 1 reservation = 2 = max)
	ok4 := cm.tryReserveInboundSlot()
	assert.False(t, ok4,
		"should not reserve when connections + reservations = max")

	// Release the remaining reservation (simulating setup failure)
	cm.releaseInboundSlot()

	// Now should be able to reserve again (1 connection + 0 reservations < 2)
	ok5 := cm.tryReserveInboundSlot()
	assert.True(t, ok5,
		"should reserve after releasing failed reservation")

	// Clean up
	cm.releaseInboundSlot()
}
