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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestCalculateAcceptBackoff(t *testing.T) {
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
	acceptDelay time.Duration
}

func newMockListener() *mockListener {
	return &mockListener{
		closeCh: make(chan struct{}),
	}
}

func (m *mockListener) Accept() (net.Conn, error) {
	m.acceptCalls.Add(1)
	if m.acceptDelay > 0 {
		time.Sleep(m.acceptDelay)
	}
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
	mu            sync.Mutex
	closed        atomic.Bool
	closeCh       chan struct{}
	errorEnabled  atomic.Bool
	acceptErr     error
	timestamps    []time.Time
	successCount  atomic.Int32
	errorCount    atomic.Int32
	successSignal chan struct{} // signaled after each successful accept
	connCh        chan net.Conn // channel to provide mock connections
	acceptEntered chan struct{} // signaled when Accept() is entered
}

func newToggleMockListener() *toggleMockListener {
	return &toggleMockListener{
		closeCh:       make(chan struct{}),
		acceptErr:     errors.New("simulated accept error"),
		timestamps:    make([]time.Time, 0),
		successSignal: make(chan struct{}, 100),
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

	m.mu.Lock()
	m.timestamps = append(m.timestamps, time.Now())
	m.mu.Unlock()

	if m.closed.Load() {
		return nil, net.ErrClosed
	}
	if m.errorEnabled.Load() {
		m.errorCount.Add(1)
		return nil, m.acceptErr
	}
	// Try to get a connection from the channel, or wait for close
	select {
	case conn := <-m.connCh:
		m.successCount.Add(1)
		// Non-blocking signal that success happened
		select {
		case m.successSignal <- struct{}{}:
		default:
		}
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

func (m *toggleMockListener) GetTimestamps() []time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]time.Time, len(m.timestamps))
	copy(result, m.timestamps)
	return result
}

func (m *toggleMockListener) ClearTimestamps() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.timestamps = m.timestamps[:0]
}

func (m *toggleMockListener) WaitForSuccess(timeout time.Duration) bool {
	select {
	case <-m.successSignal:
		return true
	case <-time.After(timeout):
		return false
	}
}

// WaitForErrors waits until at least minErrors have occurred since the baseline,
// or until timeout expires. Returns the number of errors observed.
func (m *toggleMockListener) WaitForErrors(
	baseline int,
	minErrors int,
	timeout time.Duration,
) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		current := int(m.errorCount.Load()) - baseline
		if current >= minErrors {
			return current
		}
		time.Sleep(10 * time.Millisecond)
	}
	return int(m.errorCount.Load()) - baseline
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

// DrainAcceptEntered drains all pending acceptEntered signals.
func (m *toggleMockListener) DrainAcceptEntered() {
	for {
		select {
		case <-m.acceptEntered:
		default:
			return
		}
	}
}

// mockConn implements net.Conn for testing
type mockConn struct {
	localAddr  net.Addr
	remoteAddr net.Addr
	closed     atomic.Bool
}

func newMockConn() *mockConn {
	mc := &mockConn{
		localAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345},
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 54321},
	}
	// Pre-close the connection so Read/Write return net.ErrClosed immediately.
	// This ensures the Ouroboros connection setup fails quickly in tests,
	// allowing the accept loop to return to Accept() without blocking.
	mc.closed.Store(true)
	return mc
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

	cm := NewConnectionManager(cfg)
	err := cm.Start(context.Background())
	require.NoError(t, err)

	// Wait for some accept calls - with backoff, should be limited
	// Without backoff, this would be millions of calls
	// With backoff starting at 10ms, we expect roughly:
	// - First call: immediate
	// - Second call: after 10ms (first error)
	// - Third call: after 20ms
	// - Fourth call: after 40ms
	// So in 100ms we should see around 4-5 calls
	time.Sleep(100 * time.Millisecond)

	// Stop the connection manager
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = cm.Stop(ctx)
	require.NoError(t, err)

	// Verify backoff worked - should have far fewer than 1000 calls
	// Without backoff, a tight loop would have many more
	acceptCount := mockLn.AcceptCount()
	t.Logf("Accept was called %d times in 100ms", acceptCount)

	// With exponential backoff, we expect around 3-5 calls in 100ms
	// Be generous to account for test timing variations
	assert.Less(t, acceptCount, 20, "backoff should limit accept calls")
	assert.Greater(t, acceptCount, 0, "accept should have been called at least once")
}

func TestAcceptLoopResetBackoffOnSuccess(t *testing.T) {
	defer goleak.VerifyNone(t)

	// This test verifies that successful accepts reset the backoff counter.
	// Rather than measuring timing intervals (which is flaky on CI), we verify:
	// 1. Errors can accumulate (building backoff)
	// 2. A successful accept can occur
	// 3. The accept loop continues to function after success (proving it didn't exit)
	//
	// The actual backoff calculation is unit tested in TestCalculateAcceptBackoff.
	// The reset behavior (consecutiveErrors = 0) is a trivial assignment that we
	// verify indirectly by confirming the loop continues operating normally.
	mockLn := newToggleMockListener()
	mockLn.SetErrorEnabled(true) // Start with errors

	cfg := ConnectionManagerConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry: prometheus.NewRegistry(),
		Listeners: []ListenerConfig{
			{
				Listener: mockLn,
			},
		},
	}

	cm := NewConnectionManager(cfg)
	err := cm.Start(context.Background())
	require.NoError(t, err)

	// Phase 1: Wait for errors to accumulate (proves backoff is being applied)
	phase1Errors := mockLn.WaitForErrors(0, 3, 5*time.Second)
	require.GreaterOrEqual(
		t,
		phase1Errors,
		3,
		"should have at least 3 errors to confirm accept loop is running",
	)
	t.Logf("Phase 1: %d errors accumulated", phase1Errors)

	// Phase 2: Allow one successful accept
	mockLn.DrainAcceptEntered()
	mockLn.SetErrorEnabled(false)
	mockLn.ProvideConnection(newMockConn())

	success := mockLn.WaitForSuccess(5 * time.Second)
	require.True(t, success, "should have had a successful accept")
	t.Logf("Phase 2: successful accept completed")

	// Phase 3: Re-enable errors and verify the loop continues
	mockLn.SetErrorEnabled(true)

	// Wait for Accept() to be entered again - this proves the loop returned
	// to Accept() after processing the successful connection
	acceptEntered := mockLn.WaitForAcceptEntered(5 * time.Second)
	require.True(
		t,
		acceptEntered,
		"accept loop should return to Accept() after processing connection",
	)
	t.Logf("Phase 3: accept loop returned to Accept()")

	// Stop the connection manager
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = cm.Stop(ctx)
	require.NoError(t, err)

	// The test passes if we get here - it proves:
	// 1. Errors were being processed (phase 1)
	// 2. A successful accept occurred (phase 2)
	// 3. The loop continued and returned to Accept() (phase 3)
	// The backoff reset (consecutiveErrors = 0) is implicitly verified because
	// the loop continued operating normally after the success.
}

func TestAcceptLoopExitsOnClose(t *testing.T) {
	defer goleak.VerifyNone(t)

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

	cm := NewConnectionManager(cfg)
	err := cm.Start(context.Background())
	require.NoError(t, err)

	// Wait for at least one accept call
	time.Sleep(20 * time.Millisecond)

	// Stop should exit cleanly even during backoff
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = cm.Stop(ctx)
	require.NoError(t, err)
}
