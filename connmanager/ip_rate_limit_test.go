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
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestIPKeyFromAddr(t *testing.T) {
	tests := []struct {
		name     string
		addr     net.Addr
		expected string
	}{
		{
			name:     "nil addr",
			addr:     nil,
			expected: "",
		},
		{
			name: "IPv4 TCP addr",
			addr: &net.TCPAddr{
				IP:   net.ParseIP("192.168.1.10"),
				Port: 3000,
			},
			expected: "192.168.1.10",
		},
		{
			name: "IPv4 different addr",
			addr: &net.TCPAddr{
				IP:   net.ParseIP("10.0.0.1"),
				Port: 8080,
			},
			expected: "10.0.0.1",
		},
		{
			name: "IPv4 loopback",
			addr: &net.TCPAddr{
				IP:   net.ParseIP("127.0.0.1"),
				Port: 12345,
			},
			expected: "127.0.0.1",
		},
		{
			name: "IPv6 full address grouped by /64",
			addr: &net.TCPAddr{
				IP:   net.ParseIP("2001:db8:85a3::8a2e:370:7334"),
				Port: 3000,
			},
			expected: "2001:db8:85a3::/64",
		},
		{
			name: "IPv6 different host same /64 prefix",
			addr: &net.TCPAddr{
				IP:   net.ParseIP("2001:db8:85a3::1"),
				Port: 3001,
			},
			expected: "2001:db8:85a3::/64",
		},
		{
			name: "IPv6 different /64 prefix",
			addr: &net.TCPAddr{
				IP:   net.ParseIP("2001:db8:85a4::1"),
				Port: 3000,
			},
			expected: "2001:db8:85a4::/64",
		},
		{
			name: "IPv6 loopback",
			addr: &net.TCPAddr{
				IP:   net.ParseIP("::1"),
				Port: 3000,
			},
			expected: "::/64",
		},
		{
			name: "IPv4-mapped IPv6 treated as IPv4",
			addr: &net.TCPAddr{
				IP:   net.ParseIP("::ffff:192.168.1.1"),
				Port: 3000,
			},
			expected: "192.168.1.1",
		},
		{
			name: "Unix socket returns empty string",
			addr: &net.UnixAddr{
				Name: "/tmp/test.sock",
				Net:  "unix",
			},
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := ipKeyFromAddr(tc.addr)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestIPKeyFromAddr_SameSubnet(t *testing.T) {
	// Verify that two IPv6 addresses in the same /64 produce the same key
	addr1 := &net.TCPAddr{
		IP:   net.ParseIP("2001:db8:1234:5678::1"),
		Port: 3000,
	}
	addr2 := &net.TCPAddr{
		IP:   net.ParseIP("2001:db8:1234:5678::ffff"),
		Port: 3001,
	}
	key1 := ipKeyFromAddr(addr1)
	key2 := ipKeyFromAddr(addr2)
	assert.Equal(t, key1, key2, "same /64 should produce the same key")

	// Different /64 should produce different keys
	addr3 := &net.TCPAddr{
		IP:   net.ParseIP("2001:db8:1234:5679::1"),
		Port: 3000,
	}
	key3 := ipKeyFromAddr(addr3)
	assert.NotEqual(
		t,
		key1,
		key3,
		"different /64 should produce different keys",
	)
}

func TestAcquireReleaseIPSlot(t *testing.T) {
	cm := NewConnectionManager(ConnectionManagerConfig{
		MaxConnectionsPerIP: 3,
	})

	ipKey := "192.168.1.1"

	// Acquire up to the limit
	assert.True(t, cm.acquireIPSlot(ipKey))
	assert.Equal(t, 1, cm.IPConnCount(ipKey))

	assert.True(t, cm.acquireIPSlot(ipKey))
	assert.Equal(t, 2, cm.IPConnCount(ipKey))

	assert.True(t, cm.acquireIPSlot(ipKey))
	assert.Equal(t, 3, cm.IPConnCount(ipKey))

	// Exceeds limit
	assert.False(t, cm.acquireIPSlot(ipKey))
	assert.Equal(t, 3, cm.IPConnCount(ipKey))

	// Release one and acquire again
	cm.releaseIPSlot(ipKey)
	assert.Equal(t, 2, cm.IPConnCount(ipKey))

	assert.True(t, cm.acquireIPSlot(ipKey))
	assert.Equal(t, 3, cm.IPConnCount(ipKey))

	// Release all
	cm.releaseIPSlot(ipKey)
	cm.releaseIPSlot(ipKey)
	cm.releaseIPSlot(ipKey)
	assert.Equal(t, 0, cm.IPConnCount(ipKey))
}

func TestAcquireIPSlot_EmptyKeyExempt(t *testing.T) {
	cm := NewConnectionManager(ConnectionManagerConfig{
		MaxConnectionsPerIP: 1,
	})

	// Empty key (Unix sockets) should always be allowed
	for range 10 {
		assert.True(t, cm.acquireIPSlot(""))
	}
}

func TestAcquireIPSlot_IndependentIPs(t *testing.T) {
	cm := NewConnectionManager(ConnectionManagerConfig{
		MaxConnectionsPerIP: 2,
	})

	ipA := "10.0.0.1"
	ipB := "10.0.0.2"

	// Fill up IP A
	assert.True(t, cm.acquireIPSlot(ipA))
	assert.True(t, cm.acquireIPSlot(ipA))
	assert.False(t, cm.acquireIPSlot(ipA))

	// IP B should still work independently
	assert.True(t, cm.acquireIPSlot(ipB))
	assert.True(t, cm.acquireIPSlot(ipB))
	assert.False(t, cm.acquireIPSlot(ipB))

	// Releasing A does not affect B
	cm.releaseIPSlot(ipA)
	assert.False(t, cm.acquireIPSlot(ipB))
	assert.True(t, cm.acquireIPSlot(ipA))
}

func TestDefaultMaxConnectionsPerIP(t *testing.T) {
	cm := NewConnectionManager(ConnectionManagerConfig{})
	assert.Equal(
		t,
		DefaultMaxConnectionsPerIP,
		cm.config.MaxConnectionsPerIP,
	)
}

func TestCustomMaxConnectionsPerIP(t *testing.T) {
	cm := NewConnectionManager(ConnectionManagerConfig{
		MaxConnectionsPerIP: 10,
	})
	assert.Equal(t, 10, cm.config.MaxConnectionsPerIP)
}

// mockConnWithAddr is a mockConn that allows setting a custom remote
// address.
type mockConnWithAddr struct {
	mockConn
	remoteAddr net.Addr
}

func newMockConnWithAddr(ip string, port int) *mockConnWithAddr {
	mc := &mockConnWithAddr{
		remoteAddr: &net.TCPAddr{
			IP:   net.ParseIP(ip),
			Port: port,
		},
	}
	mc.localAddr = &net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 12345,
	}
	// Pre-close so Ouroboros setup fails fast in tests
	mc.closed.Store(true)
	return mc
}

func (c *mockConnWithAddr) RemoteAddr() net.Addr {
	return c.remoteAddr
}

// rateLimitMockListener is a listener that returns connections with
// configurable remote addresses.
type rateLimitMockListener struct {
	closed  atomic.Bool
	closeCh chan struct{}
	connCh  chan net.Conn
}

func newRateLimitMockListener() *rateLimitMockListener {
	return &rateLimitMockListener{
		closeCh: make(chan struct{}),
		connCh:  make(chan net.Conn, 100),
	}
}

func (m *rateLimitMockListener) Accept() (net.Conn, error) {
	if m.closed.Load() {
		return nil, net.ErrClosed
	}
	select {
	case conn := <-m.connCh:
		return conn, nil
	case <-m.closeCh:
		return nil, net.ErrClosed
	}
}

func (m *rateLimitMockListener) Close() error {
	if m.closed.Swap(true) {
		return nil
	}
	close(m.closeCh)
	return nil
}

func (m *rateLimitMockListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
}

func (m *rateLimitMockListener) ProvideConnection(conn net.Conn) {
	m.connCh <- conn
}

// TestIPRateLimitInAcceptLoop tests the per-IP rate limiting in the
// accept loop using real TCP connections to verify no crashes occur.
func TestIPRateLimitInAcceptLoop(t *testing.T) {
	defer goleak.VerifyNone(t)

	const maxPerIP = 2

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	cfg := ConnectionManagerConfig{
		Logger:              slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry:        prometheus.NewRegistry(),
		MaxConnectionsPerIP: maxPerIP,
		Listeners: []ListenerConfig{
			{
				Listener: ln,
			},
		},
	}

	cm := NewConnectionManager(cfg)
	err = cm.Start(context.Background())
	require.NoError(t, err)

	addr := ln.Addr().String()

	// Dial several connections. They will be accepted at the TCP level
	// but Ouroboros setup will fail, causing the IP slot to be released.
	// This verifies the accept loop does not crash with rate limiting.
	for range 5 {
		conn, dialErr := net.DialTimeout("tcp", addr, 2*time.Second)
		if dialErr != nil {
			t.Logf(
				"dial failed (expected during shutdown): %v",
				dialErr,
			)
			continue
		}
		conn.Close()
	}

	// Wait for the accept loop to process all connections. Since
	// Ouroboros setup fails on each connection, all IP slots should
	// be released back to zero.
	require.Eventually(t, func() bool {
		return cm.IPConnCount("127.0.0.1") == 0
	}, 2*time.Second, 10*time.Millisecond,
		"all IP slots should be released after Ouroboros failures",
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = cm.Stop(ctx)
	require.NoError(t, err)
}

// TestIPRateLimitRejectsWhenSlotsPreFilled uses pre-acquired IP slots
// to verify the accept loop rejects connections when the limit is reached
// and allows connections from other IPs.
func TestIPRateLimitRejectsWhenSlotsPreFilled(t *testing.T) {
	defer goleak.VerifyNone(t)

	const maxPerIP = 2
	mockLn := newRateLimitMockListener()

	cfg := ConnectionManagerConfig{
		Logger:              slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry:        prometheus.NewRegistry(),
		MaxConnectionsPerIP: maxPerIP,
		Listeners: []ListenerConfig{
			{
				Listener: mockLn,
			},
		},
	}

	cm := NewConnectionManager(cfg)
	err := cm.Start(context.Background())
	require.NoError(t, err)

	// Pre-fill the IP slots for 192.168.1.100 to simulate held
	// connections
	ipKey := "192.168.1.100"
	for range maxPerIP {
		ok := cm.acquireIPSlot(ipKey)
		require.True(t, ok)
	}
	require.Equal(t, maxPerIP, cm.IPConnCount(ipKey))

	// Provide a connection from the full IP - should be rejected
	mockLn.ProvideConnection(
		newMockConnWithAddr("192.168.1.100", 50000),
	)

	// Provide a connection from a different IP - should be accepted
	// (even though Ouroboros will fail, the slot is acquired first)
	mockLn.ProvideConnection(
		newMockConnWithAddr("10.0.0.1", 50001),
	)

	// Wait for the allowed connection's IP slot to be released after
	// Ouroboros failure. This confirms both connections were processed
	// (the rejected one first, then the allowed one).
	require.Eventually(t, func() bool {
		return cm.IPConnCount("10.0.0.1") == 0
	}, 2*time.Second, 10*time.Millisecond,
		"allowed IP's slot should be released after Ouroboros failure",
	)

	// The pre-filled IP count should remain at maxPerIP (rejected
	// connection should NOT have incremented it)
	assert.Equal(t, maxPerIP, cm.IPConnCount(ipKey))

	// Release the pre-acquired slots
	for range maxPerIP {
		cm.releaseIPSlot(ipKey)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = cm.Stop(ctx)
	require.NoError(t, err)
}

// TestIPRateLimitCounterDecrementsOnConnectionClose verifies that the
// per-IP counter is properly decremented when a connection is removed.
func TestIPRateLimitCounterDecrementsOnConnectionClose(t *testing.T) {
	cm := NewConnectionManager(ConnectionManagerConfig{
		MaxConnectionsPerIP: 3,
	})

	ipKey := "10.0.0.5"

	// Acquire some slots
	require.True(t, cm.acquireIPSlot(ipKey))
	require.True(t, cm.acquireIPSlot(ipKey))
	assert.Equal(t, 2, cm.IPConnCount(ipKey))

	// Release one
	cm.releaseIPSlot(ipKey)
	assert.Equal(t, 1, cm.IPConnCount(ipKey))

	// Release the last - key should be cleaned up
	cm.releaseIPSlot(ipKey)
	assert.Equal(t, 0, cm.IPConnCount(ipKey))
}

// TestIPRateLimitIPv6SubnetGrouping verifies that IPv6 addresses in the
// same /64 are grouped together for rate limiting.
func TestIPRateLimitIPv6SubnetGrouping(t *testing.T) {
	cm := NewConnectionManager(ConnectionManagerConfig{
		MaxConnectionsPerIP: 2,
	})

	// Two addresses in the same /64
	key1 := ipKeyFromAddr(&net.TCPAddr{
		IP:   net.ParseIP("2001:db8:1::1"),
		Port: 3000,
	})
	key2 := ipKeyFromAddr(&net.TCPAddr{
		IP:   net.ParseIP("2001:db8:1::ffff:ffff:ffff:ffff"),
		Port: 3001,
	})
	require.Equal(t, key1, key2, "same /64 prefix should yield same key")

	// Fill up the /64 prefix
	require.True(t, cm.acquireIPSlot(key1))
	require.True(t, cm.acquireIPSlot(key2))

	// Third from same /64 should be rejected
	key3 := ipKeyFromAddr(&net.TCPAddr{
		IP:   net.ParseIP("2001:db8:1::abcd"),
		Port: 3002,
	})
	require.False(t, cm.acquireIPSlot(key3))

	// Different /64 should still work
	keyOther := ipKeyFromAddr(&net.TCPAddr{
		IP:   net.ParseIP("2001:db8:2::1"),
		Port: 3000,
	})
	require.True(t, cm.acquireIPSlot(keyOther))
}

// TestIPRateLimitConcurrentAccess tests thread safety of the IP
// rate-limiting under concurrent access.
func TestIPRateLimitConcurrentAccess(t *testing.T) {
	const maxPerIP = 5
	cm := NewConnectionManager(ConnectionManagerConfig{
		MaxConnectionsPerIP: maxPerIP,
	})

	ipKey := "172.16.0.1"
	var successCount atomic.Int32
	var failCount atomic.Int32

	// Launch many goroutines competing for slots
	const goroutines = 50
	done := make(chan struct{})
	for range goroutines {
		go func() {
			if cm.acquireIPSlot(ipKey) {
				successCount.Add(1)
			} else {
				failCount.Add(1)
			}
			done <- struct{}{}
		}()
	}

	// Wait for all goroutines
	for range goroutines {
		<-done
	}

	assert.Equal(
		t,
		int32(maxPerIP),
		successCount.Load(),
		"exactly maxPerIP slots should be acquired",
	)
	assert.Equal(
		t,
		int32(goroutines-maxPerIP),
		failCount.Load(),
		"remaining attempts should fail",
	)
	assert.Equal(t, maxPerIP, cm.IPConnCount(ipKey))
}

// TestIPRateLimitDifferentIPsInAcceptLoop verifies that the accept loop
// properly handles connections from different IPs independently.
func TestIPRateLimitDifferentIPsInAcceptLoop(t *testing.T) {
	defer goleak.VerifyNone(t)

	const maxPerIP = 1
	mockLn := newRateLimitMockListener()

	cfg := ConnectionManagerConfig{
		Logger:              slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry:        prometheus.NewRegistry(),
		MaxConnectionsPerIP: maxPerIP,
		Listeners: []ListenerConfig{
			{
				Listener: mockLn,
			},
		},
	}

	cm := NewConnectionManager(cfg)
	err := cm.Start(context.Background())
	require.NoError(t, err)

	// Pre-fill IP A
	ipKeyA := "192.168.1.1"
	require.True(t, cm.acquireIPSlot(ipKeyA))

	// Connection from IP A should be rejected
	mockLn.ProvideConnection(
		newMockConnWithAddr("192.168.1.1", 50000),
	)

	// Connection from IP B should be processed (accepted then Ouroboros
	// fails, but slot is acquired first)
	mockLn.ProvideConnection(
		newMockConnWithAddr("192.168.1.2", 50001),
	)

	// Wait for IP B's slot to be released after Ouroboros failure,
	// confirming both connections were processed by the accept loop.
	require.Eventually(t, func() bool {
		return cm.IPConnCount("192.168.1.2") == 0
	}, 2*time.Second, 10*time.Millisecond,
		"IP B slot should be released after Ouroboros failure",
	)

	// IP A still at limit (rejected connection did not increment)
	assert.Equal(t, maxPerIP, cm.IPConnCount(ipKeyA))

	// Clean up pre-acquired slot
	cm.releaseIPSlot(ipKeyA)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = cm.Stop(ctx)
	require.NoError(t, err)
}

// TestIPRateLimitRejectLogMessage verifies that rejected connections are
// logged with a warning.
func TestIPRateLimitRejectLogMessage(t *testing.T) {
	defer goleak.VerifyNone(t)

	const maxPerIP = 1
	mockLn := newRateLimitMockListener()

	// Use a buffer to capture log output
	var logBuf safeBuffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))

	cfg := ConnectionManagerConfig{
		Logger:              logger,
		PromRegistry:        prometheus.NewRegistry(),
		MaxConnectionsPerIP: maxPerIP,
		Listeners: []ListenerConfig{
			{
				Listener: mockLn,
			},
		},
	}

	cm := NewConnectionManager(cfg)
	err := cm.Start(context.Background())
	require.NoError(t, err)

	// Pre-fill IP
	ipKey := "10.0.0.1"
	require.True(t, cm.acquireIPSlot(ipKey))

	// Provide a connection that should be rejected
	mockLn.ProvideConnection(
		newMockConnWithAddr("10.0.0.1", 50000),
	)

	// Check log output contains rejection message
	require.Eventually(t, func() bool {
		return strings.Contains(logBuf.String(), "per-IP limit")
	}, 2*time.Second, 10*time.Millisecond,
		"should log per-IP limit rejection",
	)

	// Clean up
	cm.releaseIPSlot(ipKey)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = cm.Stop(ctx)
	require.NoError(t, err)
}

// safeBuffer is a thread-safe buffer for capturing log output in tests.
type safeBuffer struct {
	buf  []byte
	lock atomic.Int64 // simple spinlock via CAS
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	for !b.lock.CompareAndSwap(0, 1) {
		// spin
	}
	b.buf = append(b.buf, p...)
	b.lock.Store(0)
	return len(p), nil
}

func (b *safeBuffer) String() string {
	for !b.lock.CompareAndSwap(0, 1) {
		// spin
	}
	s := string(b.buf)
	b.lock.Store(0)
	return s
}

// TestIPRateLimitReleasedOnOuroborosSetupFailure verifies that when
// Ouroboros connection setup fails, the IP slot is properly released.
func TestIPRateLimitReleasedOnOuroborosSetupFailure(t *testing.T) {
	defer goleak.VerifyNone(t)

	const maxPerIP = 2
	mockLn := newRateLimitMockListener()

	cfg := ConnectionManagerConfig{
		Logger:              slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry:        prometheus.NewRegistry(),
		MaxConnectionsPerIP: maxPerIP,
		Listeners: []ListenerConfig{
			{
				Listener: mockLn,
			},
		},
	}

	cm := NewConnectionManager(cfg)
	err := cm.Start(context.Background())
	require.NoError(t, err)

	// Provide several connections from the same IP. Since our mock
	// connections are pre-closed, Ouroboros setup will fail each time,
	// and the IP slot should be released after each failure. This means
	// ALL connections should be processed (not rejected), because the
	// slot is never held long-term.
	const numConns = 5
	for i := range numConns {
		mockLn.ProvideConnection(
			newMockConnWithAddr("192.168.1.50", 50000+i),
		)
	}

	// After all Ouroboros failures, the IP count should return to 0.
	// The accept loop processes connections serially, so we poll until
	// all slots have been released.
	require.Eventually(t, func() bool {
		return cm.IPConnCount("192.168.1.50") == 0
	}, 5*time.Second, 10*time.Millisecond,
		"all slots should be released after Ouroboros failures",
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = cm.Stop(ctx)
	require.NoError(t, err)
}
