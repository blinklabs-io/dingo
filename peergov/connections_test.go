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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

func TestIsExpectedConnectionCloseError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "eof",
			err:  io.EOF,
			want: true,
		},
		{
			name: "broken pipe",
			err:  errors.New("write tcp 1.2.3.4:1234: broken pipe"),
			want: true,
		},
		{
			name: "wrapped epipe",
			err:  fmt.Errorf("write failed: %w", syscall.EPIPE),
			want: true,
		},
		{
			name: "wrapped econnreset",
			err:  fmt.Errorf("read failed: %w", syscall.ECONNRESET),
			want: true,
		},
		{
			name: "wrapped econnaborted",
			err:  fmt.Errorf("accept failed: %w", syscall.ECONNABORTED),
			want: true,
		},
		{
			name: "net op error wrapped syscall",
			err: &net.OpError{
				Op:  "write",
				Net: "tcp",
				Err: fmt.Errorf("wrapped: %w", syscall.EPIPE),
			},
			want: true,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "unexpected",
			err:  errors.New("tls: bad certificate"),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isExpectedConnectionCloseError(tc.err)
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsConnectionCancellationError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "context canceled",
			err:  context.Canceled,
			want: true,
		},
		{
			name: "wrapped context canceled",
			err:  fmt.Errorf("wrapped: %w", context.Canceled),
			want: true,
		},
		{
			name: "net err closed",
			err:  net.ErrClosed,
			want: true,
		},
		{
			name: "wrapped net err closed",
			err:  fmt.Errorf("wrapped: %w", net.ErrClosed),
			want: true,
		},
		{
			name: "operation was canceled string",
			err:  errors.New("dial tcp: operation was canceled"),
			want: true,
		},
		{
			name: "syscall econnaborted",
			err:  syscall.ECONNABORTED,
			want: false,
		},
		{
			name: "io eof",
			err:  io.EOF,
			want: false,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "unexpected",
			err:  errors.New("tls: bad certificate"),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isConnectionCancellationError(tc.err)
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsExpectedNetworkDialError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "no such host",
			err:  errors.New("lookup relay.example: no such host"),
			want: true,
		},
		{
			name: "wrapped no route to host",
			err: fmt.Errorf(
				"dial failed: %w",
				errors.New("connect: no route to host"),
			),
			want: true,
		},
		{
			name: "io timeout",
			err:  errors.New("dial tcp: i/o timeout"),
			want: true,
		},
		{
			name: "version mismatch",
			err:  errors.New("version data mismatch"),
			want: true,
		},
		{
			name: "net op error wrapping no route",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: errors.New("connect: no route to host"),
			},
			want: true,
		},
		{
			name: "syscall econnaborted",
			err:  syscall.ECONNABORTED,
			want: false,
		},
		{
			name: "io eof",
			err:  io.EOF,
			want: false,
		},
		{
			// gouroboros closes the muxer with this error when a crossing
			// duplicate connection is pruned during the handshake (duplex
			// connection-manager dedup). It is benign, not a dial failure.
			name: "connection shutdown initiated eof (duplex dedup)",
			err: fmt.Errorf(
				"connection shutdown initiated: %w",
				io.EOF,
			),
			want: true,
		},
		{
			name: "connection shutdown initiated without eof",
			err:  errors.New("connection shutdown initiated: handshake failed"),
			want: false,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "unexpected",
			err:  errors.New("tls: bad certificate"),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isExpectedNetworkDialError(tc.err)
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsAddrInUseError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "eaddrnotavail",
			err:  syscall.EADDRNOTAVAIL,
			want: true,
		},
		{
			name: "wrapped eaddrinuse",
			err:  fmt.Errorf("dial failed: %w", syscall.EADDRINUSE),
			want: true,
		},
		{
			name: "string cannot assign requested address",
			err:  errors.New("dial tcp: cannot assign requested address"),
			want: true,
		},
		{
			name: "different dial error",
			err:  errors.New("dial tcp: connection refused"),
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isAddrInUseError(tc.err)
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHandleConnectionClosedEvent_StableOutboundResetsBackoff(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	connId := outboundTestConnId()
	peer := &Peer{
		Address:           "192.168.12.101:3003",
		NormalizedAddress: "192.168.12.101:3003",
		Source:            PeerSourceTopologyLocalRoot,
		State:             PeerStateWarm,
		Connection: &PeerConnection{
			Id:       connId,
			IsClient: true,
		},
		ConnectedAt: time.Now().
			Add(-minStableConnectionDuration - time.Second),
		ReconnectCount: 5,
		ReconnectDelay: 8 * time.Second,
		// Suppress reconnect goroutine; this test only checks close accounting.
		Reconnecting: true,
	}
	pg.mu.Lock()
	pg.peers = []*Peer{peer}
	pg.mu.Unlock()

	pg.handleConnectionClosedEvent(event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{
			ConnectionId: connId,
			Error: errors.New(
				"protocol error: chain-sync: timeout waiting on transition",
			),
		},
	))

	pg.mu.Lock()
	defer pg.mu.Unlock()
	if peer.Connection != nil {
		t.Fatal("connection should be cleared")
	}
	if peer.State != PeerStateCold {
		t.Fatalf("state = %s, want cold", peer.State)
	}
	if !peer.ConnectedAt.IsZero() {
		t.Fatalf("ConnectedAt should be reset, got %s", peer.ConnectedAt)
	}
	if peer.ReconnectCount != 0 {
		t.Fatalf("ReconnectCount = %d, want 0", peer.ReconnectCount)
	}
	if peer.ReconnectDelay != 0 {
		t.Fatalf("ReconnectDelay = %s, want 0", peer.ReconnectDelay)
	}
}

func TestHandleConnectionClosedEvent_ShortLivedOutboundAppliesBackoff(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	connId := outboundTestConnId()
	peer := &Peer{
		Address:           "192.168.12.101:3003",
		NormalizedAddress: "192.168.12.101:3003",
		Source:            PeerSourceTopologyLocalRoot,
		State:             PeerStateWarm,
		Connection: &PeerConnection{
			Id:       connId,
			IsClient: true,
		},
		ConnectedAt:    time.Now().Add(-minStableConnectionDuration / 2),
		ReconnectDelay: 2 * time.Second,
		// Suppress reconnect goroutine; this test only checks close accounting.
		Reconnecting: true,
	}
	pg.mu.Lock()
	pg.peers = []*Peer{peer}
	pg.mu.Unlock()

	pg.handleConnectionClosedEvent(event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{
			ConnectionId: connId,
			Error:        io.EOF,
		},
	))

	pg.mu.Lock()
	defer pg.mu.Unlock()
	if !peer.ConnectedAt.IsZero() {
		t.Fatalf("ConnectedAt should be reset, got %s", peer.ConnectedAt)
	}
	if peer.ReconnectDelay != 4*time.Second {
		t.Fatalf("ReconnectDelay = %s, want 4s", peer.ReconnectDelay)
	}
}

func TestHandleConnectionClosedEvent_NegativeOutboundDurationLogsAndClamps(
	t *testing.T,
) {
	var logBuf bytes.Buffer
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(&logBuf, nil)),
	})
	connId := outboundTestConnId()
	peer := &Peer{
		Address:           "192.168.12.101:3003",
		NormalizedAddress: "192.168.12.101:3003",
		Source:            PeerSourceTopologyLocalRoot,
		State:             PeerStateWarm,
		Connection: &PeerConnection{
			Id:       connId,
			IsClient: true,
		},
		ConnectedAt: time.Now().Add(time.Second),
		// Suppress reconnect goroutine; this test only checks close accounting.
		Reconnecting: true,
	}
	pg.mu.Lock()
	pg.peers = []*Peer{peer}
	pg.mu.Unlock()

	pg.handleConnectionClosedEvent(event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{
			ConnectionId: connId,
			Error:        io.EOF,
		},
	))

	pg.mu.Lock()
	defer pg.mu.Unlock()
	if !peer.ConnectedAt.IsZero() {
		t.Fatalf("ConnectedAt should be reset, got %s", peer.ConnectedAt)
	}
	if peer.ReconnectDelay != initialReconnectDelay {
		t.Fatalf(
			"ReconnectDelay = %s, want %s",
			peer.ReconnectDelay,
			initialReconnectDelay,
		)
	}
	if !strings.Contains(
		logBuf.String(),
		"connection close timestamp predates connection start, clamping duration",
	) {
		t.Fatalf("expected negative duration log, got %s", logBuf.String())
	}
}

func TestCreateOutboundConnection_SuppressesRetryWhenReusableInboundSatisfiesValency(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	pg.stopCh = make(chan struct{})
	topologyPeer := &Peer{
		Address:           "44.0.0.10:3001",
		NormalizedAddress: "44.0.0.10:3001",
		Source:            PeerSourceTopologyLocalRoot,
		State:             PeerStateCold,
		GroupID:           "local-root-0",
		Valency:           1,
	}
	reusableInbound := &Peer{
		Address:           "44.0.0.11:3001",
		NormalizedAddress: "44.0.0.11:3001",
		Source:            PeerSourceTopologyLocalRoot,
		State:             PeerStateHot,
		GroupID:           "local-root-0",
		Valency:           1,
		Connection:        &PeerConnection{IsClient: true},
		InboundDuplex:     true,
	}
	pg.mu.Lock()
	pg.peers = []*Peer{topologyPeer, reusableInbound}
	pg.mu.Unlock()

	done := make(chan struct{})
	go func() {
		pg.createOutboundConnection(topologyPeer, false)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal(
			"createOutboundConnection should return when inbound valency is satisfied",
		)
	}
	pg.mu.Lock()
	defer pg.mu.Unlock()
	if topologyPeer.Reconnecting {
		t.Fatal("reconnecting flag should be cleared after early suppression")
	}
	if topologyPeer.ReconnectCount != 0 {
		t.Fatalf(
			"reconnect count changed unexpectedly: %d",
			topologyPeer.ReconnectCount,
		)
	}
}

func TestCreateOutboundConnection_ReturnsWhenGovernorStopped(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	peer := &Peer{
		Address:           "127.0.0.1:1",
		NormalizedAddress: "127.0.0.1:1",
		Source:            PeerSourceTopologyLocalRoot,
		State:             PeerStateCold,
		ReconnectDelay:    time.Nanosecond,
	}
	pg.mu.Lock()
	pg.peers = []*Peer{peer}
	// A nil stopCh means the governor has not started or has already stopped.
	pg.stopCh = nil
	pg.mu.Unlock()

	pg.createOutboundConnection(peer, false)

	pg.mu.Lock()
	defer pg.mu.Unlock()
	if peer.Reconnecting {
		t.Fatal(
			"reconnecting flag should remain clear when governor is stopped",
		)
	}
	if peer.ReconnectCount != 0 {
		t.Fatalf(
			"reconnect count changed unexpectedly: %d",
			peer.ReconnectCount,
		)
	}
	if peer.ReconnectDelay != time.Nanosecond {
		t.Fatalf(
			"reconnect delay changed unexpectedly: %s",
			peer.ReconnectDelay,
		)
	}
}

// TestSpawnOutboundConnectionLockedReservesBeforeScheduling verifies duplicate
// close events cannot schedule parallel reconnect workers for the same peer.
func TestSpawnOutboundConnectionLockedReservesBeforeScheduling(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	peer := &Peer{
		Address:           "127.0.0.1:1",
		NormalizedAddress: "127.0.0.1:1",
		Source:            PeerSourceTopologyLocalRoot,
		State:             PeerStateCold,
	}
	stopCh := make(chan struct{})
	close(stopCh)

	pg.mu.Lock()
	pg.peers = []*Peer{peer}
	pg.stopCh = stopCh
	pg.spawnOutboundConnectionLocked(peer)
	if !peer.Reconnecting {
		pg.mu.Unlock()
		t.Fatal("first spawn should reserve the peer before scheduling")
	}
	pg.spawnOutboundConnectionLocked(peer)
	pg.mu.Unlock()

	pg.wg.Wait()
	pg.mu.Lock()
	defer pg.mu.Unlock()
	if peer.Reconnecting {
		t.Fatal("reconnect reservation should clear when the worker exits")
	}
}

// TestStop_WaitsForInFlightOutboundDial reproduces a dial launched via
// startOutboundConnections (the same path Start uses) that is still in
// flight when Stop is called, and asserts Stop's p.wg.Wait() actually
// blocks until that dial goroutine exits rather than returning while it is
// still running.
//
// This matters beyond a clean shutdown: the live database restore/truncate
// quiesce path (node_lifecycle.go's quiesceForLiveLifecycleOp) calls
// PeerGovernor.Stop(context.Background()) expecting every background goroutine -- including
// in-flight outbound dials -- to have exited before it tears down and
// replaces the node's ConnectionManager. A dial goroutine not tracked by
// p.wg could finish its handshake after Stop returns and attach to (or
// publish events against) a connection manager that no longer belongs to
// this PeerGovernor incarnation.
//
// The fake TCP server below accepts the dial's TCP connection but never
// writes a handshake response, so createOutboundConnection blocks inside
// ouroboros.NewConnection's handshake -- a real "dial in flight" state --
// until the test closes the server-side socket.
func TestStop_WaitsForInFlightOutboundDial(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start fake server: %v", err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			accepted <- conn
		}
	}()

	connMgr := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			// A valid network magic is required for the client to proceed
			// past local validation into the actual on-wire handshake, which
			// is what leaves the dial blocked (in flight) against a fake
			// server that never answers.
			OutboundConnOpts: []ouroboros.ConnectionOptionFunc{
				ouroboros.WithNetworkMagic(764824073),
			},
		},
	)
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		ConnManager: connMgr,
	})
	peer := &Peer{
		Address:           ln.Addr().String(),
		NormalizedAddress: ln.Addr().String(),
		Source:            PeerSourceTopologyLocalRoot,
		State:             PeerStateCold,
	}
	pg.mu.Lock()
	pg.peers = []*Peer{peer}
	pg.stopCh = make(chan struct{})
	pg.ctx = context.Background()
	pg.mu.Unlock()

	// Launch the outbound dial exactly as Start does.
	pg.startOutboundConnections()

	// Wait for the dial to reach the fake server. At that point the
	// tracked goroutine is blocked inside the ouroboros handshake --
	// genuinely "in flight" -- since the server never responds.
	var serverConn net.Conn
	select {
	case serverConn = <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("outbound dial never reached the fake server")
	}
	defer serverConn.Close()

	stopped := make(chan struct{})
	go func() {
		_ = pg.Stop(context.Background())
		close(stopped)
	}()

	// Stop must not return while the dial goroutine is still blocked in
	// the handshake.
	select {
	case <-stopped:
		t.Fatal("Stop returned while an outbound dial was still in flight")
	case <-time.After(300 * time.Millisecond):
	}

	// Unblock the in-flight dial: closing the server side fails the
	// handshake, and createOutboundConnection's loop observes the
	// already-closed stopCh and returns.
	serverConn.Close()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the in-flight dial finished")
	}
}

func outboundTestConnId() ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.ParseIP("192.168.12.201"),
			Port: 3005,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.ParseIP("192.168.12.101"),
			Port: 3003,
		},
	}
}

// Repeated short-lived sessions must keep escalating the reconnect delay
// even though the reconnect goroutine consumes and zeroes ReconnectDelay
// before each redial. Without escalation a peer that accepts connections
// but is rejected ~600ms later (e.g. its chain fails the Mithril trust
// boundary check) is redialed every ~2s forever.
func TestHandleConnectionClosedEvent_ShortLivedBackoffEscalatesAfterDelayConsumed(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	connId := outboundTestConnId()
	peer := &Peer{
		Address:           "192.168.12.101:3003",
		NormalizedAddress: "192.168.12.101:3003",
		Source:            PeerSourceTopologyLocalRoot,
		// Suppress reconnect goroutine; this test only checks close accounting.
		Reconnecting: true,
	}
	// The short-lived backoff escalates only while the hot pool is healthy; the
	// issue #2765 cap engages when hot peers <= criticalHotPeerThreshold. Add
	// hot fillers so this test exercises the escalation path rather than the
	// critically-low cap (which has its own test below).
	pg.mu.Lock()
	pg.peers = []*Peer{
		peer,
		{Address: "10.0.0.1:3001", State: PeerStateHot},
		{Address: "10.0.0.2:3001", State: PeerStateHot},
		{Address: "10.0.0.3:3001", State: PeerStateHot},
	}
	pg.mu.Unlock()

	wantDelays := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
		64 * time.Second,
		128 * time.Second,
		128 * time.Second, // capped at maxReconnectDelay
	}
	for i, want := range wantDelays {
		// Simulate the production cycle: the reconnect goroutine consumed
		// and zeroed the stored delay, redialed successfully, and the new
		// session lasted well under minStableConnectionDuration.
		pg.mu.Lock()
		peer.ReconnectDelay = 0
		peer.Connection = &PeerConnection{
			Id:       connId,
			IsClient: true,
		}
		peer.State = PeerStateWarm
		peer.ConnectedAt = time.Now().Add(-600 * time.Millisecond)
		pg.mu.Unlock()

		pg.handleConnectionClosedEvent(event.NewEvent(
			connmanager.ConnectionClosedEventType,
			connmanager.ConnectionClosedEvent{
				ConnectionId: connId,
			},
		))

		pg.mu.Lock()
		got := peer.ReconnectDelay
		pg.mu.Unlock()
		if got != want {
			t.Fatalf(
				"close %d: ReconnectDelay = %s, want %s",
				i+1, got, want,
			)
		}
	}
}

// TestHandleConnectionClosedEvent_CriticalHotPeersCapsBackoff verifies the
// issue #2765 fix: when the hot pool is at or below criticalHotPeerThreshold,
// the short-lived reconnect backoff is capped at emergencyReconnectDelay rather
// than escalating toward maxReconnectDelay, so the node keeps reconnecting to
// its known peers instead of collapsing to a single stalled upstream on a
// network of few, flaky relays.
func TestHandleConnectionClosedEvent_CriticalHotPeersCapsBackoff(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	connId := outboundTestConnId()
	peer := &Peer{
		Address:           "192.168.12.101:3003",
		NormalizedAddress: "192.168.12.101:3003",
		Source:            PeerSourceTopologyLocalRoot,
		// Suppress reconnect goroutine; this test only checks close accounting.
		Reconnecting: true,
	}
	// No hot peers: the pool is critically low, so the cap must engage.
	pg.mu.Lock()
	pg.peers = []*Peer{peer}
	pg.mu.Unlock()

	// Unbounded escalation would reach 1,2,4,8,16,32s; the cap holds the delay
	// at emergencyReconnectDelay once it would exceed that, and never above it.
	for i := range 6 {
		pg.mu.Lock()
		peer.ReconnectDelay = 0
		peer.Connection = &PeerConnection{
			Id:       connId,
			IsClient: true,
		}
		peer.State = PeerStateWarm
		peer.ConnectedAt = time.Now().Add(-600 * time.Millisecond)
		pg.mu.Unlock()

		pg.handleConnectionClosedEvent(event.NewEvent(
			connmanager.ConnectionClosedEventType,
			connmanager.ConnectionClosedEvent{
				ConnectionId: connId,
			},
		))

		pg.mu.Lock()
		got := peer.ReconnectDelay
		pg.mu.Unlock()
		if got > emergencyReconnectDelay {
			t.Fatalf(
				"close %d: ReconnectDelay = %s, want <= %s (critically-low cap)",
				i+1,
				got,
				emergencyReconnectDelay,
			)
		}
	}
	// After enough short-lived closes to exceed the cap, the delay must sit at
	// the emergency cap, not the escalated value it would otherwise reach.
	pg.mu.Lock()
	got := peer.ReconnectDelay
	pg.mu.Unlock()
	if got != emergencyReconnectDelay {
		t.Fatalf(
			"final ReconnectDelay = %s, want %s (emergency cap)",
			got, emergencyReconnectDelay,
		)
	}
}
