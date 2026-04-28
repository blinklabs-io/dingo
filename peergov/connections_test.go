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
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"log/slog"
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

func TestCreateOutboundConnection_SuppressesRetryWhenReusableInboundSatisfiesValency(t *testing.T) {
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
		pg.createOutboundConnection(topologyPeer)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("createOutboundConnection should return when inbound valency is satisfied")
	}
	pg.mu.Lock()
	defer pg.mu.Unlock()
	if topologyPeer.Reconnecting {
		t.Fatal("reconnecting flag should be cleared after early suppression")
	}
	if topologyPeer.ReconnectCount != 0 {
		t.Fatalf("reconnect count changed unexpectedly: %d", topologyPeer.ReconnectCount)
	}
}
