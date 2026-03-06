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
