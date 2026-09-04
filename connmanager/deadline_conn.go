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
	"net"
	"sync"
	"time"
)

const socketIdleTimeout = 2 * time.Minute

// handshakeTimeout bounds the unauthenticated period for an accepted
// connection. It is intentionally shorter than the socket idle timeout: the
// listener must not wait for a peer that never completes the handshake.
const handshakeTimeout = 10 * time.Second

// deadlineConn bounds how long a single socket write may block. Nothing below
// the Ouroboros muxer sets a write deadline, so a peer that stops reading can
// wedge a protocol goroutine in Write forever. Each Write refreshes the
// deadline just before the syscall, because one SetWriteDeadline call would be
// an absolute deadline and would eventually kill healthy long-lived sessions.
//
// Read deadlines are deliberately left alone. The muxer sets its own read
// deadline immediately before each segment read as slowloris protection
// (muxer.segmentReadTimeout), which bounds a whole segment. Refreshing the read
// deadline per Read call would override that protocol-managed deadline and let
// a peer dribbling bytes below the timeout hold a segment read open
// indefinitely, so the read half of a dead socket is upstream's to bound.
type deadlineConn struct {
	net.Conn
	timeout time.Duration
}

func (c *deadlineConn) Write(p []byte) (int, error) {
	if err := c.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}

// Unwrap returns the wrapped connection. Helpers that need the concrete socket
// type — SO_LINGER via enableTCPLingerZero, Unix peer credentials — must reach
// through this wrapper rather than silently no-op on the type assertion.
func (c *deadlineConn) Unwrap() net.Conn { return c.Conn }

// handshakeDeadlineConn keeps the listener's absolute handshake deadline in
// force while the muxer installs its per-segment read deadline. Without this
// cap, the muxer can replace a short handshake deadline with its much longer
// slowloris deadline before the first read completes.
type handshakeDeadlineConn struct {
	net.Conn
	mu       sync.Mutex
	deadline time.Time
}

func withHandshakeDeadline(conn net.Conn) *handshakeDeadlineConn {
	return &handshakeDeadlineConn{Conn: conn}
}

func (c *handshakeDeadlineConn) SetDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadline = deadline
	return c.Conn.SetDeadline(deadline)
}

func (c *handshakeDeadlineConn) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	handshakeDeadline := c.deadline
	if !handshakeDeadline.IsZero() &&
		(deadline.IsZero() || deadline.After(handshakeDeadline)) {
		deadline = handshakeDeadline
	}
	return c.Conn.SetReadDeadline(deadline)
}

func (c *handshakeDeadlineConn) Unwrap() net.Conn { return c.Conn }

// unwrapConn walks any chain of Unwrap-able wrappers down to the base
// connection.
func unwrapConn(conn net.Conn) net.Conn {
	for {
		u, ok := conn.(interface{ Unwrap() net.Conn })
		if !ok {
			return conn
		}
		inner := u.Unwrap()
		if inner == nil {
			return conn
		}
		conn = inner
	}
}

// withSocketDeadlines adds a write-idle deadline to TCP bearers. Unix bearers
// are left untouched: NtC clients are local, and the concrete *net.UnixConn is
// still needed for peer credentials.
func withSocketDeadlines(conn net.Conn) net.Conn {
	if _, ok := conn.(*net.TCPConn); !ok {
		return conn
	}
	return &deadlineConn{Conn: conn, timeout: socketIdleTimeout}
}
