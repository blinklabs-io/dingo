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
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const socketIdleTimeout = 2 * time.Minute

// handshakeTimeout bounds the unauthenticated period for an accepted
// connection. It is intentionally shorter than the socket idle timeout: the
// listener must not wait for a peer that never completes the handshake.
const handshakeTimeout = 10 * time.Second

// trustedNtCIdleTimeout bounds idle time and stalled mux segment transfers on
// trusted local NtC connections. Active protocol callbacks clear this deadline
// so supported long-running requests remain unbounded.
const trustedNtCIdleTimeout = 2 * time.Minute

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

	readParser  muxSegmentParser
	writeParser muxSegmentParser
	activity    atomic.Pointer[trustedNtCActivity]
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

func (c *handshakeDeadlineConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n == 0 {
		return n, err
	}
	for range c.readParser.consume(p[:n]) {
		if activity := c.activity.Load(); activity != nil {
			activity.readProgress()
		}
	}
	return n, err
}

func (c *handshakeDeadlineConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n == 0 {
		return n, err
	}
	for _, protocolID := range c.writeParser.consume(p[:n]) {
		if activity := c.activity.Load(); activity != nil {
			activity.responseProgress(protocolID)
		}
	}
	return n, err
}

func (c *handshakeDeadlineConn) setReadDeadlineCap(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadline = deadline
	return c.Conn.SetReadDeadline(deadline)
}

func (c *handshakeDeadlineConn) attachActivity(
	activity *trustedNtCActivity,
) map[uint16]uint64 {
	c.readParser.mu.Lock()
	c.writeParser.mu.Lock()
	defer c.writeParser.mu.Unlock()
	defer c.readParser.mu.Unlock()
	preAttachWrites := c.writeParser.completedByProtocol
	c.writeParser.completedByProtocol = nil
	c.activity.Store(activity)
	return preAttachWrites
}

func (c *handshakeDeadlineConn) Unwrap() net.Conn { return c.Conn }

const muxSegmentHeaderSize = 8

type muxSegmentParser struct {
	mu                  sync.Mutex
	header              [muxSegmentHeaderSize]byte
	headerBytes         int
	payloadRemaining    int
	protocolID          uint16
	completedByProtocol map[uint16]uint64
}

func (p *muxSegmentParser) consume(data []byte) []uint16 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var completed []uint16
	for len(data) > 0 {
		if p.headerBytes < muxSegmentHeaderSize {
			n := copy(p.header[p.headerBytes:], data)
			p.headerBytes += n
			data = data[n:]
			if p.headerBytes < muxSegmentHeaderSize {
				break
			}
			p.protocolID = binary.BigEndian.Uint16(p.header[4:6]) & 0x7fff
			p.payloadRemaining = int(binary.BigEndian.Uint16(p.header[6:8]))
			if p.payloadRemaining == 0 {
				p.reset()
				continue
			}
		}
		n := min(len(data), p.payloadRemaining)
		p.payloadRemaining -= n
		data = data[n:]
		if p.payloadRemaining == 0 {
			protocolID := p.protocolID
			completed = append(completed, protocolID)
			if p.completedByProtocol == nil {
				p.completedByProtocol = make(map[uint16]uint64)
			}
			p.completedByProtocol[protocolID]++
			p.reset()
		}
	}
	return completed
}

func (p *muxSegmentParser) reset() {
	p.headerBytes = 0
	p.payloadRemaining = 0
	p.protocolID = 0
}

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

// withSocketDeadlines adds a write-idle deadline to TCP bearers and to Unix
// bearers after their unique remote-address wrapper is applied.
func withSocketDeadlines(conn net.Conn) net.Conn {
	switch conn.(type) {
	case *net.TCPConn, *UnixConn:
	default:
		return conn
	}
	return &deadlineConn{Conn: conn, timeout: socketIdleTimeout}
}
