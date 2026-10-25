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
	"io"
	"log/slog"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testDeadlineLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestWithSocketDeadlinesLeavesUnsupportedConnUnwrapped(t *testing.T) {
	t.Parallel()

	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	if got := withSocketDeadlines(left); got != left {
		t.Fatal("non-TCP connection should not be wrapped")
	}
}

func TestWithSocketDeadlinesWrapsUnix(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Windows NtC uses named pipes rather than Unix-domain sockets")
	}

	socketPath := unixTestSocketPath(t, unixTestTempDir(t), "deadline.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	defer listener.Close()

	client, err := net.Dial("unix", socketPath)
	require.NoError(t, err)
	defer client.Close()

	accepted, err := listener.Accept()
	require.NoError(t, err)
	defer accepted.Close()
	unixConn, err := NewUnixConn(accepted)
	require.NoError(t, err)

	wrapped, ok := withSocketDeadlines(unixConn).(*deadlineConn)
	require.True(t, ok, "Unix NtC connections must retain a write deadline")
	require.Equal(t, socketIdleTimeout, wrapped.timeout)
	require.Same(t, accepted, unwrapConn(wrapped))
	require.Equal(t, unixConn.RemoteAddr(), wrapped.RemoteAddr())
}

func TestDeadlineConnRefreshesWriteDeadline(t *testing.T) {
	t.Parallel()

	base := &recordingConn{}
	wrapped := &deadlineConn{Conn: base, timeout: socketIdleTimeout}
	before := time.Now()
	if _, err := wrapped.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if !base.writeDeadline.After(before) {
		t.Fatal("write deadline should be refreshed before the write")
	}
	// A second write refreshes again rather than reusing the first absolute
	// deadline, so a healthy long-lived session is never killed.
	first := base.writeDeadline
	if _, err := wrapped.Write([]byte("y")); err != nil {
		t.Fatal(err)
	}
	if base.writeDeadline.Before(first) {
		t.Fatal("write deadline should move forward on each write")
	}
}

// The muxer sets its own read deadline immediately before each segment read as
// slowloris protection. Refreshing a read deadline per Read call would override
// that protocol-managed deadline and let a peer dribbling bytes hold a segment
// read open forever, so the wrapper must not touch read deadlines.
func TestDeadlineConnLeavesReadDeadlineToTheMuxer(t *testing.T) {
	t.Parallel()

	base := &recordingConn{}
	wrapped := &deadlineConn{Conn: base, timeout: socketIdleTimeout}

	// Stand in for the muxer's per-segment deadline.
	muxerDeadline := time.Now().Add(90 * time.Second)
	if err := wrapped.SetReadDeadline(muxerDeadline); err != nil {
		t.Fatal(err)
	}
	if _, err := wrapped.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if !base.readDeadline.Equal(muxerDeadline) {
		t.Fatalf(
			"read deadline should still be the muxer's %v, got %v",
			muxerDeadline, base.readDeadline,
		)
	}
	if base.readDeadlineCalls != 1 {
		t.Fatalf(
			"expected only the muxer's SetReadDeadline call, got %d",
			base.readDeadlineCalls,
		)
	}
}

func TestHandshakeDeadlineConnCapsMuxerReadDeadline(t *testing.T) {
	t.Parallel()

	base := &recordingConn{}
	wrapped := withHandshakeDeadline(base)
	handshakeDeadline := time.Now().Add(time.Second)
	muxerDeadline := handshakeDeadline.Add(time.Minute)

	require.NoError(t, wrapped.SetDeadline(handshakeDeadline))
	require.NoError(t, wrapped.SetReadDeadline(muxerDeadline))
	if !base.readDeadline.Equal(handshakeDeadline) {
		t.Fatalf(
			"muxer read deadline should not extend handshake deadline: want %v, got %v",
			handshakeDeadline,
			base.readDeadline,
		)
	}

	// Once negotiation is complete, normal protocol-managed deadlines are
	// restored and the wrapper no longer caps the muxer's read deadline.
	require.NoError(t, wrapped.SetDeadline(time.Time{}))
	postHandshakeDeadline := time.Now().Add(time.Minute)
	require.NoError(t, wrapped.SetReadDeadline(postHandshakeDeadline))
	if !base.readDeadline.Equal(postHandshakeDeadline) {
		t.Fatalf(
			"read deadline should be restored after handshake: want %v, got %v",
			postHandshakeDeadline,
			base.readDeadline,
		)
	}
}

func TestTrustedNtCActivityClearsDeadlineUntilResponse(t *testing.T) {
	t.Parallel()

	base := &recordingConn{}
	wrapped := withHandshakeDeadline(base)
	activity := &trustedNtCActivity{
		writes:           make(map[uint16]uint64),
		pendingResponses: make(map[uint16]uint64),
	}
	require.NoError(t, activity.attach(wrapped, time.Minute, testDeadlineLogger()))
	require.False(t, base.readDeadline.IsZero())

	token := activity.begin(7)
	require.True(t, base.readDeadline.IsZero())
	require.NoError(t, wrapped.SetReadDeadline(time.Time{}))
	require.True(t, base.readDeadline.IsZero())

	token.end(true)
	require.True(t, base.readDeadline.IsZero())
	_, err := wrapped.Write(testMuxSegment(7, true, []byte{0x81, 0x03}))
	require.NoError(t, err)
	require.False(t, base.readDeadline.IsZero())
}

func TestTrustedNtCDeadlineRefreshesOnlyAfterCompleteSegment(t *testing.T) {
	t.Parallel()

	base := &recordingConn{}
	wrapped := withHandshakeDeadline(base)
	activity := &trustedNtCActivity{
		writes:           make(map[uint16]uint64),
		pendingResponses: make(map[uint16]uint64),
	}
	require.NoError(t, activity.attach(wrapped, time.Minute, testDeadlineLogger()))
	initialDeadline := base.readDeadline
	segment := testMuxSegment(7, false, []byte{0x81, 0x00})

	base.readData = segment[:4]
	_, err := wrapped.Read(make([]byte, 4))
	require.NoError(t, err)
	require.Equal(t, initialDeadline, base.readDeadline)

	base.readData = segment[4 : len(segment)-1]
	_, err = wrapped.Read(make([]byte, len(base.readData)))
	require.NoError(t, err)
	require.Equal(t, initialDeadline, base.readDeadline)

	base.readData = segment[len(segment)-1:]
	_, err = wrapped.Read(make([]byte, 1))
	require.NoError(t, err)
	require.False(t, base.readDeadline.Before(initialDeadline))
}

func testMuxSegment(protocolID uint16, response bool, payload []byte) []byte {
	if response {
		protocolID |= 0x8000
	}
	segment := make([]byte, muxSegmentHeaderSize+len(payload))
	binary.BigEndian.PutUint16(segment[4:6], protocolID)
	binary.BigEndian.PutUint16(segment[6:8], uint16(len(payload)))
	copy(segment[muxSegmentHeaderSize:], payload)
	return segment
}

// enableTCPLingerZero must reach the real socket through the deadline wrapper.
// Matching on the wrapper instead would make SO_LINGER 0 a silent no-op for
// accepted connections and reintroduce TIME_WAIT saturation of the reused
// outbound source port.
func TestEnableTCPLingerZeroThroughDeadlineWrapper(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		c, aErr := ln.Accept()
		if aErr != nil {
			accepted <- nil
			return
		}
		accepted <- c
	}()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server := <-accepted
	if server == nil {
		t.Fatal("accept failed")
	}
	defer server.Close()

	wrapped := withSocketDeadlines(server)
	if _, ok := wrapped.(*deadlineConn); !ok {
		t.Fatalf("TCP connection should be wrapped, got %T", wrapped)
	}
	if err := enableTCPLingerZero(wrapped); err != nil {
		t.Fatalf("SO_LINGER 0 must apply through the wrapper: %s", err)
	}
}

func TestUnwrapConnReachesBaseConn(t *testing.T) {
	t.Parallel()

	base := &recordingConn{}
	if got := unwrapConn(base); got != net.Conn(base) {
		t.Fatal("an unwrapped connection should be returned as-is")
	}
	once := &deadlineConn{Conn: base, timeout: socketIdleTimeout}
	if got := unwrapConn(once); got != net.Conn(base) {
		t.Fatal("one wrapper level should unwrap to the base connection")
	}
	twice := &deadlineConn{Conn: once, timeout: socketIdleTimeout}
	if got := unwrapConn(twice); got != net.Conn(base) {
		t.Fatal("nested wrappers should unwrap to the base connection")
	}
}

type recordingConn struct {
	readDeadline      time.Time
	writeDeadline     time.Time
	readDeadlineCalls int
	readData          []byte
}

func (c *recordingConn) Read(p []byte) (int, error) {
	if c.readData != nil {
		n := copy(p, c.readData)
		c.readData = c.readData[n:]
		return n, nil
	}
	return len(p), nil
}

func (c *recordingConn) Write(p []byte) (int, error) {
	return len(p), nil
}

func (*recordingConn) Close() error         { return nil }
func (*recordingConn) LocalAddr() net.Addr  { return nil }
func (*recordingConn) RemoteAddr() net.Addr { return nil }
func (c *recordingConn) SetDeadline(t time.Time) error {
	c.readDeadline, c.writeDeadline = t, t
	c.readDeadlineCalls++
	return nil
}
func (c *recordingConn) SetReadDeadline(t time.Time) error {
	c.readDeadline = t
	c.readDeadlineCalls++
	return nil
}

func (c *recordingConn) SetWriteDeadline(
	t time.Time,
) error {
	c.writeDeadline = t
	return nil
}
