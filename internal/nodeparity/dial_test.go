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

package nodeparity

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDial_CancelledContextUnblocksStalledHandshake covers a peer that
// accepts the TCP connection but never sends a byte: ouroboros.New performs
// the handshake synchronously with no context of its own, so without Dial
// closing the raw connection itself on cancellation, a caller would block
// until the peer eventually responds (in production, potentially forever).
// Verified adversarially: before Dial held onto the raw net.Conn and closed
// it on ctx cancellation, this same scenario blocked for 20+ seconds
// (reproduced against watchSession, which shares this exact pattern) rather
// than returning within the bound this test asserts.
func TestDial_CancelledContextUnblocksStalledHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	done := make(chan struct{})
	var dialErr error
	go func() {
		_, dialErr = Dial(ctx, listener.Addr().String(), 42)
		close(done)
	}()

	select {
	case conn := <-accepted:
		t.Cleanup(func() { _ = conn.Close() })
	case <-time.After(5 * time.Second):
		t.Fatal("server never observed the dial attempt")
	}

	cancel()

	select {
	case <-done:
		require.Error(
			t, dialErr,
			"a cancelled dial against a stalled peer must return an error, not a connection",
		)
	case <-time.After(5 * time.Second):
		t.Fatal(
			"Dial did not return promptly after ctx was cancelled against " +
				"a peer that accepted the connection and then never responded",
		)
	}
}

// TestDial_HandshakeStallIsBoundedByDialTimeout covers a peer that accepts
// the connection and then stalls the handshake, with a ctx that is never
// cancelled by the caller (matching a real Dial call, whose ctx normally
// only cancels on process shutdown). dialTimeout's own doc comment says it
// "bounds how long Dial waits for the initial connection and handshake" --
// so Dial must still return within roughly dialTimeout here, not hang
// until ctx eventually cancels. Regression test for a bug in Dial's own
// first fix: the raw-connection closer was registered against the
// long-lived ctx instead of dialCtx, so this exact scenario (a stalled
// handshake, no external cancellation) was left unbounded -- verified
// adversarially against that version, which did not return within
// dialTimeout+margin.
func TestDial_HandshakeStallIsBoundedByDialTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()

	done := make(chan struct{})
	var dialErr error
	go func() {
		_, dialErr = Dial(context.Background(), listener.Addr().String(), 42)
		close(done)
	}()

	select {
	case conn := <-accepted:
		t.Cleanup(func() { _ = conn.Close() })
	case <-time.After(5 * time.Second):
		t.Fatal("server never observed the dial attempt")
	}

	select {
	case <-done:
		require.Error(
			t, dialErr,
			"a stalled handshake must eventually time out, not succeed",
		)
	case <-time.After(dialTimeout + 5*time.Second):
		t.Fatal(
			"Dial did not return within dialTimeout+margin against a " +
				"stalled handshake, with no external cancellation",
		)
	}
}

// TestDial_SucceedsAgainstARealServer covers the ordinary path end to end:
// against a real gouroboros NtC server (Dial itself never calls ChainSync
// or LocalStateQuery, so no protocol config beyond the handshake is
// needed), Dial must return a working, closeable connection rather than
// only ever being exercised through its failure paths.
func TestDial_SucceedsAgainstARealServer(t *testing.T) {
	server := newTestChainSyncServer(t, 1)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	server.serve(listener, 42)

	conn, err := Dial(context.Background(), listener.Addr().String(), 42)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}
