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

package kesagent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/stretchr/testify/require"
)

// TestClient_CloseInterruptsBlockedAwaitPushedKey proves Close can interrupt
// a goroutine parked in AwaitPushedKey's unbounded idle-between-pushes read
// -- the state Run's background loop spends nearly all its time in during
// normal serve-key operation -- rather than deadlocking behind it. A
// bounded-timeout select is the failure detector, not the pass condition:
// the test still passes only once both Close and the interrupted
// AwaitPushedKey actually return.
func TestClient_CloseInterruptsBlockedAwaitPushedKey(t *testing.T) {
	t.Parallel()

	ln, sockPath := listenUnix(t)
	helloSent := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		sendHello(t, conn, ModeServeKey)
		close(helloSent)
		// Never sends a KeyPush: the client's AwaitPushedKey blocks in its
		// idle wait for one, with no read deadline (ctx below carries none).
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	c, err := NewClient(Config{SocketPath: sockPath, Mode: ModeServeKey})
	require.NoError(t, err)

	awaitDone := make(chan error, 1)
	go func() {
		_, err := c.AwaitPushedKey(context.Background())
		awaitDone <- err
	}()

	select {
	case <-helloSent:
	case <-time.After(5 * time.Second):
		t.Fatal("fake agent never completed the handshake")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- c.Close() }()

	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal(
			"Close did not return: it deadlocked behind AwaitPushedKey's blocked read",
		)
	}

	select {
	case err := <-awaitDone:
		require.Error(
			t,
			err,
			"an interrupted AwaitPushedKey must return an error, not hang or silently succeed",
		)
	case <-time.After(5 * time.Second):
		t.Fatal("AwaitPushedKey did not return after Close")
	}
}

// TestClient_RunRecoversAfterTransientFailures proves Run survives a
// connection that closes before completing the handshake (an agent restart
// or a transient failure) and keeps retrying until a push actually installs,
// rather than giving up after the first failure. It blocks on the installed
// event rather than sleeping, and uses an overall context timeout only as a
// safety net against a genuine hang, not as the pass/fail measurement.
func TestClient_RunRecoversAfterTransientFailures(t *testing.T) {
	t.Parallel()

	skeyData, vkey, opCertCBOR := testKESMaterial(t)
	ln, sockPath := listenUnix(t)

	const failuresBeforeSuccess = 2
	var accepts atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := accepts.Add(1)
			if n <= failuresBeforeSuccess {
				_ = conn.Close() // closes before any Hello: a handshake failure
				continue
			}
			sendHello(t, conn, ModeServeKey)
			_ = writeFrame(conn, MaxKeyPushFrameLen, KeyPush{
				Type:       "key_push",
				Period:     0,
				Depth:      kes.CardanoKesDepth,
				KESSignKey: skeyData,
				KESVKey:    vkey,
				OpCert:     opCertCBOR,
			})
			return
		}
	}()

	c, err := NewClient(Config{SocketPath: sockPath, Mode: ModeServeKey})
	require.NoError(t, err)
	defer c.Close()

	installed := make(chan PushedKey, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	runErr := make(chan error, 1)
	go func() {
		runErr <- c.Run(ctx, func(pk PushedKey) error {
			installed <- pk
			return errors.New("stop Run after the first successful install")
		})
	}()

	select {
	case pk := <-installed:
		require.Equal(t, skeyData, pk.KESSKeyData)
	case <-time.After(10 * time.Second):
		t.Fatal(
			"Run did not recover and install a key after transient failures",
		)
	}
	cancel()
	<-runErr
	require.GreaterOrEqual(t, int(accepts.Load()), failuresBeforeSuccess+1)
}

// TestClient_ConnectBackoffBoundsDialAttempts proves a repeatedly failing
// handshake is throttled rather than hot-looping: an agent that accepts and
// immediately closes (never completing Hello) should receive only a handful
// of connection attempts in a bounded window, not thousands. The assertion
// is a generous upper bound on a count, not a timing measurement -- it gets
// safer under a loaded machine (fewer attempts fit in the window), not
// flakier.
func TestClient_ConnectBackoffBoundsDialAttempts(t *testing.T) {
	t.Parallel()

	ln, sockPath := listenUnix(t)
	var accepts atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			_ = conn.Close()
		}
	}()

	c, err := NewClient(Config{SocketPath: sockPath, Mode: ModeServeKey})
	require.NoError(t, err)
	defer c.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		1200*time.Millisecond,
	)
	defer cancel()
	_ = c.Run(ctx, func(PushedKey) error { return nil })

	// minReconnectBackoff=250ms doubling means attempts land at ~0, 250,
	// 750, 1750ms -- roughly 3 attempts fit in a 1200ms window with room to
	// spare; a broken backoff (no throttling at all) would produce orders of
	// magnitude more in the same window.
	require.LessOrEqual(t, int(accepts.Load()), 10)
	require.GreaterOrEqual(t, int(accepts.Load()), 1)
}
