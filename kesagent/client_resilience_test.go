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
	"encoding/binary"
	"errors"
	"net"
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

	// The install callback owns the pushed key only for the duration of the
	// call -- Run wipes it on return -- so the assertion is made against a
	// copy taken inside that window, not against the caller's slice
	// afterward.
	installed := make(chan []byte, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	runErr := make(chan error, 1)
	go func() {
		runErr <- c.Run(ctx, func(pk PushedKey) error {
			installed <- append([]byte(nil), pk.KESSKeyData...)
			return errors.New("stop Run after the first successful install")
		})
	}()

	select {
	case installedKey := <-installed:
		require.Equal(t, skeyData, installedKey)
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

// TestClient_StalledFrameBodyDoesNotParkTheSubscriber covers the half of a
// frame read that an idle-tolerant subscriber must still bound. Waiting for a
// length header is legitimately unbounded -- a serve-key agent with nothing to
// say sends nothing -- but once a peer has declared a length, a body that
// never arrives has to end the read. Without the bound the subscription loop
// parks forever on a peer that announces a frame and then stops: no push, no
// reconnect, no log, and a producer that keeps forging on its current key
// until the first rotation it never received expires the operational
// certificate.
//
// The peer here sends a valid Hello, then a well-formed length header for a
// frame it never sends. FrameBodyTimeout is shortened so the assertion is
// about the bound applying at all, not about its production value; the outer
// select is a hang detector an order of magnitude larger, so a regression
// fails this test in seconds instead of hanging CI until the suite timeout.
func TestClient_StalledFrameBodyDoesNotParkTheSubscriber(t *testing.T) {
	t.Parallel()

	const bodyTimeout = 250 * time.Millisecond

	ln, sockPath := listenUnix(t)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		sendHello(t, conn, ModeServeKey)
		// A length header for a body that never follows.
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], 512)
		if _, err := conn.Write(hdr[:]); err != nil {
			return
		}
		// Hold the connection open: the peer is stalled, not gone, so a
		// plain EOF cannot be what ends the client's read.
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	c, err := NewClient(Config{
		SocketPath:       sockPath,
		Mode:             ModeServeKey,
		FrameBodyTimeout: bodyTimeout,
	})
	require.NoError(t, err)
	defer c.Close()

	// context.Background, not a timeout: a ctx deadline would bound the read
	// on its own and prove nothing about FrameBodyTimeout.
	done := make(chan error, 1)
	go func() {
		_, awaitErr := c.AwaitPushedKey(context.Background())
		done <- awaitErr
	}()

	select {
	case awaitErr := <-done:
		require.ErrorIs(t, awaitErr, errFrameBodyStalled)
	case <-time.After(30 * bodyTimeout):
		t.Fatal(
			"AwaitPushedKey parked on a declared frame body that never arrived",
		)
	}
}

// TestClient_HeaderWaitIsNotBoundedByFrameBodyTimeout is the control for the
// test above: the bound must apply to a declared body, and must not turn an
// idle subscriber -- an agent holding a connection open with nothing to push
// -- into a reconnect loop. Without this, "bound the read" could be satisfied
// by a deadline on the header too, which would tear down a healthy connection
// every FrameBodyTimeout.
func TestClient_HeaderWaitIsNotBoundedByFrameBodyTimeout(t *testing.T) {
	t.Parallel()

	const bodyTimeout = 100 * time.Millisecond

	ln, sockPath := listenUnix(t)
	pushNow := make(chan struct{})
	skeyData, vkey, opCertCBOR := testKESMaterial(t)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		sendHello(t, conn, ModeServeKey)
		<-pushNow
		_ = writeFrame(conn, MaxKeyPushFrameLen, KeyPush{
			Type:       "key_push",
			Period:     0,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: skeyData,
			KESVKey:    vkey,
			OpCert:     opCertCBOR,
		})
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	c, err := NewClient(Config{
		SocketPath:       sockPath,
		Mode:             ModeServeKey,
		FrameBodyTimeout: bodyTimeout,
	})
	require.NoError(t, err)
	defer c.Close()

	done := make(chan error, 1)
	go func() {
		_, awaitErr := c.AwaitPushedKey(context.Background())
		done <- awaitErr
	}()

	// Idle well past FrameBodyTimeout with no header in flight, then push.
	select {
	case awaitErr := <-done:
		t.Fatalf("idle subscriber was torn down before any push: %v", awaitErr)
	case <-time.After(5 * bodyTimeout):
	}
	close(pushNow)

	select {
	case awaitErr := <-done:
		require.NoError(t, awaitErr)
	case <-time.After(10 * time.Second):
		t.Fatal("push after an idle period was never delivered")
	}
}

// TestClient_InstalledKeyMaterialIsWipedAfterInstall pins the ownership window
// for pushed KES signing key material: the install callback may use it for the
// duration of the call and no longer, because Run zeroes the client's copy as
// soon as the callback returns. ledger/forging copies the bytes it keeps, so
// nothing downstream depends on the copy surviving.
//
// The callback retains the slice header rather than a copy, so the assertion
// reads the same backing array the client wiped.
func TestClient_InstalledKeyMaterialIsWipedAfterInstall(t *testing.T) {
	t.Parallel()

	skeyData, vkey, opCertCBOR := testKESMaterial(t)
	ln, sockPath := listenUnix(t)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		sendHello(t, conn, ModeServeKey)
		_ = writeFrame(conn, MaxKeyPushFrameLen, KeyPush{
			Type:       "key_push",
			Period:     0,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: skeyData,
			KESVKey:    vkey,
			OpCert:     opCertCBOR,
		})
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	c, err := NewClient(Config{SocketPath: sockPath, Mode: ModeServeKey})
	require.NoError(t, err)
	defer c.Close()

	retained := make(chan []byte, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	runErr := make(chan error, 1)
	go func() {
		runErr <- c.Run(ctx, func(pk PushedKey) error {
			// Inside the window the material must be the real key.
			require.Equal(t, skeyData, pk.KESSKeyData)
			retained <- pk.KESSKeyData
			return errors.New("stop after the first install")
		})
	}()

	var installedKey []byte
	select {
	case installedKey = <-retained:
	case <-time.After(10 * time.Second):
		t.Fatal("no key was installed")
	}
	cancel()
	<-runErr

	require.NotEmpty(t, installedKey)
	require.Equal(
		t,
		make([]byte, len(installedKey)),
		installedKey,
		"the client's copy of the pushed KES signing key must be zeroed once the install returns",
	)
}

// TestValidateKeyPushWipesItsInputKeyMaterial pins the same ownership rule one
// level down: validateKeyPush decodes a frame the caller discards and makes an
// evolvable probe copy of the signing key, and neither may outlive the call.
func TestValidateKeyPushWipesItsInputKeyMaterial(t *testing.T) {
	t.Parallel()

	skeyData, vkey, opCertCBOR := testKESMaterial(t)
	push := KeyPush{
		Type:       "key_push",
		Period:     0,
		Depth:      kes.CardanoKesDepth,
		KESSignKey: append([]byte(nil), skeyData...),
		KESVKey:    vkey,
		OpCert:     opCertCBOR,
	}
	retained := push.KESSignKey

	pk, err := validateKeyPush(push)
	require.NoError(t, err)
	require.Equal(t, skeyData, pk.KESSKeyData, "the validated copy survives")
	require.Equal(
		t,
		make([]byte, len(retained)),
		retained,
		"the decoded frame's own copy of the signing key must be zeroed",
	)
}

// TestClient_ServeKeyBackoffSurvivesAReconnect is the defect a Hello-time
// backoff reset produced: an agent whose key pushes the node cannot install
// reconnects successfully every time, so clearing the backoff on a completed
// handshake restarted the throttle at minReconnectBackoff after every
// failure. The reconnect interval then stayed flat for as long as the agent
// kept serving unusable material, which is exactly the case the throttle
// exists for.
//
// Asserted on the backoff itself rather than on an attempt count in a window:
// the difference between a flat and a doubling interval is one attempt over
// the first second, and a count would be measuring the scheduler.
func TestClient_ServeKeyBackoffSurvivesAReconnect(t *testing.T) {
	t.Parallel()

	skeyData, vkey, opCertCBOR := testKESMaterial(t)
	ln, sockPath := listenUnix(t)

	// Every connection gets a valid Hello and one valid push, so nothing the
	// client sees on the wire is a failure: only the install is.
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				sendHello(t, conn, ModeServeKey)
				_ = writeFrame(conn, MaxKeyPushFrameLen, KeyPush{
					Type:       "key_push",
					Period:     0,
					Depth:      kes.CardanoKesDepth,
					KESSignKey: skeyData,
					KESVKey:    vkey,
					OpCert:     opCertCBOR,
				})
				// Hold the connection open so the client's next read is the
				// idle wait, and the only thing that ends it is the client's
				// own invalidation after the failed install.
				_, _ = conn.Read(make([]byte, 1))
			}(conn)
		}
	}()

	c, err := NewClient(Config{SocketPath: sockPath, Mode: ModeServeKey})
	require.NoError(t, err)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const failuresWanted = 2
	var failures atomic.Int32
	runErr := make(chan error, 1)
	go func() {
		runErr <- c.Run(ctx, func(PushedKey) error {
			if failures.Add(1) >= failuresWanted {
				cancel()
			}
			return errors.New("install rejected by the node")
		})
	}()

	select {
	case <-runErr:
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not reach the second failed install")
	}
	require.GreaterOrEqual(t, int(failures.Load()), failuresWanted)
	require.Greater(
		t, c.currentBackoff(), minReconnectBackoff,
		"a second failed install must extend the reconnect backoff, "+
			"not restart it at the minimum",
	)
}

// TestClient_SignFailuresAreThrottled covers the sign-mode half of the same
// property. Every Sign failure path tears the connection down, so the next
// call redials; without recording those failures nothing throttled a
// misbehaving sign agent at all, and a forging node retried it at full speed
// on every leader slot.
//
// The success case is asserted in the same test because the two are one
// contract: the backoff has to grow on failure and clear on a verified
// signature, and a reset that never happens is as wrong as one that happens
// too early.
func TestClient_SignFailuresAreThrottled(t *testing.T) {
	t.Parallel()

	skeyData, vkey, _ := testKESMaterial(t)
	ln, sockPath := listenUnix(t)

	// Two connections answered with a signature over the wrong message --
	// well-formed, correctly typed, right period, and cryptographically
	// invalid -- then one answered correctly.
	var served atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				sendHello(t, conn, ModeSign)
				var req SignRequest
				if err := readFrame(conn, MaxSignFrameLen, &req); err != nil {
					return
				}
				message := req.Message
				if served.Add(1) <= 2 {
					message = []byte("not the requested message")
				}
				sk := &kes.SecretKey{
					Depth:  kes.CardanoKesDepth,
					Period: req.Period,
					Data:   append([]byte(nil), skeyData...),
				}
				sig, err := kes.Sign(sk, req.Period, message)
				if err != nil {
					return
				}
				_ = writeFrame(conn, MaxSignFrameLen, SignResponse{
					Type:      "sign_response",
					Period:    req.Period,
					Signature: sig,
				})
			}(conn)
		}
	}()

	c, err := NewClient(Config{
		SocketPath: sockPath,
		Mode:       ModeSign,
		KESVKey:    vkey,
	})
	require.NoError(t, err)
	defer c.Close()

	message := []byte("header-bytes")
	_, err = c.Sign(0, message)
	require.Error(t, err)
	first := c.currentBackoff()

	// The backoff from the first failure has to be waited out, or the second
	// call is refused by the throttle instead of reaching the agent -- which
	// is the throttle working, and not what this half is measuring.
	time.Sleep(first)
	_, err = c.Sign(0, message)
	require.Error(t, err)
	second := c.currentBackoff()
	require.Greater(
		t, second, first,
		"a second failed sign must extend the reconnect backoff",
	)

	time.Sleep(second)
	sig, err := c.Sign(0, message)
	require.NoError(t, err)
	require.True(t, kes.VerifySignedKES(vkey, 0, message, sig))
	c.mu.Lock()
	backoff := c.backoff
	c.mu.Unlock()
	require.Zero(
		t, backoff,
		"a verified signature must clear the backoff its failures accumulated",
	)
}
