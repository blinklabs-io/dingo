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
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/stretchr/testify/require"
)

func TestClient_ServeKeyAwaitPushedKeyValidatesAndReturns(t *testing.T) {
	t.Parallel()

	skeyData, vkey, opCertCBOR := testKESMaterial(t)
	ln, sockPath := listenUnix(t)

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
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
	}()

	c, err := NewClient(Config{SocketPath: sockPath, Mode: ModeServeKey})
	require.NoError(t, err)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pk, err := c.AwaitPushedKey(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(0), pk.AbsolutePeriod)
	require.Equal(t, skeyData, pk.KESSKeyData)
	require.Equal(t, vkey, pk.KESVKey)
	require.Equal(t, uint64(0), pk.OpCert.KESPeriod)

	<-serverDone
}

func TestClient_ServeKeyRejectsCorruptedPush(t *testing.T) {
	t.Parallel()

	skeyData, vkey, opCertCBOR := testKESMaterial(t)
	corrupted := append([]byte(nil), skeyData...)
	corrupted[0] ^= 0xFF
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
			KESSignKey: corrupted,
			KESVKey:    vkey,
			OpCert:     opCertCBOR,
		})
	}()

	c, err := NewClient(Config{SocketPath: sockPath, Mode: ModeServeKey})
	require.NoError(t, err)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = c.AwaitPushedKey(ctx)
	require.ErrorContains(t, err, "does not match its own pushed verification key")
}

func TestClient_RejectsWrongProtocol(t *testing.T) {
	t.Parallel()

	ln, sockPath := listenUnix(t)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = writeFrame(conn, MaxHelloFrameLen, Hello{Protocol: "not-bursa/1", Mode: ModeServeKey})
	}()

	c, err := NewClient(Config{SocketPath: sockPath, Mode: ModeServeKey})
	require.NoError(t, err)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = c.AwaitPushedKey(ctx)
	require.ErrorIs(t, err, ErrWrongProtocol)
}

func TestClient_RejectsWrongMode(t *testing.T) {
	t.Parallel()

	ln, sockPath := listenUnix(t)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		sendHello(t, conn, ModeSign) // server reports sign, client wants serve-key
	}()

	c, err := NewClient(Config{SocketPath: sockPath, Mode: ModeServeKey})
	require.NoError(t, err)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = c.AwaitPushedKey(ctx)
	require.ErrorIs(t, err, ErrWrongMode)
}

// TestClient_HelloTimeoutBounded proves the initial handshake never blocks
// indefinitely on an agent that accepts but never sends anything (P1:
// "unbounded Hello handshake"). The bound is asserted as an upper limit well
// above HelloTimeout, not a tight one: the point is "returns", not "returns
// fast".
func TestClient_HelloTimeoutBounded(t *testing.T) {
	t.Parallel()

	ln, sockPath := listenUnix(t)
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- conn // held open, never sends Hello
	}()

	c, err := NewClient(Config{
		SocketPath:   sockPath,
		Mode:         ModeServeKey,
		HelloTimeout: 200 * time.Millisecond,
	})
	require.NoError(t, err)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	_, err = c.AwaitPushedKey(ctx)
	elapsed := time.Since(start)
	require.Error(t, err)
	require.Less(
		t,
		elapsed,
		5*time.Second,
		"AwaitPushedKey must not block past a bounded multiple of HelloTimeout",
	)

	conn := <-accepted
	_ = conn.Close()
}

func TestClient_SignHappyPath(t *testing.T) {
	t.Parallel()

	skeyData, vkey, _ := testKESMaterial(t)
	ln, sockPath := listenUnix(t)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		sendHello(t, conn, ModeSign)
		var req SignRequest
		if err := readFrame(conn, MaxSignFrameLen, &req); err != nil {
			return
		}
		sk := &kes.SecretKey{
			Depth:  kes.CardanoKesDepth,
			Period: req.Period, // opcert KESPeriod is 0, so relative == absolute
			Data:   append([]byte(nil), skeyData...),
		}
		sig, err := kes.Sign(sk, req.Period, req.Message)
		if err != nil {
			return
		}
		_ = writeFrame(conn, MaxSignFrameLen, SignResponse{
			Type:      "sign_response",
			Period:    req.Period,
			Signature: sig,
		})
	}()

	c, err := NewClient(Config{
		SocketPath: sockPath,
		Mode:       ModeSign,
		KESVKey:    vkey,
	})
	require.NoError(t, err)
	defer c.Close()

	sig, err := c.Sign(0, []byte("header-bytes"))
	require.NoError(t, err)
	require.True(t, kes.VerifySignedKES(vkey, 0, []byte("header-bytes"), sig))
}

func TestClient_SignRejectsMismatchedResponsePeriod(t *testing.T) {
	t.Parallel()

	_, vkey, _ := testKESMaterial(t)
	ln, sockPath := listenUnix(t)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		sendHello(t, conn, ModeSign)
		var req SignRequest
		if err := readFrame(conn, MaxSignFrameLen, &req); err != nil {
			return
		}
		_ = writeFrame(conn, MaxSignFrameLen, SignResponse{
			Type:      "sign_response",
			Period:    req.Period + 1, // wrong period echoed back
			Signature: make([]byte, kes.CardanoKesSignatureSize),
		})
	}()

	c, err := NewClient(Config{SocketPath: sockPath, Mode: ModeSign, KESVKey: vkey})
	require.NoError(t, err)
	defer c.Close()

	_, err = c.Sign(0, []byte("header-bytes"))
	require.ErrorContains(t, err, "does not match requested period")
}

func TestClient_SignRejectsInvalidSignature(t *testing.T) {
	t.Parallel()

	_, vkey, _ := testKESMaterial(t)
	ln, sockPath := listenUnix(t)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		sendHello(t, conn, ModeSign)
		var req SignRequest
		if err := readFrame(conn, MaxSignFrameLen, &req); err != nil {
			return
		}
		garbage := make([]byte, kes.CardanoKesSignatureSize)
		garbage[0] = 0x42
		_ = writeFrame(conn, MaxSignFrameLen, SignResponse{
			Type:      "sign_response",
			Period:    req.Period,
			Signature: garbage,
		})
	}()

	c, err := NewClient(Config{SocketPath: sockPath, Mode: ModeSign, KESVKey: vkey})
	require.NoError(t, err)
	defer c.Close()

	_, err = c.Sign(0, []byte("header-bytes"))
	require.ErrorContains(t, err, "failed KES signature verification")
}

// TestClient_SignTimeoutBounded proves a sign-mode round trip is bounded by
// SignTimeout even when the agent never responds (P1: "sign round-trip
// ignores SignTimeout").
func TestClient_SignTimeoutBounded(t *testing.T) {
	t.Parallel()

	_, vkey, _ := testKESMaterial(t)
	ln, sockPath := listenUnix(t)
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		sendHello(t, conn, ModeSign)
		accepted <- conn // read the request but never respond
	}()

	c, err := NewClient(Config{
		SocketPath:  sockPath,
		Mode:        ModeSign,
		KESVKey:     vkey,
		SignTimeout: 200 * time.Millisecond,
	})
	require.NoError(t, err)
	defer c.Close()

	start := time.Now()
	_, err = c.Sign(0, []byte("header-bytes"))
	elapsed := time.Since(start)
	require.Error(t, err)
	require.Less(
		t,
		elapsed,
		5*time.Second,
		"Sign must not block past a bounded multiple of SignTimeout",
	)

	conn := <-accepted
	_ = conn.Close()
}
