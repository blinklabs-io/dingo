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
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/gouroboros/kes"
)

// --- test key helpers ---------------------------------------------------

// newTestKES generates a fresh KES key and a matching operational certificate
// with the given absolute start period. The returned master key is at internal
// period 0; callers clone and evolve it to produce keys at later periods.
func newTestKES(
	t *testing.T,
	start uint64,
) ([]byte, *kes.SecretKey, *forging.OpCert) {
	t.Helper()
	seed := make([]byte, kes.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sk, vkey, err := kes.KeyGen(kes.CardanoKesDepth, seed)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	opcert := &forging.OpCert{
		KESVKey:     vkey,
		IssueNumber: 0,
		KESPeriod:   start,
		Signature:   make([]byte, 64),
		ColdVKey:    make([]byte, 32),
	}
	return vkey, sk, opcert
}

// evolveClone returns a clone of master evolved forward to the given internal
// period.
func evolveClone(
	master *kes.SecretKey,
	internal uint64,
) (*kes.SecretKey, error) {
	cur := &kes.SecretKey{
		Depth:  master.Depth,
		Period: master.Period,
		Data:   bytes.Clone(master.Data),
	}
	for cur.Period < internal {
		next, err := kes.Update(cur)
		if err != nil {
			return nil, fmt.Errorf("evolve to %d: %w", internal, err)
		}
		cur = next
	}
	return cur, nil
}

// --- fake agent ---------------------------------------------------------

// fakeAgent is an in-process KES agent speaking the #675 wire protocol. Each
// accepted connection is dispatched to the handler with the current (0-based)
// connection index so tests can vary behavior across reconnects.
type fakeAgent struct {
	ln      net.Listener
	handler func(index int, conn net.Conn)
	conns   atomic.Int32
	wg      sync.WaitGroup
}

func startFakeAgent(
	t *testing.T,
	mode string,
	handler func(index int, conn net.Conn),
) *fakeAgent {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "agent.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	a := &fakeAgent{ln: ln, handler: handler}
	a.wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			idx := int(a.conns.Add(1)) - 1
			a.wg.Go(func() {
				defer func() { _ = conn.Close() }()
				// Every connection starts with the Hello handshake.
				if err := writeFrame(conn, Hello{Protocol: ProtocolID, Mode: mode}); err != nil {
					return
				}
				handler(idx, conn)
			})
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		a.wg.Wait()
	})
	return a
}

func (a *fakeAgent) socket() string { return a.ln.Addr().String() }

// --- tests --------------------------------------------------------------

// waitFor defers to the shared helper so these tests poll the same way the
// rest of the repo does, rather than hand-rolling a sleep loop.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	testutil.WaitForCondition(t, cond, d, "condition not met")
}

func TestPoolCredentialsSatisfiesKESSigner(t *testing.T) {
	// Compile-time assertion lives in forging; this documents the contract
	// from the consumer side too.
	var _ forging.KESSigner = (*forging.PoolCredentials)(nil)
	var _ forging.KESSigner = (*Client)(nil)
}

func TestServeKeyModeSignsAndVerifies(t *testing.T) {
	const start = uint64(10)
	vkey, master, opcert := newTestKES(t, start)
	const pushPeriod = uint64(12)

	agent := startFakeAgent(t, ModeServeKey, func(_ int, conn net.Conn) {
		evolved, err := evolveClone(master, pushPeriod-start)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Type:       "key_push",
			Period:     pushPeriod,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: evolved.Data,
			KESVKey:    vkey,
		})
		// Hold the connection open.
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	})

	client, err := New(
		Config{SocketPath: agent.socket(), Mode: ModeServeKey, OpCert: opcert},
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	waitFor(t, 2*time.Second, client.HasKey)

	// Sign at the pushed period and at a later period (local evolution).
	for _, period := range []uint64{pushPeriod, pushPeriod + 3} {
		if err := client.UpdateKESPeriod(period); err != nil {
			t.Fatalf("update to %d: %v", period, err)
		}
		msg := []byte("header body at period")
		sig, err := client.KESSign(period, msg)
		if err != nil {
			t.Fatalf("sign at %d: %v", period, err)
		}
		if !kes.VerifySignedKES(vkey, period-start, msg, sig) {
			t.Fatalf(
				"signature at absolute %d (relative %d) did not verify",
				period,
				period-start,
			)
		}
	}
}

func TestServeKeyModePicksUpRePush(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)

	// The agent pushes at period 1, then re-pushes (evolves) at period 4.
	// The second push waits for the test to observe the first, so the ordering
	// is a handoff rather than a race against a fixed delay.
	pushed := make(chan uint64, 4)
	releaseSecond := make(chan struct{})
	agent := startFakeAgent(t, ModeServeKey, func(_ int, conn net.Conn) {
		for _, p := range []uint64{1, 4} {
			if p == 4 {
				<-releaseSecond
			}
			evolved, err := evolveClone(master, p-start)
			if err != nil {
				return
			}
			if err := writeFrame(conn, KeyPush{
				Type:       KeyPushType,
				Period:     p,
				Depth:      kes.CardanoKesDepth,
				KESSignKey: evolved.Data,
				KESVKey:    vkey,
			}); err != nil {
				return
			}
			pushed <- p
		}
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	})

	client, err := New(
		Config{SocketPath: agent.socket(), Mode: ModeServeKey, OpCert: opcert},
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// Wait for the first push to land, then let the agent send the second, so
	// the observed jump to period 4 can only come from the re-push.
	waitFor(
		t,
		2*time.Second,
		func() bool { return client.CurrentPeriod() == 1 },
	)
	close(releaseSecond)

	// The re-pushed (evolved) key advances the client's held period without
	// any local KESSign-driven evolution.
	waitFor(
		t,
		2*time.Second,
		func() bool { return client.CurrentPeriod() == 4 },
	)

	msg := []byte("post-evolve header")
	sig, err := client.KESSign(4, msg)
	if err != nil {
		t.Fatalf("sign at 4: %v", err)
	}
	if !kes.VerifySignedKES(vkey, 4, msg, sig) {
		t.Fatal("signature at period 4 did not verify")
	}
}

func TestSignModeForwardsAndVerifies(t *testing.T) {
	const start = uint64(5)
	vkey, master, opcert := newTestKES(t, start)

	agent := startFakeAgent(t, ModeSign, func(_ int, conn net.Conn) {
		for {
			var req SignRequest
			if err := readFrame(conn, &req); err != nil {
				return
			}
			rel := req.Period - start
			sk, err := evolveClone(master, rel)
			if err != nil {
				return
			}
			sig, err := kes.Sign(sk, rel, req.Message)
			resp := SignResponse{Type: "sign_response", Period: req.Period}
			if err != nil {
				resp.Error = err.Error()
			} else {
				resp.Signature = sig
			}
			if err := writeFrame(conn, resp); err != nil {
				return
			}
		}
	})

	client, err := New(
		Config{SocketPath: agent.socket(), Mode: ModeSign, OpCert: opcert},
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// UpdateKESPeriod is a no-op in sign mode; the agent owns evolution.
	if err := client.UpdateKESPeriod(7); err != nil {
		t.Fatalf("update: %v", err)
	}
	const period = uint64(7)
	msg := []byte("remote-signed header body")
	sig, err := client.KESSign(period, msg)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if !kes.VerifySignedKES(vkey, period-start, msg, sig) {
		t.Fatal("remotely-produced signature did not verify")
	}
}

func TestSignModeRejectsInvalidSignature(t *testing.T) {
	const start = uint64(0)
	_, _, opcert := newTestKES(t, start)
	agent := startFakeAgent(t, ModeSign, func(_ int, conn net.Conn) {
		var req SignRequest
		if err := readFrame(conn, &req); err != nil {
			return
		}
		_ = writeFrame(conn, SignResponse{
			Type:      SignResponseType,
			Period:    req.Period,
			Signature: bytes.Repeat([]byte{0x01}, 448),
		})
	})

	client, err := New(Config{
		SocketPath: agent.socket(),
		Mode:       ModeSign,
		OpCert:     opcert,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	_, err = client.KESSign(1, []byte("message"))
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected invalid signature error, got %v", err)
	}
}

func TestSignModeRejectsClosedClient(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)
	client, err := New(Config{
		SocketPath: "/unused",
		Mode:       ModeSign,
		OpCert:     opcert,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Close()

	_, err = client.KESSign(0, []byte("message"))
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestRegisterConnRejectsCanceledContext(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)
	client, err := New(Config{
		SocketPath: "/unused",
		Mode:       ModeServeKey,
		OpCert:     opcert,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	clientConn, serverConn := net.Pipe()
	defer func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	}()

	if err := client.registerConn(ctx, clientConn); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf("expected canceled context error, got %v", err)
	}
	client.mu.Lock()
	registered := client.conn != nil
	client.mu.Unlock()
	if registered {
		t.Fatal("canceled connection was registered")
	}
}

func TestServeKeyModeReconnectsAfterDrop(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)

	// A serve-key handler that drops the first connection after pushing
	// period 1, then serves period 5 on the second (reconnected) connection.
	agent := startFakeAgent(t, ModeServeKey, func(index int, conn net.Conn) {
		period := uint64(1)
		if index >= 1 {
			period = 5
		}
		evolved, err := evolveClone(master, period-start)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Type:       "key_push",
			Period:     period,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: evolved.Data,
			KESVKey:    vkey,
		})
		if index == 0 {
			// Drop the first connection to force a reconnect.
			return
		}
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	})

	client, err := New(Config{
		SocketPath:   agent.socket(),
		Mode:         ModeServeKey,
		OpCert:       opcert,
		MinReconnect: 10 * time.Millisecond,
		MaxReconnect: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// After the dropped first connection, the client reconnects and receives
	// the period-5 key.
	waitFor(
		t,
		3*time.Second,
		func() bool { return client.CurrentPeriod() == 5 },
	)
	connections := agent.conns.Load()
	if connections < 2 {
		t.Fatalf("expected at least 2 connections, got %d", connections)
	}
}

func TestSignModeReconnectsAfterDrop(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)

	agent := startFakeAgent(t, ModeSign, func(index int, conn net.Conn) {
		var req SignRequest
		if err := readFrame(conn, &req); err != nil {
			return
		}
		if index == 0 {
			// Drop the first connection without responding, forcing the
			// client to reconnect for the next request.
			return
		}
		rel := req.Period - start
		sk, err := evolveClone(master, rel)
		if err != nil {
			return
		}
		sig, _ := kes.Sign(sk, rel, req.Message)
		_ = writeFrame(conn, SignResponse{
			Type:      "sign_response",
			Period:    req.Period,
			Signature: sig,
		})
	})

	client, err := New(Config{
		SocketPath:   agent.socket(),
		Mode:         ModeSign,
		OpCert:       opcert,
		MinReconnect: 10 * time.Millisecond,
		SignTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	const period = uint64(3)
	msg := []byte("reconnect header")
	// The first request hits the connection the agent drops, and must still
	// return a signature: the transport error is retried once on a fresh
	// connection. Before that retry existed this call failed with "write:
	// broken pipe" or EOF and the caller lost the block, which on a real pool
	// is the next slot win hours later.
	sig, err := client.KESSign(period, msg)
	if err != nil {
		t.Fatalf("sign across a dropped connection: %v", err)
	}
	if !kes.VerifySignedKES(vkey, period-start, msg, sig) {
		t.Fatal("signature after reconnect did not verify")
	}
	if connections := agent.conns.Load(); connections < 2 {
		t.Fatalf(
			"expected the retry to open a second connection, got %d",
			connections,
		)
	}
}

func TestServeKeyRejectsMismatchedKESVKey(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)
	// A different key whose vkey will not match the opcert.
	otherVKey, otherMaster, _ := newTestKES(t, start)

	// The agent sends the mismatched push first, then a legitimate one. Waiting
	// for the legitimate key to be installed proves the mismatched push was
	// already received and discarded, which a fixed sleep followed by
	// HasKey() == false cannot show: that assertion also holds before the push
	// arrives at all.
	agent := startFakeAgent(t, ModeServeKey, func(_ int, conn net.Conn) {
		bad, err := evolveClone(otherMaster, 3)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     3,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: bad.Data,
			KESVKey:    otherVKey,
		})
		good, err := evolveClone(master, 1)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     1,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: good.Data,
			KESVKey:    vkey,
		})
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	})

	client, err := New(
		Config{SocketPath: agent.socket(), Mode: ModeServeKey, OpCert: opcert},
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// Pinning the period is what makes this a rejection test: the bad push
	// claims a later period, so had it been accepted the legitimate push that
	// follows would have been refused as moving backward, leaving the bad key
	// installed.
	waitFor(
		t,
		2*time.Second,
		func() bool { return client.CurrentPeriod() == 1 },
	)

	// The held key must be the legitimate one: it has to verify against the
	// opcert's vkey, which the rejected key cannot do.
	msg := []byte("after rejected push")
	sig, err := client.KESSign(1, msg)
	if err != nil {
		t.Fatalf("sign with the accepted key: %v", err)
	}
	if !kes.VerifySignedKES(vkey, 1, msg, sig) {
		t.Fatal("held key is not the one committed to by the opcert")
	}
	if kes.VerifySignedKES(otherVKey, 1, msg, sig) {
		t.Fatal("mismatched key was installed")
	}
}

// TestServeKeyRejectsPushWithoutKESVKey covers the push that declares no
// verification key. Treating an absent vkey as "nothing to check" let such a
// push skip the operational-certificate cross-check entirely and be installed
// as trusted.
func TestServeKeyRejectsPushWithoutKESVKey(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)

	agent := startFakeAgent(t, ModeServeKey, func(_ int, conn net.Conn) {
		// The legitimate key, but with no verification key declared. Using the
		// real key is deliberate: it isolates the vkey comparison, since a
		// foreign key would be caught by the derivation check regardless.
		bad, err := evolveClone(master, 3)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     3,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: bad.Data,
			// No KESVKey at all.
		})
		good, err := evolveClone(master, 1)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     1,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: good.Data,
			KESVKey:    vkey,
		})
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	})

	client, err := New(
		Config{SocketPath: agent.socket(), Mode: ModeServeKey, OpCert: opcert},
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// Pinning the period is what makes this a rejection test: the bad push
	// claims a later period, so had it been accepted the legitimate push that
	// follows would have been refused as moving backward, leaving the bad key
	// installed.
	waitFor(
		t,
		2*time.Second,
		func() bool { return client.CurrentPeriod() == 1 },
	)

	msg := []byte("after vkey-less push")
	sig, err := client.KESSign(1, msg)
	if err != nil {
		t.Fatalf("sign with the accepted key: %v", err)
	}
	if !kes.VerifySignedKES(vkey, 1, msg, sig) {
		t.Fatal("a push without a vkey was installed")
	}
}

// TestServeKeyRejectsKeyNotDerivingOpCertVKey covers a push that declares the
// right verification key but carries a different secret key. The declared vkey
// is only what the agent asserts, so the key actually sent has to be checked
// against the operational certificate as well.
func TestServeKeyRejectsKeyNotDerivingOpCertVKey(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)
	_, otherMaster, _ := newTestKES(t, start)

	agent := startFakeAgent(t, ModeServeKey, func(_ int, conn net.Conn) {
		impostor, err := evolveClone(otherMaster, 3)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Type:   KeyPushType,
			Period: 3,
			Depth:  kes.CardanoKesDepth,
			// Someone else's secret key, presented under our vkey.
			KESSignKey: impostor.Data,
			KESVKey:    vkey,
		})
		good, err := evolveClone(master, 1)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     1,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: good.Data,
			KESVKey:    vkey,
		})
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	})

	client, err := New(
		Config{SocketPath: agent.socket(), Mode: ModeServeKey, OpCert: opcert},
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// Pinning the period is what makes this a rejection test: the bad push
	// claims a later period, so had it been accepted the legitimate push that
	// follows would have been refused as moving backward, leaving the bad key
	// installed.
	waitFor(
		t,
		2*time.Second,
		func() bool { return client.CurrentPeriod() == 1 },
	)

	msg := []byte("after impostor push")
	sig, err := client.KESSign(1, msg)
	if err != nil {
		t.Fatalf("sign with the accepted key: %v", err)
	}
	if !kes.VerifySignedKES(vkey, 1, msg, sig) {
		t.Fatal("a key that does not derive the opcert vkey was installed")
	}
}

// TestServeKeyRejectsMalformedKeyWithoutPanic covers a truncated key. Key
// derivation indexes into the buffer, so a short one has to be rejected on
// size before it reaches that code, or the node panics rather than declining
// the push.
func TestServeKeyRejectsMalformedKeyWithoutPanic(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)

	agent := startFakeAgent(t, ModeServeKey, func(_ int, conn net.Conn) {
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     3,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: []byte{0x01, 0x02, 0x03},
			KESVKey:    vkey,
		})
		good, err := evolveClone(master, 1)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     1,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: good.Data,
			KESVKey:    vkey,
		})
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	})

	client, err := New(
		Config{SocketPath: agent.socket(), Mode: ModeServeKey, OpCert: opcert},
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// Pinning the period is what makes this a rejection test: the bad push
	// claims a later period, so had it been accepted the legitimate push that
	// follows would have been refused as moving backward, leaving the bad key
	// installed.
	waitFor(
		t,
		2*time.Second,
		func() bool { return client.CurrentPeriod() == 1 },
	)

	msg := []byte("after malformed push")
	sig, err := client.KESSign(1, msg)
	if err != nil {
		t.Fatalf("sign with the accepted key: %v", err)
	}
	if !kes.VerifySignedKES(vkey, 1, msg, sig) {
		t.Fatal("a malformed key was installed")
	}
}

func TestServeKeyRejectsExpiredPush(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)
	client, err := New(Config{
		SocketPath: "/unused",
		Mode:       ModeServeKey,
		OpCert:     opcert,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	installed := client.applyKeyPush(t.Context(), KeyPush{
		Type:       KeyPushType,
		Period:     start + kes.MaxPeriod(kes.CardanoKesDepth),
		Depth:      kes.CardanoKesDepth,
		KESSignKey: bytes.Clone(master.Data),
		KESVKey:    vkey,
	})
	if installed {
		t.Fatal("expired KES key push was reported as installed")
	}
	if client.HasKey() {
		t.Fatal("expired KES key push was installed")
	}
}

// TestServeKeyRejectsUnsupportedDepth covers a push claiming a KES tree depth
// other than Shelley's. Such a key cannot produce Shelley-valid signatures, and
// without the depth rule the push is only refused after kes.PublicKey has
// walked an agent-controlled tree depth to derive the key — unbounded work
// driven by the peer.
//
// The assertion is on the rejection *reason*, not just that the key was
// refused: the vkey cross-check further down would reject this push either way,
// so anything weaker passes with or without the depth rule.
func TestServeKeyRejectsUnsupportedDepth(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)

	var logBuf bytes.Buffer
	var logMu sync.Mutex
	logger := slog.New(
		slog.NewTextHandler(&syncWriter{mu: &logMu, w: &logBuf}, nil),
	)

	agent := startFakeAgent(t, ModeServeKey, func(_ int, conn net.Conn) {
		// A deeper tree, with a buffer sized consistently for it so the layout
		// check passes and only the depth rule can reject it.
		deep := uint64(kes.CardanoKesDepth) + 4
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     3,
			Depth:      deep,
			KESSignKey: make([]byte, secretKeySize(deep)),
			KESVKey:    vkey,
		})
		good, err := evolveClone(master, 1)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     1,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: good.Data,
			KESVKey:    vkey,
		})
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	})

	client, err := New(Config{
		SocketPath: agent.socket(),
		Mode:       ModeServeKey,
		OpCert:     opcert,
		Logger:     logger,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// The rejected push claims the later period, so had it been installed the
	// legitimate push below would have been refused as moving backward.
	waitFor(
		t,
		2*time.Second,
		func() bool { return client.CurrentPeriod() == 1 },
	)

	logMu.Lock()
	logged := logBuf.String()
	logMu.Unlock()
	if !strings.Contains(logged, "unsupported key depth") {
		t.Fatalf(
			"depth was not rejected on the depth rule; log was:\n%s",
			logged,
		)
	}

	msg := []byte("after unsupported-depth push")
	sig, err := client.KESSign(1, msg)
	if err != nil {
		t.Fatalf("sign with the accepted key: %v", err)
	}
	if !kes.VerifySignedKES(vkey, 1, msg, sig) {
		t.Fatal("a key at an unsupported depth was installed")
	}
}

// syncWriter serializes writes from the client's background goroutine against
// the test goroutine's read of the buffer.
type syncWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// TestServeKeyRejectsKeyForgedFromPublicRootHashes covers the gap the vkey
// derivation check leaves open.
//
// For any depth above 0, kes.PublicKey with no cached public key falls through
// to publicKeyInternal, which returns HashPair of the two root public keys
// *read out of the buffer* rather than deriving them from the child secret. At
// depth 6 that inspects 64 of 608 bytes. Those 64 bytes are not secret: they
// are sig[384:448] of every KES signature the pool publishes, so anyone with
// one past block header can splice them onto random material and satisfy the
// derivation check.
//
// The push must be refused. Without the sign-and-verify probe in applyKeyPush
// the forged key installs, and every header the node then signs is rejected by
// its peers.
func TestServeKeyRejectsKeyForgedFromPublicRootHashes(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)

	// Take the two root public keys from a legitimate key, exactly as they
	// would be lifted out of a published signature, and graft them onto
	// otherwise meaningless bytes.
	legit, err := evolveClone(master, 0)
	if err != nil {
		t.Fatalf("clone master: %v", err)
	}
	forged := make([]byte, len(legit.Data))
	for i := range forged {
		forged[i] = 0xab
	}
	const rootHashOffset = 544
	copy(forged[rootHashOffset:], legit.Data[rootHashOffset:])

	// Confirm the premise: this forgery does satisfy the derivation check.
	if !bytes.Equal(
		kes.PublicKey(&kes.SecretKey{
			Depth:  kes.CardanoKesDepth,
			Period: 0,
			Data:   bytes.Clone(forged),
		}),
		vkey,
	) {
		t.Fatal(
			"premise broken: the forged key no longer derives the opcert vkey, so this test would pass for the wrong reason",
		)
	}

	agent := startFakeAgent(t, ModeServeKey, func(_ int, conn net.Conn) {
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     3,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: forged,
			KESVKey:    vkey,
		})
		good, err := evolveClone(master, 1)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     1,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: good.Data,
			KESVKey:    vkey,
		})
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	})

	client, err := New(
		Config{SocketPath: agent.socket(), Mode: ModeServeKey, OpCert: opcert},
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// The forged push claims the later period, so if it were accepted the
	// legitimate push behind it would be refused as moving backward.
	waitFor(
		t,
		2*time.Second,
		func() bool { return client.CurrentPeriod() == 1 },
	)

	msg := []byte("after forged push")
	sig, err := client.KESSign(1, msg)
	if err != nil {
		t.Fatalf("sign with the accepted key: %v", err)
	}
	if !kes.VerifySignedKES(vkey, 1, msg, sig) {
		t.Fatal("a forged key was installed and used to sign")
	}
}

// sessionLog records when each agent session begins, so a test can assert
// reconnect cadence from the intervals between sessions instead of from a count
// taken after a fixed sleep. The client sleeps before dialling, so an interval
// can only be longer than the backoff in force, never shorter: the assertions
// below hold on a loaded machine and do not need re-tuning when the reconnect
// constants change.
type sessionLog struct {
	mu sync.Mutex
	at []time.Time
}

func (l *sessionLog) mark() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.at = append(l.at, time.Now())
}

func (l *sessionLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.at)
}

// intervals returns the gaps between successive sessions.
func (l *sessionLog) intervals() []time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]time.Duration, 0, max(len(l.at)-1, 0))
	for i := 1; i < len(l.at); i++ {
		out = append(out, l.at[i].Sub(l.at[i-1]))
	}
	return out
}

// TestServeKeyBacksOffWhenSessionsEndImmediately covers the reconnect path for
// a session that ends rather than a dial that fails.
//
// An agent that completes the handshake and then closes is ordinary -- it is
// the shutdown window of a legitimate agent -- and previously sent runServeKey
// straight back to dial with no delay, measured at thousands of reconnects per
// second with a Warn line each. MinReconnect and MaxReconnect are documented
// as bounding reconnect; this pins that they do on this path too.
func TestServeKeyBacksOffWhenSessionsEndImmediately(t *testing.T) {
	const start = uint64(0)
	_, _, opcert := newTestKES(t, start)

	const minReconnect = 100 * time.Millisecond
	var sessions sessionLog
	agent := startFakeAgent(t, ModeServeKey, func(_ int, conn net.Conn) {
		sessions.mark()
		// Handshake completes inside startFakeAgent; drop immediately after.
		_ = conn.Close()
	})

	client, err := New(Config{
		SocketPath:   agent.socket(),
		Mode:         ModeServeKey,
		OpCert:       opcert,
		MinReconnect: minReconnect,
		MaxReconnect: time.Second,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// Four sessions give three intervals. Without the backoff on session end
	// the loop produced them a millisecond or two apart.
	waitFor(t, 5*time.Second, func() bool { return sessions.count() >= 4 })

	for i, gap := range sessions.intervals() {
		if gap < minReconnect {
			t.Fatalf(
				"reconnect %d came %v after the previous session; the floor is %v (all intervals: %v)",
				i+1,
				gap,
				minReconnect,
				sessions.intervals(),
			)
		}
	}
}

// TestServeKeyEscalatesBackoffWhenPushesAreRejected pins that a session which
// delivered a frame applyKeyPush refused counts as unproductive.
//
// An agent holding a key for another opcert -- a misconfigured deployment, or
// one left pointing at a rotated certificate -- pushes and disconnects on every
// session. Treating a refused push as productive reset the backoff to
// MinReconnect on every session, so the client reconnected at the floor
// indefinitely, logging a rejection each interval, and MaxReconnect was
// unreachable on this path.
func TestServeKeyEscalatesBackoffWhenPushesAreRejected(t *testing.T) {
	const start = uint64(0)
	vkey, _, opcert := newTestKES(t, start)

	// A vkey the operational certificate does not match, so every push is
	// refused. The signing key is well-formed but all zeroes: it could not
	// derive this vkey either, so the push is refused whichever check runs
	// first.
	wrongVKey := bytes.Clone(vkey)
	wrongVKey[0] ^= 0xff

	const minReconnect = 100 * time.Millisecond
	var sessions sessionLog
	agent := startFakeAgent(t, ModeServeKey, func(_ int, conn net.Conn) {
		sessions.mark()
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     start,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: make([]byte, secretKeySize(kes.CardanoKesDepth)),
			KESVKey:    wrongVKey,
		})
		_ = conn.Close()
	})

	client, err := New(Config{
		SocketPath:   agent.socket(),
		Mode:         ModeServeKey,
		OpCert:       opcert,
		MinReconnect: minReconnect,
		MaxReconnect: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// Five sessions give four intervals: 100ms, 200ms, 400ms, 800ms once a
	// refused push stops counting as progress, and a flat 100ms while it does.
	waitFor(t, 5*time.Second, func() bool { return sessions.count() >= 5 })

	intervals := sessions.intervals()
	last := intervals[len(intervals)-1]
	if last < 4*minReconnect {
		t.Fatalf(
			"backoff did not escalate across sessions whose pushes were refused: intervals %v, last %v (want >= %v)",
			intervals,
			last,
			4*minReconnect,
		)
	}
	if client.HasKey() {
		t.Fatal(
			"a push whose vkey does not match the operational certificate was installed",
		)
	}
}
