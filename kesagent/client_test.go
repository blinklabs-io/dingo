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
	"crypto/rand"
	"net"
	"path/filepath"
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
func newTestKES(t *testing.T, start uint64) ([]byte, *kes.SecretKey, *forging.OpCert) {
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
func evolveClone(t *testing.T, master *kes.SecretKey, internal uint64) *kes.SecretKey {
	t.Helper()
	cur := &kes.SecretKey{
		Depth:  master.Depth,
		Period: master.Period,
		Data:   bytes.Clone(master.Data),
	}
	for cur.Period < internal {
		next, err := kes.Update(cur)
		if err != nil {
			t.Fatalf("evolve to %d: %v", internal, err)
		}
		cur = next
	}
	return cur
}

// --- fake agent ---------------------------------------------------------

// fakeAgent is an in-process KES agent speaking the #675 wire protocol. Each
// accepted connection is dispatched to the handler with the current (0-based)
// connection index so tests can vary behavior across reconnects.
type fakeAgent struct {
	ln      net.Listener
	handler func(index int, conn net.Conn)
	conns   int32
	wg      sync.WaitGroup
}

func startFakeAgent(t *testing.T, mode string, handler func(index int, conn net.Conn)) *fakeAgent {
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
			idx := int(atomic.AddInt32(&a.conns, 1)) - 1
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
		evolved := evolveClone(t, master, pushPeriod-start)
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

	client, err := New(Config{SocketPath: agent.socket(), Mode: ModeServeKey, OpCert: opcert})
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
			t.Fatalf("signature at absolute %d (relative %d) did not verify", period, period-start)
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
			evolved := evolveClone(t, master, p-start)
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

	client, err := New(Config{SocketPath: agent.socket(), Mode: ModeServeKey, OpCert: opcert})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// Wait for the first push to land, then let the agent send the second, so
	// the observed jump to period 4 can only come from the re-push.
	waitFor(t, 2*time.Second, func() bool { return client.CurrentPeriod() == 1 })
	close(releaseSecond)

	// The re-pushed (evolved) key advances the client's held period without
	// any local KESSign-driven evolution.
	waitFor(t, 2*time.Second, func() bool { return client.CurrentPeriod() == 4 })

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
			sk := evolveClone(t, master, rel)
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

	client, err := New(Config{SocketPath: agent.socket(), Mode: ModeSign, OpCert: opcert})
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
		evolved := evolveClone(t, master, period-start)
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
	waitFor(t, 3*time.Second, func() bool { return client.CurrentPeriod() == 5 })
	if atomic.LoadInt32(&agent.conns) < 2 {
		t.Fatalf("expected at least 2 connections, got %d", agent.conns)
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
		sk := evolveClone(t, master, rel)
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
	// First request hits the dropped connection and errors.
	if _, err := client.KESSign(period, msg); err == nil {
		t.Fatal("expected first sign to fail on dropped connection")
	}
	// Second request reconnects and succeeds.
	var sig []byte
	waitFor(t, 3*time.Second, func() bool {
		s, err := client.KESSign(period, msg)
		if err != nil {
			return false
		}
		sig = s
		return true
	})
	if !kes.VerifySignedKES(vkey, period-start, msg, sig) {
		t.Fatal("signature after reconnect did not verify")
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
		bad := evolveClone(t, otherMaster, 3)
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     3,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: bad.Data,
			KESVKey:    otherVKey,
		})
		good := evolveClone(t, master, 1)
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

	client, err := New(Config{SocketPath: agent.socket(), Mode: ModeServeKey, OpCert: opcert})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// Pinning the period is what makes this a rejection test: the bad push
	// claims a later period, so had it been accepted the legitimate push that
	// follows would have been refused as moving backward, leaving the bad key
	// installed.
	waitFor(t, 2*time.Second, func() bool { return client.CurrentPeriod() == 1 })

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
		bad := evolveClone(t, master, 3)
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     3,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: bad.Data,
			// No KESVKey at all.
		})
		good := evolveClone(t, master, 1)
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
	waitFor(t, 2*time.Second, func() bool { return client.CurrentPeriod() == 1 })

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
		impostor := evolveClone(t, otherMaster, 3)
		_ = writeFrame(conn, KeyPush{
			Type:   KeyPushType,
			Period: 3,
			Depth:  kes.CardanoKesDepth,
			// Someone else's secret key, presented under our vkey.
			KESSignKey: impostor.Data,
			KESVKey:    vkey,
		})
		good := evolveClone(t, master, 1)
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
	waitFor(t, 2*time.Second, func() bool { return client.CurrentPeriod() == 1 })

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
		good := evolveClone(t, master, 1)
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
	waitFor(t, 2*time.Second, func() bool { return client.CurrentPeriod() == 1 })

	msg := []byte("after malformed push")
	sig, err := client.KESSign(1, msg)
	if err != nil {
		t.Fatalf("sign with the accepted key: %v", err)
	}
	if !kes.VerifySignedKES(vkey, 1, msg, sig) {
		t.Fatal("a malformed key was installed")
	}
}
