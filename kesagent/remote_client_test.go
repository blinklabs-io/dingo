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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/stretchr/testify/require"
)

// testCert is a throwaway Ed25519 certificate and key.
type testCert struct {
	tls  tls.Certificate
	x509 *x509.Certificate
	spki string
}

func newTestCert(t *testing.T, name string) testCert {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	require.NoError(t, err)
	parsed, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return testCert{
		tls:  tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv},
		x509: parsed,
		spki: SPKIFingerprint(parsed.RawSubjectPublicKeyInfo),
	}
}

// writePEM writes c's certificate and key to files and returns their paths.
func (c testCert) writePEM(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	dir := t.TempDir()
	certFile = filepath.Join(dir, "client.crt")
	keyFile = filepath.Join(dir, "client.key")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: c.tls.Certificate[0],
	}), 0o600))
	keyDER, err := x509.MarshalPKCS8PrivateKey(c.tls.PrivateKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{
		Type: "PRIVATE KEY", Bytes: keyDER,
	}), 0o600))
	return certFile, keyFile
}

// fakeRemoteAgent is a bursa attested sign listener with the mtls provider:
// TLS 1.3 requiring the client certificate, the Hello, the admission
// exchange, then sign requests answered with real KES signatures.
type fakeRemoteAgent struct {
	addr     string
	server   testCert
	accepts  atomic.Int32
	signs    atomic.Int32
	admitFix func(*attestationResponse)
	// dropAfterRequest closes the connection after reading a sign request
	// without answering it.
	dropAfterRequest atomic.Bool
	// lastAdmission is the most recent attestation request received.
	lastAdmission atomic.Pointer[attestationRequest]
}

func startFakeRemoteAgent(
	t *testing.T,
	client testCert,
	skeyData, vkey, opCert []byte,
) *fakeRemoteAgent {
	t.Helper()
	a := &fakeRemoteAgent{server: newTestCert(t, "agent")}
	pool := x509.NewCertPool()
	pool.AddCert(client.x509)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{a.server.tls},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	a.addr = ln.Addr().String()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			a.accepts.Add(1)
			go a.serve(conn, skeyData, vkey, opCert)
		}
	}()
	return a
}

func (a *fakeRemoteAgent) serve(conn net.Conn, skeyData, vkey, opCert []byte) {
	defer conn.Close()
	if err := writeFrame(conn, MaxSignFrameLen, Hello{
		Protocol: AttestedProtocolID, Mode: ModeSign,
	}); err != nil {
		return
	}
	var req attestationRequest
	if err := readFrame(conn, MaxSignFrameLen, &req); err != nil {
		return
	}
	a.lastAdmission.Store(&req)
	resp := attestationResponse{
		Type:     "attestation_response",
		Protocol: AttestedProtocolID,
		Nonce:    req.Nonce,
		Provider: ProviderMTLS,
		KESVKey:  vkey,
		OpCert:   opCert,
		Period:   0,
	}
	if a.admitFix != nil {
		a.admitFix(&resp)
	}
	if err := writeFrame(conn, maxAttestationFrameLen, resp); err != nil {
		return
	}
	for {
		var sreq SignRequest
		if err := readFrame(conn, MaxSignFrameLen, &sreq); err != nil {
			return
		}
		if a.dropAfterRequest.Load() {
			return
		}
		sk := &kes.SecretKey{
			Depth:  kes.CardanoKesDepth,
			Period: sreq.Period,
			Data:   append([]byte(nil), skeyData...),
		}
		sig, err := kes.Sign(sk, sreq.Period, sreq.Message)
		if err != nil {
			return
		}
		a.signs.Add(1)
		if err := writeFrame(conn, MaxSignFrameLen, SignResponse{
			Type: "sign_response", Period: sreq.Period, Signature: sig,
		}); err != nil {
			return
		}
	}
}

func newTestRemoteClient(
	t *testing.T,
	a *fakeRemoteAgent,
	client testCert,
	vkey, opCert []byte,
	pin string,
) *RemoteClient {
	t.Helper()
	certFile, keyFile := client.writePEM(t)
	c, err := NewRemoteClient(RemoteConfig{
		Address:          a.addr,
		ClientCertFile:   certFile,
		ClientKeyFile:    keyFile,
		ServerSPKISHA256: pin,
		KESVKey:          vkey,
		OpCert:           opCert,
		SignTimeout:      2 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestRemoteClientPrepareThenSignReusesSession(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	client := newTestCert(t, "node")
	a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)
	c := newTestRemoteClient(t, a, client, vkey, opCert, a.server.spki)

	require.NoError(t, c.Prepare())
	for _, msg := range []string{"header-1", "header-2"} {
		sig, err := c.Sign(0, []byte(msg))
		require.NoError(t, err)
		require.True(t, kes.VerifySignedKES(vkey, 0, []byte(msg), sig))
	}
	require.Equal(t, int32(1), a.accepts.Load(),
		"Prepare's session must carry both signs")
}

func TestRemoteClientRefusesWrongOrMissingPin(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	client := newTestCert(t, "node")
	a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)

	other := newTestCert(t, "impostor")
	c := newTestRemoteClient(t, a, client, vkey, opCert, other.spki)
	err := c.Prepare()
	require.ErrorIs(t, err, ErrServerPinMismatch)
	_, err = c.Sign(0, []byte("header"))
	require.Error(t, err)
	require.Zero(t, a.signs.Load())

	certFile, keyFile := client.writePEM(t)
	_, err = NewRemoteClient(RemoteConfig{
		Address:        a.addr,
		ClientCertFile: certFile,
		ClientKeyFile:  keyFile,
		KESVKey:        vkey,
		OpCert:         opCert,
	})
	require.ErrorContains(t, err, "server SPKI SHA-256 pin")
}

func TestRemoteClientAdmissionChecks(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	for _, tc := range []struct {
		name string
		fix  func(*attestationResponse)
	}{
		{"wrong nonce", func(r *attestationResponse) { r.Nonce = make([]byte, 32) }},
		{"other provider", func(r *attestationResponse) { r.Provider = "nitro" }},
		{"evidence present", func(r *attestationResponse) { r.Attestation = []byte{1} }},
		{"other opcert", func(r *attestationResponse) {
			r.OpCert = append(append([]byte(nil), opCert...), 0)
		}},
		{"other KES key", func(r *attestationResponse) { r.KESVKey = make([]byte, 32) }},
		{"agent error", func(r *attestationResponse) { r.Error = "attestation failed" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := newTestCert(t, "node")
			a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)
			a.admitFix = tc.fix
			c := newTestRemoteClient(t, a, client, vkey, opCert, a.server.spki)
			require.ErrorIs(t, c.Prepare(), ErrRemoteAdmission)
			require.Zero(t, a.signs.Load())
		})
	}
}

func TestRemoteClientSelectsTheAgentProfileByKESKey(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	client := newTestCert(t, "node")
	a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)
	c := newTestRemoteClient(t, a, client, vkey, opCert, a.server.spki)
	require.NoError(t, c.Prepare())
	req := a.lastAdmission.Load()
	require.NotNil(t, req)
	require.Equal(t, vkey, req.KESVKey)
	require.Empty(t, req.Profile)

	// An agent that cannot select a profile for that key says so in the
	// response, and the node reports it as an admission failure.
	b := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)
	b.admitFix = func(r *attestationResponse) {
		*r = attestationResponse{
			Type:     "attestation_response",
			Protocol: AttestedProtocolID,
			Nonce:    r.Nonce,
			Provider: r.Provider,
			Error:    "kesagent: no profile serves the requested KES key",
		}
	}
	d := newTestRemoteClient(t, b, client, vkey, opCert, b.server.spki)
	err := d.Prepare()
	require.ErrorIs(t, err, ErrRemoteAdmission)
	require.ErrorContains(t, err, "no profile serves the requested KES key")
	require.Zero(t, b.signs.Load())
}

func TestRemoteClientDoesNotResendAfterADroppedRequest(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	client := newTestCert(t, "node")
	a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)
	c := newTestRemoteClient(t, a, client, vkey, opCert, a.server.spki)

	require.NoError(t, c.Prepare())
	a.dropAfterRequest.Store(true)
	_, err := c.Sign(0, []byte("header"))
	require.Error(t, err)
	require.Equal(t, int32(1), a.accepts.Load(),
		"a request that may have reached the agent must not be resent")

	// The next call opens a new session after the reconnect backoff.
	a.dropAfterRequest.Store(false)
	require.Eventually(t, func() bool {
		_, err := c.Sign(0, []byte("next-header"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(2), a.accepts.Load())
}

func TestRemoteClientRedialsAStaleSession(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	client := newTestCert(t, "node")
	a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)
	certFile, keyFile := client.writePEM(t)
	c, err := NewRemoteClient(RemoteConfig{
		Address:          a.addr,
		ClientCertFile:   certFile,
		ClientKeyFile:    keyFile,
		ServerSPKISHA256: a.server.spki,
		KESVKey:          vkey,
		OpCert:           opCert,
		SignTimeout:      2 * time.Second,
		SessionFreshFor:  time.Nanosecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.Prepare())
	_, err = c.Sign(0, []byte("header"))
	require.NoError(t, err)
	require.Equal(t, int32(2), a.accepts.Load(),
		"a session past SessionFreshFor must be replaced before signing")
}

// silentAgent accepts TCP connections and never speaks TLS, so a dial hangs
// until its deadline.
func silentAgent(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		var conns []net.Conn
		defer func() {
			for _, c := range conns {
				_ = c.Close()
			}
		}()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			conns = append(conns, c)
		}
	}()
	return ln.Addr().String()
}

// A Prepare stuck dialing must not hold a sign past its own SignTimeout, nor
// block a readiness check or Close.
func TestRemoteClientHungPrepareDoesNotStallSignOrChecks(t *testing.T) {
	t.Parallel()
	_, vkey, opCert := testKESMaterial(t)
	client := newTestCert(t, "node")
	certFile, keyFile := client.writePEM(t)
	c, err := NewRemoteClient(RemoteConfig{
		Address:          silentAgent(t),
		ClientCertFile:   certFile,
		ClientKeyFile:    keyFile,
		ServerSPKISHA256: client.spki,
		KESVKey:          vkey,
		OpCert:           opCert,
		HelloTimeout:     3 * time.Second,
		SignTimeout:      300 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	prepared := make(chan error, 1)
	go func() { prepared <- c.Prepare() }()
	require.Eventually(t, func() bool { return len(c.turn) == 1 },
		2*time.Second, 5*time.Millisecond, "Prepare never started dialing")

	start := time.Now()
	_, err = c.Sign(0, []byte("header"))
	require.Error(t, err)
	require.Less(t, time.Since(start), time.Second,
		"Sign must give up within its SignTimeout while Prepare dials")

	start = time.Now()
	_ = c.CheckReady()
	require.Less(t, time.Since(start), 100*time.Millisecond,
		"CheckReady must not wait on a dial")

	start = time.Now()
	require.NoError(t, c.Close())
	require.Less(t, time.Since(start), 100*time.Millisecond,
		"Close must not wait on a dial")
	select {
	case err := <-prepared:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Prepare did not return")
	}
}

// After a failed dial the backoff stops Prepare, but a sign at a leader slot
// still dials.
func TestRemoteClientSignDialsThroughTheBackoff(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	client := newTestCert(t, "node")
	a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)
	other := newTestCert(t, "impostor")
	certFile, keyFile := client.writePEM(t)
	c, err := NewRemoteClient(RemoteConfig{
		Address:          a.addr,
		ClientCertFile:   certFile,
		ClientKeyFile:    keyFile,
		ServerSPKISHA256: a.server.spki,
		KESVKey:          vkey,
		OpCert:           opCert,
		SignTimeout:      2 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// Fail one dial to arm the backoff: a wrong pin, then the right one.
	c.pin, _ = parseSPKIPin(other.spki)
	require.ErrorIs(t, c.Prepare(), ErrServerPinMismatch)
	c.pin, _ = parseSPKIPin(a.server.spki)
	require.ErrorContains(t, c.Prepare(), "backoff active")
	require.Error(t, c.CheckReady(), "CheckReady reports the last failure")

	_, err = c.Sign(0, []byte("header"))
	require.NoError(t, err)
	require.NoError(t, c.CheckReady())
}
