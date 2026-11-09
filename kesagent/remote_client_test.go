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

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
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
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
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

// fakeRemoteAgent is a bursa attested sign listener with the "none"
// provider: by default TLS 1.3 requiring the client certificate, then the
// Hello, the admission exchange, and sign requests answered with real KES
// signatures.
type fakeRemoteAgent struct {
	addr     string
	server   testCert
	accepts  atomic.Int32
	signs    atomic.Int32
	admitFix func(*attestationResponse)
	// dropAfterRequest closes the connection after reading a sign request
	// without answering it.
	dropAfterRequest atomic.Bool
	// malformedRefusal answers a sign request with an error on a frame of
	// the wrong type.
	malformedRefusal atomic.Bool
}

// fakeTransport is the listener a fakeRemoteAgent runs.
type fakeTransport int

const (
	// fakeMutualTLS requires and verifies the client certificate.
	fakeMutualTLS fakeTransport = iota
	// fakeServerTLS asks for no client certificate.
	fakeServerTLS
	// fakePlainTCP runs the frames over plain TCP.
	fakePlainTCP
)

func startFakeRemoteAgent(
	t *testing.T,
	client testCert,
	skeyData, vkey, opCert []byte,
) *fakeRemoteAgent {
	t.Helper()
	return startFakeRemoteAgentOn(
		t, fakeMutualTLS, client, skeyData, vkey, opCert,
	)
}

func startFakeRemoteAgentOn(
	t *testing.T,
	transport fakeTransport,
	client testCert,
	skeyData, vkey, opCert []byte,
) *fakeRemoteAgent {
	t.Helper()
	a := &fakeRemoteAgent{server: newTestCert(t, "agent")}
	var ln net.Listener
	var err error
	switch transport {
	case fakePlainTCP:
		ln, err = net.Listen("tcp", "127.0.0.1:0")
	case fakeServerTLS:
		ln, err = tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{a.server.tls},
		})
	default:
		pool := x509.NewCertPool()
		pool.AddCert(client.x509)
		ln, err = tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{a.server.tls},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    pool,
		})
	}
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
	resp := attestationResponse{
		Type:     "attestation_response",
		Protocol: AttestedProtocolID,
		Nonce:    req.Nonce,
		Provider: ProviderNone,
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
		if a.malformedRefusal.Load() {
			if err := writeFrame(conn, MaxSignFrameLen, SignResponse{
				Type: "not_a_sign_response", Period: sreq.Period,
				Error: "refused",
			}); err != nil {
				return
			}
			continue
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

	// Without a pin the agent's self-signed certificate is verified against
	// the system roots, which do not hold it.
	certFile, keyFile := client.writePEM(t)
	unpinned, err := NewRemoteClient(RemoteConfig{
		Address:        a.addr,
		ClientCertFile: certFile,
		ClientKeyFile:  keyFile,
		KESVKey:        vkey,
		OpCert:         opCert,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = unpinned.Close() })
	var unknownAuthority x509.UnknownAuthorityError
	require.ErrorAs(t, unpinned.Prepare(), &unknownAuthority)
	require.Zero(t, a.signs.Load())
}

// TestRemoteClientTransportModes signs through each transport: a pinned
// server key with and without a client certificate, x509 verification
// without a pin, and plain TCP. Every
// mode runs the same admission and verifies every signature against the
// local operational certificate.
func TestRemoteClientTransportModes(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	for _, tc := range []struct {
		name       string
		transport  fakeTransport
		clientCert bool
		pinned     bool
		trustAgent bool
		plaintext  bool
	}{
		{
			name:       "pinned, client certificate",
			transport:  fakeMutualTLS,
			clientCert: true,
			pinned:     true,
		},
		{name: "pinned, no client certificate", transport: fakeServerTLS, pinned: true},
		{name: "verified by CA", transport: fakeServerTLS, trustAgent: true},
		{name: "plain TCP", transport: fakePlainTCP, plaintext: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := newTestCert(t, "node")
			a := startFakeRemoteAgentOn(
				t, tc.transport, client, skeyData, vkey, opCert,
			)
			cfg := RemoteConfig{
				Address:     a.addr,
				Plaintext:   tc.plaintext,
				KESVKey:     vkey,
				OpCert:      opCert,
				SignTimeout: 2 * time.Second,
			}
			if tc.pinned {
				cfg.ServerSPKISHA256 = a.server.spki
			}
			if tc.clientCert {
				cfg.ClientCertFile, cfg.ClientKeyFile = client.writePEM(t)
			}
			c, err := NewRemoteClient(cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Close() })
			if tc.trustAgent {
				// Stands in for a CA in the system roots: the agent's
				// self-signed certificate is its own authority.
				roots := x509.NewCertPool()
				roots.AddCert(a.server.x509)
				c.tlsConfig.RootCAs = roots
			}
			require.NoError(t, c.Prepare())
			sig, err := c.Sign(0, []byte("header"))
			require.NoError(t, err)
			require.True(t, kes.VerifySignedKES(vkey, 0, []byte("header"), sig))
			require.Equal(t, int32(1), a.signs.Load())
		})
	}
}

// TestRemoteClientRefusesConflictingTransportOptions checks the only refused
// combinations: options that need TLS on a plain TCP link, and half a client
// certificate.
func TestRemoteClientRefusesConflictingTransportOptions(t *testing.T) {
	t.Parallel()
	_, vkey, opCert := testKESMaterial(t)
	certFile, keyFile := newTestCert(t, "node").writePEM(t)
	pin := newTestCert(t, "agent").spki
	for _, tc := range []struct {
		name string
		cfg  RemoteConfig
		want string
	}{
		{
			"plain TCP with a pin",
			RemoteConfig{Plaintext: true, ServerSPKISHA256: pin},
			"plain TCP",
		},
		{
			"plain TCP with a client certificate",
			RemoteConfig{
				Plaintext:      true,
				ClientCertFile: certFile,
				ClientKeyFile:  keyFile,
			},
			"plain TCP",
		},
		{
			"certificate without key",
			RemoteConfig{ServerSPKISHA256: pin, ClientCertFile: certFile},
			"set together",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := tc.cfg
			cfg.Address = "127.0.0.1:1"
			cfg.KESVKey = vkey
			cfg.OpCert = opCert
			_, err := NewRemoteClient(cfg)
			require.ErrorContains(t, err, tc.want)
		})
	}
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
		{"pre-rename provider", func(r *attestationResponse) { r.Provider = "mtls" }},
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

	// The next call opens a new session: Sign dials through the reconnect
	// backoff, so one call must succeed.
	a.dropAfterRequest.Store(false)
	_, err = c.Sign(0, []byte("next-header"))
	require.NoError(t, err)
	require.Equal(t, int32(2), a.accepts.Load())
}

func TestRemoteClientDropsSessionOnMalformedRefusal(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	client := newTestCert(t, "node")
	a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)
	c := newTestRemoteClient(t, a, client, vkey, opCert, a.server.spki)

	require.NoError(t, c.Prepare())
	a.malformedRefusal.Store(true)
	_, err := c.Sign(0, []byte("header"))
	require.Error(t, err)

	a.malformedRefusal.Store(false)
	_, err = c.Sign(0, []byte("next-header"))
	require.NoError(t, err)
	require.Equal(t, int32(2), a.accepts.Load(),
		"an error on a malformed frame must not keep the session")
}

func TestRemoteClientReportsConnectedSession(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	client := newTestCert(t, "node")
	a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)
	certFile, keyFile := client.writePEM(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	c, err := NewRemoteClient(RemoteConfig{
		Address:          a.addr,
		ClientCertFile:   certFile,
		ClientKeyFile:    keyFile,
		ServerSPKISHA256: a.server.spki,
		KESVKey:          vkey,
		OpCert:           opCert,
		SignTimeout:      2 * time.Second,
		Metrics:          metrics,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.InDelta(t, 0, promtestutil.ToFloat64(metrics.connected), 0)
	require.NoError(t, c.Prepare())
	require.InDelta(t, 1, promtestutil.ToFloat64(metrics.connected), 0)
	require.NoError(t, c.Close())
	require.InDelta(t, 0, promtestutil.ToFloat64(metrics.connected), 0)
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

// opCertVariant re-encodes the test certificate with another issue number,
// KES period or cold key. The cold signature is left as it was: the client
// only recognises a candidate here, and the node validates it fully.
func opCertVariant(
	t *testing.T,
	opCert []byte,
	issue, period uint64,
	coldVKey []byte,
) []byte {
	t.Helper()
	dec, err := bursa.DecodeOpCert(opCert)
	require.NoError(t, err)
	if coldVKey == nil {
		coldVKey = dec.ColdVKey
	}
	out, err := cbor.Encode([]any{
		[]any{dec.KESVKey, issue, period, dec.ColdSig},
		coldVKey,
	})
	require.NoError(t, err)
	return out
}

func TestRemoteClientOffersANewerOpCertForTheSamePool(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	local, err := bursa.DecodeOpCert(opCert)
	require.NoError(t, err)
	otherCold := make([]byte, 32)
	otherCold[0] = 1
	newer := opCertVariant(t, opCert, local.IssueNumber+1, local.KESPeriod, nil)
	for _, tc := range []struct {
		name    string
		agent   []byte
		offered bool
	}{
		{"higher counter, same pool", newer, true},
		{
			"same counter, other bytes",
			opCertVariant(t, opCert, local.IssueNumber, local.KESPeriod+1, nil),
			false,
		},
		{
			"higher counter, other pool",
			opCertVariant(
				t, opCert, local.IssueNumber+1, local.KESPeriod, otherCold,
			),
			false,
		},
		{"not a certificate", []byte{0x01, 0x02}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := newTestCert(t, "node")
			a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)
			a.admitFix = func(r *attestationResponse) { r.OpCert = tc.agent }
			certFile, keyFile := client.writePEM(t)
			offers := make(chan []byte, 1)
			c, err := NewRemoteClient(RemoteConfig{
				Address:          a.addr,
				ClientCertFile:   certFile,
				ClientKeyFile:    keyFile,
				ServerSPKISHA256: a.server.spki,
				KESVKey:          vkey,
				OpCert:           opCert,
				OnNewerOpCert:    func(b []byte) { offers <- b },
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Close() })

			// The session is refused either way: signatures are verified
			// against the local certificate until the node reloads.
			require.ErrorIs(t, c.Prepare(), ErrRemoteAdmission)
			if tc.offered {
				require.Equal(t, tc.agent, testutil.RequireReceive(
					t, offers, time.Second, "newer certificate offered",
				))
				return
			}
			testutil.RequireNoReceive(t, offers, 20*time.Millisecond,
				"a mismatch that cannot be a rotation is not offered")
		})
	}
}
