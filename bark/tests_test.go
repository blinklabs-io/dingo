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

package bark

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"math/big"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	archivev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/archive"
	archiveconnect "github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
	databaseconnect "github.com/blinklabs-io/bark/proto/v1alpha1/database/databasev1alpha1connect"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// TestNewBark_RejectsNilDB pins that the constructor returns an error
// rather than panicking when the required database dependency is nil.
func TestNewBark_RejectsNilDB(t *testing.T) {
	t.Parallel()

	b, err := NewBark(BarkConfig{DB: nil})
	require.Error(t, err)
	assert.Nil(t, b)
	assert.Contains(t, err.Error(), "db is required")
}

// TestNewBarkBlobStore_RejectsNilUpstream pins that the constructor returns an
// error rather than panicking when the required upstream blob store is nil.
func TestNewBarkBlobStore_RejectsNilUpstream(t *testing.T) {
	t.Parallel()

	bs, err := NewBarkBlobStore(BlobStoreBarkConfig{}, nil)
	require.Error(t, err)
	assert.Nil(t, bs)
	assert.Contains(t, err.Error(), "upstream blob store is required")
}

func gzipBarkRequestBody(t *testing.T, body []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write(body)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return compressed.Bytes()
}

func sendBarkProtoRequest(
	t *testing.T,
	client *http.Client,
	baseURL string,
	procedure string,
	body []byte,
	compressed bool,
) *http.Response {
	t.Helper()
	if compressed {
		body = gzipBarkRequestBody(t, body)
	}
	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		baseURL+procedure,
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/proto")
	req.Header.Set("Connect-Protocol-Version", "1")
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestBarkConnectLimitRejectsOversizedDecompressedMessageBeforeAuth(
	t *testing.T,
) {
	t.Parallel()

	serverCertPath, serverKeyPath := writeTestTLSCertKey(t)
	_, _, clientCAPath := writeTestCA(t)
	b, err := NewBark(BarkConfig{
		DB:                              newTestDB(t),
		Lifecycle:                       newTestLifecycleService(t),
		SnapshotDir:                     t.TempDir(),
		Host:                            "127.0.0.1",
		Port:                            freeTCPPort(t),
		TlsCertFilePath:                 serverCertPath,
		TlsKeyFilePath:                  serverKeyPath,
		TlsClientCAFilePath:             clientCAPath,
		OperatorCertificateFingerprints: []string{strings.Repeat("00", 32)},
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	t.Cleanup(func() { _ = b.Stop(t.Context()) })

	// A highly-compressible unknown protobuf field expands beyond the limit.
	// The destructive procedure would reject this anonymous client at the
	// interceptor, so HTTP 429 also proves the decoder enforced the limit first.
	body := protowire.AppendTag(nil, 100, protowire.BytesType)
	body = protowire.AppendBytes(
		body,
		bytes.Repeat([]byte{'a'}, DefaultMaxRequestBody),
	)
	compressed := gzipBarkRequestBody(t, body)
	require.Less(t, len(compressed), DefaultMaxRequestBody)

	resp := sendBarkProtoRequest(
		t,
		mtlsHTTPClient(t, "", ""),
		"https://"+b.Addr(),
		databaseconnect.DatabaseServiceCancelOperationProcedure,
		body,
		true,
	)
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
}

func TestBarkConnectLimitRejectsOversizedCompressedWireMessage(t *testing.T) {
	t.Parallel()

	b, err := NewBark(BarkConfig{
		DB:   newTestDB(t),
		Host: "127.0.0.1",
		Port: freeTCPPort(t),
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	t.Cleanup(func() { _ = b.Stop(t.Context()) })

	// Random payload bytes do not compress, so gzip expands the request. The
	// decoded protobuf message remains within the limit while the compressed
	// wire message exceeds it, isolating the pre-decompression wire bound.
	payload := make([]byte, DefaultMaxRequestBody-5)
	_, err = rand.NewChaCha8([32]byte{1}).Read(payload)
	require.NoError(t, err)
	body := protowire.AppendTag(nil, 100, protowire.BytesType)
	body = protowire.AppendBytes(body, payload)
	compressed := gzipBarkRequestBody(t, body)
	require.LessOrEqual(t, len(body), DefaultMaxRequestBody)
	require.Greater(t, len(compressed), DefaultMaxRequestBody)

	resp := sendBarkProtoRequest(
		t,
		http.DefaultClient,
		"http://"+b.Addr(),
		archiveconnect.ArchiveServiceFetchBlockProcedure,
		body,
		true,
	)
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
}

func TestArchiveFetchBlockRejectsInvalidBatchSizes(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	handler := &archiveServiceHandler{bark: newTestBark(t, db)}

	t.Run("empty", func(t *testing.T) {
		_, err := handler.FetchBlock(
			t.Context(),
			connect.NewRequest(&archivev1alpha1.FetchBlockRequest{}),
		)
		require.Error(t, err)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("oversized", func(t *testing.T) {
		blocks := make(
			[]*archivev1alpha1.BlockRef,
			DefaultMaxFetchBlockRefs+1,
		)
		for i := range blocks {
			blocks[i] = &archivev1alpha1.BlockRef{}
		}
		_, err := handler.FetchBlock(
			t.Context(),
			connect.NewRequest(&archivev1alpha1.FetchBlockRequest{
				Blocks: blocks,
			}),
		)
		require.Error(t, err)
		require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
	})

	t.Run("maximum reaches storage", func(t *testing.T) {
		block := testBlock(1, 0x42)
		require.NoError(t, db.BlockCreate(block, nil))
		blocks := make(
			[]*archivev1alpha1.BlockRef,
			DefaultMaxFetchBlockRefs,
		)
		for i := range blocks {
			blocks[i] = &archivev1alpha1.BlockRef{
				Hash: new(hex.EncodeToString(block.Hash)),
				Slot: new(block.Slot),
			}
		}
		_, err := handler.FetchBlock(
			t.Context(),
			connect.NewRequest(&archivev1alpha1.FetchBlockRequest{
				Blocks: blocks,
			}),
		)
		// The in-memory test blob store deliberately does not support signed
		// URLs. Reaching its exact error proves equality passed the count guard;
		// an accidental >= comparison would return ResourceExhausted first.
		require.ErrorContains(t, err, "GetBlockURL not supported")
		require.NotEqual(t, connect.CodeResourceExhausted, connect.CodeOf(err))
	})
}

// writeTestTLSCertKey generates a throwaway self-signed cert/key pair valid
// for 127.0.0.1 and writes them as PEM files under a fresh t.TempDir(),
// returning their paths. Used to exercise Bark's TLS startup path without
// depending on any fixed certificate checked into the repo.
func writeTestTLSCertKey(t *testing.T) (certPath, keyPath string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(
		rand.Reader, template, template, &priv.PublicKey, priv,
	)
	require.NoError(t, err)

	dir := t.TempDir()
	certPath = filepath.Join(dir, "tls.crt")
	keyPath = filepath.Join(dir, "tls.key")

	certOut, err := os.Create(certPath)
	require.NoError(t, err)
	require.NoError(
		t,
		pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}),
	)
	require.NoError(t, certOut.Close())

	keyBytes, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	keyOut, err := os.OpenFile(
		keyPath,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0o600,
	)
	require.NoError(t, err)
	require.NoError(
		t,
		pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}),
	)
	require.NoError(t, keyOut.Close())

	return certPath, keyPath
}

// writeTestCA generates a throwaway self-signed CA keypair/cert and writes
// the cert as a PEM file under a fresh t.TempDir(), returning both the
// in-memory cert/key (for signing client leaf certs via
// writeTestClientCert) and the on-disk cert path (for use as a Bark
// server's TlsClientCAFilePath trust anchor).
func writeTestCA(
	t *testing.T,
) (caCert *x509.Certificate, caKey *ecdsa.PrivateKey, caCertPath string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "bark-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	derBytes, err := x509.CreateCertificate(
		rand.Reader, template, template, &priv.PublicKey, priv,
	)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(derBytes)
	require.NoError(t, err)

	caCertPath = filepath.Join(t.TempDir(), "ca.crt")
	certOut, err := os.Create(caCertPath)
	require.NoError(t, err)
	require.NoError(
		t,
		pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}),
	)
	require.NoError(t, certOut.Close())

	return cert, priv, caCertPath
}

// writeTestClientCert signs a client leaf certificate (ExtKeyUsageClientAuth)
// for commonName using caCert/caKey, and writes it plus its own private key
// as PEM files under a fresh t.TempDir(), returning their paths — for
// exercising Bark's mTLS ClientCAs verification with a cert that chains to
// a specific CA (writeTestCA's, to prove acceptance, or a different one, to
// prove rejection).
func writeTestClientCert(
	t *testing.T,
	caCert *x509.Certificate,
	caKey *ecdsa.PrivateKey,
	commonName string,
) (certPath, keyPath string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	derBytes, err := x509.CreateCertificate(
		rand.Reader, template, caCert, &priv.PublicKey, caKey,
	)
	require.NoError(t, err)

	dir := t.TempDir()
	certPath = filepath.Join(dir, "client.crt")
	keyPath = filepath.Join(dir, "client.key")

	certOut, err := os.Create(certPath)
	require.NoError(t, err)
	require.NoError(
		t,
		pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}),
	)
	require.NoError(t, certOut.Close())

	keyBytes, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	keyOut, err := os.OpenFile(
		keyPath,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0o600,
	)
	require.NoError(t, err)
	require.NoError(
		t,
		pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}),
	)
	require.NoError(t, keyOut.Close())

	return certPath, keyPath
}

func testCertificateFingerprint(t *testing.T, certPath string) string {
	t.Helper()
	pemBytes, err := os.ReadFile(certPath)
	require.NoError(t, err)
	block, _ := pem.Decode(pemBytes)
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	return certFingerprint(cert)
}

// TestTLSServerReusesPreloadedCertAfterFilesChange guards against a real
// bug: startServer's TLS path used to hand server.ServeTLS the same
// on-disk cert/key paths it had just preflight-loaded, causing ServeTLS to
// redundantly reload them from disk a second time inside the serving
// goroutine. If that second load ever failed (the files became
// unreadable or changed between the preflight and the goroutine running),
// ServeTLS returned before ever calling srv.Serve -- before Serve's own
// listener-closing defer was installed -- leaking the TCP listener and
// leaving Addr() reporting a real-looking address for a server that
// wasn't actually running.
//
// This corrupts the on-disk cert/key immediately after Start returns
// (before anything could plausibly have re-read them) and confirms the
// server keeps serving TLS traffic using the preloaded in-memory keypair
// regardless -- proving the serving goroutine never touches the files
// again after the preflight load inside Start.
func TestTLSServerReusesPreloadedCertAfterFilesChange(t *testing.T) {
	t.Parallel()

	certPath, keyPath := writeTestTLSCertKey(t)

	db := newTestDB(t)
	b, err := NewBark(BarkConfig{
		DB:              db,
		Host:            "127.0.0.1",
		Port:            freeTCPPort(t),
		TlsCertFilePath: certPath,
		TlsKeyFilePath:  keyPath,
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(context.Background()))
	t.Cleanup(func() { _ = b.Stop(context.Background()) })

	addr := b.Addr()
	require.NotEmpty(t, addr)

	// Corrupt the files on disk right away. With the fix, the serving
	// goroutine never reads them again (it reuses the cert loaded during
	// Start's synchronous preflight), so this can't affect anything.
	require.NoError(t, os.WriteFile(certPath, []byte("not a cert"), 0o600))
	require.NoError(t, os.WriteFile(keyPath, []byte("not a key"), 0o600))

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, //nolint:gosec // test-only, throwaway self-signed cert
			},
		},
		Timeout: 2 * time.Second,
	}

	testutil.WaitForCondition(t, func() bool {
		resp, getErr := client.Get(
			"https://" + addr + "/",
		) //nolint:noctx // test-only request
		if getErr != nil {
			return false
		}
		_ = resp.Body.Close()
		return true
	}, 3*time.Second,
		"TLS server should keep serving with the preloaded cert even "+
			"after the on-disk cert/key files are corrupted")

	require.NotEmpty(t, b.Addr(), "server should still be reported as serving")
}

// TestHandleServeExitClearsStateOnError exercises the exact cleanup path
// startServer's serving goroutine calls once Serve/ServeTLS returns:
// handleServeExit. Before this fix, that cleanup didn't exist at all --
// a Serve/ServeTLS error (e.g. from the redundant TLS reload this same
// change eliminates) just got logged, leaving the listener open (leaked)
// and b.server/b.listenerAddr still set as if the server were live, so
// Addr() would keep reporting a real-looking address for a server that
// wasn't actually running.
func TestHandleServeExitClearsStateOnError(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	b := newTestBark(t, db)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := &http.Server{} //nolint:gosec // test-only server
	b.mu.Lock()
	b.server = server
	b.listenerAddr = ln.Addr()
	b.mu.Unlock()
	require.NotEmpty(t, b.Addr())

	b.handleServeExit(
		server,
		ln,
		errors.New("simulated Serve/ServeTLS startup failure"),
	)

	require.Empty(
		t,
		b.Addr(),
		"Addr must clear once the serving goroutine exits with an error",
	)
	require.Nil(
		t,
		b.server,
		"server must be cleared once the serving goroutine exits with an error",
	)

	_, acceptErr := ln.Accept()
	require.Error(
		t,
		acceptErr,
		"listener must be closed once the serving goroutine exits with an error",
	)
}

// TestHandleServeExitIgnoresServerClosed verifies handleServeExit is a
// no-op for the expected, intentional shutdown signal
// (http.ErrServerClosed) -- Stop and the ctx-cancellation goroutine
// already closed the listener and cleared this state themselves in that
// case, so handleServeExit must not double-close or clobber a listener
// that another Bark instance's startServer call may have since installed
// in b.server/b.listenerAddr.
func TestHandleServeExitIgnoresServerClosed(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	b := newTestBark(t, db)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	server := &http.Server{}      //nolint:gosec // test-only server
	otherServer := &http.Server{} //nolint:gosec // test-only server
	b.mu.Lock()
	b.server = otherServer
	b.listenerAddr = ln.Addr()
	b.mu.Unlock()

	b.handleServeExit(server, ln, http.ErrServerClosed)

	require.Same(
		t,
		otherServer,
		b.server,
		"handleServeExit must not touch state belonging to a different server",
	)
	require.NotEmpty(t, b.Addr())
}
