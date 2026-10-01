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
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	databaseconnect "github.com/blinklabs-io/bark/proto/v1alpha1/database/databasev1alpha1connect"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/plugin"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDestructiveDatabaseProcedures_CoversEveryGeneratedMethod derives the
// full DatabaseService method list from the generated protobuf
// ServiceDescriptor -- the actual .proto-derived source of truth,
// independent of both destructiveDatabaseProcedures and the databaseconnect
// procedure constants -- and requires every single one to be explicitly
// classified as either destructive (destructiveDatabaseProcedures, auth.go)
// or read-only (readOnlyDatabaseProcedures, below). A prior version of this
// test only re-checked destructiveDatabaseProcedures against a hand-copied
// list of the same six names, so it could only ever notice one of those six
// being removed -- it could not catch a brand-new DatabaseService RPC added
// later landing in neither set and silently being served without mTLS. This
// version fails loudly on that: any procedure absent from both sets, or
// (a bug in itself) present in both, is a test failure.
func TestDestructiveDatabaseProcedures_CoversEveryGeneratedMethod(
	t *testing.T,
) {
	t.Parallel()

	fd := databasev1alpha1.File_v1alpha1_database_database_proto
	services := fd.Services()
	var svcIdx int
	for svcIdx = 0; svcIdx < services.Len(); svcIdx++ {
		if services.Get(svcIdx).Name() == "DatabaseService" {
			break
		}
	}
	require.Less(t, svcIdx, services.Len(),
		"DatabaseService not found in the generated file descriptor")
	svc := services.Get(svcIdx)

	methods := svc.Methods()
	require.Positive(t, methods.Len(), "DatabaseService has no methods")

	for i := 0; i < methods.Len(); i++ {
		method := methods.Get(i)
		procedure := "/" + string(svc.FullName()) + "/" + string(method.Name())
		isDestructive := destructiveDatabaseProcedures[procedure]
		isReadOnly := readOnlyDatabaseProcedures[procedure]
		assert.Truef(t, isDestructive || isReadOnly,
			"procedure %q is not classified as destructive (auth.go's "+
				"destructiveDatabaseProcedures) or read-only (this test's "+
				"readOnlyDatabaseProcedures) -- a new DatabaseService RPC "+
				"must be explicitly added to one of those", procedure)
		assert.Falsef(
			t,
			isDestructive && isReadOnly,
			"procedure %q is classified as both destructive and read-only",
			procedure,
		)
	}
}

// TestOperatorAuthInterceptor_FailsClosedForUnclassifiedProcedure pins the
// runtime property that makes readOnlyDatabaseProcedures an allowlist
// rather than destructiveDatabaseProcedures a denylist: a procedure name
// present in NEITHER map -- standing in for a new DatabaseService RPC added
// without updating either one -- must still require authentication and
// operator authorization exactly like a known destructive procedure.
func TestOperatorAuthInterceptor_FailsClosedForUnclassifiedProcedure(
	t *testing.T,
) {
	t.Parallel()

	const unclassified = "/bark.v1alpha1.database.DatabaseService/SomeFutureRPC"
	const operatorFingerprint = "operator"
	require.False(t, destructiveDatabaseProcedures[unclassified])
	require.False(t, readOnlyDatabaseProcedures[unclassified])

	interceptor := &operatorAuthInterceptor{
		logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		destructive: destructiveDatabaseProcedures,
		readOnly:    readOnlyDatabaseProcedures,
		operatorFingerprints: map[string]struct{}{
			operatorFingerprint: {},
		},
	}

	err := interceptor.authorize(context.Background(), unclassified)
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))

	err = interceptor.authorize(
		withPeerIdentity(t.Context(), peerIdentity{
			Verified:    true,
			Fingerprint: "reader",
		}),
		unclassified,
	)
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	require.NoError(t, interceptor.authorize(
		withPeerIdentity(t.Context(), peerIdentity{
			Verified:    true,
			Fingerprint: operatorFingerprint,
		}),
		unclassified,
	))
}

func TestOperatorAuthInterceptorEnforcesTwoStagesForEveryProcedure(
	t *testing.T,
) {
	t.Parallel()

	const operatorFingerprint = "operator"
	interceptor := &operatorAuthInterceptor{
		logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		destructive: destructiveDatabaseProcedures,
		readOnly:    readOnlyDatabaseProcedures,
		operatorFingerprints: map[string]struct{}{
			operatorFingerprint: {},
		},
	}
	readerCtx := withPeerIdentity(t.Context(), peerIdentity{
		Verified:    true,
		Fingerprint: "reader",
	})
	operatorCtx := withPeerIdentity(t.Context(), peerIdentity{
		Verified:    true,
		Fingerprint: operatorFingerprint,
	})

	for procedure := range readOnlyDatabaseProcedures {
		t.Run("read-only "+procedure, func(t *testing.T) {
			err := interceptor.authorize(t.Context(), procedure)
			require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
			require.NoError(t, interceptor.authorize(readerCtx, procedure))
		})
	}
	for procedure := range destructiveDatabaseProcedures {
		t.Run("destructive "+procedure, func(t *testing.T) {
			err := interceptor.authorize(t.Context(), procedure)
			require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
			err = interceptor.authorize(readerCtx, procedure)
			require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
			require.NoError(t, interceptor.authorize(operatorCtx, procedure))
		})
	}
}

// TestPeerCertContextMiddleware_KeysOffVerifiedChains pins the exact bug
// class this file's auth model previously had: peerCertContextMiddleware
// must decide Verified from r.TLS.VerifiedChains (populated only when the
// presented chain resolved to a trusted ClientCAs root), never from
// r.TLS.PeerCertificates alone (populated for whatever the client
// presented, verified or not). Driving this via a synthetic
// *tls.ConnectionState — rather than a real TLS handshake, as
// auth_test.go's wire-level test does — makes this deterministic
// regardless of whether tls.VerifyClientCertIfGiven happens to abort the
// handshake for a given unverifiable certificate (it does not always, as
// that wire-level test discovered): a bad cert can still reach this
// middleware with a non-empty PeerCertificates and an empty VerifiedChains,
// and that is exactly the case that must resolve to Verified: false.
func TestPeerCertContextMiddleware_KeysOffVerifiedChains(t *testing.T) {
	t.Parallel()

	leaf, _, _ := writeTestCA(
		t,
	) // any in-memory *x509.Certificate works as a stand-in leaf here

	interceptor := &operatorAuthInterceptor{
		logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		destructive: destructiveDatabaseProcedures,
		readOnly:    readOnlyDatabaseProcedures,
		operatorFingerprints: map[string]struct{}{
			certFingerprint(leaf): {},
		},
	}

	cases := []struct {
		name         string
		tlsState     *tls.ConnectionState
		wantVerified bool
	}{
		{
			name:         "no TLS at all (plaintext connection)",
			tlsState:     nil,
			wantVerified: false,
		},
		{
			name: "cert presented but not verified (empty VerifiedChains)",
			tlsState: &tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{leaf},
			},
			wantVerified: false,
		},
		{
			name: "cert presented and verified",
			tlsState: &tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{leaf},
				VerifiedChains:   [][]*x509.Certificate{{leaf}},
			},
			wantVerified: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotCtx context.Context
			next := http.HandlerFunc(
				func(_ http.ResponseWriter, r *http.Request) {
					gotCtx = r.Context()
				},
			)

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.TLS = tc.tlsState
			peerCertContextMiddleware(
				next,
			).ServeHTTP(httptest.NewRecorder(), req)

			id := peerIdentityFromContext(gotCtx)
			require.Equal(t, tc.wantVerified, id.Verified)

			err := interceptor.authorize(
				gotCtx,
				databaseconnect.DatabaseServiceRestoreProcedure,
			)
			if tc.wantVerified {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
			}
		})
	}
}

// newTestLifecycleService builds a minimal, never-actually-called
// dblifecycle.Service — enough to make BarkConfig.Lifecycle non-nil for the
// Start-time validation tests below, which never get far enough to invoke
// it.
func newTestLifecycleService(t *testing.T) *dblifecycle.Service {
	t.Helper()
	return dblifecycle.NewService(&config.Config{
		DatabasePath: t.TempDir(),
		Plugins: config.PluginsConfig{
			Storage: config.StoragePluginsConfig{
				Blob:     plugin.Selection{Provider: "badger"},
				Metadata: plugin.Selection{Provider: "sqlite"},
			},
		},
	}, testDestinationRegistry, nil)
}

// TestStart_RejectsLifecycleWithoutClientCA pins the fail-closed
// invariant: Start refuses to mount a DatabaseService (Lifecycle set)
// without a configured client CA, rather than silently serving its
// destructive RPCs to anonymous callers. This lives at Start, not NewBark —
// see Start's doc comment for why.
func TestStart_RejectsLifecycleWithoutClientCA(t *testing.T) {
	t.Parallel()

	serverCertPath, serverKeyPath := writeTestTLSCertKey(t)

	b, err := NewBark(BarkConfig{
		DB:              newTestDB(t),
		Lifecycle:       newTestLifecycleService(t),
		SnapshotDir:     t.TempDir(),
		Host:            "127.0.0.1",
		Port:            freeTCPPort(t),
		TlsCertFilePath: serverCertPath,
		TlsKeyFilePath:  serverKeyPath,
		// TlsClientCAFilePath deliberately left unset.
	})
	require.NoError(t, err)

	err = b.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TlsClientCAFilePath is required")
}

// TestStart_RejectsLifecycleWithoutTLS pins the companion half of the same
// invariant: a configured client CA alone isn't enough — mTLS has no
// meaning without the server's own TLS listener underneath it.
func TestStart_RejectsLifecycleWithoutTLS(t *testing.T) {
	t.Parallel()

	_, _, caCertPath := writeTestCA(t)

	b, err := NewBark(BarkConfig{
		DB:                  newTestDB(t),
		Lifecycle:           newTestLifecycleService(t),
		SnapshotDir:         t.TempDir(),
		Host:                "127.0.0.1",
		Port:                freeTCPPort(t),
		TlsClientCAFilePath: caCertPath,
		// TlsCertFilePath/TlsKeyFilePath deliberately left unset.
	})
	require.NoError(t, err)

	err = b.Start(context.Background())
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"TlsCertFilePath and TlsKeyFilePath are required",
	)
}

func TestStartRejectsLifecycleWithoutOperatorAllowlist(t *testing.T) {
	t.Parallel()

	serverCertPath, serverKeyPath := writeTestTLSCertKey(t)
	_, _, caCertPath := writeTestCA(t)

	b, err := NewBark(BarkConfig{
		DB:                  newTestDB(t),
		Lifecycle:           newTestLifecycleService(t),
		SnapshotDir:         t.TempDir(),
		Host:                "127.0.0.1",
		Port:                freeTCPPort(t),
		TlsCertFilePath:     serverCertPath,
		TlsKeyFilePath:      serverKeyPath,
		TlsClientCAFilePath: caCertPath,
	})
	require.NoError(t, err)

	err = b.Start(context.Background())
	require.ErrorContains(t, err, "OperatorCertificateFingerprint")
}

func TestNewBarkNormalizesOperatorCertificateFingerprints(t *testing.T) {
	t.Parallel()

	b, err := NewBark(BarkConfig{
		DB: newTestDB(t),
		OperatorCertificateFingerprints: []string{
			strings.Repeat("AB:", 31) + "AB",
		},
	})
	require.NoError(t, err)
	require.Contains(t, b.operatorFingerprints, strings.Repeat("ab", 32))

	_, err = NewBark(BarkConfig{
		DB:                              newTestDB(t),
		OperatorCertificateFingerprints: []string{"invalid"},
	})
	require.ErrorContains(t, err, "invalid operator certificate fingerprint")
}

// TestStart_RejectsClientCAWithoutTLS_NoLifecycle exercises startServer's
// own, Lifecycle-independent guard: even a bark instance with no
// DatabaseService at all (Archive-only) must not silently ignore a
// misconfigured TlsClientCAFilePath set without TLS cert/key.
func TestStart_RejectsClientCAWithoutTLS_NoLifecycle(t *testing.T) {
	t.Parallel()

	_, _, caCertPath := writeTestCA(t)

	b, err := NewBark(BarkConfig{
		DB:                  newTestDB(t),
		Host:                "127.0.0.1",
		Port:                freeTCPPort(t),
		TlsClientCAFilePath: caCertPath,
	})
	require.NoError(t, err)

	err = b.Start(context.Background())
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"TlsClientCAFilePath requires tls cert and key",
	)
}

// TestDatabaseServiceAuthenticationAndOperatorAuthorization is the wire-level
// proof of Bark's two-stage DatabaseService contract: every method requires a
// verified client identity, while destructive methods additionally require an
// explicitly allowed certificate fingerprint.
func TestDatabaseServiceAuthenticationAndOperatorAuthorization(t *testing.T) {
	t.Parallel()

	block1 := testBlock(1, 0x01)

	barkDataDir := t.TempDir()
	db := newDiskTestDB(t, barkDataDir)
	require.NoError(t, db.BlockCreate(block1, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: block1.Slot, Hash: block1.Hash},
		BlockNumber: block1.Number,
	}, nil))

	svcDataDir := t.TempDir()
	svcDB := newDiskTestDB(t, svcDataDir)
	require.NoError(t, svcDB.BlockCreate(block1, nil))
	require.NoError(t, svcDB.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: block1.Slot, Hash: block1.Hash},
		BlockNumber: block1.Number,
	}, nil))
	dbtest.CloseDatabase(svcDB) //nolint:errcheck

	svc := dblifecycle.NewService(&config.Config{
		DatabasePath: svcDataDir,
		Plugins: config.PluginsConfig{
			Storage: config.StoragePluginsConfig{
				Blob:     plugin.Selection{Provider: "badger"},
				Metadata: plugin.Selection{Provider: "sqlite"},
			},
		},
	}, nil, nil)

	serverCertPath, serverKeyPath := writeTestTLSCertKey(t)
	trustedCA, trustedCAKey, trustedCACertPath := writeTestCA(t)
	trustedClientCertPath, trustedClientKeyPath := writeTestClientCert(
		t, trustedCA, trustedCAKey, "trusted-operator",
	)
	readerClientCertPath, readerClientKeyPath := writeTestClientCert(
		t, trustedCA, trustedCAKey, "trusted-reader",
	)

	untrustedCA, untrustedCAKey, _ := writeTestCA(t)
	untrustedClientCertPath, untrustedClientKeyPath := writeTestClientCert(
		t, untrustedCA, untrustedCAKey, "untrusted-operator",
	)

	b, err := NewBark(BarkConfig{
		DB:                  db,
		Lifecycle:           svc,
		SnapshotDir:         t.TempDir(),
		Host:                "127.0.0.1",
		Port:                freeTCPPort(t),
		TlsCertFilePath:     serverCertPath,
		TlsKeyFilePath:      serverKeyPath,
		TlsClientCAFilePath: trustedCACertPath,
		OperatorCertificateFingerprints: []string{
			testCertificateFingerprint(t, trustedClientCertPath),
		},
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	defer func() { _ = b.Stop(context.Background()) }()
	require.NotEmpty(t, b.Addr())

	newClient := func(certPath, keyPath string) databaseconnect.DatabaseServiceClient {
		return databaseconnect.NewDatabaseServiceClient(
			mtlsHTTPClient(t, certPath, keyPath),
			"https://"+b.Addr(),
		)
	}

	t.Run(
		"anonymous client is rejected from read-only and destructive RPCs",
		func(t *testing.T) {
			client := newClient("", "")

			_, err := client.GetDatabaseInfo(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.GetDatabaseInfoRequest{}),
			)
			require.Error(t, err)
			require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))

			_, err = client.CancelOperation(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.CancelOperationRequest{
					OperationId: "nonexistent",
				}),
			)
			require.Error(t, err)
			require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
		},
	)

	t.Run(
		"authenticated reader cannot invoke destructive RPCs",
		func(t *testing.T) {
			client := newClient(readerClientCertPath, readerClientKeyPath)

			_, err := client.GetDatabaseInfo(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.GetDatabaseInfoRequest{}),
			)
			require.NoError(t, err)

			_, err = client.CancelOperation(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.CancelOperationRequest{
					OperationId: "nonexistent",
				}),
			)
			require.Error(t, err)
			require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
		},
	)

	t.Run(
		"cert signed by an untrusted CA is treated as unverified, not rejected outright",
		func(t *testing.T) {
			client := newClient(untrustedClientCertPath, untrustedClientKeyPath)

			// Whether this specific connection gets rejected at the handshake
			// or reaches the application layer anonymous is not something this
			// pins: a client can fail to actually present its configured
			// certificate for reasons entirely unrelated to CA trust (e.g. no
			// mutually acceptable signature scheme), in which case the
			// connection proceeds like any anonymous one rather than erroring,
			// alongside Go's documented handshake-level rejection of a
			// genuinely-received-but-untrusted chain. What must hold regardless
			// of which of those occurred is the actual security property:
			// peerCertContextMiddleware keys off r.TLS.VerifiedChains
			// (populated only for a chain that resolved to a trusted ClientCAs
			// root), not r.TLS.PeerCertificates (populated for whatever the
			// client presented, verified or not) — so this untrusted cert must
			// never be treated as an authenticated operator, regardless of
			// which path the connection actually took to get here.
			_, err := client.GetDatabaseInfo(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.GetDatabaseInfoRequest{}),
			)
			if err != nil {
				// Either the TLS handshake rejected the untrusted chain or the
				// DatabaseService interceptor rejected the resulting anonymous
				// identity. Both enforce the authentication boundary.
				return
			}
			require.Fail(
				t,
				"an untrusted certificate reached a read-only handler",
			)
		},
	)

	t.Run(
		"allowed operator certificate passes both stages",
		func(t *testing.T) {
			client := newClient(trustedClientCertPath, trustedClientKeyPath)

			_, err := client.CancelOperation(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.CancelOperationRequest{
					OperationId: "nonexistent",
				}),
			)
			require.Error(t, err)
			require.Equal(t, connect.CodeNotFound, connect.CodeOf(err),
				"should fail on the unknown operation id, not authentication")
		},
	)
}
