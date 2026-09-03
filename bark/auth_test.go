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
	"testing"

	"connectrpc.com/connect"
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	databaseconnect "github.com/blinklabs-io/bark/proto/v1alpha1/database/databasev1alpha1connect"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/plugin"
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
				"must be explicitly added to one of those, not silently "+
				"left unauthenticated by default", procedure)
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
// to the proto without updating either one -- must still require a
// verified client certificate, exactly like a known destructive procedure,
// not be silently let through the way "allow unless listed as destructive"
// would. TestDestructiveDatabaseProcedures_CoversEveryGeneratedMethod is
// what keeps that situation from actually arising in practice, but this
// test pins that authorize itself does not depend on that check having
// run.
func TestOperatorAuthInterceptor_FailsClosedForUnclassifiedProcedure(
	t *testing.T,
) {
	const unclassified = "/bark.v1alpha1.database.DatabaseService/SomeFutureRPC"
	require.False(t, destructiveDatabaseProcedures[unclassified])
	require.False(t, readOnlyDatabaseProcedures[unclassified])

	interceptor := &operatorAuthInterceptor{
		logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		destructive: destructiveDatabaseProcedures,
		readOnly:    readOnlyDatabaseProcedures,
	}

	err := interceptor.authorize(context.Background(), unclassified)
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
}

// TestPeerCertContextMiddleware_KeysOffVerifiedChains pins the exact bug
// class this file's auth model previously had: peerCertContextMiddleware
// must decide Verified from r.TLS.VerifiedChains (populated only when the
// presented chain resolved to a trusted ClientCAs root), never from
// r.TLS.PeerCertificates alone (populated for whatever the client
// presented, verified or not). Driving this via a synthetic
// *tls.ConnectionState — rather than a real TLS handshake, as
// auth_wire_test.go's wire-level test does — makes this deterministic
// regardless of whether tls.VerifyClientCertIfGiven happens to abort the
// handshake for a given unverifiable certificate (it does not always, as
// that wire-level test discovered): a bad cert can still reach this
// middleware with a non-empty PeerCertificates and an empty VerifiedChains,
// and that is exactly the case that must resolve to Verified: false.
func TestPeerCertContextMiddleware_KeysOffVerifiedChains(t *testing.T) {
	leaf, _, _ := writeTestCA(
		t,
	) // any in-memory *x509.Certificate works as a stand-in leaf here

	interceptor := &operatorAuthInterceptor{
		logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		destructive: destructiveDatabaseProcedures,
		readOnly:    readOnlyDatabaseProcedures,
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

// TestStart_RejectsLifecycleWithoutClientCA pins bark#2988's fail-closed
// invariant: Start refuses to mount a DatabaseService (Lifecycle set)
// without a configured client CA, rather than silently serving its
// destructive RPCs to anonymous callers. This lives at Start, not NewBark —
// see Start's doc comment for why.
func TestStart_RejectsLifecycleWithoutClientCA(t *testing.T) {
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

// TestStart_RejectsClientCAWithoutTLS_NoLifecycle exercises startServer's
// own, Lifecycle-independent guard: even a bark instance with no
// DatabaseService at all (Archive-only) must not silently ignore a
// misconfigured TlsClientCAFilePath set without TLS cert/key.
func TestStart_RejectsClientCAWithoutTLS_NoLifecycle(t *testing.T) {
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
