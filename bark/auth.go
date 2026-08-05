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
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"
	databaseconnect "github.com/blinklabs-io/bark/proto/v1alpha1/database/databasev1alpha1connect"
)

// destructiveDatabaseProcedures is the set of DatabaseService RPCs that
// mutate state, consume significant resources, or interfere with another
// caller's in-flight operation — as opposed to the service's read-only
// status/catalog RPCs (ListSnapshots, ListAvailableSnapshots,
// GetSnapshotStatus, GetRestoreStatus, GetTruncateStatus,
// StreamOperationProgress, GetOperationHistory, GetDatabaseInfo) and the
// entirely-read-only ArchiveService, none of which appear here.
// newOperatorAuthInterceptor requires a verified mTLS client certificate for
// every procedure in this set.
//
// CreateSnapshot/VerifySnapshot go beyond the literal "restore, truncate,
// delete snapshots, or cancel operations" examples in the issue this guards
// against, but fit the same threat model: CreateSnapshot consumes local (or
// billed cloud) storage on every anonymous call, and VerifySnapshot claims
// the handler's single busy flag (see database.go's acquireBusy) for a
// full restore-to-tempdir, letting an anonymous caller block a legitimate
// operator's CreateSnapshot/Restore/Truncate for as long as it runs.
var destructiveDatabaseProcedures = map[string]bool{
	databaseconnect.DatabaseServiceCreateSnapshotProcedure:  true,
	databaseconnect.DatabaseServiceDeleteSnapshotProcedure:  true,
	databaseconnect.DatabaseServiceVerifySnapshotProcedure:  true,
	databaseconnect.DatabaseServiceRestoreProcedure:         true,
	databaseconnect.DatabaseServiceTruncateProcedure:        true,
	databaseconnect.DatabaseServiceCancelOperationProcedure: true,
}

// peerIdentity is what peerCertContextMiddleware extracts from a verified
// mTLS client certificate and stashes in the request context — just enough
// to authorize (Verified) and audit-log (CommonName/Fingerprint) a
// destructive call, not a general certificate representation.
type peerIdentity struct {
	Verified    bool
	CommonName  string
	Fingerprint string
}

type peerIdentityContextKey struct{}

// withPeerIdentity returns a copy of ctx carrying id, retrievable via
// peerIdentityFromContext.
func withPeerIdentity(ctx context.Context, id peerIdentity) context.Context {
	return context.WithValue(ctx, peerIdentityContextKey{}, id)
}

// peerIdentityFromContext returns the peerIdentity peerCertContextMiddleware
// stashed in ctx, or the zero value (Verified: false) if the middleware
// never ran — e.g. a direct in-process call in a test that bypasses Start's
// handler chain entirely.
func peerIdentityFromContext(ctx context.Context) peerIdentity {
	id, _ := ctx.Value(peerIdentityContextKey{}).(peerIdentity)
	return id
}

// certFingerprint returns the hex-encoded SHA-256 fingerprint of cert's raw
// DER bytes — a stable, compact identifier for audit logging that doesn't
// require parsing the full certificate back out of the log line.
func certFingerprint(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(sum[:])
}

// peerCertContextMiddleware wraps an http.Handler and stashes whether the
// current connection presented a verified client certificate into the
// request context, keyed for peerIdentityFromContext to retrieve later once
// the request has been routed to a specific Connect/gRPC/gRPC-Web RPC.
//
// This is what makes newOperatorAuthInterceptor's check framing-agnostic:
// Connect, gRPC, and gRPC-Web are all just HTTP requests multiplexed
// through the same *http.Server underneath this middleware, so inspecting
// r.TLS here — rather than duplicating a per-framing check inside each
// protocol's own request handling — covers all three with one code path.
//
// r.TLS.PeerCertificates alone is NOT a trustworthy signal: it holds
// whatever certificate the client presented, verified or not — Go's
// tls.VerifyClientCertIfGiven (see bark.go's startServer) does not abort
// the handshake on a failed client-cert verification the way
// RequireAndVerifyClientCert would, so a self-signed or wrong-CA
// certificate still reaches this middleware with a non-empty
// PeerCertificates. r.TLS.VerifiedChains is the actual verification
// signal: it is only non-empty when the presented chain resolved to a
// trusted root in the listener's configured ClientCAs, which is exactly
// what this function keys off of.
func peerCertContextMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var id peerIdentity
		if r.TLS != nil && len(r.TLS.VerifiedChains) > 0 {
			leaf := r.TLS.VerifiedChains[0][0]
			id = peerIdentity{
				Verified:    true,
				CommonName:  leaf.Subject.CommonName,
				Fingerprint: certFingerprint(leaf),
			}
		}
		next.ServeHTTP(w, r.WithContext(withPeerIdentity(r.Context(), id)))
	})
}

// errAnonymousDestructiveCall is returned by newOperatorAuthInterceptor for
// any destructive DatabaseService call whose connection presented no
// verified client certificate.
var errAnonymousDestructiveCall = errors.New(
	"this RPC requires a verified mTLS client certificate: destructive " +
		"DatabaseService operations cannot be called anonymously",
)

// newOperatorAuthInterceptor returns a connect.Interceptor that rejects any
// call to a procedure in destructive with connect.CodeUnauthenticated
// unless peerCertContextMiddleware recorded a verified client certificate
// on the request's connection. Every destructive call — accepted or
// rejected — is logged with the caller's certificate identity (or its
// absence), so an operator can audit who ran a Restore/Truncate/
// DeleteSnapshot/etc. after the fact; GetOperationHistory (database.go) has
// no notion of caller identity of its own to fall back on.
//
// Implements the full connect.Interceptor interface — including
// WrapStreamingHandler/WrapStreamingClient, which are no-ops for every
// procedure in destructive today (none of them stream) — so the same
// interceptor and destructive-procedure-set pattern can be reused for
// bark#17's proposed LifecycleService, which explicitly calls for the same
// "no anonymous calls" requirement and may include streaming RPCs.
func newOperatorAuthInterceptor(
	logger *slog.Logger,
	destructive map[string]bool,
) connect.Interceptor {
	return &operatorAuthInterceptor{logger: logger, destructive: destructive}
}

type operatorAuthInterceptor struct {
	logger      *slog.Logger
	destructive map[string]bool
}

func (i *operatorAuthInterceptor) authorize(ctx context.Context, procedure string) error {
	if !i.destructive[procedure] {
		return nil
	}
	id := peerIdentityFromContext(ctx)
	if !id.Verified {
		i.logger.Warn(
			"rejected anonymous call to destructive DatabaseService RPC",
			"component", "bark",
			"procedure", procedure,
		)
		return connect.NewError(connect.CodeUnauthenticated, errAnonymousDestructiveCall)
	}
	i.logger.Info(
		"destructive DatabaseService RPC authenticated",
		"component", "bark",
		"procedure", procedure,
		"operator_cn", id.CommonName,
		"operator_cert_fingerprint", id.Fingerprint,
	)
	return nil
}

func (i *operatorAuthInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if err := i.authorize(ctx, req.Spec().Procedure); err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

func (i *operatorAuthInterceptor) WrapStreamingClient(
	next connect.StreamingClientFunc,
) connect.StreamingClientFunc {
	return next
}

func (i *operatorAuthInterceptor) WrapStreamingHandler(
	next connect.StreamingHandlerFunc,
) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		if err := i.authorize(ctx, conn.Spec().Procedure); err != nil {
			return err
		}
		return next(ctx, conn)
	}
}
