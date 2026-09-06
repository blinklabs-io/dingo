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
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	databaseconnect "github.com/blinklabs-io/bark/proto/v1alpha1/database/databasev1alpha1connect"
)

// destructiveDatabaseProcedures is the set of DatabaseService RPCs that
// mutate state, consume significant resources, or interfere with another
// caller's in-flight operation — as opposed to the entirely-read-only
// ArchiveService and this file's own readOnlyDatabaseProcedures. It exists
// for audit-log clarity and as the thing
// TestDestructiveDatabaseProcedures_CoversEveryGeneratedMethod keeps
// up to date with the proto's actual method list; operatorAuthInterceptor's
// runtime decision does NOT key off this map — see readOnlyDatabaseProcedures
// and authorize's doc comment for why.
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

// readOnlyDatabaseProcedures is the set of authenticated DatabaseService
// procedures that do not additionally require operator authorization. This is
// deliberately an allowlist: a procedure absent from both maps is treated as
// destructive and therefore requires an allowed operator fingerprint.
var readOnlyDatabaseProcedures = map[string]bool{
	databaseconnect.DatabaseServiceGetSnapshotStatusProcedure:       true,
	databaseconnect.DatabaseServiceListSnapshotsProcedure:           true,
	databaseconnect.DatabaseServiceGetRestoreStatusProcedure:        true,
	databaseconnect.DatabaseServiceListAvailableSnapshotsProcedure:  true,
	databaseconnect.DatabaseServiceGetTruncateStatusProcedure:       true,
	databaseconnect.DatabaseServiceStreamOperationProgressProcedure: true,
	databaseconnect.DatabaseServiceGetOperationHistoryProcedure:     true,
	databaseconnect.DatabaseServiceGetDatabaseInfoProcedure:         true,
}

// peerIdentity is what peerCertContextMiddleware extracts from a verified
// mTLS client certificate and stashes in the request context — just enough to
// authenticate every DatabaseService request, authorize destructive calls by
// fingerprint, and identify callers in audit logs.
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
// r.TLS.PeerCertificates alone is NOT a trustworthy signal: per
// crypto/tls's own documentation it holds whatever certificate chain the
// client presented, without regard to validity, whereas r.TLS.VerifiedChains
// is only populated once that chain has actually been verified against
// ClientCAs — so PeerCertificates is what a client sent, VerifiedChains is
// what the server actually trusts, and this function must key off the
// latter. This is deliberately not dependent on tls.VerifyClientCertIfGiven
// (see bark.go's startServer) rejecting a bad certificate at the handshake
// itself: that is Go's documented behavior for a certificate the server
// actually receives, but a client can fail to present its configured
// certificate at all for reasons unrelated to that check (e.g. no mutually
// acceptable signature scheme), in which case the connection proceeds as
// anonymous rather than erroring — checking VerifiedChains here handles
// both that case and a genuinely-rejected-then-somehow-reached-here case
// uniformly, without this middleware needing to know which occurred.
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

// errAnonymousDatabaseCall is returned for every DatabaseService call whose
// connection presented no verified client certificate.
var errAnonymousDatabaseCall = errors.New(
	"DatabaseService RPCs require a verified mTLS client certificate",
)

var errOperatorPermissionDenied = errors.New(
	"the authenticated client certificate is not authorized for destructive " +
		"DatabaseService operations",
)

func normalizeOperatorCertificateFingerprints(
	values []string,
) (map[string]struct{}, error) {
	ret := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := strings.ReplaceAll(strings.TrimSpace(value), ":", "")
		decoded, err := hex.DecodeString(normalized)
		if err != nil {
			return nil, fmt.Errorf("%q is not hexadecimal: %w", value, err)
		}
		if len(decoded) != sha256.Size {
			return nil, fmt.Errorf(
				"%q decodes to %d bytes, expected %d",
				value,
				len(decoded),
				sha256.Size,
			)
		}
		ret[hex.EncodeToString(decoded)] = struct{}{}
	}
	return ret, nil
}

// newOperatorAuthInterceptor enforces DatabaseService's two-stage policy:
// every call requires a verified client certificate, and every non-read-only
// call additionally requires its fingerprint in operatorFingerprints.
// Destructive calls are logged with the caller's certificate identity so an
// operator can audit who ran a Restore/Truncate/DeleteSnapshot/etc. after the
// fact; GetOperationHistory has no caller identity of its own to fall back on.
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
	readOnly map[string]bool,
	operatorFingerprints map[string]struct{},
) connect.Interceptor {
	return &operatorAuthInterceptor{
		logger:               logger,
		destructive:          destructive,
		readOnly:             readOnly,
		operatorFingerprints: operatorFingerprints,
	}
}

type operatorAuthInterceptor struct {
	logger               *slog.Logger
	destructive          map[string]bool
	readOnly             map[string]bool
	operatorFingerprints map[string]struct{}
}

// authorize is deliberately deny-by-default. Every procedure requires a
// verified certificate. A procedure absent from both classification maps —
// most plausibly a new RPC added without updating this file — additionally
// requires operator authorization exactly like a known destructive RPC.
// TestDestructiveDatabaseProcedures_CoversEveryGeneratedMethod keeps
// destructive/readOnly's classification of every current method accurate
// and pins that neither map is ever missing one, but this function's
// runtime behavior does not depend on that test having run: an
// unclassified procedure here is logged (so an operator notices and fixes
// the classification) and still requires operator authorization.
func (i *operatorAuthInterceptor) authorize(
	ctx context.Context,
	procedure string,
) error {
	id := peerIdentityFromContext(ctx)
	if !id.Verified {
		i.logger.Warn(
			"rejected anonymous call to DatabaseService RPC",
			"component", "bark",
			"procedure", procedure,
		)
		return connect.NewError(
			connect.CodeUnauthenticated,
			errAnonymousDatabaseCall,
		)
	}
	if i.readOnly[procedure] {
		return nil
	}
	if !i.destructive[procedure] {
		i.logger.Warn(
			"unclassified DatabaseService procedure treated as destructive (fail closed) — "+
				"add it to destructiveDatabaseProcedures or readOnlyDatabaseProcedures in bark/auth.go",
			"component",
			"bark",
			"procedure",
			procedure,
		)
	}
	if _, ok := i.operatorFingerprints[id.Fingerprint]; !ok {
		i.logger.Warn(
			"rejected destructive DatabaseService RPC from non-operator identity",
			"component",
			"bark",
			"procedure",
			procedure,
			"client_cn",
			id.CommonName,
			"client_cert_fingerprint",
			id.Fingerprint,
		)
		return connect.NewError(
			connect.CodePermissionDenied,
			errOperatorPermissionDenied,
		)
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

// WrapUnary runs authorize against the incoming request's procedure before
// calling the real handler — this is where every DatabaseService unary RPC
// (all six destructive ones included) actually gets gated.
func (i *operatorAuthInterceptor) WrapUnary(
	next connect.UnaryFunc,
) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if err := i.authorize(ctx, req.Spec().Procedure); err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

// WrapStreamingClient is a pass-through no-op: it governs outgoing calls
// this process makes as a Connect client, which this interceptor is never
// installed on — Bark only uses it server-side, to gate incoming requests.
func (i *operatorAuthInterceptor) WrapStreamingClient(
	next connect.StreamingClientFunc,
) connect.StreamingClientFunc {
	return next
}

// WrapStreamingHandler is WrapUnary's server-streaming counterpart, for
// completeness: no procedure in destructiveDatabaseProcedures streams
// today, but a future addition (or bark#17's proposed LifecycleService,
// reusing this same interceptor) might, and this ensures authorize runs
// for that case too rather than silently skipping it.
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
