// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	"connectrpc.com/grpcreflect"
	"github.com/blinklabs-io/dingo/internal/apiauth"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/httpcors"
	"github.com/blinklabs-io/dingo/internal/tlsutil"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query/queryconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit/submitconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync/syncconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch/watchconnect"
	betaquery "github.com/utxorpc/go-codegen/utxorpc/v1beta/query"
	betaqueryconnect "github.com/utxorpc/go-codegen/utxorpc/v1beta/query/queryconnect"
	betasubmitconnect "github.com/utxorpc/go-codegen/utxorpc/v1beta/submit/submitconnect"
	betasyncconnect "github.com/utxorpc/go-codegen/utxorpc/v1beta/sync/syncconnect"
	betawatchconnect "github.com/utxorpc/go-codegen/utxorpc/v1beta/watch/watchconnect"
)

// Default request size limits to prevent denial-of-service via
// unbounded request arrays.
const (
	DefaultMaxBlockRefs    = 100
	DefaultMaxUtxoKeys     = 1000
	DefaultMaxHistoryItems = 10000
	DefaultMaxDataKeys     = 1000
	// DefaultMaxPoolFilter caps ReadState's pool_keyhashes filter. Matching
	// the other key-list caps: a caller wanting every pool sends an empty
	// filter, so a long explicit list is not the way to ask for the whole
	// distribution.
	DefaultMaxPoolFilter = 1000
	DefaultServerTimeout = time.Hour
	// DefaultShutdownTimeout bounds Stop's graceful http.Server.Shutdown
	// before it escalates to a hard Close, matching midnight/server's
	// identical ShutdownTimeout/defaultShutdownTimeout pattern.
	DefaultShutdownTimeout = 30 * time.Second
)

type Utxorpc struct {
	server   *http.Server
	config   UtxorpcConfig
	verifier *apiauth.Verifier
	mu       sync.Mutex
}

type UtxorpcConfig struct {
	Logger      *slog.Logger
	EventBus    UtxorpcEventBus
	LedgerState UtxorpcLedgerState
	Mempool     UtxorpcMempool
	// TLS and Auth are the resolved (merged, validated) equivalents of
	// what was previously TlsCertFilePath/TlsKeyFilePath fields here --
	// see ProviderConfig's doc comment and ARCHITECTURE.md's "API
	// security" section.
	TLS  apiconfig.EffectiveTLS
	Auth apiconfig.EffectiveAuth
	Host string
	Port uint

	// Request size limits (0 = use default)
	MaxBlockRefs int
	MaxUtxoKeys  int
	// MaxHistoryItems caps DumpHistory and SearchUtxos page size; omitted
	// max_items uses this cap.
	MaxHistoryItems int
	MaxDataKeys     int
	// MaxPoolFilter caps ReadState's pool_keyhashes filter length.
	MaxPoolFilter int
	// ServerTimeout bounds long-running UTxO RPC handlers server-side
	// (0 = use default).
	ServerTimeout time.Duration
	// ShutdownTimeout bounds Stop's graceful http.Server.Shutdown before it
	// escalates to a hard Close (0 = use default). Watch* RPCs are
	// unbounded streams, so a connected client can otherwise keep
	// Shutdown blocked indefinitely.
	ShutdownTimeout time.Duration
	// CORSAllowedOrigins configures Access-Control-Allow-Origin.
	// Empty disables CORS.
	CORSAllowedOrigins []string
}

func NewUtxorpc(cfg UtxorpcConfig) *Utxorpc {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	cfg.Logger = cfg.Logger.With("component", "utxorpc")
	if cfg.Host == "" {
		cfg.Host = "0.0.0.0"
	}
	if cfg.Port == 0 {
		cfg.Port = 9090
	}
	if cfg.MaxBlockRefs <= 0 {
		cfg.MaxBlockRefs = DefaultMaxBlockRefs
	}
	if cfg.MaxUtxoKeys <= 0 {
		cfg.MaxUtxoKeys = DefaultMaxUtxoKeys
	}
	if cfg.MaxHistoryItems <= 0 {
		cfg.MaxHistoryItems = DefaultMaxHistoryItems
	}
	if cfg.MaxDataKeys <= 0 {
		cfg.MaxDataKeys = DefaultMaxDataKeys
	}
	if cfg.MaxPoolFilter <= 0 {
		cfg.MaxPoolFilter = DefaultMaxPoolFilter
	}
	if cfg.ServerTimeout <= 0 {
		cfg.ServerTimeout = DefaultServerTimeout
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = DefaultShutdownTimeout
	}
	return &Utxorpc{
		config: cfg,
	}
}

// isNilInterface reports whether v is nil at either the interface level
// (untyped nil) or the underlying pointer level (typed nil such as
// (*T)(nil) stored in an interface). Calling methods on either kind of
// nil interface value causes a runtime panic, so both must be rejected.
func isNilInterface(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() { //nolint:exhaustive
	case reflect.Chan, reflect.Func, reflect.Interface,
		reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	}
	return false
}

func (u *Utxorpc) Start(ctx context.Context) error {
	if isNilInterface(u.config.EventBus) {
		return errors.New("utxorpc: EventBus is required")
	}
	// Typed-nil guard for optional deps — untyped nil is allowed at startup
	// (handlers check per-request), but a typed nil (*T)(nil) stored in the
	// interface field would bypass handler nil-checks and cause a panic.
	if u.config.LedgerState != nil && isNilInterface(u.config.LedgerState) {
		return errors.New("utxorpc: LedgerState must not be a typed nil")
	}
	if u.config.Mempool != nil && isNilInterface(u.config.Mempool) {
		return errors.New("utxorpc: Mempool must not be a typed nil")
	}
	// Built before the mux so newServeMux can install the shared
	// credential-verification interceptor (internal/apiauth) on every
	// Connect/gRPC handler it registers, including health and reflection.
	verifier, err := apiauth.NewVerifier(u.config.Auth)
	if err != nil {
		return fmt.Errorf("utxorpc: %w", err)
	}
	u.mu.Lock()
	if u.server != nil {
		u.mu.Unlock()
		return errors.New("server already started")
	}
	u.verifier = verifier
	// CORS must wrap authentication, not the reverse: httpcors.Handler
	// fully answers an OPTIONS preflight itself and never calls the
	// handler it wraps for one, so browsers -- which never attach
	// Authorization to a preflight request -- never need a credential to
	// pass CORS negotiation. Every other request, including a
	// non-preflight OPTIONS, still reaches the mux (and so the
	// per-procedure auth interceptor) normally. See internal/apiauth's
	// Middleware doc comment for the HTTP-side statement of the same
	// ordering rule.
	handler := httpcors.Handler(
		u.newServeMux(),
		httpcors.Config{
			AllowedOrigins: u.config.CORSAllowedOrigins,
		},
	)
	var server *http.Server
	if u.config.TLS.Enabled {
		u.config.Logger.Info(
			fmt.Sprintf(
				"starting utxorpc gRPC TLS listener on %s:%d",
				u.config.Host,
				u.config.Port,
			),
		)
		server = &http.Server{
			Addr: net.JoinHostPort(
				u.config.Host,
				strconv.FormatUint(uint64(u.config.Port), 10),
			),
			Handler:           handler,
			ReadHeaderTimeout: 60 * time.Second,
			ReadTimeout:       60 * time.Second,
			IdleTimeout:       120 * time.Second,
			// WriteTimeout deliberately 0 for gRPC streaming
			// endpoints (FollowTip, WatchTx, WaitForTx).
		}
	} else {
		u.config.Logger.Info(
			fmt.Sprintf(
				"starting utxorpc gRPC listener on %s:%d",
				u.config.Host,
				u.config.Port,
			),
		)
		server = &http.Server{
			Addr: net.JoinHostPort(
				u.config.Host,
				strconv.FormatUint(uint64(u.config.Port), 10),
			),
			Handler:           handler,
			Protocols:         unencryptedHTTP2Protocols(),
			ReadHeaderTimeout: 60 * time.Second,
			ReadTimeout:       60 * time.Second,
			IdleTimeout:       120 * time.Second,
			// WriteTimeout deliberately 0 for gRPC streaming
			// endpoints (FollowTip, WatchTx, WaitForTx).
		}
	}
	u.server = server
	u.mu.Unlock()

	// Start the server
	if err := u.startServer(server); err != nil {
		u.mu.Lock()
		u.server = nil
		u.mu.Unlock()
		return err
	}

	// Monitor context for cancellation and shutdown server
	go func() { //nolint:gosec // G118: goroutine intentionally outlives ctx to perform graceful shutdown
		<-ctx.Done()
		u.mu.Lock()
		if u.server != nil {
			u.config.Logger.Debug(
				"context cancelled, shutting down utxorpc gRPC server",
			)
			//nolint:contextcheck // shutdownCtx is intentionally created from background to allow shutdown to complete even if ctx is cancelled
			shutdownCtx, cancel := context.WithTimeout(
				context.Background(),
				30*time.Second,
			)
			defer cancel()
			if err := u.server.Shutdown(shutdownCtx); err != nil { //nolint:contextcheck // shutdownCtx is intentionally created from background to allow shutdown to complete even if ctx is cancelled
				u.config.Logger.Error(
					"failed to shutdown utxorpc gRPC server on context cancellation",
					"error",
					err,
				)
			}
			u.server = nil
		}
		u.mu.Unlock()
	}()

	return nil
}

// newServeMux builds the complete routing table this listener serves: both API
// versions of every service, health checking, and gRPC reflection. It is the
// single wiring site, so tests can exercise the real routes rather than a
// reconstruction of them.
func (u *Utxorpc) newServeMux() *http.ServeMux {
	mux := http.NewServeMux()
	compress1KB := connect.WithCompressMinBytes(1024)
	// When authentication is enabled, every Connect/gRPC handler this mux
	// registers -- including health and reflection -- requires a valid
	// credential; there is no separate unauthenticated allowlist for
	// those two, unlike CORS preflight (see Start's doc comment).
	if u.verifier != nil {
		compress1KB = connect.WithOptions(
			compress1KB,
			connect.WithInterceptors(apiauth.Interceptor(u.verifier)),
		)
	}
	queryPath, queryHandler := queryconnect.NewQueryServiceHandler(
		&queryServiceServer{utxorpc: u},
		compress1KB,
	)
	submitPath, submitHandler := submitconnect.NewSubmitServiceHandler(
		&submitServiceServer{utxorpc: u},
		compress1KB,
	)
	syncPath, syncHandler := syncconnect.NewSyncServiceHandler(
		&syncServiceServer{utxorpc: u},
		compress1KB,
	)
	watchPath, watchHandler := watchconnect.NewWatchServiceHandler(
		&watchServiceServer{utxorpc: u},
		compress1KB,
	)
	// v1beta uses the same wire-compatible messages as v1alpha, but with
	// versioned protobuf descriptors. Reuse the handlers after rewriting only
	// the service path so both API versions can share this listener. This also
	// preserves streaming behavior without duplicating every service method.
	betaQueryPath := "/" + betaqueryconnect.QueryServiceName + "/"
	betaQueryHandler := betaVersionedQueryHandler(
		u,
		queryPath,
		queryHandler,
		betaQueryPath,
		compress1KB,
	)
	betaSubmitPath := "/" + betasubmitconnect.SubmitServiceName + "/"
	betaSubmitHandler := rewriteVersionHandler(
		submitHandler,
		betaSubmitPath,
		submitPath,
	)
	betaSyncPath := "/" + betasyncconnect.SyncServiceName + "/"
	betaSyncHandler := rewriteVersionHandler(
		syncHandler,
		betaSyncPath,
		syncPath,
	)
	betaWatchPath := "/" + betawatchconnect.WatchServiceName + "/"
	betaWatchHandler := rewriteVersionHandler(
		watchHandler,
		betaWatchPath,
		watchPath,
	)
	mux.Handle(queryPath, queryHandler)
	mux.Handle(submitPath, submitHandler)
	mux.Handle(syncPath, syncHandler)
	mux.Handle(watchPath, watchHandler)
	mux.Handle(betaQueryPath, betaQueryHandler)
	mux.Handle(betaSubmitPath, betaSubmitHandler)
	mux.Handle(betaSyncPath, betaSyncHandler)
	mux.Handle(betaWatchPath, betaWatchHandler)
	// One list drives health checking and both reflection versions so the
	// served set cannot drift between them. The v1alpha reflection service is
	// the older wire protocol, not an older API surface: clients that speak
	// grpc.reflection.v1alpha.ServerReflection must still discover the v1beta
	// services this listener serves.
	serviceNames := servedServiceNames()
	mux.Handle(
		grpchealth.NewHandler(
			grpchealth.NewStaticChecker(serviceNames...),
			compress1KB,
		),
	)
	reflector := grpcreflect.NewStaticReflector(serviceNames...)
	mux.Handle(grpcreflect.NewHandlerV1(reflector, compress1KB))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector, compress1KB))
	return mux
}

// servedServiceNames returns every gRPC service name this listener serves, in
// both API versions. It is the single source of truth for the health checker
// and for both reflection handlers.
func servedServiceNames() []string {
	return []string{
		queryconnect.QueryServiceName,
		betaqueryconnect.QueryServiceName,
		submitconnect.SubmitServiceName,
		betasubmitconnect.SubmitServiceName,
		syncconnect.SyncServiceName,
		betasyncconnect.SyncServiceName,
		watchconnect.WatchServiceName,
		betawatchconnect.WatchServiceName,
	}
}

// rewriteVersionHandler adapts a v1beta service path to the corresponding
// v1alpha handler. The alpha and beta protobuf messages have identical wire
// fields for the implemented methods, so Connect, gRPC, gRPC-Web, and streaming
// requests can all be handled by the existing implementation.
func rewriteVersionHandler(
	httpHandler http.Handler,
	fromPrefix, toPrefix string,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clone := r.Clone(r.Context())
		urlCopy := *r.URL
		urlCopy.Path = strings.Replace(
			urlCopy.Path,
			fromPrefix,
			toPrefix,
			1,
		)
		urlCopy.RawPath = ""
		clone.URL = &urlCopy
		httpHandler.ServeHTTP(w, clone)
	})
}

// betaVersionedQueryHandler handles the v1beta-only ReadState method while
// routing the methods implemented by Dingo through the alpha handler.
func betaVersionedQueryHandler(
	u *Utxorpc,
	alphaPath string,
	alphaHandler http.Handler,
	betaPath string,
	options connect.HandlerOption,
) http.Handler {
	queryFile := betaquery.File_utxorpc_v1beta_query_query_proto
	if queryFile == nil {
		panic("utxorpc: missing v1beta query descriptor")
	}
	queryService := queryFile.Services().ByName("QueryService")
	if queryService == nil {
		panic("utxorpc: missing v1beta QueryService descriptor")
	}
	readStateMethod := queryService.Methods().ByName("ReadState")
	if readStateMethod == nil {
		panic("utxorpc: missing v1beta QueryService.ReadState descriptor")
	}
	betaQueryServer := &betaQueryServiceServer{utxorpc: u}
	readStateHandler := connect.NewUnaryHandler(
		betaqueryconnect.QueryServiceReadStateProcedure,
		betaQueryServer.ReadState,
		connect.WithSchema(readStateMethod),
		connect.WithHandlerOptions(options),
	)
	// Build the rewrite handler once and reuse it across requests, matching
	// the beta submit/sync/watch handlers. rewriteVersionHandler is a pure
	// function of its arguments, so hoisting it out of the request closure is
	// behavior-preserving and avoids a per-request allocation.
	rewrittenQueryHandler := rewriteVersionHandler(
		alphaHandler,
		betaPath,
		alphaPath,
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == betaqueryconnect.QueryServiceReadStateProcedure {
			readStateHandler.ServeHTTP(w, r)
			return
		}
		rewrittenQueryHandler.ServeHTTP(w, r)
	})
}

func unencryptedHTTP2Protocols() *http.Protocols {
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	return protocols
}

// Stop shuts down the server, escalating to a hard Close if it does not
// complete within u.config.ShutdownTimeout (or the caller ctx's own
// deadline, if sooner), and also escalates immediately if ctx is cancelled
// (even when ctx carries no deadline at all), so a caller that wants to
// give up early is never stuck waiting out the full ShutdownTimeout.
// WatchTx/WatchMempool are unbounded streaming RPCs, so a connected client
// can otherwise keep http.Server.Shutdown blocked indefinitely, hanging any
// live database restore/truncate quiesce that waits on Stop -- matching
// midnight/server's identical Stop/gracefulStop pattern for a grpc.Server.
func (u *Utxorpc) Stop(ctx context.Context) error {
	u.mu.Lock()
	server := u.server
	u.server = nil
	u.mu.Unlock()

	if server == nil {
		return nil
	}

	timeout := u.config.ShutdownTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 {
			timeout = min(timeout, remaining)
		} else {
			timeout = 0
		}
	}

	u.config.Logger.Debug("shutting down utxorpc gRPC server")
	// Shutdown itself is given a ctx that never expires (not shutdownCtx
	// below): it races that ctx internally, and racing it a second time
	// here too would be nondeterministic about which timeout error wins --
	// sometimes returning Shutdown's own ctx-expired error instead of
	// actually escalating to the hard Close this exists for. A plain timer
	// avoids that, matching midnight/server's gracefulStop, which for the
	// same reason never passes a ctx into grpc.Server.GracefulStop either.
	shutdownErr := make(chan error, 1)
	//nolint:gosec,contextcheck // G118: intentionally outlives ctx, see comment above
	go func() {
		shutdownErr <- server.Shutdown(context.Background())
	}()

	select {
	case err := <-shutdownErr:
		if err != nil {
			return fmt.Errorf("failed to shutdown utxorpc gRPC server: %w", err)
		}
		return nil
	case <-time.After(timeout):
		u.config.Logger.Warn(
			"utxorpc gRPC graceful shutdown timed out; forcing close",
			"timeout", timeout,
		)
		return forceCloseUtxorpc(server, shutdownErr)
	case <-ctx.Done():
		// The caller gave up before ShutdownTimeout (or its own deadline)
		// elapsed -- e.g. a live database restore/truncate quiesce that
		// wants to abandon a slow shutdown quickly. Escalate immediately
		// instead of continuing to wait out the timer above.
		u.config.Logger.Warn(
			"utxorpc gRPC graceful shutdown cancelled by caller context; forcing close",
			"error",
			ctx.Err(),
		)
		return forceCloseUtxorpc(server, shutdownErr)
	}
}

// forceCloseUtxorpc hard-closes server after a graceful Shutdown failed to
// finish in time -- whether because ShutdownTimeout (or the caller's own
// deadline) elapsed or because the caller's ctx was cancelled early -- then
// drains shutdownErr so the Shutdown goroutine started in Stop never leaks.
func forceCloseUtxorpc(server *http.Server, shutdownErr <-chan error) error {
	if err := server.Close(); err != nil {
		return fmt.Errorf("failed to force-close utxorpc gRPC server: %w", err)
	}
	<-shutdownErr
	return nil
}

// startServer starts the HTTP server with deterministic error
// detection. It binds the listening socket and pre-loads any TLS
// keypair synchronously so port and certificate errors surface before
// returning, then serves in a background goroutine.
func (u *Utxorpc) startServer(server *http.Server) error {
	useTLS := u.config.TLS.Enabled
	serverType := "non-TLS"
	if useTLS {
		serverType = "TLS"
		if err := tlsutil.ConfigureServerTLS(
			server,
			u.config.TLS.CertFilePath,
			u.config.TLS.KeyFilePath,
		); err != nil {
			return fmt.Errorf(
				"failed to load TLS keypair for utxorpc gRPC %s server: %w",
				serverType, err,
			)
		}
	}
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("failed to start utxorpc gRPC %s server: %w",
			serverType, err)
	}
	go func() {
		var serveErr error
		if useTLS {
			serveErr = server.ServeTLS(
				ln,
				"",
				"",
			)
		} else {
			serveErr = server.Serve(ln)
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			u.config.Logger.Error(
				"utxorpc gRPC server error",
				"error", serveErr,
			)
		}
	}()
	return nil
}
