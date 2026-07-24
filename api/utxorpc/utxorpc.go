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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	"connectrpc.com/grpcreflect"
	"github.com/blinklabs-io/dingo/internal/httpcors"
	"github.com/blinklabs-io/dingo/internal/tlsutil"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query/queryconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit/submitconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync/syncconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch/watchconnect"
)

// Default request size limits to prevent denial-of-service via
// unbounded request arrays.
const (
	DefaultMaxBlockRefs    = 100
	DefaultMaxUtxoKeys     = 1000
	DefaultMaxHistoryItems = 10000
	DefaultMaxDataKeys     = 1000
	DefaultServerTimeout   = time.Hour
)

type Utxorpc struct {
	server *http.Server
	config UtxorpcConfig
	mu     sync.Mutex
}

type UtxorpcConfig struct {
	Logger          *slog.Logger
	EventBus        UtxorpcEventBus
	LedgerState     UtxorpcLedgerState
	Mempool         UtxorpcMempool
	TlsCertFilePath string
	TlsKeyFilePath  string
	Host            string
	Port            uint

	// Request size limits (0 = use default)
	MaxBlockRefs int
	MaxUtxoKeys  int
	// MaxHistoryItems caps DumpHistory and SearchUtxos page size; omitted
	// max_items uses this cap.
	MaxHistoryItems int
	MaxDataKeys     int
	// ServerTimeout bounds long-running UTxO RPC handlers server-side
	// (0 = use default).
	ServerTimeout time.Duration
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
	if cfg.ServerTimeout <= 0 {
		cfg.ServerTimeout = DefaultServerTimeout
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
	u.mu.Lock()
	if u.server != nil {
		u.mu.Unlock()
		return errors.New("server already started")
	}
	mux := http.NewServeMux()
	compress1KB := connect.WithCompressMinBytes(1024)
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
	mux.Handle(queryPath, queryHandler)
	mux.Handle(submitPath, submitHandler)
	mux.Handle(syncPath, syncHandler)
	mux.Handle(watchPath, watchHandler)
	mux.Handle(
		grpchealth.NewHandler(
			grpchealth.NewStaticChecker(
				queryconnect.QueryServiceName,
				submitconnect.SubmitServiceName,
				syncconnect.SyncServiceName,
				watchconnect.WatchServiceName,
			),
			compress1KB,
		),
	)
	mux.Handle(
		grpcreflect.NewHandlerV1(
			grpcreflect.NewStaticReflector(
				queryconnect.QueryServiceName,
				submitconnect.SubmitServiceName,
				syncconnect.SyncServiceName,
				watchconnect.WatchServiceName,
			),
			compress1KB,
		),
	)
	mux.Handle(
		grpcreflect.NewHandlerV1Alpha(
			grpcreflect.NewStaticReflector(
				queryconnect.QueryServiceName,
				submitconnect.SubmitServiceName,
				syncconnect.SyncServiceName,
				watchconnect.WatchServiceName,
			),
			compress1KB,
		),
	)
	handler := httpcors.Handler(
		mux,
		httpcors.Config{
			AllowedOrigins: u.config.CORSAllowedOrigins,
		},
	)
	var server *http.Server
	if u.config.TlsCertFilePath != "" && u.config.TlsKeyFilePath != "" {
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

func unencryptedHTTP2Protocols() *http.Protocols {
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	return protocols
}

func (u *Utxorpc) Stop(ctx context.Context) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.server != nil {
		u.config.Logger.Debug("shutting down utxorpc gRPC server")
		if err := u.server.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown utxorpc gRPC server: %w", err)
		}
		u.server = nil
	}
	return nil
}

// startServer starts the HTTP server with deterministic error
// detection. It binds the listening socket and pre-loads any TLS
// keypair synchronously so port and certificate errors surface before
// returning, then serves in a background goroutine.
func (u *Utxorpc) startServer(server *http.Server) error {
	if (u.config.TlsCertFilePath != "") != (u.config.TlsKeyFilePath != "") {
		return errors.New(
			"failed to start utxorpc gRPC server: both tls cert and key must be specified",
		)
	}
	useTLS := u.config.TlsCertFilePath != "" && u.config.TlsKeyFilePath != ""
	serverType := "non-TLS"
	if useTLS {
		serverType = "TLS"
		cert, err := tls.LoadX509KeyPair(
			u.config.TlsCertFilePath,
			u.config.TlsKeyFilePath,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to load TLS keypair for utxorpc gRPC %s server: %w",
				serverType, err,
			)
		}
		server.TLSConfig = tlsutil.ServerConfig(server.TLSConfig)
		server.TLSConfig.Certificates = append(
			server.TLSConfig.Certificates,
			cert,
		)
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
