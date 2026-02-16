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
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	"connectrpc.com/grpcreflect"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query/queryconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit/submitconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync/syncconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch/watchconnect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// Default request size limits to prevent denial-of-service via
// unbounded request arrays.
const (
	DefaultMaxBlockRefs    = 100
	DefaultMaxUtxoKeys     = 1000
	DefaultMaxHistoryItems = 10000
	DefaultMaxDataKeys     = 1000
)

type Utxorpc struct {
	server *http.Server
	config UtxorpcConfig
	mu     sync.Mutex
}

type UtxorpcConfig struct {
	Logger          *slog.Logger
	EventBus        *event.EventBus
	LedgerState     *ledger.LedgerState
	Mempool         *mempool.Mempool
	TlsCertFilePath string
	TlsKeyFilePath  string
	Host            string
	Port            uint

	// Request size limits (0 = use default)
	MaxBlockRefs    int
	MaxUtxoKeys     int
	MaxHistoryItems int
	MaxDataKeys     int
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
	return &Utxorpc{
		config: cfg,
	}
}

func (u *Utxorpc) Start(ctx context.Context) error {
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
			Addr: fmt.Sprintf(
				"%s:%d",
				u.config.Host,
				u.config.Port,
			),
			Handler:           mux,
			ReadHeaderTimeout: 60 * time.Second,
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
			Addr: fmt.Sprintf(
				"%s:%d",
				u.config.Host,
				u.config.Port,
			),
			// Use h2c so we can serve HTTP/2 without TLS
			Handler:           h2c.NewHandler(mux, &http2.Server{}),
			ReadHeaderTimeout: 60 * time.Second,
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
	go func() {
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

// startServer starts the HTTP server with error detection
func (u *Utxorpc) startServer(server *http.Server) error {
	startErr := make(chan error, 1)
	go func() {
		var err error
		if u.config.TlsCertFilePath != "" && u.config.TlsKeyFilePath != "" {
			err = server.ListenAndServeTLS(
				u.config.TlsCertFilePath,
				u.config.TlsKeyFilePath,
			)
		} else {
			err = server.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			select {
			case startErr <- err:
			default:
				u.config.Logger.Error(
					"utxorpc gRPC server error",
					"error", err,
				)
			}
		}
	}()

	// Wait briefly for startup to succeed or fail
	// NOTE: 100ms timeout assumes startup errors occur quickly (e.g., port binding).
	// Delayed failures (e.g., certificate loading issues) may not be detected.
	select {
	case err := <-startErr:
		serverType := "non-TLS"
		if u.config.TlsCertFilePath != "" && u.config.TlsKeyFilePath != "" {
			serverType = "TLS"
		}
		return fmt.Errorf("failed to start utxorpc gRPC %s server: %w", serverType, err)
	case <-time.After(100 * time.Millisecond):
		// Assume startup succeeded if no error within 100ms
	}
	return nil
}
