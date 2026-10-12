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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mcp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/apilistener"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Server encapsulates the MCP HTTP and SSE transport server.
type Server struct {
	lifecycleMu   sync.Mutex
	stopped       bool
	cleanupOnce   sync.Once
	config        ProviderConfig
	resolvedTLS   apiconfig.EffectiveTLS
	logger        *slog.Logger
	listenAddress string
	mcpServer     *mcp.Server
	openedDB      *sql.DB
	listener      *apilistener.Listener
	allowedOrigin []string
}

// NewServer creates a new MCP Server instance.
func NewServer(
	ctx context.Context,
	cfg ProviderConfig,
	deps ProviderDependencies,
	resolvedTLS apiconfig.EffectiveTLS,
	listenAddress string,
) (*Server, error) {
	host, _, err := net.SplitHostPort(listenAddress)
	if err != nil {
		return nil, fmt.Errorf("MCP listen address: %w", err)
	}
	if err := validateListenSecurity(host, cfg.AuthToken, resolvedTLS.Enabled); err != nil {
		return nil, err
	}
	mcpServer, openedDB, err := NewMCPServer(ctx, cfg, deps)
	if err != nil {
		return nil, fmt.Errorf("create MCP server: %w", err)
	}

	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Server{
		config:        cfg,
		resolvedTLS:   resolvedTLS,
		logger:        logger,
		listenAddress: listenAddress,
		mcpServer:     mcpServer,
		openedDB:      openedDB,
		listener:      apilistener.New("MCP API", logger),
		allowedOrigin: deps.CORSAllowedOrigins,
	}, nil
}

// handler constructs the HTTP mux and middleware stack.
func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()

	// Streamable HTTP transport handler (used by modern MCP clients and inspector)
	streamableHandler := mcp.NewStreamableHTTPHandler(
		func(_ *http.Request) *mcp.Server {
			return s.mcpServer
		},
		&mcp.StreamableHTTPOptions{
			Logger: s.logger,
		},
	)

	// SSE transport handler (legacy and inspector fallback)
	sseHandler := mcp.NewSSEHandler(func(_ *http.Request) *mcp.Server {
		return s.mcpServer
	}, nil)

	// Mount endpoints
	mux.Handle("/mcp", streamableHandler)
	mux.Handle("/mcp/", streamableHandler)
	mux.Handle("/sse", sseHandler)
	mux.Handle("/sse/", sseHandler)

	// Health endpoints
	healthHandler := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"service": "dingo-mcp",
		}); err != nil {
			s.logger.Warn("failed to encode health response", "err", err)
		}
	}
	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/health", healthHandler)

	// Default root handler routes to streamable HTTP for convenience
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "" {
			streamableHandler.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	})

	// Wrap mux with security middleware (auth, rate limiting, and CORS)
	return SecurityMiddleware(
		s.config.AuthToken,
		s.config.RateLimit,
		s.config.Burst,
		s.allowedOrigin,
		mux,
	)
}

// Start binds the socket and starts serving in a background goroutine.
func (s *Server) Start(ctx context.Context) error {
	s.lifecycleMu.Lock()
	if s.stopped {
		s.lifecycleMu.Unlock()
		return errors.New("MCP server stopped")
	}
	startDone, err := s.listener.BeginStart()
	s.lifecycleMu.Unlock()
	if err != nil {
		return err
	}
	defer s.listener.EndStart(startDone)

	server, bindDone, err := s.listener.Publish(func() *http.Server {
		return &http.Server{
			Addr:              s.listenAddress,
			Handler:           s.handler(),
			ReadHeaderTimeout: 30 * time.Second,
			ReadTimeout:       60 * time.Second,
			// SSE responses can live for the entire client session.
			WriteTimeout: 0,
			IdleTimeout:  120 * time.Second,
		}
	})
	if err != nil {
		return err
	}

	s.listener.Watch(ctx, server, stopHTTPServer)

	served, err := s.listener.Bind(server, bindDone, s.resolvedTLS)
	if err != nil {
		s.listener.Unpublish(server)
		return err
	}
	if !served {
		return nil
	}

	s.logger.Info("MCP API listener started on " + s.listenAddress)
	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	s.lifecycleMu.Lock()
	s.stopped = true
	s.lifecycleMu.Unlock()
	err := s.listener.Stop(ctx, stopHTTPServer)
	if err != nil {
		s.cleanupOnce.Do(func() { //nolint:contextcheck // Deferred shutdown cleanup must outlive the caller's timed-out ctx.
			// A timed-out Start still owns publication; wait for it before closing
			// the database, and prevent any later Start with the stopped gate.
			go func() {
				if cleanupErr := s.listener.Stop(context.WithoutCancel(ctx), func(_ context.Context, server *http.Server) error { return server.Close() }); cleanupErr != nil {
					s.logger.Error(
						"MCP deferred shutdown failed",
						"error",
						cleanupErr,
					)
					return
				}
				if s.openedDB != nil {
					_ = s.openedDB.Close()
				}
			}()
		})
		return err
	}
	if s.openedDB != nil {
		return s.openedDB.Close()
	}
	return nil
}

func stopHTTPServer(ctx context.Context, server *http.Server) error {
	if err := server.Shutdown(ctx); err != nil {
		return errors.Join(err, server.Close())
	}
	return nil
}
