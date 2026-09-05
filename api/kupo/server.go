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

package kupo

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiauth"
	"github.com/blinklabs-io/dingo/internal/apilistener"
	"github.com/blinklabs-io/dingo/internal/httpcors"
)

// Server is a Kupo-compatible HTTP API backed by Dingo's complete index.
type Server struct {
	config   Config
	logger   *slog.Logger
	node     KupoNode
	listener *apilistener.Listener
	verifier *apiauth.Verifier
	cancel   context.CancelFunc
}

// New creates a Kupo-compatible server.
func New(cfg Config, node KupoNode, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = ":1442"
	}
	logger = logger.With("component", "kupo")
	return &Server{
		config:   cfg,
		logger:   logger,
		node:     node,
		listener: apilistener.New("Kupo API", logger),
	}
}

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	for _, prefix := range []string{"", "/v1"} {
		registerRoutes(mux, prefix, s)
	}
	mux.HandleFunc("/", s.handleNotFound)

	const maxRequestBodyBytes int64 = 1 << 20
	limited := http.MaxBytesHandler(mux, maxRequestBodyBytes)
	authenticated := apiauth.Middleware(s.verifier)(limited)
	return httpcors.Handler(authenticated, httpcors.Config{
		AllowedOrigins: s.config.CORSAllowedOrigins,
	})
}

func registerRoutes(mux *http.ServeMux, prefix string, s *Server) {
	mux.HandleFunc("GET "+prefix+"/matches", s.handleMatches)
	mux.HandleFunc("GET "+prefix+"/matches/{pattern}", s.handleMatches)
	mux.HandleFunc(
		"GET "+prefix+"/matches/{payment}/{delegation}",
		s.handleMatches,
	)
	mux.HandleFunc(
		"DELETE "+prefix+"/matches/{pattern}",
		s.handleDeleteMatches,
	)
	mux.HandleFunc(
		"DELETE "+prefix+"/matches/{payment}/{delegation}",
		s.handleDeleteMatches,
	)
	mux.HandleFunc("GET "+prefix+"/datums/{datum_hash}", s.handleDatum)
	mux.HandleFunc("GET "+prefix+"/scripts/{script_hash}", s.handleScript)
	mux.HandleFunc("GET "+prefix+"/patterns", s.handlePatterns)
	mux.HandleFunc("PUT "+prefix+"/patterns", s.handlePutPatterns)
	mux.HandleFunc("GET "+prefix+"/patterns/{pattern}", s.handlePattern)
	mux.HandleFunc(
		"GET "+prefix+"/patterns/{payment}/{delegation}",
		s.handlePattern,
	)
	mux.HandleFunc("PUT "+prefix+"/patterns/{pattern}", s.handlePutPattern)
	mux.HandleFunc(
		"PUT "+prefix+"/patterns/{payment}/{delegation}",
		s.handlePutPattern,
	)
	mux.HandleFunc(
		"DELETE "+prefix+"/patterns/{pattern}",
		s.handleDeletePattern,
	)
	mux.HandleFunc(
		"DELETE "+prefix+"/patterns/{payment}/{delegation}",
		s.handleDeletePattern,
	)
	mux.HandleFunc("GET "+prefix+"/checkpoints", s.handleCheckpoints)
	mux.HandleFunc(
		"GET "+prefix+"/checkpoints/{slot_no}",
		s.handleCheckpoint,
	)
	mux.HandleFunc("GET "+prefix+"/metadata/{slot_no}", s.handleMetadata)
	mux.HandleFunc("GET "+prefix+"/health", s.handleHealth)
	mux.HandleFunc("GET "+prefix+"/metrics", s.handleMetrics)
}

// Start binds the configured HTTP listener and serves in the background.
func (s *Server) Start(ctx context.Context) error {
	serveCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	verifier, err := apiauth.NewVerifier(s.config.Auth)
	if err != nil {
		cancel()
		return fmt.Errorf("kupo: %w", err)
	}
	server, bindDone, err := s.listener.Publish(func() *http.Server {
		s.verifier = verifier
		return &http.Server{
			Addr:              s.config.ListenAddress,
			Handler:           s.handler(),
			ReadHeaderTimeout: 60 * time.Second,
			WriteTimeout:      0,
			IdleTimeout:       120 * time.Second,
		}
	})
	if err != nil {
		return err
	}
	go func() { //nolint:gosec // graceful shutdown intentionally outlives ctx
		<-serveCtx.Done()
		job, _ := s.listener.TakeIf(server)
		if job == nil {
			return
		}
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
		defer cancel()
		if err := s.listener.Shutdown(shutdownCtx, job, apilistener.Graceful); err != nil {
			s.logger.Error("failed to shutdown Kupo API server", "error", err)
		}
	}()
	served, err := s.listener.Bind(server, bindDone, s.config.TLS)
	if err != nil {
		cancel()
		s.listener.Unpublish(server)
		return err
	}
	if served {
		s.logger.Info("Kupo API listener started on " + s.config.ListenAddress)
	}
	return nil
}

// Stop gracefully stops the API and waits until its socket is released.
func (s *Server) Stop(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	job, inFlight := s.listener.Take()
	if job == nil {
		return s.listener.AwaitTeardown(ctx, inFlight)
	}
	return s.listener.Shutdown(ctx, job, apilistener.Graceful)
}
