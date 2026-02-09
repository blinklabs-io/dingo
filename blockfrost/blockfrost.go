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

package blockfrost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// Blockfrost is the Blockfrost-compatible REST API server.
type Blockfrost struct {
	config     BlockfrostConfig
	logger     *slog.Logger
	node       BlockfrostNode
	httpServer *http.Server
	mu         sync.Mutex
}

// New creates a new Blockfrost API server instance.
func New(
	cfg BlockfrostConfig,
	node BlockfrostNode,
	logger *slog.Logger,
) *Blockfrost {
	if logger == nil {
		logger = slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		)
	}
	logger = logger.With("component", "blockfrost")
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = ":3000"
	}
	return &Blockfrost{
		config: cfg,
		logger: logger,
		node:   node,
	}
}

// Start starts the HTTP server in a background goroutine.
func (b *Blockfrost) Start(
	ctx context.Context,
) error {
	b.mu.Lock()
	if b.httpServer != nil {
		b.mu.Unlock()
		return errors.New("server already started")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", b.handleRoot)
	mux.HandleFunc("GET /health", b.handleHealth)
	mux.HandleFunc(
		"GET /api/v0/blocks/latest",
		b.handleLatestBlock,
	)
	mux.HandleFunc(
		"GET /api/v0/blocks/latest/txs",
		b.handleLatestBlockTxs,
	)
	mux.HandleFunc(
		"GET /api/v0/epochs/latest",
		b.handleLatestEpoch,
	)
	mux.HandleFunc(
		"GET /api/v0/epochs/latest/parameters",
		b.handleLatestEpochParams,
	)
	mux.HandleFunc(
		"GET /api/v0/network",
		b.handleNetwork,
	)

	server := &http.Server{
		Addr:              b.config.ListenAddress,
		Handler:           mux,
		ReadHeaderTimeout: 60 * time.Second,
	}
	b.httpServer = server
	b.mu.Unlock()

	// Start the server with deterministic error detection
	if err := b.startServer(server); err != nil {
		b.mu.Lock()
		b.httpServer = nil
		b.mu.Unlock()
		return err
	}

	b.logger.Info(
		"Blockfrost API listener started on " +
			b.config.ListenAddress,
	)

	// Monitor context for cancellation
	go func() {
		<-ctx.Done()
		b.mu.Lock()
		srv := b.httpServer
		b.httpServer = nil
		b.mu.Unlock()

		if srv != nil {
			b.logger.Debug(
				"context cancelled, shutting down " +
					"Blockfrost API server",
			)
			//nolint:contextcheck
			shutdownCtx, cancel := context.WithTimeout(
				context.Background(),
				30*time.Second,
			)
			defer cancel()
			//nolint:contextcheck
			if err := srv.Shutdown(
				shutdownCtx,
			); err != nil {
				b.logger.Error(
					"failed to shutdown Blockfrost "+
						"API server on context "+
						"cancellation",
					"error", err,
				)
			}
		}
	}()

	return nil
}

// Stop gracefully shuts down the HTTP server.
func (b *Blockfrost) Stop(
	ctx context.Context,
) error {
	b.mu.Lock()
	srv := b.httpServer
	b.httpServer = nil
	b.mu.Unlock()

	if srv != nil {
		b.logger.Debug(
			"shutting down Blockfrost API server",
		)
		if err := srv.Shutdown(ctx); err != nil {
			return fmt.Errorf(
				"failed to shutdown Blockfrost API "+
					"server: %w",
				err,
			)
		}
	}
	return nil
}

// startServer starts the HTTP server with deterministic
// error detection. It binds the listening socket first so
// port conflicts are detected immediately, then serves in
// a background goroutine.
func (b *Blockfrost) startServer(
	server *http.Server,
) error {
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf(
			"failed to listen for Blockfrost API "+
				"server: %w",
			err,
		)
	}
	go func() {
		if err := server.Serve(ln); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			b.logger.Error(
				"Blockfrost API server error",
				"error", err,
			)
		}
	}()
	return nil
}
