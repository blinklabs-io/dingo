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
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/internal/apilistener"
	"github.com/blinklabs-io/dingo/internal/httpcors"
)

// responseWriteTimeout bounds how long one response write may block on a
// client that has stopped reading. It is refreshed before every write, so a
// steadily read stream of any length is unaffected.
const (
	responseWriteTimeout      = 30 * time.Second
	matchStreamTimeout        = 5 * time.Minute
	maxConcurrentMatchStreams = 3
	maxMatchStreamsPerClient  = 1
)

// Server is a Kupo-compatible HTTP API backed by Dingo's complete index.
type Server struct {
	config               Config
	logger               *slog.Logger
	node                 KupoNode
	listener             *apilistener.Listener
	responseWriteTimeout time.Duration
	matchStreamTimeout   time.Duration
	matchAdmission       *matchAdmission
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
		config:               cfg,
		logger:               logger,
		node:                 node,
		listener:             apilistener.New("Kupo API", logger),
		responseWriteTimeout: responseWriteTimeout,
		matchStreamTimeout:   matchStreamTimeout,
		matchAdmission: newMatchAdmission(
			maxConcurrentMatchStreams,
			maxMatchStreamsPerClient,
		),
	}
}

type matchAdmission struct {
	mu           sync.Mutex
	active       int
	activeClient map[string]int
	maxActive    int
	maxClient    int
}

func newMatchAdmission(maxActive, maxClient int) *matchAdmission {
	return &matchAdmission{
		activeClient: make(map[string]int),
		maxActive:    maxActive,
		maxClient:    maxClient,
	}
}

func (a *matchAdmission) acquire(ctx context.Context, client string) (func(), bool) {
	select {
	case <-ctx.Done():
		return nil, false
	default:
	}

	a.mu.Lock()
	if a.active >= a.maxActive || a.activeClient[client] >= a.maxClient {
		a.mu.Unlock()
		return nil, false
	}
	a.active++
	a.activeClient[client]++
	a.mu.Unlock()

	return func() {
		a.mu.Lock()
		a.active--
		a.activeClient[client]--
		if a.activeClient[client] == 0 {
			delete(a.activeClient, client)
		}
		a.mu.Unlock()
	}, true
}

func matchClient(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

// corsExposeHeaders lists every response header this API sets that is not
// CORS-safelisted. A browser hides any other header from scripts, so a client
// on an allowed origin could not read the checkpoint, ETag or block hash the
// Kupo contract returns.
var corsExposeHeaders = []string{
	"ETag",
	"Retry-After",
	"X-Block-Header-Hash",
	"X-Most-Recent-Checkpoint",
}

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	for _, prefix := range []string{"", "/v1"} {
		registerRoutes(mux, prefix, s)
	}
	mux.HandleFunc("/", s.handleNotFound)

	const maxRequestBodyBytes int64 = 1 << 20
	limited := http.MaxBytesHandler(s.boundWrites(mux), maxRequestBodyBytes)
	return httpcors.Handler(limited, httpcors.Config{
		AllowedOrigins: s.config.CORSAllowedOrigins,
		ExposeHeaders:  corsExposeHeaders,
	})
}

func (s *Server) boundWrites(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&progressResponseWriter{
			ResponseWriter: w,
			controller:     http.NewResponseController(w),
			timeout:        s.responseWriteTimeout,
			logger:         s.logger,
		}, r)
	})
}

// progressResponseWriter refreshes the connection write deadline before each
// body write. A response holds its read snapshot until the handler returns,
// and a client that stops reading would otherwise block a write, and keep
// that snapshot, for as long as the connection stays open. The last deadline
// stays armed while net/http flushes after the handler returns; net/http
// clears it before reusing the connection.
type progressResponseWriter struct {
	http.ResponseWriter
	controller *http.ResponseController
	timeout    time.Duration
	logger     *slog.Logger
}

func (w *progressResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *progressResponseWriter) Write(p []byte) (int, error) {
	if err := w.controller.SetWriteDeadline(
		time.Now().Add(w.timeout),
	); err != nil && !errors.Is(err, http.ErrNotSupported) {
		w.logger.Debug("could not bound Kupo response write", "error", err)
	}
	return w.ResponseWriter.Write(p)
}

func registerRoutes(mux interface {
	HandleFunc(string, func(http.ResponseWriter, *http.Request))
}, prefix string, s *Server,
) {
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
	startDone, err := s.listener.BeginStart()
	if err != nil {
		return err
	}
	defer s.listener.EndStart(startDone)

	server, bindDone, err := s.listener.Publish(func() *http.Server {
		return &http.Server{
			Addr:              s.config.ListenAddress,
			Handler:           s.handler(),
			ReadHeaderTimeout: 60 * time.Second,
			ReadTimeout:       60 * time.Second,
			// WriteTimeout deliberately 0, as in api/utxorpc: /matches
			// streams an unbounded result set, so any fixed deadline
			// truncates a legitimately large response. boundWrites instead
			// re-arms a per-write deadline, so a client that stops reading
			// releases its read snapshot within responseWriteTimeout. The
			// read-snapshot admission cap in
			// database.NewReadSnapshotContext, one below the read pool size,
			// still keeps the last connection free for rollback while
			// clients stream.
			WriteTimeout: 0,
			IdleTimeout:  120 * time.Second,
		}
	})
	if err != nil {
		return err
	}
	s.listener.Watch(ctx, server, apilistener.Graceful)
	served, err := s.listener.Bind(server, bindDone, s.config.TLS)
	if err != nil {
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
	return s.listener.Stop(ctx, apilistener.Graceful)
}
