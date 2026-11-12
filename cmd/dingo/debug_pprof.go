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

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	"github.com/blinklabs-io/dingo/internal/config"
)

type debugPprofServer struct {
	server *http.Server
	errCh  <-chan error
	addr   string
}

func newDebugPprofHTTPServer(cfg *config.Config) *http.Server {
	if cfg.DebugPort == 0 {
		return nil
	}
	debugMux := http.NewServeMux()
	debugMux.HandleFunc("/debug/pprof/", pprof.Index)
	debugMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	debugMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	debugMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	debugMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return &http.Server{
		Addr:              cfg.DebugListenAddress(),
		Handler:           debugMux,
		ReadHeaderTimeout: 60 * time.Second,
	}
}

func startDebugPprofServer(
	logger *slog.Logger,
	cfg *config.Config,
	component string,
) (*debugPprofServer, error) {
	server := newDebugPprofHTTPServer(cfg)
	if server == nil {
		return nil, nil
	}
	addr := server.Addr
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("starting debug listener on %s: %w", addr, err)
	}

	actualAddr := listener.Addr().String()
	logger.Info(
		"serving pprof debug endpoints on "+actualAddr,
		"component", component,
	)
	server.Addr = actualAddr
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		if err := server.Serve(listener); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("debug server: %w", err)
		}
	}()
	return &debugPprofServer{
		server: server,
		errCh:  errCh,
		addr:   actualAddr,
	}, nil
}

func (s *debugPprofServer) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

func (s *debugPprofServer) Err() <-chan error {
	if s == nil {
		errCh := make(chan error)
		close(errCh)
		return errCh
	}
	return s.errCh
}

// Swapped by tests: the block profile rate has no getter, so the applied
// rates are observed through these rather than through the runtime.
var (
	setMutexProfileFraction = runtime.SetMutexProfileFraction
	setBlockProfileRate     = runtime.SetBlockProfileRate
)

// applyDebugProfilingRates enables the runtime mutex and block profiles served
// by the debug listener. Only commands that start the listener call it. A rate
// is applied only when the listener is enabled and the rate is non-zero, so the
// runtime setters are never called with profiling off. The ignored-rates
// warning goes to stderr, not the logger, so a high configured log level cannot
// suppress it.
func applyDebugProfilingRates(
	logger *slog.Logger,
	stderr io.Writer,
	cfg *config.Config,
) {
	mutexFraction := cfg.DebugMutexProfileFraction
	blockRate := cfg.DebugBlockProfileRate
	if mutexFraction == 0 && blockRate == 0 {
		return
	}
	if cfg.DebugPort == 0 {
		fmt.Fprintf(
			stderr,
			"%s: mutex/block profile rates ignored: they are only readable "+
				"through the pprof listener, which is disabled (debugPort is 0)\n",
			programName,
		)
		return
	}
	if mutexFraction != 0 {
		setMutexProfileFraction(clampToInt(mutexFraction))
	}
	if blockRate != 0 {
		setBlockProfileRate(clampToInt(blockRate))
	}
	logger.Info(
		"pprof mutex/block profiling enabled",
		"mutex_profile_fraction", mutexFraction,
		"block_profile_rate_ns", blockRate,
	)
}

func clampToInt(v uint) int {
	if v > math.MaxInt {
		return math.MaxInt
	}
	return int(v)
}
