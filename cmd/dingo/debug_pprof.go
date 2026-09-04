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
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
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
