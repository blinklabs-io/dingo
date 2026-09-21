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

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/node"
)

// healthProbeServer is the liveness/readiness listener an operation outside
// `serve` keeps up.
//
// The bootstrap case is why it exists. `dingo mithril sync` runs as its own
// process, ahead of serve, and on mainnet it runs for hours; the shipped
// Dockerfile and docker-compose.yml both point a HEALTHCHECK at
// DINGO_HEALTH_PORT for that whole time. With no listener the probe is
// refused, and Swarm or ECS replaces the container after the start period and
// its retries -- long before the snapshot has finished downloading, and again
// on every replacement.
type healthProbeServer struct {
	server *http.Server
	errCh  <-chan error
	addr   string
}

// serveHealthProbe serves the probe on listener, binding cfg's health
// address itself when listener is nil, and returns nil when healthPort is 0
// and the operator has disabled it.
//
// The tip gap is deliberately nil: nothing here follows the chain, so the
// probe reports live and not ready, which is exactly the state an operator
// and an orchestrator should see during a bootstrap. Liveness is what keeps
// the container alive; readiness is what keeps it out of a load balancer.
//
// Binding and serving are separate steps because a port number and a bound
// socket are not the same fact. Between learning that a port is free and
// binding it, anything else on the host -- including this process asking the
// kernel for an arbitrary port -- can take it, and the probe then does not
// come up. A caller that already owns the socket passes the live listener
// instead, and cannot lose that race; the bound address is reported from the
// listener either way, so the log names the port actually being served
// rather than the one that was requested.
//
// A supplied listener is closed when healthPort is 0: the operator's opt-out
// still wins, and nothing would ever serve on it.
func serveHealthProbe(
	logger *slog.Logger,
	cfg *config.Config,
	component string,
	listener net.Listener,
) (*healthProbeServer, error) {
	server := node.NewHealthServer(cfg, nil)
	if server == nil {
		if listener != nil {
			_ = listener.Close()
		}
		return nil, nil
	}
	if listener == nil {
		addr := server.Addr
		bound, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf(
				"starting health listener on %s: %w",
				addr,
				err,
			)
		}
		listener = bound
	}
	actualAddr := listener.Addr().String()
	logger.Info(
		"serving health probes on "+actualAddr,
		"component", component,
	)
	server.Addr = actualAddr
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		if err := server.Serve(listener); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("health server: %w", err)
		}
	}()
	return &healthProbeServer{
		server: server,
		errCh:  errCh,
		addr:   actualAddr,
	}, nil
}

func (s *healthProbeServer) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

func (s *healthProbeServer) Err() <-chan error {
	if s == nil {
		errCh := make(chan error)
		close(errCh)
		return errCh
	}
	return s.errCh
}
