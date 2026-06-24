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

// Package server runs the MidnightState gRPC service. It is a native
// google.golang.org/grpc server (not ConnectRPC) so that clients written
// against the Acropolis tonic service are byte-for-byte compatible. This
// package is the server scaffold: it registers a stub MidnightState service
// whose RPCs all return Unimplemented; the real handlers are added separately.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/midnight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

const (
	defaultHost            = "0.0.0.0"
	defaultPort            = 50051
	defaultShutdownTimeout = 30 * time.Second
)

// Config holds the configuration for the Midnight gRPC server.
type Config struct {
	Logger *slog.Logger
	// Host and Port are the gRPC listen address. Defaults to 0.0.0.0:50051.
	Host string
	Port uint
	// TLSCertFilePath and TLSKeyFilePath enable TLS when both are set. When
	// both are empty the server listens without TLS. Setting only one is an
	// error.
	TLSCertFilePath string
	TLSKeyFilePath  string
	// ShutdownTimeout bounds GracefulStop before escalating to a hard Stop.
	// Defaults to 30s.
	ShutdownTimeout time.Duration
}

// Server runs the MidnightState gRPC service over its own listener.
type Server struct {
	mu         sync.Mutex // protects grpcServer and health
	config     Config
	grpcServer *grpc.Server
	health     *health.Server
}

// New validates cfg and returns a Server. It does not bind or serve; call
// Start for that.
func New(cfg Config) (*Server, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	cfg.Logger = cfg.Logger.With("component", "midnight-grpc")
	if cfg.Host == "" {
		cfg.Host = defaultHost
	}
	if cfg.Port == 0 {
		cfg.Port = defaultPort
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}
	if (cfg.TLSCertFilePath != "") != (cfg.TLSKeyFilePath != "") {
		return nil, errors.New(
			"midnight grpc: both tls cert and key must be specified",
		)
	}
	return &Server{config: cfg}, nil
}

// Start binds the listener and serves the gRPC server in a background
// goroutine. Binding and any TLS keypair load happen synchronously so that
// address-in-use and certificate errors surface before Start returns. A
// goroutine watches ctx and performs a bounded graceful shutdown when it is
// cancelled.
func (s *Server) Start(ctx context.Context) error {
	useTLS := s.config.TLSCertFilePath != "" && s.config.TLSKeyFilePath != ""

	var opts []grpc.ServerOption
	serverType := "non-TLS"
	if useTLS {
		serverType = "TLS"
		creds, err := credentials.NewServerTLSFromFile(
			s.config.TLSCertFilePath,
			s.config.TLSKeyFilePath,
		)
		if err != nil {
			return fmt.Errorf(
				"midnight grpc: load TLS keypair: %w",
				err,
			)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	addr := net.JoinHostPort(
		s.config.Host,
		strconv.FormatUint(uint64(s.config.Port), 10),
	)

	s.mu.Lock()
	if s.grpcServer != nil {
		s.mu.Unlock()
		return errors.New("midnight grpc: server already started")
	}

	grpcServer := grpc.NewServer(opts...)
	// Stub MidnightState service: every RPC returns Unimplemented until the
	// real handlers land.
	midnight.RegisterMidnightStateServer(grpcServer, &stubService{})

	// Health service reporting SERVING for the overall server ("") and the
	// MidnightState service by name.
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus(
		midnight.MidnightState_ServiceDesc.ServiceName,
		healthpb.HealthCheckResponse_SERVING,
	)

	// Reflection so grpcurl and similar tooling work out of the box.
	reflection.Register(grpcServer)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		s.mu.Unlock()
		grpcServer.Stop()
		return fmt.Errorf("midnight grpc: listen on %s: %w", addr, err)
	}

	s.grpcServer = grpcServer
	s.health = healthSrv
	s.mu.Unlock()

	s.config.Logger.Info(
		fmt.Sprintf(
			"starting midnight gRPC %s listener on %s",
			serverType,
			addr,
		),
	)

	go func() {
		if serveErr := grpcServer.Serve(ln); serveErr != nil &&
			!errors.Is(serveErr, grpc.ErrServerStopped) {
			s.config.Logger.Error(
				"midnight gRPC server error",
				"error",
				serveErr,
			)
		}
	}()

	go func() { //nolint:gosec // goroutine intentionally outlives ctx to perform graceful shutdown
		<-ctx.Done()
		s.config.Logger.Debug(
			"context cancelled, shutting down midnight gRPC server",
		)
		s.gracefulStop(s.config.ShutdownTimeout)
	}()

	return nil
}

// Stop gracefully shuts the server down, bounded by ctx's deadline when set
// and otherwise by the configured shutdown timeout. It is safe to call
// repeatedly and concurrently with the context-cancellation path.
func (s *Server) Stop(ctx context.Context) error {
	timeout := s.config.ShutdownTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 {
			timeout = remaining
		}
	}
	s.gracefulStop(timeout)
	return nil
}

// gracefulStop attempts GracefulStop, escalating to a hard Stop if it does not
// complete within timeout. The first caller to claim the server wins; later
// calls observe a nil server and return immediately, making this idempotent.
func (s *Server) gracefulStop(timeout time.Duration) {
	s.mu.Lock()
	grpcServer := s.grpcServer
	healthSrv := s.health
	s.grpcServer = nil
	s.health = nil
	s.mu.Unlock()

	if grpcServer == nil {
		return
	}

	// Fail readiness checks and in-flight health watches before draining.
	if healthSrv != nil {
		healthSrv.Shutdown()
	}

	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		s.config.Logger.Warn(
			"midnight gRPC graceful shutdown timed out; forcing stop",
			"timeout",
			timeout,
		)
		grpcServer.Stop()
		<-done
	}
}

// stubService is the placeholder MidnightState implementation. Embedding
// UnimplementedMidnightStateServer makes every RPC return codes.Unimplemented
// until the real handlers are added in follow-up work.
type stubService struct {
	midnight.UnimplementedMidnightStateServer
}
