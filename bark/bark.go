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

package bark

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	"connectrpc.com/grpcreflect"
	archiveconnect "github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/httpcors"
)

type Bark struct {
	mu     sync.Mutex // protects server
	server *http.Server
	config BarkConfig
}

type BarkConfig struct {
	Logger          *slog.Logger
	DB              *database.Database
	TlsCertFilePath string
	TlsKeyFilePath  string
	Host            string
	Port            uint
	// CORSAllowedOrigins configures Access-Control-Allow-Origin.
	// Empty disables CORS.
	CORSAllowedOrigins []string
}

func NewBark(cfg BarkConfig) (*Bark, error) {
	if cfg.DB == nil {
		return nil, errors.New("bark: db is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if cfg.Host == "" {
		cfg.Host = "0.0.0.0"
	}
	if cfg.Port == 0 {
		cfg.Port = 9091
	}
	return &Bark{
		config: cfg,
	}, nil
}

func (b *Bark) Start(ctx context.Context) error {
	b.mu.Lock()
	if b.server != nil {
		b.mu.Unlock()
		return errors.New("server already started")
	}

	mux := http.NewServeMux()
	compress1KB := connect.WithCompressMinBytes(1024)

	archivePath, archiveHandler := archiveconnect.NewArchiveServiceHandler(
		&archiveServiceHandler{bark: b},
		compress1KB,
	)

	mux.Handle(archivePath, archiveHandler)
	mux.Handle(
		grpchealth.NewHandler(
			grpchealth.NewStaticChecker(archiveconnect.ArchiveServiceName),
			compress1KB,
		),
	)
	mux.Handle(
		grpcreflect.NewHandlerV1(
			grpcreflect.NewStaticReflector(
				archiveconnect.ArchiveServiceName,
			),
			compress1KB,
		),
	)

	handler := httpcors.Handler(
		mux,
		httpcors.Config{
			AllowedOrigins: b.config.CORSAllowedOrigins,
		},
	)
	var server *http.Server
	if b.config.TlsCertFilePath != "" && b.config.TlsKeyFilePath != "" {
		b.config.Logger.Info(
			fmt.Sprintf("starting bark gRPC TLS listener on %s:%d",
				b.config.Host,
				b.config.Port,
			),
		)

		server = &http.Server{
			Addr: fmt.Sprintf(
				"%s:%d",
				b.config.Host,
				b.config.Port,
			),
			Handler:           handler,
			ReadHeaderTimeout: 60 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
	} else {
		b.config.Logger.Info(
			fmt.Sprintf("starting bark gRPC listener on %s:%d",
				b.config.Host,
				b.config.Port,
			),
		)
		server = &http.Server{
			Addr: fmt.Sprintf(
				"%s:%d",
				b.config.Host,
				b.config.Port,
			),
			Handler:           handler,
			Protocols:         unencryptedHTTP2Protocols(),
			ReadHeaderTimeout: 60 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
	}
	b.server = server
	b.mu.Unlock()

	if err := b.startServer(server); err != nil {
		b.mu.Lock()
		b.server = nil
		b.mu.Unlock()
		return err
	}

	go func() { //nolint:gosec // G118: goroutine intentionally outlives ctx to perform graceful shutdown
		<-ctx.Done()
		b.mu.Lock()
		if b.server == server {
			b.config.Logger.Debug(
				"context cancelled, shutting down bark gRPC server",
			)

			//nolint:contextcheck //shutdownCtx is intentionally created from background to allow shutdown to complete even if ctx is cancelled
			shutdownCtx, cancel := context.WithTimeout(
				context.Background(),
				30*time.Second,
			)
			defer cancel()
			if err := server.Shutdown(shutdownCtx); err != nil { //nolint:contextcheck //shutdownCtx is intentionally created from background to allow shutdown to complete even if ctx is cancelled
				b.config.Logger.Error(
					"failed to shutdown bark gRPC server on context cancellation",
					"error",
					err,
				)
			}
			b.server = nil
		}
		b.mu.Unlock()
	}()

	return nil
}

func unencryptedHTTP2Protocols() *http.Protocols {
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	return protocols
}

func configureServerTLS(server *http.Server) {
	if server.TLSConfig == nil {
		server.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		return
	}
	if server.TLSConfig.MinVersion < tls.VersionTLS12 {
		server.TLSConfig.MinVersion = tls.VersionTLS12
	}
}

// startServer starts the HTTP server with deterministic error
// detection. It validates TLS configuration, binds the listening
// socket and pre-loads any TLS keypair synchronously so port and
// certificate errors surface before returning, then serves in a
// background goroutine.
func (b *Bark) startServer(server *http.Server) error {
	if (b.config.TlsCertFilePath != "") != (b.config.TlsKeyFilePath != "") {
		return errors.New(
			"failed to start bark gRPC server: both tls cert and key must be specified",
		)
	}
	useTLS := b.config.TlsCertFilePath != "" && b.config.TlsKeyFilePath != ""
	serverType := "non-TLS"
	if useTLS {
		serverType = "TLS"
		if _, err := tls.LoadX509KeyPair(
			b.config.TlsCertFilePath,
			b.config.TlsKeyFilePath,
		); err != nil {
			return fmt.Errorf(
				"failed to load TLS keypair for bark gRPC %s server: %w",
				serverType, err,
			)
		}
		configureServerTLS(server)
	}
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("failed to start bark gRPC %s server: %w",
			serverType, err)
	}
	go func() {
		var serveErr error
		if useTLS {
			serveErr = server.ServeTLS(
				ln,
				b.config.TlsCertFilePath,
				b.config.TlsKeyFilePath,
			)
		} else {
			serveErr = server.Serve(ln)
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			b.config.Logger.Error(
				"bark gRPC server error",
				"error", serveErr,
			)
		}
	}()
	return nil
}

func (b *Bark) Stop(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.server != nil {
		b.config.Logger.Debug("shutting down bark gRPC server")
		if err := b.server.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown bark gRPC server: %w", err)
		}
		b.server = nil
	}
	return nil
}
