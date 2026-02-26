package bark

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
	archiveconnect "github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
	"github.com/blinklabs-io/dingo/database"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
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
}

func NewBark(cfg BarkConfig) *Bark {
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
	}
}

func (b *Bark) Start(ctx context.Context) error {
	b.mu.Lock()
	if b.server != nil {
		b.mu.Unlock()
		return errors.New("server already started")
	}

	if b.config.DB == nil {
		b.mu.Unlock()
		return errors.New("database not configured")
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
			Handler:           mux,
			ReadHeaderTimeout: 60 * time.Second,
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
			Handler:           h2c.NewHandler(mux, &http2.Server{}),
			ReadHeaderTimeout: 60 * time.Second,
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

	go func() {
		<-ctx.Done()
		b.mu.Lock()
		if b.server != nil {
			b.config.Logger.Debug(
				"context cancelled, shutting down bark gRPC server",
			)

			//nolint:contextcheck //shutdownCtx is intentionally created from background to allow shutdown to complete even if ctx is cancelled
			shutdownCtx, cancel := context.WithTimeout(
				context.Background(),
				30*time.Second,
			)
			defer cancel()
			if err := b.server.Shutdown(shutdownCtx); err != nil { //nolint:contextcheck //shutdownCtx is intentionally created from background to allow shutdown to complete even if ctx is cancelled
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

func (b *Bark) startServer(server *http.Server) error {
	startErr := make(chan error, 1)
	go func() {
		var err error

		if b.config.TlsCertFilePath != "" && b.config.TlsKeyFilePath == "" ||
			b.config.TlsCertFilePath == "" && b.config.TlsKeyFilePath != "" {
			err = errors.New("both tls cert and key must be specified")
			return
		}

		if b.config.TlsCertFilePath != "" && b.config.TlsKeyFilePath != "" {
			err = server.ListenAndServeTLS(
				b.config.TlsCertFilePath,
				b.config.TlsKeyFilePath,
			)
		} else {
			err = server.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			select {
			case startErr <- err:
			default:
				b.config.Logger.Error(
					"bark gRPC server error",
					"error", err)
			}
		}
	}()

	select {
	case err := <-startErr:
		serverType := "non-TLS"
		if b.config.TlsCertFilePath != "" && b.config.TlsKeyFilePath != "" {
			serverType = "TLS"
		}
		return fmt.Errorf("failed to start bark gRPC %s server: %w", serverType, err)
	case <-time.After(100 * time.Millisecond):
		// Assume startup succeeded if no error within 100ms
	}

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
