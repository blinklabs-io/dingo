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
	databaseconnect "github.com/blinklabs-io/bark/proto/v1alpha1/database/databasev1alpha1connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/httpcors"
	"github.com/blinklabs-io/dingo/internal/tlsutil"
)

type Bark struct {
	mu           sync.Mutex // protects server, config.DB, and listenerAddr
	server       *http.Server
	config       BarkConfig
	listenerAddr net.Addr
	// dbGate guards config.DB across a live Restore/Truncate's close-and-
	// replace window: PauseDB write-locks it before the old database is
	// closed, ResumeDB publishes the replacement and unlocks it. Acquire
	// read-locks it (via TryRLock, never blocking) for the duration of one
	// request. See Acquire's doc comment for the full race this prevents.
	dbGate sync.RWMutex
}

type BarkConfig struct {
	Logger    *slog.Logger
	DB        *database.Database
	Lifecycle *dblifecycle.Service
	// SnapshotDir is the base directory the DatabaseService's CreateSnapshot/
	// Restore RPCs write to and read from — required when Lifecycle is set.
	// There is no separate snapshot catalog store (see database.go's doc
	// comment); ListSnapshots/ListAvailableSnapshots scan this directory
	// for manifest.json files instead, so each snapshot's generated ID is
	// also its directory name directly under SnapshotDir.
	SnapshotDir string
	// SnapshotCloudDestination, if set, is the same cloud destination URI
	// as databaseLifecycle.snapshotCloudDestination — passed through here
	// so ListAvailableSnapshots can additionally list what's stored there
	// (via database/lifecycle.ListCloudSnapshots), merged with the local
	// catalog. Empty disables cloud listing; CreateSnapshot's own upload
	// path doesn't need this field since it goes through Lifecycle, which
	// already has its own copy of the same config value.
	SnapshotCloudDestination string
	TlsCertFilePath          string
	TlsKeyFilePath           string
	Host                     string
	Port                     uint
	// CORSAllowedOrigins configures Access-Control-Allow-Origin.
	// Empty disables CORS.
	CORSAllowedOrigins []string
}

// ErrDBUnavailable is returned by Acquire when there is currently no
// usable database to hand out — either none has been set yet, or a live
// Restore/Truncate has paused access via PauseDB while it swaps the old
// one out for a freshly rebuilt one. Handlers should map this to
// connect.CodeUnavailable rather than surfacing it as an internal error.
var ErrDBUnavailable = errors.New("bark: database temporarily unavailable")

// Acquire pins the current database for the duration of one request:
// callers must use the returned db for every call they make during the
// request, then call release exactly once (typically via defer) when
// done with it. Pinning matters because a live Restore/Truncate closes
// the old database and opens a new one in place — without pinning, a
// request that fetched the pointer at the top and kept calling methods
// on it over its lifetime (as GetDatabaseInfo and FetchBlock both do)
// could end up racing that close, anywhere from a confusing internal
// error (sqlite queries against a closed *sql.DB) to an outright panic
// (Badger panics opening a transaction against a closed DB). Acquire's
// underlying dbGate stays read-locked for exactly as long as release is
// unheld, so PauseDB's write-lock acquisition — and therefore the actual
// database close it's guarding — waits for every in-flight Acquire to
// finish first.
//
// Returns ErrDBUnavailable (with a nil db and release) if no database is
// currently set, or if PauseDB currently has the gate held: Acquire never
// blocks waiting for a pause to end, since that could be a long-running
// Restore/Truncate — callers report unavailable immediately instead.
func (b *Bark) Acquire() (db *database.Database, release func(), err error) {
	if !b.dbGate.TryRLock() {
		return nil, nil, ErrDBUnavailable
	}
	b.mu.Lock()
	db = b.config.DB
	b.mu.Unlock()
	if db == nil {
		b.dbGate.RUnlock()
		return nil, nil, ErrDBUnavailable
	}
	return db, b.dbGate.RUnlock, nil
}

// PauseDB blocks new Acquire calls (which fail immediately with
// ErrDBUnavailable rather than blocking behind it) and waits for every
// currently in-flight Acquire to release, so the database it currently
// points at can be safely closed once this returns. Must always be
// followed by a later ResumeDB call — typically bracketing a live
// Restore/Truncate's quiesce-close-reinitialize sequence — or Bark's
// database access is left paused permanently.
func (b *Bark) PauseDB() {
	b.dbGate.Lock()
}

// ResumeDB publishes db as what Acquire hands out going forward, then
// releases the pause PauseDB put in place. Call this only once the
// replacement database is fully initialized and ready to serve — e.g.
// from a live Restore/Truncate's reinitializeAPIServers step — so no
// Acquire caller ever observes a database that's still mid-setup.
func (b *Bark) ResumeDB(db *database.Database) {
	b.mu.Lock()
	b.config.DB = db
	b.mu.Unlock()
	b.dbGate.Unlock()
}

// Addr returns the address the server is actually listening on (e.g.
// "127.0.0.1:54321"), populated once Start has bound the listener — most
// useful when Port was 0, letting a test or an operator discover the
// OS-assigned port without a separate, racy net.Listen-then-close probe.
// Returns "" before Start has been called, and again once the server has
// stopped (Stop, or the listener automatically shutting down when Start's
// ctx is cancelled) — never a stale address for a listener that is no
// longer actually open.
func (b *Bark) Addr() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.listenerAddr == nil {
		return ""
	}
	return b.listenerAddr.String()
}

func NewBark(cfg BarkConfig) (*Bark, error) {
	if cfg.DB == nil {
		return nil, errors.New("bark: db is required")
	}
	if cfg.Lifecycle != nil && cfg.SnapshotDir == "" {
		return nil, errors.New(
			"bark: snapshot dir is required when lifecycle is set",
		)
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

	serviceNames := []string{archiveconnect.ArchiveServiceName}

	archivePath, archiveHandler := archiveconnect.NewArchiveServiceHandler(
		&archiveServiceHandler{bark: b},
		compress1KB,
	)
	mux.Handle(archivePath, archiveHandler)

	if b.config.Lifecycle != nil {
		databasePath, databaseHandler := databaseconnect.NewDatabaseServiceHandler(
			newDatabaseServiceHandler(b),
			compress1KB,
		)
		mux.Handle(databasePath, databaseHandler)
		serviceNames = append(serviceNames, databaseconnect.DatabaseServiceName)
	}

	mux.Handle(
		grpchealth.NewHandler(
			grpchealth.NewStaticChecker(serviceNames...),
			compress1KB,
		),
	)
	mux.Handle(
		grpcreflect.NewHandlerV1(
			grpcreflect.NewStaticReflector(serviceNames...),
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
			b.listenerAddr = nil
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
		server.TLSConfig = tlsutil.ServerConfig(server.TLSConfig)
	}
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("failed to start bark gRPC %s server: %w",
			serverType, err)
	}
	b.mu.Lock()
	b.listenerAddr = ln.Addr()
	b.mu.Unlock()
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
		// http.Server.Shutdown closes every open listener essentially
		// immediately, before it starts waiting (bounded by ctx) for
		// already-active connections to drain -- so the listener is gone
		// whether Shutdown returns because that drain finished or because
		// ctx's deadline was hit first. b.server/b.listenerAddr must be
		// cleared in both cases, not only on success, or a timed-out Stop
		// would leave Addr() reporting a listener that (per its own
		// doc comment's no-stale-address contract) is no longer actually
		// open.
		err := b.server.Shutdown(ctx)
		b.server = nil
		b.listenerAddr = nil
		if err != nil {
			return fmt.Errorf("failed to shutdown bark gRPC server: %w", err)
		}
	}
	return nil
}
