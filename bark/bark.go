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
	"strconv"
	"sync"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	"connectrpc.com/grpcreflect"
	archiveconnect "github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
	databaseconnect "github.com/blinklabs-io/bark/proto/v1alpha1/database/databasev1alpha1connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/httpcors"
	"github.com/blinklabs-io/dingo/internal/tlsutil"
)

type Bark struct {
	// mu protects server and listenerAddr, plus config.DB writes (ResumeDB
	// takes it too). It must never be held across a blocking call whose
	// completion depends on Acquire succeeding (e.g. server.Shutdown
	// draining an in-flight request that itself calls Acquire) — see
	// Acquire's doc comment for why its config.DB read deliberately does
	// NOT take mu, to avoid exactly that deadlock.
	mu           sync.Mutex
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
	// DestinationRegistry supplies the cloud destination schemes (s3, gcs)
	// this Bark instance's DatabaseService handler can resolve
	// SnapshotCloudDestination/cloud snapshot URIs against — composition
	// code owns constructing it; nil is valid when no cloud destination
	// is ever configured.
	DestinationRegistry *lifecycle.DestinationRegistry
	TlsCertFilePath     string
	TlsKeyFilePath      string
	// TlsClientCAFilePath is a PEM CA bundle used to verify client
	// certificates (mTLS) on this listener. Required whenever Lifecycle is
	// set: the DatabaseService's destructive RPCs (CreateSnapshot,
	// DeleteSnapshot, VerifySnapshot, Restore, Truncate, CancelOperation)
	// refuse any request whose connection didn't present a certificate
	// verified against this CA — see newOperatorAuthInterceptor in auth.go.
	// Read-only RPCs (status/catalog/Archive) never require one. Requires
	// TlsCertFilePath/TlsKeyFilePath to also be set, since mTLS has no
	// meaning without the server's own TLS listener underneath it. Start
	// (not NewBark) fails closed if Lifecycle is set without this — see
	// Start's doc comment for why the check lives there.
	TlsClientCAFilePath string
	Host                string
	Port                uint
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
//
// The config.DB read below deliberately does not take b.mu: Stop holds
// b.mu for the entire duration of its blocking server.Shutdown call, which
// waits for in-flight requests — including ones calling Acquire — to
// finish. If Acquire also needed b.mu here, a request whose handler is
// exactly what Shutdown is waiting to drain could deadlock against Stop
// forever. It's safe to skip: dbGate alone is sufficient synchronization
// for this access, since ResumeDB always finishes writing config.DB (under
// its own b.mu critical section) before it unlocks dbGate, and Go's mutex
// happens-before guarantee means that unlock is visible to whichever
// Acquire's TryRLock above next succeeds — no separate b.mu read needed.
func (b *Bark) Acquire() (db *database.Database, release func(), err error) {
	if !b.dbGate.TryRLock() {
		return nil, nil, ErrDBUnavailable
	}
	db = b.config.DB
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

func barkListenAddr(host string, port uint) string {
	return net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10))
}

func (b *Bark) Start(ctx context.Context) error {
	b.mu.Lock()
	if b.server != nil {
		b.mu.Unlock()
		return errors.New("server already started")
	}

	// Fail closed here, at the point the DatabaseService's destructive RPCs
	// actually become network-reachable, rather than in NewBark: a
	// *databaseServiceHandler built via newDatabaseServiceHandler and
	// exercised through direct in-process Go calls (as most of this
	// package's own handler-level tests do) never goes through Start's
	// mux/interceptor wiring at all, so requiring TLS/a client CA in the
	// constructor would reject a composition path that was never actually
	// exposed over the network in the first place.
	if b.config.Lifecycle != nil && b.config.TlsClientCAFilePath == "" {
		b.mu.Unlock()
		return errors.New(
			"bark: TlsClientCAFilePath is required to start with lifecycle set — " +
				"the DatabaseService's destructive RPCs (CreateSnapshot/" +
				"DeleteSnapshot/VerifySnapshot/Restore/Truncate/CancelOperation) " +
				"must not be mounted without a way to authenticate callers",
		)
	}
	if b.config.Lifecycle != nil &&
		(b.config.TlsCertFilePath == "" || b.config.TlsKeyFilePath == "") {
		b.mu.Unlock()
		return errors.New(
			"bark: TlsCertFilePath and TlsKeyFilePath are required to start " +
				"with lifecycle set — mTLS client-certificate verification has " +
				"no meaning without the server's own TLS listener",
		)
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
			connect.WithInterceptors(newOperatorAuthInterceptor(
				b.config.Logger,
				destructiveDatabaseProcedures,
				readOnlyDatabaseProcedures,
			)),
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

	handler := peerCertContextMiddleware(httpcors.Handler(
		mux,
		httpcors.Config{
			AllowedOrigins: b.config.CORSAllowedOrigins,
		},
	))
	listenAddr := barkListenAddr(b.config.Host, b.config.Port)
	var server *http.Server
	if b.config.TlsCertFilePath != "" && b.config.TlsKeyFilePath != "" {
		b.config.Logger.Info(
			"starting bark gRPC TLS listener on " + listenAddr,
		)

		server = &http.Server{
			Addr:              listenAddr,
			Handler:           handler,
			ReadHeaderTimeout: 60 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
	} else {
		b.config.Logger.Info(
			"starting bark gRPC listener on " + listenAddr,
		)
		server = &http.Server{
			Addr:              listenAddr,
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
	if b.config.TlsClientCAFilePath != "" && !useTLS {
		return errors.New(
			"failed to start bark gRPC server: TlsClientCAFilePath requires tls cert and key to also be set",
		)
	}
	serverType := "non-TLS"
	if useTLS {
		serverType = "TLS"
		cert, err := tls.LoadX509KeyPair(
			b.config.TlsCertFilePath,
			b.config.TlsKeyFilePath,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to load TLS keypair for bark gRPC %s server: %w",
				serverType, err,
			)
		}
		server.TLSConfig = tlsutil.ServerConfig(server.TLSConfig)
		// Pin the preflight-loaded keypair onto the config so the
		// serving goroutine's ServeTLS call below (passed "", "" for
		// certFile/keyFile) reuses it instead of reloading the same
		// files from disk a second time. Without this, ServeTLS always
		// reloads regardless of configHasCert whenever it's passed
		// non-empty cert/key paths, and if that redundant reload failed
		// (e.g. the files changed or became unreadable between here and
		// the goroutine running), ServeTLS returns before ever calling
		// srv.Serve — meaning it returns before Serve's own
		// `defer l.Close()` is installed, leaking ln and leaving
		// b.listenerAddr/b.server reporting a listener that is open but
		// nobody is Accept-ing on.
		server.TLSConfig.Certificates = []tls.Certificate{cert}

		if b.config.TlsClientCAFilePath != "" {
			pool, caErr := tlsutil.LoadClientCAPool(
				b.config.TlsClientCAFilePath,
			)
			if caErr != nil {
				return fmt.Errorf(
					"failed to load client CA for bark gRPC %s server: %w",
					serverType, caErr,
				)
			}
			server.TLSConfig.ClientCAs = pool
			// VerifyClientCertIfGiven, not RequireAndVerifyClientCert: this
			// listener also serves read-only RPCs (status/catalog/Archive)
			// that must keep working for a caller with no client cert at
			// all. Only the destructive DatabaseService procedures actually
			// require one — enforced per-request by
			// newOperatorAuthInterceptor (auth.go), not at the handshake.
			// Go's TLS stack still fully chain-verifies any cert that IS
			// presented against ClientCAs during the handshake itself, so a
			// cert signed by an untrusted CA never reaches the request
			// layer regardless of which RPC it's calling.
			server.TLSConfig.ClientAuth = tls.VerifyClientCertIfGiven
		}
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
			serveErr = server.ServeTLS(ln, "", "")
		} else {
			serveErr = server.Serve(ln)
		}
		b.handleServeExit(server, ln, serveErr)
	}()
	return nil
}

// handleServeExit runs once the serving goroutine's Serve/ServeTLS call
// returns. A nil error, or http.ErrServerClosed, means an intentional
// shutdown path (Stop, or Start's ctx-cancellation goroutine) already
// closed the listener and cleared server/listenerAddr itself, so there's
// nothing to do. Any other error means Serve/ServeTLS exited on its own —
// belt-and-suspenders alongside startServer reusing the preflight-loaded
// TLS keypair (to avoid the redundant reload that used to cause exactly
// this): close ln and clear server/listenerAddr (only if they still
// match this call's server, so a concurrent Stop/cancellation isn't
// clobbered) so Addr() stops reporting a dead address for a listener
// that's either already leaked or about to be if left unclosed here.
func (b *Bark) handleServeExit(
	server *http.Server,
	ln net.Listener,
	serveErr error,
) {
	if serveErr == nil || errors.Is(serveErr, http.ErrServerClosed) {
		return
	}
	b.config.Logger.Error(
		"bark gRPC server error",
		"error", serveErr,
	)
	_ = ln.Close()
	b.mu.Lock()
	if b.server == server {
		b.server = nil
		b.listenerAddr = nil
	}
	b.mu.Unlock()
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
