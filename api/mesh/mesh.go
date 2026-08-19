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

package mesh

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiauth"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/httpcors"
	"github.com/blinklabs-io/dingo/internal/tlsutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

const (
	// rosettaVersion is pinned to 1.4.15 for
	// compatibility with existing Mesh/Rosetta tooling
	// (mesh-cli, exchanges). Upgrading to 1.7.x is
	// tracked for a future release.
	rosettaVersion    = "1.4.15"
	nodeVersion       = "0.1.0"
	blockchain        = "cardano"
	defaultListenAddr = ":8080"
	maxRequestBody    = 1 << 20 // 1 MB

	// mainnetMagic is the network magic for Cardano
	// mainnet, used to determine the address network.
	mainnetMagic = 764824073
)

// ServerConfig holds configuration for the Mesh API server.
type ServerConfig struct {
	Logger      *slog.Logger
	LedgerState MeshLedgerState
	Database    MeshDatabase
	Chain       MeshChain
	Mempool     MeshMempool
	// ListenAddress is the TCP address to listen on (e.g. ":8080").
	ListenAddress string
	Network       string
	NetworkMagic  uint32
	// GenesisHash is the Byron genesis block hash (hex-encoded).
	GenesisHash string
	// GenesisStartTimeSec is the Unix timestamp (seconds) of slot 0 for the
	// configured network. Used to convert slot numbers to absolute timestamps.
	GenesisStartTimeSec int64
	// CORSAllowedOrigins configures Access-Control-Allow-Origin.
	// Empty disables CORS.
	CORSAllowedOrigins []string
	// TLS and Auth are the resolved (merged, validated) TLS/authentication
	// policy for this listener -- see ProviderConfig's doc comment and
	// ARCHITECTURE.md's "API security" section.
	TLS  apiconfig.EffectiveTLS
	Auth apiconfig.EffectiveAuth
}

// Server is the Mesh-compatible REST API server.
type Server struct {
	config              ServerConfig
	logger              *slog.Logger
	networkID           *NetworkIdentifier
	genesisID           *BlockIdentifier
	genesisStartTimeSec int64
	addrNetworkID       uint8
	httpServer          *http.Server
	listener            net.Listener
	// bindDone is closed by startServer once the listening socket has been
	// either published on s.listener or closed again. Stop waits on it so a
	// bind still in flight cannot outlive the Stop that raced it.
	bindDone chan struct{}
	// teardown is closed once the caller that detached the server has
	// finished shutting it down. Stop and the context monitor race to detach;
	// the loser gets no server back and would otherwise report the server
	// down while the winner's shutdown was still releasing the port.
	teardown chan struct{}
	verifier *apiauth.Verifier
	mu       sync.Mutex
}

// NewServer creates a new Mesh API server instance.
// Returns an error if required configuration fields are
// missing.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Chain == nil {
		return nil, errors.New(
			"mesh: Chain is required",
		)
	}
	if cfg.Database == nil {
		return nil, errors.New(
			"mesh: Database is required",
		)
	}
	if cfg.LedgerState == nil {
		return nil, errors.New(
			"mesh: LedgerState is required",
		)
	}
	if cfg.Mempool == nil {
		return nil, errors.New(
			"mesh: Mempool is required",
		)
	}
	if cfg.Network == "" {
		return nil, errors.New(
			"mesh: Network is required",
		)
	}
	if cfg.GenesisHash == "" {
		return nil, errors.New(
			"mesh: GenesisHash is required",
		)
	}
	if _, err := hex.DecodeString(
		cfg.GenesisHash,
	); err != nil {
		return nil, fmt.Errorf(
			"mesh: invalid GenesisHash: %w", err,
		)
	}
	if cfg.GenesisStartTimeSec <= 0 {
		return nil, errors.New(
			"mesh: GenesisStartTimeSec must be " +
				"positive",
		)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		)
	}
	logger := cfg.Logger.With("component", "mesh")
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = defaultListenAddr
	}

	var addrNetID uint8 = lcommon.AddressNetworkTestnet
	if cfg.NetworkMagic == mainnetMagic {
		addrNetID = lcommon.AddressNetworkMainnet
	}

	networkID := &NetworkIdentifier{
		Blockchain: blockchain,
		Network:    cfg.Network,
	}

	genesisID := &BlockIdentifier{
		Index: 0,
		Hash:  cfg.GenesisHash,
	}

	return &Server{
		config:              cfg,
		logger:              logger,
		networkID:           networkID,
		genesisID:           genesisID,
		genesisStartTimeSec: cfg.GenesisStartTimeSec,
		addrNetworkID:       addrNetID,
	}, nil
}

// Start starts the HTTP server in a background goroutine.
func (s *Server) Start(ctx context.Context) error {
	// Built before the handler chain so it can install the shared
	// credential-verification middleware (internal/apiauth).
	verifier, err := apiauth.NewVerifier(s.config.Auth)
	if err != nil {
		return fmt.Errorf("mesh: %w", err)
	}
	s.mu.Lock()
	if s.httpServer != nil {
		s.mu.Unlock()
		return errors.New("server already started")
	}
	s.verifier = verifier

	mux := http.NewServeMux()
	s.registerRoutes(mux)

	// CORS must wrap authentication, not the reverse: httpcors.Handler
	// fully answers an OPTIONS preflight itself and never calls the
	// handler it wraps for one, so browsers -- which never attach
	// Authorization to a preflight request -- never need a credential to
	// pass CORS negotiation. Every other request, including a
	// non-preflight OPTIONS, still reaches the mux normally. See
	// internal/apiauth's Middleware doc comment for the general statement
	// of this ordering rule.
	authenticated := apiauth.Middleware(s.verifier)(mux)
	server := &http.Server{
		Addr: s.config.ListenAddress,
		Handler: httpcors.Handler(
			authenticated,
			httpcors.Config{
				AllowedOrigins: s.config.CORSAllowedOrigins,
			},
		),
		ReadHeaderTimeout: 60 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	s.httpServer = server
	bindDone := make(chan struct{})
	s.bindDone = bindDone

	// Launch context monitor before unlocking so there
	// is no window where Stop() could race with the
	// goroutine not yet existing.
	go func() { //nolint:gosec // G118: goroutine intentionally outlives ctx to perform graceful shutdown
		<-ctx.Done()
		job, _ := s.takeServer()
		// A concurrent Stop may have won the detach. It owns the teardown
		// and its caller is already waiting on it, so there is nothing to
		// do and nothing to wait for here.
		if job != nil {
			s.logger.Debug(
				"context cancelled, shutting down " +
					"Mesh API server",
			)
			//nolint:contextcheck
			shutdownCtx, cancel := context.WithTimeout(
				context.Background(),
				30*time.Second,
			)
			defer cancel()
			//nolint:contextcheck
			if err := s.shutdown(shutdownCtx, job); err != nil {
				s.logger.Error(
					"failed to shutdown Mesh API "+
						"server on context "+
						"cancellation",
					"error", err,
				)
			}
		}
	}()

	s.mu.Unlock()

	if err := s.startServer(server, bindDone); err != nil {
		s.mu.Lock()
		// Guarded: an overlapping Stop or restart may already have
		// detached or replaced this server, and clearing the fields
		// unconditionally would discard the newer one. Cleared as a set,
		// matching takeServer, so "no server present" never leaves a
		// listener or bind channel behind for the next caller to find.
		if s.httpServer == server {
			s.httpServer, s.listener, s.bindDone = nil, nil, nil
		}
		s.mu.Unlock()
		return err
	}

	s.logger.Info(
		"Mesh API listener started on " +
			s.config.ListenAddress,
	)

	return nil
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	job, inFlight := s.takeServer()
	if job == nil {
		return awaitTeardown(ctx, inFlight)
	}
	s.logger.Debug("shutting down Mesh API server")
	return s.shutdown(ctx, job)
}

// shutdownJob is everything one caller detaches from the Server in order to
// tear it down, including the channel it must close when finished.
type shutdownJob struct {
	srv      *http.Server
	ln       net.Listener
	bindDone chan struct{}
	done     chan struct{}
}

// takeServer detaches the running HTTP server and its listener so exactly one
// caller shuts them down: Stop and the context monitor started by Start both
// race for them.
//
// The winner gets a job and owns closing job.done. The loser gets a nil job and
// the winner's completion channel, which it must wait on — returning early
// would report the server down while the port was still bound, and an immediate
// restart on the same port would then fail to bind.
func (s *Server) takeServer() (*shutdownJob, chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.httpServer == nil {
		// Either never started, or someone else is already tearing it down.
		return nil, s.teardown
	}
	job := &shutdownJob{
		srv:      s.httpServer,
		ln:       s.listener,
		bindDone: s.bindDone,
		done:     make(chan struct{}),
	}
	s.httpServer, s.listener, s.bindDone = nil, nil, nil
	s.teardown = job.done
	return job, nil
}

// shutdown runs one detached job to completion and reports what went wrong.
// The bind wait is not allowed to skip the teardown: a caller whose context
// expires mid-wait still holds the only reference to a bound socket, so
// returning early would leave the port bound with nothing left to close it.
func (s *Server) shutdown(ctx context.Context, job *shutdownJob) error {
	waitErr := awaitBind(ctx, job.bindDone)
	stopErr := shutdownServer(ctx, job.srv, job.ln)
	if waitErr == nil {
		close(job.done)
		return stopErr
	}
	// The bind is still in flight, so startServer still owns a socket this
	// call cannot close. Closing job.done now would let a waiting Stop report
	// the server down while that socket was still bound. startServer always
	// closes bindDone on its way out -- and closes its own listener once it
	// sees the detach -- so hand the signalling off until then, which also
	// bounds this goroutine.
	go func() {
		<-job.bindDone
		close(job.done)
	}()
	return errors.Join(waitErr, stopErr)
}

// awaitTeardown waits for another caller's in-flight shutdown to finish.
func awaitTeardown(ctx context.Context, done chan struct{}) error {
	return awaitSignal(ctx, done, "an in-flight Mesh API shutdown")
}

// awaitSignal waits for ch to close, bounded by ctx. A nil channel has nothing
// to wait for and succeeds immediately.
//
// One implementation on purpose. Every caller here needs the same recheck: when
// ch closes at the same moment ctx expires, select picks at random, and
// reporting a timeout for work that actually finished turns a clean shutdown
// into a spurious error — and, for the bind, defers the teardown signal that
// another caller is blocked on. Written once, the recheck cannot be present in
// one copy and missing from the next.
func awaitSignal(ctx context.Context, ch chan struct{}, what string) error {
	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		select {
		case <-ch:
			return nil
		default:
		}
		return fmt.Errorf("timed out waiting for %s: %w", what, ctx.Err())
	}
}

// awaitBind waits for an in-flight startServer to finish releasing or
// publishing its socket. Detaching the server first (takeServer) is what makes
// that bind close its own listener, so waiting here is what lets Stop promise
// the port is free by the time it returns rather than merely started closing.
func awaitBind(ctx context.Context, bindDone chan struct{}) error {
	return awaitSignal(ctx, bindDone, "the Mesh API listener bind to settle")
}

// shutdownServer drains in-flight requests, then closes the listening
// socket. http.Server.Shutdown only closes listeners that Serve has
// registered, and startServer registers ours from a goroutine it does
// not wait for, so Shutdown on its own can return while the socket is
// still accepting connections -- leaving the port bound after Stop
// returns, which a capability restart on the same port (see
// node_lifecycle.go) then fails to rebind. Closing the listener here
// makes releasing the port part of what Stop waits for. Closing after
// Shutdown, not before, keeps Serve's exit quiet: Shutdown marks the
// server as shutting down first, so the resulting accept failure
// surfaces as http.ErrServerClosed rather than an error log.
func shutdownServer(
	ctx context.Context,
	srv *http.Server,
	ln net.Listener,
) error {
	err := srv.Shutdown(ctx)
	if ln != nil {
		// Serve closes the listener on its own way out, so an
		// already-closed listener is the expected case, not a failure.
		if closeErr := ln.Close(); closeErr != nil &&
			!errors.Is(closeErr, net.ErrClosed) {
			err = errors.Join(err, closeErr)
		}
	}
	if err != nil {
		return fmt.Errorf(
			"failed to shutdown Mesh API server: %w",
			err,
		)
	}
	return nil
}

// startServer starts the HTTP server with deterministic
// error detection.
func (s *Server) startServer(
	server *http.Server,
	bindDone chan struct{},
) error {
	// Closed on every exit path -- bind failure, publication, or closing our
	// own socket after losing the race to Stop -- so a waiting Stop is never
	// left hanging on a bind that already finished.
	defer close(bindDone)
	useTLS := s.config.TLS.Enabled
	if useTLS {
		if err := tlsutil.ConfigureServerTLS(
			server,
			s.config.TLS.CertFilePath,
			s.config.TLS.KeyFilePath,
		); err != nil {
			return fmt.Errorf(
				"failed to load TLS keypair for Mesh API server: %w",
				err,
			)
		}
	}
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf(
			"failed to listen for Mesh API server: %w",
			err,
		)
	}
	// Recorded so Stop can close the socket itself rather than relying on
	// the Serve goroutine below having registered it (see shutdownServer),
	// but only while this call's server is still the current one. Stop can
	// detach it between Start publishing the server and this point; a bare
	// assignment would then strand a bound socket no later Stop can reach,
	// because takeServer hands back a nil server and shutdownServer never
	// runs. The same guard stops an overlapping restart from overwriting the
	// newer server's listener with this one.
	s.mu.Lock()
	current := s.httpServer == server
	if current {
		s.listener = ln
	}
	s.mu.Unlock()
	if !current {
		// Already stopped or replaced: close our own socket instead of
		// leaving it bound, and never hand it to Serve. Reported by log
		// rather than returned, because Start's error path nils
		// s.httpServer and would clobber the newer server here.
		if closeErr := ln.Close(); closeErr != nil &&
			!errors.Is(closeErr, net.ErrClosed) {
			s.logger.Error(
				"failed to close the listener of a "+
					"stopped Mesh API server",
				"error", closeErr,
			)
		}
		return nil
	}
	go func() {
		var serveErr error
		if useTLS {
			serveErr = server.ServeTLS(ln, "", "")
		} else {
			serveErr = server.Serve(ln)
		}
		if serveErr != nil &&
			!errors.Is(serveErr, http.ErrServerClosed) {
			s.logger.Error(
				"Mesh API server error",
				"error", serveErr,
			)
		}
	}()
	return nil
}

// registerRoutes registers all Mesh API endpoints.
func (s *Server) registerRoutes(mux *http.ServeMux) {
	// Network API
	mux.HandleFunc(
		"POST /network/list",
		s.handleNetworkList,
	)
	mux.HandleFunc(
		"POST /network/options",
		s.handleNetworkOptions,
	)
	mux.HandleFunc(
		"POST /network/status",
		s.handleNetworkStatus,
	)

	// Block API
	mux.HandleFunc("POST /block", s.handleBlock)
	mux.HandleFunc(
		"POST /block/transaction",
		s.handleBlockTransaction,
	)

	// Account API
	mux.HandleFunc(
		"POST /account/balance",
		s.handleAccountBalance,
	)
	mux.HandleFunc(
		"POST /account/coins",
		s.handleAccountCoins,
	)

	// Mempool API
	mux.HandleFunc("POST /mempool", s.handleMempool)
	mux.HandleFunc(
		"POST /mempool/transaction",
		s.handleMempoolTransaction,
	)

	// Construction API
	mux.HandleFunc(
		"POST /construction/derive",
		s.handleConstructionDerive,
	)
	mux.HandleFunc(
		"POST /construction/preprocess",
		s.handleConstructionPreprocess,
	)
	mux.HandleFunc(
		"POST /construction/metadata",
		s.handleConstructionMetadata,
	)
	mux.HandleFunc(
		"POST /construction/payloads",
		s.handleConstructionPayloads,
	)
	mux.HandleFunc(
		"POST /construction/combine",
		s.handleConstructionCombine,
	)
	mux.HandleFunc(
		"POST /construction/parse",
		s.handleConstructionParse,
	)
	mux.HandleFunc(
		"POST /construction/hash",
		s.handleConstructionHash,
	)
	mux.HandleFunc(
		"POST /construction/submit",
		s.handleConstructionSubmit,
	)
}

// networkRequest is implemented by request types that
// carry a NetworkIdentifier for validation.
type networkRequest interface {
	networkID() *NetworkIdentifier
}

// decodeAndValidate decodes a JSON request body and
// validates the network identifier. Returns a non-nil
// *Error if decoding or validation fails.
func (s *Server) decodeAndValidate(
	w http.ResponseWriter,
	r *http.Request,
	dst networkRequest,
) *Error {
	if err := decodeRequest(w, r, dst); err != nil {
		return wrapErr(ErrInvalidRequest, err)
	}
	id := dst.networkID()
	if id == nil {
		return ErrInvalidRequest
	}
	if id.Blockchain != s.networkID.Blockchain ||
		id.Network != s.networkID.Network {
		return ErrNetworkNotSupported
	}
	return nil
}

// slotToTimestamp converts a slot number to a Unix
// timestamp in milliseconds. It delegates to
// LedgerState.SlotToTime which handles Byron-to-Shelley
// slot duration transitions via the epoch cache. Falls
// back to simple 1s-per-slot calculation if the epoch
// cache is not yet populated.
func (s *Server) slotToTimestamp(slot uint64) int64 {
	t, err := s.config.LedgerState.SlotToTime(slot)
	if err == nil {
		return t.UnixMilli()
	}
	// Fallback: assume 1s slots (Shelley+).
	// #nosec G115 -- slot fits in int64
	return (s.genesisStartTimeSec +
		int64(slot)) * 1000
}

// tipBlockID returns a BlockIdentifier for the given
// chain tip.
func (s *Server) tipBlockID(
	tip ochainsync.Tip,
) *BlockIdentifier {
	return &BlockIdentifier{
		Index: int64(tip.BlockNumber), // #nosec G115
		Hash:  hex.EncodeToString(tip.Point.Hash),
	}
}

// writeJSON writes a JSON response.
func writeJSON(
	w http.ResponseWriter,
	status int,
	v any,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Default().Error(
			"failed to encode JSON response",
			"error", err,
		)
	}
}

// writeError writes a Mesh error response.
func writeError(w http.ResponseWriter, meshErr *Error) {
	status := http.StatusInternalServerError
	switch meshErr.Code {
	case ErrNetworkNotSupported.Code,
		ErrBlockNotFound.Code,
		ErrTransactionNotFound.Code,
		ErrAccountNotFound.Code:
		status = http.StatusNotFound
	case ErrInvalidRequest.Code,
		ErrInvalidPublicKey.Code,
		ErrInvalidTransaction.Code,
		ErrSubmitFailed.Code:
		status = http.StatusBadRequest
	case ErrNotImplemented.Code:
		status = http.StatusNotImplemented
	case ErrUnavailable.Code:
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, meshErr)
}

// decodeRequest decodes a JSON request body into dst.
func decodeRequest(
	w http.ResponseWriter,
	r *http.Request,
	dst any,
) error {
	body := http.MaxBytesReader(w, r.Body, maxRequestBody)
	defer body.Close()
	decoder := json.NewDecoder(body)
	return decoder.Decode(dst)
}
