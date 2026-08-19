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
	"net/http"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiauth"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/apilistener"
	"github.com/blinklabs-io/dingo/internal/httpcors"
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
	// listener owns the start/stop protocol, including releasing the
	// listening socket as part of what Stop waits for -- see
	// internal/apilistener.
	listener *apilistener.Listener
	verifier *apiauth.Verifier
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
		listener:            apilistener.New("Mesh API", logger),
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
	// The verifier is installed inside the build callback so it is published
	// with the server it belongs to: a second Start is rejected before the
	// callback runs, and so cannot replace a running server's verifier.
	server, bindDone, err := s.listener.Publish(func() *http.Server {
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
		return &http.Server{
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
	})
	if err != nil {
		return err
	}

	// Launched before the bind so a context cancelled mid-bind still tears the
	// server down: Take is what makes an in-flight bind close its own socket.
	go func() { //nolint:gosec // G118: goroutine intentionally outlives ctx to perform graceful shutdown
		<-ctx.Done()
		job, _ := s.listener.Take()
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
			if err := s.listener.Shutdown(
				shutdownCtx, job, apilistener.Graceful,
			); err != nil {
				s.logger.Error(
					"failed to shutdown Mesh API "+
						"server on context "+
						"cancellation",
					"error", err,
				)
			}
		}
	}()

	if err := s.listener.Bind(server, bindDone, s.config.TLS); err != nil {
		s.listener.Unpublish(server)
		return err
	}

	s.logger.Info(
		"Mesh API listener started on " +
			s.config.ListenAddress,
	)

	return nil
}

// Stop gracefully shuts down the HTTP server, and does not return until the
// listening socket has been released -- see internal/apilistener.
func (s *Server) Stop(ctx context.Context) error {
	job, inFlight := s.listener.Take()
	if job == nil {
		return s.listener.AwaitTeardown(ctx, inFlight)
	}
	s.logger.Debug("shutting down Mesh API server")
	return s.listener.Shutdown(ctx, job, apilistener.Graceful)
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
