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

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
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
	Logger        *slog.Logger
	EventBus      *event.EventBus
	LedgerState   *ledger.LedgerState
	Database      *database.Database
	Chain         *chain.Chain
	Mempool       *mempool.Mempool
	ListenAddress string
	Network       string
	NetworkMagic  uint32
	// GenesisHash is the Byron genesis block hash
	// (hex-encoded).
	GenesisHash string
	// GenesisStartTimeSec is the Unix timestamp (seconds)
	// of slot 0 for the configured network. Used to
	// convert slot numbers to absolute timestamps.
	GenesisStartTimeSec int64
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
	mu                  sync.Mutex
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
	s.mu.Lock()
	if s.httpServer != nil {
		s.mu.Unlock()
		return errors.New("server already started")
	}

	mux := http.NewServeMux()
	s.registerRoutes(mux)

	server := &http.Server{
		Addr:              s.config.ListenAddress,
		Handler:           mux,
		ReadHeaderTimeout: 60 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	s.httpServer = server

	// Launch context monitor before unlocking so there
	// is no window where Stop() could race with the
	// goroutine not yet existing.
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		srv := s.httpServer
		s.httpServer = nil
		s.mu.Unlock()

		if srv != nil {
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
			if err := srv.Shutdown(
				shutdownCtx,
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

	s.mu.Unlock()

	if err := s.startServer(server); err != nil {
		s.mu.Lock()
		s.httpServer = nil
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
	s.mu.Lock()
	srv := s.httpServer
	s.httpServer = nil
	s.mu.Unlock()

	if srv != nil {
		s.logger.Debug("shutting down Mesh API server")
		if err := srv.Shutdown(ctx); err != nil {
			return fmt.Errorf(
				"failed to shutdown Mesh API server: %w",
				err,
			)
		}
	}
	return nil
}

// startServer starts the HTTP server with deterministic
// error detection.
func (s *Server) startServer(
	server *http.Server,
) error {
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf(
			"failed to listen for Mesh API server: %w",
			err,
		)
	}
	go func() {
		if err := server.Serve(ln); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			s.logger.Error(
				"Mesh API server error",
				"error", err,
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
