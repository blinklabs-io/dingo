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

	// An /account/coins response streams the account's UTxOs from storage in
	// pages of at most defaultAccountCoinsPageSize matches, scanning at most
	// defaultAccountCoinsPageCandidates outputs and retaining at most
	// defaultAccountCoinsPageBytes of output CBOR per page. Server memory is
	// bounded by one page however many coins the account holds.
	defaultAccountCoinsPageSize       = 500
	defaultAccountCoinsPageCandidates = 5000
	defaultAccountCoinsPageBytes      = 1 << 20

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
	// CORSAllowedOrigins configures Access-Control-Allow-Origin.
	// Empty disables CORS.
	CORSAllowedOrigins []string
	TLS                apiconfig.EffectiveTLS
	// accountCoinsPageSize, accountCoinsPageCandidates and
	// accountCoinsPageBytes bound one streamed /account/coins page. They are
	// unexported so paging stays a server invariant rather than part of the
	// Mesh wire contract.
	accountCoinsPageSize       int
	accountCoinsPageCandidates int
	accountCoinsPageBytes      int64
}

// Server is the Mesh-compatible REST API server.
type Server struct {
	config        ServerConfig
	logger        *slog.Logger
	networkID     *NetworkIdentifier
	genesisID     *BlockIdentifier
	addrNetworkID uint8
	// listener owns the start/stop protocol, including releasing the
	// listening socket as part of what Stop waits for -- see
	// internal/apilistener.
	listener *apilistener.Listener
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
	if cfg.Logger == nil {
		cfg.Logger = slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		)
	}
	logger := cfg.Logger.With("component", "mesh")
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = defaultListenAddr
	}

	if cfg.accountCoinsPageSize <= 0 {
		cfg.accountCoinsPageSize = defaultAccountCoinsPageSize
	}
	if cfg.accountCoinsPageCandidates <= 0 {
		cfg.accountCoinsPageCandidates = defaultAccountCoinsPageCandidates
	}
	if cfg.accountCoinsPageBytes <= 0 {
		cfg.accountCoinsPageBytes = defaultAccountCoinsPageBytes
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
		config:        cfg,
		logger:        logger,
		networkID:     networkID,
		genesisID:     genesisID,
		addrNetworkID: addrNetID,
		listener:      apilistener.New("Mesh API", logger),
	}, nil
}

// Start starts the HTTP server in a background goroutine.
func (s *Server) Start(ctx context.Context) error {
	startDone, err := s.listener.BeginStart()
	if err != nil {
		return err
	}
	defer s.listener.EndStart(startDone)

	server, bindDone, err := s.listener.Publish(func() *http.Server {
		mux := http.NewServeMux()
		s.registerRoutes(mux)
		return &http.Server{
			Addr: s.config.ListenAddress,
			Handler: httpcors.Handler(
				mux,
				httpcors.Config{
					AllowedOrigins: s.config.CORSAllowedOrigins,
				},
			),
		}
	})
	if err != nil {
		return err
	}

	// Watched before the bind so a context cancelled mid-bind still tears the
	// server down: the detach is what makes an in-flight bind close its own
	// socket.
	s.listener.Watch(ctx, server, apilistener.Graceful)

	served, err := s.listener.Bind(server, bindDone, s.config.TLS)
	if err != nil {
		s.listener.Unpublish(server)
		return err
	}
	if !served {
		// A context cancellation detached this server while it was binding, so
		// Bind closed the socket rather than serving it. Saying the listener
		// came up would be false.
		return nil
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
	return s.listener.Stop(ctx, apilistener.Graceful)
}

// registerRoutes registers all Mesh API endpoints.
func (s *Server) registerRoutes(mux interface {
	HandleFunc(string, func(http.ResponseWriter, *http.Request))
},
) {
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
	if err := s.decodeRequest(w, r, dst); err != nil {
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
// slot duration transitions via the epoch cache.
func (s *Server) slotToTimestamp(slot uint64) (int64, error) {
	t, err := s.config.LedgerState.SlotToTime(slot)
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
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

// writeError writes a Mesh error response. The Mesh specification declares
// only 200 and 500 for every operation and carries the error class in the
// Error body, so every error is a 500; clients retry on Error.retriable.
func writeError(w http.ResponseWriter, meshErr *Error) {
	writeJSON(w, http.StatusInternalServerError, meshErr)
}

// decodeRequest decodes a JSON request body into dst, requiring the body to
// hold exactly one JSON value.
func (s *Server) decodeRequest(
	_ http.ResponseWriter,
	r *http.Request,
	dst any,
) error {
	body := r.Body
	defer body.Close()
	decoder := json.NewDecoder(body)
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return errors.New("request body must contain only one JSON value")
	}
	return nil
}
