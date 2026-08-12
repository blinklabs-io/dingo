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

package blockfrost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiauth"
	"github.com/blinklabs-io/dingo/internal/httpcors"
	"github.com/blinklabs-io/dingo/internal/tlsutil"
)

// blockfrostProjectIDHeader is the header real Blockfrost clients send
// their API key in. Presenting the shared token there authenticates
// exactly as presenting it via "Authorization: Bearer <token>" does --
// see ARCHITECTURE.md/README.md's "API security" section for this
// compatibility decision.
const blockfrostProjectIDHeader = "project_id"

// Blockfrost is the Blockfrost-compatible REST API server.
type Blockfrost struct {
	config     BlockfrostConfig
	logger     *slog.Logger
	node       BlockfrostNode
	httpServer *http.Server
	verifier   *apiauth.Verifier
	mu         sync.Mutex
}

// New creates a new Blockfrost API server instance.
func New(
	cfg BlockfrostConfig,
	node BlockfrostNode,
	logger *slog.Logger,
) *Blockfrost {
	if logger == nil {
		logger = slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		)
	}
	logger = logger.With("component", "blockfrost")
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = ":3000"
	}
	return &Blockfrost{
		config: cfg,
		logger: logger,
		node:   node,
	}
}

// handler builds the HTTP handler for the Blockfrost API,
// including route registration and middleware.
func (b *Blockfrost) handler() http.Handler {
	mux := http.NewServeMux()
	// "GET /{$}" matches only the literal root path. Without
	// "{$}" the pattern would act as a subtree match and
	// silently catch every unimplemented route.
	mux.HandleFunc("GET /{$}", b.handleRoot)
	mux.HandleFunc("GET /health", b.handleHealth)
	mux.HandleFunc(
		"GET /api/v0/blocks/latest",
		b.handleLatestBlock,
	)
	mux.HandleFunc(
		"GET /api/v0/blocks/latest/txs",
		b.handleLatestBlockTxs,
	)
	mux.HandleFunc(
		"GET /api/v0/blocks/{hash_or_number}",
		b.handleBlock,
	)
	mux.HandleFunc(
		"GET /api/v0/epochs/latest",
		b.handleLatestEpoch,
	)
	mux.HandleFunc(
		"GET /api/v0/epochs/latest/parameters",
		b.handleLatestEpochParams,
	)
	mux.HandleFunc(
		"GET /api/v0/epochs/{number}/parameters",
		b.handleEpochParams,
	)
	mux.HandleFunc(
		"GET /api/v0/network",
		b.handleNetwork,
	)
	mux.HandleFunc(
		"GET /api/v0/network/eras",
		b.handleNetworkEras,
	)
	mux.HandleFunc(
		"GET /api/v0/genesis",
		b.handleGenesis,
	)
	mux.HandleFunc(
		"GET /api/v0/assets/{asset}",
		b.handleAsset,
	)
	mux.HandleFunc(
		"GET /api/v0/assets/{asset}/addresses",
		b.handleAssetAddresses,
	)
	mux.HandleFunc(
		"GET /api/v0/pools",
		b.handlePoolsList,
	)
	mux.HandleFunc(
		"GET /api/v0/pools/extended",
		b.handlePoolsExtended,
	)
	mux.HandleFunc(
		"GET /api/v0/pools/retiring",
		b.handlePoolsRetiring,
	)
	mux.HandleFunc(
		"GET /api/v0/pools/{pool_id}/metadata",
		b.handlePoolMetadata,
	)
	mux.HandleFunc(
		"GET /api/v0/pools/{pool_id}",
		b.handlePoolDetail,
	)
	mux.HandleFunc(
		"GET /api/v0/governance/dreps",
		b.handleDReps,
	)
	mux.HandleFunc(
		"GET /api/v0/governance/dreps/{drep_id}",
		b.handleDRep,
	)
	mux.HandleFunc(
		"GET /api/v0/addresses/{address}",
		b.handleAddress,
	)
	mux.HandleFunc(
		"GET /api/v0/addresses/{address}/utxos",
		b.handleAddressUTXOs,
	)
	mux.HandleFunc(
		"GET /api/v0/addresses/{address}/transactions",
		b.handleAddressTransactions,
	)
	mux.HandleFunc(
		"GET /api/v0/metadata/txs/labels/{label}",
		b.handleMetadataTransactions,
	)
	mux.HandleFunc(
		"GET /api/v0/metadata/txs/labels/{label}/cbor",
		b.handleMetadataTransactionsCBOR,
	)
	mux.HandleFunc(
		"GET /api/v0/txs/{hash}",
		b.handleTransaction,
	)
	mux.HandleFunc(
		"POST /api/v0/tx/submit",
		b.handleTransactionSubmit,
	)
	mux.HandleFunc(
		"GET /api/v0/txs/{hash}/cbor",
		b.handleTransactionCBOR,
	)
	mux.HandleFunc(
		"GET /api/v0/txs/{hash}/metadata",
		b.handleTransactionMetadata,
	)
	mux.HandleFunc(
		"GET /api/v0/txs/{hash}/metadata/cbor",
		b.handleTransactionMetadataCBOR,
	)
	mux.HandleFunc(
		"GET /api/v0/txs/{hash}/utxos",
		b.handleTransactionUTXOs,
	)
	mux.HandleFunc(
		"GET /api/v0/txs/{hash}/delegations",
		b.handleTransactionDelegations,
	)
	mux.HandleFunc(
		"GET /api/v0/txs/{hash}/stakes",
		b.handleTransactionStakeAddresses,
	)
	mux.HandleFunc(
		"GET /api/v0/txs/{hash}/withdrawals",
		b.handleTransactionWithdrawals,
	)
	mux.HandleFunc(
		"GET /api/v0/txs/{hash}/mirs",
		b.handleTransactionMIRs,
	)
	mux.HandleFunc(
		"GET /api/v0/txs/{hash}/pool_updates",
		b.handleTransactionPoolUpdates,
	)
	mux.HandleFunc(
		"GET /api/v0/txs/{hash}/pool_retires",
		b.handleTransactionPoolRetires,
	)
	mux.HandleFunc(
		"GET /api/v0/txs/{hash}/redeemers",
		b.handleTransactionRedeemers,
	)
	mux.HandleFunc(
		"GET /api/v0/txs/{hash}/required_signers",
		b.handleTransactionRequiredSigners,
	)
	mux.HandleFunc(
		"GET /api/v0/accounts/{stake_address}",
		b.handleAccount,
	)
	mux.HandleFunc(
		"GET /api/v0/accounts/{stake_address}/addresses",
		b.handleAccountAssociatedAddresses,
	)
	mux.HandleFunc(
		"GET /api/v0/accounts/{stake_address}/delegations",
		b.handleAccountDelegationHistory,
	)
	mux.HandleFunc(
		"GET /api/v0/accounts/{stake_address}/registrations",
		b.handleAccountRegistrationHistory,
	)
	mux.HandleFunc(
		"GET /api/v0/accounts/{stake_address}/rewards",
		b.handleAccountRewardHistory,
	)
	mux.HandleFunc(
		"GET /api/v0/accounts/{stake_address}/utxos",
		b.handleAccountUTXOs,
	)
	mux.HandleFunc(
		"GET /api/v0/accounts/{stake_address}/withdrawals",
		b.handleAccountWithdrawals,
	)
	mux.HandleFunc(
		"GET /api/v0/accounts/{stake_address}/transactions",
		b.handleAccountTransactions,
	)

	// Catch-all for any path not matched above. Registered
	// last so more specific patterns still take precedence;
	// ServeMux resolves by pattern specificity, not
	// registration order.
	mux.HandleFunc("/", b.handleNotFound)

	// Wrap handler with a request body size limit (1 MB)
	// as defense-in-depth against oversized payloads.
	const maxRequestBodyBytes int64 = 1 << 20 // 1 MB
	limited := http.MaxBytesHandler(mux, maxRequestBodyBytes)
	// CORS must wrap authentication, not the reverse: httpcors.Handler
	// fully answers an OPTIONS preflight itself and never calls the
	// handler it wraps for one, so browsers -- which never attach
	// Authorization to a preflight request -- never need a credential to
	// pass CORS negotiation. Every other request, including a
	// non-preflight OPTIONS, still reaches the mux normally. See
	// internal/apiauth's Middleware doc comment for the general statement
	// of this ordering rule.
	authenticated := apiauth.Middleware(
		b.verifier,
		apiauth.WithAliasHeader(blockfrostProjectIDHeader),
	)(limited)
	return httpcors.Handler(
		authenticated,
		httpcors.Config{
			AllowedOrigins: b.config.CORSAllowedOrigins,
		},
	)
}

// Start starts the HTTP server in a background goroutine.
func (b *Blockfrost) Start(
	ctx context.Context,
) error {
	// Built before the handler so handler() can install the shared
	// credential-verification middleware (internal/apiauth).
	verifier, err := apiauth.NewVerifier(b.config.Auth)
	if err != nil {
		return fmt.Errorf("blockfrost: %w", err)
	}
	b.mu.Lock()
	if b.httpServer != nil {
		b.mu.Unlock()
		return errors.New("server already started")
	}
	b.verifier = verifier

	server := &http.Server{
		Addr:              b.config.ListenAddress,
		Handler:           b.handler(),
		ReadHeaderTimeout: 60 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	b.httpServer = server
	b.mu.Unlock()

	// Start the server with deterministic error detection
	if err := b.startServer(server); err != nil {
		b.mu.Lock()
		b.httpServer = nil
		b.mu.Unlock()
		return err
	}

	b.logger.Info(
		"Blockfrost API listener started on " +
			b.config.ListenAddress,
	)

	// Monitor context for cancellation
	go func() { //nolint:gosec // G118: goroutine intentionally outlives ctx to perform graceful shutdown
		<-ctx.Done()
		b.mu.Lock()
		srv := b.httpServer
		b.httpServer = nil
		b.mu.Unlock()

		if srv != nil {
			b.logger.Debug(
				"context cancelled, shutting down " +
					"Blockfrost API server",
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
				b.logger.Error(
					"failed to shutdown Blockfrost "+
						"API server on context "+
						"cancellation",
					"error", err,
				)
			}
		}
	}()

	return nil
}

// Stop gracefully shuts down the HTTP server.
func (b *Blockfrost) Stop(
	ctx context.Context,
) error {
	b.mu.Lock()
	srv := b.httpServer
	b.httpServer = nil
	b.mu.Unlock()

	if srv != nil {
		b.logger.Debug(
			"shutting down Blockfrost API server",
		)
		if err := srv.Shutdown(ctx); err != nil {
			return fmt.Errorf(
				"failed to shutdown Blockfrost API "+
					"server: %w",
				err,
			)
		}
	}
	return nil
}

// startServer starts the HTTP server with deterministic
// error detection. It binds the listening socket first so
// port conflicts are detected immediately, then serves in
// a background goroutine.
func (b *Blockfrost) startServer(
	server *http.Server,
) error {
	useTLS := b.config.TLS.Enabled
	if useTLS {
		if err := tlsutil.ConfigureServerTLS(
			server,
			b.config.TLS.CertFilePath,
			b.config.TLS.KeyFilePath,
		); err != nil {
			return fmt.Errorf(
				"failed to load TLS keypair for Blockfrost API server: %w",
				err,
			)
		}
	}
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf(
			"failed to listen for Blockfrost API "+
				"server: %w",
			err,
		)
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
			b.logger.Error(
				"Blockfrost API server error",
				"error", serveErr,
			)
		}
	}()
	return nil
}
