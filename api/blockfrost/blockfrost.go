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
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/blinklabs-io/dingo/internal/apilistener"
	"github.com/blinklabs-io/dingo/internal/httpcors"
)

// blockfrostProjectIDHeader is the header real Blockfrost clients send
// their API key in. Presenting the shared token there authenticates
// exactly as presenting it via "Authorization: Bearer <token>" does --
// see ARCHITECTURE.md/README.md's "API security" section for this
// compatibility decision.
const blockfrostProjectIDHeader = "project_id"

// Leave time to report a stalled body before the 30-second write deadline.
const defaultRequestBodyTimeout = 15 * time.Second

// Blockfrost is the Blockfrost-compatible REST API server.
type Blockfrost struct {
	config             BlockfrostConfig
	logger             *slog.Logger
	requestBodyTimeout time.Duration
	node               BlockfrostNode
	// listener owns the start/stop protocol, including releasing the
	// listening socket as part of what Stop waits for -- see
	// internal/apilistener.
	listener *apilistener.Listener
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
		config:             cfg,
		requestBodyTimeout: defaultRequestBodyTimeout,
		logger:             logger,
		node:               node,
		listener:           apilistener.New("Blockfrost API", logger),
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
		"POST /api/v0/utils/txs/evaluate",
		b.handleTransactionEvaluate,
	)
	mux.HandleFunc(
		"POST /api/v0/utils/txs/evaluate/utxos",
		b.handleTransactionEvaluateUtxos,
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
	return httpcors.Handler(
		limited,
		httpcors.Config{
			AllowedOrigins: b.config.CORSAllowedOrigins,
		},
	)
}

// Start starts the HTTP server in a background goroutine.
func (b *Blockfrost) Start(
	ctx context.Context,
) error {
	server, bindDone, err := b.listener.Publish(func() *http.Server {
		return &http.Server{
			Addr:              b.config.ListenAddress,
			Handler:           b.handler(),
			ReadHeaderTimeout: 60 * time.Second,
			ReadTimeout:       60 * time.Second,
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
		job, _ := b.listener.TakeIf(server)
		// Nil when a concurrent Stop won the detach -- it owns the teardown
		// and its caller is already waiting on it -- or when this server was
		// already stopped and a restart published another one, which is not
		// this monitor's to touch. Either way there is nothing to do here.
		if job != nil {
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
			if err := b.listener.Shutdown(
				shutdownCtx, job, apilistener.Graceful,
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

	// Bound with deterministic error detection: the socket is opened
	// synchronously so a port conflict surfaces here rather than in a log line
	// from a goroutine nobody is watching.
	served, err := b.listener.Bind(server, bindDone, b.config.TLS)
	if err != nil {
		b.listener.Unpublish(server)
		return err
	}
	if !served {
		// A concurrent Stop or context cancellation detached this server while
		// it was binding, so Bind closed the socket rather than serving it.
		// Saying the listener came up would be false.
		return nil
	}

	b.logger.Info(
		"Blockfrost API listener started on " +
			b.config.ListenAddress,
	)

	return nil
}

// Stop gracefully shuts down the HTTP server, and does not return until the
// listening socket has been released -- see internal/apilistener.
func (b *Blockfrost) Stop(
	ctx context.Context,
) error {
	job, inFlight := b.listener.Take()
	if job == nil {
		return b.listener.AwaitTeardown(ctx, inFlight)
	}
	b.logger.Debug("shutting down Blockfrost API server")
	return b.listener.Shutdown(ctx, job, apilistener.Graceful)
}

// readRequestBody bounds both bytes and time before transaction processing.
func (b *Blockfrost) readRequestBody(
	w http.ResponseWriter,
	r *http.Request,
	limit int64,
) ([]byte, error) {
	b.setRequestBodyDeadline(w)
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	body, err := io.ReadAll(r.Body)
	if err == nil {
		// A completed read must not expire during transaction processing or
		// a later request. Preserve failed-read deadlines: net/http can still
		// drain the unread body before sending the error response.
		if err := http.NewResponseController(w).SetReadDeadline(time.Time{}); err != nil &&
			!errors.Is(err, http.ErrNotSupported) {
			b.logger.Debug("could not clear request body deadline", "error", err)
		}
	}
	return body, err
}

func (b *Blockfrost) setRequestBodyDeadline(w http.ResponseWriter) {
	if err := http.NewResponseController(w).SetReadDeadline(
		time.Now().Add(b.requestBodyTimeout),
	); err != nil && !errors.Is(err, http.ErrNotSupported) {
		b.logger.Debug("could not bound request body read", "error", err)
	}
}
