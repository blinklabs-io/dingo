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

package blockfrost

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/internal/apilistener"
	"github.com/blinklabs-io/dingo/internal/httpcors"
)

// Leave time to report a stalled body before the 30-second write deadline.
const (
	defaultRequestBodyTimeout                   = 15 * time.Second
	defaultListenHost                           = "127.0.0.1"
	defaultMaxConcurrentMutations          uint = 4
	defaultMaxConcurrentMutationsPerClient      = 1
	defaultMaxConcurrentDRepDetails        uint = 4
	defaultMaxDRepDetailsPerClient              = 1
	defaultMaxAddressSummaries             uint = 4
	defaultMaxAddressSummariesPerClient         = 2
	defaultMaxChainSummaries               uint = 4
	defaultMaxChainSummariesPerClient           = 2
	defaultMaxAssetDetails                 uint = 4
	defaultMaxAssetDetailsPerClient             = 2
	defaultMaxMempoolAddresses             uint = 4
	defaultMaxMempoolAddressesPerClient         = 1
)

// Blockfrost is the Blockfrost-compatible REST API server.
type Blockfrost struct {
	config             BlockfrostConfig
	logger             *slog.Logger
	requestBodyTimeout time.Duration
	node               BlockfrostNode
	mutationAuth       mutationAuth
	mutationAdmission  *requestAdmission
	drepAdmission      *requestAdmission
	// addressSummaryAdmission is shared by every route that scans a whole
	// address or account history: the base and extended summaries, the
	// address transaction pages, the account summary, the address and account
	// totals, the account asset balances, and the account associated-address
	// pages.
	addressSummaryAdmission *requestAdmission
	// chainSummaryAdmission is shared by the routes that aggregate over the
	// whole chain or a whole epoch: the network supply and stake summary, the
	// latest epoch, a single epoch and the epochs before and after one. It is
	// separate from addressSummaryAdmission so a burst of address scans cannot
	// starve these routes, and the reverse.
	chainSummaryAdmission *requestAdmission
	// assetDetailAdmission bounds concurrent asset lookups, each of which
	// totals every live holding of the asset.
	assetDetailAdmission    *requestAdmission
	mempoolAddressAdmission *requestAdmission
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
		cfg.ListenAddress = net.JoinHostPort(defaultListenHost, "3000")
	}
	if cfg.MaxConcurrentMutations == 0 {
		cfg.MaxConcurrentMutations = defaultMaxConcurrentMutations
	}
	if cfg.MaxConcurrentMutationsPerClient == 0 {
		cfg.MaxConcurrentMutationsPerClient = defaultMaxConcurrentMutationsPerClient
	}
	return &Blockfrost{
		config:             cfg,
		requestBodyTimeout: defaultRequestBodyTimeout,
		logger:             logger,
		node:               node,
		mutationAuth: newMutationAuth(
			cfg.AuthToken,
		),
		mutationAdmission: newRequestAdmission(
			cfg.MaxConcurrentMutations,
			cfg.MaxConcurrentMutationsPerClient,
		),
		drepAdmission: newRequestAdmission(
			defaultMaxConcurrentDRepDetails,
			defaultMaxDRepDetailsPerClient,
		),
		addressSummaryAdmission: newRequestAdmission(
			defaultMaxAddressSummaries,
			defaultMaxAddressSummariesPerClient,
		),
		chainSummaryAdmission: newRequestAdmission(
			defaultMaxChainSummaries,
			defaultMaxChainSummariesPerClient,
		),
		assetDetailAdmission: newRequestAdmission(
			defaultMaxAssetDetails,
			defaultMaxAssetDetailsPerClient,
		),
		mempoolAddressAdmission: newRequestAdmission(
			defaultMaxMempoolAddresses,
			defaultMaxMempoolAddressesPerClient,
		),
		listener: apilistener.New("Blockfrost API", logger),
	}
}

type mutationAuth struct {
	enabled   bool
	tokenHash [sha256.Size]byte
}

func newMutationAuth(token string) mutationAuth {
	return mutationAuth{
		enabled:   token != "",
		tokenHash: sha256.Sum256([]byte(token)),
	}
}

func (a mutationAuth) authorized(r *http.Request) bool {
	if !a.enabled {
		return false
	}
	candidates := []string{
		r.Header.Get("project_id"),
		r.Header.Get("X-Blockfrost-Project-Id"),
	}
	scheme, value, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if ok && strings.EqualFold(scheme, "Bearer") {
		candidates = append(candidates, strings.TrimLeft(value, " "))
	}
	// Every credential is compared so a stale value in one header cannot mask
	// a valid one in another, and the work does not depend on which matched.
	matched := 0
	for _, provided := range candidates {
		providedHash := sha256.Sum256([]byte(provided))
		matched |= subtle.ConstantTimeCompare(
			providedHash[:],
			a.tokenHash[:],
		)
	}
	return matched == 1
}

type requestAdmission struct {
	mu           sync.Mutex
	active       uint
	activeClient map[string]uint
	maxActive    uint
	maxClient    uint
}

func newRequestAdmission(maxActive, maxClient uint) *requestAdmission {
	return &requestAdmission{
		activeClient: make(map[string]uint),
		maxActive:    maxActive,
		maxClient:    maxClient,
	}
}

func (a *requestAdmission) acquire(
	ctx context.Context,
	client string,
) (func(), bool) {
	select {
	case <-ctx.Done():
		return nil, false
	default:
	}

	a.mu.Lock()
	if a.active >= a.maxActive || a.activeClient[client] >= a.maxClient {
		a.mu.Unlock()
		return nil, false
	}
	a.active++
	a.activeClient[client]++
	a.mu.Unlock()

	return func() {
		a.mu.Lock()
		a.active--
		a.activeClient[client]--
		if a.activeClient[client] == 0 {
			delete(a.activeClient, client)
		}
		a.mu.Unlock()
	}, true
}

func requestClient(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func (b *Blockfrost) mutationHandler(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !b.mutationAuth.authorized(r) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(
				w,
				http.StatusUnauthorized,
				"Unauthorized",
				"missing or invalid authentication token",
			)
			return
		}
		release, ok := b.mutationAdmission.acquire(
			r.Context(),
			requestClient(r),
		)
		if !ok {
			w.Header().Set("Retry-After", "1")
			writeError(
				w,
				http.StatusTooManyRequests,
				"Too Many Requests",
				"transaction mutation capacity reached, try again later",
			)
			return
		}
		defer release()
		next(w, r)
	})
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
		"GET /api/v0/blocks/latest/txs/cbor",
		b.handleLatestBlockTransactionsCBOR,
	)
	mux.HandleFunc(
		"GET /api/v0/blocks/{hash_or_number}",
		b.handleBlock,
	)
	mux.HandleFunc(
		"GET /api/v0/blocks/{hash_or_number}/{section}",
		b.handleBlockSection,
	)
	mux.HandleFunc(
		"GET /api/v0/blocks/{hash_or_number}/txs/cbor",
		b.handleBlockTransactionsCBOR,
	)
	mux.HandleFunc(
		"GET /api/v0/blocks/epoch/{epoch_number}/slot/{slot_number}",
		b.handleBlockByEpochSlot,
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
	mux.HandleFunc("GET /api/v0/epochs/{number}", b.handleEpoch)
	mux.HandleFunc("GET /api/v0/epochs/{number}/next", b.handleEpochNext)
	mux.HandleFunc(
		"GET /api/v0/epochs/{number}/previous",
		b.handleEpochPrevious,
	)
	mux.HandleFunc("GET /api/v0/epochs/{number}/stakes", b.handleEpochStakes)
	mux.HandleFunc(
		"GET /api/v0/epochs/{number}/stakes/{pool_id}",
		b.handleEpochPoolStakes,
	)
	mux.HandleFunc("GET /api/v0/epochs/{number}/blocks", b.handleEpochBlocks)
	mux.HandleFunc(
		"GET /api/v0/epochs/{number}/blocks/{pool_id}",
		b.handleEpochPoolBlocks,
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
		"GET /api/v0/assets",
		b.handleAssets,
	)
	mux.HandleFunc(
		"GET /api/v0/assets/{asset}/{section}",
		b.handleAssetSection,
	)
	mux.HandleFunc(
		"GET /api/v0/scripts",
		b.handleScripts,
	)
	mux.HandleFunc(
		"GET /api/v0/scripts/{script_hash}",
		b.handleScript,
	)
	mux.HandleFunc(
		"GET /api/v0/scripts/{script_hash}/{section}",
		b.handleScriptSection,
	)
	mux.HandleFunc(
		"GET /api/v0/scripts/datum/{datum_hash}/cbor",
		b.handleDatumCBOR,
	)
	mux.HandleFunc(
		"GET /api/v0/mempool",
		b.handleMempool,
	)
	mux.HandleFunc(
		"GET /api/v0/mempool/{hash}",
		b.handleMempoolTransaction,
	)
	mux.HandleFunc(
		"GET /api/v0/mempool/addresses/{address}",
		b.handleMempoolAddress,
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
	mux.HandleFunc("GET /api/v0/pools/retired", b.handlePoolsRetired)
	mux.HandleFunc("GET /api/v0/pools/{pool_id}/history", b.handlePoolHistory)
	mux.HandleFunc("GET /api/v0/pools/{pool_id}/relays", b.handlePoolRelays)
	mux.HandleFunc(
		"GET /api/v0/pools/{pool_id}/delegators",
		b.handlePoolDelegators,
	)
	mux.HandleFunc("GET /api/v0/pools/{pool_id}/blocks", b.handlePoolBlocks)
	mux.HandleFunc("GET /api/v0/pools/{pool_id}/updates", b.handlePoolUpdates)
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
		"GET /api/v0/governance/dreps/{drep_id}/delegators",
		b.handleDRepDelegators,
	)
	mux.HandleFunc(
		"GET /api/v0/governance/dreps/{drep_id}/metadata",
		b.handleDRepMetadata,
	)
	mux.HandleFunc(
		"GET /api/v0/governance/dreps/{drep_id}/updates",
		b.handleDRepUpdates,
	)
	mux.HandleFunc(
		"GET /api/v0/governance/dreps/{drep_id}/votes",
		b.handleDRepVotes,
	)
	mux.HandleFunc("GET /api/v0/governance/committee", b.handleCommittee)
	mux.HandleFunc(
		"GET /api/v0/governance/committee/votes",
		b.handleCommitteeVotes,
	)
	mux.HandleFunc(
		"GET /api/v0/governance/committee/{cc_id}/votes",
		b.handleCommitteeMemberVotes,
	)
	mux.HandleFunc("GET /api/v0/governance/proposals", b.handleProposals)
	mux.HandleFunc(
		"GET /api/v0/governance/proposals/{first}",
		b.handleProposalByID,
	)
	mux.HandleFunc(
		"GET /api/v0/governance/proposals/{first}/{second}",
		b.handleProposalTwoSegments,
	)
	mux.HandleFunc(
		"GET /api/v0/governance/proposals/{first}/{second}/{third}",
		b.handleProposalThreeSegments,
	)
	mux.HandleFunc("GET /api/v0/pools/{pool_id}/votes", b.handlePoolVotes)
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
		"GET /api/v0/addresses/{address}/txs",
		b.handleAddressTransactionHashes,
	)
	mux.HandleFunc(
		"GET /api/v0/addresses/{address}/extended",
		b.handleAddressExtended,
	)
	mux.HandleFunc(
		"GET /api/v0/addresses/{address}/total",
		b.handleAddressTotal,
	)
	mux.HandleFunc(
		"GET /api/v0/addresses/{address}/utxos/{asset}",
		b.handleAddressAssetUTXOs,
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
	mux.Handle(
		"POST /api/v0/tx/submit",
		b.mutationHandler(b.handleTransactionSubmit),
	)
	mux.Handle(
		"POST /api/v0/utils/txs/evaluate",
		b.mutationHandler(b.handleTransactionEvaluate),
	)
	mux.Handle(
		"POST /api/v0/utils/txs/evaluate/utxos",
		b.mutationHandler(b.handleTransactionEvaluateUtxos),
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
		"GET /api/v0/accounts/{stake_address}/history",
		b.handleAccountStakeHistory,
	)
	mux.HandleFunc(
		"GET /api/v0/accounts/{stake_address}/mirs",
		b.handleAccountMIRs,
	)
	mux.HandleFunc(
		"GET /api/v0/accounts/{stake_address}/addresses/assets",
		b.handleAccountAddressAssets,
	)
	mux.HandleFunc(
		"GET /api/v0/accounts/{stake_address}/addresses/total",
		b.handleAccountAddressTotal,
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

	b.registerUnsupportedLiterals(mux)

	// Catch-all for any path not matched above. Registered
	// last so more specific patterns still take precedence;
	// ServeMux resolves by pattern specificity, not
	// registration order.
	mux.HandleFunc("/", b.handleNotFound)

	// Wrap handler with a request body size limit (1 MB)
	// as defense-in-depth against oversized payloads.
	const maxRequestBodyBytes int64 = 1 << 20 // 1 MB
	routed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if b.writeUnsupportedMethod(w, r) {
			return
		}
		mux.ServeHTTP(w, r)
	})
	limited := http.MaxBytesHandler(routed, maxRequestBodyBytes)
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
	startDone, err := b.listener.BeginStart()
	if err != nil {
		return err
	}
	defer b.listener.EndStart(startDone)

	host, _, err := net.SplitHostPort(b.config.ListenAddress)
	if err != nil {
		return fmt.Errorf("blockfrost: invalid listen address: %w", err)
	}
	if err := validateListenSecurity(
		host,
		b.config.AuthToken,
		b.config.TLS,
	); err != nil {
		return fmt.Errorf("blockfrost: %w", err)
	}

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

	// Watched before the bind so a context cancelled mid-bind still tears the
	// server down: the detach is what makes an in-flight bind close its own
	// socket.
	b.listener.Watch(ctx, server, apilistener.Graceful)

	// Bound with deterministic error detection: the socket is opened
	// synchronously so a port conflict surfaces here rather than in a log line
	// from a goroutine nobody is watching.
	served, err := b.listener.Bind(server, bindDone, b.config.TLS)
	if err != nil {
		b.listener.Unpublish(server)
		return err
	}
	if !served {
		// A context cancellation detached this server while it was binding, so
		// Bind closed the socket rather than serving it. Saying the listener
		// came up would be false.
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
	return b.listener.Stop(ctx, apilistener.Graceful)
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
			b.logger.Debug(
				"could not clear request body deadline",
				"error",
				err,
			)
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
