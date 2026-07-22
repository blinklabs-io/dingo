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

package ouroboros

import (
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	oprotocol "github.com/blinklabs-io/gouroboros/protocol"
	oblockfetch "github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	okeepalive "github.com/blinklabs-io/gouroboros/protocol/keepalive"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	oleiosvotes "github.com/blinklabs-io/gouroboros/protocol/leiosvotes"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	olocaltxmonitor "github.com/blinklabs-io/gouroboros/protocol/localtxmonitor"
	olocaltxsubmission "github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
	otxsubmission "github.com/blinklabs-io/gouroboros/protocol/txsubmission"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// defaultMaxChainsyncClients is the fallback maximum number of
// concurrent chainsync clients when the ChainsyncState is not
// available. Uses the same value as chainsync.DefaultMaxClients
// to keep defaults consistent.
const defaultMaxChainsyncClients = chainsync.DefaultMaxClients

// Mempool is the protocol-facing subset of the backend-neutral mempool. Keeping
// this interface local prevents networking from depending on FIFO internals.
type Mempool interface {
	AddTransaction(txType uint, txBytes []byte) error
	Transactions() []mempool.MempoolTransaction
	CapacityBytes() int64
	AddConsumer(connId ouroboros.ConnectionId) mempool.RelayConsumer
	RemoveConsumer(connId ouroboros.ConnectionId)
	Consumer(connId ouroboros.ConnectionId) mempool.RelayConsumer
}

func blockfetchConfig(
	opts ...oblockfetch.BlockFetchOptionFunc,
) oblockfetch.Config {
	cfg, err := oblockfetch.NewConfig(opts...)
	if err != nil {
		panic(fmt.Sprintf("invalid blockfetch config: %v", err))
	}
	return cfg
}

type Ouroboros struct {
	ConnManager              *connmanager.ConnectionManager
	PeerGov                  *peergov.PeerGovernor
	ChainsyncState           *chainsync.State
	EventBus                 *event.EventBus
	Mempool                  Mempool
	LedgerState              *ledger.LedgerState
	LeiosVotes               LeiosVoteHandler
	LeiosPipeline            LeiosPipelineHandler
	config                   OuroborosConfig
	blockfetchMetrics        *blockfetchMetrics
	protocolMetrics          *protocolMetrics
	blockFetchStarts         map[ouroboros.ConnectionId]time.Time
	blockFetchMutex          sync.Mutex
	blockfetchNoBlocksCounts map[ouroboros.ConnectionId]blockfetchNoBlocksState
	// ChainSync measurement tracking for peer scoring
	chainsyncStats map[ouroboros.ConnectionId]*chainsyncPeerStats
	chainsyncMutex sync.Mutex
	// Per-connection mutex to serialize chainsync restarts
	restartMu sync.Map // ouroboros.ConnectionId → *sync.Mutex
	// Per-peer rate limiter for TxSubmission server
	txSubmissionRateLimiter *txSubmissionRateLimiter
	// Cached Leios EB material fetched from peers. This lets NtC
	// ChainSync serve merged RB+EB blocks without coupling the chain
	// package to Leios prototype protocols.
	leiosEndorserBlocks map[string]*leiosEndorserBlockData
	leiosMu             sync.RWMutex
	// Waiters blocked in the NtC serving path until an endorser block's
	// transaction closure is cached. Keyed by leiosBlockKey(ebHash); each
	// channel is closed once a complete closure is stored for that key.
	leiosClosureWaiters map[string][]chan struct{}
	// NtC CertRB closure-resolution metrics.
	leiosMetrics *leiosMetrics

	// Per-connection serialization and bound for asynchronous leios-fetch
	// client operations (manifest and EB-tx fetches). The leios-fetch client
	// is strict request/response, so operations on one connection are
	// serialized; running them off the leios-notify handler keeps a
	// multi-second EB fetch from head-of-line blocking every later offer on
	// the connection.
	leiosFetchGuards sync.Map // ouroboros.ConnectionId → *leiosFetchGuard
	// EB hashes with a fetch already in progress, so a given endorser block is
	// fetched once across all connections (it is offered on every connection).
	leiosFetchInProgress sync.Map // string(eb hash) → struct{}

	// Locally-forged EB broadcast log (cursors are owned by the log).
	leiosEBLog *leiosForgedEBLog

	// Asynchronous best-effort persistence of fetched endorser blocks to the
	// blob store for historical serving. The blob write (CBOR encode + commit)
	// is moved off the leios-fetch hot path onto a single background writer so
	// it does not serialize against block application during catch-up. Jobs
	// coalesce by EB hash — a complete job (with txs) supersedes a
	// manifest-only one — which also elides the backfiller's duplicate manifest
	// write. Lazily started on first enqueue; stopped via StopLeiosPersistWriter.
	leiosPersistOnce     sync.Once
	leiosPersistStopOnce sync.Once
	leiosPersistStarted  atomic.Bool
	leiosPersistMu       sync.Mutex
	leiosPersistPending  map[string]*leiosPersistJob
	leiosPersistSignal   chan struct{}
	leiosPersistStop     chan struct{}
	leiosPersistDone     chan struct{}
	leiosPersistDropped  atomic.Uint64
}

// chainsyncPeerStats tracks ChainSync performance metrics per peer connection.
type chainsyncPeerStats struct {
	lastObservationTime time.Time
	headerCount         int64
}

type OuroborosConfig struct {
	Logger          *slog.Logger
	EventBus        *event.EventBus
	ConnManager     *connmanager.ConnectionManager
	IntersectPoints []ocommon.Point
	NetworkMagic    uint32
	PeerSharing     bool
	IntersectTip    bool
	PromRegistry    prometheus.Registerer
	// ChainsyncBlockTimeout bounds how long NtN chain-sync can wait for a
	// block reply after a peer enters a server-agency state. Values below
	// the protocol maximum are raised to the protocol maximum.
	ChainsyncBlockTimeout time.Duration
	// MaxTxSubmissionsPerSecond is the maximum number of transaction
	// submissions accepted per peer per second via the TxSubmission
	// mini-protocol. A value of 0 disables rate limiting, which is the
	// default because TxSubmission is pull-based and the local server
	// already controls the request pace. A negative value also disables
	// rate limiting.
	MaxTxSubmissionsPerSecond int
	// ChainsyncIngressEligible reports whether a peer is allowed to
	// feed chainsync events into the ledger pipeline. This lets us
	// keep inbound/public noise out of ledger ingress while still
	// tracking peer tips separately for selection and observability.
	ChainsyncIngressEligible func(ouroboros.ConnectionId) bool
	// Enable experimental Leios protocol support
	EnableLeios bool
	// LeiosClosureWaitTimeout optionally overrides how long the NtC chainsync
	// server waits for a certifying ranking block's endorser block transaction
	// closure to become available before closing the connection. When 0 (the
	// default) the wait is derived from the ledger's Leios pipeline timing
	// (EndorserBlockWaitSlots × slot length), matching ledger application.
	LeiosClosureWaitTimeout time.Duration
	// EnableLeiosVotes initiates the standalone leios-votes mini-protocol
	// (protocol 20) toward peers. This is a dingo extension that is ahead
	// of the IOG Leios prototype: the prototype Haskell node does not run a
	// leios-votes responder, so sending on protocol 20 makes its muxer tear
	// down the whole bearer (observed as "connection reset by peer"). On the
	// prototype network, votes are instead pushed inline over leios-notify
	// (MsgVotes, tag 4). Keep this disabled when talking to the prototype;
	// enable it only for peers known to support the standalone protocol.
	EnableLeiosVotes bool
	// EnableLeiosTxFetch requests endorser-block transaction bodies over
	// leios-fetch in response to the peer's transactions offer
	// (MsgBlockTxsOffer) — the relay's signal that the EB's transactions are
	// ready. It is enabled on the Leios network (wired from
	// enableLeiosNetworking in node.go). Fetching before that offer (e.g. right
	// after the EB manifest) makes the IOG prototype relay reset the
	// connection, so the fetch is gated on the txs offer rather than disabled
	// outright. Best-effort: a fetch failure never tears down the shared
	// connection.
	EnableLeiosTxFetch bool
	// LeiosTxFetchTailBudget bounds how long an endorser-block tx fetch keeps
	// re-requesting the still-diffusing tail (the relay diffuses an EB's
	// transactions over several seconds, so the last partial window may lag)
	// before giving up, instead of aborting on the first no-progress round.
	// Zero disables tail-retry (fetch aborts on the first miss). Sourced from
	// the Leios diffusion window in node.go.
	LeiosTxFetchTailBudget time.Duration
	// KeepAliveTimeout overrides how long the keep-alive client waits for a
	// peer's pong before treating the connection as failed. Zero uses the
	// gouroboros default (10s). It is raised on the Musashi prototype network
	// (to okeepalive.ServerTimeout) so a pong delayed by the single relay's
	// saturated shared muxer does not trigger a false-positive drop and an
	// expensive reconnect + fork rollback; see keepaliveConnOpts. Values above
	// that bound are clamped there. Unset (0) on other networks, so dead peers
	// are still evicted quickly.
	KeepAliveTimeout time.Duration
}

type blockfetchMetrics struct {
	servedBlockCount   prometheus.Counter
	blockDelay         prometheus.Gauge
	lateBlocks         prometheus.Counter
	blockDelayCdfOne   prometheus.Gauge
	blockDelayCdfThree prometheus.Gauge
	blockDelayCdfFive  prometheus.Gauge
	totalBlocksFetched atomic.Int64
	blocksUnder1s      atomic.Int64
	blocksUnder3s      atomic.Int64
	blocksUnder5s      atomic.Int64
}

func NewOuroboros(cfg OuroborosConfig) *Ouroboros {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	cfg.Logger = cfg.Logger.With("component", "ouroboros")
	cfg.ChainsyncBlockTimeout = effectiveChainsyncBlockTimeout(
		cfg.ChainsyncBlockTimeout,
	)
	o := &Ouroboros{
		config:                   cfg,
		EventBus:                 cfg.EventBus,
		ConnManager:              cfg.ConnManager,
		blockFetchStarts:         make(map[ouroboros.ConnectionId]time.Time),
		blockfetchNoBlocksCounts: make(map[ouroboros.ConnectionId]blockfetchNoBlocksState),
		chainsyncStats:           make(map[ouroboros.ConnectionId]*chainsyncPeerStats),
		leiosEndorserBlocks:      make(map[string]*leiosEndorserBlockData),
		leiosClosureWaiters:      make(map[string][]chan struct{}),
		leiosEBLog:               newLeiosForgedEBLog(),
	}
	// Initialize per-peer TxSubmission rate limiter
	txRate := cfg.MaxTxSubmissionsPerSecond
	if txRate > 0 {
		// Burst allows small batches: 2x the per-second rate
		burst := float64(txRate) * 2
		o.txSubmissionRateLimiter = newTxSubmissionRateLimiter(
			float64(txRate),
			burst,
		)
	}
	if cfg.PromRegistry != nil {
		o.initBlockfetchMetrics()
		o.initProtocolMetrics()
		o.initLeiosMetrics()
	}
	return o
}

func (o *Ouroboros) initBlockfetchMetrics() {
	promautoFactory := promauto.With(o.config.PromRegistry)
	o.blockfetchMetrics = &blockfetchMetrics{}
	o.blockfetchMetrics.servedBlockCount = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_served_block_count_int",
			Help: "total blocks served to clients",
		},
	)
	o.blockfetchMetrics.blockDelay = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_blockfetchclient_blockdelay_s",
		Help: "delay in seconds for the most recent block fetch",
	})
	o.blockfetchMetrics.lateBlocks = promautoFactory.NewCounter(prometheus.CounterOpts{
		Name: "cardano_node_metrics_blockfetchclient_lateblocks",
		Help: "blocks that took more than 5 seconds to fetch",
	})
	o.blockfetchMetrics.blockDelayCdfOne = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_blockfetchclient_blockdelay_cdfOne",
		Help: "percentage of blocks fetched in less than 1 second",
	})
	o.blockfetchMetrics.blockDelayCdfThree = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "cardano_node_metrics_blockfetchclient_blockdelay_cdfThree",
			Help: "percentage of blocks fetched in less than 3 seconds",
		},
	)
	o.blockfetchMetrics.blockDelayCdfFive = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_blockfetchclient_blockdelay_cdfFive",
		Help: "percentage of blocks fetched in less than 5 seconds",
	})
}

func (o *Ouroboros) ConfigureListeners(
	listeners []connmanager.ListenerConfig,
) []connmanager.ListenerConfig {
	tmpListeners := make([]connmanager.ListenerConfig, len(listeners))
	for idx, l := range listeners {
		if l.UseNtC {
			// Node-to-client
			l.ConnectionOpts = append(
				l.ConnectionOpts,
				ouroboros.WithNetworkMagic(o.config.NetworkMagic),
				ouroboros.WithChainSyncConfig(
					ochainsync.NewConfig(
						o.chainsyncServerConnOpts()...,
					),
				),
				ouroboros.WithLocalStateQueryConfig(
					olocalstatequery.NewConfig(
						o.localstatequeryServerConnOpts()...,
					),
				),
				ouroboros.WithLocalTxMonitorConfig(
					olocaltxmonitor.NewConfig(
						o.localtxmonitorServerConnOpts()...,
					),
				),
				ouroboros.WithLocalTxSubmissionConfig(
					olocaltxsubmission.NewConfig(
						o.localtxsubmissionServerConnOpts()...,
					),
				),
			)
		} else {
			// Node-to-node config: full duplex with both client and
			// server handlers, matching cardano-node behavior. This
			// allows a single inbound TCP connection to carry both
			// directions of the Ouroboros mini-protocols.
			l.ConnectionOpts = append(
				l.ConnectionOpts,
				ouroboros.WithFullDuplex(true),
				ouroboros.WithKeepAlive(true),
				ouroboros.WithKeepAliveConfig(
					okeepalive.NewConfig(o.keepaliveConnOpts()...),
				),
				ouroboros.WithPeerSharing(o.config.PeerSharing),
				ouroboros.WithNetworkMagic(o.config.NetworkMagic),
				ouroboros.WithPeerSharingConfig(
					o.peerSharingConfig(),
				),
				ouroboros.WithTxSubmissionConfig(
					otxsubmission.NewConfig(
						slices.Concat(
							o.txsubmissionClientConnOpts(),
							o.txsubmissionServerConnOpts(),
						)...,
					),
				),
				ouroboros.WithChainSyncConfig(
					ochainsync.NewConfig(
						slices.Concat(
							o.chainsyncClientConnOpts(),
							o.chainsyncServerConnOpts(),
						)...,
					),
				),
				ouroboros.WithBlockFetchConfig(
					blockfetchConfig(
						slices.Concat(
							o.blockfetchClientConnOpts(),
							o.blockfetchServerConnOpts(),
						)...,
					),
				),
			)
			if o.config.EnableLeios {
				l.ConnectionOpts = append(
					l.ConnectionOpts,
					ouroboros.WithLeiosFetchConfig(
						oleiosfetch.NewConfig(
							slices.Concat(
								o.leiosfetchClientConnOpts(),
								o.leiosfetchServerConnOpts(),
							)...,
						),
					),
					ouroboros.WithLeiosNotifyConfig(
						oleiosnotify.NewConfig(
							slices.Concat(
								o.leiosnotifyClientConnOpts(),
								o.leiosnotifyServerConnOpts(),
							)...,
						),
					),
					ouroboros.WithLeiosVotesConfig(
						oleiosvotes.NewConfig(
							slices.Concat(
								o.leiosvotesClientConnOpts(),
								o.leiosvotesServerConnOpts(),
							)...,
						),
					),
				)
			}
		}
		tmpListeners[idx] = l
	}
	return tmpListeners
}

func (o *Ouroboros) OutboundConnOpts() []ouroboros.ConnectionOptionFunc {
	opts := []ouroboros.ConnectionOptionFunc{
		ouroboros.WithNetworkMagic(o.config.NetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithKeepAliveConfig(
			okeepalive.NewConfig(o.keepaliveConnOpts()...),
		),
		ouroboros.WithFullDuplex(true),
		ouroboros.WithPeerSharing(o.config.PeerSharing),
		ouroboros.WithPeerSharingConfig(
			o.peerSharingConfig(),
		),
		ouroboros.WithTxSubmissionConfig(
			otxsubmission.NewConfig(
				slices.Concat(
					o.txsubmissionClientConnOpts(),
					o.txsubmissionServerConnOpts(),
				)...,
			),
		),
		ouroboros.WithChainSyncConfig(
			ochainsync.NewConfig(
				slices.Concat(
					o.chainsyncClientConnOpts(),
					o.chainsyncServerConnOpts(),
				)...,
			),
		),
		ouroboros.WithBlockFetchConfig(
			blockfetchConfig(
				slices.Concat(
					o.blockfetchClientConnOpts(),
					o.blockfetchServerConnOpts(),
				)...,
			),
		),
	}
	if o.config.EnableLeios {
		opts = append(
			opts,
			ouroboros.WithLeiosFetchConfig(
				oleiosfetch.NewConfig(
					slices.Concat(
						o.leiosfetchClientConnOpts(),
						o.leiosfetchServerConnOpts(),
					)...,
				),
			),
			ouroboros.WithLeiosNotifyConfig(
				oleiosnotify.NewConfig(
					slices.Concat(
						o.leiosnotifyClientConnOpts(),
						o.leiosnotifyServerConnOpts(),
					)...,
				),
			),
			ouroboros.WithLeiosVotesConfig(
				oleiosvotes.NewConfig(
					slices.Concat(
						o.leiosvotesClientConnOpts(),
						o.leiosvotesServerConnOpts(),
					)...,
				),
			),
		)
	}
	return opts
}

func (o *Ouroboros) HandleConnClosedEvent(evt event.Event) {
	e, ok := evt.Data.(connmanager.ConnectionClosedEvent)
	if !ok {
		o.config.Logger.Warn(
			"received unexpected event data type for connection closed event",
			"expected", "connmanager.ConnectionClosedEvent",
			"got", fmt.Sprintf("%T", evt.Data),
		)
		return
	}
	connId := e.ConnectionId

	// Record connection stability observation for peer scoring
	// Connection closure indicates reduced stability
	if o.PeerGov != nil {
		// Treat connection closure as negative stability signal (0.0)
		o.PeerGov.UpdatePeerConnectionStability(connId, 0.0)
	}

	// Remove any chainsync client state
	if o.ChainsyncState != nil {
		o.ChainsyncState.RemoveClient(connId)
		o.ChainsyncState.RemoveClientConnId(connId)
	}
	// Remove mempool consumer
	if o.Mempool != nil {
		o.Mempool.RemoveConsumer(connId)
	}
	// Clean up any pending block fetch start times and NoBlocks counters
	o.blockFetchMutex.Lock()
	delete(o.blockFetchStarts, connId)
	delete(o.blockfetchNoBlocksCounts, connId)
	o.blockFetchMutex.Unlock()
	// Clean up chainsync stats
	o.chainsyncMutex.Lock()
	delete(o.chainsyncStats, connId)
	o.chainsyncMutex.Unlock()
	// Clean up per-connection restart mutex
	o.restartMu.Delete(connId)
	// Clean up TxSubmission rate limiter state
	if o.txSubmissionRateLimiter != nil {
		o.txSubmissionRateLimiter.RemovePeer(connId)
	}
	// Clean up Leios vote serving state
	if o.LeiosVotes != nil {
		o.LeiosVotes.RemoveConnection(leiosConnectionIdString(connId))
	}
	// Release the EB log cursor for this connection; frees any log
	// entries that were only being held for this connection.
	o.leiosEBLog.removeConn(leiosConnectionIdString(connId))
	// Drop the per-connection leios-fetch guard. In-flight fetch goroutines
	// hold their own reference, so they finish safely after this.
	o.leiosFetchGuards.Delete(connId)
}

func (o *Ouroboros) HandlePeerEligibilityChangedEvent(evt event.Event) {
	e, ok := evt.Data.(peergov.PeerEligibilityChangedEvent)
	if !ok {
		o.config.Logger.Warn(
			"received unexpected event data type for peer eligibility change event",
			"expected", "peergov.PeerEligibilityChangedEvent",
			"got", fmt.Sprintf("%T", evt.Data),
		)
		return
	}
	_ = o.reconcileChainsyncIngressAdmission(
		e.ConnectionId,
		e.Eligible,
	)
}

func (o *Ouroboros) HandleOutboundConnEvent(evt event.Event) {
	e, ok := evt.Data.(peergov.OutboundConnectionEvent)
	if !ok {
		o.config.Logger.Warn(
			"received unexpected event data type for outbound connection event",
			"expected", "peergov.OutboundConnectionEvent",
			"got", fmt.Sprintf("%T", evt.Data),
		)
		return
	}
	connId := e.ConnectionId

	// Record connection stability observation for peer scoring
	// Successful outbound connection establishment indicates good stability
	if o.PeerGov != nil {
		// Treat successful connection as positive stability signal (1.0)
		o.PeerGov.UpdatePeerConnectionStability(connId, 1.0)
	}

	// Log the negotiated node-to-node protocol version and diffusion mode for
	// this connection. This is the key diagnostic for Leios mini-protocol
	// interop: it tells us which NtN version the peer agreed to, which
	// determines whether the peer runs responders for the Leios protocol IDs.
	if conn := o.ConnManager.GetConnectionById(connId); conn != nil {
		ver, verData := conn.ProtocolVersion()
		fullDuplex := false
		peerSharing := false
		if verData != nil {
			// DiffusionMode() returns the raw wire bool, where true means
			// initiator-only (half duplex) and false means
			// initiator-and-responder (full duplex). Compare against the
			// constant so the log reports the negotiated mode correctly.
			fullDuplex = verData.DiffusionMode() == oprotocol.DiffusionModeInitiatorAndResponder
			peerSharing = verData.PeerSharing()
		}
		o.config.Logger.Debug(
			"outbound connection handshake negotiated",
			"component", "network",
			"connection_id", connId.String(),
			"ntn_version", ver,
			"full_duplex", fullDuplex,
			"peer_sharing", peerSharing,
			"enable_leios", o.config.EnableLeios,
		)
	}

	// Start chainsync client for this connection if not already tracking it.
	// If chainsync negotiation fails, we return early and do NOT start
	// txsubmission -- the connection is unusable if chainsync fails because
	// the Ouroboros handshake/protocol negotiation has already failed and the
	// peer will reject further mini-protocol starts on this connection.
	if o.ChainsyncState != nil {
		// Registration runs before the tracked client exists, so the
		// direction-aware fallback in shouldPublishChainsyncToLedger
		// cannot see us yet. Outbound keeps its legacy default of
		// eligible when no ChainsyncIngressEligible policy is wired.
		ingressEligible := true
		if o.config.ChainsyncIngressEligible != nil {
			ingressEligible = o.config.ChainsyncIngressEligible(connId)
		}
		shouldStartChainsync := o.registerTrackedChainsyncClient(
			connId,
			ingressEligible,
			true, // startedAsOutbound
		)
		if shouldStartChainsync {
			if err := o.chainsyncClientStart(connId); err != nil {
				// Roll back the registration on failure
				o.ChainsyncState.RemoveClientConnId(connId)
				o.config.Logger.Error(
					"failed to start chainsync client",
					"error",
					err,
				)
				return
			}
			o.config.Logger.Debug(
				"started chainsync client",
				"connection_id", connId.String(),
				"total_clients", o.ChainsyncState.ClientConnCount(),
			)
		} else if !o.ChainsyncState.HasClientConnId(connId) {
			// Not already tracked and TryAdd failed means limit reached
			o.config.Logger.Debug(
				"chainsync client limit reached, skipping eligible admission",
				"connection_id", connId.String(),
			)
		}
	}
	// Start txsubmission client
	if err := o.txsubmissionClientStart(connId); err != nil {
		o.config.Logger.Error(
			"failed to start txsubmission client",
			"error",
			err,
		)
		return
	}
	// Start leiosnotify client
	if o.config.EnableLeios {
		if err := o.leiosnotifyClientStart(connId); err != nil {
			o.config.Logger.Error(
				"failed to start leiosnotify client",
				"error",
				err,
			)
			return
		}
		// Start leiosvotes client only when the standalone leios-votes
		// protocol is enabled. The Leios prototype relays do not run a
		// protocol-20 responder, so initiating it resets the connection;
		// there, votes arrive inline over leios-notify instead.
		if o.config.EnableLeiosVotes {
			if err := o.leiosvotesClientStart(connId); err != nil {
				o.config.Logger.Error(
					"failed to start leiosvotes client",
					"error",
					err,
				)
				return
			}
		}
	}
}

// HandleInboundConnEvent starts client-side mini-protocols on full-duplex
// inbound connections. When the remote peer negotiated InitiatorAndResponder
// mode, a single TCP connection can carry both directions of the Ouroboros
// protocols, matching cardano-node's connection manager behavior.
func (o *Ouroboros) HandleInboundConnEvent(evt event.Event) {
	e, ok := evt.Data.(connmanager.InboundConnectionEvent)
	if !ok {
		o.config.Logger.Warn(
			"received unexpected event data type for inbound connection event",
			"expected", "connmanager.InboundConnectionEvent",
			"got", fmt.Sprintf("%T", evt.Data),
		)
		return
	}
	// Skip node-to-client connections — N2N mini-protocols don't
	// apply to local client connections.
	if e.IsNtC {
		return
	}
	connId := e.ConnectionId

	// Look up the connection to check its negotiated diffusion mode
	conn := o.ConnManager.GetConnectionById(connId)
	if conn == nil {
		o.config.Logger.Debug(
			"inbound connection not found, skipping client start",
			"connection_id", connId.String(),
		)
		return
	}

	// Only start client protocols if the peer negotiated full duplex
	_, versionData := conn.ProtocolVersion()
	if versionData == nil || versionData.DiffusionMode() != oprotocol.DiffusionModeInitiatorAndResponder {
		o.config.Logger.Debug(
			"inbound connection is not full-duplex, skipping client start",
			"connection_id", connId.String(),
		)
		return
	}

	o.config.Logger.Info(
		"full-duplex inbound connection, starting client protocols",
		"connection_id", connId.String(),
	)

	// Start chainsync client on this full-duplex inbound connection.
	// If the chainsync client fails (e.g. intersection not found,
	// stale connection ID after rollback), close the connection so
	// it doesn't consume a per-IP slot as a zombie. Zombie
	// connections that lack chainsync but count toward the per-IP
	// limit can prevent functional reconnections, causing permanent
	// chainsync stalls after rollback events.
	//
	// Ingress eligibility is delegated to ChainsyncIngressEligible so a
	// trusted upstream peer that happens to dial us first still feeds
	// the ledger. Random inbound peers remain observability-only because
	// peergov filters them at chainSelectionEligible. The default when no
	// policy is wired is fail-closed for inbound: we will not feed the
	// ledger from a peer we never decided to trust.
	if o.ChainsyncState != nil {
		ingressEligible := false
		if o.config.ChainsyncIngressEligible != nil {
			ingressEligible = o.config.ChainsyncIngressEligible(connId)
		}
		if o.registerTrackedChainsyncClient(connId, ingressEligible, false) {
			if err := o.chainsyncClientStart(connId); err != nil {
				o.ChainsyncState.RemoveClientConnId(connId)
				o.config.Logger.Warn(
					"chainsync client failed on inbound connection, closing to free per-IP slot",
					"error", err,
					"connection_id", connId.String(),
				)
				conn.Close()
				return
			} else {
				o.config.Logger.Debug(
					"started chainsync client on inbound connection",
					"connection_id", connId.String(),
					"total_clients", o.ChainsyncState.ClientConnCount(),
				)
			}
		}
	}
	// Start TxSubmission client on full-duplex inbound connections.
	// This registers a mempool consumer and sends Init() so we can
	// offer our transactions to the peer. Without this, peers that
	// connect TO us (and whose inbound suppresses our outbound dial)
	// would never receive our mempool transactions.
	//
	// The remote peer's TxSubmission server is guaranteed to be ready
	// because the Ouroboros handshake completed bilaterally before
	// either side starts mini-protocol messages — the remote's
	// NewConnection() has finished and all protocol handlers are
	// registered by the time our InboundConnectionEvent fires.
	if o.Mempool != nil {
		if err := o.txsubmissionClientStart(connId); err != nil {
			o.config.Logger.Warn(
				"txsubmission client failed on inbound connection",
				"error", err,
				"connection_id", connId.String(),
			)
			// Non-fatal: the connection is still useful for
			// chainsync, keep-alive, and peer-sharing.
		} else {
			o.config.Logger.Debug(
				"started txsubmission client on inbound connection",
				"connection_id", connId.String(),
			)
		}
	}
	if o.config.EnableLeios {
		if err := o.leiosnotifyClientStart(connId); err != nil {
			o.config.Logger.Warn(
				"leiosnotify client failed on inbound connection",
				"error", err,
				"connection_id", connId.String(),
			)
		} else {
			o.config.Logger.Debug(
				"started leiosnotify client on inbound connection",
				"connection_id", connId.String(),
			)
		}
		if o.config.EnableLeiosVotes {
			if err := o.leiosvotesClientStart(connId); err != nil {
				o.config.Logger.Warn(
					"leiosvotes client failed on inbound connection",
					"error", err,
					"connection_id", connId.String(),
				)
			} else {
				o.config.Logger.Debug(
					"started leiosvotes client on inbound connection",
					"connection_id", connId.String(),
				)
			}
		}
	}
}
