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
	oblockfetch "github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oprotocol "github.com/blinklabs-io/gouroboros/protocol"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	olocaltxmonitor "github.com/blinklabs-io/gouroboros/protocol/localtxmonitor"
	olocaltxsubmission "github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
	opeersharing "github.com/blinklabs-io/gouroboros/protocol/peersharing"
	otxsubmission "github.com/blinklabs-io/gouroboros/protocol/txsubmission"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// maxChainsyncClients is the maximum number of concurrent chainsync clients.
const maxChainsyncClients = 10

type Ouroboros struct {
	ConnManager      *connmanager.ConnectionManager
	PeerGov          *peergov.PeerGovernor
	ChainsyncState   *chainsync.State
	EventBus         *event.EventBus
	Mempool          *mempool.Mempool
	LedgerState      *ledger.LedgerState
	config           OuroborosConfig
	metrics          *blockfetchMetrics
	blockFetchStarts map[ouroboros.ConnectionId]time.Time
	blockFetchMutex  sync.Mutex
	// ChainSync measurement tracking for peer scoring
	chainsyncStats map[ouroboros.ConnectionId]*chainsyncPeerStats
	chainsyncMutex sync.Mutex
	// hasBlocks is set to true once at least one block has been
	// added to the chain. Used by the ChainSync server to reject
	// FindIntersect without acquiring any locks when the chain is
	// empty (prevents reader starvation from continuous writes).
	hasBlocks atomic.Bool
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
}

type blockfetchMetrics struct {
	servedBlockCount   prometheus.Counter
	blockDelay         prometheus.Gauge
	lateBlocks         prometheus.Counter
	blockDelayCdfOne   prometheus.Gauge
	blockDelayCdfThree prometheus.Gauge
	blockDelayCdfFive  prometheus.Gauge
	totalBlocksFetched int64
	blocksUnder1s      int64
	blocksUnder3s      int64
	blocksUnder5s      int64
}

func NewOuroboros(cfg OuroborosConfig) *Ouroboros {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	cfg.Logger = cfg.Logger.With("component", "ouroboros")
	// Override the global chainsync StateMap timeouts.
	// The chainsync server protocol uses the global StateMap directly
	// without applying config overrides (unlike the client which
	// copies and modifies it). Increase from the defaults to avoid
	// false timeouts when callbacks block on lock contention.
	for state, entry := range ochainsync.StateMap {
		switch state.Name {
		case "Intersect":
			entry.Timeout = 30 * time.Second
			ochainsync.StateMap[state] = entry
		case "Idle":
			entry.Timeout = 300 * time.Second
			ochainsync.StateMap[state] = entry
		}
	}
	// Override the global TxSubmission StateMap timeouts.
	// TxIdsBlocking defaults to 60s which is too short when
	// the mempool is empty (common in DevNets). Increase to
	// 300s to match the Idle timeout and avoid unnecessary
	// connection teardowns.
	for state, entry := range otxsubmission.StateMap {
		if state.Name == "TxIdsBlocking" {
			entry.Timeout = 300 * time.Second
			otxsubmission.StateMap[state] = entry
		}
	}
	o := &Ouroboros{
		config:           cfg,
		EventBus:         cfg.EventBus,
		ConnManager:      cfg.ConnManager,
		blockFetchStarts: make(map[ouroboros.ConnectionId]time.Time),
		chainsyncStats:   make(map[ouroboros.ConnectionId]*chainsyncPeerStats),
	}
	if cfg.PromRegistry != nil {
		o.initMetrics()
	}
	return o
}

// SetHasBlocks marks that the chain contains at least one block.
// Call this after loading an existing database so the ChainSync
// server can accept FindIntersect immediately instead of waiting
// for the first blockfetch to arrive.
func (o *Ouroboros) SetHasBlocks() {
	o.hasBlocks.Store(true)
}

func (o *Ouroboros) initMetrics() {
	promautoFactory := promauto.With(o.config.PromRegistry)
	o.metrics = &blockfetchMetrics{}
	o.metrics.servedBlockCount = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_served_block_count_int",
			Help: "total blocks served to clients",
		},
	)
	o.metrics.blockDelay = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_blockfetchclient_blockdelay_s",
		Help: "delay in seconds for the most recent block fetch",
	})
	o.metrics.lateBlocks = promautoFactory.NewCounter(prometheus.CounterOpts{
		Name: "cardano_node_metrics_blockfetchclient_lateblocks",
		Help: "blocks that took more than 5 seconds to fetch",
	})
	o.metrics.blockDelayCdfOne = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_blockfetchclient_blockdelay_cdfOne",
		Help: "percentage of blocks fetched in less than 1 second",
	})
	o.metrics.blockDelayCdfThree = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "cardano_node_metrics_blockfetchclient_blockdelay_cdfThree",
			Help: "percentage of blocks fetched in less than 3 seconds",
		},
	)
	o.metrics.blockDelayCdfFive = promautoFactory.NewGauge(prometheus.GaugeOpts{
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
				ouroboros.WithPeerSharing(o.config.PeerSharing),
				ouroboros.WithNetworkMagic(o.config.NetworkMagic),
				ouroboros.WithPeerSharingConfig(
					opeersharing.NewConfig(
						slices.Concat(
							o.peersharingClientConnOpts(),
							o.peersharingServerConnOpts(),
						)...,
					),
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
					oblockfetch.NewConfig(
						slices.Concat(
							o.blockfetchClientConnOpts(),
							o.blockfetchServerConnOpts(),
						)...,
					),
				),
			)
		}
		tmpListeners[idx] = l
	}
	return tmpListeners
}

func (o *Ouroboros) OutboundConnOpts() []ouroboros.ConnectionOptionFunc {
	return []ouroboros.ConnectionOptionFunc{
		ouroboros.WithNetworkMagic(o.config.NetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithFullDuplex(true),
		ouroboros.WithPeerSharing(o.config.PeerSharing),
		ouroboros.WithPeerSharingConfig(
			opeersharing.NewConfig(
				slices.Concat(
					o.peersharingClientConnOpts(),
					o.peersharingServerConnOpts(),
				)...,
			),
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
			oblockfetch.NewConfig(
				slices.Concat(
					o.blockfetchClientConnOpts(),
					o.blockfetchServerConnOpts(),
				)...,
			),
		),
	}
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
	// Clean up any pending block fetch start times
	o.blockFetchMutex.Lock()
	delete(o.blockFetchStarts, connId)
	o.blockFetchMutex.Unlock()
	// Clean up chainsync stats
	o.chainsyncMutex.Lock()
	delete(o.chainsyncStats, connId)
	o.chainsyncMutex.Unlock()
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

	// Start chainsync client for this connection if not already tracking it
	if o.ChainsyncState != nil {
		// Atomically check and add the client to avoid race conditions
		if o.ChainsyncState.TryAddClientConnId(connId, maxChainsyncClients) {
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
				"chainsync client limit reached, skipping",
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
	if versionData.DiffusionMode() != oprotocol.DiffusionModeInitiatorAndResponder {
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

	// Start chainsync client on this full-duplex inbound connection
	if o.ChainsyncState != nil {
		if o.ChainsyncState.TryAddClientConnId(connId, maxChainsyncClients) {
			if err := o.chainsyncClientStart(connId); err != nil {
				o.ChainsyncState.RemoveClientConnId(connId)
				o.config.Logger.Error(
					"failed to start chainsync client on inbound connection",
					"error", err,
					"connection_id", connId.String(),
				)
				return
			}
			o.config.Logger.Debug(
				"started chainsync client on inbound connection",
				"connection_id", connId.String(),
				"total_clients", o.ChainsyncState.ClientConnCount(),
			)
		}
	}
	// Start txsubmission client
	if err := o.txsubmissionClientStart(connId); err != nil {
		o.config.Logger.Error(
			"failed to start txsubmission client on inbound connection",
			"error", err,
			"connection_id", connId.String(),
		)
	}
}
