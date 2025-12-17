// Copyright 2025 Blink Labs Software
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
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	olocaltxmonitor "github.com/blinklabs-io/gouroboros/protocol/localtxmonitor"
	olocaltxsubmission "github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
	opeersharing "github.com/blinklabs-io/gouroboros/protocol/peersharing"
	otxsubmission "github.com/blinklabs-io/gouroboros/protocol/txsubmission"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

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
	o := &Ouroboros{
		config:           cfg,
		EventBus:         cfg.EventBus,
		ConnManager:      cfg.ConnManager,
		blockFetchStarts: make(map[ouroboros.ConnectionId]time.Time),
	}
	if cfg.PromRegistry != nil {
		o.initMetrics()
	}
	return o
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
			// Node-to-node config
			l.ConnectionOpts = append(
				l.ConnectionOpts,
				ouroboros.WithPeerSharing(o.config.PeerSharing),
				ouroboros.WithNetworkMagic(o.config.NetworkMagic),
				ouroboros.WithPeerSharingConfig(
					opeersharing.NewConfig(
						o.peersharingServerConnOpts()...,
					),
				),
				ouroboros.WithTxSubmissionConfig(
					otxsubmission.NewConfig(
						o.txsubmissionServerConnOpts()...,
					),
				),
				ouroboros.WithChainSyncConfig(
					ochainsync.NewConfig(
						o.chainsyncServerConnOpts()...,
					),
				),
				ouroboros.WithBlockFetchConfig(
					oblockfetch.NewConfig(
						o.blockfetchServerConnOpts()...,
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

	// TODO: replace this with handling for multiple chainsync clients (#385)
	// Start chainsync client if we don't have another
	if o.ChainsyncState != nil {
		chainsyncClientConnId := o.ChainsyncState.GetClientConnId()
		if chainsyncClientConnId == nil {
			if err := o.chainsyncClientStart(connId); err != nil {
				o.config.Logger.Error(
					"failed to start chainsync client",
					"error",
					err,
				)
				return
			}
			o.ChainsyncState.SetClientConnId(connId)
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
