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

package peergov

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/topology"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	defaultReconcileInterval    = 30 * time.Minute
	defaultMaxReconnectFailures = 3
	defaultMinHotPeers          = 3
	defaultInactivityTimeout    = 10 * time.Minute
)

const (
	initialReconnectDelay  = 1 * time.Second
	maxReconnectDelay      = 128 * time.Second
	reconnectBackoffFactor = 2
)

type PeerGovernor struct {
	config          PeerGovernorConfig
	metrics         *peerGovernorMetrics
	reconcileTicker *time.Ticker
	stopCh          chan struct{}
	peers           []*Peer
	mu              sync.Mutex
}

type PeerGovernorConfig struct {
	PromRegistry         prometheus.Registerer
	Logger               *slog.Logger
	EventBus             *event.EventBus
	ConnManager          *connmanager.ConnectionManager
	PeerRequestFunc      func(peer *Peer) []string
	ReconcileInterval    time.Duration
	MaxReconnectFailures int
	MinHotPeers          int
	InactivityTimeout    time.Duration
	DisableOutbound      bool
}

type peerGovernorMetrics struct {
	coldPeers        prometheus.Gauge
	warmPeers        prometheus.Gauge
	hotPeers         prometheus.Gauge
	activePeers      prometheus.Gauge
	establishedPeers prometheus.Gauge
	knownPeers       prometheus.Gauge
	// Churn counters
	coldPeersPromotions  prometheus.Counter
	warmPeersPromotions  prometheus.Counter
	warmPeersDemotions   prometheus.Counter
	increasedKnownPeers  prometheus.Counter
	decreasedKnownPeers  prometheus.Counter
	increasedActivePeers prometheus.Counter
	decreasedActivePeers prometheus.Counter
}

func NewPeerGovernor(cfg PeerGovernorConfig) *PeerGovernor {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if cfg.ReconcileInterval == 0 {
		cfg.ReconcileInterval = defaultReconcileInterval
	}
	if cfg.MaxReconnectFailures == 0 {
		cfg.MaxReconnectFailures = defaultMaxReconnectFailures
	}
	if cfg.MinHotPeers == 0 {
		cfg.MinHotPeers = defaultMinHotPeers
	}
	if cfg.InactivityTimeout == 0 {
		cfg.InactivityTimeout = defaultInactivityTimeout
	}
	cfg.Logger = cfg.Logger.With("component", "peergov")
	p := &PeerGovernor{
		config: cfg,
		peers:  []*Peer{},
	}
	if cfg.PromRegistry != nil {
		p.initMetrics()
	}
	return p
}

func (p *PeerGovernor) initMetrics() {
	promautoFactory := promauto.With(p.config.PromRegistry)
	p.metrics = &peerGovernorMetrics{}
	p.metrics.coldPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_cold",
		Help: "number of cold peers",
	})
	p.metrics.warmPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_warm",
		Help: "number of warm peers",
	})
	p.metrics.hotPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_hot",
		Help: "number of hot peers",
	})
	p.metrics.activePeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_ActivePeers",
		Help: "number of active peers",
	})
	p.metrics.establishedPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_EstablishedPeers",
		Help: "number of established peers",
	})
	p.metrics.knownPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_KnownPeers",
		Help: "number of known peers",
	})
	// Churn counters
	p.metrics.coldPeersPromotions = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_ColdPeersPromotions",
			Help: "number of cold peers promoted to warm",
		},
	)
	p.metrics.warmPeersPromotions = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_WarmPeersPromotions",
			Help: "number of warm peers promoted to hot",
		},
	)
	p.metrics.warmPeersDemotions = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_WarmPeersDemotions",
			Help: "number of hot peers demoted to warm",
		},
	)
	p.metrics.increasedKnownPeers = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_churn_IncreasedKnownPeers",
			Help: "number of peers added to known set",
		},
	)
	p.metrics.decreasedKnownPeers = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_churn_DecreasedKnownPeers",
			Help: "number of peers removed from known set",
		},
	)
	p.metrics.increasedActivePeers = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_churn_IncreasedActivePeers",
			Help: "number of active peers increased",
		},
	)
	p.metrics.decreasedActivePeers = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_churn_DecreasedActivePeers",
			Help: "number of active peers decreased",
		},
	)
}

// updatePeerMetrics updates the Prometheus metrics for peer counts.
// This function assumes p.mu is already held by the caller.
func (p *PeerGovernor) updatePeerMetrics() {
	if p.metrics == nil {
		return
	}
	// NOTE: Caller must hold p.mu

	coldCount := 0
	warmCount := 0
	hotCount := 0
	activeCount := 0
	establishedCount := 0
	knownCount := len(p.peers)

	for _, peer := range p.peers {
		switch peer.State {
		case PeerStateCold:
			coldCount++
		case PeerStateWarm:
			warmCount++
			// Warm peers have established connections
			if peer.Connection != nil {
				establishedCount++
			}
		case PeerStateHot:
			hotCount++
			// Hot peers have established connections and are active
			if peer.Connection != nil {
				establishedCount++
				activeCount++
			}
		}
	}

	p.metrics.coldPeers.Set(float64(coldCount))
	p.metrics.warmPeers.Set(float64(warmCount))
	p.metrics.hotPeers.Set(float64(hotCount))
	p.metrics.activePeers.Set(float64(activeCount))
	p.metrics.establishedPeers.Set(float64(establishedCount))
	p.metrics.knownPeers.Set(float64(knownCount))
}

func (p *PeerGovernor) Start() error {
	// Setup connmanager event listeners
	if p.config.EventBus != nil {
		p.config.EventBus.SubscribeFunc(
			connmanager.InboundConnectionEventType,
			p.handleInboundConnectionEvent,
		)
		p.config.EventBus.SubscribeFunc(
			connmanager.ConnectionClosedEventType,
			p.handleConnectionClosedEvent,
		)
	}
	// Start reconcile loop
	ticker := time.NewTicker(p.config.ReconcileInterval)
	stopCh := make(chan struct{})
	p.mu.Lock()
	p.reconcileTicker = ticker
	p.stopCh = stopCh
	p.mu.Unlock()
	go func(t *time.Ticker, stop <-chan struct{}) {
		for {
			select {
			case <-t.C:
				p.reconcile()
			case <-stop:
				return
			}
		}
	}(ticker, stopCh)
	// Start outbound connections
	p.startOutboundConnections()
	return nil
}

// Stop gracefully shuts down the peer governor
func (p *PeerGovernor) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.reconcileTicker != nil {
		p.reconcileTicker.Stop()
		if p.stopCh != nil {
			close(p.stopCh)
		}
		p.reconcileTicker = nil
		p.stopCh = nil
	}
}

func (p *PeerGovernor) LoadTopologyConfig(
	topologyConfig *topology.TopologyConfig,
) {
	if p.config.DisableOutbound {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// Remove peers originally sourced from the topology
	tmpPeers := []*Peer{}
	for _, tmpPeer := range p.peers {
		if tmpPeer == nil {
			continue
		}
		if tmpPeer.Source == PeerSourceTopologyBootstrapPeer ||
			tmpPeer.Source == PeerSourceTopologyLocalRoot ||
			tmpPeer.Source == PeerSourceTopologyPublicRoot {
			continue
		}
		tmpPeers = append(tmpPeers, tmpPeer)
	}
	p.peers = tmpPeers
	// Add topology bootstrap peers
	for _, bootstrapPeer := range topologyConfig.BootstrapPeers {
		tmpAddress := net.JoinHostPort(
			bootstrapPeer.Address,
			strconv.FormatUint(uint64(bootstrapPeer.Port), 10),
		)
		p.peers = append(
			p.peers,
			&Peer{
				Address: tmpAddress,
				Source:  PeerSourceTopologyBootstrapPeer,
				State:   PeerStateCold,
			},
		)
	}
	// Add topology local roots
	for _, localRoot := range topologyConfig.LocalRoots {
		for _, ap := range localRoot.AccessPoints {
			tmpAddress := net.JoinHostPort(
				ap.Address,
				strconv.FormatUint(uint64(ap.Port), 10),
			)
			tmpPeer := &Peer{
				Address:  tmpAddress,
				Source:   PeerSourceTopologyLocalRoot,
				State:    PeerStateCold,
				Sharable: localRoot.Advertise,
			}
			// Check for existing peer with this address
			existingPeerIdx := -1
			for i, existingPeer := range p.peers {
				if existingPeer != nil && existingPeer.Address == tmpAddress {
					existingPeerIdx = i
					break
				}
			}
			if existingPeerIdx >= 0 {
				existingPeer := p.peers[existingPeerIdx]
				// Preserve active inbound connection state
				if existingPeer.Source == PeerSourceInboundConn &&
					existingPeer.Connection != nil {
					tmpPeer.Connection = existingPeer.Connection
					tmpPeer.State = existingPeer.State
					tmpPeer.ReconnectCount = existingPeer.ReconnectCount
					tmpPeer.ReconnectDelay = existingPeer.ReconnectDelay
					tmpPeer.Sharable = existingPeer.Sharable
				}
				// Remove the existing peer
				p.peers = slices.Delete(
					p.peers, existingPeerIdx,
					existingPeerIdx+1)
			}
			p.peers = append(p.peers, tmpPeer)
		}
	}
	// Add topology public roots
	for _, publicRoot := range topologyConfig.PublicRoots {
		for _, ap := range publicRoot.AccessPoints {
			tmpAddress := net.JoinHostPort(
				ap.Address,
				strconv.FormatUint(uint64(ap.Port), 10),
			)
			tmpPeer := &Peer{
				Address:  tmpAddress,
				Source:   PeerSourceTopologyPublicRoot,
				State:    PeerStateCold,
				Sharable: publicRoot.Advertise,
			}
			// Check for existing peer with this address
			existingPeerIdx := -1
			for i, existingPeer := range p.peers {
				if existingPeer != nil && existingPeer.Address == tmpAddress {
					existingPeerIdx = i
					break
				}
			}
			if existingPeerIdx >= 0 {
				existingPeer := p.peers[existingPeerIdx]
				// Preserve active inbound connection state
				if existingPeer.Source == PeerSourceInboundConn &&
					existingPeer.Connection != nil {
					tmpPeer.Connection = existingPeer.Connection
					tmpPeer.State = existingPeer.State
					tmpPeer.ReconnectCount = existingPeer.ReconnectCount
					tmpPeer.ReconnectDelay = existingPeer.ReconnectDelay
					tmpPeer.Sharable = existingPeer.Sharable
				}
				// Remove the existing peer
				p.peers = slices.Delete(
					p.peers, existingPeerIdx,
					existingPeerIdx+1)
			}
			p.peers = append(p.peers, tmpPeer)
		}
	}
	p.updatePeerMetrics()
}

func (p *PeerGovernor) GetPeers() []Peer {
	p.mu.Lock()
	defer p.mu.Unlock()
	ret := make([]Peer, 0, len(p.peers))
	for _, peer := range p.peers {
		if peer != nil {
			ret = append(ret, *peer)
		}
	}
	return ret
}

func (p *PeerGovernor) AddPeer(address string, source PeerSource) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Check if already exists
	for _, peer := range p.peers {
		if peer != nil && peer.Address == address {
			return
		}
	}
	p.peers = append(p.peers, &Peer{
		Address: address,
		Source:  source,
		State:   PeerStateCold,
	})
	p.updatePeerMetrics()
	if source == PeerSourceP2PGossip && p.metrics != nil {
		p.metrics.increasedKnownPeers.Inc()
	}
	if p.config.EventBus != nil {
		reason := "manual"
		switch source {
		case PeerSourceP2PGossip:
			reason = "peer sharing"
		case PeerSourceTopologyBootstrapPeer,
			PeerSourceTopologyLocalRoot,
			PeerSourceTopologyPublicRoot:
			reason = "topology"
		case PeerSourceInboundConn:
			reason = "inbound connection"
		}
		p.config.EventBus.Publish(
			PeerAddedEventType,
			event.NewEvent(
				PeerAddedEventType,
				PeerStateChangeEvent{Address: address, Reason: reason},
			),
		)
	}
}

func (p *PeerGovernor) peerIndexByAddress(address string) int {
	for idx, tmpPeer := range p.peers {
		if tmpPeer != nil && tmpPeer.Address == address {
			return idx
		}
	}
	return -1
}

func (p *PeerGovernor) peerIndexByConnId(connId ouroboros.ConnectionId) int {
	for idx, tmpPeer := range p.peers {
		if tmpPeer == nil || tmpPeer.Connection == nil {
			continue
		}
		if tmpPeer.Connection.Id == connId {
			return idx
		}
	}
	return -1
}

func (p *PeerGovernor) SetPeerHotByConnId(connId ouroboros.ConnectionId) {
	p.mu.Lock()
	defer p.mu.Unlock()
	peerIdx := p.peerIndexByConnId(connId)
	if peerIdx != -1 && p.peers[peerIdx] != nil {
		p.peers[peerIdx].State = PeerStateHot
		p.peers[peerIdx].LastActivity = time.Now()
		p.updatePeerMetrics()
	}
}

func (p *PeerGovernor) startOutboundConnections() {
	// Skip outbound connections if disabled
	if p.config.DisableOutbound {
		p.config.Logger.Info(
			"outbound connections disabled, skipping outbound connections",
			"role", "client",
		)
		return
	}

	p.config.Logger.Debug(
		"starting connections",
		"role", "client",
	)

	for _, tmpPeer := range p.peers {
		if tmpPeer != nil {
			go p.createOutboundConnection(tmpPeer)
		}
	}
}

func (p *PeerGovernor) createOutboundConnection(peer *Peer) {
	if peer == nil {
		return
	}
	for {
		conn, err := p.config.ConnManager.CreateOutboundConn(peer.Address)
		if err == nil {
			connId := conn.Id()
			p.mu.Lock()
			peer.ReconnectCount = 0
			peer.setConnection(conn, true)
			peer.State = PeerStateWarm
			p.updatePeerMetrics()
			p.mu.Unlock()
			// Generate event
			if p.config.EventBus != nil {
				p.config.EventBus.Publish(
					OutboundConnectionEventType,
					event.NewEvent(
						OutboundConnectionEventType,
						OutboundConnectionEvent{
							ConnectionId: connId,
						},
					),
				)
			}
			return
		}
		p.config.Logger.Error(
			fmt.Sprintf(
				"outbound: failed to establish connection to %s: %s",
				peer.Address,
				err,
			),
		)
		p.mu.Lock()
		if peer.ReconnectDelay == 0 {
			peer.ReconnectDelay = initialReconnectDelay
		} else if peer.ReconnectDelay < maxReconnectDelay {
			peer.ReconnectDelay = peer.ReconnectDelay * reconnectBackoffFactor
		}
		peer.ReconnectCount += 1
		p.mu.Unlock()
		p.config.Logger.Info(
			fmt.Sprintf(
				"outbound: delaying %s (retry %d) before reconnecting to %s",
				peer.ReconnectDelay,
				peer.ReconnectCount,
				peer.Address,
			),
		)
		time.Sleep(peer.ReconnectDelay)
	}
}

func (p *PeerGovernor) handleInboundConnectionEvent(evt event.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := evt.Data.(connmanager.InboundConnectionEvent)
	var tmpPeer *Peer
	peerIdx := p.peerIndexByAddress(e.RemoteAddr.String())
	if peerIdx == -1 {
		tmpPeer = &Peer{
			Address: e.RemoteAddr.String(),
			Source:  PeerSourceInboundConn,
			State:   PeerStateCold,
		}
		// Add inbound peer
		p.peers = append(
			p.peers,
			tmpPeer,
		)
	} else {
		tmpPeer = p.peers[peerIdx]
	}
	if tmpPeer == nil {
		return
	}
	if p.config.ConnManager != nil {
		conn := p.config.ConnManager.GetConnectionById(e.ConnectionId)
		if conn != nil {
			tmpPeer.setConnection(conn, false)
			if tmpPeer.Connection != nil {
				tmpPeer.Sharable = tmpPeer.Connection.VersionData.PeerSharing()
				tmpPeer.State = PeerStateWarm
			}
		}
	}
	p.updatePeerMetrics()
}

func (p *PeerGovernor) handleConnectionClosedEvent(evt event.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := evt.Data.(connmanager.ConnectionClosedEvent)
	if e.Error != nil {
		p.config.Logger.Error(
			fmt.Sprintf(
				"unexpected connection failure: %s",
				e.Error,
			),
			"connection_id", e.ConnectionId.String(),
		)
	} else {
		p.config.Logger.Info("connection closed",
			"connection_id", e.ConnectionId.String(),
		)
	}
	peerIdx := p.peerIndexByConnId(e.ConnectionId)
	if peerIdx != -1 && p.peers[peerIdx] != nil {
		p.peers[peerIdx].Connection = nil
		p.peers[peerIdx].State = PeerStateCold
		p.updatePeerMetrics()
		if p.peers[peerIdx].Source != PeerSourceInboundConn {
			go p.createOutboundConnection(p.peers[peerIdx])
		}
	}
}

func (p *PeerGovernor) reconcile() {
	p.mu.Lock()

	p.config.Logger.Debug("starting peer reconcile")

	// Track changes for metrics
	var coldPromotions, warmPromotions, warmDemotions, knownRemoved, activeIncreased, activeDecreased int

	// Demotion/Promotion Logic
	for i := len(p.peers) - 1; i >= 0; i-- {
		peer := p.peers[i]
		if peer == nil {
			continue
		}
		switch peer.State {
		case PeerStateHot:
			// Demote if inactive (no connection or last activity > timeout)
			if peer.Connection == nil ||
				time.Since(peer.LastActivity) > p.config.InactivityTimeout {
				p.peers[i].State = PeerStateWarm
				warmDemotions++
				activeDecreased++
				p.config.Logger.Info(
					"demoted peer to warm due to inactivity",
					"address",
					peer.Address,
				)
				if p.config.EventBus != nil {
					p.config.EventBus.Publish(
						PeerDemotedEventType,
						event.NewEvent(
							PeerDemotedEventType,
							PeerStateChangeEvent{
								Address: peer.Address,
								Reason:  "inactive",
							},
						),
					)
				}
			}
		case PeerStateWarm:
			// Promote if stable connection
			if peer.Connection != nil {
				p.peers[i].State = PeerStateHot
				p.peers[i].LastActivity = time.Now()
				warmPromotions++
				activeIncreased++
				p.config.Logger.Info(
					"promoted peer to hot",
					"address",
					peer.Address,
				)
				if p.config.EventBus != nil {
					p.config.EventBus.Publish(
						PeerPromotedEventType,
						event.NewEvent(
							PeerPromotedEventType,
							PeerStateChangeEvent{
								Address: peer.Address,
								Reason:  "stable connection",
							},
						),
					)
				}
			}
		case PeerStateCold:
			// Promote to warm if connection exists
			if peer.Connection != nil {
				p.peers[i].State = PeerStateWarm
				coldPromotions++
				p.config.Logger.Info(
					"promoted peer to warm",
					"address",
					peer.Address,
				)
				if p.config.EventBus != nil {
					p.config.EventBus.Publish(
						PeerPromotedEventType,
						event.NewEvent(
							PeerPromotedEventType,
							PeerStateChangeEvent{
								Address: peer.Address,
								Reason:  "connection established",
							},
						),
					)
				}
			} else if peer.ReconnectCount > p.config.MaxReconnectFailures {
				knownRemoved++
				p.config.Logger.Info(
					"removing failed peer",
					"address",
					peer.Address,
					"failures",
					peer.ReconnectCount,
				)
				if p.config.EventBus != nil {
					p.config.EventBus.Publish(
						PeerRemovedEventType,
						event.NewEvent(
							PeerRemovedEventType,
							PeerStateChangeEvent{
								Address: peer.Address,
								Reason:  "excessive failures",
							},
						),
					)
				}
				// Remove from slice (safe while iterating backwards)
				p.peers = append(p.peers[:i], p.peers[i+1:]...)
			}
		}
	}

	// Ensure minimum hot peers (simple: promote more warm if needed)
	hotCount := 0
	for _, peer := range p.peers {
		if peer != nil && peer.State == PeerStateHot {
			hotCount++
		}
	}
	if hotCount < p.config.MinHotPeers {
		for _, peer := range p.peers {
			if peer != nil && peer.State == PeerStateWarm &&
				peer.Connection != nil {
				peer.State = PeerStateHot
				peer.LastActivity = time.Now()
				warmPromotions++
				activeIncreased++
				p.config.Logger.Info(
					"promoted peer to hot to meet minimum",
					"address",
					peer.Address,
				)
				if p.config.EventBus != nil {
					p.config.EventBus.Publish(
						PeerPromotedEventType,
						event.NewEvent(
							PeerPromotedEventType,
							PeerStateChangeEvent{
								Address: peer.Address,
								Reason:  "minimum hot peers",
							},
						),
					)
				}
				hotCount++
				if hotCount >= p.config.MinHotPeers {
					break
				}
			}
		}
	}

	// Collect eligible peers for peer sharing
	var eligiblePeers []*Peer
	if p.config.PeerRequestFunc != nil {
		for _, peer := range p.peers {
			if peer != nil && peer.State == PeerStateHot &&
				peer.Connection != nil && peer.Source != PeerSourceTopologyLocalRoot {
				eligiblePeers = append(eligiblePeers, peer)
			}
		}
	}

	// Update metrics
	p.updatePeerMetrics()
	if p.metrics != nil {
		p.metrics.coldPeersPromotions.Add(float64(coldPromotions))
		p.metrics.warmPeersPromotions.Add(float64(warmPromotions))
		p.metrics.warmPeersDemotions.Add(float64(warmDemotions))
		p.metrics.decreasedKnownPeers.Add(float64(knownRemoved))
		p.metrics.increasedActivePeers.Add(float64(activeIncreased))
		p.metrics.decreasedActivePeers.Add(float64(activeDecreased))
	}

	p.config.Logger.Debug(
		"peer reconcile completed",
		"changes",
		coldPromotions+warmPromotions+warmDemotions+knownRemoved,
	)

	// Peer Discovery via Peer Sharing (outside lock)
	p.mu.Unlock()
	for _, peer := range eligiblePeers {
		addrs := p.config.PeerRequestFunc(peer)
		for _, addr := range addrs {
			p.AddPeer(addr, PeerSourceP2PGossip)
		}
	}
}
