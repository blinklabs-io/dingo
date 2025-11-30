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
	initialReconnectDelay  = 1 * time.Second
	maxReconnectDelay      = 128 * time.Second
	reconnectBackoffFactor = 2
)

type PeerGovernor struct {
	config  PeerGovernorConfig
	peers   []*Peer
	mu      sync.Mutex
	metrics *peerGovernorMetrics
}

type PeerGovernorConfig struct {
	Logger          *slog.Logger
	EventBus        *event.EventBus
	ConnManager     *connmanager.ConnectionManager
	DisableOutbound bool
	PromRegistry    prometheus.Registerer
}

type peerGovernorMetrics struct {
	coldPeers        prometheus.Gauge
	warmPeers        prometheus.Gauge
	hotPeers         prometheus.Gauge
	activePeers      prometheus.Gauge
	establishedPeers prometheus.Gauge
	knownPeers       prometheus.Gauge
}

func NewPeerGovernor(cfg PeerGovernorConfig) *PeerGovernor {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
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
	p.config.EventBus.SubscribeFunc(
		connmanager.InboundConnectionEventType,
		p.handleInboundConnectionEvent,
	)
	p.config.EventBus.SubscribeFunc(
		connmanager.ConnectionClosedEventType,
		p.handleConnectionClosedEvent,
	)
	// Start outbound connections
	p.startOutboundConnections()
	return nil
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
		if peer.ReconnectDelay == 0 {
			peer.ReconnectDelay = initialReconnectDelay
		} else if peer.ReconnectDelay < maxReconnectDelay {
			peer.ReconnectDelay = peer.ReconnectDelay * reconnectBackoffFactor
		}
		peer.ReconnectCount += 1
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
	conn := p.config.ConnManager.GetConnectionById(e.ConnectionId)
	tmpPeer.setConnection(conn, false)
	if tmpPeer.Connection != nil {
		tmpPeer.Sharable = tmpPeer.Connection.VersionData.PeerSharing()
		tmpPeer.State = PeerStateWarm
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
