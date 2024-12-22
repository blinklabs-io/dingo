// Copyright 2024 Blink Labs Software
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
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/topology"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

const (
	initialReconnectDelay  = 1 * time.Second
	maxReconnectDelay      = 128 * time.Second
	reconnectBackoffFactor = 2
)

type PeerGovernor struct {
	mu     sync.Mutex
	config PeerGovernorConfig
	peers  []*Peer
}

type PeerGovernorConfig struct {
	Logger      *slog.Logger
	EventBus    *event.EventBus
	ConnManager *connmanager.ConnectionManager
}

func NewPeerGovernor(cfg PeerGovernorConfig) *PeerGovernor {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	cfg.Logger = cfg.Logger.With("component", "peergov")
	return &PeerGovernor{
		config: cfg,
	}
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
	p.mu.Lock()
	defer p.mu.Unlock()
	// Remove peers originally sourced from the topology
	tmpPeers := []*Peer{}
	for _, tmpPeer := range p.peers {
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
		p.peers = append(
			p.peers,
			&Peer{
				Address: net.JoinHostPort(
					bootstrapPeer.Address,
					fmt.Sprintf("%d", bootstrapPeer.Port),
				),
				Source: PeerSourceTopologyBootstrapPeer,
			},
		)
	}
	// Add topology local roots
	for _, localRoot := range topologyConfig.LocalRoots {
		for _, ap := range localRoot.AccessPoints {
			p.peers = append(
				p.peers,
				&Peer{
					Address: net.JoinHostPort(
						ap.Address,
						fmt.Sprintf("%d", ap.Port),
					),
					Source:   PeerSourceTopologyLocalRoot,
					Sharable: localRoot.Advertise,
				},
			)
		}
	}
	// Add topology public roots
	for _, publicRoot := range topologyConfig.PublicRoots {
		for _, ap := range publicRoot.AccessPoints {
			p.peers = append(
				p.peers,
				&Peer{
					Address: net.JoinHostPort(
						ap.Address,
						fmt.Sprintf("%d", ap.Port),
					),
					Source:   PeerSourceTopologyPublicRoot,
					Sharable: publicRoot.Advertise,
				},
			)
		}
	}
}

func (p *PeerGovernor) GetPeers() []Peer {
	p.mu.Lock()
	defer p.mu.Unlock()
	ret := make([]Peer, len(p.peers))
	for idx, peer := range p.peers {
		ret[idx] = *peer
	}
	return ret
}

func (p *PeerGovernor) peerIndexByAddress(address string) int {
	for idx, tmpPeer := range p.peers {
		if tmpPeer.Address == address {
			return idx
		}
	}
	return -1
}

func (p *PeerGovernor) peerIndexByConnId(connId ouroboros.ConnectionId) int {
	for idx, tmpPeer := range p.peers {
		if tmpPeer.ConnectionId == nil {
			continue
		}
		if *tmpPeer.ConnectionId == connId {
			return idx
		}
	}
	return -1
}

func (p *PeerGovernor) startOutboundConnections() {
	p.config.Logger.Debug(
		"starting connections",
		"role", "client",
	)
	for _, tmpPeer := range p.peers {
		go p.createOutboundConnection(tmpPeer)
	}
}

func (p *PeerGovernor) createOutboundConnection(peer *Peer) {
	for {
		conn, err := p.config.ConnManager.CreateOutboundConn(peer.Address)
		if err == nil {
			connId := conn.Id()
			peer.ConnectionId = &connId
			peer.ReconnectCount = 0
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
	peerIdx := p.peerIndexByAddress(e.RemoteAddr.String())
	if peerIdx == -1 {
		// Add inbound peer
		p.peers = append(
			p.peers,
			&Peer{
				Address: e.RemoteAddr.String(),
				Source:  PeerSourceInboundConn,
				// TODO: use handshake params to figure out if we should share
				Sharable:     true,
				ConnectionId: &e.ConnectionId,
			},
		)
	} else {
		// Set connection on existing peer
		p.peers[peerIdx].ConnectionId = &e.ConnectionId
	}
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
	if peerIdx != -1 {
		p.peers[peerIdx].ConnectionId = nil
		if p.peers[peerIdx].Source != PeerSourceInboundConn {
			go p.createOutboundConnection(p.peers[peerIdx])
		}
	}
}
