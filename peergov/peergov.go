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
	"sync"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/topology"
)

type PeerGovernor struct {
	mu     sync.Mutex
	config PeerGovernorConfig
	peers  []Peer
	// TODO
}

type PeerGovernorConfig struct {
	Logger      *slog.Logger
	EventBus    *event.EventBus
	ConnManager *connmanager.ConnectionManager
	// TODO
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
	// TODO: start outbound connections
	return nil
}

func (p *PeerGovernor) AddPeer(
	accessPoints []PeerAccessPoint,
	source PeerSource,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// TODO: look for existing peer with matching access point(s) and merge (?)
	tmpPeer := Peer{
		AccessPoints: accessPoints,
		Source:       source,
	}
	p.config.Logger.Debug(
		fmt.Sprintf(
			"adding peer: %+v",
			tmpPeer,
		),
	)
	p.peers = append(
		p.peers,
		tmpPeer,
	)
}

func (p *PeerGovernor) AddPeersFromTopology(
	topologyConfig *topology.TopologyConfig,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Remove peers originally sourced from the topology
	var tmpPeers []Peer
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
		p.AddPeer(
			[]PeerAccessPoint{
				{
					Address: bootstrapPeer.Address,
					Port:    bootstrapPeer.Port,
				},
			},
			PeerSourceTopologyBootstrapPeer,
		)
	}
	// Add topology local roots
	for _, localRoot := range topologyConfig.LocalRoots {
		var tmpAccessPoints []PeerAccessPoint
		for _, ap := range localRoot.AccessPoints {
			tmpAccessPoints = append(
				tmpAccessPoints,
				PeerAccessPoint{
					Address: ap.Address,
					Port:    ap.Port,
				},
			)
		}
		p.AddPeer(
			tmpAccessPoints,
			PeerSourceTopologyLocalRoot,
		)
	}
	// Add topology public roots
	for _, publicRoot := range topologyConfig.PublicRoots {
		var tmpAccessPoints []PeerAccessPoint
		for _, ap := range publicRoot.AccessPoints {
			tmpAccessPoints = append(
				tmpAccessPoints,
				PeerAccessPoint{
					Address: ap.Address,
					Port:    ap.Port,
				},
			)
		}
		p.AddPeer(
			tmpAccessPoints,
			PeerSourceTopologyPublicRoot,
		)
	}
}
