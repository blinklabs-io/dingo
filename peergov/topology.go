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
	"net"
	"slices"
	"strconv"
	"time"

	"github.com/blinklabs-io/dingo/topology"
)

func (p *PeerGovernor) LoadTopologyConfig(
	topologyConfig *topology.TopologyConfig,
) {
	if topologyConfig == nil {
		return
	}
	if p.config.DisableOutbound {
		return
	}

	// Resolve all addresses before acquiring the lock to avoid blocking DNS
	// while holding the mutex
	type resolvedPeer struct {
		address    string
		normalized string
	}

	bootstrapResolved := make([]resolvedPeer, 0, len(topologyConfig.BootstrapPeers))
	for _, bootstrapPeer := range topologyConfig.BootstrapPeers {
		tmpAddress := net.JoinHostPort(
			bootstrapPeer.Address,
			strconv.FormatUint(uint64(bootstrapPeer.Port), 10),
		)
		bootstrapResolved = append(bootstrapResolved, resolvedPeer{
			address:    tmpAddress,
			normalized: p.resolveAddress(tmpAddress),
		})
	}

	type resolvedLocalRoot struct {
		peers       []resolvedPeer
		advertise   bool
		valency     uint
		warmValency uint
	}
	localRootsResolved := make([]resolvedLocalRoot, 0, len(topologyConfig.LocalRoots))
	for _, localRoot := range topologyConfig.LocalRoots {
		peers := make([]resolvedPeer, 0, len(localRoot.AccessPoints))
		for _, ap := range localRoot.AccessPoints {
			tmpAddress := net.JoinHostPort(
				ap.Address,
				strconv.FormatUint(uint64(ap.Port), 10),
			)
			peers = append(peers, resolvedPeer{
				address:    tmpAddress,
				normalized: p.resolveAddress(tmpAddress),
			})
		}
		localRootsResolved = append(localRootsResolved, resolvedLocalRoot{
			peers:       peers,
			advertise:   localRoot.Advertise,
			valency:     localRoot.Valency,
			warmValency: localRoot.WarmValency,
		})
	}

	type resolvedPublicRoot struct {
		peers       []resolvedPeer
		advertise   bool
		valency     uint
		warmValency uint
	}
	publicRootsResolved := make([]resolvedPublicRoot, 0, len(topologyConfig.PublicRoots))
	for _, publicRoot := range topologyConfig.PublicRoots {
		peers := make([]resolvedPeer, 0, len(publicRoot.AccessPoints))
		for _, ap := range publicRoot.AccessPoints {
			tmpAddress := net.JoinHostPort(
				ap.Address,
				strconv.FormatUint(uint64(ap.Port), 10),
			)
			peers = append(peers, resolvedPeer{
				address:    tmpAddress,
				normalized: p.resolveAddress(tmpAddress),
			})
		}
		publicRootsResolved = append(publicRootsResolved, resolvedPublicRoot{
			peers:       peers,
			advertise:   publicRoot.Advertise,
			valency:     publicRoot.Valency,
			warmValency: publicRoot.WarmValency,
		})
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
	now := time.Now()
	for _, resolved := range bootstrapResolved {
		p.peers = append(
			p.peers,
			&Peer{
				Address:           resolved.address,
				NormalizedAddress: resolved.normalized,
				Source:            PeerSourceTopologyBootstrapPeer,
				State:             PeerStateCold,
				EMAAlpha:          p.config.EMAAlpha,
				FirstSeen:         now,
			},
		)
	}
	// Add topology local roots
	for groupIdx, localRoot := range localRootsResolved {
		groupID := fmt.Sprintf("local-root-%d", groupIdx)
		for _, resolved := range localRoot.peers {
			tmpPeer := &Peer{
				Address:           resolved.address,
				NormalizedAddress: resolved.normalized,
				Source:            PeerSourceTopologyLocalRoot,
				State:             PeerStateCold,
				Sharable:          localRoot.advertise,
				EMAAlpha:          p.config.EMAAlpha,
				Valency:           localRoot.valency,
				WarmValency:       localRoot.warmValency,
				GroupID:           groupID,
				FirstSeen:         now,
			}
			// Check for existing peer with this address
			existingPeerIdx := -1
			for i, existingPeer := range p.peers {
				if existingPeer != nil &&
					existingPeer.NormalizedAddress == resolved.normalized {
					existingPeerIdx = i
					break
				}
			}
			if existingPeerIdx >= 0 {
				existingPeer := p.peers[existingPeerIdx]
				// Preserve active connection state for any peer with a connection
				if existingPeer.Connection != nil {
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
	for groupIdx, publicRoot := range publicRootsResolved {
		groupID := fmt.Sprintf("public-root-%d", groupIdx)
		for _, resolved := range publicRoot.peers {
			tmpPeer := &Peer{
				Address:           resolved.address,
				NormalizedAddress: resolved.normalized,
				Source:            PeerSourceTopologyPublicRoot,
				State:             PeerStateCold,
				Sharable:          publicRoot.advertise,
				EMAAlpha:          p.config.EMAAlpha,
				Valency:           publicRoot.valency,
				WarmValency:       publicRoot.warmValency,
				GroupID:           groupID,
				FirstSeen:         now,
			}
			// Check for existing peer with this address
			existingPeerIdx := -1
			for i, existingPeer := range p.peers {
				if existingPeer != nil &&
					existingPeer.NormalizedAddress == resolved.normalized {
					existingPeerIdx = i
					break
				}
			}
			if existingPeerIdx >= 0 {
				existingPeer := p.peers[existingPeerIdx]
				// Preserve active connection state for any peer with a connection
				if existingPeer.Connection != nil {
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
