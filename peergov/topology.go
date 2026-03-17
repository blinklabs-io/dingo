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

	var events []pendingEvent
	p.mu.Lock()
	isTopologySource := func(source PeerSource) bool {
		return source == PeerSourceTopologyBootstrapPeer ||
			source == PeerSourceTopologyLocalRoot ||
			source == PeerSourceTopologyPublicRoot
	}
	topologySnapshot := make(map[string]*Peer)
	// Remove peers originally sourced from the topology, but keep a snapshot
	// so we can reuse any still-present entries and preserve connection state.
	tmpPeers := make([]*Peer, 0, len(p.peers))
	for _, tmpPeer := range p.peers {
		if tmpPeer == nil {
			continue
		}
		if isTopologySource(tmpPeer.Source) {
			topologySnapshot[tmpPeer.NormalizedAddress] = tmpPeer
			continue
		}
		tmpPeers = append(tmpPeers, tmpPeer)
	}
	p.peers = tmpPeers
	now := time.Now()
	upsertTopologyPeer := func(
		address string,
		normalized string,
		source PeerSource,
		sharable bool,
		valency uint,
		warmValency uint,
		groupID string,
	) {
		newPeer := func() *Peer {
			return &Peer{
				Address:           address,
				NormalizedAddress: normalized,
				Source:            source,
				State:             PeerStateCold,
				Sharable:          sharable,
				EMAAlpha:          p.config.EMAAlpha,
				Valency:           valency,
				WarmValency:       warmValency,
				GroupID:           groupID,
				FirstSeen:         now,
			}
		}
		updatePeer := func(peer *Peer) *Peer {
			if peer.FirstSeen.IsZero() {
				peer.FirstSeen = now
			}
			peer.Address = address
			peer.NormalizedAddress = normalized
			peer.Source = source
			peer.Sharable = sharable
			peer.EMAAlpha = p.config.EMAAlpha
			peer.Valency = valency
			peer.WarmValency = warmValency
			peer.GroupID = groupID
			return peer
		}
		existingPeerIdx := -1
		for i, existingPeer := range p.peers {
			if existingPeer != nil &&
				existingPeer.NormalizedAddress == normalized {
				existingPeerIdx = i
				break
			}
		}
		if existingPeerIdx >= 0 {
			existingPeer := p.peers[existingPeerIdx]
			oldSource := existingPeer.Source
			oldConn := clonePeerConnection(existingPeer.Connection)
			p.peers = slices.Delete(
				p.peers,
				existingPeerIdx,
				existingPeerIdx+1,
			)
			p.peers = append(p.peers, updatePeer(existingPeer))
			events = p.appendChainSelectionEventsLocked(
				events,
				p.bootstrapExited,
				oldSource,
				oldConn,
				existingPeer,
			)
			return
		}
		if existingPeer, ok := topologySnapshot[normalized]; ok {
			delete(topologySnapshot, normalized)
			oldSource := existingPeer.Source
			oldConn := clonePeerConnection(existingPeer.Connection)
			p.peers = append(p.peers, updatePeer(existingPeer))
			events = p.appendChainSelectionEventsLocked(
				events,
				p.bootstrapExited,
				oldSource,
				oldConn,
				existingPeer,
			)
			return
		}
		p.peers = append(p.peers, newPeer())
	}
	// Add topology bootstrap peers
	for _, resolved := range bootstrapResolved {
		upsertTopologyPeer(
			resolved.address,
			resolved.normalized,
			PeerSourceTopologyBootstrapPeer,
			false,
			0,
			0,
			"",
		)
	}
	// Add topology local roots
	for groupIdx, localRoot := range localRootsResolved {
		groupID := fmt.Sprintf("local-root-%d", groupIdx)
		for _, resolved := range localRoot.peers {
			upsertTopologyPeer(
				resolved.address,
				resolved.normalized,
				PeerSourceTopologyLocalRoot,
				localRoot.advertise,
				localRoot.valency,
				localRoot.warmValency,
				groupID,
			)
		}
	}
	// Add topology public roots
	for groupIdx, publicRoot := range publicRootsResolved {
		groupID := fmt.Sprintf("public-root-%d", groupIdx)
		for _, resolved := range publicRoot.peers {
			upsertTopologyPeer(
				resolved.address,
				resolved.normalized,
				PeerSourceTopologyPublicRoot,
				publicRoot.advertise,
				publicRoot.valency,
				publicRoot.warmValency,
				groupID,
			)
		}
	}
	for _, orphanPeer := range topologySnapshot {
		if orphanPeer == nil {
			continue
		}
		events = p.appendChainSelectionEventsLocked(
			events,
			p.bootstrapExited,
			orphanPeer.Source,
			clonePeerConnection(orphanPeer.Connection),
			nil,
		)
		events = append(events, pendingEvent{
			PeerRemovedEventType,
			PeerStateChangeEvent{
				Address: orphanPeer.Address,
				Reason:  "topology removed",
			},
		})
		if orphanPeer.Connection != nil && p.config.ConnManager != nil {
			if conn := p.config.ConnManager.GetConnectionById(
				orphanPeer.Connection.Id,
			); conn != nil {
				conn.Close()
			}
		}
	}
	p.updatePeerMetrics()
	p.mu.Unlock()

	p.publishPendingEvents(events)
}
