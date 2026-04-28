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
	"errors"
	"net"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

// defaultMinPeerListCap is the minimum hard cap for the peer list size,
// used when 2 * TargetNumberOfKnownPeers is smaller.
const defaultMinPeerListCap = 200

// ErrPeerListFull is returned when a non-topology peer is rejected because
// the peer list has reached its hard capacity limit.
var ErrPeerListFull = errors.New("peer list at capacity")

// maxPeerListSize returns the hard cap for the total number of peers.
// This prevents unbounded growth between reconciliation cycles.
// The cap is max(2 * TargetNumberOfKnownPeers, defaultMinPeerListCap).
func (p *PeerGovernor) maxPeerListSize() int {
	return max(2*p.config.TargetNumberOfKnownPeers, defaultMinPeerListCap)
}

// isAtPeerCapLocked returns true if the peer list has reached the hard cap.
// Must be called with p.mu held.
func (p *PeerGovernor) isAtPeerCapLocked() bool {
	return len(p.peers) >= p.maxPeerListSize()
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

// ErrUnroutableAddress is returned when a peer address resolves to a
// non-routable IP (private, loopback, link-local, multicast, or
// unspecified).
var ErrUnroutableAddress = errors.New("unroutable peer address")

// isRoutableAddr checks whether the host portion of an address is a
// publicly-routable unicast IP. It returns false for private (RFC 1918 /
// RFC 4193), loopback, link-local, multicast, and unspecified addresses.
// If the host is not a valid IP (e.g. unresolved hostname), it is
// considered routable so that DNS-based topology peers still work.
func isRoutableAddr(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		// Bare host or malformed — treat as routable to be safe
		host = address
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// Not an IP literal (hostname) — allow it
		return true
	}
	if ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsMulticast() ||
		ip.IsUnspecified() {
		return false
	}
	return true
}

func (p *PeerGovernor) AddPeer(
	address string,
	source PeerSource,
) error {
	// Resolve address before acquiring lock to avoid blocking DNS
	normalized := p.resolveAddress(address)

	// Reject non-routable IPs early — topology peers bypass this check
	// so operators can use private addresses for local relays. Inbound
	// connections are also exempt since they are already established.
	if !p.isTopologyPeer(source) &&
		source != PeerSourceInboundConn &&
		!isRoutableAddr(normalized) {
		p.config.Logger.Debug(
			"rejecting non-routable peer address",
			"address", address,
			"resolved", normalized,
			"source", source,
		)
		return ErrUnroutableAddress
	}
	var evt *pendingEvent

	p.mu.Lock()
	// Check deny list before adding
	if p.isDeniedLocked(normalized) {
		p.config.Logger.Debug(
			"not adding denied peer",
			"address", address,
		)
		p.mu.Unlock()
		return nil
	}
	// Check if already exists (use normalized address for deduplication)
	for _, peer := range p.peers {
		if peer != nil && peer.NormalizedAddress == normalized {
			p.mu.Unlock()
			return nil
		}
	}
	// Enforce hard cap on peer list size to prevent unbounded growth
	// between reconciliation cycles. Topology peers (operator-configured)
	// are always accepted regardless of the cap.
	if p.isAtPeerCapLocked() && !p.isTopologyPeer(source) {
		p.config.Logger.Debug(
			"rejecting peer: peer list at capacity",
			"address", address,
			"source", source,
			"cap", p.maxPeerListSize(),
			"current", len(p.peers),
		)
		p.mu.Unlock()
		return ErrPeerListFull
	}
	newPeer := &Peer{
		Address:           address,
		NormalizedAddress: normalized,
		Source:            source,
		State:             PeerStateCold,
		EMAAlpha:          p.config.EMAAlpha,
		FirstSeen:         time.Now(),
	}
	// Gossip-discovered peers are sharable since they were already shared
	if source == PeerSourceP2PGossip {
		newPeer.Sharable = true
	}
	p.peers = append(p.peers, newPeer)
	p.updatePeerMetrics()
	if source == PeerSourceP2PGossip && p.metrics != nil {
		p.metrics.increasedKnownPeers.Inc()
	}
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
	// Check if the governor is running and outbound connections
	// are enabled before spawning. Topology peers added before
	// Start() are covered by startOutboundConnections(); this
	// handles peers added at runtime (e.g., gossip).
	shouldConnect := p.stopCh != nil && !p.config.DisableOutbound &&
		source != PeerSourceInboundConn
	evt = &pendingEvent{
		PeerAddedEventType,
		PeerStateChangeEvent{Address: address, Reason: reason},
	}
	p.mu.Unlock()

	// Publish event outside of lock to avoid deadlock
	p.publishEvent(evt.eventType, evt.data)

	// Spawn an outbound connection goroutine for the new peer.
	// Without this, peers added after startup stay cold
	// indefinitely.
	if shouldConnect {
		go p.createOutboundConnection(newPeer)
	}
	return nil
}

// normalizeAddress normalizes an address for deduplication without blocking DNS.
// For IP addresses, it normalizes the format (e.g., IPv6 normalization).
// For hostnames, it lowercases the hostname without DNS resolution.
// This function is safe to call while holding locks.
func (p *PeerGovernor) normalizeAddress(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return strings.ToLower(address)
	}

	// Try to parse as IP first
	ip := net.ParseIP(host)
	if ip != nil {
		// Normalize IPv6 addresses (e.g., ::1 vs 0:0:0:0:0:0:0:1)
		return net.JoinHostPort(ip.String(), port)
	}

	// It's a hostname - just lowercase it (no DNS lookup)
	return net.JoinHostPort(strings.ToLower(host), port)
}

// resolveAddress resolves a hostname in an address to its IP and returns
// the normalized address. This function performs blocking DNS lookups and
// must NOT be called while holding locks.
// If the address is already an IP, it returns the normalized IP address.
// If DNS resolution fails, it returns the lowercased hostname address.
func (p *PeerGovernor) resolveAddress(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return strings.ToLower(address)
	}

	// Try to parse as IP first
	ip := net.ParseIP(host)
	if ip != nil {
		// Already an IP, normalize it
		return net.JoinHostPort(ip.String(), port)
	}

	// It's a hostname - try to resolve it
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		// Can't resolve, just lowercase the hostname
		return net.JoinHostPort(strings.ToLower(host), port)
	}

	// Use first resolved IP for normalization
	return net.JoinHostPort(ips[0].String(), port)
}

func (p *PeerGovernor) publishEvent(eventType event.EventType, data any) {
	if p.config.EventBus == nil {
		return
	}
	p.config.EventBus.Publish(eventType, event.NewEvent(eventType, data))
}

func (p *PeerGovernor) publishPendingEvents(events []pendingEvent) {
	for _, evt := range events {
		p.publishEvent(evt.eventType, evt.data)
	}
}

func (p *PeerGovernor) filterPeers(predicate func(*Peer) bool) []*Peer {
	result := make([]*Peer, 0, len(p.peers))
	for _, peer := range p.peers {
		if peer != nil && predicate(peer) {
			result = append(result, peer)
		}
	}
	return result
}

func (p *PeerGovernor) peerIndexByAddress(address string) int {
	normalized := p.normalizeAddress(address)
	for i, peer := range p.peers {
		if peer == nil {
			continue
		}
		// Check both DNS-resolved normalized address and non-resolved normalized
		// original address to handle the case where storage used DNS resolution
		// but lookup uses the original hostname.
		if peer.NormalizedAddress == normalized ||
			p.normalizeAddress(peer.Address) == normalized {
			return i
		}
	}
	return -1
}

// addressHost returns the host portion of a host:port string with IPv6
// addresses normalized to canonical form. Empty on parse failure. Used
// by the inbound topology-host match, which deliberately ignores the
// port because inbound source ports are ephemeral.
func addressHost(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return ""
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	return strings.ToLower(host)
}

// resolveInboundIdentity selects an existing peer entry to attribute an
// inbound arrival to. It must be called with p.mu held.
//
// Match rules, applied in order (first win):
//
//  1. Exact address match: peer.Address == remoteAddr OR
//     peer.NormalizedAddress == normalizedRemoteAddr. Covers peers that
//     connect from their listener port and prior inbound peers on the
//     same 4-tuple. Preserves phase-1 behavior.
//
//  2. Topology host match, unambiguous only: exactly one topology-sourced
//     peer has a NormalizedAddress whose host portion equals the
//     inbound's host portion. Supports the operator pattern where a
//     configured topology peer dials us from an ephemeral source port.
//     When two or more topology peers share the host we refuse to
//     guess, because merging distinct configured identities would
//     silently violate operator intent.
//
//  3. No match: caller creates a fresh PeerSourceInboundConn entry.
//
// Rule 2 only consults topology peers; gossip/ledger/other inbound
// entries never widen their identity, because the affordance granted
// by a topology match (trust, valency) is specific to operator-declared
// peers.
//
// The second return value is the GroupID of the matched peer when that
// peer is topology-sourced — regardless of whether the match came from
// rule 1 or rule 2. A topology peer connecting from its configured port
// is just as much a "topology match" as one connecting from an
// ephemeral port; the rule that found it does not change that
// classification. Empty when the matched peer is not a topology peer.
func (p *PeerGovernor) resolveInboundIdentity(
	remoteAddr, normalizedRemoteAddr string,
) (idx int, topologyGroupID string) {
	// Rule 1: exact address or normalized match.
	for i, peer := range p.peers {
		if peer == nil {
			continue
		}
		if peer.Address == remoteAddr ||
			peer.NormalizedAddress == normalizedRemoteAddr {
			return i, topologyGroupIDForPeer(peer, p.isTopologyPeer(peer.Source))
		}
	}
	// Rule 2: unambiguous topology-host match.
	inboundHost := addressHost(normalizedRemoteAddr)
	if inboundHost == "" {
		return -1, ""
	}
	candidateIdx := -1
	for i, peer := range p.peers {
		if peer == nil || !p.isTopologyPeer(peer.Source) {
			continue
		}
		if addressHost(peer.NormalizedAddress) != inboundHost {
			continue
		}
		if candidateIdx != -1 {
			// More than one topology peer shares this host. Refuse to
			// guess which configured identity the inbound is; the
			// caller will create a new inbound entry.
			return -1, ""
		}
		candidateIdx = i
	}
	if candidateIdx == -1 {
		return -1, ""
	}
	return candidateIdx, p.peers[candidateIdx].GroupID
}

// topologyGroupIDForPeer returns the matched peer's GroupID when the
// peer is topology-sourced. Factored out so the rule-1 and rule-2
// branches of resolveInboundIdentity agree on classification.
func topologyGroupIDForPeer(peer *Peer, isTopology bool) string {
	if peer == nil || !isTopology {
		return ""
	}
	return peer.GroupID
}

func (p *PeerGovernor) peerIndexByConnId(connId ouroboros.ConnectionId) int {
	for i, peer := range p.peers {
		if peer != nil && peer.Connection != nil &&
			peer.Connection.Id == connId {
			return i
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

func (p *PeerGovernor) TouchPeerByConnId(connId ouroboros.ConnectionId) {
	p.mu.Lock()
	defer p.mu.Unlock()
	peerIdx := p.peerIndexByConnId(connId)
	if peerIdx != -1 && p.peers[peerIdx] != nil {
		p.peers[peerIdx].LastActivity = time.Now()
	}
}

func (p *PeerGovernor) IsChainSelectionEligible(
	connId ouroboros.ConnectionId,
) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	peerIdx := p.peerIndexByConnId(connId)
	if peerIdx == -1 || p.peers[peerIdx] == nil {
		return false
	}
	return chainSelectionState(
		p.bootstrapExited,
		p.peers[peerIdx].Source,
		p.peers[peerIdx].Connection,
	).eligible
}

func clonePeerConnection(conn *PeerConnection) *PeerConnection {
	if conn == nil {
		return nil
	}
	connCopy := *conn
	return &connCopy
}

func chainSelectionEligible(source PeerSource, conn *PeerConnection) bool {
	if conn == nil || !conn.IsClient {
		return false
	}
	// A peer whose only record comes from an unsolicited inbound
	// connection is not a trusted source of chain truth. Topology and
	// P2P-discovered peers keep their source even when they happen to
	// dial us first, so they stay eligible on full-duplex inbound.
	return source != PeerSourceInboundConn
}

func chainSelectionPriority(source PeerSource) int {
	switch source {
	case PeerSourceTopologyLocalRoot:
		return 50
	case PeerSourceTopologyBootstrapPeer:
		return 40
	case PeerSourceTopologyPublicRoot:
		return 30
	case PeerSourceP2PGossip:
		return 20
	case PeerSourceP2PLedger:
		return 10
	default:
		return 0
	}
}

type chainSelectionPeerState struct {
	connId   ouroboros.ConnectionId
	eligible bool
	priority int
	ok       bool
}

func chainSelectionState(
	bootstrapExited bool,
	source PeerSource,
	conn *PeerConnection,
) chainSelectionPeerState {
	if conn == nil {
		return chainSelectionPeerState{}
	}
	eligible := chainSelectionEligible(source, conn)
	priority := chainSelectionPriority(source)
	if source == PeerSourceTopologyBootstrapPeer && bootstrapExited {
		eligible = false
		priority = 0
	}
	return chainSelectionPeerState{
		connId:   conn.Id,
		eligible: eligible,
		priority: priority,
		ok:       true,
	}
}

func (p *PeerGovernor) appendChainSelectionEventsLocked(
	events []pendingEvent,
	oldBootstrapExited bool,
	oldSource PeerSource,
	oldConn *PeerConnection,
	peer *Peer,
) []pendingEvent {
	oldState := chainSelectionState(oldBootstrapExited, oldSource, oldConn)
	newState := chainSelectionPeerState{}
	if peer != nil {
		newState = chainSelectionState(
			p.bootstrapExited,
			peer.Source,
			peer.Connection,
		)
	}
	if oldState.ok && newState.ok && oldState.connId == newState.connId {
		if oldState.eligible != newState.eligible {
			events = append(events, pendingEvent{
				PeerEligibilityChangedEventType,
				PeerEligibilityChangedEvent{
					ConnectionId: newState.connId,
					Eligible:     newState.eligible,
				},
			})
		}
		if oldState.priority != newState.priority {
			events = append(events, pendingEvent{
				PeerPriorityChangedEventType,
				PeerPriorityChangedEvent{
					ConnectionId: newState.connId,
					Priority:     newState.priority,
				},
			})
		}
		return events
	}
	if oldState.ok {
		if oldState.eligible {
			events = append(events, pendingEvent{
				PeerEligibilityChangedEventType,
				PeerEligibilityChangedEvent{
					ConnectionId: oldState.connId,
					Eligible:     false,
				},
			})
		}
		if oldState.priority != 0 {
			events = append(events, pendingEvent{
				PeerPriorityChangedEventType,
				PeerPriorityChangedEvent{
					ConnectionId: oldState.connId,
					Priority:     0,
				},
			})
		}
	}
	if newState.ok {
		if oldState.connId != newState.connId ||
			oldState.eligible != newState.eligible {
			events = append(events, pendingEvent{
				PeerEligibilityChangedEventType,
				PeerEligibilityChangedEvent{
					ConnectionId: newState.connId,
					Eligible:     newState.eligible,
				},
			})
		}
		if newState.priority != 0 &&
			(oldState.connId != newState.connId ||
				oldState.priority != newState.priority) {
			events = append(events, pendingEvent{
				PeerPriorityChangedEventType,
				PeerPriorityChangedEvent{
					ConnectionId: newState.connId,
					Priority:     newState.priority,
				},
			})
		}
	}
	return events
}

func (p *PeerGovernor) UpdatePeerBlockFetchObservation(
	connId ouroboros.ConnectionId,
	latencyMs float64,
	success bool,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	peerIdx := p.peerIndexByConnId(connId)
	if peerIdx != -1 && p.peers[peerIdx] != nil {
		p.peers[peerIdx].UpdateBlockFetchObservation(latencyMs, success)
	}
}

func (p *PeerGovernor) UpdatePeerConnectionStability(
	connId ouroboros.ConnectionId,
	stability float64,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	peerIdx := p.peerIndexByConnId(connId)
	if peerIdx != -1 && p.peers[peerIdx] != nil {
		p.peers[peerIdx].UpdateConnectionStability(stability)
	}
}

func (p *PeerGovernor) UpdatePeerChainSyncObservation(
	connId ouroboros.ConnectionId,
	headerRate float64,
	tipDelta int64,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	peerIdx := p.peerIndexByConnId(connId)
	if peerIdx != -1 && p.peers[peerIdx] != nil {
		p.peers[peerIdx].UpdateChainSyncObservation(headerRate, tipDelta)
	}
}
