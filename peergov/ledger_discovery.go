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
	"context"
	"time"
)

// discoverLedgerPeers discovers peers from on-chain stake pool relay registrations.
// This method is called during reconciliation if ledger peers are enabled.
//
// Selection is bounded: normal discovery adds only enough peers to reach
// LedgerPeerTarget. Emergency discovery may add one target-sized batch even
// when the known ledger-peer target is already satisfied, because stale or
// unusable peers must not block fresh relay candidates while the node is short
// of connected upstreams. Candidates are shuffled uniformly so no single pool
// dominates across refreshes.
//
//nolint:unused // Kept as a context-free test helper for existing discovery tests.
func (p *PeerGovernor) discoverLedgerPeers() {
	p.discoverLedgerPeersContext(context.Background())
}

func (p *PeerGovernor) discoverLedgerPeersContext(ctx context.Context) {
	// Check if ledger peer provider is configured
	if p.config.LedgerPeerProvider == nil {
		p.config.Logger.Debug(
			"ledger peer discovery skipped: provider is nil",
		)
		return
	}
	// Check UseLedgerAfterSlot threshold first (before claiming refresh)
	if p.config.UseLedgerAfterSlot < 0 {
		// Ledger peers are disabled
		return
	}
	if p.config.UseLedgerAfterSlot > 0 {
		currentSlot := p.config.LedgerPeerProvider.CurrentSlot()
		// Safe conversion: UseLedgerAfterSlot is already checked to be > 0
		useLedgerAfterSlot := uint64(p.config.UseLedgerAfterSlot) // #nosec G115
		if currentSlot < useLedgerAfterSlot {
			p.config.Logger.Debug(
				"ledger peers not yet enabled",
				"current_slot", currentSlot,
				"use_ledger_after_slot", p.config.UseLedgerAfterSlot,
			)
			return
		}
	}

	// Count existing ledger peers to determine how many we need.
	urgent := p.ledgerPeersUrgent()
	if !urgent {
		// Recovered: the next starvation event starts again at the base
		// emergency cadence rather than an escalated one.
		p.emergencyRefreshRounds.Store(0)
	}
	needed := p.ledgerPeerDeficit()
	p.config.Logger.Debug(
		"ledger peer discovery starting",
		"use_ledger_after_slot", p.config.UseLedgerAfterSlot,
		"needed", needed,
		"emergency", urgent,
	)
	// Deliberately no early return when needed <= 0 && !urgent: reconciling
	// ledgerKnownAddrs against the ledger's current relay set (below) must
	// still happen on this cadence even when no new peer is currently
	// needed, or a delisted relay whose stale association keeps the target
	// looking satisfied would never be reconciled away, permanently masking
	// the real deficit it leaves behind. The refresh-interval gate right
	// below is what actually bounds how often this fetches from the ledger;
	// deficit/urgency only decide extraAdds further down.

	// Atomically check and claim the refresh to prevent concurrent discoveries.
	// Use CompareAndSwap to ensure only one goroutine proceeds. The normal
	// cadence is LedgerPeerRefreshInterval, but when the node is critically
	// short of connected upstreams it replenishes on a much shorter emergency
	// interval instead of waiting up to an hour: a node must never wedge on a
	// collapsed peer pool while the ledger still lists plenty of relays.
	refreshInterval := p.config.LedgerPeerRefreshInterval
	if urgent {
		refreshInterval = p.emergencyLedgerRefreshInterval()
	}
	now := time.Now().UnixNano()
	lastRefresh := p.lastLedgerPeerRefresh.Load()
	if time.Duration(now-lastRefresh) < refreshInterval {
		return
	}

	// The timestamp gates when discovery may begin, but a slow provider can
	// outlive that interval. Hold a separate generation-owned claim across the
	// provider query and candidate pass so a later tick cannot overlap it.
	generation := p.ledgerDiscoveryGeneration.Add(1)
	if !p.ledgerDiscoveryInFlight.CompareAndSwap(0, generation) {
		return
	}
	claimedTimestamp := false
	completed := false
	defer func() {
		if claimedTimestamp && !completed {
			p.lastLedgerPeerRefresh.CompareAndSwap(now, lastRefresh)
		}
		p.ledgerDiscoveryInFlight.CompareAndSwap(generation, 0)
	}()

	// A prior owner may have completed between the first interval check and
	// this generation obtaining the claim. Recheck while ownership is held.
	lastRefresh = p.lastLedgerPeerRefresh.Load()
	if time.Duration(now-lastRefresh) < refreshInterval ||
		!p.lastLedgerPeerRefresh.CompareAndSwap(lastRefresh, now) {
		return
	}
	claimedTimestamp = true

	// Get pool relays from ledger
	if err := ctx.Err(); err != nil {
		return
	}
	relays, err := p.config.LedgerPeerProvider.GetPoolRelays()
	if err != nil {
		p.config.Logger.Error(
			"failed to get ledger peers",
			"error", err,
			"emergency", urgent,
		)
		return
	}
	if err := ctx.Err(); err != nil {
		return
	}

	candidates := dedupeRelayCandidates(flattenRelayCandidates(relays))

	// Reconcile ledgerKnownAddrs against this round's actual on-chain relay
	// set before adding anything new: an address a pool no longer registers
	// must stop counting toward LedgerPeerTarget once the chain itself has
	// moved on, independent of whether the peer that address matched is
	// still connected for some other reason. pruneLedgerKnownAddrsLocked
	// (run every reconcile) only catches the peer actually leaving the peer
	// list, which is a different and less frequent condition than the
	// ledger delisting a relay while its peer stays connected.
	p.reconcileLedgerKnownAddrs(candidates)

	extraAdds := 0
	if urgent && needed <= 0 {
		extraAdds = p.config.LedgerPeerTarget
	}
	addedCount := p.addLedgerRelaysContext(ctx, relays, extraAdds)

	if addedCount > 0 {
		p.config.Logger.Info(
			"discovered ledger peers",
			"added", addedCount,
			"target", p.config.LedgerPeerTarget,
			"candidates", len(candidates),
			"emergency", urgent,
		)
	} else {
		p.config.Logger.Debug(
			"ledger peer discovery complete",
			"candidates", len(candidates),
			"new_peers", 0,
			"emergency", urgent,
		)
	}
	if err := ctx.Err(); err != nil {
		return
	}
	if urgent {
		// Count only a complete urgent round. Interval-gated, failed, canceled,
		// or panicking rounds retain the existing backoff and retry immediately.
		p.emergencyRefreshRounds.Add(1)
	}
	completed = true
}

// ledgerPeersUrgent reports whether the node is critically short of connected
// upstreams and must replenish ledger peers on the emergency cadence rather
// than waiting for the normal refresh interval. Ledger discovery must be
// enabled (target > 0) for this to apply. The threshold is the hot-peer
// target: while the node has fewer eligible upstreams than it wants hot
// peers, it keeps pulling fresh relays so it never gets stuck on a shrinking
// pool of bad peers.
func (p *PeerGovernor) ledgerPeersUrgent() bool {
	if p.config.LedgerPeerTarget <= 0 {
		return false
	}
	p.mu.Lock()
	upstreams := p.countEligibleUpstreamsLocked()
	p.mu.Unlock()
	return upstreams < p.config.MinHotPeers
}

// emergencyLedgerRefreshInterval returns the refresh interval for an urgent
// ledger-discovery round. It starts at the configured emergency cadence and
// doubles for each consecutive round the node has spent short of upstreams,
// capped at the normal refresh interval so an urgent node never discovers
// less often than a healthy one. The counter resets on recovery, so a
// genuinely transient collapse is still served at the base cadence.
//
// Without the escalation a node whose relay pool is polluted (dead
// hostnames, wrong-network relays) never leaves the urgent state and runs
// discovery at the base cadence indefinitely, re-walking the whole relay set
// every round for as long as the node is up.
func (p *PeerGovernor) emergencyLedgerRefreshInterval() time.Duration {
	base := p.config.EmergencyLedgerPeerRefreshInterval
	normal := p.config.LedgerPeerRefreshInterval
	if normal <= 0 || base >= normal {
		return base
	}
	interval := base
	for range p.emergencyRefreshRounds.Load() {
		if interval >= normal {
			break
		}
		interval *= emergencyLedgerRefreshBackoffFactor
	}
	if interval > normal {
		return normal
	}
	return interval
}

// ledgerPeerDeficit returns how many more ledger peers are needed to reach
// the configured target. Returns 0 when the target is already satisfied.
func (p *PeerGovernor) ledgerPeerDeficit() int {
	target := p.config.LedgerPeerTarget
	if target <= 0 {
		return 0
	}
	p.mu.Lock()
	current := p.countLedgerPeersLocked()
	p.mu.Unlock()
	deficit := target - current
	if deficit < 0 {
		return 0
	}
	return deficit
}

// countLedgerPeersLocked returns the number of known peers that satisfy the
// ledger peer target. A peer counts if its source is PeerSourceP2PLedger or
// its address was seen during ledger discovery (covering peers that were
// already known from another source such as topology). Must be called with
// p.mu held.
func (p *PeerGovernor) countLedgerPeersLocked() int {
	count := 0
	for _, peer := range p.peers {
		if peer == nil {
			continue
		}
		if peer.Source == PeerSourceP2PLedger {
			count++
			continue
		}
		// ledgerKnownAddrs is keyed on normalizeAddress(peer.Address) (see
		// addLedgerPeerContext), not peer.NormalizedAddress.
		if _, ok := p.ledgerKnownAddrs[p.normalizeAddress(peer.Address)]; ok {
			count++
		}
	}
	return count
}

// flattenRelayCandidates converts a slice of PoolRelays into a flat list
// of "host:port" address strings suitable for addLedgerPeer.
func flattenRelayCandidates(relays []PoolRelay) []string {
	// Pre-size: most relays have 1-2 addresses.
	candidates := make([]string, 0, len(relays)*2)
	for _, relay := range relays {
		candidates = append(candidates, relay.Addresses()...)
	}
	return candidates
}

func dedupeRelayCandidates(candidates []string) []string {
	if len(candidates) < 2 {
		return candidates
	}

	seen := make(map[string]struct{}, len(candidates))
	unique := candidates[:0]
	for _, candidate := range candidates {
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		unique = append(unique, candidate)
	}
	return unique
}

// reconcileLedgerKnownAddrs prunes ledgerKnownAddrs entries whose address is
// not present in candidates, the current round's freshly fetched relay
// addresses. This is what actually reconciles the map against current
// ledger state: an address a pool delists (or moves to a different
// relay) is dropped here even if the peer it used to match is still
// connected under some other source, since that peer is no longer backed
// by an on-chain relay registration and should no longer count toward
// LedgerPeerTarget.
//
// candidates are pre-DNS "host:port" strings straight from the ledger
// provider; comparing them via normalizeAddress (lowercasing only, no
// lookup) against the same pre-DNS form ledgerKnownAddrs is keyed on lets
// this run every round without re-resolving every candidate, matching the
// no-DNS-until-necessary discipline the rest of ledger discovery follows.
// Must NOT be called while holding p.mu.
func (p *PeerGovernor) reconcileLedgerKnownAddrs(candidates []string) {
	// An empty result is treated as "no information" rather than "every
	// relay was delisted": GetPoolRelays returning zero addresses without an
	// error is not expected on a live chain (there are always registered
	// relays), so wiping the whole map on what is more likely a transient or
	// degenerate provider response would be actively harmful for no benefit.
	if len(candidates) == 0 {
		return
	}
	current := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		current[p.normalizeAddress(candidate)] = struct{}{}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	// Each entry's value is the raw candidate string the peer at that key
	// was last matched against (see addLedgerPeerContext /
	// ledgerPeerRejectedWithoutDNS); compare that value, not the key, since
	// the key is the peer's own address and may differ syntactically from
	// the ledger's candidate form for the same relay.
	for peerKey, matchedCandidate := range p.ledgerKnownAddrs {
		if _, ok := current[matchedCandidate]; !ok {
			delete(p.ledgerKnownAddrs, peerKey)
		}
	}
}

// addLedgerPeer adds a peer from ledger discovery with deduplication.
// Returns true if the peer was added, false if it already exists or is denied.
//
//nolint:unused // Kept as a context-free test helper for existing peer tests.
func (p *PeerGovernor) addLedgerPeer(address string) bool {
	return p.addLedgerPeerContext(context.Background(), address)
}

func (p *PeerGovernor) addLedgerPeerContext(
	ctx context.Context,
	address string,
) bool {
	if err := ctx.Err(); err != nil {
		return false
	}
	// Decide what can be decided without DNS first. Discovery re-offers the
	// full relay set on every round, so resolving ahead of the deny and
	// exists checks re-resolves every already-connected peer and every dead
	// hostname every round; neither lookup can change the outcome. Deny
	// entries for an unresolvable hostname are keyed on exactly the
	// lock-free normalized form, which is what makes the dead-hostname case
	// answerable here.
	if p.ledgerPeerRejectedWithoutDNS(address) {
		return false
	}
	// Resolve address (with DNS lookup) before acquiring lock to avoid
	// blocking while holding the mutex. Ledger relay hostnames are
	// attacker-supplied, and resolveLedgerDialTarget's fast path dials
	// whatever ends up here unchanged for the peer's whole lifetime, so this
	// (unlike resolveAddress) filters to a locally-dialable address family.
	normalized := p.resolveLedgerDiscoveryAddress(ctx, address)
	// Rechecked after resolution, not just before it: a canceled DNS lookup
	// falls back to the bare hostname, which isRoutableAddr accepts, so
	// without this a shutdown that lands during resolution would still add
	// the peer and let the reconnect path start dialing it.
	if err := ctx.Err(); err != nil {
		return false
	}

	// Reject non-routable IPs (private, loopback, link-local, etc.)
	if !isRoutableAddr(normalized) {
		return false
	}
	var evt *pendingEvent
	added := false

	p.mu.Lock()

	// Rechecked under the lock, which is what actually closes the window:
	// the pre-resolution and post-resolution checks above both run lock-free,
	// so a cancellation landing between them and here would otherwise still
	// mutate peer state and spawn a reconnect. Every mutation below happens
	// under this same acquisition, so nothing can slip past this point.
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		return false
	}

	hostnameNormalized := p.normalizeAddress(address)

	// Check deny list
	if p.isDeniedLocked(normalized) ||
		p.isDeniedLocked(hostnameNormalized) {
		p.mu.Unlock()
		return false
	}

	// Check for existing peer using cached NormalizedAddress. The address
	// comparison is normalized on both sides, as in AddPeer, so a peer
	// holding the same hostname under different casing is not duplicated.
	var existingPeer *Peer
	for _, peer := range p.peers {
		if peer == nil {
			continue
		}
		if peer.NormalizedAddress == normalized ||
			peer.NormalizedAddress == hostnameNormalized ||
			p.normalizeAddress(peer.Address) == hostnameNormalized {
			existingPeer = peer
			break
		}
	}
	if existingPeer != nil {
		// The candidate is a valid ledger relay backed by a peer we already
		// retain from another source. Record that retained peer's address so
		// it counts toward the ledger target without adding a duplicate.
		//
		// Keyed on the retained peer's own normalizeAddress(peer.Address)
		// (matching countLedgerPeersLocked/pruneLedgerKnownAddrsLocked), not
		// on the candidate's hostnameNormalized form: the two can differ
		// (e.g. the peer was added under its resolved IP while the ledger
		// lists a hostname for the same relay), and keying on the candidate
		// would silently break both the counting lookup and peer-retention
		// pruning for that peer. The value is the raw candidate string
		// (pre-DNS form) so a later discovery round's
		// reconcileLedgerKnownAddrs can compare it against the ledger's
		// fresh relay list, itself pre-resolution, without re-resolving
		// every candidate.
		p.ledgerKnownAddrs[p.normalizeAddress(existingPeer.Address)] = hostnameNormalized
		p.mu.Unlock()
		return false
	}

	// Enforce hard cap on peer list size
	if p.isAtPeerCapLocked() {
		p.config.Logger.Debug(
			"rejecting ledger peer: peer list at capacity",
			"address", address,
			"cap", p.maxPeerListSize(),
			"current", len(p.peers),
		)
		p.mu.Unlock()
		return false
	}

	// Record only admitted ledger addresses. Keeping duplicate or
	// capacity-rejected candidates would make them count toward the ledger
	// target even though the governor does not retain them as peers. Keyed
	// on normalizeAddress(newPeer.Address), matching the existingPeer branch
	// above; for a brand-new peer Address is exactly address, so this key is
	// always identical to hostnameNormalized here.
	p.ledgerKnownAddrs[p.normalizeAddress(address)] = hostnameNormalized

	// Add as new peer
	newPeer := &Peer{
		Address:           address,
		NormalizedAddress: normalized,
		Source:            PeerSourceP2PLedger,
		State:             PeerStateCold,
		Sharable:          true, // Ledger peers are public relays
		EMAAlpha:          p.config.EMAAlpha,
		FirstSeen:         time.Now(),
	}
	p.peers = append(p.peers, newPeer)
	p.updatePeerMetrics()

	if p.metrics != nil {
		p.metrics.increasedKnownPeers.Inc()
	}

	// Check if the governor is running (stopCh is set by Start)
	// and outbound connections are enabled before spawning.
	//
	// Spawning must happen inside this same critical section: see
	// AddPeer's identical comment in peers.go for why calling
	// spawnOutboundConnection after unlocking races Stop's
	// stopCh-clearing + p.wg.Wait() and can let Stop return before this
	// dial is ever registered with that WaitGroup.
	shouldConnect := p.stopCh != nil && !p.config.DisableOutbound
	if shouldConnect {
		p.spawnOutboundConnectionLocked(newPeer)
	}
	evt = &pendingEvent{
		PeerAddedEventType,
		PeerStateChangeEvent{Address: address, Reason: "ledger"},
	}
	added = true
	p.mu.Unlock()

	// Publish event outside of lock to avoid deadlock
	p.publishEvent(evt.eventType, evt.data)

	return added
}

// ledgerPeerRejectedWithoutDNS reports whether a ledger relay candidate can
// be rejected from the raw address alone, before any DNS lookup.
//
// Only rejection is decided here: a peer is never added without a
// resolution, so a candidate that survives this check still goes through the
// full post-resolution deny and exists checks under the lock.
func (p *PeerGovernor) ledgerPeerRejectedWithoutDNS(address string) bool {
	hostnameNormalized := p.normalizeAddress(address)
	p.mu.Lock()
	defer p.mu.Unlock()
	// A deny entry recorded for an unresolvable hostname is keyed on the
	// lowercased hostname, so this catches exactly the candidates whose
	// resolution would fail anyway.
	if p.isDeniedLocked(hostnameNormalized) {
		return true
	}
	for _, peer := range p.peers {
		if peer == nil {
			continue
		}
		// Both sides are normalized, matching AddPeer: Peer.Address is
		// stored verbatim, so a topology or gossip peer can hold the same
		// relay hostname under different casing.
		if peer.NormalizedAddress != hostnameNormalized &&
			p.normalizeAddress(peer.Address) != hostnameNormalized {
			continue
		}
		// This is a valid ledger relay already retained from another source,
		// not an unusable rejected candidate. Associate the retained peer with
		// ledger discovery so it contributes to LedgerPeerTarget. Keyed on
		// the retained peer's own address, matching addLedgerPeerContext;
		// see that function's existingPeer branch for why keying on the
		// candidate's hostname form instead would be wrong.
		p.ledgerKnownAddrs[p.normalizeAddress(peer.Address)] = hostnameNormalized
		return true
	}
	return false
}
