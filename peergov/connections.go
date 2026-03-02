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
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
)

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

	// Take a snapshot of peers under lock to avoid racing with AddPeer
	// or inbound events
	p.mu.Lock()
	peers := append([]*Peer(nil), p.peers...)
	p.mu.Unlock()

	for _, tmpPeer := range peers {
		if tmpPeer != nil {
			go p.createOutboundConnection(tmpPeer)
		}
	}
}

func (p *PeerGovernor) createOutboundConnection(peer *Peer) {
	if peer == nil {
		return
	}
	// Guard against multiple goroutines reconnecting to the same peer.
	// This can happen when an inbound connection to a topology peer
	// closes while an outbound retry loop is already running.
	p.mu.Lock()
	idx := p.peerIndexByAddress(peer.NormalizedAddress)
	if idx == -1 {
		p.mu.Unlock()
		return
	}
	currentPeer := p.peers[idx]
	if currentPeer.Reconnecting {
		p.mu.Unlock()
		p.config.Logger.Debug(
			"outbound: reconnect goroutine already active, skipping",
			"address", peer.Address,
		)
		return
	}
	currentPeer.Reconnecting = true
	// Read any pre-existing reconnect delay set by
	// handleConnectionClosedEvent for short-lived connections.
	preDelay := currentPeer.ReconnectDelay
	// Zero the stored delay after consuming it so the retry
	// loop's backoff starts cleanly from the intended rung
	// and does not double-apply the pre-delay.
	currentPeer.ReconnectDelay = 0
	p.mu.Unlock()
	// Ensure Reconnecting is cleared when this goroutine exits
	defer func() {
		p.mu.Lock()
		idx := p.peerIndexByAddress(peer.NormalizedAddress)
		if idx != -1 {
			p.peers[idx].Reconnecting = false
		}
		p.mu.Unlock()
	}()
	if preDelay > 0 {
		p.config.Logger.Info(
			fmt.Sprintf(
				"outbound: delaying %s before reconnecting to %s (connection instability backoff)",
				preDelay,
				peer.Address,
			),
		)
		select {
		case <-p.stopCh:
			return
		case <-time.After(preDelay):
		}
	}
	for {
		// Check if PeerGovernor is stopping
		select {
		case <-p.stopCh:
			p.config.Logger.Debug(
				"outbound: stopping connection attempts due to shutdown",
				"address", peer.Address,
			)
			return
		default:
		}

		// Check if peer still exists and is not denied
		p.mu.Lock()
		peerIdx := p.peerIndexByAddress(peer.NormalizedAddress)
		if peerIdx == -1 {
			p.mu.Unlock()
			p.config.Logger.Debug(
				"outbound: peer removed, stopping connection attempts",
				"address", peer.Address,
			)
			return
		}
		if p.isDeniedLocked(peer.NormalizedAddress) {
			p.mu.Unlock()
			p.config.Logger.Debug(
				"outbound: peer denied, stopping connection attempts",
				"address", peer.Address,
			)
			return
		}
		p.mu.Unlock()

		// Skip outbound if the peer already has an inbound connection
		// from the same host. When both nodes reuse their listen port
		// for outbound (OutboundSourcePort), a separate outbound would
		// create an identical TCP 4-tuple and fail with EADDRINUSE.
		// The existing inbound connection serves both directions.
		// Poll periodically rather than returning so that when the
		// inbound drops the outbound attempt proceeds naturally.
		if p.config.ConnManager != nil &&
			p.config.ConnManager.HasInboundFromHost(peer.Address) {
			p.config.Logger.Info(
				"outbound: inbound from same host exists, waiting",
				"address", peer.Address,
			)
			select {
			case <-p.stopCh:
				return
			case <-time.After(inboundCheckDelay):
			}
			continue
		}

		conn, err := p.config.ConnManager.CreateOutboundConn(
			p.ctx,
			peer.Address,
		)
		if err == nil {
			connId := conn.Id()
			p.mu.Lock()
			// Re-check that peer is still valid after connection was created
			// to eliminate TOCTOU race window
			peerIdx := p.peerIndexByAddress(peer.NormalizedAddress)
			if peerIdx == -1 || p.isDeniedLocked(peer.NormalizedAddress) {
				p.mu.Unlock()
				// Peer was removed or denied while connecting, close connection
				conn.Close()
				p.config.Logger.Debug(
					"outbound: peer removed/denied during connection, closing",
					"address", peer.Address,
				)
				return
			}
			// Use the peer from the slice in case the pointer changed
			currentPeer := p.peers[peerIdx]
			currentPeer.ConnectedAt = time.Now()
			currentPeer.setConnection(conn, true)
			currentPeer.State = PeerStateWarm
			p.updatePeerMetrics()
			p.mu.Unlock()
			// Generate event
			p.publishEvent(
				OutboundConnectionEventType,
				OutboundConnectionEvent{ConnectionId: connId},
			)
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
		// Re-lookup peer to avoid stale pointer if slice was rebuilt
		peerIdx = p.peerIndexByAddress(peer.NormalizedAddress)
		if peerIdx == -1 {
			p.mu.Unlock()
			p.config.Logger.Debug(
				"outbound: peer removed during connection attempt, stopping",
				"address", peer.Address,
			)
			return
		}
		currentPeer := p.peers[peerIdx]
		if currentPeer.ReconnectDelay == 0 {
			currentPeer.ReconnectDelay = initialReconnectDelay
		} else if currentPeer.ReconnectDelay < maxReconnectDelay {
			currentPeer.ReconnectDelay *= reconnectBackoffFactor
			if currentPeer.ReconnectDelay > maxReconnectDelay {
				currentPeer.ReconnectDelay = maxReconnectDelay
			}
		}
		currentPeer.ReconnectCount++
		// Copy values while holding lock for use after unlock
		reconnectDelay := currentPeer.ReconnectDelay
		reconnectCount := currentPeer.ReconnectCount
		p.mu.Unlock()
		p.config.Logger.Info(
			fmt.Sprintf(
				"outbound: delaying %s (retry %d) before reconnecting to %s",
				reconnectDelay,
				reconnectCount,
				peer.Address,
			),
		)
		// Use select with stopCh for interruptible sleep
		select {
		case <-p.stopCh:
			p.config.Logger.Debug(
				"outbound: stopping connection attempts due to shutdown",
				"address", peer.Address,
			)
			return
		case <-time.After(reconnectDelay):
		}
	}
}

func (p *PeerGovernor) handleInboundConnectionEvent(evt event.Event) {
	e, ok := evt.Data.(connmanager.InboundConnectionEvent)
	if !ok {
		p.config.Logger.Warn(
			"handleInboundConnectionEvent: unexpected event data type",
			"type", fmt.Sprintf("%T", evt.Data),
		)
		return
	}
	address := e.RemoteAddr.String()
	// Resolve address before acquiring lock to avoid blocking DNS
	normalized := p.resolveAddress(address)

	p.mu.Lock()
	defer p.mu.Unlock()
	peerIdx := -1
	// Check if peer already exists (possibly as inbound)
	for i, peer := range p.peers {
		if peer == nil {
			continue
		}
		// Match by exact address or normalized address
		if peer.Address == address || peer.NormalizedAddress == normalized {
			peerIdx = i
			break
		}
	}
	var tmpPeer *Peer
	if peerIdx == -1 {
		// Enforce hard cap on peer list size for inbound peers
		if p.isAtPeerCapLocked() {
			p.config.Logger.Debug(
				"rejecting inbound peer: peer list at capacity",
				"address", address,
				"cap", p.maxPeerListSize(),
				"current", len(p.peers),
			)
			return
		}
		tmpPeer = &Peer{
			Address:           address,
			NormalizedAddress: normalized,
			Source:            PeerSourceInboundConn,
			State:             PeerStateCold,
			EMAAlpha:          p.config.EMAAlpha,
			FirstSeen:         time.Now(),
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
	// Reset outbound backoff when an inbound connection from a
	// topology peer succeeds. The inbound proves the peer is
	// reachable, so if it drops later, the outbound should retry
	// immediately rather than waiting on accumulated backoff.
	if tmpPeer.Source != PeerSourceInboundConn {
		tmpPeer.ReconnectDelay = 0
		tmpPeer.ReconnectCount = 0
	}
	p.updatePeerMetrics()
}

func (p *PeerGovernor) handleConnectionClosedEvent(evt event.Event) {
	e, ok := evt.Data.(connmanager.ConnectionClosedEvent)
	if !ok {
		p.config.Logger.Warn(
			"handleConnectionClosedEvent: unexpected event data type",
			"type", fmt.Sprintf("%T", evt.Data),
		)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
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
		peer := p.peers[peerIdx]
		peer.Connection = nil
		peer.State = PeerStateCold
		p.updatePeerMetrics()
		// Only reconnect for outbound peers that are not on the deny list
		if peer.Source != PeerSourceInboundConn &&
			!p.isDeniedLocked(peer.NormalizedAddress) {
			// Apply backoff for short-lived connections to prevent
			// rapid reconnection cycles that exhaust ephemeral ports.
			// Only reset backoff when connection proved stable.
			connDur := time.Since(peer.ConnectedAt)
			if !peer.ConnectedAt.IsZero() &&
				connDur >= minStableConnectionDuration {
				// Connection was stable, reset backoff
				peer.ReconnectCount = 0
				peer.ReconnectDelay = 0
			} else if !peer.ConnectedAt.IsZero() {
				// Short-lived connection: apply exponential backoff
				if peer.ReconnectDelay == 0 {
					peer.ReconnectDelay = initialReconnectDelay
				} else if peer.ReconnectDelay < maxReconnectDelay {
					peer.ReconnectDelay *= reconnectBackoffFactor
					if peer.ReconnectDelay > maxReconnectDelay {
						peer.ReconnectDelay = maxReconnectDelay
					}
				}
				p.config.Logger.Warn(
					"short-lived connection detected, applying backoff",
					"address", peer.Address,
					"connection_duration", connDur,
					"next_delay", peer.ReconnectDelay,
				)
			}
			peer.ConnectedAt = time.Time{} // Reset for next connection
			// Only spawn a new reconnect goroutine if one is not
			// already running. The active goroutine's defer
			// cleanup in createOutboundConnection will clear
			// Reconnecting when it exits.
			// Do NOT set Reconnecting here — createOutboundConnection
			// sets it itself after acquiring the lock. Setting it
			// here would cause the goroutine to see it as already
			// active and immediately return.
			if !peer.Reconnecting {
				go p.createOutboundConnection(peer)
			}
		}
	}
}

// DenyPeer adds a peer to the deny list for the specified duration.
// If duration is 0, the configured default duration is used.
// This method is thread-safe.
func (p *PeerGovernor) DenyPeer(address string, duration time.Duration) {
	if duration == 0 {
		duration = p.config.DenyDuration
	}
	// Resolve address before acquiring lock to avoid blocking DNS
	normalized := p.resolveAddress(address)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.denyList[normalized] = time.Now().Add(duration)
	p.config.Logger.Debug(
		"peer added to deny list",
		"address", address,
		"duration", duration,
	)
}

// IsDenied checks if a peer is currently on the deny list.
// Returns true if the peer is denied and the denial has not expired.
// This method is thread-safe.
func (p *PeerGovernor) IsDenied(address string) bool {
	// Resolve address before acquiring lock to avoid blocking DNS
	normalized := p.resolveAddress(address)
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.isDeniedLocked(normalized)
}

// isDeniedLocked checks if a peer is on the deny list.
// This method assumes the mutex is already held by the caller.
func (p *PeerGovernor) isDeniedLocked(address string) bool {
	expiry, exists := p.denyList[address]
	if !exists {
		return false
	}
	if time.Now().After(expiry) {
		// Expired, remove from deny list
		delete(p.denyList, address)
		return false
	}
	return true
}

// cleanupDenyList removes expired entries from the deny list.
// This method assumes the mutex is already held by the caller.
func (p *PeerGovernor) cleanupDenyList() {
	now := time.Now()
	for address, expiry := range p.denyList {
		if now.After(expiry) {
			delete(p.denyList, address)
		}
	}
}

// TestPeer tests a peer's suitability by attempting a connection and verifying
// the Ouroboros protocol handshake succeeds. Returns true if the peer is
// suitable, false otherwise. Results are cached to avoid excessive testing.
// This method is thread-safe.
func (p *PeerGovernor) TestPeer(address string) (bool, error) {
	// Resolve address before acquiring lock to avoid blocking DNS
	normalized := p.resolveAddress(address)
	p.mu.Lock()

	// Find or create peer entry
	var peer *Peer
	if idx := p.peerIndexByAddress(normalized); idx == -1 {
		// Enforce hard cap on peer list size
		if p.isAtPeerCapLocked() {
			p.mu.Unlock()
			return false, ErrPeerListFull
		}
		// Peer not known yet, create temporary entry to track test result
		peer = &Peer{
			Address:           address,
			NormalizedAddress: normalized,
			Source:            PeerSourceUnknown,
			State:             PeerStateCold,
			EMAAlpha:          p.config.EMAAlpha,
			FirstSeen:         time.Now(),
		}
		p.peers = append(p.peers, peer)
		p.updatePeerMetrics()
	} else {
		peer = p.peers[idx]
	}

	// Check if recently tested (within cooldown)
	if !peer.LastTestTime.IsZero() &&
		time.Since(peer.LastTestTime) < p.config.TestCooldown {
		result := peer.LastTestResult == TestResultPass
		p.mu.Unlock()
		if result {
			return true, nil
		}
		return false, errors.New("peer failed previous test")
	}

	p.mu.Unlock()

	// Perform the test (outside lock to avoid blocking)
	var testErr error
	if p.config.PeerTestFunc != nil {
		// Use custom test function if provided
		testErr = p.config.PeerTestFunc(address)
	} else if p.config.ConnManager != nil {
		// Default: attempt connection via ConnManager
		conn, err := p.config.ConnManager.CreateOutboundConn(p.ctx, address)
		if err != nil {
			testErr = err
		} else {
			// Connection succeeded, close it since this is just a test
			conn.Close()
		}
	} else {
		testErr = errors.New("no test function or connection manager configured")
	}

	// Update test result
	p.mu.Lock()
	defer p.mu.Unlock()

	// Re-find peer in case slice changed
	idx := p.peerIndexByAddress(normalized)
	if idx == -1 {
		// Peer was removed during test, nothing to update
		if testErr != nil {
			return false, testErr
		}
		return true, nil
	}
	peer = p.peers[idx]

	peer.LastTestTime = time.Now()
	if testErr != nil {
		peer.LastTestResult = TestResultFail
		// Add to deny list (we already hold the lock)
		// Use pre-resolved normalized address for consistent deny list lookups
		p.denyList[normalized] = time.Now().Add(p.config.DenyDuration)
		p.config.Logger.Debug(
			"peer suitability test failed, added to deny list",
			"address", address,
			"error", testErr,
			"deny_duration", p.config.DenyDuration,
		)
		return false, testErr
	}

	peer.LastTestResult = TestResultPass
	p.config.Logger.Debug(
		"peer suitability test passed",
		"address", address,
	)
	return true, nil
}
