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

		conn, err := p.config.ConnManager.CreateOutboundConn(p.ctx, peer.Address)
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
			currentPeer.ReconnectCount = 0
			currentPeer.ReconnectDelay = 0
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
			go p.createOutboundConnection(peer)
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
