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
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
)

func isConnectionCancellationError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, net.ErrClosed) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "operation was canceled") ||
		strings.Contains(msg, "context canceled") ||
		strings.Contains(msg, "use of closed network connection")
}

func isExpectedNetworkDialError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "server misbehaving") ||
		strings.Contains(msg, "connect: connection refused") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "cannot assign requested address") ||
		strings.Contains(msg, "version data mismatch") ||
		strings.Contains(msg, "timeout waiting on transition")
}

// isAddrInUseError returns true if the error is a "cannot assign
// requested address" or EADDRINUSE, indicating a TCP 4-tuple collision.
func isAddrInUseError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EADDRNOTAVAIL) ||
		errors.Is(err, syscall.EADDRINUSE) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "cannot assign requested address")
}

func isExpectedConnectionCloseError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "timeout waiting on transition") ||
		msg == "eof"
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
	reconnectPeer := currentPeer
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
		idx := p.peerIndexByAddress(reconnectPeer.NormalizedAddress)
		if idx != -1 && p.peers[idx] == reconnectPeer {
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
		if currentPeer := p.peers[peerIdx]; p.inboundSatisfiesTopologyValencyLocked(currentPeer) {
			p.mu.Unlock()
			p.config.Logger.Info(
				"outbound: inbound reusable topology connections already satisfy valency, suppressing outbound attempts",
				"address", peer.Address,
				"group", currentPeer.GroupID,
				"valency", currentPeer.Valency,
			)
			return
		}
		// Only a client-capable connection can replace the outbound dial.
		// This matches ouroboros-network's duplex-connection reuse: an
		// existing inbound responder-only connection is not enough to
		// satisfy the local initiator side.
		if currentPeer := p.peers[peerIdx]; currentPeer.hasClientConnection() {
			p.mu.Unlock()
			p.config.Logger.Info(
				"outbound: peer already connected via reusable duplex connection, stopping outbound attempts",
				"address", peer.Address,
			)
			return
		}
		currentPeerConnKnown := p.peers[peerIdx].Connection != nil
		p.mu.Unlock()

		// Skip outbound if there is already an inbound connection from the
		// same exact remote peer address. Matching on the full remote
		// address avoids suppressing valid outbound dials to peers that
		// happen to share a host but use a different source port.
		//
		// We only wait here while the peer connection state is not yet known.
		// Once the inbound side has finished negotiating, only a client-capable
		// duplex connection should suppress the outbound dial.
		if !currentPeerConnKnown && p.config.ConnManager != nil &&
			p.config.ConnManager.HasInboundPeerAddress(peer.NormalizedAddress) {
			p.config.Logger.Info(
				"outbound: inbound from same peer address exists before negotiation completes, waiting",
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
			oldSource := currentPeer.Source
			oldConn := clonePeerConnection(currentPeer.Connection)
			currentPeer.ConnectedAt = time.Now()
			currentPeer.setConnection(conn, true)
			currentPeer.State = PeerStateWarm
			selectionEvents := p.appendChainSelectionEventsLocked(
				nil,
				p.bootstrapExited,
				oldSource,
				oldConn,
				currentPeer,
			)
			p.updatePeerMetrics()
			p.mu.Unlock()
			p.publishPendingEvents(selectionEvents)
			// Generate event
			p.publishEvent(
				OutboundConnectionEventType,
				OutboundConnectionEvent{ConnectionId: connId},
			)
			return
		}
		if isConnectionCancellationError(err) {
			select {
			case <-p.stopCh:
				p.config.Logger.Debug(
					"outbound: connection attempt canceled during shutdown",
					"address", peer.Address,
				)
				return
			default:
			}
			if p.ctx.Err() != nil {
				p.config.Logger.Debug(
					"outbound: connection attempt canceled, governor context done",
					"address", peer.Address,
				)
				return
			}
			p.mu.Lock()
			peerIdx = p.peerIndexByAddress(peer.NormalizedAddress)
			if peerIdx == -1 {
				p.mu.Unlock()
				p.config.Logger.Debug(
					"outbound: peer removed during canceled connection attempt",
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
			reconnectDelay := currentPeer.ReconnectDelay
			p.mu.Unlock()
			p.config.Logger.Debug(
				"outbound: connection attempt canceled, retrying with backoff",
				"address", peer.Address,
				"error", err,
				"delay", reconnectDelay,
			)
			select {
			case <-p.stopCh:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}
		failMsg := fmt.Sprintf(
			"outbound: failed to establish connection to %s: %s",
			peer.Address,
			err,
		)
		if isExpectedNetworkDialError(err) {
			p.config.Logger.Info(failMsg)
		} else {
			p.config.Logger.Error(failMsg)
		}
		// When the dial fails because an inbound connection already
		// occupies the TCP 4-tuple, don't count it as a peer failure.
		// Loop back to the exact inbound peer-address check instead.
		if isAddrInUseError(err) {
			p.mu.Lock()
			peerIdx := p.peerIndexByAddress(peer.NormalizedAddress)
			currentPeerConnKnown := false
			if peerIdx != -1 {
				currentPeerConnKnown = p.peers[peerIdx].Connection != nil
			}
			p.mu.Unlock()
			if !currentPeerConnKnown &&
				p.config.ConnManager != nil &&
				p.config.ConnManager.HasInboundPeerAddress(peer.NormalizedAddress) {
				p.config.Logger.Info(
					"outbound: dial failed while exact inbound peer address is still negotiating, waiting",
					"address", peer.Address,
				)
				continue
			}
		}
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
		peerSource := currentPeer.Source
		peerAddress := currentPeer.Address
		peerNormalizedAddress := currentPeer.NormalizedAddress
		if !p.isTopologyPeer(peerSource) &&
			reconnectCount > p.config.MaxReconnectFailureThreshold {
			// Fail fast for non-topology peers. Waiting for the long
			// reconcile interval causes large reconnect storms.
			p.denyList[peerNormalizedAddress] = time.Now().Add(
				p.config.DenyDuration,
			)
			p.peers = append(p.peers[:peerIdx], p.peers[peerIdx+1:]...)
			p.updatePeerMetrics()
			p.mu.Unlock()
			p.config.Logger.Info(
				"outbound: removing peer after repeated connection failures",
				"address", peerAddress,
				"source", peerSource.String(),
				"retry_count", reconnectCount,
				"deny_duration", p.config.DenyDuration,
			)
			p.publishEvent(
				PeerRemovedEventType,
				PeerStateChangeEvent{
					Address: peerAddress,
					Reason:  "excessive outbound connection failures",
				},
			)
			return
		}
		p.mu.Unlock()
		select {
		case <-p.stopCh:
			p.config.Logger.Debug(
				"outbound: stopping connection attempts due to shutdown",
				"address", peer.Address,
			)
			return
		default:
		}
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
	// Skip node-to-client connections — they are local clients
	// (wallets, tools), not network peers.
	if e.IsNtC {
		return
	}
	address := e.RemoteAddr.String()
	// Key on the connmanager's canonical transport identity so inbound
	// peer entries share a key space with HasInboundPeerAddress and
	// inboundPeerAddrs. Never DNS-resolve the inbound remote: the
	// 4-tuple is an IP:port and calling resolveAddress would diverge
	// from the connmanager's non-DNS normalization. When an event is
	// synthesized in tests without a populated NormalizedRemoteAddr,
	// fall back to NormalizePeerAddr to preserve the same contract.
	normalized := e.NormalizedRemoteAddr
	if normalized == "" {
		normalized = connmanager.NormalizePeerAddr(address)
	}
	now := time.Now()

	var selectionEvents []pendingEvent
	p.mu.Lock()
	if p.isDeniedLocked(normalized) {
		p.recordInboundLifecycle("denied")
		p.config.Logger.Info(
			"denied inbound peer during cooldown",
			"address", address,
		)
		p.mu.Unlock()
		return
	}
	peerIdx, topologyGroupID := p.resolveInboundIdentity(address, normalized)
	var tmpPeer *Peer
	if peerIdx == -1 {
		// Enforce hard cap on peer list size for inbound peers
		if p.isAtPeerCapLocked() {
			p.recordInboundLifecycle("rejected")
			p.config.Logger.Debug(
				"rejecting inbound peer: peer list at capacity",
				"address", address,
				"cap", p.maxPeerListSize(),
				"current", len(p.peers),
			)
			p.mu.Unlock()
			return
		}
		tmpPeer = &Peer{
			Address:           address,
			NormalizedAddress: normalized,
			Source:            PeerSourceInboundConn,
			State:             PeerStateCold,
			EMAAlpha:          p.config.EMAAlpha,
			FirstSeen:         now,
		}
		// Add inbound peer
		p.peers = append(
			p.peers,
			tmpPeer,
		)
		p.recordInboundLifecycle("accepted")
	} else {
		tmpPeer = p.peers[peerIdx]
		if tmpPeer == nil {
			p.mu.Unlock()
			return
		}
		p.recordInboundLifecycle("accepted")
		// Record the topology identity once on first rule-2 match and
		// keep it across subsequent reconnects. Rule-1 matches yield
		// topologyGroupID == "" and must not clear a prior match.
		if topologyGroupID != "" && tmpPeer.InboundTopologyMatch == "" {
			tmpPeer.InboundTopologyMatch = topologyGroupID
		}
	}
	oldSource := tmpPeer.Source
	oldConn := clonePeerConnection(tmpPeer.Connection)
	hadClientConnection := tmpPeer.hasClientConnection()
	// Accept an event-embedded duplex=true hint as a provisional upgrade;
	// do not clear a previously known true value on best-effort false.
	// The connmanager lookup below is authoritative when present.
	if e.IsDuplex {
		tmpPeer.InboundDuplex = true
	}
	tmpPeer.InboundConnectedAt = now
	if p.config.ConnManager != nil {
		conn := p.config.ConnManager.GetConnectionById(e.ConnectionId)
		if conn != nil {
			inboundPeerConn := &Peer{}
			inboundPeerConn.setConnection(conn, false)
			inboundIsClient := inboundPeerConn.hasClientConnection()
			if !hadClientConnection || inboundIsClient {
				// Preserve duplex-reuse semantics: a responder-only inbound
				// must not replace an authoritative client-capable connection.
				tmpPeer.setConnection(conn, false)
				if tmpPeer.Connection != nil {
					tmpPeer.Sharable = tmpPeer.Connection.VersionData.PeerSharing()
					tmpPeer.State = PeerStateWarm
					p.recordInboundLifecycle("warmed")
				}
			}
			// setConnection derives IsClient from live handshake data; when
			// available, treat it as authoritative for inbound duplex metadata.
			tmpPeer.InboundDuplex = inboundIsClient
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
	selectionEvents = p.appendChainSelectionEventsLocked(
		selectionEvents,
		p.bootstrapExited,
		oldSource,
		oldConn,
		tmpPeer,
	)
	if p.metrics != nil {
		p.metrics.inboundArrivalsTotal.Inc()
	}
	p.updatePeerMetrics()
	p.mu.Unlock()

	p.publishPendingEvents(selectionEvents)
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
	var selectionEvents []pendingEvent
	p.mu.Lock()
	if e.Error != nil {
		closeMsg := fmt.Sprintf(
			"unexpected connection failure: %s",
			e.Error,
		)
		if isExpectedConnectionCloseError(e.Error) {
			p.config.Logger.Info(
				closeMsg,
				"connection_id", e.ConnectionId.String(),
			)
		} else {
			p.config.Logger.Error(
				closeMsg,
				"connection_id", e.ConnectionId.String(),
			)
		}
	} else {
		p.config.Logger.Info("connection closed",
			"connection_id", e.ConnectionId.String(),
		)
	}
	peerIdx := p.peerIndexByConnId(e.ConnectionId)
	if peerIdx != -1 && p.peers[peerIdx] != nil {
		peer := p.peers[peerIdx]
		oldSource := peer.Source
		oldConn := clonePeerConnection(peer.Connection)
		connClosedAt := time.Now()
		if peer.Source == PeerSourceInboundConn {
			connDur := time.Duration(0)
			if !peer.InboundConnectedAt.IsZero() {
				connDur = connClosedAt.Sub(peer.InboundConnectedAt)
			}
			peer.LastInboundSessionDuration = connDur
			// Reset burst when reconnects are no longer clustered inside the
			// inbound cooldown window.
			if !peer.LastInboundDisconnect.IsZero() &&
				connClosedAt.Sub(peer.LastInboundDisconnect) >= p.config.InboundCooldown {
				peer.InboundShortLivedCount = 0
			}
			if !peer.InboundConnectedAt.IsZero() &&
				connDur < minStableConnectionDuration {
				peer.InboundShortLivedCount++
			} else if !peer.InboundConnectedAt.IsZero() {
				peer.InboundShortLivedCount = 0
			}
			peer.LastInboundDisconnect = connClosedAt
			peer.InboundConnectedAt = time.Time{}
		}
		peer.Connection = nil
		peer.State = PeerStateCold
		selectionEvents = p.appendChainSelectionEventsLocked(
			selectionEvents,
			p.bootstrapExited,
			oldSource,
			oldConn,
			peer,
		)
		if peer.Source != PeerSourceInboundConn {
			peer.ConnectedAt = time.Time{}
		}
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
	p.mu.Unlock()

	p.publishPendingEvents(selectionEvents)
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
