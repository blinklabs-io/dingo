// Copyright 2026 Blink Labs Software
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

package chainselection

import (
	"context"
	"io"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

const (
	defaultEvaluationInterval = 10 * time.Second
	defaultStaleTipThreshold  = 60 * time.Second

	// DefaultMaxTrackedPeers is the maximum number of peers tracked by
	// the ChainSelector. When a new peer is added and the limit is reached,
	// the least-recently-updated peer is evicted. This bounds memory usage
	// and CPU cost of chain selection, preventing Sybil-based resource
	// exhaustion.
	DefaultMaxTrackedPeers = 200
)

// safeBlockDiff computes the difference between two block numbers as int64,
// handling potential overflow by clamping to math.MaxInt64.
func safeBlockDiff(a, b uint64) int64 {
	if a >= b {
		diff := a - b
		if diff > math.MaxInt64 {
			return math.MaxInt64
		}
		return int64(diff)
	}
	diff := b - a
	if diff > math.MaxInt64 {
		return math.MinInt64
	}
	return -int64(diff)
}

// safeUint64ToInt64 converts uint64 to int64, clamping to math.MaxInt64 on overflow.
func safeUint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// ChainSelectorConfig holds configuration for the ChainSelector.
type ChainSelectorConfig struct {
	Logger             *slog.Logger
	EventBus           *event.EventBus
	EvaluationInterval time.Duration
	StaleTipThreshold  time.Duration
	SecurityParam      uint64
	MaxTrackedPeers    int // 0 means use DefaultMaxTrackedPeers
}

// ChainSelector tracks chain tips from multiple peers and selects the best
// chain according to Ouroboros Praos rules.
type ChainSelector struct {
	config          ChainSelectorConfig
	securityParam   uint64
	maxTrackedPeers int
	peerTips        map[ouroboros.ConnectionId]*PeerChainTip
	bestPeerConn    *ouroboros.ConnectionId
	localTip        ochainsync.Tip
	mutex           sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewChainSelector creates a new ChainSelector with the given configuration.
func NewChainSelector(cfg ChainSelectorConfig) *ChainSelector {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	cfg.Logger = cfg.Logger.With("component", "chainselection")
	if cfg.EvaluationInterval == 0 {
		cfg.EvaluationInterval = defaultEvaluationInterval
	}
	if cfg.StaleTipThreshold == 0 {
		cfg.StaleTipThreshold = defaultStaleTipThreshold
	}
	maxPeers := cfg.MaxTrackedPeers
	if maxPeers <= 0 {
		maxPeers = DefaultMaxTrackedPeers
	}
	return &ChainSelector{
		config:          cfg,
		securityParam:   cfg.SecurityParam,
		maxTrackedPeers: maxPeers,
		peerTips:        make(map[ouroboros.ConnectionId]*PeerChainTip),
	}
}

// Start begins the chain selector's background evaluation loop and subscribes
// to relevant events.
func (cs *ChainSelector) Start(ctx context.Context) error {
	cs.ctx, cs.cancel = context.WithCancel(ctx)
	go cs.evaluationLoop()
	return nil
}

// Stop stops the chain selector.
func (cs *ChainSelector) Stop() {
	if cs.cancel != nil {
		cs.cancel()
	}
}

// UpdatePeerTip updates the chain tip for a specific peer and triggers
// evaluation if needed. The vrfOutput parameter is the VRF output from the
// tip block header, used for tie-breaking when chains have equal block number
// and slot.
//
// Returns true if the tip was accepted, false if it was rejected as
// implausible. A tip is considered implausible if it claims a block number
// more than securityParam (k) blocks ahead of the local tip. This check is
// skipped during initial sync (when securityParam is 0 or localTip is at
// block 0).
func (cs *ChainSelector) UpdatePeerTip(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	vrfOutput []byte,
) bool {
	shouldEvaluate := false
	accepted := true
	var evictedConn *ouroboros.ConnectionId

	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()

		// Reject implausible tips that claim to be too far ahead of our
		// local chain. This prevents a malicious peer from spoofing an
		// extremely high block number to hijack chain selection.
		// Skip the check during initial sync when we have no local tip
		// or securityParam is not yet set.
		if cs.securityParam > 0 && cs.localTip.BlockNumber > 0 {
			maxPlausibleBlock := cs.localTip.BlockNumber +
				cs.securityParam
			if tip.BlockNumber > maxPlausibleBlock {
				cs.config.Logger.Warn(
					"rejecting implausible peer tip",
					"connection_id", connId.String(),
					"claimed_block", tip.BlockNumber,
					"local_block", cs.localTip.BlockNumber,
					"security_param", cs.securityParam,
					"max_plausible_block", maxPlausibleBlock,
				)
				accepted = false
				return
			}
		}

		if peerTip, exists := cs.peerTips[connId]; exists {
			peerTip.UpdateTip(tip, vrfOutput)
		} else {
			// Evict the least-recently-updated peer if at capacity
			if len(cs.peerTips) >= cs.maxTrackedPeers {
				evictedConn = cs.evictLeastRecentPeerLocked()
				if evictedConn == nil {
					cs.config.Logger.Warn(
						"cannot accept new peer: at capacity and best peer is the only tracked peer",
						"connection_id", connId.String(),
						"peer_count", len(cs.peerTips),
						"max_tracked_peers", cs.maxTrackedPeers,
					)
					accepted = false
					return
				}
			}
			cs.peerTips[connId] = NewPeerChainTip(connId, tip, vrfOutput)
		}

		cs.config.Logger.Debug(
			"updated peer tip",
			"connection_id", connId.String(),
			"block_number", tip.BlockNumber,
			"slot", tip.Point.Slot,
		)

		// Check if this peer's tip is better than the current best peer's tip
		if cs.bestPeerConn != nil {
			if bestPeerTip, ok := cs.peerTips[*cs.bestPeerConn]; ok {
				if IsBetterChain(tip, bestPeerTip.Tip) {
					shouldEvaluate = true
				}
			}
		} else {
			// No best peer yet, trigger evaluation
			shouldEvaluate = true
		}
	}()

	// Publish eviction event outside the lock to prevent deadlock
	if evictedConn != nil && cs.config.EventBus != nil {
		evt := event.NewEvent(
			PeerEvictedEventType,
			PeerEvictedEvent{ConnectionId: *evictedConn},
		)
		cs.config.EventBus.Publish(PeerEvictedEventType, evt)
	}

	if !accepted {
		return false
	}

	if shouldEvaluate {
		cs.EvaluateAndSwitch()
	}

	return true
}

// evictLeastRecentPeerLocked removes the peer with the oldest LastUpdated
// timestamp from the peerTips map. It never evicts the current best peer.
// When multiple peers share the same LastUpdated timestamp (common on
// Windows where clock resolution is ~15ms), the peer with the lowest
// block number is evicted first. If block numbers also tie, the
// connection ID string is used as a final deterministic tie-breaker.
// Returns a pointer to the evicted connection ID, or nil if no eviction
// was possible (e.g. the only tracked peer is the best peer).
// Must be called with cs.mutex held.
func (cs *ChainSelector) evictLeastRecentPeerLocked() *ouroboros.ConnectionId {
	var oldestConn ouroboros.ConnectionId
	var oldestTip *PeerChainTip
	found := false

	for connId, peerTip := range cs.peerTips {
		if peerTip == nil {
			continue
		}
		// Never evict the current best peer
		if cs.bestPeerConn != nil && *cs.bestPeerConn == connId {
			continue
		}
		if !found {
			oldestConn = connId
			oldestTip = peerTip
			found = true
			continue
		}
		// Primary: oldest LastUpdated wins eviction
		if peerTip.LastUpdated.Before(oldestTip.LastUpdated) {
			oldestConn = connId
			oldestTip = peerTip
		} else if peerTip.LastUpdated.Equal(oldestTip.LastUpdated) {
			// Tie-break on block number: evict the peer with
			// the lower block number (less useful chain)
			if peerTip.Tip.BlockNumber < oldestTip.Tip.BlockNumber {
				oldestConn = connId
				oldestTip = peerTip
			} else if peerTip.Tip.BlockNumber == oldestTip.Tip.BlockNumber {
				// Final tie-break: deterministic by connection ID
				if connId.String() < oldestConn.String() {
					oldestConn = connId
					oldestTip = peerTip
				}
			}
		}
	}

	if found {
		cs.config.Logger.Debug(
			"evicting least-recent peer due to tracking limit",
			"connection_id", oldestConn.String(),
			"last_updated", oldestTip.LastUpdated,
			"peer_count", len(cs.peerTips),
			"max_tracked_peers", cs.maxTrackedPeers,
		)
		delete(cs.peerTips, oldestConn)
		return &oldestConn
	}
	return nil
}

// RemovePeer removes a peer from tracking.
func (cs *ChainSelector) RemovePeer(connId ouroboros.ConnectionId) {
	var switchEvent *event.Event

	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()

		delete(cs.peerTips, connId)

		if cs.bestPeerConn != nil && *cs.bestPeerConn == connId {
			previousBest := *cs.bestPeerConn
			cs.bestPeerConn = nil
			cs.config.Logger.Info(
				"best peer disconnected, selecting new best",
				"connection_id", connId.String(),
			)
			// Immediately select a new best peer from remaining peers
			newBest := cs.selectBestChainLocked()
			cs.bestPeerConn = newBest

			if newBest != nil {
				cs.config.Logger.Info(
					"selected new best peer after disconnect",
					"connection_id", newBest.String(),
				)
				// Emit ChainSwitchEvent so subscribers know to switch connections
				if cs.config.EventBus != nil {
					newPeerTip := cs.peerTips[*newBest]
					evt := event.NewEvent(
						ChainSwitchEventType,
						ChainSwitchEvent{
							PreviousConnectionId: previousBest,
							NewConnectionId:      *newBest,
							NewTip:               newPeerTip.Tip,
							ComparisonResult:     ChainComparisonUnknown,
							BlockDifference: safeUint64ToInt64(
								newPeerTip.Tip.BlockNumber,
							),
						},
					)
					switchEvent = &evt
				}
			}
		}
	}()

	// Publish event outside the lock to prevent deadlock if subscribers
	// call back into ChainSelector
	if switchEvent != nil {
		cs.config.EventBus.Publish(ChainSwitchEventType, *switchEvent)
	}
}

// SetLocalTip updates the local chain tip for comparison.
func (cs *ChainSelector) SetLocalTip(tip ochainsync.Tip) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.localTip = tip
}

// SetSecurityParam updates the security parameter (k) dynamically.
// This allows the selector to use protocol parameters for density-based
// comparison.
func (cs *ChainSelector) SetSecurityParam(k uint64) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.securityParam = k
}

// GetBestPeer returns the connection ID of the peer with the best chain, or
// nil if no suitable peer is available.
func (cs *ChainSelector) GetBestPeer() *ouroboros.ConnectionId {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return cs.bestPeerConn
}

// GetPeerTip returns a deep copy of the chain tip for a specific peer.
// Returns nil if the peer is not tracked.
func (cs *ChainSelector) GetPeerTip(
	connId ouroboros.ConnectionId,
) *PeerChainTip {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	pt := cs.peerTips[connId]
	if pt == nil {
		return nil
	}
	tipCopy := *pt
	if pt.VRFOutput != nil {
		tipCopy.VRFOutput = make([]byte, len(pt.VRFOutput))
		copy(tipCopy.VRFOutput, pt.VRFOutput)
	}
	return &tipCopy
}

// GetAllPeerTips returns a deep copy of all tracked peer tips.
func (cs *ChainSelector) GetAllPeerTips() map[ouroboros.ConnectionId]*PeerChainTip {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	result := make(
		map[ouroboros.ConnectionId]*PeerChainTip,
		len(cs.peerTips),
	)
	for k, v := range cs.peerTips {
		tipCopy := *v
		if v.VRFOutput != nil {
			tipCopy.VRFOutput = make([]byte, len(v.VRFOutput))
			copy(tipCopy.VRFOutput, v.VRFOutput)
		}
		result[k] = &tipCopy
	}
	return result
}

// PeerCount returns the number of peers being tracked.
func (cs *ChainSelector) PeerCount() int {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return len(cs.peerTips)
}

// SelectBestChain evaluates all peer tips and returns the connection ID of
// the peer with the best chain.
func (cs *ChainSelector) SelectBestChain() *ouroboros.ConnectionId {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	return cs.selectBestChainLocked()
}

func (cs *ChainSelector) selectBestChainLocked() *ouroboros.ConnectionId {
	if len(cs.peerTips) == 0 {
		return nil
	}

	var bestConnId ouroboros.ConnectionId
	var bestPeerTip *PeerChainTip

	for connId, peerTip := range cs.peerTips {
		if peerTip.IsStale(cs.config.StaleTipThreshold) {
			cs.config.Logger.Debug(
				"skipping stale peer",
				"connection_id", connId.String(),
				"last_updated", peerTip.LastUpdated,
			)
			continue
		}

		if bestPeerTip == nil {
			bestConnId = connId
			bestPeerTip = peerTip
			continue
		}

		comparison := CompareChains(peerTip.Tip, bestPeerTip.Tip)
		switch comparison {
		case ChainABetter:
			bestConnId = connId
			bestPeerTip = peerTip
		case ChainEqual:
			// VRF tiebreaker: lower VRF output wins (per Ouroboros Praos)
			vrfComparison := CompareVRFOutputs(
				peerTip.VRFOutput,
				bestPeerTip.VRFOutput,
			)
			switch vrfComparison {
			case ChainABetter:
				// peerTip has lower VRF, it wins
				bestConnId = connId
				bestPeerTip = peerTip
			case ChainEqual:
				// VRF outputs are equal (or one/both nil), use connection ID
				// as final deterministic tiebreaker
				if connId.String() < bestConnId.String() {
					bestConnId = connId
					bestPeerTip = peerTip
				}
			case ChainBBetter, ChainComparisonUnknown:
				// bestPeerTip has lower VRF (or unknown); no change
			}
		case ChainBBetter, ChainComparisonUnknown:
			// Current best is better (or unknown); no change needed
		}
	}

	if bestPeerTip == nil {
		return nil
	}
	return &bestConnId
}

// EvaluateAndSwitch evaluates all peer tips and switches to the best chain if
// it differs from the current best. Returns true if a switch occurred.
func (cs *ChainSelector) EvaluateAndSwitch() bool {
	// Collect events to publish outside the lock to prevent deadlock
	var switchEvent *event.Event
	var selectionEvent *event.Event
	switchOccurred := false

	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()

		newBest := cs.selectBestChainLocked()
		if newBest == nil {
			// Clear stale reference to avoid returning a disconnected peer
			cs.bestPeerConn = nil
			return
		}

		previousBest := cs.bestPeerConn

		if previousBest == nil || *previousBest != *newBest {
			newPeerTip, ok := cs.peerTips[*newBest]
			if !ok {
				return
			}
			newTip := newPeerTip.Tip
			cs.bestPeerConn = newBest
			switchOccurred = true

			cs.config.Logger.Info(
				"selected new best peer",
				"connection_id", newBest.String(),
				"block_number", newTip.BlockNumber,
				"slot", newTip.Point.Slot,
			)

			if cs.config.EventBus != nil {
				var previousTip ochainsync.Tip
				var previousConnId ouroboros.ConnectionId
				if previousBest != nil {
					previousConnId = *previousBest
					if pt, ok := cs.peerTips[*previousBest]; ok {
						previousTip = pt.Tip
					}
				}
				// Compute comparison result and block difference
				comparisonResult := CompareChains(newTip, previousTip)
				blockDiff := safeBlockDiff(
					newTip.BlockNumber,
					previousTip.BlockNumber,
				)
				evt := event.NewEvent(
					ChainSwitchEventType,
					ChainSwitchEvent{
						PreviousConnectionId: previousConnId,
						NewConnectionId:      *newBest,
						NewTip:               newTip,
						PreviousTip:          previousTip,
						ComparisonResult:     comparisonResult,
						BlockDifference:      blockDiff,
					},
				)
				switchEvent = &evt
			}
		}

		if cs.config.EventBus != nil && cs.bestPeerConn != nil {
			bestPeerTip, ok := cs.peerTips[*cs.bestPeerConn]
			if !ok {
				return
			}
			bestTip := bestPeerTip.Tip
			evt := event.NewEvent(
				ChainSelectionEventType,
				ChainSelectionEvent{
					BestConnectionId: *cs.bestPeerConn,
					BestTip:          bestTip,
					PeerCount:        len(cs.peerTips),
					SwitchOccurred:   switchOccurred,
				},
			)
			selectionEvent = &evt
		}
	}()

	// Publish events outside the lock to prevent deadlock if subscribers
	// call back into ChainSelector
	if cs.config.EventBus != nil {
		if switchEvent != nil {
			cs.config.EventBus.Publish(ChainSwitchEventType, *switchEvent)
		}
		if selectionEvent != nil {
			cs.config.EventBus.Publish(ChainSelectionEventType, *selectionEvent)
		}
	}

	return switchOccurred
}

// HandlePeerTipUpdateEvent handles PeerTipUpdateEvent from the event bus.
func (cs *ChainSelector) HandlePeerTipUpdateEvent(evt event.Event) {
	e, ok := evt.Data.(PeerTipUpdateEvent)
	if !ok {
		cs.config.Logger.Warn(
			"received unexpected event data type",
			"expected", "PeerTipUpdateEvent",
		)
		return
	}
	cs.UpdatePeerTip(e.ConnectionId, e.Tip, e.VRFOutput)
}

func (cs *ChainSelector) evaluationLoop() {
	ticker := time.NewTicker(cs.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cs.ctx.Done():
			return
		case <-ticker.C:
			cs.runEvaluationTick()
		}
	}
}

// runEvaluationTick runs one evaluation tick with panic recovery.
// If a panic occurs, it's logged and the loop continues.
func (cs *ChainSelector) runEvaluationTick() {
	defer func() {
		if r := recover(); r != nil {
			cs.config.Logger.Error(
				"panic in evaluation tick, continuing",
				"panic", r,
			)
		}
	}()
	cs.cleanupStalePeers()
	cs.EvaluateAndSwitch()
}

func (cs *ChainSelector) cleanupStalePeers() {
	var switchEvent *event.Event

	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()

		var previousBest *ouroboros.ConnectionId

		for connId, peerTip := range cs.peerTips {
			// Use 2x the stale threshold for "very stale" cleanup. Peers are
			// skipped from selection after StaleTipThreshold, but we keep them
			// tracked for an additional period in case they reconnect or update.
			// After 2x the threshold, we consider them truly gone and remove them.
			if peerTip.IsStale(cs.config.StaleTipThreshold * 2) {
				cs.config.Logger.Debug(
					"removing very stale peer",
					"connection_id", connId.String(),
					"last_updated", peerTip.LastUpdated,
				)
				delete(cs.peerTips, connId)
				// Track if this was the best peer
				if cs.bestPeerConn != nil && *cs.bestPeerConn == connId {
					connIdCopy := connId
					previousBest = &connIdCopy
					cs.bestPeerConn = nil
				}
			}
		}

		// If the best peer was removed, select a new one and emit event
		if previousBest != nil {
			cs.config.Logger.Info(
				"best peer became stale, selecting new best",
				"connection_id", previousBest.String(),
			)
			newBest := cs.selectBestChainLocked()
			cs.bestPeerConn = newBest

			if newBest != nil {
				cs.config.Logger.Info(
					"selected new best peer after stale cleanup",
					"connection_id", newBest.String(),
				)
				// Emit ChainSwitchEvent so subscribers know to switch connections
				if cs.config.EventBus != nil {
					newPeerTip := cs.peerTips[*newBest]
					evt := event.NewEvent(
						ChainSwitchEventType,
						ChainSwitchEvent{
							PreviousConnectionId: *previousBest,
							NewConnectionId:      *newBest,
							NewTip:               newPeerTip.Tip,
							ComparisonResult:     ChainComparisonUnknown,
							BlockDifference: safeUint64ToInt64(
								newPeerTip.Tip.BlockNumber,
							),
						},
					)
					switchEvent = &evt
				}
			}
		}
	}()

	// Publish event outside the lock to prevent deadlock if subscribers
	// call back into ChainSelector
	if switchEvent != nil {
		cs.config.EventBus.Publish(ChainSwitchEventType, *switchEvent)
	}
}
