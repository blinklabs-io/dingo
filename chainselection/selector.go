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
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// safeAddUint64 returns a + b, clamped to math.MaxUint64 on overflow.
func safeAddUint64(a, b uint64) uint64 {
	if a > math.MaxUint64-b {
		return math.MaxUint64
	}
	return a + b
}

const (
	defaultEvaluationInterval = 10 * time.Second
	defaultStaleTipThreshold  = 60 * time.Second
	defaultMinSwitchBlockDiff = 2

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
	MinSwitchBlockDiff uint64
	SecurityParam      uint64
	GenesisMode        bool
	GenesisWindowSlots uint64
	ConnectionLive     func(ouroboros.ConnectionId) bool
	ConnectionEligible func(ouroboros.ConnectionId) bool
	ConnectionPriority func(ouroboros.ConnectionId) int
	MaxTrackedPeers    int // 0 means use DefaultMaxTrackedPeers
}

// ChainSelector tracks chain tips from multiple peers and selects the best
// chain according to Ouroboros Praos rules.
type ChainSelector struct {
	config            ChainSelectorConfig
	securityParam     uint64
	maxTrackedPeers   int
	mode              SelectionMode
	peerTips          map[ouroboros.ConnectionId]*PeerChainTip
	eligible          map[ouroboros.ConnectionId]bool
	priority          map[ouroboros.ConnectionId]int
	evaluationTrigger chan struct{}
	bestPeerConn      *ouroboros.ConnectionId
	localTip          ochainsync.Tip
	mutex             sync.RWMutex
	ctx               context.Context
	cancel            context.CancelFunc
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
	if cfg.MinSwitchBlockDiff == 0 {
		cfg.MinSwitchBlockDiff = defaultMinSwitchBlockDiff
	}
	maxPeers := cfg.MaxTrackedPeers
	if maxPeers <= 0 {
		maxPeers = DefaultMaxTrackedPeers
	}
	cs := &ChainSelector{
		config:            cfg,
		securityParam:     cfg.SecurityParam,
		maxTrackedPeers:   maxPeers,
		mode:              SelectionModePraos,
		peerTips:          make(map[ouroboros.ConnectionId]*PeerChainTip),
		eligible:          make(map[ouroboros.ConnectionId]bool),
		priority:          make(map[ouroboros.ConnectionId]int),
		evaluationTrigger: make(chan struct{}, 1),
	}
	if cfg.GenesisMode {
		cs.mode = SelectionModeGenesis
	}
	if cfg.EventBus != nil {
		cfg.EventBus.SubscribeFunc(
			peergov.PeerEligibilityChangedEventType,
			cs.handlePeerEligibilityChangedEvent,
		)
		cfg.EventBus.SubscribeFunc(
			peergov.PeerPriorityChangedEventType,
			cs.handlePeerPriorityChangedEvent,
		)
		cfg.EventBus.SubscribeFunc(
			PeerRollbackEventType,
			cs.handlePeerRollbackEvent,
		)
	}
	return cs
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

func (cs *ChainSelector) genesisWindowSlotsLocked() uint64 {
	if cs.config.GenesisWindowSlots > 0 {
		return cs.config.GenesisWindowSlots
	}
	if cs.securityParam > 0 {
		return safeAddUint64(
			cs.securityParam,
			safeAddUint64(cs.securityParam, cs.securityParam),
		)
	}
	return defaultGenesisWindowSlots
}

func (cs *ChainSelector) bestKnownGenesisSlotLocked() uint64 {
	var best uint64
	for connId, pt := range cs.peerTips {
		if !cs.isPeerSelectableLocked(connId, pt, false) {
			continue
		}
		tip := pt.SelectionTip()
		if tip.Point.Slot > best {
			best = tip.Point.Slot
		}
	}
	return best
}

func (cs *ChainSelector) shouldExitGenesisModeLocked() bool {
	if cs.mode != SelectionModeGenesis {
		return false
	}
	bestSlot := cs.bestKnownGenesisSlotLocked()
	if bestSlot == 0 {
		return false
	}
	return safeAddUint64(
		cs.localTip.Point.Slot,
		cs.genesisWindowSlotsLocked(),
	) >= bestSlot
}

func (cs *ChainSelector) advanceSelectionModeLocked() bool {
	if !cs.shouldExitGenesisModeLocked() {
		return false
	}
	cs.mode = SelectionModePraos
	cs.config.Logger.Info(
		"exiting Genesis selection mode",
		"local_slot", cs.localTip.Point.Slot,
		"best_known_slot", cs.bestKnownGenesisSlotLocked(),
		"genesis_window_slots", cs.genesisWindowSlotsLocked(),
	)
	return true
}

// UpdatePeerTip updates the chain tip for a specific peer and triggers
// evaluation if needed. The vrfOutput parameter is the VRF output from the
// tip block header, used for tie-breaking when chains have equal block number
// and slot.
//
// Returns true if the tip was accepted, false if it was rejected as
// implausible. A tip is considered implausible if it claims a block number
// more than securityParam (k) blocks ahead of a reference point. For known
// peers, the reference is the peer's own previous tip; for new peers, the
// reference is the best known peer tip. This avoids rejecting legitimate
// peers during sync (where the local tip is far behind).
func (cs *ChainSelector) UpdatePeerTip(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	vrfOutput []byte,
) bool {
	return cs.updatePeerTipObserved(connId, tip, tip, vrfOutput)
}

func (cs *ChainSelector) updatePeerTipObserved(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	observedTip ochainsync.Tip,
	vrfOutput []byte,
) bool {
	if cs.config.ConnectionLive != nil &&
		!cs.config.ConnectionLive(connId) {
		cs.config.Logger.Debug(
			"ignoring tip update from closed connection",
			"connection_id", connId.String(),
			"block_number", tip.BlockNumber,
			"slot", tip.Point.Slot,
		)
		return false
	}
	shouldEvaluate := false
	accepted := true
	var evictedConn *ouroboros.ConnectionId
	var modeChanged bool

	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()

		// Reject implausible tips that claim to be too far ahead of
		// a reference point. Three cases:
		//  1. Known peer: compare against the peer's own previous
		//     tip — chainsync advances incrementally so the delta
		//     is always small. Always checked (even if prev == 0).
		//  2. New peer with existing peers: compare against the
		//     best known peer tip to prevent a malicious newcomer
		//     from spoofing an extremely high block number.
		//  3. First peer ever: no reference exists, accept to
		//     allow bootstrap.
		if cs.securityParam > 0 {
			rejectTip := false
			var referenceBlock uint64
			if prevTip, exists := cs.peerTips[connId]; exists {
				// Case 1: known peer — compare against the peer's
				// own previous tip (chainsync advances incrementally).
				referenceBlock = prevTip.Tip.BlockNumber
				rejectTip = tip.BlockNumber >
					safeAddUint64(referenceBlock, cs.securityParam)
			} else if len(cs.peerTips) > 0 {
				// Case 2: new peer — check against best known
				for _, pt := range cs.peerTips {
					if pt.Tip.BlockNumber > referenceBlock {
						referenceBlock = pt.Tip.BlockNumber
					}
				}
				rejectTip = tip.BlockNumber >
					safeAddUint64(referenceBlock, cs.securityParam)
			}
			// Catch-up relaxation: after a stall, recorded peer tips
			// go stale while the network advances. A peer whose
			// delta from the stale reference exceeds K looks
			// implausible, but the network legitimately moved on.
			// Accept the tip if it is within 2*K of the local tip
			// AND the reference itself is stale (reference <=
			// local tip, meaning the node hasn't updated peer
			// records since the stall began).
			if rejectTip && cs.localTip.BlockNumber > 0 &&
				referenceBlock <= cs.localTip.BlockNumber {
				rejectTip = tip.BlockNumber >
					safeAddUint64(
						cs.localTip.BlockNumber,
						safeAddUint64(cs.securityParam, cs.securityParam),
					)
			}
			// Case 3: len(peerTips)==0 && peer not known → bootstrap
			if rejectTip {
				cs.config.Logger.Warn(
					"rejecting implausible peer tip",
					"connection_id", connId.String(),
					"claimed_block", tip.BlockNumber,
					"reference_block", referenceBlock,
					"security_param", cs.securityParam,
					"max_plausible_block",
					safeAddUint64(referenceBlock, cs.securityParam),
				)
				accepted = false
				return
			}
		}

		if peerTip, exists := cs.peerTips[connId]; exists {
			peerTip.UpdateTipWithObserved(
				tip,
				observedTip,
				vrfOutput,
			)
			peerTip.recordObservedSlot(
				observedTip.Point.Slot,
				cs.genesisWindowSlotsLocked(),
			)
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
			peerTip := NewPeerChainTip(connId, tip, vrfOutput)
			peerTip.ObservedTip = observedTip
			peerTip.recordObservedSlot(
				observedTip.Point.Slot,
				cs.genesisWindowSlotsLocked(),
			)
			cs.peerTips[connId] = peerTip
		}

		modeChanged = cs.advanceSelectionModeLocked()

		cs.config.Logger.Debug(
			"updated peer tip",
			"connection_id", connId.String(),
			"block_number", tip.BlockNumber,
			"slot", tip.Point.Slot,
		)

		// Check if this peer's tip is better than the current best peer's tip
		if modeChanged {
			shouldEvaluate = true
		} else if cs.bestPeerConn != nil {
			if bestPeerTip, ok := cs.peerTips[*cs.bestPeerConn]; ok {
				if cs.mode == SelectionModeGenesis {
					shouldEvaluate = cs.comparePeerTips(
						connId,
						cs.peerTips[connId],
						*cs.bestPeerConn,
						bestPeerTip,
					) == ChainABetter
				} else {
					comparison := CompareChains(
						cs.peerTips[connId].SelectionTip(),
						bestPeerTip.SelectionTip(),
					)
					if comparison == ChainABetter {
						shouldEvaluate = true
					} else if comparison == ChainEqual &&
						cs.comparePeerTips(
							connId,
							cs.peerTips[connId],
							*cs.bestPeerConn,
							bestPeerTip,
						) == ChainABetter {
						shouldEvaluate = true
					}
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

func (cs *ChainSelector) TouchPeerActivity(connId ouroboros.ConnectionId) {
	if cs.config.ConnectionLive != nil &&
		!cs.config.ConnectionLive(connId) {
		cs.config.Logger.Debug(
			"ignoring peer activity from closed connection",
			"connection_id", connId.String(),
		)
		return
	}
	var switchEvent *event.Event
	var selectionEvent *event.Event

	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()

		peerTip, exists := cs.peerTips[connId]
		if !exists {
			return
		}
		peerTip.Touch()
		_, switchEvent, selectionEvent = cs.evaluateBestPeerLocked()
	}()

	cs.publishSelectionEvents(switchEvent, selectionEvent)
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
	var oldestUpdated time.Time
	var oldestBlockNumber uint64
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
			oldestUpdated = peerTip.LastUpdated
			oldestBlockNumber = peerTip.Tip.BlockNumber
			found = true
			continue
		}
		// Primary: oldest LastUpdated wins eviction
		if peerTip.LastUpdated.Before(oldestUpdated) {
			oldestConn = connId
			oldestUpdated = peerTip.LastUpdated
			oldestBlockNumber = peerTip.Tip.BlockNumber
		} else if peerTip.LastUpdated.Equal(oldestUpdated) {
			// Tie-break on block number: evict the peer with
			// the lower block number (less useful chain)
			if peerTip.Tip.BlockNumber < oldestBlockNumber {
				oldestConn = connId
				oldestUpdated = peerTip.LastUpdated
				oldestBlockNumber = peerTip.Tip.BlockNumber
			} else if peerTip.Tip.BlockNumber == oldestBlockNumber {
				// Final tie-break: deterministic by connection ID
				if connId.String() < oldestConn.String() {
					oldestConn = connId
					oldestUpdated = peerTip.LastUpdated
					oldestBlockNumber = peerTip.Tip.BlockNumber
				}
			}
		}
	}

	if found {
		cs.config.Logger.Debug(
			"evicting least-recent peer due to tracking limit",
			"connection_id", oldestConn.String(),
			"last_updated", oldestUpdated,
			"peer_count", len(cs.peerTips),
			"max_tracked_peers", cs.maxTrackedPeers,
		)
		cs.deletePeerLocked(oldestConn)
		return &oldestConn
	}
	return nil
}

func (cs *ChainSelector) deletePeerLocked(connId ouroboros.ConnectionId) {
	delete(cs.peerTips, connId)
	delete(cs.eligible, connId)
	delete(cs.priority, connId)
}

// RemovePeer removes a peer from tracking.
func (cs *ChainSelector) RemovePeer(connId ouroboros.ConnectionId) {
	var switchEvent *event.Event

	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()

		cs.deletePeerLocked(connId)

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
	shouldEvaluate := false
	cs.mutex.Lock()
	cs.localTip = tip
	shouldEvaluate = cs.advanceSelectionModeLocked()
	cs.mutex.Unlock()
	if shouldEvaluate {
		cs.EvaluateAndSwitch()
	}
}

// SetSecurityParam updates the security parameter (k) dynamically.
// This allows the selector to use protocol parameters for density-based
// comparison.
func (cs *ChainSelector) SetSecurityParam(k uint64) {
	shouldEvaluate := false
	cs.mutex.Lock()
	cs.securityParam = k
	shouldEvaluate = cs.advanceSelectionModeLocked()
	cs.mutex.Unlock()
	if shouldEvaluate {
		cs.EvaluateAndSwitch()
	}
}

// SelectionMode returns the selector's current mode.
func (cs *ChainSelector) SelectionMode() SelectionMode {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return cs.mode
}

// GenesisWindowSlots returns the slot window used for Genesis density checks.
func (cs *ChainSelector) GenesisWindowSlots() uint64 {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return cs.genesisWindowSlotsLocked()
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
	if len(pt.observedSlots) > 0 {
		tipCopy.observedSlots = make([]uint64, len(pt.observedSlots))
		copy(tipCopy.observedSlots, pt.observedSlots)
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
		if len(v.observedSlots) > 0 {
			tipCopy.observedSlots = make([]uint64, len(v.observedSlots))
			copy(tipCopy.observedSlots, v.observedSlots)
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

func (cs *ChainSelector) isPeerSelectableLocked(
	connId ouroboros.ConnectionId,
	peerTip *PeerChainTip,
	logSkip bool,
) bool {
	if peerTip == nil {
		return false
	}
	if cs.config.ConnectionLive != nil &&
		!cs.config.ConnectionLive(connId) {
		if logSkip {
			cs.config.Logger.Debug(
				"skipping closed peer",
				"connection_id", connId.String(),
			)
		}
		return false
	}
	if !cs.isConnectionEligible(connId) {
		if logSkip {
			cs.config.Logger.Debug(
				"skipping ineligible peer",
				"connection_id", connId.String(),
			)
		}
		return false
	}
	if cs.securityParam > 0 && cs.localTip.BlockNumber > 0 &&
		safeAddUint64(peerTip.Tip.BlockNumber, cs.securityParam) <
			cs.localTip.BlockNumber {
		if logSkip {
			cs.config.Logger.Debug(
				"skipping implausibly-behind peer",
				"connection_id", connId.String(),
				"peer_block_number", peerTip.Tip.BlockNumber,
				"local_block_number", cs.localTip.BlockNumber,
				"security_param", cs.securityParam,
			)
		}
		return false
	}
	// Skip peers whose tip is far behind the best known peer tip.
	// During catch-up, switching to a behind peer causes pipeline
	// stalls and dropped rollbacks that cost minutes of sync time.
	// Use securityParam (K) as the threshold — peers within K blocks
	// of the best are acceptable (normal fork variance), but peers
	// further behind are not useful for syncing.
	if cs.securityParam > 0 {
		bestBlock := cs.bestKnownBlockNumber()
		if bestBlock > 0 &&
			safeAddUint64(peerTip.Tip.BlockNumber, cs.securityParam) < bestBlock {
			if logSkip {
				cs.config.Logger.Debug(
					"skipping peer behind best known tip",
					"connection_id", connId.String(),
					"peer_block_number", peerTip.Tip.BlockNumber,
					"best_known_block", bestBlock,
					"security_param", cs.securityParam,
				)
			}
			return false
		}
	}
	if cs.isPeerTipStale(peerTip) {
		if logSkip {
			cs.config.Logger.Debug(
				"skipping stale peer",
				"connection_id", connId.String(),
				"last_updated", peerTip.LastUpdated,
			)
		}
		return false
	}
	return true
}

func (cs *ChainSelector) selectBestChainLocked() *ouroboros.ConnectionId {
	cs.advanceSelectionModeLocked()
	if len(cs.peerTips) == 0 {
		return nil
	}

	var bestConnId ouroboros.ConnectionId
	var bestPeerTip *PeerChainTip

	for connId, peerTip := range cs.peerTips {
		if !cs.isPeerSelectableLocked(connId, peerTip, true) {
			continue
		}

		if bestPeerTip == nil {
			bestConnId = connId
			bestPeerTip = peerTip
			continue
		}

		comparison := cs.comparePeerTips(
			connId,
			peerTip,
			bestConnId,
			bestPeerTip,
		)
		switch comparison {
		case ChainABetter:
			bestConnId = connId
			bestPeerTip = peerTip
		case ChainBBetter, ChainComparisonUnknown, ChainEqual:
			// Current best is better (or unknown); no change needed
		}
	}

	if bestPeerTip == nil {
		return nil
	}
	return &bestConnId
}

// bestKnownBlockNumber returns the highest block number reported by any
// eligible, non-stale peer. Used to skip peers that are far behind the
// network tip during catch-up. Only considers peers that pass eligibility
// and staleness checks to avoid letting an ineligible outlier suppress
// valid peer selection.
func (cs *ChainSelector) bestKnownBlockNumber() uint64 {
	var best uint64
	for connId, pt := range cs.peerTips {
		if !cs.isConnectionEligible(connId) || cs.isPeerTipStale(pt) {
			continue
		}
		if pt.Tip.BlockNumber > best {
			best = pt.Tip.BlockNumber
		}
	}
	return best
}

func (cs *ChainSelector) isConnectionEligible(
	connId ouroboros.ConnectionId,
) bool {
	eligible, ok := cs.eligible[connId]
	if !ok {
		return true
	}
	return eligible
}

func (cs *ChainSelector) connectionPriority(
	connId ouroboros.ConnectionId,
) int {
	return cs.priority[connId]
}

func (cs *ChainSelector) isPeerTipStale(peerTip *PeerChainTip) bool {
	return peerTip != nil &&
		peerTip.IsStale(cs.config.StaleTipThreshold)
}

func (cs *ChainSelector) comparePeerTips(
	connIdA ouroboros.ConnectionId,
	peerTipA *PeerChainTip,
	connIdB ouroboros.ConnectionId,
	peerTipB *PeerChainTip,
) ChainComparisonResult {
	if peerTipA == nil || peerTipB == nil {
		return ChainComparisonUnknown
	}
	if cs.mode == SelectionModeGenesis {
		genesisWindow := cs.genesisWindowSlotsLocked()
		densityA := peerTipA.observedDensity(genesisWindow)
		densityB := peerTipB.observedDensity(genesisWindow)
		if densityA > densityB {
			return ChainABetter
		}
		if densityB > densityA {
			return ChainBBetter
		}
	}
	return cs.comparePeerTipsPraos(
		connIdA,
		peerTipA,
		connIdB,
		peerTipB,
	)
}

func (cs *ChainSelector) comparePeerTipsPraos(
	connIdA ouroboros.ConnectionId,
	peerTipA *PeerChainTip,
	connIdB ouroboros.ConnectionId,
	peerTipB *PeerChainTip,
) ChainComparisonResult {
	comparison := CompareChains(
		peerTipA.SelectionTip(),
		peerTipB.SelectionTip(),
	)
	switch comparison {
	case ChainABetter, ChainBBetter, ChainComparisonUnknown:
		return comparison
	case ChainEqual:
		priorityA := cs.connectionPriority(connIdA)
		priorityB := cs.connectionPriority(connIdB)
		if priorityA > priorityB {
			return ChainABetter
		}
		if priorityB > priorityA {
			return ChainBBetter
		}
		vrfComparison := CompareVRFOutputs(
			peerTipA.VRFOutput,
			peerTipB.VRFOutput,
		)
		switch vrfComparison {
		case ChainABetter, ChainBBetter:
			return vrfComparison
		case ChainEqual, ChainComparisonUnknown:
		}
		if connIdA.String() < connIdB.String() {
			return ChainABetter
		}
		if connIdB.String() < connIdA.String() {
			return ChainBBetter
		}
		return ChainEqual
	default:
		return ChainComparisonUnknown
	}
}

func (cs *ChainSelector) handlePeerEligibilityChangedEvent(evt event.Event) {
	e, ok := evt.Data.(peergov.PeerEligibilityChangedEvent)
	if !ok {
		return
	}
	cs.mutex.Lock()
	cs.eligible[e.ConnectionId] = e.Eligible
	cs.mutex.Unlock()
	cs.triggerEvaluation()
}

func (cs *ChainSelector) handlePeerPriorityChangedEvent(evt event.Event) {
	e, ok := evt.Data.(peergov.PeerPriorityChangedEvent)
	if !ok {
		return
	}
	cs.mutex.Lock()
	cs.priority[e.ConnectionId] = e.Priority
	cs.mutex.Unlock()
	cs.triggerEvaluation()
}

func (cs *ChainSelector) triggerEvaluation() {
	select {
	case cs.evaluationTrigger <- struct{}{}:
	default:
	}
}

func (cs *ChainSelector) publishSelectionEvents(
	switchEvent *event.Event,
	selectionEvent *event.Event,
) {
	if cs.config.EventBus == nil {
		return
	}
	if switchEvent != nil {
		cs.config.EventBus.Publish(ChainSwitchEventType, *switchEvent)
	}
	if selectionEvent != nil {
		cs.config.EventBus.Publish(ChainSelectionEventType, *selectionEvent)
	}
}

func (cs *ChainSelector) evaluateBestPeerLocked() (
	bool,
	*event.Event,
	*event.Event,
) {
	var switchEvent *event.Event
	var selectionEvent *event.Event
	switchOccurred := false

	newBest := cs.selectBestChainLocked()
	if newBest == nil {
		// Clear stale reference to avoid returning a disconnected peer
		cs.bestPeerConn = nil
		return false, nil, nil
	}

	previousBest := cs.bestPeerConn
	if previousBest != nil && *previousBest != *newBest {
		previousPeerTip, ok := cs.peerTips[*previousBest]
		if ok && cs.isPeerSelectableLocked(*previousBest, previousPeerTip, false) {
			newPeerTip, ok := cs.peerTips[*newBest]
			if !ok {
				return false, nil, nil
			}
			if CompareChains(
				newPeerTip.SelectionTip(),
				previousPeerTip.SelectionTip(),
			) == ChainEqual {
				// When two peers have delivered the same observed frontier,
				// keep following the incumbent. The first peer to deliver the
				// fitting header already won on latency; switching on source
				// priority or VRF alone just creates churn near tip.
				newBest = previousBest
			} else if
			// Preserve the incumbent only when it still wins the same
			// full comparison used during normal best-peer selection.
			cs.comparePeerTips(
				*previousBest,
				previousPeerTip,
				*newBest,
				newPeerTip,
			) == ChainABetter {
				newBest = previousBest
			} else if newPeerTip.SelectionTip().BlockNumber >
				previousPeerTip.SelectionTip().BlockNumber &&
				!IsSignificantlyBetter(
					newPeerTip.SelectionTip(),
					previousPeerTip.SelectionTip(),
					cs.config.MinSwitchBlockDiff,
				) {
				// Suppress switches where the challenger is only marginally
				// ahead in SelectionTip (observed frontier, which includes
				// in-flight headers from RollForward). The original guard used
				// Tip.BlockNumber (the peer's claimed remote tip), which can be
				// ahead of ObservedTip when a peer advertises N+1 in a
				// RollForward tip claim before actually delivering the N+1
				// header. That caused both peers to show Tip.BlockNumber=N+1
				// while only one had SelectionTip=N+1, making the `>` guard
				// fail and allowing a switch for a 1-block observed lead.
				newBest = previousBest
			}
		}
	}

	if previousBest == nil || *previousBest != *newBest {
		newPeerTip, ok := cs.peerTips[*newBest]
		if !ok {
			return false, nil, nil
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
			return false, nil, nil
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

	return switchOccurred, switchEvent, selectionEvent
}

// EvaluateAndSwitch evaluates all peer tips and switches to the best chain if
// it differs from the current best. Returns true if a switch occurred.
func (cs *ChainSelector) EvaluateAndSwitch() bool {
	var switchEvent *event.Event
	var selectionEvent *event.Event
	switchOccurred := false

	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()
		switchOccurred, switchEvent, selectionEvent = cs.evaluateBestPeerLocked()
	}()

	cs.publishSelectionEvents(switchEvent, selectionEvent)
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
	cs.updatePeerTipObserved(
		e.ConnectionId,
		e.Tip,
		e.ObservedTip,
		e.VRFOutput,
	)
}

// HandlePeerActivityEvent refreshes a peer's liveness on non-tip protocol
// activity such as keepalive responses.
func (cs *ChainSelector) HandlePeerActivityEvent(evt event.Event) {
	e, ok := evt.Data.(PeerActivityEvent)
	if !ok {
		cs.config.Logger.Warn(
			"received unexpected event data type",
			"expected", "PeerActivityEvent",
		)
		return
	}
	cs.TouchPeerActivity(e.ConnectionId)
}

// handlePeerRollbackEvent trims Genesis observed history and refreshes the
// tracked peer tip after a rollback.
func (cs *ChainSelector) handlePeerRollbackEvent(evt event.Event) {
	e, ok := evt.Data.(PeerRollbackEvent)
	if !ok {
		cs.config.Logger.Warn(
			"received unexpected event data type",
			"expected", "PeerRollbackEvent",
		)
		return
	}

	var shouldEvaluate bool
	cs.mutex.Lock()
	if peerTip, exists := cs.peerTips[e.ConnectionId]; exists {
		peerTip.ApplyRollback(e.Point, e.Tip)
		shouldEvaluate = true
	}
	cs.mutex.Unlock()

	if shouldEvaluate {
		cs.EvaluateAndSwitch()
	}
}

func (cs *ChainSelector) evaluationLoop() {
	ticker := time.NewTicker(cs.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cs.ctx.Done():
			return
		case <-cs.evaluationTrigger:
			cs.runTriggeredEvaluation()
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

func (cs *ChainSelector) runTriggeredEvaluation() {
	defer func() {
		if r := recover(); r != nil {
			cs.config.Logger.Error(
				"panic in triggered evaluation, continuing",
				"panic", r,
			)
		}
	}()
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
				cs.deletePeerLocked(connId)
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
