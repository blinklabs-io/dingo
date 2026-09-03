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

package dingo

import (
	"context"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/chainsyncrecycler"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

// chainsyncObservePeerTip synchronously feeds a peer tip update into chain
// selection (and peergov) when the Genesis corroboration gate is active, so the
// ChainsyncApplyEligible check that immediately follows in the roll-forward
// handler reflects the header currently being admitted. This closes the race
// where the apply gate would otherwise read corroboration state that predates
// this header (the tip update is normally delivered asynchronously). It returns
// true when it handled the observation synchronously, so the ouroboros layer
// skips the async PeerTipUpdateEvent publish to avoid a double update.
//
// When corroboration is inactive the async path is used unchanged (returns
// false), so normal high-throughput sync keeps its parallelism.
func (n *Node) chainsyncObservePeerTip(
	e chainselection.PeerTipUpdateEvent,
) bool {
	if n.chainSelector == nil ||
		!n.chainSelector.GenesisCorroborationActive() {
		return false
	}
	n.chainSelector.HandlePeerTipUpdateEvent(
		event.NewEvent(chainselection.PeerTipUpdateEventType, e),
	)
	if n.peerGov != nil {
		n.peerGov.TouchPeerByConnId(e.ConnectionId)
	}
	return true
}

// chainsyncObservePeerRollback synchronously applies a peer rollback into chain
// selection when the Genesis corroboration gate is active, so the
// ChainsyncApplyEligible check that immediately follows in the roll-backward
// handler reflects the post-rollback corroboration state. A rollback trims the
// peer's observed frontier (ApplyRollback), which can change its corroboration
// status; delivering that observation asynchronously would let the apply gate
// read pre-trim state and forward a rollback for a peer that the rollback has
// just made uncorroborated (issue #2928). It returns true when handled
// synchronously, so the ouroboros layer skips the async PeerRollbackEvent
// publish to avoid a double update. Unlike chainsyncObservePeerTip there is no
// peergov touch: only the chain selector subscribes to PeerRollbackEvent.
//
// When corroboration is inactive the async path is used unchanged (returns
// false).
func (n *Node) chainsyncObservePeerRollback(
	e chainselection.PeerRollbackEvent,
) bool {
	if n.chainSelector == nil ||
		!n.chainSelector.GenesisCorroborationActive() {
		return false
	}
	n.chainSelector.HandlePeerRollbackEvent(
		event.NewEvent(chainselection.PeerRollbackEventType, e),
	)
	return true
}

// chainsyncApplyEligible gates whether a peer's headers/rollbacks are APPLIED to
// the ledger, on top of ingress eligibility. It defers to the chain selector's
// corroboration decision so an uncorroborated Genesis fast source is observed
// (its tips still feed corroboration) but its blocks are withheld — the real
// enforcement of the corroboration stall, since ingress is otherwise independent
// of the selected best peer. Returns true (apply) when no chain selector is
// wired yet or outside Genesis corroboration.
func (n *Node) chainsyncApplyEligible(
	connId ouroboros.ConnectionId,
) bool {
	if n.chainSelector == nil {
		return true
	}
	return n.chainSelector.ShouldApplyIngress(connId)
}

func (n *Node) isChainsyncIngressEligible(
	connId ouroboros.ConnectionId,
) bool {
	if n.peerGov != nil {
		return n.peerGov.IsChainSelectionEligible(connId)
	}
	n.chainsyncIngressEligibilityMu.RLock()
	defer n.chainsyncIngressEligibilityMu.RUnlock()
	if n.chainsyncIngressEligibilityCache == nil {
		return false
	}
	eligible, ok := n.chainsyncIngressEligibilityCache[connId]
	if !ok {
		return false
	}
	return eligible
}

func (n *Node) setChainsyncIngressEligibility(
	connId ouroboros.ConnectionId,
	eligible bool,
) {
	n.chainsyncIngressEligibilityMu.Lock()
	defer n.chainsyncIngressEligibilityMu.Unlock()
	if n.chainsyncIngressEligibilityCache == nil {
		n.chainsyncIngressEligibilityCache = make(
			map[ouroboros.ConnectionId]bool,
		)
	}
	n.chainsyncIngressEligibilityCache[connId] = eligible
}

func (n *Node) deleteChainsyncIngressEligibility(
	connId ouroboros.ConnectionId,
) {
	n.chainsyncIngressEligibilityMu.Lock()
	defer n.chainsyncIngressEligibilityMu.Unlock()
	if n.chainsyncIngressEligibilityCache == nil {
		return
	}
	delete(n.chainsyncIngressEligibilityCache, connId)
}

func (n *Node) handlePeerEligibilityChangedEvent(evt event.Event) {
	e, ok := evt.Data.(peergov.PeerEligibilityChangedEvent)
	if !ok {
		return
	}
	n.setChainsyncIngressEligibility(e.ConnectionId, e.Eligible)
}

func (n *Node) handleChainSwitchEvent(evt event.Event) {
	e, ok := evt.Data.(chainselection.ChainSwitchEvent)
	if !ok {
		return
	}
	// chainSelector's evaluation loop is never paused during a live
	// database restore/truncate, which briefly nils n.chainsyncState while
	// swapping in a rebuilt one and holds n.liveLifecycleMu for its entire
	// quiesce-through-reinitialize duration -- so this event can still fire
	// mid-operation. TryLock, not Lock, matching nodeRecyclerComponents'
	// identical guard below: this handler runs on the EventBus's own
	// per-subscriber dispatch goroutine, so blocking it for a possibly
	// long-running truncate is worse than dropping one update, since
	// chainSelector re-evaluates and emits again once connections reattach
	// after reinit.
	if !n.liveLifecycleMu.TryLock() {
		return
	}
	defer n.liveLifecycleMu.Unlock()
	if n.chainsyncState == nil {
		return
	}
	prevConn := "(none)"
	if e.PreviousConnectionId.LocalAddr != nil &&
		e.PreviousConnectionId.RemoteAddr != nil {
		prevConn = e.PreviousConnectionId.String()
	}
	n.config.logger.Info(
		"chain switch: updating active connection",
		"previous_connection", prevConn,
		"new_connection", e.NewConnectionId.String(),
		"new_tip_block", e.NewTip.BlockNumber,
		"new_tip_slot", e.NewTip.Point.Slot,
	)
	// Peer switches only change which already-running chainsync stream feeds
	// the ledger. Restarting chainsync here re-enters FindIntersect and can
	// race the protocol state machine under load.
	n.chainsyncState.SetClientConnId(e.NewConnectionId)
}

// handleChainSelectedNoneEvent logs a selected-to-none transition. Chain
// selection has stalled with no eligible/corroborated peer; under Genesis
// corroboration the stalled source's blocks are already withheld from the ledger
// by the ChainsyncApplyEligible gate, so this is observability only.
func (n *Node) handleChainSelectedNoneEvent(evt event.Event) {
	e, ok := evt.Data.(chainselection.ChainSelectedNoneEvent)
	if !ok {
		return
	}
	prevConn := "(none)"
	if e.PreviousConnectionId.LocalAddr != nil &&
		e.PreviousConnectionId.RemoteAddr != nil {
		prevConn = e.PreviousConnectionId.String()
	}
	n.config.logger.Info(
		"chain selection stalled: no selectable peer",
		"previous_connection", prevConn,
		"genesis_corroboration", e.GenesisCorroboration,
	)
}

// nodeRecyclerComponents adapts the node's swappable storage/networking
// components to the recycler's ComponentProvider contract.
type nodeRecyclerComponents struct {
	node *Node
}

// recyclerComponents returns the provider the stall recycler reads live
// components through.
func (n *Node) recyclerComponents() chainsyncrecycler.ComponentProvider {
	return nodeRecyclerComponents{node: n}
}

// WithLiveComponents runs fn against the node's current ledger, chainsync
// state, and chain selector while holding liveLifecycleMu, so a live database
// restore/truncate cannot swap them mid-tick.
//
// A live restore/truncate briefly nils n.ledgerState and n.chainsyncState while
// it swaps in rebuilt ones, and holds n.liveLifecycleMu for its entire
// quiesce-through-reinitialize duration. TryLock, not Lock: the recycler's tick
// is a best-effort periodic check, so it must skip a contended tick rather than
// block waiting behind a possibly long-running truncate — and a blocking Lock()
// cannot be interrupted by context cancellation, so shutdown (which waits for
// the recycler) would hang behind it past its configured timeout. The lock is
// held for the whole callback, not just the nil check, because the tick
// dereferences both fields many more times after that check. The two fields are
// also not nilled/reassigned atomically together — reinitializeCoreStorage
// rebuilds n.ledgerState before reinitializeNetworkingCore rebuilds
// n.chainsyncState — so both are checked even under the lock.
func (c nodeRecyclerComponents) WithLiveComponents(
	fn func(chainsyncrecycler.LiveComponents),
) bool {
	n := c.node
	if !n.liveLifecycleMu.TryLock() {
		return false
	}
	defer n.liveLifecycleMu.Unlock()
	if n.ledgerState == nil || n.chainsyncState == nil {
		return false
	}
	live := chainsyncrecycler.LiveComponents{
		Ledger:         n.ledgerState,
		ChainsyncState: n.chainsyncState,
	}
	// Assign the interface only when the selector exists: a typed-nil
	// *ChainSelector in the interface would be non-nil to the recycler.
	if n.chainSelector != nil {
		live.ChainSelector = n.chainSelector
	}
	fn(live)
	return true
}

// startChainsyncStallRecycler builds and starts the chainsync stall recycler.
// It detects stalled chainsync clients and recycles truly stuck connections,
// using a grace period + cooldown to avoid flapping healthy but quiet peers.
func (n *Node) startChainsyncStallRecycler(
	ctx context.Context,
	chainsyncCfg chainsync.Config,
) error {
	n.chainsyncStallRecycler = chainsyncrecycler.New(
		chainsyncrecycler.Config{
			Components:   n.recyclerComponents(),
			EventBus:     n.eventBus,
			Logger:       n.config.logger,
			StallTimeout: chainsyncCfg.StallTimeout,
			Interval: min(
				max(chainsyncCfg.StallTimeout/2, 10*time.Second),
				30*time.Second,
			),
			Grace:    max(chainsyncCfg.StallTimeout, 30*time.Second),
			Cooldown: max(2*chainsyncCfg.StallTimeout, 2*time.Minute),
		},
	)
	return n.chainsyncStallRecycler.Start(ctx)
}

// waitChainsyncStallRecycler stops the recycler and waits for it to exit.
//
// This wait is intentionally not bounded by the shutdown timeout: advancing
// while the recycler is still active can race dependency teardown.
func (n *Node) waitChainsyncStallRecycler() {
	if n.chainsyncStallRecycler == nil {
		return
	}
	n.chainsyncStallRecycler.Stop()
}
