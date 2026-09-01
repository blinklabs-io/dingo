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
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func (n *Node) chainsyncSyncTarget(
	update chainselection.PeerTipUpdateEvent,
) (ochainsync.Tip, bool) {
	if n.chainSelector == nil {
		return update.ObservedTip, update.ObservedTip.Point.Slot != 0 ||
			update.ObservedTip.BlockNumber != 0
	}
	return n.chainSelector.SyncTargetForPeerTipUpdate(update)
}

// ledgerStateConfig builds the ledger.LedgerStateConfig for this node.
//
// This is the single construction site for that config. Run() uses it at
// startup and reinitializeCoreStorage uses it when a live Restore or
// Truncate rebuilds the ledger, so a field added here reaches both paths.
// The two were previously written out separately, and every divergence
// silently disabled operator-configured behavior until the process
// restarted: the CIP-23/CIP-50/CIP-0163 reward flags, then the block
// pipeline flags, then GenesisSelectionStateFunc (issue #3273), which left
// a restored or truncated node resolving deep forks by Praos length alone
// with Ouroboros Genesis density selection switched off. Building the
// config once makes that class of drift structurally impossible rather
// than something each new field has to remember.
//
// Callers must assign n.chainManager and n.db before calling: those two
// fields are read here, not resolved lazily.
//
// Every callback is a closure over n rather than a method value, because
// n.ouroboros, n.chainsyncState, n.connManager, n.ledgerState and
// n.chainSelector are all replaced by a live rebuild -- a method value
// would pin the rebuilt ledger to the outgoing instance.
func (n *Node) ledgerStateConfig() ledger.LedgerStateConfig {
	return ledger.LedgerStateConfig{
		ChainManager:       n.chainManager,
		Database:           n.db,
		EventBus:           n.eventBus,
		Logger:             n.config.logger,
		CardanoNodeConfig:  n.config.cardanoNodeConfig,
		Network:            n.config.network,
		PromRegistry:       n.config.promRegistry,
		ForgeBlocks:        n.config.isDevMode(),
		ValidateHistorical: n.config.validateHistorical,
		EnableDijkstra:     n.config.experimentalDijkstraEnabled(),
		StartInDijkstra:    n.config.startEra.IsDijkstra(),
		// Parallel block-decode pipeline for the chainsync replay loop
		// (issue #1894 phase 1). Not consensus-affecting; off by default.
		BlockPipelineEnabled: n.config.blockPipelineEnabled,
		// Parallel VRF/KES validate stage for the same pipeline (issue
		// #1894 phase 3). Off by default; requires BlockPipelineEnabled.
		BlockPipelineValidateEnabled: n.config.blockPipelineValidateEnabled,
		// Supplies fetched Leios endorser-block transactions so the ledger
		// can apply them when their referencing Dijkstra ranking block is
		// processed (completing the UTxO set for endorser-resident outputs).
		// Closure, not a method value: at startup n.ouroboros does not
		// exist yet, and a live rebuild replaces it, so this resolves it
		// when the callback fires. Same for EndorserBlockFetcher and
		// BlockfetchRequestRangeFunc below.
		EndorserBlockProvider: func(
			ebHash []byte,
		) (uint64, []cbor.RawMessage, bool) {
			return n.ouroboros().EndorserBlockTxsByHash(ebHash)
		},
		// Actively fetches a referenced endorser block by point and caches
		// it. Used during historical catch-up: the prototype relay serves
		// any endorser block by point on demand, so a from-scratch sync can
		// backfill older ranking blocks' endorser-resident outputs and build
		// a complete UTxO set instead of trusting the chain.
		EndorserBlockFetcher: func(ebSlot uint64, ebHash []byte) error {
			return n.ouroboros().FetchEndorserBlockByPoint(ebSlot, ebHash)
		},
		// Wait, at the tip, for a ranking block's referenced endorser block
		// to arrive before applying it. Sourced from the pipeline timing
		// (CIP-0164-tracking, override-able via WithLeiosPipelineTiming)
		// rather than a hardcoded duration. We use CertifyByDeadlineSlots,
		// not DiffuseWindowSlots: by the time a ranking block references an
		// EB, that EB has already been certified, and measurement shows the
		// prototype relay's tx-offer delay (~3s) plus the fetch (~3s, incl.
		// the still-diffusing tail) exceeds the diffusion window — so the
		// certify deadline is the bound that matches when the EB is actually
		// available to fetch.
		EndorserBlockWaitSlots: n.leiosPipelineTiming().CertifyByDeadlineSlots,
		// Two-path Leios ledger selection: the Musashi prototype
		// (prototype-2026w29) applies only the certified parent EB, without
		// validation or consumed-input recovery (Haskell-conformant), whereas
		// dingo's forward path applies the current announcement normally
		// (CIP-conformant).
		LeiosApplyEndorserBlockTxs: !n.config.isMusashiNetwork(),
		// dingo's leadership stake omits reward-account balances (staking
		// rewards are not yet computed), which spuriously rejects the
		// dominant pool's eligible blocks on Musashi's concentrated
		// topology and wedges the chain. Trust rather than reject there
		// until reward calculation lands; enforce on real networks where
		// the omission is negligible. TPraos bootstrap pool-threshold
		// checks are waived separately inside header validation after
		// genesis overlay slots are handled.
		SkipLeaderStakeThresholdCheck: n.config.prototypeTrustBypassesEnabled(),
		// On Musashi, certified endorser txs and Dijkstra ranking-block txs are
		// trusted by the prototype; skip dingo's per-tx validation to match it
		// and keep block application at the production rate.
		SkipDijkstraTxValidation: n.config.prototypeTrustBypassesEnabled(),
		// CIP-23 minimum pool margin (minimum variable fee). Operator-set,
		// off by default (0), effective only in Dijkstra and later.
		MinPoolMargin: n.config.minPoolMargin,
		// CIP-50 pledge-leverage reward cap. Operator-set (not derived from
		// the network) and off by default; enable only where every node
		// also enables it.
		PledgeLeverageEnabled: n.config.pledgeLeverageEnabled,
		PledgeLeverage:        n.config.pledgeLeverage,
		// CIP-0163 full-pot reward distribution. Operator-set (not derived
		// from the network) and off by default; enable only where every node
		// also enables it.
		FullPotRewardsEnabled: n.config.fullPotRewardsEnabled,
		// CIP-0163 reward-account inactivity expiry (operator-set)
		DelegatorInactivityEnabled: n.config.delegatorInactivityEnabled,
		DelegatorInactivity:        n.config.delegatorInactivity,
		BlockfetchRequestRangeFunc: func(
			connId ouroboros.ConnectionId,
			start ocommon.Point,
			end ocommon.Point,
		) error {
			return n.ouroboros().
				BlockfetchClientRequestRange(connId, start, end)
		},
		PeersWithBlockFunc: func(
			origin ouroboros.ConnectionId,
			point ocommon.Point,
		) []ouroboros.ConnectionId {
			var peers []ouroboros.ConnectionId
			n.withLiveChainsyncState(func(state *chainsync.State) {
				peers = state.PeersWithBlock(origin, point)
			})
			return peers
		},
		RecordBlockfetchLatencyFunc: func(
			connId ouroboros.ConnectionId,
			latency time.Duration,
		) {
			n.withLiveChainsyncState(func(state *chainsync.State) {
				state.RecordBlockfetchLatency(
					connId,
					latency,
				)
			})
		},
		BlockfetchLatencyFunc: func(
			connId ouroboros.ConnectionId,
		) (time.Duration, bool) {
			var (
				latency time.Duration
				ok      bool
			)
			n.withLiveChainsyncState(func(state *chainsync.State) {
				latency, ok = state.BlockfetchLatency(connId)
			})
			return latency, ok
		},
		BlockfetchLatencyMedianFunc: func() (time.Duration, int) {
			var (
				latency time.Duration
				count   int
			)
			n.withLiveChainsyncState(func(state *chainsync.State) {
				latency, count = state.BlockfetchLatencyMedian()
			})
			return latency, count
		},
		DatabaseWorkerPoolConfig: n.config.DatabaseWorkerPoolConfig,
		GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
			// Return the current best peer for rollback filtering and
			// blockfetch fallback. Headers can arrive from any eligible
			// peer, but rollbacks and retry selection still need a
			// current best connection.
			var active *ouroboros.ConnectionId
			n.withLiveChainsyncState(func(state *chainsync.State) {
				active = state.GetClientConnId()
			})
			return active
		},
		GetPeerObservedTipFunc: func(
			connId ouroboros.ConnectionId,
		) (ochainsync.Tip, bool) {
			if n.chainSelector == nil {
				return ochainsync.Tip{}, false
			}
			peerTip := n.chainSelector.GetPeerTip(connId)
			if peerTip == nil {
				return ochainsync.Tip{}, false
			}
			return peerTip.SelectionTip(), true
		},
		GetPeerSyncTargetFunc: func(
			connId ouroboros.ConnectionId,
		) (ochainsync.Tip, bool) {
			if n.chainSelector == nil {
				return ochainsync.Tip{}, false
			}
			return n.chainSelector.GetPeerSyncTarget(connId)
		},
		ConnectionLiveFunc: func(connId ouroboros.ConnectionId) bool {
			return n.connManager != nil &&
				n.connManager.GetConnectionById(connId) != nil
		},
		ConnectionSwitchFunc: func() {
			// Retain older seen-header history so a switched peer
			// can replay only the post-tip segment from the local
			// intersect point without re-delivering older headers.
			n.withLiveChainsyncState(func(state *chainsync.State) {
				if n.ledgerState == nil {
					return
				}
				state.ClearSeenHeadersFrom(
					n.ledgerState.Tip().Point.Slot,
				)
			})
		},
		ClearSeenHeadersFromFunc: func(fromSlot uint64) {
			n.withLiveChainsyncState(func(state *chainsync.State) {
				state.ClearSeenHeadersFrom(fromSlot)
			})
		},
		PeerHeaderLookupFunc: func(
			connId ouroboros.ConnectionId,
			hash []byte,
		) (ledger.ChainsyncEvent, []byte, bool) {
			var (
				h        chainsync.ObservedHeader
				prevHash []byte
				ok       bool
			)
			n.withLiveChainsyncState(func(state *chainsync.State) {
				h, prevHash, ok = state.LookupObservedHeader(connId, hash)
			})
			if !ok {
				return ledger.ChainsyncEvent{}, nil, false
			}
			return ledger.ChainsyncEvent{
				ConnectionId: h.ConnectionId,
				BlockHeader:  h.BlockHeader,
				ArrivalTime:  h.ArrivalTime,
				Point:        h.Point,
				Tip:          h.Tip,
				BlockNumber:  h.BlockNumber,
				Type:         h.Type,
				Rollback:     h.Rollback,
			}, prevHash, true
		},
		GenesisSelectionStateFunc: func() (bool, uint64) {
			if n.chainSelector == nil {
				return false, 0
			}
			return n.chainSelector.GenesisSelectionState()
		},
		FatalErrorFunc: func(err error) {
			n.config.logger.Error(
				"fatal ledger error, initiating shutdown",
				"error", err,
			)
			n.cancel()
		},
	}
}
