// Copyright 2025 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ledger

import (
	"encoding/hex"
)

// chainsyncPeerBehindOnOurChain reports whether a chainsync peer that asked us
// to roll back is merely behind on our own chain rather than offering a
// competing one, and by how many blocks.
//
// Chain.IntersectPoints samples our history at doubling offsets past a dense
// band of 32 points, so a peer L blocks behind resolves its FindIntersect to
// the next rung at or past L, not to L itself. The rollback it then asks for is
// as deep as that rung, which can exceed the security parameter K while the
// peer is still inside K — with a 32-point dense band the rejection starts at a
// lag of only K/2-ish blocks. Fork depth there is the ladder's granularity, not
// evidence of divergence.
//
// The peer's own advertised tip settles it: if that tip is a strict ancestor of
// our tip on our primary chain, the peer holds a prefix of our chain. There is
// nothing to roll back to and nothing to follow — we are ahead — so the peer
// must be kept and simply left unselected until it catches up. The Mithril
// branch of the same handler already discriminates this way.
//
// Every condition must hold, and anything unknown fails safe to "not behind"
// so the security-parameter rejection keeps its full force:
//
//   - the peer advertised a tip at all (a zero tip is unknown, not behind);
//   - that tip is strictly behind our tip (a peer at or past our tip is
//     offering a competing chain, not a prefix of ours);
//   - the rollback point is at or below the peer's own tip (a peer cannot
//     intersect above its own tip);
//   - both the peer's tip and the rollback point are on our primary chain.
//
// Callers must hold chainsyncMutex.
func (ls *LedgerState) chainsyncPeerBehindOnOurChain(
	e ChainsyncEvent,
) (uint64, bool) {
	if ls.chain == nil {
		return 0, false
	}
	peerTip := e.Tip.Point
	if peerTip.Slot == 0 && len(peerTip.Hash) == 0 {
		return 0, false
	}
	if e.Point.Slot == 0 && len(e.Point.Hash) == 0 {
		return 0, false
	}
	localTip := ls.chain.HeaderTip()
	if peerTip.Slot >= localTip.Point.Slot {
		return 0, false
	}
	if e.Point.Slot > peerTip.Slot {
		return 0, false
	}
	onChain, err := ls.primaryChainContainsPoint(peerTip)
	if err != nil {
		ls.logChainsyncBehindLookupError(e, "peer tip", err)
		return 0, false
	}
	if !onChain {
		return 0, false
	}
	onChain, err = ls.primaryChainContainsPoint(e.Point)
	if err != nil {
		ls.logChainsyncBehindLookupError(e, "rollback point", err)
		return 0, false
	}
	if !onChain {
		return 0, false
	}
	var depth uint64
	if localTip.BlockNumber > 0 && e.Tip.BlockNumber > 0 &&
		localTip.BlockNumber >= e.Tip.BlockNumber {
		depth = localTip.BlockNumber - e.Tip.BlockNumber
	}
	return depth, true
}

// logChainsyncBehindLookupError records a failed primary-chain membership
// lookup. The caller falls back to treating the peer as divergent, so this is
// only diagnostic.
func (ls *LedgerState) logChainsyncBehindLookupError(
	e ChainsyncEvent,
	subject string,
	err error,
) {
	if ls.config.Logger == nil {
		return
	}
	ls.config.Logger.Debug(
		"failed to check whether a chainsync peer is behind on our chain",
		"component", "ledger",
		"subject", subject,
		"rollback_slot", e.Point.Slot,
		"peer_tip_slot", e.Tip.Point.Slot,
		"connection_id", e.ConnectionId.String(),
		"error", err,
	)
}

// noteChainsyncPeerBehind records a peer left attached because it is behind on
// our own chain. It is deliberately Info, not Warn: nothing is wrong locally,
// and the condition clears itself when the peer catches up.
func (ls *LedgerState) noteChainsyncPeerBehind(
	e ChainsyncEvent,
	depth uint64,
	path string,
) {
	if ls.metrics.chainsyncBehindPeers != nil {
		ls.metrics.chainsyncBehindPeers.Inc()
	}
	if ls.config.Logger == nil {
		return
	}
	localTip := ls.chain.HeaderTip()
	ls.config.Logger.Info(
		"chainsync peer is behind on our chain, waiting for it to catch up",
		"component", "ledger",
		"behind_blocks", depth,
		"rollback_slot", e.Point.Slot,
		"rollback_hash", hex.EncodeToString(e.Point.Hash),
		"peer_tip_slot", e.Tip.Point.Slot,
		"peer_tip_block", e.Tip.BlockNumber,
		"local_tip_slot", localTip.Point.Slot,
		"local_tip_block", localTip.BlockNumber,
		"path", path,
		"connection_id", e.ConnectionId.String(),
	)
}
