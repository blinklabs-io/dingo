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
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// catchUpBandHoldLocked reports whether connId's next delivered frontier is
// past what the peer-relative plausibility bound admits only because the local
// tip lags the headers the node is being delivered. It reads selector state
// and changes none. Must be called with cs.mutex held; localTip is the applied
// tip the caller read.
//
// entered is false for a connection that has not yet been held. It is held
// when the reference is inside (localTip, localTip+K], where neither stale nor
// catch-up relaxation applies, and the frontier is past reference+K. A
// connection that is already held (entered) stays held after the reference
// passes the local tip, until the frontier is within the bound that then
// applies, max(reference+K, localTip+2K); otherwise the first header released
// would meet the far-claim path that cannot corroborate a lone header.
//
// A frontier no chain could have reached from the local tip is never held, so
// it still meets the plausibility rejection at once.
func (cs *ChainSelector) catchUpBandHoldLocked(
	connId ouroboros.ConnectionId,
	observedTip ochainsync.Tip,
	localTip ochainsync.Tip,
	entered bool,
) bool {
	if cs.securityParam == 0 || localTip.BlockNumber == 0 {
		return false
	}
	// Admission stages against the pending far-tip claims when it has any.
	claims := cs.pendingFarTipClaims
	if claims == nil {
		claims = cs.farTipClaims
	}
	ref := cs.peerTipReferenceLocked(connId, claims, true)
	if !ref.has {
		return false
	}
	local := localTip.BlockNumber
	ceiling := safeAddUint64(ref.block, cs.securityParam)
	if claim, ok := claims[connId]; ok && claim.corroborated {
		// A corroborated far claim is the connection's own reference.
		ceiling = max(ceiling, safeAddUint64(claim.block, cs.securityParam))
	}
	inBand := ref.block > local &&
		ref.block <= safeAddUint64(local, cs.securityParam)
	if !inBand {
		if !entered {
			return false
		}
		// Past the band the catch-up ceiling applies as well.
		ceiling = max(ceiling, safeAddUint64(
			local, safeAddUint64(cs.securityParam, cs.securityParam),
		))
	}
	return observedTip.BlockNumber > ceiling &&
		cs.blockWithinSlotDistanceFromLocked(localTip, observedTip)
}

// catchUpBandHold evaluates the hold against the live applied tip when one is
// configured, as admission does, so the two never disagree about the local tip.
// The applied tip is read before the selector lock is taken, since the ledger
// guards it, and the lock is a read lock.
func (cs *ChainSelector) catchUpBandHold(
	connId ouroboros.ConnectionId,
	observedTip ochainsync.Tip,
	entered bool,
) bool {
	var applied ochainsync.Tip
	haveApplied := false
	if cs.config.AppliedTip != nil {
		applied, haveApplied = cs.config.AppliedTip()
	}
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	localTip := cs.localTip
	if haveApplied {
		localTip = applied
	}
	return cs.catchUpBandHoldLocked(connId, observedTip, localTip, entered)
}

// AwaitCatchUpBand blocks while connId's next delivered frontier is held by
// catchUpBandHoldLocked, and returns the context's error if it is cancelled
// first. It returns at once when the frontier is not held, so only a
// connection whose header overshoots the band because the ledger lags waits.
//
// The wait reads selector state and records nothing: no frontier, claim, or
// witness, so a waiting connection can neither be selected nor corroborate
// another. It takes the selector lock only for each check, never while parked.
// The reference, the applied tip, and the connection set are re-read on every
// check, so the wait ends when the ledger advances, the reference moves, or
// the reference peers go away. The caller bounds it with the context.
//
// The caller's chainsync callback blocks here, which stops that connection's
// header requests without discarding a header: the cursor stays put and the
// header is processed in full once it is admissible.
func (cs *ChainSelector) AwaitCatchUpBand(
	ctx context.Context,
	connId ouroboros.ConnectionId,
	observedTip ochainsync.Tip,
) error {
	if !cs.catchUpBandHold(connId, observedTip, false) {
		return nil
	}
	defer cs.pauseEagerness(connId)()
	ticker := time.NewTicker(eagernessPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if !cs.catchUpBandHold(connId, observedTip, true) {
				return nil
			}
		}
	}
}
