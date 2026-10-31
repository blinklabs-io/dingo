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

package ledger

import (
	"context"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const maxHeaderAdmissionSettleAttempts = 4

// pinSettledHeaderAdmission bridges queue admission and its deferred selector
// callback. Callers hold no ledger or chainsync lock while a transition settles.
// An expired verdict can be replaced only by proof that the exact header has
// already been applied on the current chain; a queued or retained fork header
// does not establish that fact. Membership is read before taking the new pin;
// the pin then rejects a publication or transition that invalidated that read.
func (ls *LedgerState) pinSettledHeaderAdmission(
	ctx context.Context,
	e *ChainsyncEvent,
	generation uint64,
) (func(), bool) {
	if generation == 0 || e == nil {
		return nil, false
	}
	if release, current := ls.PinHeaderCryptoVerificationGeneration(generation); current {
		return release, true
	}
	waitCtx, cancel := context.WithTimeout(ctx, blockfetchBusyTimeout)
	defer cancel()
	for range maxHeaderAdmissionSettleAttempts {
		settled := ls.txValidationSettledChan()
		if release, current := ls.PinHeaderCryptoVerificationGeneration(generation); current {
			return release, true
		}
		currentGeneration := ls.CurrentHeaderCryptoVerificationGeneration()
		if currentGeneration != 0 {
			applied := e.BlockHeader != nil &&
				pointMatches(e.Point, ocommon.NewPoint(
					e.BlockHeader.SlotNumber(), e.BlockHeader.Hash().Bytes(),
				)) &&
				ls.headerAppliedWithContext(waitCtx, e.BlockHeader)
			release, current := ls.PinHeaderCryptoVerificationGeneration(
				currentGeneration,
			)
			if !current {
				continue
			}
			if !applied {
				release()
				return nil, false
			}
			e.HeaderCryptoVerificationGeneration = currentGeneration
			if e.PeerTipUpdate != nil {
				e.PeerTipUpdate.HeaderCryptoVerificationGeneration = currentGeneration
			}
			return release, true
		}
		select {
		case <-settled:
		case <-waitCtx.Done():
			return nil, false
		case <-ls.closeCh():
			return nil, false
		}
	}
	return nil, false
}
