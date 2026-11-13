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
// An expired verdict is replaced in two cases: the exact header has already
// been applied on the current chain, or the header is still queued and
// reverify returns a verdict for the current publication. Queue admission
// releases its pin just before the callback runs, and a block applied in
// between publishes a tip that expires the verdict though nothing about the
// header changed. A header that is neither applied nor queued keeps its
// expired verdict, and being queued does not replace one by itself.
// Membership is read before taking the new pin; the pin then rejects a
// publication or transition that invalidated that read.
func (ls *LedgerState) pinSettledHeaderAdmission(
	ctx context.Context,
	e *ChainsyncEvent,
	generation uint64,
	reverify func() (uint64, error),
) (func(), bool) {
	if generation == 0 || e == nil {
		return nil, false
	}
	if release, current := ls.PinHeaderCryptoVerificationGeneration(generation); current {
		return release, true
	}
	bound := e.BlockHeader != nil && pointMatches(e.Point, ocommon.NewPoint(
		e.BlockHeader.SlotNumber(), e.BlockHeader.Hash().Bytes(),
	))
	waitCtx, cancel := context.WithTimeout(ctx, blockfetchBusyTimeout)
	defer cancel()
	for range maxHeaderAdmissionSettleAttempts {
		settled := ls.txValidationSettledChan()
		if release, current := ls.PinHeaderCryptoVerificationGeneration(generation); current {
			return release, true
		}
		currentGeneration := ls.CurrentHeaderCryptoVerificationGeneration()
		if currentGeneration != 0 {
			applied, membershipSettled := false, true
			if bound {
				applied, membershipSettled = ls.headerAppliedSettled(waitCtx, e.BlockHeader)
			}
			if !membershipSettled {
				continue
			}
			release, current := ls.PinHeaderCryptoVerificationGeneration(
				currentGeneration,
			)
			if !current {
				continue
			}
			if !applied {
				release()
				if !bound || reverify == nil || ls.chain == nil ||
					!ls.chain.HoldsQueuedHeader(e.Point) {
					return nil, false
				}
				fresh, err := reverify()
				if err != nil && !IsHeaderVerificationDeferred(err) {
					return nil, false
				}
				if err != nil {
					// A deferral names a publication or transition under
					// way; the next attempt waits for it to settle.
					continue
				}
				release, current = ls.PinHeaderCryptoVerificationGeneration(
					fresh,
				)
				if !current {
					continue
				}
				currentGeneration = fresh
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

// maxHeaderAdmissionVerifyAttempts bounds how often a header whose verdict
// expired before it could be pinned is verified again.
const maxHeaderAdmissionVerifyAttempts = 3

// pinHeaderVerdict pins generation for header admission. Blocks applied during
// catch-up publish a tip every few milliseconds, so a verdict can expire
// between its verification and its pin even though nothing about the header
// changed; verifying again yields a verdict for the current publication.
// verify is nil when no fresh verdict can be produced. It returns a nil
// release and a nil error once every attempt expired, and the verification
// error unchanged when one fails.
func (ls *LedgerState) pinHeaderVerdict(
	generation uint64,
	verify func() (uint64, error),
) (uint64, func(), error) {
	if generation != 0 {
		if release, current := ls.PinHeaderCryptoVerificationGeneration(
			generation,
		); current {
			return generation, release, nil
		}
	}
	if verify == nil {
		return generation, nil, nil
	}
	for range maxHeaderAdmissionVerifyAttempts {
		fresh, err := verify()
		if err != nil {
			return 0, nil, err
		}
		if release, current := ls.PinHeaderCryptoVerificationGeneration(
			fresh,
		); current {
			return fresh, release, nil
		}
	}
	return generation, nil, nil
}
