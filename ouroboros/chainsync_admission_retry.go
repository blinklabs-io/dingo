// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ouroboros

import (
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

const maxPeerTipAdmissionAttempts = 4

// A publication can expire a verdict between authentication and staging,
// including while continuity repair reads storage. Reauthenticate the same
// header instead of turning that local race into a protocol failure.
func (o *Ouroboros) observeChainsyncPeerTip(
	cb ochainsync.CallbackContext,
	e *ledger.ChainsyncEvent,
	update *chainselection.PeerTipUpdateEvent,
	session chainsync.ClientSessionToken,
) (chainselection.PeerTipUpdateDisposition, error) {
	sessionCurrent := func() bool {
		return o.chainsyncState == nil ||
			o.chainsyncState.ClientSession(cb.ConnectionId) == session
	}
	observe := func() (chainselection.PeerTipUpdateDisposition, error) {
		if !sessionCurrent() {
			return chainselection.PeerTipUpdateDeferred, ochainsync.ErrStopSyncProcess
		}
		return o.config.ChainsyncObservePeerTip(*update), nil
	}
	disposition, err := observe()
	if err != nil || disposition != chainselection.PeerTipUpdateDeferred ||
		update.HeaderCryptoVerificationGeneration == 0 ||
		o.chainSelectionVerifyHeaderCrypto == nil {
		return disposition, err
	}
	admissionCtx, cancelAdmission := chainsyncAdmissionContext(cb)
	defer cancelAdmission()
	retryCtx, cancelRetry := context.WithTimeout(
		admissionCtx,
		o.catchUpBandWaitTimeout,
	)
	defer cancelRetry()
	wait := deferredVerificationWait{ctx: retryCtx}
	for range maxPeerTipAdmissionAttempts - 1 {
		if retryCtx.Err() != nil {
			if retryCtx.Err() == context.Canceled {
				return disposition, context.Canceled
			}
			return disposition, nil
		}
		if !sessionCurrent() {
			return disposition, ochainsync.ErrStopSyncProcess
		}
		var baseline ochainsync.Tip
		generation, err := o.verifyHeaderAwaitingLedger(
			cb,
			e.BlockHeader,
			&baseline,
			&wait,
		)
		if !sessionCurrent() {
			return disposition, ochainsync.ErrStopSyncProcess
		}
		if retryCtx.Err() == context.Canceled {
			return disposition, context.Canceled
		}
		if retryCtx.Err() != nil || ledger.IsHeaderVerificationDeferred(err) ||
			ledger.IsHeaderVerificationWithheld(err) {
			return disposition, nil
		}
		if err != nil {
			return chainselection.PeerTipUpdateRejected, fmt.Errorf(
				"reauthenticate deferred peer tip: %w",
				err,
			)
		}
		if generation == 0 {
			return disposition, nil
		}
		e.HeaderCryptoVerificationGeneration = generation
		update.HeaderCryptoVerificationGeneration = generation
		if o.config.ChainsyncRecordDeliveredPeerTip != nil {
			o.config.ChainsyncRecordDeliveredPeerTip(*update)
		}
		if o.chainsyncState != nil {
			o.chainsyncState.RecordAuthenticatedDeliveryForSession(
				session,
				chainsync.ObservedHeader{
					ConnectionId: cb.ConnectionId, BlockHeader: e.BlockHeader, ArrivalTime: e.ArrivalTime,
					Point: e.Point, Tip: e.Tip, BlockNumber: e.BlockHeader.BlockNumber(), Type: e.Type,
					HeaderCryptoVerificationGeneration: generation,
				},
			)
		}
		disposition, err = observe()
		if err != nil || disposition != chainselection.PeerTipUpdateDeferred {
			return disposition, err
		}
	}
	return chainselection.PeerTipUpdateDeferred, nil
}
