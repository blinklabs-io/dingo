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
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// A callback can park before authenticating its incoming header. Refresh only
// the last authenticated delivery, so an observer waiting for a forecast
// cannot strand its older eagerness proof after a ledger publication.
func (o *Ouroboros) startChainsyncEagernessRecovery(
	ctx context.Context,
	cb ochainsync.CallbackContext,
	session chainsync.ClientSessionToken,
) func() {
	recoverPath := o.config.ChainsyncRecoverDeliveredPeerTip
	if recoverPath == nil || o.chainsyncState == nil {
		return func() {}
	}
	retryCtx, cancel := context.WithCancel(ctx)
	sessionCurrent := func() bool {
		return o.chainsyncState.ClientSession(cb.ConnectionId) == session
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-retryCtx.Done():
				return
			case <-ticker.C:
				if retryCtx.Err() != nil || !sessionCurrent() {
					return
				}
				delivery, ok := o.chainsyncState.LatestAuthenticatedDeliveryForSession(
					session,
					cb.ConnectionId,
				)
				if !ok {
					continue
				}
				update := chainselection.PeerTipUpdateEvent{
					ConnectionId: cb.ConnectionId, Tip: delivery.Tip,
					ObservedTip: ochainsync.Tip{
						Point:       delivery.Point,
						BlockNumber: delivery.BlockNumber,
					},
					ObservedPrevHash: delivery.BlockHeader.PrevHash().
						Bytes(),
					ObservedBoundary:                   delivery.Type == gledger.BlockTypeByronEbb,
					HeaderCryptoVerificationGeneration: delivery.HeaderCryptoVerificationGeneration,
				}
				recoverPath(
					retryCtx,
					cb,
					update,
					delivery.BlockHeader,
					sessionCurrent,
				)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
