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
	"bytes"
	"context"

	"github.com/blinklabs-io/dingo/chainselection"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

func (n *Node) chainsyncRecoverDeliveredPeerTip(
	ctx context.Context,
	cb ochainsync.CallbackContext,
	update chainselection.PeerTipUpdateEvent,
	header gledger.BlockHeader,
	sessionCurrent func() bool,
) {
	ls := n.wallClockLedger.Load()
	if ctx.Err() != nil || n.chainSelector == nil || ls == nil || sessionCurrent == nil || !sessionCurrent() ||
		header == nil ||
		update.HeaderCryptoVerificationGeneration == 0 ||
		cb.ConnectionId != update.ConnectionId ||
		header.BlockNumber() != update.ObservedTip.BlockNumber ||
		header.SlotNumber() != update.ObservedTip.Point.Slot ||
		!bytes.Equal(header.Hash().Bytes(), update.ObservedTip.Point.Hash) {
		return
	}
	anchor, limit, needed := n.chainSelector.EagernessDeliveryRepair(update)
	if !needed {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var protocolDone <-chan struct{}
	if cb.Client != nil && cb.Client.ProtocolInstance() != nil {
		protocolDone = cb.Client.ProtocolInstance().StopChan()
	}
	if protocolDone != nil || cb.ConnectionDoneChan != nil {
		go func() {
			select {
			case <-protocolDone:
			case <-cb.ConnectionDoneChan:
			case <-ctx.Done():
			}
			cancel()
		}()
	}
	proof, err := ls.VerifyPeerHeaderAncestry(
		ctx,
		update.ConnectionId,
		header,
		anchor,
		limit,
	)
	if err != nil {
		return
	}
	if ctx.Err() != nil {
		return
	}
	points := proof.Points
	proof.Points = nil
	n.chainSelector.RecordVerifiedEagernessPath(
		update,
		anchor,
		points,
		proof.Generation,
		func() bool {
			return sessionCurrent() && n.wallClockLedger.Load() == ls &&
				proof.Current()
		},
		func() bool {
			return sessionCurrent() && n.wallClockLedger.Load() == ls &&
				proof.Unwithdrawn()
		},
	)
}
