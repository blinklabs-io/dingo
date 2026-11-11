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

package ouroboros

import (
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// An observer's headers never move its admitted cursor, so the idle proof it
// hands chain selection must name the last header it delivered. A cursor left
// at the intersection never matches the delivered frontier, and the Genesis
// exit, which needs every ChainSync connection idle, never fires.
func TestChainsyncObserverAwaitReplyNamesDeliveredHeader(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, false)
	f.o.config.ChainsyncRecordDeliveredPeerTip = func(
		chainselection.PeerTipUpdateEvent,
	) {
	}
	f.o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		return 1, nil
	}
	var observed []ocommon.Point
	f.o.config.ChainsyncObserveAwaitReply = func(
		_ ouroboros.ConnectionId,
		point ocommon.Point,
	) {
		observed = append(observed, point)
	}
	header := newTestBlockHeader(100, 1, 0xaa)

	f.rollForward(t, header)
	require.NoError(t, f.o.chainsyncClientAwaitReply(
		ochainsync.CallbackContext{ConnectionId: f.conn},
	))

	require.Equal(t, []ocommon.Point{
		ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes()),
	}, observed)
}
