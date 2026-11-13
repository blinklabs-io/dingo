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
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chain"
)

// newMithrilBoundaryLedger builds a ledger over a two-block primary chain
// whose tip is the Mithril trust boundary, as on a node right after a
// Mithril bootstrap.
func newMithrilBoundaryLedger(t *testing.T) (*LedgerState, ochainsync.Tip) {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)
	parentHash := testHashBytes("mithril-parent-block")
	boundaryHash := testHashBytes("mithril-boundary-block")
	blocks := []chain.RawBlock{
		{
			Slot:        10,
			Hash:        parentHash,
			BlockNumber: 1,
			Type:        gledger.BlockTypeBabbage,
			Cbor:        []byte{0x80},
		},
		{
			Slot:        20,
			Hash:        boundaryHash,
			BlockNumber: 2,
			Type:        gledger.BlockTypeBabbage,
			PrevHash:    parentHash,
			Cbor:        []byte{0x80},
		},
	}
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(context.Background(), blocks),
	)
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	ls.metrics.init(prometheus.NewRegistry())
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(20, boundaryHash),
		BlockNumber: 2,
	}
	ls.Lock()
	ls.mithrilLedgerSlot = tip.Point.Slot
	ls.mithrilLedgerHash = tip.Point.Hash
	ls.Unlock()
	return ls, tip
}

// TestMithrilCoverageRequiresTheCertifiedChain pins coverage to the blocks
// the import certified: a point at a covered slot is covered only when it is
// on the primary chain with the trust boundary block.
func TestMithrilCoverageRequiresTheCertifiedChain(t *testing.T) {
	t.Parallel()

	ls, tip := newMithrilBoundaryLedger(t)
	require.True(t, ls.pointCoveredByMithril(tip.Point))
	_, trusted := ls.chainsyncHeaderCryptoPolicy(tip.Point)
	require.True(t, trusted)
	require.False(t, ls.ShouldVerifyChainSelectionHeaderCrypto(tip.Point))

	forged := ocommon.NewPoint(
		tip.Point.Slot,
		testHashBytes("forged-at-boundary-slot"),
	)
	require.False(t, ls.pointCoveredByMithril(forged))
	_, trusted = ls.chainsyncHeaderCryptoPolicy(forged)
	require.False(t, trusted)
	require.True(t, ls.ShouldVerifyChainSelectionHeaderCrypto(forged))
}

// TestForgedHeaderAtMithrilCoveredSlotDoesNotExtendTheChain is the attack on
// a Mithril-bootstrapped node: a header that names the local tip as its parent
// and claims the next block number, but carries a slot at or below the trust
// boundary. Neither the slot alone nor the header itself may make it part of
// the chain.
func TestForgedHeaderAtMithrilCoveredSlotDoesNotExtendTheChain(t *testing.T) {
	t.Parallel()

	for _, slot := range []uint64{15, 20} {
		t.Run(strconv.FormatUint(slot, 10), func(t *testing.T) {
			t.Parallel()
			ls, tip := newMithrilBoundaryLedger(t)
			forgedHash := lcommon.NewBlake2b256(
				testHashBytes("forged-header-" + strconv.FormatUint(slot, 10)),
			)
			err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
				ConnectionId: ouroboros.ConnectionId{
					LocalAddr: &net.TCPAddr{
						IP:   net.IPv4(127, 0, 0, 1),
						Port: 3001,
					},
					RemoteAddr: &net.TCPAddr{
						IP:   net.IPv4(127, 0, 0, 2),
						Port: 3001,
					},
				},
				BlockHeader: mockHeader{
					hash:        forgedHash,
					prevHash:    lcommon.NewBlake2b256(tip.Point.Hash),
					blockNumber: tip.BlockNumber + 1,
					slot:        slot,
				},
				Point: ocommon.NewPoint(slot, forgedHash.Bytes()),
				Tip: ochainsync.Tip{
					Point:       ocommon.NewPoint(slot, forgedHash.Bytes()),
					BlockNumber: tip.BlockNumber + 1,
				},
			})
			var slotErr chain.SlotNotAfterParentError
			require.ErrorAs(t, err, &slotErr)
			require.Equal(t, 0, ls.chain.HeaderCount())
			require.Equal(t, tip.Point, ls.chain.HeaderTip().Point)
		})
	}
}
