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
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestApplyByronBlockRecordsAppliedPoint applies a real Byron block through
// the production batch-apply loop. Byron has no evolving nonce, so the
// block_nonce row it leaves is the only durable record that the block was
// applied, and the reconciler's applied-point lookups depend on it.
func TestApplyByronBlockRecordsAppliedPoint(t *testing.T) {
	t.Parallel()

	ls, lastByron, _ := newByronShelleyBoundaryLedger(t)
	byronPoint := ocommon.NewPoint(
		lastByron.SlotNumber(),
		lastByron.Hash().Bytes(),
	)
	// Restart from origin so applying the block is a forward step that needs
	// no earlier Byron history.
	parentTip := ochainsync.Tip{}
	require.NoError(t, ls.db.DeleteBlockNoncesAfterPoint(parentTip.Point, nil))
	require.NoError(t, ls.db.SetTip(parentTip, nil))
	ls.currentTip = parentTip
	ls.currentTipBlockNonce = nil
	ls.validationEnabled = false
	ls.publishSnapshotsLocked()

	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{lastByron}}
	close(results)
	require.NoError(t, ls.ledgerProcessBlocksFromSource(
		context.Background(),
		results,
	))
	require.Equal(t, byronPoint, ls.currentTip.Point)

	rows, err := ls.db.GetBlockNoncesInSlotRange(
		byronPoint.Slot,
		byronPoint.Slot+1,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1, "an applied Byron block must leave an applied-point row")
	require.Equal(t, byronPoint.Hash, rows[0].Hash)
	require.Empty(t, rows[0].Nonce, "Byron has no evolving nonce")

	floor, ok, err := ls.durableAppliedFloor()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, byronPoint, floor)
}

// addNilNonceAppliedBlock extends the fixture's primary chain with one block
// whose applied-point row carries no evolving nonce, the shape Byron blocks
// leave, and returns its point.
func addNilNonceAppliedBlock(
	t *testing.T,
	fixture *chainsyncRollbackFixture,
	name string,
) ocommon.Point {
	t.Helper()
	tip := fixture.currentTip
	point := ocommon.NewPoint(tip.Point.Slot+5, testHashBytes(name))
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{{
		Slot:        point.Slot,
		Hash:        point.Hash,
		BlockNumber: tip.BlockNumber + 1,
		Type:        1,
		PrevHash:    tip.Point.Hash,
		Cbor:        []byte{0x80},
	}}))
	require.NoError(t, fixture.ls.db.SetBlockNonce(
		point.Hash, point.Slot, nil, false, nil,
	))
	return point
}

// TestLatestLedgerPrimaryChainAncestorFindsNilNonceAppliedPoint requires the
// common-ancestor search to return an applied block whose row has no nonce
// instead of skipping to an older block that has one.
func TestLatestLedgerPrimaryChainAncestorFindsNilNonceAppliedPoint(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	byronLike := addNilNonceAppliedBlock(t, fixture, "ancestor-nil-nonce")

	diverged := ocommon.NewPoint(
		byronLike.Slot+10,
		testHashBytes("ancestor-diverged"),
	)
	ancestor, ok, err := fixture.ls.latestLedgerPrimaryChainAncestor(
		diverged,
		false,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, byronLike, ancestor)
}

// TestDurableAppliedFloorIncludesNilNonceAppliedPoint requires the recovery
// floor to be the highest applied block even when its row has no nonce.
func TestDurableAppliedFloorIncludesNilNonceAppliedPoint(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	byronLike := addNilNonceAppliedBlock(t, fixture, "floor-nil-nonce")

	floor, ok, err := fixture.ls.durableAppliedFloor()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, byronLike, floor)
}
