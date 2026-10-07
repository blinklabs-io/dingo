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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

const pinTestStabilityWindow = 1_000

// TestCapUtxoPruneFloor_NoPinsKeepsDefault pins the baseline: with nothing
// acquired, cleanup prunes exactly as before this change.
func TestCapUtxoPruneFloor_NoPinsKeepsDefault(t *testing.T) {
	t.Parallel()
	var ls LedgerState
	require.Equal(t, uint64(5_000),
		ls.capUtxoPruneFloor(5_000, pinTestStabilityWindow))
}

// TestCapUtxoPruneFloor_PinLowersFloor is the retain half of the race: a prune
// that runs after a point is acquired must not cut past it.
func TestCapUtxoPruneFloor_PinLowersFloor(t *testing.T) {
	t.Parallel()
	var ls LedgerState
	release := ls.PinAcquiredPoint(4_200)
	defer release()
	require.Equal(t, uint64(4_200),
		ls.capUtxoPruneFloor(5_000, pinTestStabilityWindow),
		"a pin below the default floor must hold the floor at the pin")
}

// TestCapUtxoPruneFloor_PinAboveFloorIsIgnored covers a pin already inside
// the retained window: it needs nothing kept, so the floor is unchanged.
func TestCapUtxoPruneFloor_PinAboveFloorIsIgnored(t *testing.T) {
	t.Parallel()
	var ls LedgerState
	release := ls.PinAcquiredPoint(9_000)
	defer release()
	require.Equal(t, uint64(5_000),
		ls.capUtxoPruneFloor(5_000, pinTestStabilityWindow))
}

// TestCapUtxoPruneFloor_BackstopBoundsAnAbandonedPin covers the issue's
// resource-exhaustion requirement: a client that acquires and never releases
// cannot stop spent-UTxO pruning for good. The pin holds the floor back by at
// most acquiredPointMaxUtxoHoldWindows stability windows.
func TestCapUtxoPruneFloor_BackstopBoundsAnAbandonedPin(t *testing.T) {
	t.Parallel()
	var ls LedgerState
	release := ls.PinAcquiredPoint(10)
	defer release()
	const floor = 100_000
	want := floor - pinTestStabilityWindow*acquiredPointMaxUtxoHoldWindows
	require.Equal(t, uint64(want),
		ls.capUtxoPruneFloor(floor, pinTestStabilityWindow),
		"a pin older than the backstop must stop holding the floor back")
}

// TestPinAcquiredPoint_ReleaseIsIdempotent matters because more than one
// clear path can reach the same pin -- a Release followed by the connection
// closing -- and a double release must not underflow or panic.
func TestPinAcquiredPoint_ReleaseIsIdempotent(t *testing.T) {
	t.Parallel()
	var ls LedgerState
	release := ls.PinAcquiredPoint(4_200)
	require.Equal(t, 1, ls.AcquiredPointPinCountForTesting())
	release()
	release()
	require.Equal(t, 0, ls.AcquiredPointPinCountForTesting())
	require.Equal(t, uint64(5_000),
		ls.capUtxoPruneFloor(5_000, pinTestStabilityWindow),
		"a released pin must stop holding the floor back")
}

// TestCapUtxoPruneFloor_AnnouncedFloorIsMonotonic: once a floor has been
// announced, rows below it may already be gone, so a later, lower cap must not
// lower what verify checks against.
func TestCapUtxoPruneFloor_AnnouncedFloorIsMonotonic(t *testing.T) {
	t.Parallel()
	var ls LedgerState
	ls.capUtxoPruneFloor(5_000, pinTestStabilityWindow)
	release := ls.PinAcquiredPoint(4_000)
	defer release()
	ls.capUtxoPruneFloor(5_000, pinTestStabilityWindow)
	utxo, _ := ls.announcedPruneFloors()
	require.Equal(t, uint64(5_000), utxo)
}

// TestCapPoolSnapshotPruneBefore_PinLowersBoundary is the pool-snapshot half
// of the race: an acquired point keeps the mark snapshot its stake distribution
// is computed from.
func TestCapPoolSnapshotPruneBefore_PinLowersBoundary(t *testing.T) {
	t.Parallel()
	var ls LedgerState
	release := ls.PinAcquiredPoint(700)
	defer release()
	epochOf := func(uint64) (uint64, bool) { return 7, true }
	// Epoch 7's mark snapshot is epoch 6 (praos.StakeSnapshotEpoch).
	require.Equal(t, uint64(6),
		ls.capPoolSnapshotPruneBefore(8, 0, epochOf))
}

// TestCapPoolSnapshotPruneBefore_UnmappableRetainsToBackstop: a pin whose
// epoch cannot be resolved gets the conservative answer, retain everything,
// still bounded by the backstop.
func TestCapPoolSnapshotPruneBefore_UnmappableRetainsToBackstop(t *testing.T) {
	t.Parallel()
	var ls LedgerState
	release := ls.PinAcquiredPoint(700)
	defer release()
	epochOf := func(uint64) (uint64, bool) { return 0, false }
	require.Equal(t, uint64(4),
		ls.capPoolSnapshotPruneBefore(8, 4, epochOf))
}

// The two tests below are the race itself. Acquire pins, then verifies, then
// records the point; a concurrent prune caps, announces, then deletes. Only
// two interleavings matter, and each is driven here explicitly rather than by
// racing goroutines, so the test cannot pass by luck.

// TestVerifyPointQueryable_PinBeforePrune_IsRetained: the pin lands first, so
// the prune that follows must retain the acquired point.
func TestVerifyPointQueryable_PinBeforePrune_IsRetained(t *testing.T) {
	t.Parallel()
	ls, at := newPinnedPointLedger(t)

	release := ls.PinAcquiredPoint(at.Slot)
	defer release()
	require.NoError(t, ls.VerifyPointQueryable(nil, at))

	// The prune now runs, and would have cut well past the point.
	floor := ls.capUtxoPruneFloor(at.Slot+500, pinTestStabilityWindow)
	require.Equal(t, at.Slot, floor,
		"a prune after the pin must not cut past the acquired point")
}

// TestVerifyPointQueryable_PruneAnnouncedBeforePin_IsRejected: the prune
// announced its floor first, and its delete has not yet committed, so the
// persisted floor still reads as safe. Without the announced-floor check the
// verify passes and the point is recorded on state about to be deleted --
// exactly the mid-query failure this exists to prevent. It must be refused
// here, where the protocol has a clean AcquireFailure.
func TestVerifyPointQueryable_PruneAnnouncedBeforePin_IsRejected(t *testing.T) {
	t.Parallel()
	ls, at := newPinnedPointLedger(t)

	ls.capUtxoPruneFloor(at.Slot+500, pinTestStabilityWindow)

	release := ls.PinAcquiredPoint(at.Slot)
	defer release()
	err := ls.VerifyPointQueryable(nil, at)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable,
		"a point below an announced prune floor must be refused at Acquire")
}

// newPinnedPointLedger returns a ledger with a single on-chain point that
// every retention check in VerifyPointQueryable accepts, so a rejection in
// the tests above can only come from the announced floor.
func newPinnedPointLedger(t *testing.T) (*LedgerState, QueryPoint) {
	t.Helper()
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = cardanoNodeConfigWithMaxLovelaceSupply(
		t, 45_000_000_000_000_000,
	)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 3}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3})
	require.NoError(t, db.Metadata().SetNetworkState(0, 1_000, 300, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, hash),
	}, nil))
	return ls, QueryPoint{Slot: 350, Hash: hash}
}

// TestVerifyPointQueryable_PoolSnapshotPruneAnnouncedBeforePin_IsRejected is
// the pool-snapshot counterpart of the prune-first test above. The snapshot
// guard announced a boundary that removes the point's mark snapshot, and its
// delete has not committed. The epoch-relative retention check still accepts
// the point here, so only the announced pool-snapshot floor can refuse it.
func TestVerifyPointQueryable_PoolSnapshotPruneAnnouncedBeforePin_IsRejected(
	t *testing.T,
) {
	t.Parallel()
	ls, at := newPinnedPointLedger(t)
	// The point is in epoch 3, so it needs the mark snapshot from epoch 2.
	// No pins yet, so the boundary is announced as given.
	ls.capPoolSnapshotPruneBefore(3, 0, func(uint64) (uint64, bool) {
		return 3, true
	})

	release := ls.PinAcquiredPoint(at.Slot)
	defer release()
	err := ls.VerifyPointQueryable(nil, at)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable,
		"a point whose snapshot is below an announced boundary must be "+
			"refused at Acquire")
}
