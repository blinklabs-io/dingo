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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

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
		ls.capPoolSnapshotPruneBeforeWith(8, 0, epochOf, ls.LeaderStakeSnapshotEpoch))
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
		ls.capPoolSnapshotPruneBeforeWith(8, 4, epochOf, ls.LeaderStakeSnapshotEpoch))
}

// The two tests below are the race itself. Acquire pins, then verifies, then
// records the point; a concurrent prune caps, announces, then deletes. Only
// two interleavings matter, and each is driven here explicitly rather than by
// racing goroutines, so the test cannot pass by luck.

// newPinnedPointLedger returns a ledger with a single on-chain point that
// every retention check in VerifyPointQueryable accepts, so a rejection in
// the tests above can only come from the announced floor.
func newPinnedPointLedger(t *testing.T) (*LedgerState, QueryPoint) {
	t.Helper()
	db := newTestDB(t)
	ls := &LedgerState{
		db:     db,
		config: LedgerStateConfig{Logger: testLogger()},
	}
	attachTestLedgerDB(t, ls)
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
	require.NoError(t, seedDB(t, db).SetNetworkState(0, 1_000, 300, nil))
	point := ocommon.NewPoint(350, hash)
	// The set's journal records the block, so the point is one it can read at.
	require.NoError(t, ls.config.UtxoSet.ApplyUtxoChanges(&point, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{Point: point}, nil))
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
	ls.capPoolSnapshotPruneBeforeWith(3, 0, func(uint64) (uint64, bool) {
		return 3, true
	}, ls.LeaderStakeSnapshotEpoch)

	release := ls.PinAcquiredPoint(at.Slot)
	defer release()
	err := ls.VerifyPointQueryable(context.Background(), nil, at)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable,
		"a point whose snapshot is below an announced boundary must be "+
			"refused at Acquire")
}
