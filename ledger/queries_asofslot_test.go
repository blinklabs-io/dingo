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
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedEpochs writes an epoch record starting at each given slot, so
// database.GetEpochBySlot can resolve which epoch covers an arbitrary slot
// in between two consecutive entries.
func seedEpochs(t *testing.T, ls *LedgerState, startSlotByEpoch map[uint64]uint64) {
	t.Helper()
	for startSlot, epoch := range startSlotByEpoch {
		require.NoError(t, ls.db.SetEpoch(
			startSlot, epoch, nil, nil, nil, nil, 0, 1, 100, nil,
		))
	}
}

// TestPoolStakeDistribution_AsOfSlot_ReadsHistoricalEpochSnapshot covers the
// core #382 stake-distribution fix: a pinned point in an older epoch must
// read that epoch's own mark snapshot, not the live tip's -- the two are
// seeded with deliberately different stake for the same pool so a test that
// silently fell back to live data would be caught.
func TestPoolStakeDistribution_AsOfSlot_ReadsHistoricalEpochSnapshot(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(repeatedBytes(28, 0x11)))
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{PoolKeyHash: pkh.Bytes(), VrfKeyHash: repeatedBytes(32, 0xAA)},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  repeatedBytes(32, 0xAA),
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))

	// Epoch 3's mark snapshot (praos.StakeSnapshotEpoch(3) == 2).
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(&models.PoolStakeSnapshot{
		Epoch: 2, SnapshotType: snapshotTypeMark,
		PoolKeyHash: pkh.Bytes(), TotalStake: dbtypes.Uint64(1_000_000),
		CapturedSlot: 1,
	}, nil))
	// Epoch 6's mark snapshot (praos.StakeSnapshotEpoch(6) == 5) -- live.
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(&models.PoolStakeSnapshot{
		Epoch: 5, SnapshotType: snapshotTypeMark,
		PoolKeyHash: pkh.Bytes(), TotalStake: dbtypes.Uint64(9_000_000),
		CapturedSlot: 1,
	}, nil))

	seedEpochs(t, ls, map[uint64]uint64{300: 3, 600: 6})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	// asOfSlot 350 falls inside epoch 3's range: exactly 3 epochs behind the
	// live epoch (6), the edge of the retention window (still allowed).
	hist, err := ls.PoolStakeDistribution(nil, 350)
	require.NoError(t, err)
	require.Len(t, hist.Pools, 1)
	assert.Equal(t, uint64(1_000_000), hist.Pools[0].Stake)

	live, err := ls.PoolStakeDistribution(nil, 0)
	require.NoError(t, err)
	require.Len(t, live.Pools, 1)
	assert.Equal(t, uint64(9_000_000), live.Pools[0].Stake)
}

// TestPoolStakeDistribution_AsOfSlot_TooOldRejected covers the retention
// boundary: an asOfSlot resolving to an epoch further behind the live epoch
// than the pool mark-snapshot retention window (ledger/snapshot's
// cleanupOldSnapshots currentEpoch-3 default) must fail rather than
// silently return an empty or wrong distribution for rows already pruned.
func TestPoolStakeDistribution_AsOfSlot_TooOldRejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	seedEpochs(t, ls, map[uint64]uint64{300: 3, 1000: 10})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1050, repeatedBytes(32, 0x0B)),
	}, nil))

	// Epoch 3 is 7 epochs behind the live epoch (10) -- outside the 3-epoch
	// retention window.
	_, err := ls.PoolStakeDistribution(nil, 350)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestPoolStakeDistribution_AsOfSlot_AheadOfLiveRejected covers a caller
// naming a point ahead of the live tip -- nonsensical (there is no future
// state to reconstruct) and must fail clearly rather than answering with
// whatever the live snapshot happens to be.
func TestPoolStakeDistribution_AsOfSlot_AheadOfLiveRejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	seedEpochs(t, ls, map[uint64]uint64{300: 3, 600: 6})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, repeatedBytes(32, 0x0B)),
	}, nil))

	// asOfSlot 650 resolves to epoch 6, ahead of the live tip's epoch 3.
	_, err := ls.PoolStakeDistribution(nil, 650)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestQueryShelleyCurrentProtocolParams_SameEpochAsLive_Succeeds covers the
// safe case for #382's protocol-parameters gap: a pinned point in the same
// epoch as the live tip is answerable, since protocol parameters only
// change at epoch boundaries -- "as of asOfSlot" and "live right now" are
// necessarily the same value within one epoch.
func TestQueryShelleyCurrentProtocolParams_SameEpochAsLive_Succeeds(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(map[uint][]int64{0: {1, 1, 1}})
	ls.publishSnapshotsLocked()

	seedEpochs(t, ls, map[uint64]uint64{300: 3})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, repeatedBytes(32, 0x0B)),
	}, nil))

	result, err := ls.queryShelleyCurrentProtocolParams(320)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// TestQueryShelleyCurrentProtocolParams_DifferentEpochFromLive_Rejected
// covers the actual gap this session's review identified: protocol
// parameters have no persisted historical-by-epoch record, so a pin naming
// a slot in an epoch other than the live tip's must fail rather than
// silently answer with the live (possibly different) value.
func TestQueryShelleyCurrentProtocolParams_DifferentEpochFromLive_Rejected(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(map[uint][]int64{0: {1, 1, 1}})
	ls.publishSnapshotsLocked()

	seedEpochs(t, ls, map[uint64]uint64{300: 3, 600: 6})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	_, err := ls.queryShelleyCurrentProtocolParams(350)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestQueryShelleyEpochNo_AsOfSlot_ReadsHistoricalEpoch covers GetEpochNo
// pinned to a historical point: unlike stake distribution or protocol
// parameters, epoch records carry no retention window or other coupled
// state, so resolving a historical epoch has nothing to reject -- it just
// answers whichever epoch actually covered the pinned slot, live tip
// notwithstanding.
func TestQueryShelleyEpochNo_AsOfSlot_ReadsHistoricalEpoch(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()

	seedEpochs(t, ls, map[uint64]uint64{300: 3, 600: 6})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	hist, err := ls.queryShelleyEpochNo(350)
	require.NoError(t, err)
	arr, ok := hist.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	assert.Equal(t, uint64(3), arr[0])

	live, err := ls.queryShelleyEpochNo(0)
	require.NoError(t, err)
	arr, ok = live.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	assert.Equal(t, uint64(6), arr[0])
}
