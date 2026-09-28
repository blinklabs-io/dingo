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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedEpochs writes an epoch record starting at each given slot, so
// database.GetEpochBySlot can resolve which epoch covers an arbitrary slot
// in between two consecutive entries.
func seedEpochs(
	t *testing.T,
	ls *LedgerState,
	startSlotByEpoch map[uint64]uint64,
) {
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
func TestPoolStakeDistribution_AsOfSlot_ReadsHistoricalEpochSnapshot(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(repeatedBytes(28, 0x11)))
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  repeatedBytes(32, 0xAA),
		},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  repeatedBytes(32, 0xAA),
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	),
	)

	// Epoch 4's mark snapshot (praos.StakeSnapshotEpoch(4) == 3) -- exactly
	// the retained-boundary case: at live epoch 6, cleanupOldSnapshots'
	// default window deletes snapshot epochs below 6-3=3, so epoch 3 is the
	// oldest surviving row. Epoch 3, one epoch older still (its own
	// snapshot would be epoch 2), is already pruned -- see
	// TestPoolStakeDistribution_AsOfSlot_TooOldRejected's sibling case
	// below and checkAsOfEpochRecency's doc comment for the exact shift.
	require.NoError(
		t,
		db.Metadata().SavePoolStakeSnapshot(&models.PoolStakeSnapshot{
			Epoch: 3, SnapshotType: snapshotTypeMark,
			PoolKeyHash: pkh.Bytes(), TotalStake: dbtypes.Uint64(1_000_000),
			CapturedSlot: 1,
		}, nil),
	)
	// Epoch 6's mark snapshot (praos.StakeSnapshotEpoch(6) == 5) -- live.
	require.NoError(
		t,
		db.Metadata().SavePoolStakeSnapshot(&models.PoolStakeSnapshot{
			Epoch: 5, SnapshotType: snapshotTypeMark,
			PoolKeyHash: pkh.Bytes(), TotalStake: dbtypes.Uint64(9_000_000),
			CapturedSlot: 1,
		}, nil),
	)

	seedEpochs(t, ls, map[uint64]uint64{300: 3, 400: 4, 600: 6})
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	// asOfSlot 450 falls inside epoch 4's range: exactly the
	// retained-boundary case (see the snapshot comment above).
	hist, err := ls.PoolStakeDistribution(nil, QueryPoint{Slot: 450}, nil)
	require.NoError(t, err)
	require.Len(t, hist.Pools, 1)
	assert.Equal(t, uint64(1_000_000), hist.Pools[0].Stake)

	live, err := ls.PoolStakeDistribution(nil, QueryPoint{}, nil)
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
	_, err := ls.PoolStakeDistribution(nil, QueryPoint{Slot: 350}, nil)
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
	_, err := ls.PoolStakeDistribution(nil, QueryPoint{Slot: 650}, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestQueryShelleyCurrentProtocolParams_SameEpochAsLive_Succeeds covers the
// safe case for #382's protocol-parameters gap: a pinned point in the same
// epoch as the live tip is answerable, since protocol parameters only
// change at epoch boundaries -- "as of asOfSlot" and "live right now" are
// necessarily the same value within one epoch.
func TestQueryShelleyCurrentProtocolParams_SameEpochAsLive_Succeeds(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	// The live epoch this handler compares against comes from the published
	// consensus snapshot (loadConsensusSnapshot), not from a database read
	// -- see queryShelleyCurrentProtocolParams' doc comment for why. So the
	// in-memory epoch has to match the database epoch record seeded below,
	// the same way production code keeps both in step on every transition.
	ls.currentEpoch = models.Epoch{EpochId: 3}
	ls.publishSnapshotsLocked()

	seedEpochs(t, ls, map[uint64]uint64{300: 3})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, repeatedBytes(32, 0x0B)),
	}, nil))

	result, err := ls.queryShelleyCurrentProtocolParams(
		QueryPoint{Slot: 320},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// TestQueryShelleyCurrentProtocolParams_DifferentEpochFromLive_ReadsPersistedRow
// covers the real fix: a pin naming a slot in an epoch other than the live
// tip's is now answered from that epoch's own persisted pparams row
// (database.Database.GetPParams / loadPersistedProtocolParameters), the
// same row SetPParams already writes on every era transition and
// governance-enacted update. Epoch 3's persisted cost models are seeded to
// a value deliberately different from the live (epoch 6) in-memory value,
// so a query that silently fell back to live data (the bug this closes)
// would return the wrong cost models rather than merely succeeding.
func TestQueryShelleyCurrentProtocolParams_DifferentEpochFromLive_ReadsPersistedRow(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {9, 9, 9}},
	)
	// See the same-epoch test's comment: the live epoch this handler
	// compares against comes from the published consensus snapshot, so it
	// has to match the database epoch record seeded below for this test to
	// exercise a real epoch 3 vs. epoch 6 mismatch rather than an
	// incidental one against an unset in-memory epoch.
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()

	conwayEraId := uint(eras.ConwayEraDesc.Id)
	require.NoError(t, ls.db.SetEpoch(
		300, 3, nil, nil, nil, nil, conwayEraId, 1, 100, nil,
	))
	require.NoError(t, ls.db.SetEpoch(
		600, 6, nil, nil, nil, nil, conwayEraId, 1, 100, nil,
	))
	historicalPParams := conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	historicalCbor, err := cbor.Encode(historicalPParams)
	require.NoError(t, err)
	require.NoError(t, ls.db.SetPParams(
		historicalCbor, 300, 3, conwayEraId, nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	result, err := ls.queryShelleyCurrentProtocolParams(
		QueryPoint{Slot: 350}, nil,
	)
	require.NoError(t, err, "a persisted historical row must now be answered")
	results, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, results, 1)
	got, ok := results[0].(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.Equal(
		t, []int64{1, 1, 1}, got.CostModels[0],
		"must return epoch 3's own persisted cost models, not the live "+
			"in-memory epoch 6 value",
	)
}

// TestQueryShelleyCurrentProtocolParams_NoPersistedRow_Rejected covers an
// epoch with no persisted pparams row at all (never had a parameter change
// recorded, or one pruned after a rollback by DeletePParamsAfterSlot):
// unlike TestQueryShelleyCurrentProtocolParams_DifferentEpochFromLive_ReadsPersistedRow,
// there is nothing to answer from, so this must still fail with
// ErrHistoricalStateUnavailable rather than silently falling back to live
// data.
func TestQueryShelleyCurrentProtocolParams_NoPersistedRow_Rejected(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {9, 9, 9}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()

	conwayEraId := uint(eras.ConwayEraDesc.Id)
	require.NoError(t, ls.db.SetEpoch(
		300, 3, nil, nil, nil, nil, conwayEraId, 1, 100, nil,
	))
	require.NoError(t, ls.db.SetEpoch(
		600, 6, nil, nil, nil, nil, conwayEraId, 1, 100, nil,
	))
	// Deliberately no SetPParams call for epoch 3.
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	_, err := ls.queryShelleyCurrentProtocolParams(QueryPoint{Slot: 350}, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestQueryShelleyCurrentProtocolParams_HistoricalRowStripsSyntheticV2CostModel
// covers a pinned epoch whose persisted pparams row still carries
// HardForkBabbage's fabricated PlutusV2 cost model (blinklabs-io/dingo#3825):
// transitionToEraFrom persists newPParams verbatim, synthetic or not, so a
// historical epoch from before real V2 data arrived carries that same
// fabrication in its persisted CBOR. Answering it unfiltered would show a
// value no real cardano-node ever reported during that window -- the exact
// thing withoutSyntheticV2CostModel exists to prevent for the live path.
func TestQueryShelleyCurrentProtocolParams_HistoricalRowStripsSyntheticV2CostModel(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	// Live pparams already carry real V2 data (not the synthetic default),
	// distinguishing the live-case result from the historical one below.
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{1: {9, 9, 9}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()

	conwayEraId := uint(eras.ConwayEraDesc.Id)
	require.NoError(t, ls.db.SetEpoch(
		300, 3, nil, nil, nil, nil, conwayEraId, 1, 100, nil,
	))
	require.NoError(t, ls.db.SetEpoch(
		600, 6, nil, nil, nil, nil, conwayEraId, 1, 100, nil,
	))
	// Epoch 3's persisted row still has the fabricated default -- real V2
	// data had not landed yet at that historical point.
	historicalPParams := conwayPParamsWithCostModels(
		map[uint][]int64{1: eras.DefaultPlutusV2CostModel},
	)
	historicalCbor, err := cbor.Encode(historicalPParams)
	require.NoError(t, err)
	require.NoError(t, ls.db.SetPParams(
		historicalCbor, 300, 3, conwayEraId, nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	result, err := ls.queryShelleyCurrentProtocolParams(
		QueryPoint{Slot: 350}, nil,
	)
	require.NoError(t, err)
	results, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, results, 1)
	got, ok := results[0].(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	_, hasV2 := got.CostModels[1]
	assert.False(
		t, hasV2,
		"a historical row still carrying the fabricated PlutusV2 cost "+
			"model must have it stripped, matching what a real "+
			"cardano-node would have reported at that same point",
	)
}

// TestQueryShelleyCurrentProtocolParams_HistoricalRowRealReaffirmedDefaultNotStripped
// covers the opposite of the previous test: a persisted row whose PlutusV2
// cost model happens to equal eras.DefaultPlutusV2CostModel by coincidence,
// but where real (non-synthetic) data was already durably confirmed at or
// before this epoch (SyntheticV2CostModelClearedEpoch). The value-based
// heuristic alone (resolveSyntheticV2CostModel) cannot distinguish this from
// genuinely-still-synthetic data; the cleared-epoch provenance can, and must
// take precedence so a real value is never wrongly stripped from the reply.
func TestQueryShelleyCurrentProtocolParams_HistoricalRowRealReaffirmedDefaultNotStripped(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{1: {9, 9, 9}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()

	conwayEraId := uint(eras.ConwayEraDesc.Id)
	require.NoError(t, ls.db.SetEpoch(
		300, 3, nil, nil, nil, nil, conwayEraId, 1, 100, nil,
	))
	require.NoError(t, ls.db.SetEpoch(
		600, 6, nil, nil, nil, nil, conwayEraId, 1, 100, nil,
	))
	// Real data was confirmed as of epoch 2 -- before the targetEpoch (3)
	// this test pins to.
	require.NoError(t, database.SetSyntheticV2CostModelClearedEpoch(db, nil, 2))
	// Epoch 3's persisted row happens to carry the exact same values as the
	// fabricated default, but this is real, confirmed data, not the
	// fabrication.
	historicalPParams := conwayPParamsWithCostModels(
		map[uint][]int64{1: eras.DefaultPlutusV2CostModel},
	)
	historicalCbor, err := cbor.Encode(historicalPParams)
	require.NoError(t, err)
	require.NoError(t, ls.db.SetPParams(
		historicalCbor, 300, 3, conwayEraId, nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	result, err := ls.queryShelleyCurrentProtocolParams(
		QueryPoint{Slot: 350}, nil,
	)
	require.NoError(t, err)
	results, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, results, 1)
	got, ok := results[0].(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	v2, hasV2 := got.CostModels[1]
	require.True(
		t, hasV2,
		"real, confirmed data must not be stripped just because it "+
			"happens to match the fabricated default's value",
	)
	assert.Equal(t, []int64(eras.DefaultPlutusV2CostModel), v2)
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

	hist, err := ls.queryShelleyEpochNo(QueryPoint{Slot: 350}, nil)
	require.NoError(t, err)
	arr, ok := hist.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	assert.Equal(t, uint64(3), arr[0])

	live, err := ls.queryShelleyEpochNo(QueryPoint{}, nil)
	require.NoError(t, err)
	arr, ok = live.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	assert.Equal(t, uint64(6), arr[0])
}
