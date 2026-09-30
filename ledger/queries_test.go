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
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
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

// TestQueryHardFork_CurrentEra_PinnedPointResolvesEraAtThatPoint is the
// regression test for the gap this session's node-parity --from-genesis
// live validation surfaced (blinklabs-io/dingo#1900): HardForkCurrentEraQuery
// used to always answer with dingo's live era regardless of the pinned
// point, a real point-pinning gap #382's original scope decision left open
// (it covered queryShelleyCurrentProtocolParams and friends, not this
// HardFork-mini-protocol query type). gouroboros's client-side
// GetCurrentProtocolParams queries this first specifically to decide which
// era-shaped struct to decode the *next* query's reply into, so answering
// with the wrong era here breaks decoding a perfectly correct, already
// point-aware queryShelleyCurrentProtocolParams reply for any pinned point
// whose real era differs from dingo's live one -- confirmed live pinning at
// genesis (slot 0, Shelley) against a dingo instance already in a later
// era. Epoch 3 (Shelley) and epoch 6 (Conway, live) are seeded with
// deliberately different eras so a handler that silently fell back to the
// live era would be caught.
func TestQueryHardFork_CurrentEra_PinnedPointResolvesEraAtThatPoint(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()

	require.NoError(t, ls.db.SetEpoch(
		300, 3, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil,
	))
	require.NoError(t, ls.db.SetEpoch(
		600, 6, nil, nil, nil, nil, eras.ConwayEraDesc.Id, 1, 100, nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	pinned, err := ls.queryHardFork(
		&olocalstatequery.HardForkQuery{
			Query: &olocalstatequery.HardForkCurrentEraQuery{},
		},
		QueryPoint{Slot: 350},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		eras.ShelleyEraDesc.Id,
		pinned,
		"a point pinned in epoch 3 (Shelley) must resolve Shelley, not the live epoch 6 (Conway) era",
	)

	live, err := ls.queryHardFork(
		&olocalstatequery.HardForkQuery{
			Query: &olocalstatequery.HardForkCurrentEraQuery{},
		},
		QueryPoint{},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, eras.ConwayEraDesc.Id, live)
}

// TestQueryHardFork_CurrentEra_NoEpochRecordRejected covers a pinned point
// whose epoch has no epoch record at all: the era genuinely cannot be
// resolved, and this must fail with ErrHistoricalStateUnavailable rather
// than silently falling back to the live era (the exact bug this handler
// otherwise reproduces every time) or panicking on a nil era descriptor.
//
// Deliberately seeds an epoch-0 row too: resolveAsOfEpoch previously fell
// back to epoch 0, with no way to distinguish "genuinely epoch 0" from "no
// covering row found," when
// GetEpochBySlot found nothing for the pinned slot. On any genesis-synced
// node an epoch-0 row always exists, so GetEpoch(0) would then succeed and
// silently answer Byron for a point actually in a later era -- this test's
// previous fixture omitted the epoch-0 row entirely, so it passed for the
// wrong reason (both the fallback epoch and the real target epoch were
// missing) without ever exercising that silent-wrong-answer path. Slot 50
// here resolves to neither epoch 0 (slots 0-9) nor epoch 6 (starts at slot
// 600) -- a genuine gap, not the chain's start -- so a correct fix must
// still reject it even with epoch 0 present.
func TestQueryHardFork_CurrentEra_NoEpochRecordRejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()

	require.NoError(t, ls.db.SetEpoch(
		0, 0, nil, nil, nil, nil, eras.ByronEraDesc.Id, 1, 10, nil,
	))
	require.NoError(t, ls.db.SetEpoch(
		600, 6, nil, nil, nil, nil, eras.ConwayEraDesc.Id, 1, 100, nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	_, err := ls.queryHardFork(
		&olocalstatequery.HardForkQuery{
			Query: &olocalstatequery.HardForkCurrentEraQuery{},
		},
		QueryPoint{Slot: 50},
		nil,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestQueryHardForkEraHistory_EmitsAllKnownEras pins an invariant that
// existing tests don't: the CBOR result always contains one entry per era in
// eras.Eras (7 entries for Cardano), even when most eras have no epochs in the
// DB. Clients rely on this shape.
func TestQueryHardForkEraHistory_EmitsAllKnownEras(t *testing.T) {
	t.Parallel()

	const (
		tipSlot        = uint64(200_000)
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
	)

	db := newTestDB(t)
	// Populate only Conway — every other era is empty.
	require.NoError(t, db.SetEpoch(
		epochStartSlot, epochId,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, slotLenMs, epochLen,
		nil,
	))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.publishSnapshotsLocked()
	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)
	list, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)
	require.Len(t, list, len(eras.Eras),
		"era history must emit one entry per era in eras.Eras")

	// Inspect each entry: [start, end, params].
	for i, entry := range list {
		era, ok := entry.([]any)
		require.True(t, ok, "entry %d is not []any", i)
		require.Len(t, era, 3, "entry %d must have [start, end, params]", i)

		start, ok := era[0].([]any)
		require.True(t, ok, "entry %d start is not []any", i)
		require.Len(t, start, 3, "entry %d start must be 3-tuple", i)

		end, ok := era[1].([]any)
		require.True(t, ok, "entry %d end is not []any", i)
		require.Len(t, end, 3, "entry %d end must be 3-tuple", i)

		params, ok := era[2].([]any)
		require.True(t, ok, "entry %d params is not []any", i)
		require.Len(t, params, 4, "entry %d params must be 4-tuple", i)
	}
}

// TestQueryHardForkEraHistory_TransitionUnknown_TipNearEpochEnd pins the
// Haskell HFC semantics: when tipSlot + safeZone crosses into a later epoch,
// the forecast boundary extends to *that* epoch's end rather than clamping
// to the current epoch's end. This is the behavior unlocked by delegating
// the safe-zone computation to hardfork.BuildSummary; dingo's legacy
// pre-HFC-adapter code clamped too conservatively to the last-in-DB epoch's
// end.
func TestQueryHardForkEraHistory_TransitionUnknown_TipNearEpochEnd(
	t *testing.T,
) {
	t.Parallel()

	const (
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
		// tipSlot near the epoch end: tip + safeZone (25_920) = 545_920
		// crosses into epoch 501 (which spans [532_000, 964_000)).
		tipSlot = uint64(520_000)
	)
	const expectedEraEndSlot = uint64(964_000) // start of epoch 502
	const expectedEraEndEpoch = uint64(502)

	db := newTestDB(t)
	require.NoError(t, db.SetEpoch(
		epochStartSlot, epochId,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, slotLenMs, epochLen,
		nil,
	))

	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		transitionInfo: hardfork.NewTransitionUnknown(),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.publishSnapshotsLocked()
	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)
	eraList := result.(cbor.IndefLengthList)
	lastEra := eraList[len(eraList)-1].([]any)
	eraEnd := lastEra[1].([]any)

	slot, ok := eraEnd[1].(uint64)
	require.True(t, ok)
	epoch, ok := eraEnd[2].(uint64)
	require.True(t, ok)

	assert.Equal(
		t,
		expectedEraEndSlot,
		slot,
		"Unknown: tip+safeZone crossing into next epoch should extend EraEnd to that epoch's end",
	)
	assert.Equal(
		t,
		expectedEraEndEpoch,
		epoch,
		"Unknown: EraEnd epoch should be the epoch *after* the one containing tip+safeZone",
	)
}

// TestQueryHardForkEraHistory_AtEpochOverride_SurfacesKnownEnd pins that
// TestShelleyHardForkAtEpoch + ExperimentalHardForksEnabled propagates through
// to queryHardForkEraHistory as a TransitionKnown end: the open era's EraEnd
// epoch is the override epoch, not the stale tipSlot + safeZone cap.
//
// Setup: a Byron-only DB with a single epoch 3 occupying slots
// [epochStart, epochStart+length). TestShelleyHardForkAtEpoch is 5 and
// ExperimentalHardForksEnabled is true, so Byron's NextEraTrigger resolves to
// AtEpoch(5). With currentEpoch=3 < 5, evaluateTriggerAtEpoch will set
// TransitionKnown(5) and the Byron era's End must snap to epoch 5's start.
func TestQueryHardForkEraHistory_AtEpochOverride_SurfacesKnownEnd(
	t *testing.T,
) {
	t.Parallel()

	const (
		epochId        = uint64(3)
		epochLen       = uint(21_600)   // Byron epoch length (10k, k=2160)
		slotLenMs      = uint(20_000)   // 20s Byron slot
		epochStartSlot = uint64(64_800) // epoch 3 starts after 3 * 21_600
		tipSlot        = uint64(70_000)
	)

	db := newTestDB(t)
	require.NoError(t, db.SetEpoch(
		epochStartSlot, epochId,
		nil, nil, nil, nil,
		eras.ByronEraDesc.Id, slotLenMs, epochLen,
		nil,
	))

	cfg := newTestEraHistoryCfg(t)
	enabled := true
	override := uint64(5)
	cfg.ExperimentalHardForksEnabled = &enabled
	cfg.TestShelleyHardForkAtEpoch = &override

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ByronEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       epochId,
			StartSlot:     epochStartSlot,
			LengthInSlots: epochLen,
			SlotLength:    slotLenMs,
			EraId:         eras.ByronEraDesc.Id,
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Apply the trigger so transitionInfo reflects TransitionKnown(5) as it
	// would at runtime via Start() / the tip-update path.
	ls.evaluateTriggerAtEpoch()
	require.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	require.Equal(t, override, ls.transitionInfo.KnownEpoch)

	ls.publishSnapshotsLocked()
	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)
	list, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)
	require.NotEmpty(t, list)

	// The currently-open era (Byron at index 0) should have EraEnd.Epoch == 5.
	byronEra, ok := list[0].([]any)
	require.True(t, ok)
	require.Len(t, byronEra, 3)
	byronEnd, ok := byronEra[1].([]any)
	require.True(t, ok)
	require.Len(t, byronEnd, 3, "EraEnd is [relTime, slot, epoch]")

	endEpoch, ok := byronEnd[2].(uint64)
	require.True(t, ok, "EraEnd epoch must be uint64, got %T", byronEnd[2])
	assert.Equal(
		t,
		override,
		endEpoch,
		"AtEpoch override must pin the open era's EraEnd.Epoch at the override value",
	)
}

// TestQueryHardForkEraHistory_AdjacentErasContiguous pins that whenever two
// adjacent eras both carry real data, era[i].End bounds match era[i+1].Start
// on all three axes (relTime, slot, epoch). This is the invariant that would
// catch a timespan-accumulation bug like the one the legacy code flagged with
// a hand-written "timespan.Sub" after the transition-epoch detection.
func TestQueryHardForkEraHistory_AdjacentErasContiguous(t *testing.T) {
	t.Parallel()

	const (
		byronEpoch0     = uint64(0)
		byronEpoch0Len  = uint(21_600)
		byronSlotLenMs  = uint(20_000)
		shelleyEpoch1   = uint64(1)
		shelleyStart    = uint64(21_600) // after Byron epoch 0
		shelleyEpochLen = uint(432_000)
		slotLenMs       = uint(1_000)
		tipSlot         = shelleyStart + 100
	)

	db := newTestDB(t)
	require.NoError(t, db.SetEpoch(
		0, byronEpoch0, nil, nil, nil, nil,
		eras.ByronEraDesc.Id, byronSlotLenMs, byronEpoch0Len, nil,
	))
	require.NoError(t, db.SetEpoch(
		shelleyStart, shelleyEpoch1, nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id, slotLenMs, shelleyEpochLen, nil,
	))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.publishSnapshotsLocked()
	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)
	list, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)

	// Entry 0 = Byron (populated, closed), entry 1 = Shelley (populated, open).
	byronEra := list[0].([]any)
	shelleyEra := list[1].([]any)
	byronEnd := byronEra[1].([]any)
	shelleyStartBound := shelleyEra[0].([]any)

	// relTime — *big.Int picoseconds
	byronEndRel, ok := byronEnd[0].(*big.Int)
	require.True(
		t,
		ok,
		"byron end relTime must be *big.Int, got %T",
		byronEnd[0],
	)
	shelleyStartRel, ok := shelleyStartBound[0].(*big.Int)
	require.True(
		t,
		ok,
		"shelley start relTime must be *big.Int, got %T",
		shelleyStartBound[0],
	)
	assert.Equal(t, 0, byronEndRel.Cmp(shelleyStartRel),
		"byron end relTime (%s) must equal shelley start relTime (%s)",
		byronEndRel, shelleyStartRel,
	)

	assert.Equal(t, byronEnd[1], shelleyStartBound[1],
		"byron end slot must equal shelley start slot")
	assert.Equal(t, byronEnd[2], shelleyStartBound[2],
		"byron end epoch must equal shelley start epoch")
}

// TestQueryHardForkEraHistory_TransitionImpossible_MultiEpochEra reproduces
// the real-world case that the single-epoch TransitionImpossible tests miss:
// the current era has been running for several epochs, and the current
// epoch is well past the first.
//
// dingo sets TransitionImpossible in `evaluateTransitionImpossible` when the
// CURRENT epoch's end is inside the safe-zone horizon. The caller therefore
// expects `queryHardForkEraHistory` to serve the CURRENT epoch's end as
// EraEnd. But if the caller naively forwards `TransitionImpossible` into
// `hardfork.BuildSummary`, BuildSummary's Haskell-aligned semantics apply
// the safe zone from `current.Start` (the *first* epoch of the era) — and
// the resulting EraEnd lags many epochs behind the tip.
func TestQueryHardForkEraHistory_TransitionImpossible_MultiEpochEra(
	t *testing.T,
) {
	t.Parallel()

	const (
		slotLenMs = uint(1_000)
		epochLen  = uint(432_000)

		// Conway era has epochs 500, 501, 502 in the DB. Current epoch is 502.
		firstEpochId    = uint64(500)
		firstEpochSlot  = uint64(100_000)
		secondEpochId   = uint64(501)
		secondEpochSlot = uint64(532_000) // 100_000 + 432_000
		thirdEpochId    = uint64(502)
		thirdEpochSlot  = uint64(964_000) // 532_000 + 432_000
		thirdEpochEnd   = uint64(1_396_000)

		// Tip well inside epoch 502.
		tipSlot = uint64(1_100_000)
	)

	db := newTestDB(t)
	require.NoError(t, db.SetEpoch(
		firstEpochSlot, firstEpochId,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, slotLenMs, epochLen,
		nil,
	))
	require.NoError(t, db.SetEpoch(
		secondEpochSlot, secondEpochId,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, slotLenMs, epochLen,
		nil,
	))
	require.NoError(t, db.SetEpoch(
		thirdEpochSlot, thirdEpochId,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, slotLenMs, epochLen,
		nil,
	))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionImpossible(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)
	eraList := result.(cbor.IndefLengthList)
	lastEra := eraList[len(eraList)-1].([]any)
	eraEnd := lastEra[1].([]any)

	slot, ok := eraEnd[1].(uint64)
	require.True(t, ok, "EraEnd slot should be uint64")
	epoch, ok := eraEnd[2].(uint64)
	require.True(t, ok, "EraEnd epoch should be uint64")

	assert.Equal(
		t,
		thirdEpochEnd,
		slot,
		"TransitionImpossible with a multi-epoch era must serve the CURRENT epoch's end "+
			"(slot %d, end of epoch %d), not a safe-zone projection from the era's first epoch",
		thirdEpochEnd,
		thirdEpochId,
	)
	assert.Equal(t, thirdEpochId+1, epoch,
		"TransitionImpossible EraEnd epoch must be the current epoch + 1 (%d)",
		thirdEpochId+1)
	assert.GreaterOrEqual(t, slot, tipSlot,
		"TransitionImpossible EraEnd (%d) must never lag the tip (%d)",
		slot, tipSlot)
}

// seedBlockAtSlot writes a minimal block index entry for slot/hash, enough
// for database.BlockBySlot to find it -- Query's verifyPointOnChain doesn't
// decode the block, only compares Hash, so no real CBOR content is needed.
func seedBlockAtSlot(t *testing.T, ls *LedgerState, slot uint64, hash []byte) {
	t.Helper()
	require.NoError(t, ls.db.BlockCreate(models.Block{
		ID:   slot,
		Slot: slot,
		Hash: hash,
	}, nil))
}

// TestQuery_PinnedPointOnChain_Succeeds covers the common case: a pinned
// point naming a block this node's current chain actually has at that slot
// must be accepted, dispatching through to the query as normal
// (blinklabs-io/dingo#382, #5 in the follow-up review).
func TestQuery_PinnedPointOnChain_Succeeds(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	hash := bytes.Repeat([]byte{0xAB}, 32)
	seedBlockAtSlot(t, ls, 100, hash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(100, hash),
	}, nil))

	_, err := ls.Query(utxoWholeQuery(), QueryPoint{Slot: 100, Hash: hash})
	require.NoError(t, err)
}

// TestQuery_PinnedPointWrongHash_Rejected covers a rollback/fork scenario:
// the caller acquired a point at slot 100 naming hash A, but this node's
// current chain now has a different block (hash B) at slot 100 -- e.g. a
// rollback happened between Acquire and Query. Before verifyPointOnChain
// existed, a purely slot-keyed reconstruction would have silently answered
// against the new fork's data; it must instead fail with ErrPointNotOnChain.
// The tip is set at slot 100 (not left at origin) so this exercises the
// hash-mismatch rejection specifically, not the separate above-the-tip
// rejection TestQuery_PinnedPointAboveTip_Rejected covers.
func TestQuery_PinnedPointWrongHash_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	actualHash := bytes.Repeat([]byte{0xAB}, 32)
	acquiredHash := bytes.Repeat([]byte{0xCD}, 32)
	seedBlockAtSlot(t, ls, 100, actualHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(100, actualHash),
	}, nil))

	_, err := ls.Query(
		utxoWholeQuery(),
		QueryPoint{Slot: 100, Hash: acquiredHash},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPointNotOnChain)
}

// TestQuery_PinnedPointNoBlockAtSlot_Rejected covers a point naming a slot
// this node has no block for at all (never seen it, or it was never a real
// chain point) -- must fail with ErrPointNotOnChain rather than silently
// treating "no block" as equivalent to "empty state as of that slot". The
// tip is set past slot 999 (via a block at a later slot) so this exercises
// the no-block-found rejection specifically, not the above-the-tip one.
func TestQuery_PinnedPointNoBlockAtSlot_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	laterHash := bytes.Repeat([]byte{0xAB}, 32)
	seedBlockAtSlot(t, ls, 1000, laterHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1000, laterHash),
	}, nil))

	_, err := ls.Query(
		utxoWholeQuery(),
		QueryPoint{Slot: 999, Hash: bytes.Repeat([]byte{0xEE}, 32)},
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPointNotOnChain))
}

// TestQuery_PinnedPointAboveTip_Rejected covers the gap a purely
// slot-keyed lookup left open: database.BlockBySlot has no notion of
// "applied tip" at all, so a block retained in the blob store at a slot
// ahead of what has actually been applied to ledger state (e.g. a
// header-ahead-of-ledger buffer entry) could satisfy both the slot and
// hash check even though the ledger state a query is about to read from
// has not incorporated it. A point naming that block must be rejected
// as not on the (applied) chain, regardless of whether the block itself
// is retained.
func TestQuery_PinnedPointAboveTip_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	tipHash := bytes.Repeat([]byte{0xAB}, 32)
	aheadHash := bytes.Repeat([]byte{0xCD}, 32)
	seedBlockAtSlot(t, ls, 100, tipHash)
	seedBlockAtSlot(t, ls, 200, aheadHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(100, tipHash),
	}, nil))

	_, err := ls.Query(
		utxoWholeQuery(),
		QueryPoint{Slot: 200, Hash: aheadHash},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPointNotOnChain)
}

// TestQuery_SlotZeroWithHashIsPinned_Rejected covers QueryPoint.pinned()'s
// own predicate: a slot-0 point with a nonempty Hash is a real chain point
// (a real Byron genesis-adjacent block could sit at slot 0), not the origin
// sentinel (QueryPoint{}, both fields zero), and must still go through
// verifyPointOnChain -- not be silently treated as unpinned and answered
// from live state. This node has no such block, so it must be rejected the
// same way any other non-existent point would be.
func TestQuery_SlotZeroWithHashIsPinned_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	_, err := ls.Query(
		utxoWholeQuery(),
		QueryPoint{Slot: 0, Hash: bytes.Repeat([]byte{0xEE}, 32)},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPointNotOnChain)
}

// epochNoQuery wraps the leaf query the way the wire delivers GetEpochNo.
func epochNoQuery() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyEpochNoQuery{},
		},
	}
}

// TestQuery_SlotZeroPinned_DispatchesHistorically goes one step past
// TestQuery_SlotZeroWithHashIsPinned_Rejected: this node genuinely has a
// block at slot 0 matching the acquired hash, so verifyPointOnChain accepts
// it -- proving pinned-ness survives all the way through dispatch to the
// handler is the real point of this test. Every point-aware handler was
// passed a bare `asOfSlot uint64` derived from at.Slot, and every one of
// them treated asOfSlot == 0 as "live" -- so a point genuinely pinned at
// slot 0 passed validation only to have the handler silently ignore the
// pin and answer with the live epoch (6) instead of slot 0's own epoch
// (0). Handlers now take the whole QueryPoint and check at.pinned()
// instead of comparing a bare slot against zero.
func TestQuery_SlotZeroPinned_DispatchesHistorically(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()

	genesisHash := bytes.Repeat([]byte{0xAB}, 32)
	seedBlockAtSlot(t, ls, 0, genesisHash)
	seedEpochs(t, ls, map[uint64]uint64{0: 0, 600: 6})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	result, err := ls.Query(
		epochNoQuery(),
		QueryPoint{Slot: 0, Hash: genesisHash},
	)
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	assert.Equal(
		t, uint64(0), arr[0],
		"a point pinned at slot 0 must resolve slot 0's own epoch, not "+
			"silently fall through to the live epoch",
	)
}

// TestQuery_UnpinnedSkipsPointValidation covers the live path: a zero-value
// QueryPoint must not trigger verifyPointOnChain at all (no block need
// exist at slot 0), preserving every existing unpinned query's behavior.
func TestQuery_UnpinnedSkipsPointValidation(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	_, err := ls.Query(utxoWholeQuery(), QueryPoint{})
	require.NoError(t, err)
}

// utxoByTxInAsOf calls queryShelleyUtxoByTxIn directly for a single ref
// pinned at atSlot -- bypassing Query's verifyPointOnChain, the same way
// queries_asofslot_test.go's PoolStakeDistribution AsOf tests call
// PoolStakeDistribution directly -- and decodes the reply into a
// UtxoId->TransactionOutput map for assertions.
func utxoByTxInAsOf(
	t *testing.T,
	ls *LedgerState,
	txId []byte,
	outputIdx uint32,
	atSlot uint64,
) map[olocalstatequery.UtxoId]ledger.TransactionOutput {
	t.Helper()
	txIn := ledger.NewShelleyTransactionInput(
		hex.EncodeToString(txId),
		int(outputIdx),
	)
	result, err := ls.queryShelleyUtxoByTxIn(
		[]ledger.ShelleyTransactionInput{txIn},
		QueryPoint{Slot: atSlot},
		nil,
	)
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok, "expected []any result")
	require.Len(t, arr, 1)
	m, ok := arr[0].(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok, "expected UtxoId map")
	return m
}

// TestQueryShelleyUtxoByTxIn_AsOfSlot_LiveBetweenCreationAndSpend covers the
// core #1900 UTxO-query fix: a pinned point between a UTxO's creation
// (slot 100, via seedBabbageUtxo) and its later spend (marked deleted at
// slot 500) must report it live. Before this fix, this handler ignored the
// pinned point entirely and always answered from live state -- exactly the
// false-"missing"/false-"present" divergence node-parity's incremental
// mode proved live: acquiring an older point and querying a ref that a
// later block touched returned that later state through the same
// still-acquired session.
func TestQueryShelleyUtxoByTxIn_AsOfSlot_LiveBetweenCreationAndSpend(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1_000, repeatedBytes(32, 0x0B)),
	}, nil))

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	// seedBabbageUtxo always creates its row at AddedSlot 100.
	txId := seedBabbageUtxo(t, db, 0xC1, 0, addr, 5_000_000)
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		nil,
		[]dbtypes.UtxoKey{{TxId: txId, OutputIdx: 0}},
		500,
	))

	m := utxoByTxInAsOf(t, ls, txId, 0, 300)
	require.Len(
		t, m, 1,
		"utxo must be reported live between its creation (100) and spend (500)",
	)
	out := m[olocalstatequery.UtxoId{Hash: ledger.NewBlake2b256(txId), Idx: 0}]
	require.NotNil(t, out)
	require.Equal(t, uint64(5_000_000), out.Amount().Uint64())
}

// TestQueryShelleyUtxoByTxIn_AsOfSlot_BeforeCreation_Absent covers a pinned
// point earlier than the UTxO's own creation: it must not exist yet.
func TestQueryShelleyUtxoByTxIn_AsOfSlot_BeforeCreation_Absent(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1_000, repeatedBytes(32, 0x0B)),
	}, nil))

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	txId := seedBabbageUtxo(t, db, 0xC2, 0, addr, 1_000_000)
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		nil,
		[]dbtypes.UtxoKey{{TxId: txId, OutputIdx: 0}},
		500,
	))

	m := utxoByTxInAsOf(t, ls, txId, 0, 50)
	require.Empty(t, m, "utxo must not exist before its own creation slot")
}

// TestQueryShelleyUtxoByTxIn_AsOfSlot_SpentAtExactSlot_Absent covers the
// spend-side boundary: a pin naming exactly the slot a UTxO was spent at
// must report it absent (spent "at" a slot, not "strictly after" it, is
// already gone as of that slot) -- the mirror of AddedSlot's own
// at-or-before inclusion on the creation side.
func TestQueryShelleyUtxoByTxIn_AsOfSlot_SpentAtExactSlot_Absent(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1_000, repeatedBytes(32, 0x0B)),
	}, nil))

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	txId := seedBabbageUtxo(t, db, 0xC3, 0, addr, 1_000_000)
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		nil,
		[]dbtypes.UtxoKey{{TxId: txId, OutputIdx: 0}},
		500,
	))

	m := utxoByTxInAsOf(t, ls, txId, 0, 500)
	require.Empty(t, m, "utxo spent at slot 500 must be absent as of slot 500")
}

// TestQueryShelleyUtxoByTxIn_AsOfSlot_AfterSpend_Absent covers a pinned
// point after the UTxO was spent: it must be reported absent, not the
// live-state answer this handler gave before the #1900 fix.
func TestQueryShelleyUtxoByTxIn_AsOfSlot_AfterSpend_Absent(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1_000, repeatedBytes(32, 0x0B)),
	}, nil))

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	txId := seedBabbageUtxo(t, db, 0xC4, 0, addr, 1_000_000)
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		nil,
		[]dbtypes.UtxoKey{{TxId: txId, OutputIdx: 0}},
		500,
	))

	m := utxoByTxInAsOf(t, ls, txId, 0, 700)
	require.Empty(
		t,
		m,
		"utxo must be reported absent once spent, even live-state-wise",
	)
}

// TestQueryShelleyUtxoByTxIn_AsOfSlot_NeverSpent_StillLive covers the
// simple never-spent case at an arbitrary later pinned point: a UTxO that
// has never been marked deleted must remain live at any slot at or after
// its creation, regardless of how far the live tip has since advanced.
func TestQueryShelleyUtxoByTxIn_AsOfSlot_NeverSpent_StillLive(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(100_000, repeatedBytes(32, 0x0B)),
	}, nil))

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	txId := seedBabbageUtxo(t, db, 0xC5, 0, addr, 1_000_000)

	m := utxoByTxInAsOf(t, ls, txId, 0, 99_000)
	require.Len(
		t,
		m,
		1,
		"a never-spent utxo must remain live at any later pinned point",
	)
}

// TestQueryShelleyUtxoByTxIn_RetentionWindow_TooOldRejected covers the
// retention-window boundary: a pinned point older than this node's
// spent-UTxO retention floor (tip - stability window, the same threshold
// UtxosDeleteConsumed prunes by) must reject cleanly with
// ErrHistoricalStateUnavailable rather than risk answering "absent" for a
// ref that may have been live at that point but whose spend record has
// since been hard-deleted.
func TestQueryShelleyUtxoByTxIn_RetentionWindow_TooOldRejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	// newPoolDistr2Ledger leaves CardanoNodeConfig nil, so
	// calculateStabilityWindow returns the default (50_000) regardless of
	// era -- see TestCleanupConsumedUtxos_CoreModePrunes's identical setup.
	const tipSlot = 200_000
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, repeatedBytes(32, 0x0B)),
	}, nil))

	// floor = 200_000 - 50_000 = 150_000; one slot behind it must reject.
	txIn := ledger.NewShelleyTransactionInput(
		hex.EncodeToString(repeatedBytes(32, 0xEE)),
		0,
	)
	_, err := ls.queryShelleyUtxoByTxIn(
		[]ledger.ShelleyTransactionInput{txIn},
		QueryPoint{Slot: 149_999},
		nil,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestQueryShelleyUtxoByTxIn_RetentionWindow_AtFloor_Succeeds covers the
// exact floor slot itself: a ref spent at any slot strictly after the
// floor is guaranteed to have survived the periodic cleanup sweep (see
// checkUtxoRetentionWindow's doc comment for why), so a pin naming the
// floor slot exactly must be accepted, not rejected.
func TestQueryShelleyUtxoByTxIn_RetentionWindow_AtFloor_Succeeds(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	const tipSlot = 200_000
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, repeatedBytes(32, 0x0B)),
	}, nil))

	txIn := ledger.NewShelleyTransactionInput(
		hex.EncodeToString(repeatedBytes(32, 0xEE)),
		0,
	)
	_, err := ls.queryShelleyUtxoByTxIn(
		[]ledger.ShelleyTransactionInput{txIn},
		QueryPoint{Slot: 150_000},
		nil,
	)
	require.NoError(t, err)
}

// TestQueryShelleyUtxoByTxIn_RetentionWindow_APIModeNeverRejects covers
// API storage mode, which never hard-deletes spent UTxO rows (see
// cleanupConsumedUtxos' identical StorageModeAPI check) -- so no
// retention-window rejection applies there, even for a pin that would be
// rejected in core mode.
func TestQueryShelleyUtxoByTxIn_RetentionWindow_APIModeNeverRejects(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDBForCleanup(t, dbtypes.StorageModeAPI)
	ls := newPoolDistr2Ledger(t, db)
	const tipSlot = 200_000
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, repeatedBytes(32, 0x0B)),
	}, nil))

	txIn := ledger.NewShelleyTransactionInput(
		hex.EncodeToString(repeatedBytes(32, 0xEE)),
		0,
	)
	_, err := ls.queryShelleyUtxoByTxIn(
		[]ledger.ShelleyTransactionInput{txIn},
		// Far below what would be the core-mode floor (150_000).
		QueryPoint{Slot: 1},
		nil,
	)
	require.NoError(t, err)
}

// TestQueryShelleyUtxoByTxIn_RetentionWindow_PersistedFloorOverridesLenientLiveWindow
// covers the gap a retention floor derived from only the CURRENT tip and
// era's own stability window leaves open: a rollback lowering the tip, or
// an era transition widening the window (Byron's small 2k vs every
// Shelley+ era's much larger 3k/f), can each make a freshly-computed floor
// look more lenient than the floor real cleanup already committed to and
// pruned against -- see persistConsumedUtxoPruneFloor's doc comment.
// checkUtxoRetentionWindow must reject using the durably persisted floor
// even when the live tip/window alone would compute a smaller one.
func TestQueryShelleyUtxoByTxIn_RetentionWindow_PersistedFloorOverridesLenientLiveWindow(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	// newPoolDistr2Ledger leaves CardanoNodeConfig nil, so
	// calculateStabilityWindow returns the default (50_000).
	const tipSlot = 100_000
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, repeatedBytes(32, 0x0B)),
	}, nil))

	// Simulates an earlier cleanup pass (e.g. while still in Byron, or
	// before a rollback lowered the tip) that already pruned up to slot
	// 99_500 -- stricter than what today's live tip/window alone would
	// compute (100_000 - 50_000 = 50_000).
	require.NoError(t, ls.persistConsumedUtxoPruneFloor(99_500, nil))

	txIn := ledger.NewShelleyTransactionInput(
		hex.EncodeToString(repeatedBytes(32, 0xEE)),
		0,
	)
	_, err := ls.queryShelleyUtxoByTxIn(
		[]ledger.ShelleyTransactionInput{txIn},
		QueryPoint{Slot: 60_000},
		nil,
	)
	require.Error(
		t,
		err,
		"a pin the live tip/window alone would wrongly call safe must "+
			"still be rejected using the durably persisted floor",
	)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestQueryShelleyUtxoByTxIn_RetentionWindow_DeferredForCatchup_NotRejected
// covers checkUtxoRetentionWindow's utxoPruningDeferredForCatchup conjunct
// (ledger/queries.go): while this node is still catching up to a known
// upstream target, cleanupConsumedUtxos itself defers pruning entirely (see
// utxoPruningDeferredForCatchup's doc comment), so nothing has actually been
// pruned yet and the live-tip-derived retention floor must not apply either
// -- only the durably persisted floor (unset here, so zero) still can.
//
// Same shape as TestQueryShelleyUtxoByTxIn_RetentionWindow_TooOldRejected
// (identical tip and default 50_000 stability window, so the live-tip floor
// would otherwise be 150_000) except an active upstream connection with no
// admitted target yet marks pruning as deferred for catchup -- mirroring
// utxo_pruning_catchup_test.go's "active upstream, target not yet known"
// case. No-opping the !ls.utxoPruningDeferredForCatchup(...) conjunct in
// checkUtxoRetentionWindow makes this test fail with
// ErrHistoricalStateUnavailable instead of the required nil.
func TestQueryShelleyUtxoByTxIn_RetentionWindow_DeferredForCatchup_NotRejected(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	// newPoolDistr2Ledger leaves CardanoNodeConfig nil, so
	// calculateStabilityWindow returns the default (50_000).
	const tipSlot = 200_000
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, repeatedBytes(32, 0x0B)),
	}, nil))

	// An active upstream connection with no admitted target yet: "still
	// syncing," per utxoPruningDeferredForCatchup's own doc comment, so it
	// reports deferred regardless of tipSlot/stabilityWindow.
	connA := testChainsyncConnId(6301, 3301)
	ls.config.GetActiveConnectionFunc = func() *ouroboros.ConnectionId {
		return &connA
	}
	ls.publishActiveUpstream(connA)

	// Without deferral this would compute floor = 200_000 - 50_000 =
	// 150_000 (see RetentionWindow_TooOldRejected) and reject slot 1
	// outright.
	txIn := ledger.NewShelleyTransactionInput(
		hex.EncodeToString(repeatedBytes(32, 0xEE)),
		0,
	)
	_, err := ls.queryShelleyUtxoByTxIn(
		[]ledger.ShelleyTransactionInput{txIn},
		QueryPoint{Slot: 1},
		nil,
	)
	require.NoError(
		t,
		err,
		"pruning deferred for catchup means nothing has actually been "+
			"pruned yet, so the live-tip retention floor must not reject "+
			"this pin",
	)
}

// TestQuery_UtxoByTxIn_WiredThroughDispatch is an end-to-end check that
// Query's dispatch switch (ledger/queries.go) actually threads at and txn
// into queryShelleyUtxoByTxIn -- not just that the handler works when
// called directly, which every other test in this file exercises.
func TestQuery_UtxoByTxIn_WiredThroughDispatch(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	txId := seedBabbageUtxo(t, db, 0xC6, 0, addr, 1_000_000)
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		nil,
		[]dbtypes.UtxoKey{{TxId: txId, OutputIdx: 0}},
		500,
	))

	pointHash := bytes.Repeat([]byte{0xAB}, 32)
	seedBlockAtSlot(t, ls, 300, pointHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(300, pointHash),
	}, nil))

	txIn := ledger.NewShelleyTransactionInput(hex.EncodeToString(txId), 0)
	query := &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyUtxoByTxinQuery{
				TxIns: []ledger.ShelleyTransactionInput{txIn},
			},
		},
	}

	result, err := ls.Query(query, QueryPoint{Slot: 300, Hash: pointHash})
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	m, ok := arr[0].(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok)
	require.Len(
		t, m, 1,
		"utxo created at slot 100 and spent at slot 500 must be live as "+
			"of the pinned point (slot 300), proving the pin -- not live "+
			"state -- drove this answer",
	)
}
