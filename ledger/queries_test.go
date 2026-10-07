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
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/gouroboros/protocol"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
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
// core stake-distribution fix: a pinned point in an older epoch must
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
// safe case for protocol-parameters gap: a pinned point in the same
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
// HardForkBabbage's fabricated PlutusV2 cost model:
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
// live validation surfaced: HardForkCurrentEraQuery
// used to always answer with dingo's live era regardless of the pinned
// point, a real point-pinning gap original scope decision left open
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
	result, err := ls.queryHardForkEraHistory(nil)
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
	result, err := ls.queryHardForkEraHistory(nil)
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
	result, err := ls.queryHardForkEraHistory(nil)
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
	result, err := ls.queryHardForkEraHistory(nil)
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

	result, err := ls.queryHardForkEraHistory(nil)
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
// queries_test.go's PoolStakeDistribution AsOf tests call
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
// core UTxO-query fix: a pinned point between a UTxO's creation
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
// live-state answer this handler gave before the fix.
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
// state_test.go's "active upstream, target not yet known"
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

// newTestEraHistoryCfg builds a CardanoNodeConfig with both Byron and Shelley
// genesis data, including slotLength and epochLength needed by EpochLengthShelley
// and the security parameters needed by calculateStabilityWindowForEra.
func newTestEraHistoryCfg(t testing.TB) *cardano.CardanoNodeConfig {
	t.Helper()
	byronGenesisJSON := `{
		"blockVersionData": { "slotDuration": "20000" },
		"protocolConsts": { "k": 432 }
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotLength": 1,
		"epochLength": 432000,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{}
	err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON))
	require.NoError(t, err)
	err = cfg.LoadShelleyGenesisFromReader(
		strings.NewReader(shelleyGenesisJSON),
	)
	require.NoError(t, err)
	return cfg
}

func TestGenesisConfigResultUsesNegotiatedLayout(t *testing.T) {
	t.Parallel()

	cfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	genesis := cfg.ShelleyGenesis()
	require.NotNil(t, genesis.ExtraConfig)
	ls := &LedgerState{config: LedgerStateConfig{CardanoNodeConfig: cfg}}
	legacyResult, err := ls.queryShelleyGenesisConfig(
		20 + protocol.ProtocolVersionNtCOffset,
	)
	require.NoError(t, err)
	legacyValues, ok := legacyResult.([]any)
	require.True(t, ok)
	require.Len(t, legacyValues, 1)
	require.Same(t, genesis, legacyValues[0])
	currentResult, err := ls.queryShelleyGenesisConfig(
		21 + protocol.ProtocolVersionNtCOffset,
	)
	require.NoError(t, err)
	currentValues, ok := currentResult.([]any)
	require.True(t, ok)
	require.Len(t, currentValues, 1)
	currentGenesis, ok := currentValues[0].(olocalstatequery.GenesisConfigResult)
	require.True(t, ok)
	require.NotNil(t, currentGenesis.ExtraConfig)
	query := &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyGenesisConfigQuery{},
		},
	}
	queried, err := ls.QueryWithProtocolVersion(
		query,
		QueryPoint{},
		21+protocol.ProtocolVersionNtCOffset,
	)
	require.NoError(t, err)
	queriedValues, ok := queried.([]any)
	require.True(t, ok)
	queriedGenesis, ok := queriedValues[0].(olocalstatequery.GenesisConfigResult)
	require.True(t, ok)
	require.NotNil(t, queriedGenesis.ExtraConfig)

	legacy, err := genesis.MarshalCBOR()
	require.NoError(t, err)
	var legacyFields []cbor.RawMessage
	_, err = cbor.Decode(legacy, &legacyFields)
	require.NoError(t, err)
	require.Len(t, legacyFields, 15)

	result, err := genesisConfigResult(genesis)
	require.NoError(t, err)
	encoded, err := cbor.Encode(result)
	require.NoError(t, err)
	var currentFields []cbor.RawMessage
	_, err = cbor.Decode(encoded, &currentFields)
	require.NoError(t, err)
	require.Len(t, currentFields, 16)
	var currentPParams []cbor.RawMessage
	_, err = cbor.Decode(currentFields[11], &currentPParams)
	require.NoError(t, err)
	require.Len(t, currentPParams, 17)
	var extra []cbor.RawMessage
	_, err = cbor.Decode(currentFields[15], &extra)
	require.NoError(t, err)
	require.Len(t, extra, 1)
	var decodedCurrent olocalstatequery.GenesisConfigResult
	_, err = cbor.Decode(encoded, &decodedCurrent)
	require.NoError(t, err)
	require.NotEmpty(t, decodedCurrent.ExtraConfig)
	var decodedInitialFunds []cbor.RawMessage
	_, err = cbor.Decode(decodedCurrent.InitialFunds, &decodedInitialFunds)
	require.NoError(t, err)
	require.Empty(t, decodedInitialFunds)
	var decodedStaking []cbor.RawMessage
	_, err = cbor.Decode(decodedCurrent.Staking, &decodedStaking)
	require.NoError(t, err)
	require.Len(t, decodedStaking, 2)

	var legacyWireValues []any
	_, err = cbor.Decode(legacy, &legacyWireValues)
	require.NoError(t, err)
	require.Len(t, legacyWireValues, 15)
}

func requireEraDesc(t testing.TB, eraId uint) eras.EraDesc {
	t.Helper()
	era := eras.GetEraById(eraId)
	require.NotNil(t, era)
	return *era
}

// TestQueryHardForkEraHistory_OpenEraEndBoundedBySafeZone proves that the
// current era's EraEnd is snapped to the end of the epoch that contains
// ledgerTip + safeZone.  Within a single epoch slot↔time is linear (constant
// slot length), so the epoch-end boundary is the safe forecast limit.
//
// Setup:
//   - One Conway epoch: startSlot=100_000, length=432_000 (ends at slot 532_000)
//   - ledgerTip at slot 200_000 (well inside the epoch)
//   - safeZone = ceil(3k/f) = ceil(3*432/0.05) = 25_920
//   - safeEndSlot = 225_920, which is within the epoch (< 532_000)
//
// Expected EraEnd slot: 532_000 (epoch end), epoch number: 501
func TestQueryHardForkEraHistory_OpenEraEndBoundedBySafeZone(t *testing.T) {
	t.Parallel()

	const (
		tipSlot        = uint64(200_000)
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000) // 1 second in milliseconds
		epochId        = uint64(500)
	)
	// safeZone = ceil(3 * 432 / 0.05) = 25_920; safeEndSlot = 225_920 < 532_000
	const expectedSafeZone = uint64(25_920)
	expectedEraEndSlot := epochStartSlot + uint64(epochLen) // 532_000

	db := newTestDB(t)
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
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.publishSnapshotsLocked()
	result, err := ls.queryHardForkEraHistory(nil)
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)
	require.NotEmpty(t, eraList)

	// The last entry in the list is the Conway (current, open) era.
	lastEra, ok := eraList[len(eraList)-1].([]any)
	require.True(t, ok, "era entry should be []any")
	require.Len(t, lastEra, 3, "era entry should be [start, end, params]")

	eraEnd, ok := lastEra[1].([]any)
	require.True(t, ok, "EraEnd should be []any")
	require.Len(t, eraEnd, 3, "EraEnd should be [relTime, slot, epoch]")

	actualEraEndSlot, ok := eraEnd[1].(uint64)
	require.True(t, ok, "EraEnd slot should be uint64")

	actualEraEndEpoch, ok := eraEnd[2].(uint64)
	require.True(t, ok, "EraEnd epoch should be uint64")

	assert.Equal(
		t,
		expectedEraEndSlot,
		actualEraEndSlot,
		"open era EraEnd slot should snap to epoch boundary (%d), not mid-epoch safeEndSlot (%d)",
		expectedEraEndSlot,
		tipSlot+expectedSafeZone,
	)
	assert.Equal(t, epochId+1, actualEraEndEpoch,
		"open era EraEnd epoch number should be epochId+1 (%d)", epochId+1,
	)
}

func TestQueryShelleyUtxoByAddress_EmptySlice(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	result, err := ls.queryShelleyUtxoByAddress(nil, QueryPoint{}, nil)
	require.NoError(t, err)
	// Should return []any{empty map}
	arr, ok := result.([]any)
	require.True(t, ok, "expected []any result")
	require.Len(t, arr, 1)
	m, ok := arr[0].(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok, "expected UtxoId map")
	require.Empty(t, m)
}

// TestQueryShelleyUtxoByAddress_MultipleAddresses proves the local-state-query
// handler resolves UTxOs for every address in the request, not just the
// first -- the wire query already carries the full set via q.Addrs.
func TestQueryShelleyUtxoByAddress_MultipleAddresses(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	seedAddressUtxo := func(
		addr lcommon.Address,
		txId []byte,
		amount uint64,
	) {
		require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
			TxId:       txId,
			OutputIdx:  0,
			PaymentKey: addr.PaymentKeyHash().Bytes(),
			AddedSlot:  1,
			Amount:     types.Uint64(amount),
		}))
		encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
			OutputAddress: addr,
			OutputAmount:  amount,
		})
		require.NoError(t, err)
		require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
			return db.Blob().SetUtxo(txn.Blob(), txId, 0, encoded)
		}))
	}

	addr1, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xa1}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	addr2, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xa2}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)

	txId1 := bytes.Repeat([]byte{0x01}, 32)
	txId2 := bytes.Repeat([]byte{0x02}, 32)
	seedAddressUtxo(addr1, txId1, 1_000_000)
	seedAddressUtxo(addr2, txId2, 2_000_000)

	ls := &LedgerState{db: db}
	result, err := ls.queryShelleyUtxoByAddress(
		[]ledger.Address{addr1, addr2},
		QueryPoint{},
		nil,
	)
	require.NoError(t, err)

	arr, ok := result.([]any)
	require.True(t, ok, "expected []any result")
	require.Len(t, arr, 1)
	m, ok := arr[0].(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok, "expected UtxoId map")
	require.Len(
		t,
		m,
		2,
		"must include UTxOs for both addresses, not just addrs[0]",
	)
}

func TestQueryShelleyUtxoByTxIn_EmptySlice(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	result, err := ls.queryShelleyUtxoByTxIn(nil, QueryPoint{}, nil)
	require.NoError(t, err)
	// Should return []any{empty map}
	arr, ok := result.([]any)
	require.True(t, ok, "expected []any result")
	require.Len(t, arr, 1)
	m, ok := arr[0].(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok, "expected UtxoId map")
	require.Empty(t, m)
}

// TestQueryShelleyUtxoByTxIn_MultipleInputs proves the GetUTxOByTxIn query
// resolves every requested TxIn in one call, not just the first, and
// silently omits a requested TxIn that has no matching live UTxO instead of
// failing the whole query.
//
// The two real TxIns are deliberately drawn from two distinct blocks'
// (rather than a single transaction's) produced outputs so the test does
// not depend on any one fixture transaction producing more than one UTxO:
// with only one genuinely resolvable input, a regression back to resolving
// just txIns[0] would still pass a count-based assertion.
//
// Blocks are stored in chain order, so a later block's transaction could
// spend an earlier block's collected candidate output before the test gets
// to use it. Liveness of every collected candidate is re-checked after each
// new block is stored, and only candidates still live at that point are
// kept; the loop stops as soon as two remain, so no further block storage
// (and thus no further spends) can happen before they're used below.
func TestQueryShelleyUtxoByTxIn_MultipleInputs(t *testing.T) {
	t.Parallel()

	db := newUtxoStorageTestDB(t)
	iter := newUtxoStorageTestIterator(t)

	utxoIdKey := func(id models.UtxoId) string {
		return fmt.Sprintf("%x:%d", id.Hash, id.Idx)
	}

	var candidates []models.UtxoId
	var live []models.UtxoId
	for len(live) < 2 {
		block, blockCbor := nextProducingBlock(t, db, iter)
		txn := db.Transaction(true)
		var produced lcommon.Utxo
		err := txn.Do(func(txn *database.Txn) error {
			tx := storeBlockFirstTx(t, db, txn, block, blockCbor)
			produced = tx.Produced()[0]
			return nil
		})
		require.NoError(t, err)
		candidates = append(candidates, models.UtxoId{
			Hash: produced.Id.Id().Bytes(),
			Idx:  produced.Id.Index(),
		})

		results, err := db.UtxosByRefs(candidates, nil)
		require.NoError(t, err)
		liveSet := make(map[string]struct{}, len(results))
		for _, u := range results {
			liveSet[utxoIdKey(models.UtxoId{Hash: u.TxId, Idx: u.OutputIdx})] = struct{}{}
		}
		live = live[:0]
		for _, c := range candidates {
			if _, ok := liveSet[utxoIdKey(c)]; ok {
				live = append(live, c)
			}
		}
	}
	live = live[:2]

	realTxIns := make([]ledger.ShelleyTransactionInput, len(live))
	for i, ref := range live {
		realTxIns[i] = ledger.NewShelleyTransactionInput(
			hex.EncodeToString(ref.Hash),
			int(ref.Idx),
		)
	}

	// A TxIn with no matching live UTxO must be silently omitted from the
	// result, not fail the whole batch.
	txIns := append(
		realTxIns,
		ledger.NewShelleyTransactionInput(strings.Repeat("00", 32), 9999),
	)

	ls := &LedgerState{db: db}
	result, err := ls.queryShelleyUtxoByTxIn(txIns, QueryPoint{}, nil)
	require.NoError(t, err)

	arr, ok := result.([]any)
	require.True(t, ok, "expected []any result")
	require.Len(t, arr, 1)
	m, ok := arr[0].(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok, "expected UtxoId map")
	require.Len(
		t,
		m,
		len(realTxIns),
		"exactly the real TxIns should resolve; bogus TxIn should be silently omitted",
	)

	for _, txIn := range realTxIns {
		utxoId := olocalstatequery.UtxoId{
			Hash: ledger.NewBlake2b256(txIn.Id().Bytes()),
			Idx:  int(txIn.Index()),
		}
		_, ok := m[utxoId]
		require.True(
			t,
			ok,
			"missing result for %s#%d",
			txIn.Id().String(),
			txIn.Index(),
		)
	}
}

// --- GetStakePools (ShelleyStakePoolsQuery) ---------------------------------

// poolHash28 builds a 28-byte pool key hash from a single byte pattern.
func poolHash28(b byte) []byte {
	out := make([]byte, 28)
	for i := range out {
		out[i] = b
	}
	return out
}

// TestStakePoolsResult_CanonicalEncoding proves the GetStakePools result is
// wire-compatible with cardano-cli: the pool set is sorted into ascending
// canonical order and CBOR-encodes to a set (tag 258) wrapped in the
// single-element result array, round-tripping through gouroboros'
// StakePoolsResult (the type cardano-node uses on the wire). An untagged or
// unsorted set is rejected by cardano-cli ("expected tag" / "Canonicity
// violation while decoding Set").
func TestStakePoolsResult_CanonicalEncoding(t *testing.T) {
	t.Parallel()

	// Deliberately unsorted input.
	keyHashes := [][]byte{
		poolHash28(0xCC),
		poolHash28(0x11),
		poolHash28(0x99),
	}
	result, err := stakePoolsResult(keyHashes)
	require.NoError(t, err)

	// Wire shape: []any{ cbor.Set{ poolIds... } }
	require.Len(t, result, 1)
	set, ok := result[0].(cbor.Set)
	require.True(t, ok, "inner element must be a cbor.Set (tag 258)")
	require.Len(t, set, 3)

	// Elements must be in ascending byte order (canonical set).
	for i := 1; i < len(set); i++ {
		prev := set[i-1].(ledger.PoolId)
		cur := set[i].(ledger.PoolId)
		assert.Negative(t, bytes.Compare(prev[:], cur[:]),
			"pool ids must be sorted ascending for a canonical set")
	}

	// Encode and decode through gouroboros' StakePoolsResult, which is the
	// exact type cardano clients use to read GetStakePools off the wire.
	encoded, err := cbor.Encode(result)
	require.NoError(t, err)
	var decoded olocalstatequery.StakePoolsResult
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err, "result must decode as cardano-cli expects")
	require.Len(t, decoded.Results, 3)
	// The decoded order matches the canonical (sorted) order we emitted.
	assert.Equal(t, ledger.PoolId(poolHash28(0x11)), decoded.Results[0])
	assert.Equal(t, ledger.PoolId(poolHash28(0x99)), decoded.Results[1])
	assert.Equal(t, ledger.PoolId(poolHash28(0xCC)), decoded.Results[2])
}

// TestStakePoolsResult_Empty verifies an empty pool set still produces the
// tagged, wrapped wire shape (an empty set), not a bare/absent value.
func TestStakePoolsResult_Empty(t *testing.T) {
	t.Parallel()

	result, err := stakePoolsResult(nil)
	require.NoError(t, err)
	require.Len(t, result, 1)
	set, ok := result[0].(cbor.Set)
	require.True(t, ok, "inner element must be a cbor.Set")
	require.Empty(t, set)

	encoded, err := cbor.Encode(result)
	require.NoError(t, err)
	var decoded olocalstatequery.StakePoolsResult
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err)
	require.Empty(t, decoded.Results)
}

// --- GetDRepState (ShelleyDRepStateQuery) -----------------------------------

// TestQueryShelleyDRepState_EmptyDB proves GetDRepState answers (rather than
// tears down the connection) when no DReps are registered: an empty
// credential set means "all DReps", which with no data is an empty map. The
// result is a bare CBOR map that round-trips through gouroboros'
// DRepStateResult (the type cardano clients decode into).
func TestQueryShelleyDRepState_EmptyDB(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	result, err := ls.queryShelleyDRepState(nil, nil)
	require.NoError(t, err)
	// Wire shape: []any{ map }. cardano-cli expects the result map wrapped in
	// the single-element result array; verified against cardano-node, whose
	// empty GetDRepState reply is the CBOR `81 a0` ([ {} ]).
	arr, ok := result.([]any)
	require.True(t, ok, "expected []any wrapper")
	require.Len(t, arr, 1)
	m, ok := arr[0].(olocalstatequery.DRepStateResult)
	require.True(t, ok, "inner element must be a DRepStateResult map")
	require.Empty(t, m)

	encoded, err := cbor.Encode(result)
	require.NoError(t, err)
	assert.Equal(
		t,
		"81a0",
		hex.EncodeToString(encoded),
		"empty GetDRepState result must encode to [ {} ] (matches cardano-node)",
	)
}

// TestQueryShelleyDRepState_Populated pins the per-DRep value to cardano-node's
// 4-element shape [ expiry, anchor, deposit, delegators ]: anchor is a
// StrictMaybe encoded as a list (empty for none, not CBOR null) and delegators
// is a tag-258 set of the delegating stake credentials. A 3-element value (or a
// null anchor) makes cardano-cli fail with "Size mismatch when decoding Record
// RecD. Expected 3, but found 4" while balancing a transaction.
func TestQueryShelleyDRepState_Populated(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	drepCred := stakeCred28(0xC1)
	delegKey := stakeCred28(0xD2)
	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		Credential:    drepCred,
		CredentialTag: 0,
		ExpiryEpoch:   22,
		Active:        true,
		AddedSlot:     10,
	}))
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey:    delegKey,
		CredentialTag: 0,
		Drep:          drepCred,
		DrepType:      models.DrepTypeAddrKeyHash,
		Active:        true,
		AddedSlot:     100,
	}))
	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	result, err := ls.queryShelleyDRepState(nil, nil)
	require.NoError(t, err)

	encoded, err := cbor.Encode(result)
	require.NoError(t, err)

	// [ { [0, drepCred] : [22, [], 0, set([ [0, delegKey] ]) ] } ]
	//   81 a1  8200 581c<drep>  84 16 80 00  d90102 81 8200 581c<deleg>
	// deposit is 0 here because no pparams are loaded in the bare test ledger.
	want := "81a1" +
		"8200581c" + strings.Repeat("c1", 28) +
		"84" + "16" + "80" + "00" +
		"d9010281" + "8200581c" + strings.Repeat("d2", 28)
	assert.Equal(t, want, hex.EncodeToString(encoded),
		"populated GetDRepState must encode the 4-element value with a "+
			"StrictMaybe-list anchor and a tag-258 delegators set (matches cardano-node)")
}

// --- GetAccountState (ShelleyAccountStateQuery) -----------------------------

// TestQueryShelleyAccountState_Empty proves GetAccountState answers with the
// treasury/reserves pots even when no network state has been captured yet
// (zeros). The wire shape is [ [treasury, reserves] ] (CBOR 81 82 00 00),
// verified against cardano-node's GetAccountState reply.
func TestQueryShelleyAccountState_Empty(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}

	result, err := ls.queryShelleyAccountState(QueryPoint{}, nil)
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok, "expected []any wrapper")
	require.Len(t, arr, 1)
	st, ok := arr[0].(olocalstatequery.AccountState)
	require.True(t, ok, "inner element must be an AccountState")
	assert.Zero(t, st.Treasury)
	assert.Zero(t, st.Reserves)

	encoded, err := cbor.Encode(result)
	require.NoError(t, err)
	assert.Equal(
		t,
		"81820000",
		hex.EncodeToString(encoded),
		"empty GetAccountState must encode to [ [0, 0] ] (matches cardano-node)",
	)
}

// --- ShelleyFilteredDelegationAndRewardAccountsQuery -----------------------

// stakeCred28 builds a 28-byte stake-credential hash from a single byte
// pattern, so test data is easy to read at a glance.
func stakeCred28(b byte) []byte {
	out := make([]byte, 28)
	for i := range out {
		out[i] = b
	}
	return out
}

func toBlake2b224(b []byte) lcommon.Blake2b224 {
	var h lcommon.Blake2b224
	copy(h[:], b)
	return h
}

// unwrapFilteredDelegationResult unpacks the
// []any{[]any{delegations, rewards}} wire shape and asserts both maps are
// of the expected typed shape.
func unwrapFilteredDelegationResult(
	t *testing.T,
	result any,
) (map[olocalstatequery.StakeCredential]lcommon.Blake2b224,
	map[olocalstatequery.StakeCredential]uint64,
) {
	t.Helper()
	outer, ok := result.([]any)
	require.True(t, ok, "expected outer []any")
	require.Len(t, outer, 1, "expected outer array of length 1")
	inner, ok := outer[0].([]any)
	require.True(t, ok, "expected inner []any")
	require.Len(t, inner, 2, "expected inner array [delegations, rewards]")
	dels, ok := inner[0].(map[olocalstatequery.StakeCredential]lcommon.Blake2b224)
	require.True(t, ok, "expected delegations map type")
	rwds, ok := inner[1].(map[olocalstatequery.StakeCredential]uint64)
	require.True(t, ok, "expected rewards map type")
	return dels, rwds
}

func TestQueryShelleyFilteredDelegationAndRewardAccounts_EmptyCreds(
	t *testing.T,
) {
	t.Parallel()

	ls := &LedgerState{}
	result, err := ls.queryShelleyFilteredDelegationAndRewardAccounts(nil, QueryPoint{}, nil)
	require.NoError(t, err)
	dels, rwds := unwrapFilteredDelegationResult(t, result)
	assert.Empty(t, dels, "delegations map should be empty for empty input")
	assert.Empty(t, rwds, "rewards map should be empty for empty input")
}

func TestQueryShelleyFilteredDelegationAndRewardAccounts_UnknownCred(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}

	cred := olocalstatequery.StakeCredential{
		Tag:   0,
		Bytes: toBlake2b224(stakeCred28(0xAA)),
	}
	result, err := ls.queryShelleyFilteredDelegationAndRewardAccounts(
		[]olocalstatequery.StakeCredential{cred},
		QueryPoint{},
		nil,
	)
	require.NoError(t, err)
	dels, rwds := unwrapFilteredDelegationResult(t, result)
	assert.Empty(t, dels, "unknown cred should not appear in delegations")
	assert.Empty(t, rwds, "unknown cred should not appear in rewards")
}

func TestQueryShelleyFilteredDelegationAndRewardAccounts_RegisteredUndelegated(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	stakeKey := stakeCred28(0xAA)
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey: stakeKey,
		Pool:       nil, // undelegated
		Reward:     types.Uint64(1_000_000),
		Active:     true,
	}))
	ls := &LedgerState{db: db}

	cred := olocalstatequery.StakeCredential{
		Tag:   0,
		Bytes: toBlake2b224(stakeKey),
	}
	result, err := ls.queryShelleyFilteredDelegationAndRewardAccounts(
		[]olocalstatequery.StakeCredential{cred},
		QueryPoint{},
		nil,
	)
	require.NoError(t, err)
	dels, rwds := unwrapFilteredDelegationResult(t, result)

	assert.NotContains(t, dels, cred,
		"undelegated account must not appear in delegations map")
	assert.Equal(t, uint64(1_000_000), rwds[cred],
		"reward balance must be returned for registered account")
}

// TestQueryShelleyFilteredDelegationAndRewardAccounts_AfterWithdrawal verifies
// LocalStateQuery observes the persisted reward balance after a withdrawal.
func TestQueryShelleyFilteredDelegationAndRewardAccounts_AfterWithdrawal(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	stakeKey := stakeCred28(0xAB)
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey: stakeKey,
		Reward:     types.Uint64(1_000_000),
		Active:     true,
	}))
	require.NoError(t, db.Metadata().ApplyAccountRewardWithdrawal(
		0,
		stakeKey,
		1_000_000,
		42,
		bytes.Repeat([]byte{0x55}, 32),
		nil,
	))
	ls := &LedgerState{db: db}

	cred := olocalstatequery.StakeCredential{
		Tag:   0,
		Bytes: toBlake2b224(stakeKey),
	}
	result, err := ls.queryShelleyFilteredDelegationAndRewardAccounts(
		[]olocalstatequery.StakeCredential{cred},
		QueryPoint{},
		nil,
	)
	require.NoError(t, err)
	_, rwds := unwrapFilteredDelegationResult(t, result)

	require.Contains(t, rwds, cred,
		"queried credential must be present in rewards map")
	assert.Equal(t, uint64(0), rwds[cred],
		"withdrawn reward balance must be reflected in LocalStateQuery")
}

func TestQueryShelleyFilteredDelegationAndRewardAccounts_RegisteredDelegated(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	stakeKey := stakeCred28(0xBB)
	poolHash := stakeCred28(0xCC) // 28 bytes is also pool key hash size
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey: stakeKey,
		Pool:       poolHash,
		Reward:     types.Uint64(2_500_000),
		Active:     true,
	}))
	ls := &LedgerState{db: db}

	cred := olocalstatequery.StakeCredential{
		Tag:   0,
		Bytes: toBlake2b224(stakeKey),
	}
	result, err := ls.queryShelleyFilteredDelegationAndRewardAccounts(
		[]olocalstatequery.StakeCredential{cred},
		QueryPoint{},
		nil,
	)
	require.NoError(t, err)
	dels, rwds := unwrapFilteredDelegationResult(t, result)

	assert.Equal(t, toBlake2b224(poolHash), dels[cred],
		"delegated account must report its pool")
	assert.Equal(t, uint64(2_500_000), rwds[cred],
		"reward balance must be returned for delegated account")
}

func TestQueryShelleyFilteredDelegationAndRewardAccounts_Mixed(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	delegatedKey := stakeCred28(0x01)
	delegatedPool := stakeCred28(0x10)
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey: delegatedKey,
		Pool:       delegatedPool,
		Reward:     types.Uint64(100),
		Active:     true,
	}))

	undelegatedKey := stakeCred28(0x02)
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey: undelegatedKey,
		Pool:       nil,
		Reward:     types.Uint64(200),
		Active:     true,
	}))

	unknownKey := stakeCred28(0x03) // never inserted

	ls := &LedgerState{db: db}

	creds := []olocalstatequery.StakeCredential{
		{Tag: 0, Bytes: toBlake2b224(delegatedKey)},
		{Tag: 0, Bytes: toBlake2b224(undelegatedKey)},
		{Tag: 0, Bytes: toBlake2b224(unknownKey)},
	}
	result, err := ls.queryShelleyFilteredDelegationAndRewardAccounts(creds, QueryPoint{}, nil)
	require.NoError(t, err)
	dels, rwds := unwrapFilteredDelegationResult(t, result)

	// Delegated cred → in both maps.
	assert.Equal(t, toBlake2b224(delegatedPool), dels[creds[0]])
	assert.Equal(t, uint64(100), rwds[creds[0]])

	// Undelegated cred → in rewards only.
	assert.NotContains(t, dels, creds[1])
	assert.Equal(t, uint64(200), rwds[creds[1]])

	// Unknown cred → in neither.
	assert.NotContains(t, dels, creds[2])
	assert.NotContains(t, rwds, creds[2])

	assert.Len(t, dels, 1, "exactly one delegation expected")
	assert.Len(t, rwds, 2, "exactly two reward entries expected")
}

// TestQueryShelleyFilteredDelegationAndRewardAccounts_TagAware verifies that
// filtered account lookup treats key and script credentials with the same hash
// as distinct reward accounts.
func TestQueryShelleyFilteredDelegationAndRewardAccounts_TagAware(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	stakeKey := stakeCred28(0x44)
	keyPool := stakeCred28(0x45)
	scriptPool := stakeCred28(0x46)

	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Pool:          keyPool,
		Reward:        types.Uint64(100),
		Active:        true,
	}))
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 1,
		Pool:          scriptPool,
		Reward:        types.Uint64(200),
		Active:        true,
	}))

	ls := &LedgerState{db: db}
	keyCred := olocalstatequery.StakeCredential{
		Tag:   0,
		Bytes: toBlake2b224(stakeKey),
	}
	scriptCred := olocalstatequery.StakeCredential{
		Tag:   1,
		Bytes: toBlake2b224(stakeKey),
	}

	result, err := ls.queryShelleyFilteredDelegationAndRewardAccounts(
		[]olocalstatequery.StakeCredential{keyCred, scriptCred},
		QueryPoint{},
		nil,
	)
	require.NoError(t, err)
	dels, rwds := unwrapFilteredDelegationResult(t, result)

	assert.Equal(t, toBlake2b224(keyPool), dels[keyCred])
	assert.Equal(t, uint64(100), rwds[keyCred])
	assert.Equal(t, toBlake2b224(scriptPool), dels[scriptCred])
	assert.Equal(t, uint64(200), rwds[scriptCred])
}

func TestQueryShelleyStakeDelegDeposits(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	stakeKey := stakeCred28(0x51)
	cred := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(stakeKey),
	}
	txBuilder := mockledger.NewTransactionBuilder()
	txBuilder.WithId(bytes.Repeat([]byte{0x52}, 32))
	txBuilder.WithValid(true)
	input, err := mockledger.NewSimpleTransactionInput(
		bytes.Repeat([]byte{0x54}, 32),
		0,
	)
	require.NoError(t, err)
	txBuilder.WithInputs(input)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(
			"addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd",
		).
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	txBuilder.WithOutputs(output)
	txBuilder.WithCertificates(&lcommon.StakeRegistrationCertificate{
		StakeCredential: cred,
	})
	tx, err := txBuilder.Build()
	require.NoError(t, err)
	require.NoError(t, db.SetTransactionMetadataOnly(
		tx,
		ocommon.NewPoint(100, bytes.Repeat([]byte{0x53}, 32)),
		0,
		map[int]uint64{0: 2_000_000},
		nil, 0,
	))

	ls := &LedgerState{db: db}
	queryCred := olocalstatequery.StakeCredential{
		Tag:   0,
		Bytes: lcommon.NewBlake2b224(stakeKey),
	}
	unknownCred := olocalstatequery.StakeCredential{
		Tag:   0,
		Bytes: lcommon.NewBlake2b224(stakeCred28(0x55)),
	}
	result, err := ls.queryShelleyStakeDelegDeposits(
		[]olocalstatequery.StakeCredential{queryCred, unknownCred},
		QueryPoint{},
		nil,
	)
	require.NoError(t, err)
	outer, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, outer, 1)
	deposits, ok := outer[0].(olocalstatequery.StakeDelegDepositsResult)
	require.True(t, ok)
	assert.Equal(t, uint64(2_000_000), deposits[queryCred])
	assert.NotContains(t, deposits, unknownCred)

	encoded, err := cbor.Encode(result)
	require.NoError(t, err)
	var decoded olocalstatequery.StakeDelegDepositsResult
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err)
	assert.Equal(t, uint64(2_000_000), decoded[queryCred])
}

func TestQueryShelleyFilteredVoteDelegatees(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	stakeKey := stakeCred28(0x61)
	drepKey := stakeCred28(0x62)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Drep:          drepKey,
		DrepType:      models.DrepTypeAddrKeyHash,
		Active:        true,
	}))
	ls := &LedgerState{db: db}
	cred := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(stakeKey),
	}

	result, err := ls.queryShelleyFilteredVoteDelegatees(
		[]lcommon.Credential{cred},
		QueryPoint{},
		nil,
	)
	require.NoError(t, err)
	outer, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, outer, 1)
	delegatees, ok := outer[0].(olocalstatequery.FilteredVoteDelegateesResult)
	require.True(t, ok)
	queryCred := olocalstatequery.StakeCredential{
		Tag:   0,
		Bytes: lcommon.NewBlake2b224(stakeKey),
	}
	require.Contains(t, delegatees, queryCred)
	assert.Equal(t, int(models.DrepTypeAddrKeyHash), delegatees[queryCred].Type)
	assert.Equal(t, drepKey, delegatees[queryCred].Credential)

	encoded, err := cbor.Encode(result)
	require.NoError(t, err)
	var decoded olocalstatequery.FilteredVoteDelegateesResult
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err)
	assert.Equal(t, delegatees[queryCred], decoded[queryCred])
}

func TestQueryShelleyGetProposalsReturnsDepositProcedure(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	txHash := bytes.Repeat([]byte{0x71}, 32)
	returnAddressBytes := append(
		[]byte{0xe0},
		bytes.Repeat([]byte{0x72}, 28)...,
	)
	govAction, err := cbor.Encode([]any{uint64(lcommon.GovActionTypeInfo)})
	require.NoError(t, err)
	proposal := &models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   1,
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch: 0,
		ExpiresEpoch:  10,
		AnchorURL:     "https://example.com/proposal.json",
		AnchorHash:    bytes.Repeat([]byte{0x73}, 32),
		Deposit:       100_000_000,
		ReturnAddress: returnAddressBytes,
		GovActionCbor: govAction,
		AddedSlot:     100,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:         proposal.ID,
		VoterType:          models.VoterTypeDRep,
		VoterCredentialTag: 0,
		VoterCredential:    stakeCred28(0x74),
		Vote:               models.VoteYes,
		AddedSlot:          101,
	}, nil))
	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	result, err := ls.queryShelleyGetProposals(nil, nil)
	require.NoError(t, err)
	outer, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, outer, 1)
	proposals, ok := outer[0].(olocalstatequery.ProposalsResult)
	require.True(t, ok)
	require.Len(t, proposals, 1)
	assert.Len(t, proposals[0].DRepVotes, 1)

	var procedure conway.ConwayProposalProcedure
	_, err = cbor.Decode(proposals[0].ProposalProcedure, &procedure)
	require.NoError(t, err)
	assert.Equal(t, proposal.Deposit, procedure.Deposit())
	gotReturnAddress, err := procedure.RewardAccount().Bytes()
	require.NoError(t, err)
	assert.Equal(t, returnAddressBytes, gotReturnAddress)
}

func TestEpochPicoseconds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		slotLength    uint
		lengthInSlots uint
		expected      *big.Int
	}{
		{
			// Shelley epoch: 1000ms slots, 432000 slots
			// 1000 * 432000 * 1e9 = 432_000_000_000_000_000
			name:          "shelley epoch",
			slotLength:    1000,
			lengthInSlots: 432000,
			expected: new(big.Int).SetUint64(
				432_000_000_000_000_000,
			),
		},
		{
			// Byron epoch: 20000ms slots, 21600 slots
			// 20000 * 21600 * 1e9 = 432_000_000_000_000_000
			name:          "byron epoch",
			slotLength:    20000,
			lengthInSlots: 21600,
			expected: new(big.Int).SetUint64(
				432_000_000_000_000_000,
			),
		},
		{
			name:          "zero slot length",
			slotLength:    0,
			lengthInSlots: 432000,
			expected:      big.NewInt(0),
		},
		{
			name:          "zero length in slots",
			slotLength:    1000,
			lengthInSlots: 0,
			expected:      big.NewInt(0),
		},
		{
			// Large values that would overflow uint64 in
			// naive uint multiplication:
			// MaxUint32 * MaxUint32 * 1e9 overflows uint64,
			// but big.Int handles it correctly.
			name:          "large values no overflow",
			slotLength:    math.MaxUint32,
			lengthInSlots: math.MaxUint32,
			expected: func() *big.Int {
				a := new(big.Int).SetUint64(math.MaxUint32)
				b := new(big.Int).SetUint64(math.MaxUint32)
				r := new(big.Int).Mul(a, b)
				r.Mul(r, big.NewInt(1_000_000_000))
				return r
			}(),
		},
		{
			name:          "single slot single ms",
			slotLength:    1,
			lengthInSlots: 1,
			expected:      big.NewInt(1_000_000_000),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := epochPicoseconds(
				tc.slotLength,
				tc.lengthInSlots,
			)
			// Use Cmp instead of Equal because big.Int
			// internal representation of zero varies
			// (nil abs vs empty abs).
			assert.Equal(
				t,
				0,
				tc.expected.Cmp(result),
				"picosecond calculation mismatch: "+
					"expected %s, got %s",
				tc.expected.String(), result.String(),
			)
		})
	}
}

func TestEpochPicoseconds_OverflowSafe(t *testing.T) {
	t.Parallel()

	// Verify that large values that would overflow uint64
	// in naive multiplication are handled correctly by
	// big.Int arithmetic.
	//
	// MaxUint32 * MaxUint32 = 18446744065119617025
	// which is close to MaxUint64 (18446744073709551615).
	// Multiplying by 1e9 would massively overflow uint64.
	result := epochPicoseconds(
		math.MaxUint32,
		math.MaxUint32,
	)

	// The result must be larger than MaxUint64
	maxU64 := new(big.Int).SetUint64(math.MaxUint64)
	assert.Equal(
		t,
		1,
		result.Cmp(maxU64),
		"result should exceed MaxUint64",
	)

	// Verify the exact value:
	// MaxUint32^2 * 1e9 =
	// 4294967295 * 4294967295 * 1000000000 =
	// 18446744065119617025000000000
	expected, ok := new(big.Int).SetString(
		"18446744065119617025000000000",
		10,
	)
	require.True(t, ok)
	assert.Equal(
		t,
		0,
		expected.Cmp(result),
		"exact overflow value mismatch",
	)
}

// TestQueryHardForkEraHistory_TransitionKnown proves that when TransitionInfo
// is set to TransitionKnown the era history response uses the transition
// epoch's StartSlot as the exact EraEnd, rather than the safe-zone cap.
//
// Setup:
//   - Two Conway epochs: epoch 500 (startSlot=100_000, length=432_000) and
//     epoch 501 (startSlot=532_000, length=432_000) — epoch 501 is the
//     transition epoch (stored with the old era's EraId).
//   - transitionInfo = {State: TransitionKnown, KnownEpoch: 501}
//   - ledgerTip at slot 200_000 (well inside epoch 500)
//
// Expected EraEnd slot: 532_000 (epoch 501's StartSlot — the exact boundary)
// Without TransitionKnown: EraEnd would be 200_000 + 25_920 = 225_920
func TestQueryHardForkEraHistory_TransitionKnown(t *testing.T) {
	t.Parallel()

	const (
		tipSlot       = uint64(200_000)
		epoch500Start = uint64(100_000)
		epoch501Start = uint64(532_000)
		epochLen      = uint(432_000)
		slotLenMs     = uint(1_000)
		epoch500Id    = uint64(500)
		epoch501Id    = uint64(501)
	)

	db := newTestDB(t)
	// Epoch 500: the active epoch in the old era
	require.NoError(t, db.SetEpoch(
		epoch500Start, epoch500Id,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, slotLenMs, epochLen,
		nil,
	))
	// Epoch 501: the transition epoch, still stored with Conway era's EraId
	require.NoError(t, db.SetEpoch(
		epoch501Start, epoch501Id,
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
		transitionInfo: hardfork.NewTransitionKnown(epoch501Id),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.publishSnapshotsLocked()
	result, err := ls.queryHardForkEraHistory(nil)
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)
	require.NotEmpty(t, eraList)

	// The last entry in the list is the Conway (current, open) era.
	lastEra, ok := eraList[len(eraList)-1].([]any)
	require.True(t, ok, "era entry should be []any")
	require.Len(t, lastEra, 3, "era entry should be [start, end, params]")

	eraEnd, ok := lastEra[1].([]any)
	require.True(t, ok, "EraEnd should be []any")
	require.Len(t, eraEnd, 3, "EraEnd should be [relTime, slot, epoch]")

	actualSlot, ok := eraEnd[1].(uint64)
	require.True(t, ok, "EraEnd slot should be uint64")
	assert.Equal(
		t,
		epoch501Start,
		actualSlot,
		"TransitionKnown: EraEnd slot should be transition epoch's StartSlot (%d), not safe-zone cap",
		epoch501Start,
	)

	// Verify EraEnd epoch number is the transition epoch ID
	actualEpoch, ok := eraEnd[2].(uint64)
	require.True(t, ok, "EraEnd epoch should be uint64")
	assert.Equal(t, epoch501Id, actualEpoch,
		"TransitionKnown: EraEnd epoch should be the transition epoch ID",
	)
}

// TestQueryHardForkEraHistory_TransitionKnown_MissingEpochFallsBackToSafeZone
// verifies that TransitionKnown with a KnownEpoch absent from the DB falls back
// to the safe-zone path, which snaps to the epoch-end boundary.
//
// Setup: one Conway epoch (500), transitionInfo.KnownEpoch = 999 (not in DB).
// Expected: falls back to epoch-end snap (532_000), epoch number 501.
func TestQueryHardForkEraHistory_TransitionKnown_MissingEpochFallsBackToSafeZone(
	t *testing.T,
) {
	t.Parallel()

	const (
		tipSlot        = uint64(200_000)
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
		missingEpoch   = uint64(999) // deliberately absent from DB
	)
	const expectedSafeZone = uint64(25_920)
	expectedEraEndSlot := epochStartSlot + uint64(
		epochLen,
	) // 532_000 (epoch end)

	db := newTestDB(t)
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
		transitionInfo: hardfork.NewTransitionKnown(missingEpoch),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.publishSnapshotsLocked()
	result, err := ls.queryHardForkEraHistory(nil)
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)
	require.NotEmpty(t, eraList)

	lastEra, ok := eraList[len(eraList)-1].([]any)
	require.True(t, ok, "era entry should be []any")
	eraEnd, ok := lastEra[1].([]any)
	require.True(t, ok, "EraEnd should be []any")
	actualSlot, ok := eraEnd[1].(uint64)
	require.True(t, ok, "EraEnd slot should be uint64")

	actualEpoch, ok := eraEnd[2].(uint64)
	require.True(t, ok, "EraEnd epoch should be uint64")

	assert.Equal(
		t,
		expectedEraEndSlot,
		actualSlot,
		"TransitionKnown with missing KnownEpoch must fall back to epoch-end snap (%d)",
		expectedEraEndSlot,
	)
	assert.Equal(t, epochId+1, actualEpoch,
		"EraEnd epoch should be epochId+1 (%d)", epochId+1,
	)
}

// TestQueryHardForkEraHistory_TransitionUnknown_FallsBackToSafeZone confirms
// that TransitionUnknown snaps to the epoch-end boundary (not the raw
// safeEndSlot), matching Haskell's slotToEpochBound behaviour.
func TestQueryHardForkEraHistory_TransitionUnknown_FallsBackToSafeZone(
	t *testing.T,
) {
	t.Parallel()

	const (
		tipSlot        = uint64(200_000)
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
	)
	// safeZone = 25_920; safeEndSlot = 225_920 which is inside the epoch (< 532_000)
	const expectedSafeZone = uint64(25_920)
	expectedEraEndSlot := epochStartSlot + uint64(epochLen) // 532_000

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
	result, err := ls.queryHardForkEraHistory(nil)
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)
	require.NotEmpty(t, eraList)

	lastEra, ok := eraList[len(eraList)-1].([]any)
	require.True(t, ok)
	eraEnd, ok := lastEra[1].([]any)
	require.True(t, ok)
	actualSlot, ok := eraEnd[1].(uint64)
	require.True(t, ok)
	actualEpoch, ok := eraEnd[2].(uint64)
	require.True(t, ok)
	assert.Equal(
		t,
		expectedEraEndSlot,
		actualSlot,
		"TransitionUnknown: EraEnd slot should snap to epoch end (%d), not mid-epoch safeEndSlot (%d)",
		expectedEraEndSlot,
		tipSlot+expectedSafeZone,
	)
	assert.Equal(t, epochId+1, actualEpoch,
		"TransitionUnknown: EraEnd epoch should be epochId+1 (%d)", epochId+1,
	)
}

// TestQueryHardForkEraHistory_TransitionImpossible_ServesEpochEnd verifies
// that when TransitionImpossible is set, queryHardForkEraHistory returns the
// full epoch-end slot rather than a safe-zone cap.
//
// Setup (mirrors TestQueryHardForkEraHistory_OpenEraEndBoundedBySafeZone):
//   - Conway epoch 500: startSlot=100_000, length=432_000 (ends at 532_000)
//   - tipSlot past the safe-zone boundary (safeEnd >= epochEnd)
//   - transitionInfo = TransitionImpossible
//
// Expected EraEnd slot: 532_000 (confirmed epoch end, no cap)
func TestQueryHardForkEraHistory_TransitionImpossible_ServesEpochEnd(
	t *testing.T,
) {
	t.Parallel()

	const (
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		epochEndSlot   = uint64(532_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
		// tipSlot well past the safe-zone decision point
		tipSlot = uint64(520_000)
	)

	db := newTestDB(t)
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
		transitionInfo: hardfork.NewTransitionImpossible(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.publishSnapshotsLocked()
	result, err := ls.queryHardForkEraHistory(nil)
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)
	require.NotEmpty(t, eraList)

	lastEra, ok := eraList[len(eraList)-1].([]any)
	require.True(t, ok, "era entry should be []any")
	eraEnd, ok := lastEra[1].([]any)
	require.True(t, ok, "EraEnd should be []any")
	actualSlot, ok := eraEnd[1].(uint64)
	require.True(t, ok, "EraEnd slot should be uint64")

	assert.Equal(
		t,
		epochEndSlot,
		actualSlot,
		"TransitionImpossible: EraEnd slot should be the confirmed epoch end (%d), not a safeZone cap",
		epochEndSlot,
	)
}

// TestQueryHardForkEraHistory_TransitionImpossible_EpochNumberIsNextEpoch
// verifies that the EraEnd epoch number is epochId+1 when TransitionImpossible
// is set (the epoch-loop sets tmpEnd with epochId+1 for the last epoch).
func TestQueryHardForkEraHistory_TransitionImpossible_EpochNumberIsNextEpoch(
	t *testing.T,
) {
	t.Parallel()

	const (
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
		tipSlot        = uint64(520_000)
	)

	db := newTestDB(t)
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
		transitionInfo: hardfork.NewTransitionImpossible(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.publishSnapshotsLocked()
	result, err := ls.queryHardForkEraHistory(nil)
	require.NoError(t, err)

	eraList := result.(cbor.IndefLengthList)
	lastEra := eraList[len(eraList)-1].([]any)
	eraEnd := lastEra[1].([]any)

	actualEpoch, ok := eraEnd[2].(uint64)
	require.True(t, ok, "EraEnd epoch should be uint64")
	assert.Equal(
		t,
		epochId+1,
		actualEpoch,
		"TransitionImpossible: EraEnd epoch should be epochId+1 (%d)",
		epochId+1,
	)
}

// TestQueryHardForkEraHistory_TransitionImpossible_vs_Unknown_Comparison
// confirms that TransitionImpossible and TransitionUnknown converge on the
// same epoch-end boundary when tipSlot + safeZone still lies within the
// current epoch — the common steady-state early-in-epoch case.
//
// Divergence at late-in-epoch tips (tip + safeZone crossing into the next
// epoch) is covered by TransitionUnknown_FallsBackToSafeZone and matches
// Haskell HFC's slotToEpochBound semantics.
func TestQueryHardForkEraHistory_TransitionImpossible_vs_Unknown_Comparison(
	t *testing.T,
) {
	t.Parallel()

	const (
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
		// tipSlot well inside the epoch so tip + safeZone (25_920) stays in
		// the same epoch — both states snap to the same epoch-end boundary.
		tipSlot = uint64(200_000)
	)

	setupLS := func(state hardfork.TransitionState) *LedgerState {
		db := newTestDB(t)
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
			transitionInfo: hardfork.TransitionInfo{State: state},
			config: LedgerStateConfig{
				CardanoNodeConfig: newTestEraHistoryCfg(t),
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}
		ls.publishSnapshotsLocked()
		return ls
	}

	eraEndSlot := func(ls *LedgerState) uint64 {
		result, err := ls.queryHardForkEraHistory(nil)
		require.NoError(t, err)
		eraList := result.(cbor.IndefLengthList)
		lastEra := eraList[len(eraList)-1].([]any)
		eraEnd := lastEra[1].([]any)
		slot, ok := eraEnd[1].(uint64)
		require.True(t, ok)
		return slot
	}

	impossibleSlot := eraEndSlot(setupLS(hardfork.TransitionImpossible))
	unknownSlot := eraEndSlot(setupLS(hardfork.TransitionUnknown))

	assert.Equal(t, uint64(532_000), impossibleSlot,
		"TransitionImpossible must serve the epoch end")
	assert.Equal(
		t,
		uint64(532_000),
		unknownSlot,
		"TransitionUnknown snaps to epoch end when tip+safeZone stays in the same epoch",
	)
	assert.Equal(t, impossibleSlot, unknownSlot,
		"both states return the same epoch-end slot")
}

func TestCheckedSlotAdd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		startSlot uint64
		length    uint64
		expected  uint64
		expectErr bool
	}{
		{
			name:      "normal addition",
			startSlot: 100,
			length:    200,
			expected:  300,
		},
		{
			name:      "zero plus zero",
			startSlot: 0,
			length:    0,
			expected:  0,
		},
		{
			name:      "zero plus value",
			startSlot: 0,
			length:    1000,
			expected:  1000,
		},
		{
			name:      "max minus one plus one",
			startSlot: math.MaxUint64 - 1,
			length:    1,
			expected:  math.MaxUint64,
		},
		{
			name:      "max plus zero",
			startSlot: math.MaxUint64,
			length:    0,
			expected:  math.MaxUint64,
		},
		{
			name:      "overflow max plus one",
			startSlot: math.MaxUint64,
			length:    1,
			expectErr: true,
		},
		{
			name:      "overflow large values",
			startSlot: math.MaxUint64 / 2,
			length:    math.MaxUint64/2 + 2,
			expectErr: true,
		},
		{
			name:      "realistic shelley epoch end",
			startSlot: 86400000,
			length:    432000,
			expected:  86832000,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := checkedSlotAdd(
				tc.startSlot,
				tc.length,
			)
			if tc.expectErr {
				require.Error(t, err)
				assert.Contains(
					t,
					err.Error(),
					"era history overflow",
				)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestReconstructTransitionInfo verifies that reconstructTransitionInfo sets
// TransitionKnown when the loaded pparams carry a protocol version that maps
// to a later era than the current epoch's stored EraId — i.e., the node was
// stopped in the window between an epoch-rollover version bump and the first
// block of the new era.
func TestReconstructTransitionInfo(t *testing.T) {
	t.Parallel()

	babbageEra := eras.GetEraById(eras.BabbageEraDesc.Id)
	require.NotNil(t, babbageEra)
	conwayEra := eras.GetEraById(eras.ConwayEraDesc.Id)
	require.NotNil(t, conwayEra)
	tests := []struct {
		name           string
		currentEra     eras.EraDesc
		currentEpoch   models.Epoch
		currentPParams lcommon.ProtocolParameters
		expectedState  hardfork.TransitionState
		expectedEpoch  uint64
	}{
		{
			// Babbage pparams with Conway major version (9): TransitionKnown.
			// This is the pre-Conway restart window scenario.
			name:       "babbage era pparams with conway version → TransitionKnown",
			currentEra: *babbageEra,
			currentEpoch: models.Epoch{
				EpochId: 500,
				EraId:   eras.BabbageEraDesc.Id,
			},
			currentPParams: &babbage.BabbageProtocolParameters{
				ProtocolMajor: 9, // Conway major version, stored under Babbage era
			},
			expectedState: hardfork.TransitionKnown,
			expectedEpoch: 500,
		},
		{
			// Babbage pparams with normal Babbage version: no transition.
			name:       "babbage era pparams with babbage version → TransitionUnknown",
			currentEra: *babbageEra,
			currentEpoch: models.Epoch{
				EpochId: 499,
				EraId:   eras.BabbageEraDesc.Id,
			},
			currentPParams: &babbage.BabbageProtocolParameters{
				ProtocolMajor: 8,
			},
			expectedState: hardfork.TransitionUnknown,
		},
		{
			// Conway pparams in Conway era: pparamsEra == currentEra, no transition.
			name:       "conway era pparams with conway version → TransitionUnknown",
			currentEra: *conwayEra,
			currentEpoch: models.Epoch{
				EpochId: 600,
				EraId:   eras.ConwayEraDesc.Id,
			},
			currentPParams: &conway.ConwayProtocolParameters{
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: 9,
				},
			},
			expectedState: hardfork.TransitionUnknown,
		},
		{
			// Nil pparams: must not panic, leave TransitionUnknown.
			name:       "nil pparams → TransitionUnknown",
			currentEra: *babbageEra,
			currentEpoch: models.Epoch{
				EpochId: 400,
				EraId:   eras.BabbageEraDesc.Id,
			},
			currentPParams: nil,
			expectedState:  hardfork.TransitionUnknown,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ls := &LedgerState{
				currentEra:     tc.currentEra,
				currentEpoch:   tc.currentEpoch,
				currentPParams: tc.currentPParams,
				// transitionInfo starts at zero value (TransitionUnknown)
			}
			ls.reconstructTransitionInfo()

			assert.Equal(t, tc.expectedState, ls.transitionInfo.State)
			if tc.expectedState == hardfork.TransitionKnown {
				assert.Equal(t, tc.expectedEpoch, ls.transitionInfo.KnownEpoch)
			}
		})
	}
}

// TestQueryHardForkEraHistory_PastEra_NormalEpochEnd verifies that a closed
// past era whose last epoch carries the same-era protocol version produces an
// EraEnd equal to lastEp.StartSlot + lastEp.LengthInSlots (the raw boundary),
// not some alternative.
//
// Setup:
//   - currentEra = Conway (era 9)
//   - Babbage epoch 499: startSlot=64_000_000, length=432_000 → raw end=64_432_000
//   - Babbage pparams for epoch 499: ProtocolMajor=8 (Babbage version)
//
// Expected Babbage EraEnd slot: 64_432_000
func TestQueryHardForkEraHistory_PastEra_NormalEpochEnd(t *testing.T) {
	t.Parallel()

	const (
		epochId    = uint64(499)
		epochStart = uint64(64_000_000)
		epochLen   = uint(432_000)
		slotLenMs  = uint(1_000)
		rawEraEnd  = epochStart + uint64(epochLen) // 64_432_000
	)

	db := newTestDB(t)
	// Store the Babbage epoch.
	require.NoError(t, db.SetEpoch(
		epochStart, epochId,
		nil, nil, nil, nil,
		eras.BabbageEraDesc.Id, slotLenMs, epochLen,
		nil,
	))
	// Store Babbage pparams for epoch 499 with Babbage major version (8).
	pp := &babbage.BabbageProtocolParameters{ProtocolMajor: 8}
	ppCbor, err := cbor.Encode(pp)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		ppCbor,
		epochStart, // slot
		epochId,
		eras.BabbageEraDesc.Id,
		nil,
	))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(epochStart+1, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.publishSnapshotsLocked()
	result, err := ls.queryHardForkEraHistory(nil)
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)

	// Find the Babbage entry (second-to-last before Conway).
	var babbageEraEnd []any
	for _, entry := range eraList {
		eraEntry, ok := entry.([]any)
		if !ok || len(eraEntry) < 3 {
			continue
		}
		end, ok := eraEntry[1].([]any)
		if !ok || len(end) < 3 {
			continue
		}
		slot, ok := end[1].(uint64)
		if !ok {
			continue
		}
		// The Babbage era's EraEnd epoch number is epochId+1 in the normal case.
		epochNo, ok := end[2].(uint64)
		if !ok {
			continue
		}
		if epochNo == epochId+1 && slot == rawEraEnd {
			babbageEraEnd = end
			break
		}
	}
	require.NotNil(
		t,
		babbageEraEnd,
		"expected to find Babbage EraEnd with slot=%d, epochNo=%d in era history",
		rawEraEnd,
		epochId+1,
	)
	assert.Equal(
		t,
		rawEraEnd,
		babbageEraEnd[1].(uint64),
		"past era with normal pparams version: EraEnd slot should be raw boundary (%d)",
		rawEraEnd,
	)
}

// TestQueryHardForkEraHistory_PastEra_TransitionEpoch verifies that a closed
// past era whose last epoch carries the next-era protocol version produces an
// EraEnd equal to lastEp.StartSlot (the confirmed boundary), not the raw
// StartSlot+LengthInSlots which would overshoot.
//
// This is the SCENARIO 2 case: TransitionKnown was active, epoch K was created
// under OLD_ERA's EraId, but its pparams already show the new-era version.
// After the hard fork, epoch K ends up as the last epoch of OLD_ERA in the DB,
// but the actual era boundary is at epoch K's *start*, not its end.
//
// Setup:
//   - currentEra = Conway (era 9)
//   - Babbage epoch 499: startSlot=64_000_000, length=432_000 → raw end=64_432_000
//   - Babbage pparams for epoch 499: ProtocolMajor=9 (Conway version — transition epoch)
//
// Expected Babbage EraEnd slot: 64_000_000 (epoch 499's StartSlot)
func TestQueryHardForkEraHistory_PastEra_TransitionEpoch(t *testing.T) {
	t.Parallel()

	const (
		epochId    = uint64(499)
		epochStart = uint64(64_000_000)
		epochLen   = uint(432_000)
		slotLenMs  = uint(1_000)
		rawEraEnd  = epochStart + uint64(epochLen) // 64_432_000 — wrong
		// correct boundary is epochStart because epoch 499 is a transition epoch
	)

	db := newTestDB(t)
	require.NoError(t, db.SetEpoch(
		epochStart, epochId,
		nil, nil, nil, nil,
		eras.BabbageEraDesc.Id, slotLenMs, epochLen,
		nil,
	))
	// Pparams carry Conway major version (9) — this is a transition epoch.
	pp := &babbage.BabbageProtocolParameters{ProtocolMajor: 9}
	ppCbor, err := cbor.Encode(pp)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		ppCbor,
		epochStart,
		epochId,
		eras.BabbageEraDesc.Id,
		nil,
	))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(epochStart+1, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.publishSnapshotsLocked()
	result, err := ls.queryHardForkEraHistory(nil)
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)

	// Find the Babbage entry: for a transition epoch, EraEnd slot = epochStart
	// and EraEnd epochNo = epochId (not epochId+1).
	var babbageEraEnd []any
	for _, entry := range eraList {
		eraEntry, ok := entry.([]any)
		if !ok || len(eraEntry) < 3 {
			continue
		}
		end, ok := eraEntry[1].([]any)
		if !ok || len(end) < 3 {
			continue
		}
		slot, ok := end[1].(uint64)
		if !ok {
			continue
		}
		epochNo, ok := end[2].(uint64)
		if !ok {
			continue
		}
		// Both the raw boundary and the corrected boundary are non-zero;
		// distinguish by era params or just search for the corrected slot.
		if slot == epochStart && epochNo == epochId {
			babbageEraEnd = end
			break
		}
	}

	require.NotNil(t, babbageEraEnd,
		"expected Babbage EraEnd with slot=%d epochNo=%d; "+
			"got raw boundary %d instead — transition epoch not detected",
		epochStart, epochId, rawEraEnd,
	)
	assert.Equal(t, epochStart, babbageEraEnd[1].(uint64),
		"past era with transition-epoch pparams: EraEnd slot should be "+
			"confirmed boundary (%d = lastEp.StartSlot), not raw end (%d)",
		epochStart, rawEraEnd,
	)
	assert.Equal(t, epochId, babbageEraEnd[2].(uint64),
		"past era with transition-epoch pparams: EraEnd epochNo should be "+
			"the transition epoch's own ID (%d), not next epoch (%d)",
		epochId, epochId+1,
	)
	// Sanity: the raw boundary must NOT appear as the EraEnd slot.
	assert.NotEqual(
		t,
		rawEraEnd,
		babbageEraEnd[1].(uint64),
		"raw EraEnd slot (%d) must not be used for a transition epoch",
		rawEraEnd,
	)
}

// TestQueryHardForkEraHistory_PastEra_TransitionEpoch_Contiguity verifies that
// when a past era ends on a transition epoch the rolled-back timespan produces
// contiguous era boundaries: the closed era's EraEnd.relTime must equal the
// open era's EraStart.relTime.
//
// Without the timespan roll-back fix, there would be a gap of
// epochPicoseconds(lastEp) between the two, because timespan was not decremented
// after correcting tmpEnd.
//
// Setup:
//   - currentEra = Conway (era 9)
//   - Babbage epoch 499: startSlot=64_000_000, slotLen=1_000ms, length=432_000
//     → picoseconds = 1_000 * 432_000 * 1e9 = 432_000_000_000_000_000
//     → raw end slot = 64_432_000  (transition epoch — pparams ProtocolMajor=9)
//   - Conway epoch 500: startSlot=64_000_000 (same as Babbage 499's StartSlot),
//     slotLen=1_000ms, length=432_000
//
// Expected: babbageEraEnd.relTime == conwayEraStart.relTime
func TestQueryHardForkEraHistory_PastEra_TransitionEpoch_Contiguity(
	t *testing.T,
) {
	t.Parallel()

	const (
		babbageEpochId    = uint64(499)
		babbageEpochStart = uint64(64_000_000)
		babbageEpochLen   = uint(432_000)
		slotLenMs         = uint(1_000)
		conwayEpochId     = uint64(500)
		// The Conway epoch starts at the Babbage transition epoch's StartSlot
		// (the confirmed era boundary).
		conwayEpochStart = babbageEpochStart
		conwayEpochLen   = uint(432_000)
	)

	db := newTestDB(t)
	// Babbage transition epoch (pparams carry Conway major version).
	require.NoError(t, db.SetEpoch(
		babbageEpochStart, babbageEpochId,
		nil, nil, nil, nil,
		eras.BabbageEraDesc.Id, slotLenMs, babbageEpochLen,
		nil,
	))
	pp := &babbage.BabbageProtocolParameters{ProtocolMajor: 9}
	ppCbor, err := cbor.Encode(pp)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		ppCbor,
		babbageEpochStart,
		babbageEpochId,
		eras.BabbageEraDesc.Id,
		nil,
	))
	// Conway epoch starts at the confirmed boundary.
	require.NoError(t, db.SetEpoch(
		conwayEpochStart, conwayEpochId,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, slotLenMs, conwayEpochLen,
		nil,
	))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(conwayEpochStart+1, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.publishSnapshotsLocked()
	result, err := ls.queryHardForkEraHistory(nil)
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)

	// Extract relTime (big.Int) from an era's start or end tuple.
	relTime := func(tuple []any) *big.Int {
		require.GreaterOrEqual(t, len(tuple), 1)
		v, ok := tuple[0].(*big.Int)
		require.True(t, ok, "relTime should be *big.Int, got %T", tuple[0])
		return v
	}

	// Locate Babbage and Conway entries in the list by matching their EraEnd
	// epoch numbers.
	var babbageEnd, conwayStart []any
	for _, entry := range eraList {
		eraEntry, ok := entry.([]any)
		if !ok || len(eraEntry) < 3 {
			continue
		}
		start, ok := eraEntry[0].([]any)
		if !ok {
			continue
		}
		end, ok := eraEntry[1].([]any)
		if !ok {
			continue
		}
		if len(end) < 3 {
			continue
		}
		endEpoch, ok := end[2].(uint64)
		if !ok {
			continue
		}
		// Babbage's corrected EraEnd has epochNo = babbageEpochId.
		if endEpoch == babbageEpochId {
			babbageEnd = end
		}
		// Conway's EraStart has epochNo = conwayEpochId.
		if len(start) >= 3 {
			startEpoch, ok := start[2].(uint64)
			if ok && startEpoch == conwayEpochId {
				conwayStart = start
			}
		}
	}

	require.NotNil(
		t,
		babbageEnd,
		"Babbage EraEnd with epochNo=%d not found in era history",
		babbageEpochId,
	)
	require.NotNil(
		t,
		conwayStart,
		"Conway EraStart with epochNo=%d not found in era history",
		conwayEpochId,
	)

	babbageEndTime := relTime(babbageEnd)
	conwayStartTime := relTime(conwayStart)

	assert.Equal(
		t,
		0,
		babbageEndTime.Cmp(conwayStartTime),
		"era boundaries must be contiguous: Babbage EraEnd.relTime (%s) != Conway EraStart.relTime (%s); "+
			"timespan was not rolled back after correcting the transition-epoch EraEnd",
		babbageEndTime.String(),
		conwayStartTime.String(),
	)
}

// TestQueryChainBlockNoAtGenesis verifies origin is encoded as WithOrigin [0].
func TestQueryChainBlockNoAtGenesis(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	ls.publishSnapshotsLocked()
	result, err := ls.queryChainBlockNo(QueryPoint{}, nil)
	assert.NoError(t, err)
	// WithOrigin at genesis: [0]
	assert.Equal(t, []any{0}, result)
}

// TestQueryChainBlockNoAtBlock verifies a non-origin tip is encoded as [1, blockNo].
func TestQueryChainBlockNoAtBlock(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.Point{
			Hash: []byte("tip-hash"),
		},
		BlockNumber: 12345,
	}
	ls.publishSnapshotsLocked()
	result, err := ls.queryChainBlockNo(QueryPoint{}, nil)
	assert.NoError(t, err)
	// WithOrigin at block: [1, blockNo]
	assert.Equal(t, []any{1, uint64(12345)}, result)
}

// TestQueryChainBlockNoAtFirstBlock verifies BlockNo 0 is not treated as origin.
func TestQueryChainBlockNoAtFirstBlock(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.Point{
			Hash: []byte("first-block-hash"),
		},
		BlockNumber: 0,
	}
	ls.publishSnapshotsLocked()
	result, err := ls.queryChainBlockNo(QueryPoint{}, nil)
	assert.NoError(t, err)
	// Cardano block numbers are 0-indexed, so block 0 is not origin.
	assert.Equal(t, []any{1, uint64(0)}, result)
}

// protocolParamsQuery wraps GetCurrentProtocolParams the way the wire
// delivers it, matching poolDistr2Query/stakeDistributionQuery in the
// neighboring query test files.
func protocolParamsQuery() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyCurrentProtocolParamsQuery{},
		},
	}
}

// conwayPParamsWithCostModels builds a Conway pparams value with every
// cbor.Rat-bearing field populated, not just CostModels --
// a fixture that only sets CostModels type-asserts fine
// but is not actually encodable, since cbor.Rat.MarshalCBOR panics on the nil
// *big.Rat a zero-value cbor.Rat (or a nil *cbor.Rat pointer field) carries,
// and PoolVotingThresholds/DRepVotingThresholds's value-typed cbor.Rat fields
// are always encoded (never skippable as CBOR null the way a nil *cbor.Rat
// pointer field is). This is what real cardano-node protocol-parameter data
// always has populated, so an end-to-end wire test should encode a value
// shaped like the real thing, not a partial struct that happens to satisfy a
// type assertion.
func conwayPParamsWithCostModels(
	costModels map[uint][]int64,
) *conway.ConwayProtocolParameters {
	rat := func(n, d int64) cbor.Rat { return cbor.Rat{Rat: big.NewRat(n, d)} }
	ratPtr := func(n, d int64) *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(n, d)} }
	return &conway.ConwayProtocolParameters{
		CostModels:                 costModels,
		A0:                         ratPtr(3, 10),
		Rho:                        ratPtr(3, 1000),
		Tau:                        ratPtr(1, 5),
		MinFeeRefScriptCostPerByte: ratPtr(15, 1),
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  ratPtr(577, 10000),
			StepPrice: ratPtr(721, 10000000),
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    rat(51, 100),
			CommitteeNormal:       rat(51, 100),
			CommitteeNoConfidence: rat(51, 100),
			HardForkInitiation:    rat(51, 100),
			PpSecurityGroup:       rat(51, 100),
		},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    rat(67, 100),
			CommitteeNormal:       rat(67, 100),
			CommitteeNoConfidence: rat(60, 100),
			UpdateToConstitution:  rat(75, 100),
			HardForkInitiation:    rat(60, 100),
			PpNetworkGroup:        rat(67, 100),
			PpEconomicGroup:       rat(67, 100),
			PpTechnicalGroup:      rat(67, 100),
			PpGovGroup:            rat(75, 100),
			TreasuryWithdrawal:    rat(67, 100),
		},
	}
}

// TestQueryShelleyCurrentProtocolParams_OmitsSyntheticV2CostModel is the
// end-to-end regression test: confirmed against
// a real cardano-node's raw wire bytes (captured via a temporary diagnostic,
// decoded with the real client-side type, independent of any display-layer
// bug) that on a chain which has never received a real PlutusV2
// cost-model update, a real cardano-node's GetCurrentProtocolParams reply
// has no PlutusV2 entry at all -- while Dingo's internal state always
// carries HardForkBabbage's fabricated one, needed for real script
// validation. The LocalStateQuery reply must match the real node's
// observable behavior; internal validation must not be affected.
func TestQueryShelleyCurrentProtocolParams_OmitsSyntheticV2CostModel(
	t *testing.T,
) {
	t.Parallel()

	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(map[uint][]int64{
		0: {1, 1, 1},
		1: eras.DefaultPlutusV2CostModel,
		2: {3, 3, 3},
	})
	ls.syntheticV2CostModel = true
	ls.publishSnapshotsLocked()

	result, err := ls.Query(protocolParamsQuery(), QueryPoint{})
	require.NoError(t, err)

	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	pp, ok := arr[0].(*conway.ConwayProtocolParameters)
	require.True(t, ok)

	assert.NotContains(t, pp.CostModels, uint(1),
		"the reply must omit the synthetic PlutusV2 cost model")
	assert.Contains(t, pp.CostModels, uint(0))
	assert.Contains(t, pp.CostModels, uint(2))

	// This is a wire-level regression test, not just a type-assertion check:
	// encode what the reply actually contains and decode it back with the
	// real client-side type, matching the raw-CBOR verification this issue's
	// original diagnosis relied on independent of any display-layer bug.
	encoded, err := cbor.Encode(pp)
	require.NoError(t, err)
	var decoded conway.ConwayProtocolParameters
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err)
	assert.NotContains(
		t,
		decoded.CostModels,
		uint(1),
		"the encoded wire bytes must not carry the synthetic PlutusV2 cost model",
	)

	// Internal validation state must be completely unaffected by the query.
	internal, ok := ls.currentPParams.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.Contains(t, internal.CostModels, uint(1),
		"internal state must keep the default for real script validation")
}

// TestQueryShelleyCurrentProtocolParams_IncludesRealV2CostModel covers the
// other half: once real governance data has cleared the synthetic marker
// (LedgerState.syntheticV2CostModel == false), the reply must include
// whatever is actually in CostModels -- including a value that happens to
// equal the known synthetic default, since real governance re-affirming
// that exact value is still real data, not still a guess.
func TestQueryShelleyCurrentProtocolParams_IncludesRealV2CostModel(
	t *testing.T,
) {
	t.Parallel()

	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(map[uint][]int64{
		0: {1, 1, 1},
		1: eras.DefaultPlutusV2CostModel,
		2: {3, 3, 3},
	})
	ls.syntheticV2CostModel = false
	ls.publishSnapshotsLocked()

	result, err := ls.Query(protocolParamsQuery(), QueryPoint{})
	require.NoError(t, err)

	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	pp, ok := arr[0].(*conway.ConwayProtocolParameters)
	require.True(t, ok)

	assert.Contains(t, pp.CostModels, uint(1))
	assert.Equal(t, eras.DefaultPlutusV2CostModel, pp.CostModels[1])

	encoded, err := cbor.Encode(pp)
	require.NoError(t, err)
	var decoded conway.ConwayProtocolParameters
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err)
	assert.Equal(
		t,
		eras.DefaultPlutusV2CostModel,
		decoded.CostModels[1],
		"real data equal to the known default must still round-trip on the wire",
	)
}

// cardanoNodeConfigWithMaxLovelaceSupply builds a *cardano.CardanoNodeConfig
// whose ShelleyGenesis().MaxLovelaceSupply is nonzero -- the exact condition
// circulatingSupplyGenesis (ledger/queries.go) gates
// verifyStakeDistributionRetentionOnly's network_state floor on. A fixture
// with no CardanoNodeConfig at all leaves that floor inactive, so a case
// built without this helper can pass for the wrong reason: pinning
// over-rejection when the gate it means to test was never active, not the
// real requirement.
func cardanoNodeConfigWithMaxLovelaceSupply(t *testing.T, maxLovelaceSupply uint64) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(
		fmt.Sprintf(`{"maxLovelaceSupply": %d}`, maxLovelaceSupply),
	)))
	return cfg
}

// TestVerifyPointQueryable_WithinAllFloors_Accepted covers the accept
// direction: a point inside every point-aware query type's own retention
// floor -- on chain, within the UTxO/stake/pparams/era windows -- must be
// accepted so a well-behaved client's Acquire actually succeeds, not just
// so a stale one is rejected.
func TestVerifyPointQueryable_WithinAllFloors_Accepted(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	// Activates verifyStakeDistributionRetentionOnly's network_state floor
	// (circulatingSupplyGenesis) -- see cardanoNodeConfigWithMaxLovelaceSupply's
	// doc comment for why this fixture must set it to genuinely prove the
	// accept direction, not just the case where the floor never runs at all.
	ls.config.CardanoNodeConfig = cardanoNodeConfigWithMaxLovelaceSupply(t, 45_000_000_000_000_000)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 3}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3})
	// verifyStakeDistributionRetentionOnly's second floor requires a
	// network_state row at or before the pinned slot, matching what
	// PoolStakeDistribution's own totalCirculatingSupply call separately
	// requires -- without this row, a point can be inside the epoch-based
	// retention window and still get rejected.
	require.NoError(t, db.Metadata().SetNetworkState(0, 1_000, 300, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, hash),
	}, nil))

	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.NoError(t, err)
}

// TestVerifyPointQueryable_PastRetentionFloor_Rejected covers the reject
// direction: a point on-chain but past the stake-snapshot retention floor
// must fail with ErrHistoricalStateUnavailable, the sentinel
// localstatequeryServerAcquire maps to a clean wire-level
// AcquireFailurePointTooOld -- exactly mirroring
// TestPoolStakeDistribution_AsOfSlot_TooOldRejected's scenario, but through
// VerifyPointQueryable (which checks verifyPointOnChain first, unlike a
// bare PoolStakeDistribution call) to prove the whole upfront check
// rejects it, not just the one retention check it happens to hit first.
func TestVerifyPointQueryable_PastRetentionFloor_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 10}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3, 1000: 10})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1050, repeatedBytes(32, 0x0C)),
	}, nil))

	// Epoch 3 is 7 epochs behind the live epoch (10) -- outside the
	// 3-epoch stake-snapshot retention window.
	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestVerifyPointQueryable_APIStorageMode_PastRetentionFloor_Accepted covers
// checkAsOfEpochRecency's apiStorageMode early-return branch (ledger/pool_stake_distribution.go):
// pool-stake snapshots are never pruned when the database runs in API
// storage mode, so a point whose mark-snapshot epoch would be rejected under
// the default (core) retention window must still be accepted here, matching
// cleanupOldSnapshots' own API-mode carve-out that this check mirrors.
//
// Same shape as TestVerifyPointQueryable_PastRetentionFloor_Rejected -- an
// epoch 3 pinned point 7 epochs behind live epoch 10, well outside the
// 3-epoch stake-snapshot retention window -- except the database is opened
// in API storage mode instead of the default core mode, and epoch 3 carries
// its own persisted epoch row and pparams row (so the historical-epoch reads
// further down VerifyPointQueryable, which that rejected-in-core-mode test
// never reaches, succeed here on their own merits rather than accidentally
// masking the check under test). That test proves core mode must reject
// this point; this test proves API mode must accept the identical point
// instead. No-opping the apiStorageMode branch in checkAsOfEpochRecency
// (i.e. falling through to the pruning-window check regardless of storage
// mode) makes this test fail with ErrHistoricalStateUnavailable instead of
// the required nil.
func TestVerifyPointQueryable_APIStorageMode_PastRetentionFloor_Accepted(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeAPI)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {9, 9, 9}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 10}
	ls.publishSnapshotsLocked()

	conwayEraId := uint(eras.ConwayEraDesc.Id)
	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	require.NoError(t, ls.db.SetEpoch(
		300, 3, nil, nil, nil, nil, conwayEraId, 1, 100, nil,
	))
	require.NoError(t, ls.db.SetEpoch(
		1000, 10, nil, nil, nil, nil, conwayEraId, 1, 100, nil,
	))
	historicalPParams := conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	historicalCbor, err := cbor.Encode(historicalPParams)
	require.NoError(t, err)
	require.NoError(t, ls.db.SetPParams(
		historicalCbor, 300, 3, conwayEraId, nil,
	))
	require.NoError(t, db.Metadata().SetNetworkState(0, 0, 0, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1050, repeatedBytes(32, 0x0C)),
	}, nil))

	// Epoch 3 is 7 epochs behind the live epoch (10) -- outside the
	// 3-epoch stake-snapshot retention window that applies in core storage
	// mode (see TestVerifyPointQueryable_PastRetentionFloor_Rejected). In
	// API storage mode, pool-stake snapshots are never pruned, so this must
	// be accepted instead.
	verifyErr := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.NoError(t, verifyErr)
}

// TestVerifyPointQueryable_NoNetworkStateRow_Rejected covers a regression:
// checking only checkAsOfEpochRecency's mark-snapshot floor is not enough,
// since PoolStakeDistribution's own totalCirculatingSupply call separately
// requires a network_state row at or before the pinned slot (asOfSlot,
// non-nil for a pinned point) and rejects with ErrHistoricalStateUnavailable
// when missing. Before this second floor was added, VerifyPointQueryable
// accepted this exact point,
// and a client that then Acquired it and issued GetPoolDistr2 or
// GetStakeDistribution got that same error from a live query instead --
// handleQuery returns it bare, tearing the connection down, the identical
// failure this whole change exists to close at Acquire time instead.
//
// Identical to TestVerifyPointQueryable_WithinAllFloors_Accepted (pinned
// point's epoch equals the live epoch, so queryShelleyCurrentProtocolParams
// answers from the live snapshot rather than needing a historical pparams
// row, and CardanoNodeConfig is set so the network_state floor is actually
// active -- see cardanoNodeConfigWithMaxLovelaceSupply's doc comment; a
// fixture without it would pass here for the wrong reason, since the floor
// this test targets would never run at all) except for the one thing this
// test is about: no db.Metadata().SetNetworkState call, so no
// network_state row exists at any slot. Isolating every other floor this
// way means only the new check this test targets can be why this fails.
func TestVerifyPointQueryable_NoNetworkStateRow_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = cardanoNodeConfigWithMaxLovelaceSupply(t, 45_000_000_000_000_000)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 3}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, hash),
	}, nil))

	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestVerifyPointQueryable_NoNetworkStateRow_RejectedWithoutGenesis covers
// the case TestVerifyPointQueryable_NoNetworkStateRow_Rejected cannot: with
// CardanoNodeConfig nil, totalCirculatingSupply never reads network_state, so
// the stake-distribution floor is inactive, but GetAccountState still needs
// a row at or before the point. The point must be refused at Acquire, and
// accepted once a row covers it.
func TestVerifyPointQueryable_NoNetworkStateRow_RejectedWithoutGenesis(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 3}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, hash),
	}, nil))
	at := QueryPoint{Slot: 350, Hash: hash}

	err := ls.VerifyPointQueryable(nil, at)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)

	require.NoError(t, db.Metadata().SetNetworkState(0, 0, 0, nil))
	require.NoError(t, ls.VerifyPointQueryable(nil, at))
}

// TestVerifyPointQueryable_UnknownEraId_Rejected covers a regression:
// VerifyPointQueryable's queryHardFork call (HardForkCurrentEraQuery) is
// the only one of its five checks that ever inspects an epoch row's era at
// all, so nothing else here
// would catch it being silently dropped. Both existing regression tests
// pass whether or not that call exists, because neither fixture gives it
// anything to reject on: WithinAllFloors_Accepted's epoch row names a real
// era, and PastRetentionFloor_Rejected is already rejected earlier by
// verifyStakeDistributionRetentionOnly.
//
// Identical to TestVerifyPointQueryable_WithinAllFloors_Accepted (every
// other floor passes cleanly: on chain, within the UTxO/stake/pparams
// windows, with a covering network_state row) except the epoch row itself
// names era 255, which eras.GetEraById cannot resolve. Deleting the
// queryHardFork call from VerifyPointQueryable would make this pass when it
// must fail.
func TestVerifyPointQueryable_UnknownEraId_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 3}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	require.NoError(t, ls.db.SetEpoch(
		300, 3, nil, nil, nil, nil, 255, 1, 100, nil,
	))
	require.NoError(t, db.Metadata().SetNetworkState(0, 1_000, 300, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, hash),
	}, nil))

	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestVerifyPointQueryable_PParamsRowOnly_Rejected covers a gap the other
// TestVerifyPointQueryable* cases leave open: deleting the
// checkUtxoRetentionWindow call, or this queryShelleyCurrentProtocolParams
// call, from VerifyPointQueryable leaves every existing
// TestVerifyPointQueryable* case green -- neither deletion
// changes PastRetentionFloor_Rejected's outcome, since the stake floor
// already rejects that fixture, and no case has a missing pparams row as its
// only failure. The pinned point's epoch (3) sits exactly at the
// stake-retention window's edge relative to the live epoch (5) -- mark
// snapshot epoch 2 equals the floor 5-3=2, so checkAsOfEpochRecency accepts
// it (the same boundary TestQueryShelleyUtxoByTxIn_RetentionWindow_AtFloor_Succeeds
// covers for the UTxO floor) -- and no CardanoNodeConfig means the
// network_state floor never runs, so only the missing persisted pparams row
// for epoch 3 can reject this point.
func TestVerifyPointQueryable_PParamsRowOnly_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 5}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3, 700: 5})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(750, repeatedBytes(32, 0x0C)),
	}, nil))

	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}
