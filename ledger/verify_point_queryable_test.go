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
	"fmt"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// cardanoNodeConfigWithMaxLovelaceSupply builds a *cardano.CardanoNodeConfig
// whose ShelleyGenesis().MaxLovelaceSupply is nonzero -- the exact condition
// circulatingSupplyGenesis (ledger/queries.go) gates
// verifyStakeDistributionRetentionOnly's network_state floor on. Without
// this, that floor is inactive and a fixture cannot actually exercise it
// either direction (human review, Chris Guiney, dingo#4319/#4320: a first
// version of TestVerifyPointQueryable_NoNetworkStateRow_Rejected used a
// fixture with no CardanoNodeConfig at all, so it happened to pass, but
// only because the gate it meant to test was never active -- pinning
// over-rejection, not the real requirement).
func cardanoNodeConfigWithMaxLovelaceSupply(t *testing.T, maxLovelaceSupply uint64) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(
		fmt.Sprintf(`{"maxLovelaceSupply": %d}`, maxLovelaceSupply),
	)))
	return cfg
}

// TestVerifyPointQueryable_WithinAllFloors_Accepted covers the accept
// direction (human review, Chris Guiney, dingo#4319/#4320: "VerifyPointQueryable
// has no reference in any _test.go in the repository... only the accept
// direction keeps working clients alive"): a point inside every
// point-aware query type's own retention floor -- on chain, within the
// UTxO/stake/pparams/era windows -- must be accepted so a well-behaved
// client's Acquire actually succeeds, not just so a stale one is rejected.
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
	// verifyStakeDistributionRetentionOnly's second floor (human review,
	// Chris Guiney, dingo#4319/#4320) requires a network_state row at or
	// before the pinned slot, matching what PoolStakeDistribution's own
	// totalCirculatingSupply call separately requires -- without this row,
	// a point can be inside the epoch-based retention window and still get
	// rejected.
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

// TestVerifyPointQueryable_NoNetworkStateRow_Rejected is the regression a
// human reviewer found (Chris Guiney, dingo#4319/#4320):
// verifyStakeDistributionRetentionOnly's first version checked only
// checkAsOfEpochRecency's mark-snapshot floor, but PoolStakeDistribution's
// own totalCirculatingSupply call separately requires a network_state row
// at or before the pinned slot (asOfSlot, non-nil for a pinned point) and
// rejects with ErrHistoricalStateUnavailable when missing. Before this
// second floor was added, VerifyPointQueryable accepted this exact point,
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

// TestVerifyPointQueryable_NoNetworkStateRow_AcceptedWhenFloorInactive is
// the regression a human reviewer found (Chris Guiney, dingo#4319/#4320):
// verifyStakeDistributionRetentionOnly's network_state floor was
// unconditional, so it rejected a point every real query would have
// answered whenever totalCirculatingSupply itself never reaches
// GetNetworkStateAsOfSlot -- no CardanoNodeConfig (as here, and as every
// other ledger test in this repository already constructs a LedgerState),
// no ShelleyGenesis, or a genesis with no MaxLovelaceSupply. Identical to
// TestVerifyPointQueryable_NoNetworkStateRow_Rejected (same missing row)
// except CardanoNodeConfig is left nil, so this one must accept where that
// one must reject -- proving the floor is genuinely conditional, not just
// present or absent.
func TestVerifyPointQueryable_NoNetworkStateRow_AcceptedWhenFloorInactive(
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

	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.NoError(t, err)
}

// TestVerifyPointQueryable_UnknownEraId_Rejected is the regression a human
// reviewer found (Chris Guiney, dingo#4319/#4320): VerifyPointQueryable's
// queryHardFork call (HardForkCurrentEraQuery) is the only one of its five
// checks that ever inspects an epoch row's era at all, so nothing else here
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
