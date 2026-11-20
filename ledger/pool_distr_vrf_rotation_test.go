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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A pool that rotates its VRF key after the electing snapshot was captured is
// still elected, and validated, on the snapshot's key until the rotation
// reaches a later snapshot. GetPoolDistr/GetPoolDistr2 answer from that same
// snapshot in cardano-ledger (individualPoolStakeVrf), so they must report
// the key the snapshot carries, not the pool's latest registration.
func TestPoolStakeDistribution_ReportsElectingVrfAfterMidEpochRotation(
	t *testing.T,
) {
	t.Parallel()
	db := newTestDB(t)

	oldVrf := repeatedBytes(32, 0xAA)
	newVrf := repeatedBytes(32, 0xBB)
	const snapshotEpoch = 0
	pkh := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(repeatedBytes(28, 0x11)),
	)
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{PoolKeyHash: pkh.Bytes(), VrfKeyHash: oldVrf},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  oldVrf,
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        snapshotEpoch,
			SnapshotType: snapshotTypeMark,
			PoolKeyHash:  pkh.Bytes(),
			VrfKeyHash:   oldVrf,
			TotalStake:   dbtypes.Uint64(1_000_000),
			CapturedSlot: 1,
		},
		nil,
	))
	// The re-registration lands after the capture.
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{PoolKeyHash: pkh.Bytes(), VrfKeyHash: newVrf},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  newVrf,
			AddedSlot:   5,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))
	ls := newPoolDistr2Ledger(t, db)

	electing, ok, err := ls.electingVrfKeyHashFromSnapshotWithContext(
		t.Context(), pkh, ls.epochCacheSnapshot(),
		ls.resolveElectingSnapshotAtSlot(
			1, 1, pkh, ls.epochCacheSnapshot(), nil,
		),
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, oldVrf, electing.Bytes(),
		"header validation holds the pool to the snapshot key")

	dist, err := ls.PoolStakeDistribution(t.Context(), nil, QueryPoint{}, nil)
	require.NoError(t, err)
	require.Len(t, dist.Pools, 1)
	assert.Equal(t, oldVrf, dist.Pools[0].VrfKeyHash[:],
		"the distribution must report the key header validation requires")
}

// A snapshot row written without its VRF key misses the bulk read and is
// resolved per pool. With no epoch history to place the capture, that
// resolution falls back to the latest registration rather than dropping the
// pool.
func TestPoolStakeDistribution_SnapshotRowWithoutVrfFallsBackPerPool(
	t *testing.T,
) {
	t.Parallel()
	db := newTestDB(t)

	oldVrf := repeatedBytes(32, 0xAA)
	newVrf := repeatedBytes(32, 0xBB)
	const snapshotEpoch = 0
	pkh := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(repeatedBytes(28, 0x11)),
	)
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{PoolKeyHash: pkh.Bytes(), VrfKeyHash: oldVrf},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  oldVrf,
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        snapshotEpoch,
			SnapshotType: snapshotTypeMark,
			PoolKeyHash:  pkh.Bytes(),
			TotalStake:   dbtypes.Uint64(1_000_000),
			CapturedSlot: 1,
		},
		nil,
	))
	// The re-registration lands after the capture.
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{PoolKeyHash: pkh.Bytes(), VrfKeyHash: newVrf},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  newVrf,
			AddedSlot:   5,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))
	ls := newPoolDistr2Ledger(t, db)

	dist, err := ls.PoolStakeDistribution(t.Context(), nil, QueryPoint{}, nil)
	require.NoError(t, err)
	require.Len(t, dist.Pools, 1)
	assert.Equal(t, newVrf, dist.Pools[0].VrfKeyHash[:],
		"a key the history cannot place falls back to the latest registration")
}

// GetStakeDistribution answers from live stake but names the key header
// validation holds the pool to, so a registration newer than the electing
// snapshot must not change the reported key.
func TestQueryShelleyStakeDistribution_ReportsElectingVrfAfterMidEpochRotation(
	t *testing.T,
) {
	t.Parallel()
	db := newTestDB(t)

	oldVrf := repeatedBytes(32, 0xAA)
	newVrf := repeatedBytes(32, 0xBB)
	pkh := seedLiveStakeFixture(
		t, db, repeatedBytes(28, 0x11), oldVrf, 1_000_000, 1,
	)
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        0,
			SnapshotType: snapshotTypeMark,
			PoolKeyHash:  pkh.Bytes(),
			VrfKeyHash:   oldVrf,
			TotalStake:   dbtypes.Uint64(1_000_000),
			CapturedSlot: 1,
		},
		nil,
	))
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{PoolKeyHash: pkh.Bytes(), VrfKeyHash: newVrf},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  newVrf,
			AddedSlot:   5,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, completeTestShelleyGenesis(cfg).LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.1,
		"epochLength": 100,
		"maxLovelaceSupply": 4000000,
		"securityParam": 10,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	require.NoError(t, db.Metadata().SetNetworkState(0, 0, 1, nil))
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = cfg
	seedEpochs(t, ls, map[uint64]uint64{0: 0})
	tipHash := bytes.Repeat([]byte{0xEF}, 32)
	seedBlockAtSlot(t, ls, 1, tipHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1, tipHash),
	}, nil))

	result, err := ls.Query(t.Context(), stakeDistributionQuery(), QueryPoint{})
	require.NoError(t, err)
	dist := decodeStakeDistributionResult(t, result)
	entry, ok := dist.Results[lcommon.PoolId(pkh)]
	require.True(t, ok, "pool missing from the distribution")
	assert.Equal(t, oldVrf, entry.VrfHash[:],
		"the reply must name the key header validation requires")
}
