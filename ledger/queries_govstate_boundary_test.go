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
	"encoding/json"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryShelleyGovStateDRepDistributionHoldsThroughTheEpoch(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	ls, db := newGovStateLedger(t, models.Epoch{EpochId: 1, StartSlot: 100})
	boundaryDRep := stakeCred28(0xc1)
	require.NoError(t, db.CreateDrep(ctx, nil, &models.Drep{
		Credential: boundaryDRep,
		Active:     true,
		AddedSlot:  10,
	}))
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey: stakeCred28(0xd1),
		Drep:       boundaryDRep,
		DrepType:   models.DrepTypeAddrKeyHash,
		Reward:     5_000_000,
		Active:     true,
		AddedSlot:  20,
	}))
	recordGovStateBoundary(t, ls)

	// Within the epoch a second account delegates to the same DRep and a new
	// DRep registers with stake of its own.
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey: stakeCred28(0xd2),
		Drep:       boundaryDRep,
		DrepType:   models.DrepTypeAddrKeyHash,
		Reward:     7_000_000,
		Active:     true,
		AddedSlot:  150,
	}))
	laterDRep := stakeCred28(0xc2)
	require.NoError(t, db.CreateDrep(ctx, nil, &models.Drep{
		Credential: laterDRep,
		Active:     true,
		AddedSlot:  160,
	}))
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey: stakeCred28(0xd3),
		Drep:       laterDRep,
		DrepType:   models.DrepTypeAddrKeyHash,
		Reward:     3_000_000,
		Active:     true,
		AddedSlot:  170,
	}))

	reply, err := ls.queryShelleyGovState(ctx, QueryPoint{}, nil)
	require.NoError(t, err)
	state := decodeGovStateReply(t, reply)
	pulsing, err := state.DecodeDRepPulsingState()
	require.NoError(t, err)
	require.Len(t, pulsing.Snapshot.DRepDistr, 1)
	assert.Equal(
		t, boundaryDRep, pulsing.Snapshot.DRepDistr[0].Drep.Credential,
	)
	assert.Equal(t, uint64(5_000_000), pulsing.Snapshot.DRepDistr[0].Stake)
}

func TestQueryShelleyGovStateRefusesWithoutBoundaryDRepDistribution(
	t *testing.T,
) {
	t.Parallel()

	ls, _ := newGovStateLedger(t, models.Epoch{EpochId: 1, StartSlot: 100})
	// A distribution recorded for another epoch does not stand in.
	require.NoError(t, ls.captureDRepDistributionSnapshot(
		context.Background(), nil, 0, 0,
	))

	_, err := ls.queryShelleyGovState(context.Background(), QueryPoint{}, nil)
	require.ErrorIs(t, err, ErrQueryNotAnswered)
}

func TestQueryShelleyGovStatePoolDistributionIsTheBoundaryMark(t *testing.T) {
	t.Parallel()

	ls, db := newGovStateLedger(t, models.Epoch{EpochId: 5, StartSlot: 500})
	pool := seedPoolDistr2Fixture(
		t, db, repeatedBytes(28, 0x11), repeatedBytes(32, 0xaa), 1_000_000, 4,
	)
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        5,
			SnapshotType: models.PoolStakeSnapshotTypeMark,
			PoolKeyHash:  pool.Bytes(),
			TotalStake:   dbtypes.Uint64(2_000_000),
			CapturedSlot: 499,
		},
		nil,
	))
	recordGovStateBoundary(t, ls)

	reply, err := ls.queryShelleyGovState(
		context.Background(), QueryPoint{}, nil,
	)
	require.NoError(t, err)
	state := decodeGovStateReply(t, reply)
	pulsing, err := state.DecodeDRepPulsingState()
	require.NoError(t, err)
	assert.Equal(
		t,
		map[ledger.Blake2b224]uint64{ledger.Blake2b224(pool): 2_000_000},
		pulsing.Snapshot.PoolDistr,
	)
}

func TestQueryShelleyGovStateRefusesWhileTheBoundaryMarkIsPending(
	t *testing.T,
) {
	t.Parallel()

	ls, db := newGovStateLedger(t, models.Epoch{EpochId: 5, StartSlot: 500})
	recordGovStateBoundary(t, ls)
	raw, err := json.Marshal(pendingRatificationRecord{
		Epoch: 5, BoundarySlot: 500, Snapshot: true,
	})
	require.NoError(t, err)
	require.NoError(t, db.SetSyncState(
		pendingRatificationSyncKey, string(raw), nil,
	))

	_, err = ls.queryShelleyGovState(context.Background(), QueryPoint{}, nil)
	require.ErrorIs(t, err, ErrQueryNotAnswered)
}

func TestEpochRolloverRecordsTheBoundaryDRepDistribution(t *testing.T) {
	t.Parallel()

	f := newHardForkRatifyFixture(t)
	result := f.rollover(t, f.ls.currentEpoch, f.pparams)

	snapshot, err := f.db.Metadata().GetDRepDistributionSnapshot(
		result.NewCurrentEpoch.EpochId, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.Equal(t, result.NewCurrentEpoch.StartSlot, snapshot.BoundarySlot)
	assert.NotEmpty(t, snapshot.Distribution)
}

func TestRollbackRemovesLaterDRepDistributionSnapshots(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	for epoch, boundary := range map[uint64]uint64{1: 100, 2: 200} {
		require.NoError(t, db.Metadata().SaveDRepDistributionSnapshot(
			&models.DRepDistributionSnapshot{
				Epoch:        epoch,
				Distribution: []byte{0xa0},
				BoundarySlot: boundary,
			},
			nil,
		))
	}

	require.NoError(t, db.DeleteGovernanceProposalsAfterSlot(
		context.Background(), 150, nil,
	))

	kept, err := db.Metadata().GetDRepDistributionSnapshot(1, nil)
	require.NoError(t, err)
	assert.NotNil(t, kept)
	removed, err := db.Metadata().GetDRepDistributionSnapshot(2, nil)
	require.NoError(t, err)
	assert.Nil(t, removed)
}
