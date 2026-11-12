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
	"context"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/stretchr/testify/require"
)

// A pool re-registration submitted during the ended epoch only takes effect
// after the snapshot, so the live capture resolves owners from the earlier
// certificate. The rebuild of a pruned epoch must resolve them the same way,
// or the new owner's stake is flagged as owner stake and the reconstruction
// no longer reconciles with the retained reward_pool_input OwnerStake.
func TestRebuildPrunedRewardStakeInputsResolvesOwnersAtSnapshotWindow(
	t *testing.T,
) {
	t.Parallel()

	const (
		endedEpoch     = uint64(1)
		snapshotEpoch  = uint64(2)
		epochLength    = uint64(100)
		boundarySlot   = snapshotEpoch * epochLength
		reRegisterSlot = endedEpoch*epochLength + 50
	)
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()
	poolHash := bytes.Repeat([]byte{0xd2}, 28)
	ownerKey := bytes.Repeat([]byte{0x81}, 28)
	addedOwnerKey := bytes.Repeat([]byte{0x82}, 28)
	delegatorKey := bytes.Repeat([]byte{0x83}, 28)

	registration := func(
		slot uint64,
		owners ...[]byte,
	) *models.PoolRegistration {
		reg := &models.PoolRegistration{
			PoolKeyHash:   poolHash,
			AddedSlot:     slot,
			Pledge:        5_000_000,
			Cost:          340_000_000,
			Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
			VrfKeyHash:    make([]byte, 32),
			RewardAccount: ownerKey,
		}
		for _, owner := range owners {
			reg.Owners = append(reg.Owners, models.PoolRegistrationOwner{
				KeyHash: append([]byte(nil), owner...),
			})
		}
		return reg
	}
	newPool := func() *models.Pool {
		return &models.Pool{
			PoolKeyHash:   poolHash,
			VrfKeyHash:    make([]byte, 32),
			Pledge:        5_000_000,
			Cost:          340_000_000,
			Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
			RewardAccount: ownerKey,
		}
	}
	require.NoError(t, db.ImportPool(
		context.Background(), nil, newPool(), registration(10, ownerKey),
	))
	require.NoError(t, db.ImportPool(
		context.Background(), nil, newPool(),
		registration(reRegisterSlot, ownerKey, addedOwnerKey),
	))
	// The added owner's stake is far above the reconstruction tolerance, so a
	// wrong owner set cannot be folded away.
	for i, key := range [][]byte{ownerKey, addedOwnerKey, delegatorKey} {
		require.NoError(t, db.CreateAccount(
			context.Background(), nil, &models.Account{
				StakingKey: key,
				Pool:       poolHash,
				AddedSlot:  0,
				Active:     true,
			},
		))
		require.NoError(t, db.CreateUtxo(
			context.Background(), nil, &models.Utxo{
				TxId:       bytes.Repeat([]byte{byte(0x20 + i)}, 32),
				OutputIdx:  0,
				StakingKey: key,
				Amount:     types.Uint64(50_000_000),
				AddedSlot:  0,
			},
		))
	}
	for epoch := range endedEpoch + 1 {
		require.NoError(t, meta.SetEpoch(
			epoch*epochLength, epoch, nil, nil, nil, nil,
			eras.ShelleyEraDesc.Id, 1, uint(epochLength), nil,
		))
	}

	mgr := snapshot.NewManager(db, newTestEventBus(t, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   endedEpoch,
		NewEpoch:        snapshotEpoch,
		BoundarySlot:    boundarySlot,
		EpochNonce:      []byte{0x0a, 0x0b},
		ProtocolVersion: 7,
		SnapshotSlot:    boundarySlot - 1,
	}
	captureTxn := db.Transaction(context.Background(), true)
	require.NoError(t, mgr.ComputeEpochBoundarySnapshot(
		context.Background(), captureTxn, evt,
	))
	require.NoError(t, meta.SetEpoch(
		boundarySlot, snapshotEpoch, nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, uint(epochLength), captureTxn.Metadata(),
	))
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
		context.Background(), captureTxn, evt,
	))
	require.NoError(t, captureTxn.Commit())

	captured, err := meta.GetRewardStakeInputs(snapshotEpoch, nil)
	require.NoError(t, err)
	capturedOwner := make(map[string]bool, len(captured))
	for _, input := range captured {
		capturedOwner[string(input.StakingKey)] = input.Owner
	}
	require.Equal(t, map[string]bool{
		string(ownerKey):      true,
		string(addedOwnerKey): false,
		string(delegatorKey):  false,
	}, capturedOwner,
		"the live capture must resolve owners from the registration "+
			"effective before the ended epoch",
	)

	rewardSnapshot, err := meta.GetRewardSnapshot(snapshotEpoch, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, rewardSnapshot)
	poolInputs, err := meta.GetRewardPoolInputs(snapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolInputs, 1)
	require.NoError(t, meta.DeleteRewardStakeInputBeforeEpoch(
		context.Background(), snapshotEpoch+1, nil,
	))

	rebuilt, err := ls.rebuildPrunedRewardStakeInputs(
		meta, nil, snapshotEpoch, rewardSnapshot, poolInputs,
	)
	require.NoError(t, err)
	require.NotEmpty(t, rebuilt,
		"the rebuild must reconcile with the retained owner stake instead "+
			"of discarding the epoch",
	)
	rebuiltOwner := make(map[string]bool, len(rebuilt))
	for _, input := range rebuilt {
		rebuiltOwner[string(input.StakingKey)] = input.Owner
	}
	require.Equal(t, capturedOwner, rebuiltOwner,
		"the rebuild must resolve owners exactly as the live capture did",
	)
}

// Epoch 0 has no ended epoch before it, so the rebuild must still resolve its
// owners from epoch 0's own registrations rather than leave every row
// non-owner, which would fail reconciliation against a nonzero OwnerStake.
func TestRebuildPrunedRewardStakeInputsResolvesOwnersForEpochZero(
	t *testing.T,
) {
	t.Parallel()

	const (
		snapshotEpoch = uint64(0)
		epochLength   = uint64(100)
	)
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()
	poolHash := bytes.Repeat([]byte{0xd3}, 28)
	ownerKey := bytes.Repeat([]byte{0x91}, 28)
	delegatorKey := bytes.Repeat([]byte{0x92}, 28)

	require.NoError(t, db.ImportPool(
		context.Background(), nil,
		&models.Pool{
			PoolKeyHash:   poolHash,
			VrfKeyHash:    make([]byte, 32),
			Pledge:        5_000_000,
			Cost:          340_000_000,
			Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
			RewardAccount: ownerKey,
		},
		&models.PoolRegistration{
			PoolKeyHash:   poolHash,
			AddedSlot:     0,
			Pledge:        5_000_000,
			Cost:          340_000_000,
			Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
			VrfKeyHash:    make([]byte, 32),
			RewardAccount: ownerKey,
			Owners: []models.PoolRegistrationOwner{
				{KeyHash: append([]byte(nil), ownerKey...)},
			},
		},
	))
	for i, key := range [][]byte{ownerKey, delegatorKey} {
		require.NoError(t, db.CreateAccount(
			context.Background(), nil, &models.Account{
				StakingKey: key,
				Pool:       poolHash,
				AddedSlot:  0,
				Active:     true,
			},
		))
		require.NoError(t, db.CreateUtxo(
			context.Background(), nil, &models.Utxo{
				TxId:       bytes.Repeat([]byte{byte(0x40 + i)}, 32),
				OutputIdx:  0,
				StakingKey: key,
				Amount:     types.Uint64(50_000_000),
				AddedSlot:  0,
			},
		))
	}
	require.NoError(t, meta.SetEpoch(
		0, snapshotEpoch, nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, uint(epochLength), nil,
	))

	mgr := snapshot.NewManager(db, newTestEventBus(t, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   snapshotEpoch,
		NewEpoch:        snapshotEpoch,
		BoundarySlot:    0,
		EpochNonce:      []byte{0x0a, 0x0b},
		ProtocolVersion: 7,
		SnapshotSlot:    0,
	}
	captureTxn := db.Transaction(context.Background(), true)
	require.NoError(t, mgr.ComputeEpochBoundarySnapshot(
		context.Background(), captureTxn, evt,
	))
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
		context.Background(), captureTxn, evt,
	))
	require.NoError(t, captureTxn.Commit())

	captured, err := meta.GetRewardStakeInputs(snapshotEpoch, nil)
	require.NoError(t, err)
	capturedOwner := make(map[string]bool, len(captured))
	for _, input := range captured {
		capturedOwner[string(input.StakingKey)] = input.Owner
	}
	require.Equal(t, map[string]bool{
		string(ownerKey):     true,
		string(delegatorKey): false,
	}, capturedOwner)

	rewardSnapshot, err := meta.GetRewardSnapshot(snapshotEpoch, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, rewardSnapshot)
	poolInputs, err := meta.GetRewardPoolInputs(snapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolInputs, 1)
	require.NoError(t, meta.DeleteRewardStakeInputBeforeEpoch(
		context.Background(), snapshotEpoch+1, nil,
	))

	rebuilt, err := ls.rebuildPrunedRewardStakeInputs(
		meta, nil, snapshotEpoch, rewardSnapshot, poolInputs,
	)
	require.NoError(t, err)
	require.NotEmpty(t, rebuilt,
		"the epoch 0 rebuild must reconcile with the retained owner stake",
	)
	rebuiltOwner := make(map[string]bool, len(rebuilt))
	for _, input := range rebuilt {
		rebuiltOwner[string(input.StakingKey)] = input.Owner
	}
	require.Equal(t, capturedOwner, rebuiltOwner)
}
