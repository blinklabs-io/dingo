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

package snapshot

import (
	"bytes"
	"context"
	"database/sql"
	"math/big"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// seedPointerStakeFixture builds a pool, a credential registered and
// delegated entirely through certificate history (so pointer resolution has
// real stake_registration/stake_delegation rows to join against, exactly as
// the reference ledger's saPtrs lookup does), a base-address UTxO for that
// credential, and a pointer-address UTxO naming the registration's own
// position. It returns the credential's staking key so a caller can inspect
// reward_live_stake directly.
//
// This is the dingo #3854/#3811 shape: a pool whose stake is understated
// because part of one delegator's stake sits at a pointer address.
func seedPointerStakeFixture(
	t *testing.T,
	db *database.Database,
	poolHash []byte,
) []byte {
	t.Helper()
	require.NoError(t, db.ImportPool(nil, &models.Pool{
		PoolKeyHash: poolHash,
		VrfKeyHash:  make([]byte, 32),
		Pledge:      1_000_000,
		Cost:        340_000_000,
		Margin:      &types.Rat{Rat: big.NewRat(1, 100)},
	}, &models.PoolRegistration{
		PoolKeyHash: poolHash,
		AddedSlot:   50,
		Pledge:      1_000_000,
		Cost:        340_000_000,
		Margin:      &types.Rat{Rat: big.NewRat(1, 100)},
		VrfKeyHash:  make([]byte, 32),
	}), "import pool")

	raw := snapshotSQLDB(t, db)
	stakeKey := bytes.Repeat([]byte{0x9c}, 28)
	regCertID := seedCertificate(
		t, raw, 100, 0, 0, lcommon.CertificateTypeStakeRegistration,
	)
	seedStakeRegistration(t, raw, models.StakeRegistration{
		StakingKey:    stakeKey,
		AddedSlot:     100,
		CertificateID: regCertID,
	})
	delCertID := seedCertificate(
		t, raw, 100, 0, 1, lcommon.CertificateTypeStakeDelegation,
	)
	seedStakeDelegation(t, raw, models.StakeDelegation{
		StakingKey:    stakeKey,
		PoolKeyHash:   poolHash,
		AddedSlot:     100,
		CertificateID: delCertID,
	})

	// Account first, then UTxOs: CreateAccount and CreateUtxo each refresh
	// reward_live_stake for the ref they touch by re-summing from the utxo
	// table as it stands at call time, so the account row (which carries
	// registered=true and the delegated pool) must exist before the UTxOs are
	// written for the final refresh to see the correct registration state.
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: stakeKey,
		Pool:       poolHash,
		AddedSlot:  100,
		Active:     true,
	}), "create account")
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       bytes.Repeat([]byte{0x01}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     700,
		AddedSlot:  150,
	}), "create base-address utxo")
	// The pointer-address utxo names the registration's own position
	// (100, 0, 0). Its StakingKey stays empty by design (see
	// database/models/utxo.go); only utxo_pointer records the position.
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:      bytes.Repeat([]byte{0x02}, 32),
		OutputIdx: 0,
		Amount:    600,
		AddedSlot: 200,
		Pointer:   &models.UtxoPointer{Slot: 100, TxIndex: 0, CertIndex: 0},
	}), "create pointer-address utxo")

	return stakeKey
}

// TestCaptureEpochBoundaryAgreesOnPointerStake is the dingo#3854 review's
// blocking finding: ComputeEpochBoundarySnapshot (the SNAP-point hook a
// normally operating node installs) reads only the live aggregate, while the
// event-driven fallback (no stashed SNAP-point distribution) reconstructs
// historically -- and only the historical route resolved pointer stake. Two
// nodes on the same chain, or one node across a restart that lost the
// SNAP-point read, would persist different Mark stake for a pool holding
// pointer stake.
//
// Both routes must now report the same pool total for the same epoch.
func TestCaptureEpochBoundaryAgreesOnPointerStake(t *testing.T) {
	for _, tc := range []struct {
		name        string
		computeSnap bool
	}{
		{
			name:        "authoritative SNAP-point path (live aggregate + pointer overlay)",
			computeSnap: true,
		},
		{
			name:        "event-driven fallback (historical reconstruction)",
			computeSnap: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := setupTestDB(t)
			seedEpochs(t, db, []models.Epoch{
				{EpochId: 0, StartSlot: 0, LengthInSlots: 432_000},
			})
			poolHash := bytes.Repeat([]byte{0xb2}, 28)
			seedPointerStakeFixture(t, db, poolHash)

			mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
			evt := event.EpochTransitionEvent{
				PreviousEpoch:   0,
				NewEpoch:        1,
				BoundarySlot:    432_000,
				EpochNonce:      []byte{0x0a, 0x0b},
				ProtocolVersion: 8,
				SnapshotSlot:    431_999,
			}

			txn := db.Transaction(true)
			if tc.computeSnap {
				// Authoritative path: SNAP-point read, then persist reuses
				// the stashed distribution.
				require.NoError(t, mgr.ComputeEpochBoundarySnapshot(
					context.Background(), txn, evt,
				))
			}
			// Without the compute call, this is the "missing/failed SNAP
			// hook" shape: persist has nothing stashed and reconstructs
			// historically.
			require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
				context.Background(), txn, evt,
			))
			require.NoError(t, txn.Commit())

			poolSnapshot, err := db.Metadata().GetPoolStakeSnapshot(
				1, "mark", poolHash, nil,
			)
			require.NoError(t, err)
			require.NotNil(t, poolSnapshot)
			require.Equal(
				t,
				uint64(1_300),
				uint64(poolSnapshot.TotalStake),
				"both capture routes must attribute the pointer-address "+
					"stake identically for the same epoch",
			)
		})
	}
}

// TestRewardLiveStakeRebuildAgreesWithIncrementalOnPointerAddresses covers the
// constraint the PR's own package doc states: reward_live_stake never carries
// pointer-derived UTxO stake, because attribution depends on certificate
// history at the slot being evaluated rather than on anything the live,
// tip-keyed aggregate can express. That has to hold identically whichever way
// reward_live_stake was populated -- a full RebuildRewardLiveStake pass or the
// normal incremental per-write refresh -- or a node that rebuilds would
// silently start disagreeing with one that never has.
func TestRewardLiveStakeRebuildAgreesWithIncrementalOnPointerAddresses(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432_000},
	})
	poolHash := bytes.Repeat([]byte{0xb4}, 28)
	stakeKey := seedPointerStakeFixture(t, db, poolHash)

	raw := snapshotSQLDB(t, db)
	incremental := rewardLiveStakeTotalStake(t, raw, stakeKey)

	require.NoError(t, db.RebuildRewardLiveStake(1_000, nil))

	rebuilt := rewardLiveStakeTotalStake(t, raw, stakeKey)

	require.Equal(
		t,
		incremental,
		rebuilt,
		"RebuildRewardLiveStake must not diverge from incremental "+
			"maintenance for a credential holding pointer-address stake",
	)
	require.Equal(
		t,
		uint64(700),
		incremental,
		"reward_live_stake must exclude pointer-address stake on both "+
			"maintenance paths -- the live snapshot path adds it back "+
			"separately, from utxo_pointer, not from this table",
	)
}

// rewardLiveStakeTotalStake reads reward_live_stake.total_stake for a
// credential directly, bypassing GetLiveStakeInputsForPools's pool filter so
// the read is unaffected by which pool the credential is delegated to.
func rewardLiveStakeTotalStake(
	t *testing.T,
	raw *sql.DB,
	stakingKey []byte,
) uint64 {
	t.Helper()
	var total string
	require.NoError(t, raw.QueryRow(
		"SELECT total_stake FROM reward_live_stake WHERE staking_key = ?",
		stakingKey,
	).Scan(&total))
	value, err := strconv.ParseUint(total, 10, 64)
	require.NoError(t, err)
	return value
}
