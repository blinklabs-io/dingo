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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

const (
	retentionNewEpoch            = uint64(4)
	retentionRewardSnapshotEpoch = uint64(1)
	retentionPerformanceEpoch    = uint64(2)
	retentionPotsEpoch           = uint64(3)
	retentionBoundarySlot        = uint64(400)
)

// seedRetentionRewardEpochs seeds the epoch rows, protocol parameters, and ADA
// pots that reward application reads before it reaches the retention skip, so a
// test that removes the skip fails inside the reward calculation rather than
// earlier on missing epoch metadata.
func seedRetentionRewardEpochs(t *testing.T, db *database.Database) {
	t.Helper()
	meta := db.Metadata()
	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)
	for _, epoch := range []struct {
		startSlot uint64
		id        uint64
	}{
		{0, retentionRewardSnapshotEpoch},
		{100, retentionPerformanceEpoch},
		{200, retentionPotsEpoch},
	} {
		require.NoError(t, meta.SetEpoch(
			epoch.startSlot, epoch.id, nil, nil, nil, nil,
			eras.ShelleyEraDesc.Id, 1, 100, nil,
		), "set epoch %d", epoch.id)
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		retentionPerformanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        retentionPotsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 300,
	}, nil))
}

// TestApplyStakeRewardsSkipsPrunedStakeInputs covers the retention interaction
// introduced with dingo #2987. reward_ada_pots, reward_snapshot,
// reward_pool_input and reward_pool_output are retained for the life of the
// database while reward_stake_input is pruned to the rotation window, so an
// aged-out epoch presents complete-looking pots and snapshot rows over an empty
// credential set. Reward application must skip that epoch rather than hand
// validateRewardCalculatorInputs an unreconcilable snapshot, whose error would
// fail the whole epoch rollover.
func TestApplyStakeRewardsSkipsPrunedStakeInputs(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	poolKey := rewardCalcHash(0x11)
	rewardAccount := rewardCalcHash(0x22)

	seedRetentionRewardEpochs(t, db)

	// Snapshot and pool input survive retention, and the snapshot still claims
	// two delegators whose credential rows have aged out.
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            retentionRewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      retentionRewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	// reward_stake_input is deliberately absent: those rows aged out of the
	// retention window.

	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: rewardAccount,
		Pool:       poolKey,
		Active:     true,
	}))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(
			txn, retentionNewEpoch, retentionBoundarySlot,
		)
	}), "aged-out stake inputs must skip reward application, not error")

	// Nothing was credited and no outputs were persisted for the skipped epoch.
	account, err := db.GetAccountByCredential(0, rewardAccount, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Zero(
		t,
		uint64(account.Reward),
		"skipped epoch must not credit rewards",
	)

	poolOutputs, err := meta.GetRewardPoolOutputs(
		retentionRewardSnapshotEpoch, nil,
	)
	require.NoError(t, err)
	require.Empty(t, poolOutputs, "skipped epoch must not persist pool outputs")

	accountOutputs, err := meta.GetRewardAccountOutputs(
		retentionRewardSnapshotEpoch, nil,
	)
	require.NoError(t, err)
	require.Empty(
		t,
		accountOutputs,
		"skipped epoch must not persist account outputs",
	)

	var deltas int64
	require.NoError(t, rewardCalcSQLDB(t, db).QueryRow(
		"SELECT COUNT(*) FROM account_reward_delta WHERE added_slot = ?",
		retentionBoundarySlot,
	).Scan(&deltas))
	require.Zero(t, deltas, "skipped epoch must not record reward deltas")
}

// TestApplyStakeRewardsAcceptsZeroDelegatorSnapshot guards the skip predicate
// itself: an epoch that legitimately captured no delegators has an empty
// credential set too, and must still reconcile as a normal (non-pruned)
// snapshot rather than tripping the retention skip.
func TestApplyStakeRewardsAcceptsZeroDelegatorSnapshot(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	seedRetentionRewardEpochs(t, db)
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            retentionRewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 0,
		TotalPoolCount:   0,
		TotalDelegators:  0,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(
			txn, retentionNewEpoch, retentionBoundarySlot,
		)
	}), "an empty snapshot must reconcile normally, not error")
}
