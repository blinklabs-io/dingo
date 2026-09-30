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

package sqlstore

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestPendingRewardCreditRoundReadersScalePastParameterLimit(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	limitSQLiteVariableNumber(t, store.readDB, store.dialect.ParameterLimit())
	limitSQLiteVariableNumber(t, store.writeDB, store.dialect.ParameterLimit())
	const roundCount = 1_000
	poolKey := bytesRepeat(0x31, 28)
	stakingKey := bytesRepeat(0x41, 28)
	drepKey := bytesRepeat(0x51, 28)
	rounds := make([]models.RewardCreditRound, roundCount)
	for i := range rounds {
		rounds[i] = models.RewardCreditRound{
			SnapshotEpoch: uint64(i),
			BoundarySlot:  uint64(i + 1),
		}
	}
	require.NoError(t, store.SetPendingRewardCreditRounds(rounds, nil))
	require.NoError(t, store.SaveRewardAccountOutputs(
		[]*models.RewardAccountOutput{{
			Epoch:         roundCount - 1,
			CredentialTag: 0,
			StakingKey:    stakingKey,
			PoolKeyHash:   poolKey,
			RewardType:    "member",
			Amount:        types.Uint64(25),
			Spendable:     true,
			BoundarySlot:  roundCount,
		}},
		nil,
	))
	_, err := store.writeDB.Exec(store.dialect.Rebind(`
INSERT INTO reward_live_stake (
    pool_key_hash, staking_key, credential_tag, utxo_stake, reward_stake,
    total_stake, registered, updated_slot, calculation_version
) VALUES (?, ?, 0, '10', '2', '12', TRUE, 1, ?)`),
		poolKey, stakingKey, models.RewardStakeCalculationVersion,
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(store.dialect.Rebind(`
INSERT INTO account (staking_key, credential_tag, drep, drep_type, active,
    expiration_epoch, reward)
VALUES (?, 0, ?, 0, TRUE, 0, '2')`), stakingKey, drepKey)
	require.NoError(t, err)

	inputs, err := store.GetLiveStakeInputsForPools([][]byte{poolKey}, 0, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, types.Uint64(37), inputs[0].Stake)

	selected := map[historicalRewardKey]struct{}{
		{tag: 0, key: string(stakingKey)}: {},
	}
	before, err := store.pendingCreditsForCredentials(
		context.Background(), store.writeDB, roundCount-1, selected,
	)
	require.NoError(t, err)
	require.Empty(t, before, "the round is not visible before its boundary slot")
	after, err := store.pendingCreditsForCredentials(
		context.Background(), store.writeDB, roundCount, selected,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(25), after[historicalRewardKey{tag: 0, key: string(stakingKey)}])

	byCredential, _, err := store.pendingDRepCredits(
		context.Background(), store.writeDB, 0, "drep", []any{drepKey},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(25), byCredential[models.NewStakeCredentialRef(0, drepKey).MapKey()])
	hasPending, err := store.HasPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.True(t, hasPending)
	require.NoError(t, store.FoldPendingRewardAccountOutputs(0, stakingKey, nil))
	hasPending, err = store.HasPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.False(t, hasPending, "folded credits must no longer count as pending")
	hasApplied, err := store.HasAppliedRewardCreditRound(roundCount-1, nil)
	require.NoError(t, err)
	require.True(t, hasApplied, "the applied-round marker must survive folding")
}

func TestDeleteRewardStateBeforeEpochPreservesPendingCreditBalance(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	stakingKey := bytesRepeat(0x71, 28)
	pendingPool := bytesRepeat(0x72, 28)
	foldedPool := bytesRepeat(0x73, 28)
	untrackedPool := bytesRepeat(0x74, 28)
	require.NoError(t, store.SetPendingRewardCreditRounds(
		[]models.RewardCreditRound{{SnapshotEpoch: 1, BoundarySlot: 100}}, nil,
	))
	require.NoError(t, store.SaveRewardAccountOutputs(
		[]*models.RewardAccountOutput{
			{
				Epoch: 1, CredentialTag: 0, StakingKey: stakingKey,
				PoolKeyHash: pendingPool, RewardType: "member",
				Amount: types.Uint64(25), Spendable: true,
				CapturedSlot: 10, BoundarySlot: 100,
			},
			{
				Epoch: 1, CredentialTag: 0, StakingKey: stakingKey,
				PoolKeyHash: foldedPool, RewardType: "member",
				Amount: types.Uint64(11), Spendable: true,
				CapturedSlot: 10, BoundarySlot: 100,
			},
			{
				Epoch: 0, CredentialTag: 0, StakingKey: stakingKey,
				PoolKeyHash: untrackedPool, RewardType: "member",
				Amount: types.Uint64(7), Spendable: true,
				CapturedSlot: 5, BoundarySlot: 50,
			},
		}, nil,
	))
	_, err := store.writeDB.Exec(
		`UPDATE reward_account_output SET folded = TRUE WHERE epoch = 1 AND pool_key_hash = ?`,
		foldedPool,
	)
	require.NoError(t, err)

	require.NoError(t, store.DeleteRewardStateBeforeEpoch(2, nil))
	outputs, err := store.GetRewardAccountOutputs(1, nil)
	require.NoError(t, err)
	require.Len(t, outputs, 1, "only the applied round's unfolded output survives")
	require.Equal(t, pendingPool, outputs[0].PoolKeyHash)
	selected := map[historicalRewardKey]struct{}{
		{tag: 0, key: string(stakingKey)}: {},
	}
	pending, err := store.pendingCreditsForCredentials(
		context.Background(), store.writeDB, 100, selected,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(25), pending[historicalRewardKey{
		tag: 0, key: string(stakingKey),
	}], "the surviving output remains part of the pending balance")
}

func limitSQLiteVariableNumber(t *testing.T, db *sql.DB, limit int) {
	t.Helper()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	_, err = sqlite.Limit(conn, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, limit)
	require.NoError(t, err)
	current, err := sqlite.Limit(conn, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, -1)
	require.NoError(t, err)
	require.Equal(t, limit, current)
	require.NoError(t, conn.Close())
}

func bytesRepeat(value byte, count int) []byte {
	ret := make([]byte, count)
	for index := range ret {
		ret[index] = value
	}
	return ret
}

func TestRollbackRewardCreditUnfoldUsesEpochIndex(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	plan := queryPlan(t, store.writeDB, rollbackUnfoldRewardAccountOutputsSQL, 100)
	require.Contains(t, plan, "idx_reward_account_output_epoch_cred_pool_type")
	require.NotContains(t, plan, "SCAN reward_account_output")
	require.True(t, strings.Contains(plan, "SEARCH reward_account_output"), plan)
}
