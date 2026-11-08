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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// stakeReader is one reader of the stake and reward bookkeeping tables. The
// SQL is the statement the named function runs, with its bound values
// replaced by the placeholders it binds.
type stakeReader struct {
	name  string
	table string
	// wantIndex, when set, must appear in the table's plan line, so a reader
	// that moved to a different index is noticed.
	wantIndex string
	query     string
	args      []any
}

func stakeReaders() []stakeReader {
	key := make([]byte, 28)
	hash := make([]byte, 32)
	return []stakeReader{
		{
			name: "GetAccountSums withdrawals", table: "account_reward_delta",
			wantIndex: "idx_account_reward_delta_credential",
			query: `SELECT amount FROM account_reward_delta
WHERE withdrawal = TRUE AND credential_tag = ? AND staking_key = ?`,
			args: []any{0, key},
		},
		{
			name: "AddAccountRewardsByCredential applied lookup", table: "account_reward_delta",
			wantIndex: "idx_account_reward_delta_w_tx_s_slot",
			query: `SELECT tx_hash, credential_tag, staking_key, added_slot
FROM account_reward_delta WHERE withdrawal = FALSE AND tx_hash IN (?, ?)`,
			args: []any{hash, hash},
		},
		{
			name: "RewardCreditsAlreadyApplied", table: "account_reward_delta",
			wantIndex: "idx_account_reward_delta_w_tx_s_slot",
			query: `SELECT tx_hash, credential_tag, staking_key, added_slot
FROM account_reward_delta WHERE withdrawal = FALSE AND tx_hash IN (?)`,
			args: []any{hash},
		},
		{
			name: "withdrawal journal exists", table: "account_reward_delta",
			wantIndex: "idx_account_reward_delta_w_tx_s_slot",
			query: `SELECT EXISTS (SELECT 1 FROM account_reward_delta
WHERE withdrawal = TRUE AND tx_hash = ? AND credential_tag = ? AND staking_key = ?)`,
			args: []any{hash, 0, key},
		},
		{
			name: "GetPostSnapshotRewardCredits", table: "account_reward_delta",
			wantIndex: "idx_account_reward_delta_added_slot",
			query: `SELECT credential_tag, staking_key, amount FROM account_reward_delta
WHERE added_slot = ? AND post_snapshot = TRUE AND withdrawal = FALSE
ORDER BY credential_tag, staking_key, id`,
			args: []any{100},
		},
		{
			name: "MarkPostSnapshotAccountRewardCredit", table: "account_reward_delta",
			wantIndex: "idx_account_reward_delta_w_tx_s_slot",
			query: `UPDATE account_reward_delta SET post_snapshot = TRUE
WHERE withdrawal = FALSE AND tx_hash = ? AND credential_tag = ?
  AND staking_key = ? AND added_slot = ?`,
			args: []any{hash, 0, key, 100},
		},
		{
			name: "account rollback sweep", table: "account_reward_delta",
			wantIndex: "idx_account_reward_delta_added_slot",
			query: `SELECT staking_key, credential_tag, amount, previous_reward, withdrawal
FROM account_reward_delta WHERE added_slot > ? ORDER BY added_slot DESC, id DESC`,
			args: []any{100},
		},
		{
			name: "account credential rollback delete", table: "account_reward_delta",
			wantIndex: "idx_account_reward_delta_credential",
			query: `DELETE FROM account_reward_delta WHERE added_slot > ?
AND credential_tag = ? AND staking_key IN (?, ?)`,
			args: []any{100, 0, key, key},
		},
		{
			name: "withdrawal witness slots", table: "account_reward_delta",
			query: `SELECT credential_tag, staking_key FROM account_reward_delta
WHERE withdrawal = TRUE AND added_slot > ?`,
			args: []any{100},
		},
		{
			name: "withdrawalHistoryQuery", table: "account_reward_delta",
			wantIndex: "idx_account_reward_delta_credential",
			query: `SELECT ard.tx_hash, ard.amount, tx.slot, tx.block_index, tx.block_hash
FROM account_reward_delta ard JOIN "transaction" tx ON tx.hash = ard.tx_hash
WHERE ard.withdrawal = TRUE AND ard.credential_tag = ? AND ard.staking_key = ?`,
			args: []any{0, key},
		},
		{
			name: "GetLiveStakeInputsForPools", table: "reward_live_stake",
			wantIndex: "idx_reward_live_stake_pool_order",
			query: `SELECT rls.pool_key_hash, rls.staking_key, rls.credential_tag, rls.total_stake
FROM reward_live_stake rls
WHERE rls.pool_key_hash IN (?, ?) AND rls.registered = TRUE
ORDER BY rls.pool_key_hash ASC, rls.credential_tag ASC, rls.staking_key ASC`,
			args: []any{key, key},
		},
		{
			name: "GetDelegatedPoolKeyHashes", table: "reward_live_stake",
			wantIndex: "idx_reward_live_stake_pool_order",
			query: `SELECT DISTINCT pool_key_hash FROM reward_live_stake
WHERE pool_key_hash IS NOT NULL AND LENGTH(pool_key_hash) > 0`,
		},
		{
			name: "retired pool live stake clear", table: "reward_live_stake",
			wantIndex: "idx_reward_live_stake_pool_order",
			query: `UPDATE reward_live_stake SET pool_key_hash = NULL, updated_slot = ?
WHERE pool_key_hash = ?`,
			args: []any{100, key},
		},
		{
			name: "priorLiveStake", table: "reward_live_stake",
			wantIndex: "idx_reward_live_stake_cred",
			query:     rewardLiveStakeUtxoStakeQuery,
			args:      []any{0, key},
		},
		{
			name: "account by drep and active", table: "account",
			wantIndex: "idx_account_drep_active_staking_key",
			query: `SELECT a.credential_tag, a.staking_key FROM account a
WHERE a.drep = ? AND a.active = TRUE`,
			args: []any{key},
		},
		{
			name: "account by drep type", table: "account",
			query: `SELECT credential_tag, staking_key, drep_type, drep FROM account
WHERE active = TRUE AND drep IS NOT NULL AND drep_type <= 1 AND added_slot <= ?`,
			args: []any{100},
		},
		{
			name: "GetRewardAccountOutputsByCredential", table: "reward_account_output",
			wantIndex: "idx_reward_account_output_credential_spendable_guarded",
			query: `SELECT amount FROM reward_account_output
WHERE credential_tag = ? AND staking_key = ? AND spendable = TRUE AND guarded = FALSE
ORDER BY epoch ASC, pool_key_hash ASC, reward_type ASC`,
			args: []any{0, key},
		},
		{
			name: "CountRewardAccountOutputsByCredential", table: "reward_account_output",
			query: `SELECT COUNT(*) FROM reward_account_output
WHERE credential_tag = ? AND staking_key = ? AND spendable = TRUE AND guarded = FALSE`,
			args: []any{0, key},
		},
		{
			name: "rewardAccountOutputsForCredential", table: "reward_account_output",
			query: `SELECT amount FROM reward_account_output
WHERE credential_tag = ? AND staking_key = ? AND epoch IN (?, ?)
  AND spendable = TRUE AND guarded = FALSE AND NOT folded
ORDER BY epoch, pool_key_hash, reward_type`,
			args: []any{0, key, 1, 2},
		},
		{
			name: "GetPendingRewardAccountOutputsForCredential", table: "reward_account_output",
			query: `SELECT amount FROM reward_account_output rao
WHERE rao.credential_tag = ? AND rao.staking_key = ?
  AND rao.spendable = TRUE AND rao.guarded = FALSE AND NOT rao.folded
  AND EXISTS (SELECT 1 FROM reward_credit_round rcr WHERE rcr.snapshot_epoch = rao.epoch)
ORDER BY rao.epoch, rao.pool_key_hash, rao.reward_type`,
			args: []any{0, key},
		},
		{
			name: "FoldRewardAccountOutputs", table: "reward_account_output",
			query: `UPDATE reward_account_output SET folded = TRUE
WHERE credential_tag = ? AND staking_key = ? AND epoch IN (?)
  AND spendable = TRUE AND guarded = FALSE AND NOT folded`,
			args: []any{0, key, 1},
		},
		{
			name: "pending credit round scan", table: "reward_account_output",
			query: `SELECT amount FROM reward_account_output
WHERE spendable = TRUE AND guarded = FALSE AND folded = FALSE AND epoch <= ?`,
			args: []any{5},
		},
	}
}

func explainPlan(
	t *testing.T,
	store *Store,
	reader stakeReader,
) []string {
	t.Helper()
	rows, err := store.writeDB.Query(
		"EXPLAIN QUERY PLAN "+reader.query,
		reader.args...)
	require.NoError(t, err, reader.name)
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan = append(plan, detail)
	}
	require.NoError(t, rows.Err())
	return plan
}

// TestStakeBookkeepingReadersUseAnIndex runs every inventoried reader of the
// stake and reward bookkeeping tables through EXPLAIN QUERY PLAN on the
// migrated schema, before and after ANALYZE has populated sqlite_stat1, and
// requires its table access to name an index rather than scan the table.
func TestStakeBookkeepingReadersUseAnIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	w := newStakeWorkload(t, store, 300, 10)
	w.register(50)
	w.epochBoundary(100)
	w.syncBlocks(4, 20)
	w.epochBoundary(101)

	for _, analyzed := range []bool{false, true} {
		if analyzed {
			_, err := store.writeDB.Exec("ANALYZE")
			require.NoError(t, err)
		}
		for _, reader := range stakeReaders() {
			t.Run(
				fmt.Sprintf("analyze=%t/%s", analyzed, reader.name),
				func(t *testing.T) {
					plan := explainPlan(t, store, reader)
					found := false
					for _, line := range plan {
						if !strings.Contains(line, reader.table) {
							continue
						}
						found = true
						require.Contains(
							t,
							line,
							"USING",
							"%s scans %s without an index: %v",
							reader.name,
							reader.table,
							plan,
						)
						if reader.wantIndex != "" {
							require.Contains(
								t,
								line,
								reader.wantIndex,
								"%s left %s: %v",
								reader.name,
								reader.wantIndex,
								plan,
							)
						}
					}
					require.True(
						t,
						found,
						"no plan line for %s: %v",
						reader.table,
						plan,
					)
				},
			)
		}
	}
}
