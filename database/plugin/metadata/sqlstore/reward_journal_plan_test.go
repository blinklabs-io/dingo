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
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// seedRewardJournalPlanRows fills the journal with enough rows that a plan
// that ignores an index shows up as a scan, and that statistics prefer an
// index over a scan of a table the size of a batched IN list.
func seedRewardJournalPlanRows(t *testing.T, store *Store) {
	t.Helper()
	const accounts = 6000
	for _, statement := range []string{
		fmt.Sprintf(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < %d)
INSERT INTO account (staking_key, credential_tag, added_slot, created_slot, active, reward)
SELECT randomblob(28), 0, 0, 0, TRUE, '0' FROM n`, accounts),
		`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 6000)
INSERT INTO reward_source (hash) SELECT randomblob(32) FROM n`,
		`INSERT INTO reward_event_credit (source_id, account_id, slot, amount, post_snapshot)
SELECT s.id, a.id, 100 + a.id % 7, '5', a.id % 3 = 0
FROM account a JOIN reward_source s ON s.id = a.id`,
		`INSERT INTO "transaction" (hash, slot, block_index)
SELECT randomblob(32), 200 + id % 11, 0 FROM account`,
		`INSERT INTO reward_withdrawal (tx_id, account_id, slot, amount, previous_reward)
SELECT t.id, a.id, 200 + a.id % 11, '3', '9'
FROM account a JOIN "transaction" t ON t.id = a.id`,
		`INSERT INTO reward_credit (boundary_slot, account_id, output_id, amount)
SELECT 300 + (id % 4) * 100, id, id, '4' FROM account`,
		`INSERT INTO reward_credit_slot (boundary_slot) VALUES (300), (400), (500), (600)`,
	} {
		_, err := store.writeDB.Exec(statement)
		require.NoError(t, err, statement)
	}
}

// TestRewardJournalPlansDoNotDependOnStatistics pins the access path of every
// journal reader and of the rollback statements, before and after ANALYZE:
// each is served by the journal table's own key or a named index, never a scan
// of a journal table or an automatic index.
func TestRewardJournalPlansDoNotDependOnStatistics(t *testing.T) {
	t.Parallel()
	lastSlotsArgs := []any{0}
	for range rewardWithdrawalWitnessKeys {
		lastSlotsArgs = append(lastSlotsArgs, []byte{1})
	}
	lastSlotsArgs = append(lastSlotsArgs, 500)
	history, historyArgs := withdrawalHistoryQuery(0, []byte{1})
	for _, tc := range []struct {
		name  string
		query string
		args  []any
		want  []string
	}{
		{"history accounts", rewardHistoryAccountsSQL, append([]any{0}, idArgs([]int64{1})...),
			[]string{`idx_account_credential`}},
		{"history withdrawals", rewardHistoryWithdrawalsSQL[false], idArgs([]int64{1}, 150),
			[]string{`idx_reward_withdrawal_account_slot`}},
		{"history boundary withdrawals", rewardHistoryWithdrawalsSQL[true], idArgs([]int64{1}, 150),
			[]string{`idx_reward_withdrawal_account_slot`}},
		{"history credits", rewardHistoryCreditsSQL, idArgs([]int64{1}, 150),
			[]string{`SEARCH c USING PRIMARY KEY \(boundary_slot=\? AND account_id=\?\)`}},
		{"history events", rewardHistoryEventsSQL[false], idArgs([]int64{1}, 150),
			[]string{`idx_reward_event_credit_account`}},
		{"history events with post-snapshot", rewardHistoryEventsSQL[true], idArgs([]int64{1}, 150, 150),
			[]string{`idx_reward_event_credit_account`}},
		{"last withdrawal slots", lastRewardWithdrawalSlotsSQL, lastSlotsArgs,
			[]string{`idx_account_credential`, `idx_reward_withdrawal_account_slot`}},
		{"witnessed after a slot", rewardWithdrawalsWitnessedAfterSQL, []any{150},
			[]string{`idx_reward_withdrawal_slot`}},
		{"post-snapshot credits", postSnapshotRewardCreditsSQL, []any{100},
			[]string{`idx_reward_event_credit_slot`}},
		{"post-snapshot events", postSnapshotRewardCreditEventsSQL, []any{100},
			[]string{`idx_reward_event_credit_slot`}},
		{"withdrawal history", history + " ORDER BY tx_slot ASC, block_index ASC, tx_hash ASC", historyArgs,
			[]string{`idx_reward_withdrawal_account_slot`}},
		{"rollback withdrawals", rollbackRewardWithdrawalsSQL, []any{209},
			[]string{`idx_reward_withdrawal_slot`}},
		{"rollback events", rollbackRewardEventAccountsSQL, []any{105},
			[]string{`idx_reward_event_credit_slot`}},
		{"rollback credits of a slot", rollbackRewardCreditsOfSlotSQL, []any{300},
			[]string{`PRIMARY KEY \(boundary_slot=\?\)`}},
		{"account credit delete", deleteAccountRewardCreditsAfterSlotSQL, []any{1, 300},
			[]string{`PRIMARY KEY`}},
		{"account event delete", deleteAccountRewardEventCreditsAfterSlotSQL, []any{1, 300},
			[]string{`idx_reward_event_credit_account`}},
		{"account withdrawal delete", deleteAccountRewardWithdrawalsAfterSlotSQL, []any{1, 300},
			[]string{`idx_reward_withdrawal_account_slot`}},
	} {
		for _, analyzed := range []bool{false, true} {
			t.Run(
				fmt.Sprintf("%s analyzed=%v", tc.name, analyzed),
				func(t *testing.T) {
					t.Parallel()
					store := newManagementTestStore(t)
					seedRewardJournalPlanRows(t, store)
					if analyzed {
						_, err := store.writeDB.Exec(`ANALYZE`)
						require.NoError(t, err)
					}
					plan := queryPlan(t, store.writeDB, tc.query, tc.args...)
					for _, want := range tc.want {
						require.Regexp(
							t,
							regexp.MustCompile(want),
							plan,
							"plan:\n%s",
							plan,
						)
					}
					require.NotRegexp(
						t,
						`SCAN (c|w|e|s|reward_\w+)\b`,
						plan,
						"plan:\n%s",
						plan,
					)
					require.NotContains(t, plan, "AUTOMATIC", "plan:\n%s", plan)
				},
			)
		}
	}
}
