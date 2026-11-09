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
	"bytes"
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// seedRewardJournalPlanRows fills the journal with enough rows that a plan
// that ignores an index shows up as a scan.
func seedRewardJournalPlanRows(t *testing.T, store *Store) {
	t.Helper()
	tx, err := store.writeDB.Begin()
	require.NoError(t, err)
	for i := range 60 {
		key := bytes.Repeat([]byte{byte(i + 1)}, 28)
		insertTestRewardRow(t, tx, testRewardRow{
			key: key, hash: []byte(fmt.Sprintf("evt-%04d", i)), amount: "5",
			slot: int64(100 + i%7), postSnapshot: i%3 == 0,
		})
		insertTestRewardRow(t, tx, testRewardRow{
			key: key, hash: []byte(fmt.Sprintf("tx--%04d", i)), amount: "3",
			previous: "9", slot: int64(200 + i%11), withdrawal: true,
		})
		_, err := tx.Exec(`INSERT INTO reward_credit (boundary_slot, account_id, output_id, amount)
SELECT ?, id, ?, '4' FROM account WHERE staking_key = ?`, 300+(i%4)*100, i+1, key)
		require.NoError(t, err)
	}
	for _, slot := range []int{300, 400, 500, 600} {
		_, err := tx.Exec(`INSERT INTO reward_credit_slot (boundary_slot) VALUES (?)`, slot)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
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
		{"credit slots after a slot", rewardCreditSlotsAfterSQL, []any{300},
			[]string{`SEARCH reward_credit_slot USING (COVERING )?INDEX|SEARCH reward_credit_slot USING PRIMARY KEY`}},
		{"credits of a slot and account", roundRewardCreditsAtSlotSQL, []any{300, 1},
			[]string{`PRIMARY KEY \(boundary_slot=\? AND account_id=\?\)`}},
		{"event credits after a slot", rewardEventCreditsAfterSQL, []any{1, 150},
			[]string{`idx_reward_event_credit_account`}},
		{"event credits including post-snapshot", rewardEventCreditsAfterOrPostSnapshotSQL, []any{1, 150, 150},
			[]string{`idx_reward_event_credit_account`}},
		{"first withdrawal after a slot", firstRewardWithdrawalAfterSQL, []any{1, 150},
			[]string{`idx_reward_withdrawal_account_slot`}},
		{"first withdrawal from a slot", firstRewardWithdrawalFromSQL, []any{1, 150},
			[]string{`idx_reward_withdrawal_account_slot`}},
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
			t.Run(fmt.Sprintf("%s analyzed=%v", tc.name, analyzed), func(t *testing.T) {
				t.Parallel()
				store := newManagementTestStore(t)
				seedRewardJournalPlanRows(t, store)
				if analyzed {
					_, err := store.writeDB.Exec(`ANALYZE`)
					require.NoError(t, err)
				}
				plan := queryPlan(t, store.writeDB, tc.query, tc.args...)
				for _, want := range tc.want {
					require.Regexp(t, regexp.MustCompile(want), plan, "plan:\n%s", plan)
				}
				require.NotRegexp(t, `SCAN (c|w|e|reward_\w+)\b`, plan, "plan:\n%s", plan)
				require.NotContains(t, plan, "AUTOMATIC", "plan:\n%s", plan)
			})
		}
	}
}
