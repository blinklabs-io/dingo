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
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

type rewardJournalExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

// testRewardRow is a journal row in the shape fixtures are written in; the
// helper files it in the table that holds it.
type testRewardRow struct {
	key          []byte
	tag          uint8
	hash         []byte
	amount       string
	previous     string
	slot         int64
	withdrawal   bool
	postSnapshot bool
}

// insertTestRewardRow files row as a withdrawal (owned by a bare transaction
// row for hash) or an event credit (sourced by hash), creating the account
// row when it does not exist.
func insertTestRewardRow(
	t *testing.T,
	db rewardJournalExecer,
	row testRewardRow,
) {
	t.Helper()
	_, err := db.Exec(`
INSERT INTO account (staking_key, credential_tag, added_slot, created_slot, active, reward)
SELECT ?, ?, 0, 0, TRUE, '0'
WHERE NOT EXISTS (
    SELECT 1 FROM account WHERE staking_key = ? AND credential_tag = ?
)`, row.key, row.tag, row.key, row.tag)
	require.NoError(t, err)
	var accountID int64
	require.NoError(t, db.QueryRow(
		`SELECT id FROM account WHERE staking_key = ? AND credential_tag = ?`,
		row.key, row.tag,
	).Scan(&accountID))
	if row.withdrawal {
		_, err = db.Exec(
			`INSERT INTO "transaction" (hash, slot, block_index) VALUES (?, ?, 0)
ON CONFLICT (hash) DO NOTHING`, row.hash, row.slot)
		require.NoError(t, err)
		var txID int64
		require.NoError(t, db.QueryRow(
			`SELECT id FROM "transaction" WHERE hash = ?`, row.hash,
		).Scan(&txID))
		previous := row.previous
		if previous == "" {
			previous = "0"
		}
		_, err = db.Exec(`
INSERT INTO reward_withdrawal (tx_id, account_id, slot, amount, previous_reward)
VALUES (?, ?, ?, ?, ?)`, txID, accountID, row.slot, row.amount, previous)
		require.NoError(t, err)
		return
	}
	_, err = db.Exec(
		`INSERT INTO reward_source (hash) VALUES (?) ON CONFLICT (hash) DO NOTHING`,
		row.hash)
	require.NoError(t, err)
	var sourceID int64
	require.NoError(t, db.QueryRow(
		`SELECT id FROM reward_source WHERE hash = ?`, row.hash,
	).Scan(&sourceID))
	_, err = db.Exec(`
INSERT INTO reward_event_credit (source_id, account_id, slot, amount, post_snapshot)
VALUES (?, ?, ?, ?, ?)`, sourceID, accountID, row.slot, row.amount, row.postSnapshot)
	require.NoError(t, err)
}
