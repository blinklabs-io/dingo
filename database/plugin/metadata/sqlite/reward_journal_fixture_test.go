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

package sqlite

import (
	"database/sql"
)

// insertRewardJournalRow files a journal row, written in the shape the
// removed account_reward_delta table had, as a withdrawal (owned by an applied
// transaction row for hash) or an event credit (sourced by hash). The account
// row is created when it does not exist.
func insertRewardJournalRow(
	db *sql.DB,
	key []byte,
	tag uint8,
	hash []byte,
	amount, previous string,
	slot int64,
	withdrawal, postSnapshot bool,
) error {
	if _, err := db.Exec(`
INSERT INTO account (staking_key, credential_tag, added_slot, created_slot, active, reward)
SELECT ?, ?, 0, 0, TRUE, '0'
WHERE NOT EXISTS (
    SELECT 1 FROM account WHERE staking_key = ? AND credential_tag = ?
)`, key, tag, key, tag); err != nil {
		return err
	}
	var accountID int64
	if err := db.QueryRow(
		`SELECT id FROM account WHERE staking_key = ? AND credential_tag = ?`,
		key, tag,
	).Scan(&accountID); err != nil {
		return err
	}
	if withdrawal {
		if _, err := db.Exec(
			`INSERT INTO "transaction" (hash) VALUES (?)
ON CONFLICT (hash) DO NOTHING`, hash); err != nil {
			return err
		}
		var txID int64
		if err := db.QueryRow(
			`SELECT id FROM "transaction" WHERE hash = ?`, hash,
		).Scan(&txID); err != nil {
			return err
		}
		if _, err := db.Exec(
			`INSERT INTO applied_transaction (tx_id, slot, block_index, valid)
VALUES (?, ?, 0, TRUE) ON CONFLICT (tx_id) DO NOTHING`, txID, slot); err != nil {
			return err
		}
		if previous == "" {
			previous = "0"
		}
		_, err := db.Exec(`
INSERT INTO reward_withdrawal (tx_id, account_id, slot, amount, previous_reward)
VALUES (?, ?, ?, ?, ?)`, txID, accountID, slot, amount, previous)
		return err
	}
	if _, err := db.Exec(
		`INSERT INTO reward_source (hash) VALUES (?) ON CONFLICT (hash) DO NOTHING`,
		hash); err != nil {
		return err
	}
	var sourceID int64
	if err := db.QueryRow(
		`SELECT id FROM reward_source WHERE hash = ?`, hash,
	).Scan(&sourceID); err != nil {
		return err
	}
	_, err := db.Exec(`
INSERT INTO reward_event_credit (source_id, account_id, slot, amount, post_snapshot)
VALUES (?, ?, ?, ?, ?)`, sourceID, accountID, slot, amount, postSnapshot)
	return err
}
