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

package accounthistory

import (
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/sqldialect"
	"gorm.io/gorm"
)

// QueryWithdrawalHistoryByCredential returns paginated withdrawal history
// rows for a stake credential, joining the rollback-aware
// account_reward_delta withdrawal rows against the transaction that made
// each withdrawal to recover its slot, position, and block hash.
func QueryWithdrawalHistoryByCredential(
	db *gorm.DB,
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
) ([]models.AccountWithdrawalHistoryRow, error) {
	ret := make([]models.AccountWithdrawalHistoryRow, 0)
	if len(stakingKey) == 0 {
		return ret, nil
	}

	query, args := withdrawalHistoryQuery(db, credentialTag, stakingKey)
	if strings.EqualFold(order, "asc") {
		query += " ORDER BY tx_slot ASC, block_index ASC, tx_hash ASC"
	} else {
		query += " ORDER BY tx_slot DESC, block_index DESC, tx_hash DESC"
	}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	if offset > 0 {
		query += " OFFSET ?"
		args = append(args, offset)
	}
	if err := db.Raw(query, args...).Scan(&ret).Error; err != nil {
		return nil, fmt.Errorf("get account withdrawal history: %w", err)
	}
	return ret, nil
}

// CountWithdrawalHistoryByCredential returns the total number of withdrawal
// history rows for a stake credential.
func CountWithdrawalHistoryByCredential(
	db *gorm.DB,
	credentialTag uint8,
	stakingKey []byte,
) (int, error) {
	if len(stakingKey) == 0 {
		return 0, nil
	}
	query, args := withdrawalHistoryQuery(db, credentialTag, stakingKey)
	var count int64
	if err := db.Raw(
		"SELECT COUNT(*) AS count FROM ("+query+") AS withdrawal_history",
		args...,
	).Scan(&count).Error; err != nil {
		return 0, fmt.Errorf("count account withdrawal history: %w", err)
	}
	return int(count), nil
}

// withdrawalHistoryQuery builds the shared SELECT (without ORDER BY/LIMIT)
// used by both the row and count queries above. account_reward_delta has no
// unique index on tx_hash alone, but transaction.hash is unique, so the join
// can never fan a single withdrawal row out into duplicates.
func withdrawalHistoryQuery(
	db *gorm.DB,
	credentialTag uint8,
	stakingKey []byte,
) (string, []any) {
	query := fmt.Sprintf(
		`SELECT account_reward_delta.tx_hash AS tx_hash,
			account_reward_delta.amount AS amount,
			tx.slot AS tx_slot,
			tx.block_index AS block_index,
			tx.block_hash AS block_hash
		FROM account_reward_delta
		INNER JOIN %s tx ON tx.hash = account_reward_delta.tx_hash
		WHERE account_reward_delta.withdrawal = ?
			AND account_reward_delta.credential_tag = ?
			AND account_reward_delta.staking_key = ?`,
		sqldialect.TransactionTableName(db),
	)
	return query, []any{true, credentialTag, stakingKey}
}
