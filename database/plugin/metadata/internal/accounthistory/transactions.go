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

// QueryAddressTransactionsByCredential returns one page of (payment
// address, transaction) association rows for a stake credential, ordered
// by (slot, tx_index, payment_key) so results are deterministic even when
// several addresses share a transaction. The optional from/to positions
// are applied as a SQL predicate, and LIMIT/OFFSET are the final word: no
// row this query does not return is ever inspected, so a page-size request
// costs work proportional to the page, not to the credential's full
// transaction history.
func QueryAddressTransactionsByCredential(
	db *gorm.DB,
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	from *models.AddressTransactionPosition,
	to *models.AddressTransactionPosition,
) ([]models.AccountTransactionAssociationRow, error) {
	ret := make([]models.AccountTransactionAssociationRow, 0)
	if len(stakingKey) == 0 {
		return ret, nil
	}

	query, args := addressTransactionRangeQuery(
		db,
		credentialTag,
		stakingKey,
		from,
		to,
	)
	if strings.EqualFold(order, "asc") {
		query += " ORDER BY at.slot ASC, at.tx_index ASC, at.payment_key ASC"
	} else {
		query += " ORDER BY at.slot DESC, at.tx_index DESC, at.payment_key DESC"
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
		return nil, fmt.Errorf(
			"get address transactions by credential: %w",
			err,
		)
	}
	return ret, nil
}

// CountAddressTransactionsByCredential returns the total number of
// (payment address, transaction) association rows for a stake credential
// within the same optional from/to range.
func CountAddressTransactionsByCredential(
	db *gorm.DB,
	credentialTag uint8,
	stakingKey []byte,
	from *models.AddressTransactionPosition,
	to *models.AddressTransactionPosition,
) (int, error) {
	if len(stakingKey) == 0 {
		return 0, nil
	}
	query, args := addressTransactionRangeQuery(
		db,
		credentialTag,
		stakingKey,
		from,
		to,
	)
	var count int64
	if err := db.Raw(
		"SELECT COUNT(*) AS count FROM ("+query+") AS address_transaction_range",
		args...,
	).Scan(&count).Error; err != nil {
		return 0, fmt.Errorf(
			"count address transactions by credential: %w",
			err,
		)
	}
	return int(count), nil
}

// addressTransactionRangeQuery builds the shared SELECT (without ORDER
// BY/LIMIT) used by both the row and count queries above. address_transaction
// already carries slot/tx_index directly (no join needed for the range
// predicate); the join to the transaction table only resolves tx_hash and
// block_hash, which are not duplicated onto address_transaction.
//
// The from/to bound is a two-column range comparison against (at.slot,
// at.tx_index) that must fold into an index range scan against
// idx_addr_tx_stake_position rather than degrade into a row-by-row filter
// after the credential lookup - otherwise a page-size request would cost
// work proportional to the row's position within the credential's history
// instead of to the page. There are two logically equivalent ways to write
// that comparison, and which one achieves that depends on the backend:
// sqlite and postgres fold the row-value form, "(slot, tx_index) >= (?,
// ?)", directly into the index seek, while mysql's optimizer does not
// (MySQL Bug #104128, #111952) and instead needs the expanded "slot > ? OR
// (slot = ? AND tx_index >= ?)" form. sqldialect.TwoColumnRangeCondition
// picks the form proven correct for db's dialect - see its doc comment for
// the EXPLAIN evidence from all three backends, and
// transactions_test.go/DATABASE.md for the pinned per-dialect regression
// coverage.
func addressTransactionRangeQuery(
	db *gorm.DB,
	credentialTag uint8,
	stakingKey []byte,
	from *models.AddressTransactionPosition,
	to *models.AddressTransactionPosition,
) (string, []any) {
	query := fmt.Sprintf(
		`SELECT at.payment_key AS payment_key,
			tx.hash AS tx_hash,
			at.slot AS tx_slot,
			at.tx_index AS tx_index,
			tx.block_hash AS block_hash
		FROM address_transaction at
		INNER JOIN %s tx ON tx.id = at.transaction_id
		WHERE at.credential_tag = ?
			AND at.staking_key = ?`,
		sqldialect.TransactionTableName(db),
	)
	args := []any{credentialTag, stakingKey}
	if from != nil {
		cond, condArgs := sqldialect.TwoColumnRangeCondition(
			db, "at.slot", "at.tx_index", ">=", from.Slot, from.TxIndex,
		)
		query += " AND " + cond
		args = append(args, condArgs...)
	}
	if to != nil {
		cond, condArgs := sqldialect.TwoColumnRangeCondition(
			db, "at.slot", "at.tx_index", "<=", to.Slot, to.TxIndex,
		)
		query += " AND " + cond
		args = append(args, condArgs...)
	}
	return query, args
}
