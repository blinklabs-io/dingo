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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// newAddressTransactionsTestDB builds an in-memory sqlite DB with the real
// (non-legacy) AddressTransaction and Transaction schemas, so AutoMigrate
// creates idx_addr_tx_stake_position exactly as it would on a fresh
// production database.
func newAddressTransactionsTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.AddressTransaction{}, &models.Transaction{}))
	return db
}

// seedAddressTransactions creates n transactions, each with one
// address_transaction row under the same stake credential at increasing
// slots (tx_index 0), so a query over the whole credential has a
// meaningful history to sort/range over.
func seedAddressTransactions(
	t *testing.T,
	db *gorm.DB,
	n int,
) (credentialTag uint8, stakingKey []byte) {
	t.Helper()
	stakingKey = make([]byte, 28)
	for i := range stakingKey {
		stakingKey[i] = 0xAB
	}
	for i := range n {
		hash := make([]byte, 32)
		hash[0] = byte(i)
		hash[1] = byte(i >> 8)
		require.NoError(t, db.Create(&models.Transaction{
			ID:         uint(i + 1), //nolint:gosec
			Hash:       hash,
			BlockHash:  make([]byte, 32),
			Slot:       uint64(i),
			BlockIndex: 0,
		}).Error)
		paymentKey := make([]byte, 28)
		paymentKey[0] = byte(i)
		require.NoError(t, db.Create(&models.AddressTransaction{
			PaymentKey:    paymentKey,
			StakingKey:    stakingKey,
			CredentialTag: 0,
			TransactionID: uint(i + 1), //nolint:gosec
			Slot:          uint64(i),
			TxIndex:       0,
		}).Error)
	}
	return 0, stakingKey
}

// explainPlan runs EXPLAIN QUERY PLAN for query/args and returns the
// joined detail column, following the established pattern in
// sqlite/utxo_test.go, sqlite/drep_test.go, and
// internal/rewardstate/livestake_test.go.
func explainPlan(t *testing.T, db *gorm.DB, query string, args []any) string {
	t.Helper()
	rows, err := db.Raw("EXPLAIN QUERY PLAN "+query, args...).Rows()
	require.NoError(t, err)
	defer rows.Close()

	var details []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
		details = append(details, detail)
	}
	require.NoError(t, rows.Err())
	return strings.Join(details, "\n")
}

// TestQueryAddressTransactionsByCredentialUsesStakePositionIndexAsc is the
// EXPLAIN QUERY PLAN regression test for the unbounded-sort bug:
// idx_addr_tx_stake_position must drive both the credential lookup and the
// ORDER BY, so LIMIT can short-circuit instead of sorting the credential's
// entire history in a temp B-tree.
func TestQueryAddressTransactionsByCredentialUsesStakePositionIndexAsc(t *testing.T) {
	db := newAddressTransactionsTestDB(t)
	credentialTag, stakingKey := seedAddressTransactions(t, db, 2000)

	query, args := addressTransactionRangeQuery(
		db, credentialTag, stakingKey, nil, nil,
	)
	query += " ORDER BY at.slot ASC, at.tx_index ASC, at.payment_key ASC LIMIT ?"
	args = append(args, 100)

	plan := explainPlan(t, db, query, args)
	assert.Contains(t, plan, "idx_addr_tx_stake_position",
		"expected the composite index to drive the search; plan was:\n%s", plan)
	assert.NotContains(t, plan, "USE TEMP B-TREE FOR ORDER BY",
		"sort must be satisfied by the index, not a temp B-tree over the "+
			"whole credential history; plan was:\n%s", plan)
}

// TestQueryAddressTransactionsByCredentialUsesStakePositionIndexDesc mirrors
// the ascending case for order=desc: SQLite can walk a B-tree index
// backwards, but that must be confirmed rather than assumed.
func TestQueryAddressTransactionsByCredentialUsesStakePositionIndexDesc(t *testing.T) {
	db := newAddressTransactionsTestDB(t)
	credentialTag, stakingKey := seedAddressTransactions(t, db, 2000)

	query, args := addressTransactionRangeQuery(
		db, credentialTag, stakingKey, nil, nil,
	)
	query += " ORDER BY at.slot DESC, at.tx_index DESC, at.payment_key DESC LIMIT ?"
	args = append(args, 100)

	plan := explainPlan(t, db, query, args)
	assert.Contains(t, plan, "idx_addr_tx_stake_position",
		"expected the composite index to drive the search; plan was:\n%s", plan)
	assert.NotContains(t, plan, "USE TEMP B-TREE FOR ORDER BY",
		"descending order must also be served by a backward index scan, "+
			"not a temp B-tree; plan was:\n%s", plan)
}

// TestQueryAddressTransactionsByCredentialRangeIsIndexSeek verifies that an
// inclusive from/to (slot, tx_index) range is folded into the
// idx_addr_tx_stake_position index seek itself (a genuine range scan), not
// merely a post-search row filter after the index locates the credential.
// The row-value comparison form is what makes this possible: SQLite's
// index USING clause lists the (slot, tx_index) bounds only when written
// as "(slot, tx_index) >= (?, ?)"; the logically equivalent OR form does
// not get folded into the seek.
func TestQueryAddressTransactionsByCredentialRangeIsIndexSeek(t *testing.T) {
	db := newAddressTransactionsTestDB(t)
	credentialTag, stakingKey := seedAddressTransactions(t, db, 2000)

	from := &models.AddressTransactionPosition{Slot: 500, TxIndex: 0}
	to := &models.AddressTransactionPosition{Slot: 1500, TxIndex: 0}
	query, args := addressTransactionRangeQuery(
		db, credentialTag, stakingKey, from, to,
	)
	query += " ORDER BY at.slot ASC, at.tx_index ASC, at.payment_key ASC LIMIT ?"
	args = append(args, 100)

	plan := explainPlan(t, db, query, args)
	assert.NotContains(t, plan, "USE TEMP B-TREE FOR ORDER BY",
		"range query sort must be satisfied by the index; plan was:\n%s", plan)
	assert.Contains(t, plan, "idx_addr_tx_stake_position",
		"expected the composite index to drive the range search; plan was:\n%s",
		plan)
	assert.Contains(t, plan, "(slot,tx_index)>",
		"expected the from bound to be folded into the index seek as a "+
			"range constraint, not applied as a post-search filter; plan was:\n%s",
		plan)
	assert.Contains(t, plan, "(slot,tx_index)<",
		"expected the to bound to be folded into the index seek as a "+
			"range constraint, not applied as a post-search filter; plan was:\n%s",
		plan)
}
