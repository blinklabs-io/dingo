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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// The fingerprint derives from the policy and name, so the history reader
// computes it instead of reading a stored column.
func TestAssetMintBurnHistoryDerivesFingerprint(t *testing.T) {
	t.Parallel()
	store, db := newManagementTestStoreWithRawDB(t)
	policy := assetPolicy(0x07)
	insertMintBurn(t, db, credentialHash(0x41), policy, "T", 10, 0, "3")

	rows, err := store.GetAssetMintBurnHistory(
		lcommon.NewBlake2b224(policy), []byte("T"), 0, 0, false, nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t,
		lcommon.NewAssetFingerprint(policy, []byte("T")).String(),
		string(rows[0].Fingerprint),
	)
	require.Equal(t, credentialHash(0x41), rows[0].TxHash)
}

func mintBurnPlan(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+query, args...)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
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

// The per-asset reads search idx_asset_mint_burn_asset_id for the asset's
// events in (slot, tx_index) order, the rollback's slot delete searches
// idx_asset_mint_burn_slot, and a transaction's events are a prefix of the
// primary key.
func TestAssetMintBurnQueryPlansUseTheirIndexes(t *testing.T) {
	t.Parallel()
	_, db := newManagementTestStoreWithRawDB(t)
	policy := assetPolicy(0x07)
	for i := range 50 {
		insertMintBurn(
			t, db, credentialHash(byte(i+1)), policy, "T", uint64(i), 0, "1",
		)
	}
	for i := range 200 {
		insertMintBurn(
			t, db, credentialHash(byte(i+1)), policy,
			"other"+string(rune('a'+i%26))+string(rune('a'+i/26)),
			uint64(100+i), 0, "1",
		)
	}
	_, err := db.Exec("ANALYZE")
	require.NoError(t, err)

	history := mintBurnPlan(t, db, `
SELECT t.hash, i.policy_id, i.name, m.slot, m.quantity, m.tx_index
FROM asset_identity AS i
JOIN asset_mint_burn AS m ON m.asset_id = i.id
JOIN "transaction" AS t ON t.id = m.transaction_id
WHERE i.policy_id = ? AND i.name = ?
ORDER BY m.slot ASC, m.tx_index ASC, m.transaction_id ASC`,
		policy, []byte("T"))
	require.Contains(t, history, "idx_asset_mint_burn_asset_id", history)
	require.NotContains(t, history, "TEMP B-TREE", history)

	rollback := mintBurnPlan(t, db,
		`DELETE FROM asset_mint_burn WHERE slot > ?`, 5)
	require.Contains(t, rollback, "idx_asset_mint_burn_slot", rollback)

	byTransaction := mintBurnPlan(t, db,
		`SELECT asset_id FROM asset_mint_burn WHERE transaction_id = ?`, 1)
	require.Contains(t, byTransaction, "PRIMARY KEY", byTransaction)
	require.NotContains(t, byTransaction, "SCAN", byTransaction)
}

// Queued events are written against the transaction's row id and the asset's
// identity id, one row per (transaction, asset) pair.
func TestAssetMintBurnFlushKeysByIdentityIDs(t *testing.T) {
	t.Parallel()
	_, db := newManagementTestStoreWithRawDB(t)
	policy := assetPolicy(0x09)
	testutil.InsertAssetMintBurn(
		t, db, credentialHash(0x51), policy, []byte("a"), 1, 0, "1",
	)
	var identities, events int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM asset_identity").Scan(&identities))
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM asset_mint_burn").Scan(&events))
	require.Equal(t, 1, identities)
	require.Equal(t, 1, events)
}
