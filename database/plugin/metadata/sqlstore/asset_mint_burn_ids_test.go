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
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
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

// A rollback deletes the mint events of the rolled-back transactions before
// their identity rows; the foreign key from asset_mint_burn to the
// transaction would otherwise refuse the identity delete. The asset identity
// row, a dictionary entry, stays.
func TestRollbackDeletesMintBurnEventsBeforeTransactionIdentity(t *testing.T) {
	t.Parallel()
	store := newForeignKeySQLiteStore(t)
	store.storageMode = types.StorageModeAPI
	txID := bytes32(0x93)
	mint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.NewBlake2b224(assetPolicy(0x0a)): {
				cbor.NewByteString([]byte("t")): big.NewInt(1),
			},
		},
	)
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(txID)
	tx.WithOutputs(producerTestOutput(t, 1_500_000))
	tx.WithMint(&mint)
	tx.WithValid(true)
	require.NoError(t, store.SetTransaction(
		tx, ocommon.Point{Slot: 10, Hash: txID}, 0, nil, false, nil, 0,
	))
	var events int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM asset_mint_burn`).Scan(&events))
	require.Equal(t, 1, events)

	require.NoError(t, store.DeleteTransactionsAfterSlot(5, nil))

	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM asset_mint_burn`).Scan(&events))
	require.Zero(t, events)
	require.Zero(t, identityCount(t, store, txID))
	var identities, violations int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM asset_identity`).Scan(&identities))
	require.Equal(t, 1, identities)
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM pragma_foreign_key_check`).Scan(&violations))
	require.Zero(t, violations)
}
