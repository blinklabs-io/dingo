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

package database

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// sqliteStore exposes the underlying sqlite store of a test Database so a test
// can seed rows directly.
func sqliteStore(t *testing.T, db *Database) *sqlite.MetadataStoreSqlite {
	t.Helper()
	store, ok := db.Metadata().(*sqlite.MetadataStoreSqlite)
	require.True(t, ok, "test Database is not backed by the sqlite plugin")
	return store
}

func h32(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

// TestRevokeEndorserTransactionClosureCascades verifies that revoking an
// endorser transaction also revokes the endorser transaction that spent one of
// its outputs (endorser-on-endorser), and that a non-endorser (ranking-block)
// spender is left untouched.
//
// Layout:
//
//	src#0 --spent by--> EB1 --produces--> EB1#0 --spent by--> EB2 (endorser)
//	                                      EB1#1 --spent by--> RB  (ranking, NOT endorser)
//
// Revoking the closure rooted at EB1 must revoke EB1 and EB2 (restoring src#0
// and deleting EB1#0, EB2's outputs) but must NOT revoke RB.
func TestRevokeEndorserTransactionClosureCascades(t *testing.T) {
	db := openTestDB(t)
	store := sqliteStore(t, db)

	src := h32(0x50)
	eb1, eb2, rb := h32(0xE1), h32(0xE2), h32(0x4B)

	// Transaction rows for every spender (spent_at_tx_id FK) plus the producers.
	for _, h := range [][]byte{eb1, eb2, rb} {
		require.NoError(t, store.DB().Create(&models.Transaction{
			Hash: h, BlockHash: h32(0x01), Slot: 200,
		}).Error)
	}

	// src#0 spent by EB1.
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: src, OutputIdx: 0, AddedSlot: 100,
		DeletedSlot: 200, SpentAtTxId: types.NullableHash(eb1),
	}).Error)
	// EB1#0 (spent by EB2) and EB1#1 (spent by RB).
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: eb1, OutputIdx: 0, AddedSlot: 200,
		DeletedSlot: 201, SpentAtTxId: types.NullableHash(eb2),
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: eb1, OutputIdx: 1, AddedSlot: 200,
		DeletedSlot: 202, SpentAtTxId: types.NullableHash(rb),
	}).Error)
	// EB2's output (unspent).
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: eb2, OutputIdx: 0, AddedSlot: 201,
	}).Error)

	// EB1 and EB2 are endorser transactions; RB is not.
	require.NoError(
		t,
		db.AddEndorserTransactions([][]byte{eb1, eb2}, 200, nil),
	)

	// Revoke the closure rooted at EB1 (the spender of src#0).
	revoked, err := db.RevokeEndorserTransactionClosure([][]byte{eb1}, nil)
	require.NoError(t, err)
	require.Equal(t, 2, revoked, "should revoke EB1 and EB2, not RB")

	txn := db.Transaction(false)
	defer txn.Release()

	// src#0 restored to unspent.
	srcUtxo, err := store.GetUtxoIncludingSpent(src, 0, txn.Metadata())
	require.NoError(t, err)
	require.NotNil(t, srcUtxo)
	require.Equal(t, uint64(0), srcUtxo.DeletedSlot)
	require.Empty(t, srcUtxo.SpentAtTxId)

	// EB1's and EB2's outputs are gone.
	for _, ref := range []struct {
		id  []byte
		idx uint32
	}{{eb1, 0}, {eb1, 1}, {eb2, 0}} {
		u, err := store.GetUtxoIncludingSpent(ref.id, ref.idx, txn.Metadata())
		require.NoError(t, err)
		require.Nil(t, u, "output %x#%d should be deleted", ref.id, ref.idx)
	}

	// EB1 and EB2 provenance is gone; RB was never endorser provenance and its
	// transaction row survives.
	remaining, err := db.FilterEndorserTransactions(
		[][]byte{eb1, eb2}, txn,
	)
	require.NoError(t, err)
	require.Empty(t, remaining)

	var rbCount int64
	require.NoError(t, store.DB().Model(&models.Transaction{}).
		Where("hash = ?", rb).Count(&rbCount).Error)
	require.Equal(t, int64(1), rbCount, "ranking-block tx must not be revoked")
}

// TestRevokeEndorserTransactionClosureIgnoresNonEndorserRoots verifies that a
// root that is not a recorded endorser transaction (a genuine ranking-block
// spender) revokes nothing, so a ranking-block-vs-ranking-block conflict is not
// masked.
func TestRevokeEndorserTransactionClosureIgnoresNonEndorserRoots(t *testing.T) {
	db := openTestDB(t)
	store := sqliteStore(t, db)

	rb := h32(0x4B)
	require.NoError(t, store.DB().Create(&models.Transaction{
		Hash: rb, BlockHash: h32(0x01), Slot: 200,
	}).Error)

	revoked, err := db.RevokeEndorserTransactionClosure([][]byte{rb}, nil)
	require.NoError(t, err)
	require.Equal(t, 0, revoked)

	var rbCount int64
	require.NoError(t, store.DB().Model(&models.Transaction{}).
		Where("hash = ?", rb).Count(&rbCount).Error)
	require.Equal(t, int64(1), rbCount)
}
