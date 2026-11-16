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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

type addressOrderFixture struct {
	store *Store
	addr  []byte
}

// seedAddressOrderStore stores outputs for one address in API mode. Each spec
// is a transaction (id, applied slot, block index; slot 0 means never
// applied), and its outputs share the address.
func seedAddressOrderStore(t *testing.T) *addressOrderFixture {
	t.Helper()
	store := newMigratedSQLiteStore(t)
	store.storageMode = types.StorageModeAPI
	fx := &addressOrderFixture{store: store, addr: []byte("addr_test1order")}
	type tx struct {
		id         int64
		slot       int64
		blockIndex int64
		outputs    int
	}
	// Ids deliberately disagree with chain order so a sort by id fails.
	txs := []tx{
		{1, 30, 1, 2},
		{2, 10, 4, 1},
		{3, 10, 2, 3},
		{4, 0, 0, 2}, // imported: never applied, takes added_slot
		{5, 20, 0, 1},
	}
	ctx := context.Background()
	for _, x := range txs {
		hash := bytes.Repeat([]byte{byte(x.id)}, 32)
		_, err := store.writeDB.ExecContext(ctx,
			`INSERT INTO "transaction" (id, hash) VALUES (?, ?)`, x.id, hash)
		require.NoError(t, err)
		if x.slot != 0 {
			_, err = store.writeDB.ExecContext(ctx,
				`INSERT INTO applied_transaction (tx_id, slot, block_index)
VALUES (?, ?, ?)`, x.id, x.slot, x.blockIndex)
			require.NoError(t, err)
		}
		for o := 0; o < x.outputs; o++ {
			added := x.slot
			if added == 0 {
				added = 15
			}
			result, err := store.writeDB.ExecContext(ctx,
				`INSERT INTO utxo (tx_id, output_idx, amount, added_slot,
deleted_slot) VALUES (?, ?, '1', ?, 0)`, x.id, o, added)
			require.NoError(t, err)
			utxoID, err := result.LastInsertId()
			require.NoError(t, err)
			require.NoError(t, store.SetUtxoAddress(uint(utxoID), fx.addr, nil))
		}
	}
	// A spent output must stay out of the page.
	_, err := store.writeDB.ExecContext(ctx,
		`UPDATE utxo SET deleted_slot = 99 WHERE tx_id = 3 AND output_idx = 1`)
	require.NoError(t, err)
	return fx
}

type orderKey struct {
	slot       uint64
	blockIndex uint32
	txID       byte
	outputIdx  uint32
}

func (fx *addressOrderFixture) page(
	t *testing.T,
	query models.UtxoWithOrderingQuery,
) []orderKey {
	t.Helper()
	query.IndexedExactAddress = fx.addr
	query.SkipAssets = true
	rows, err := fx.store.GetUtxosByAddressWithOrdering(&query, nil)
	require.NoError(t, err)
	keys := make([]orderKey, len(rows))
	for i, row := range rows {
		keys[i] = orderKey{
			row.TxSlot, row.TxBlockIndex, row.TxId[0], row.OutputIdx,
		}
	}
	return keys
}

func reverseKeys(keys []orderKey) []orderKey {
	ret := make([]orderKey, len(keys))
	for i, key := range keys {
		ret[len(keys)-1-i] = key
	}
	return ret
}

// TestIndexedAddressPageOrder checks an indexed exact-address page follows
// the producing transaction's chain position, with an unapplied producer at
// its output's added_slot, in both directions, with offsets and keyset
// bounds.
func TestIndexedAddressPageOrder(t *testing.T) {
	t.Parallel()
	fx := seedAddressOrderStore(t)
	want := []orderKey{
		{10, 2, 3, 0},
		{10, 2, 3, 2},
		{10, 4, 2, 0},
		{15, 0, 4, 0},
		{15, 0, 4, 1},
		{20, 0, 5, 0},
		{30, 1, 1, 0},
		{30, 1, 1, 1},
	}
	require.Equal(t, want, fx.page(t, models.UtxoWithOrderingQuery{}))
	require.Equal(
		t,
		reverseKeys(want),
		fx.page(t, models.UtxoWithOrderingQuery{Descending: true}),
	)
	require.Equal(
		t,
		want[2:5],
		fx.page(t, models.UtxoWithOrderingQuery{Limit: 3, Offset: 2}),
	)
	after := want[3]
	require.Equal(
		t,
		want[4:7],
		fx.page(t, models.UtxoWithOrderingQuery{
			Limit: 3,
			After: &models.UtxoOrderingCursor{
				Slot:       after.slot,
				BlockIndex: after.blockIndex,
				OutputIdx:  after.outputIdx,
				TxId:       bytes.Repeat([]byte{after.txID}, 32),
			},
		}),
	)
}

// TestIndexedAddressPageFollowsLaterApplication checks that applying a
// transaction after its output was stored moves the output to the applied
// position.
func TestIndexedAddressPageFollowsLaterApplication(t *testing.T) {
	t.Parallel()
	fx := seedAddressOrderStore(t)
	hash := bytes.Repeat([]byte{4}, 32)
	_, _, err := fx.store.insertTransactionUnbatched(
		context.Background(), fx.store.writeDB,
		hash, nil, nil, uint64(5), 0, "0", "0", "0", uint32(7), true,
	)
	require.NoError(t, err)
	keys := fx.page(t, models.UtxoWithOrderingQuery{})
	require.Equal(t, orderKey{5, 7, 4, 0}, keys[0])
	require.Equal(t, orderKey{5, 7, 4, 1}, keys[1])
}

// TestIndexedAddressPageStatementAvoidsSort checks the statement the store
// runs for an exact-address page: it must read the ordering index in order,
// in either direction and under a keyset bound, rather than sort the
// address's live set.
func TestIndexedAddressPageStatementAvoidsSort(t *testing.T) {
	t.Parallel()
	fx := seedAddressOrderStore(t)
	cursor := &models.UtxoOrderingCursor{Slot: 15, BlockIndex: 0, OutputIdx: 0}
	for name, query := range map[string]models.UtxoWithOrderingQuery{
		"ascending":  {Limit: 10},
		"descending": {Limit: 10, Descending: true},
		"keyset":     {Limit: 10, After: cursor},
		"offset":     {Limit: 10, Offset: 20},
	} {
		t.Run(name, func(t *testing.T) {
			query.IndexedExactAddress = fx.addr
			from, predicate, args, err := utxoOrderingClauses(&query, true)
			require.NoError(t, err)
			var afterTxID any
			if query.After != nil {
				afterTxID = int64(1)
			}
			statement, args := utxoOrderingStatement(
				&query, from, predicate, args, afterTxID,
			)
			plan := queryPlan(t, fx.store.writeDB, statement, args...)
			require.Contains(t, plan, "idx_utxo_address_order")
			require.NotContains(t, plan, "TEMP B-TREE")
		})
	}
}

// TestIndexedAddressJoinSyntaxFollowsDialect checks CROSS JOIN carries an ON
// predicate only where the dialect accepts it: PostgreSQL rejects ON after
// CROSS JOIN, so every non-SQLite caller gets a plain JOIN.
func TestIndexedAddressJoinSyntaxFollowsDialect(t *testing.T) {
	t.Parallel()
	query := &models.UtxoWithOrderingQuery{IndexedExactAddress: []byte("addr")}
	sqliteFrom, _, _, err := utxoOrderingClauses(query, true)
	require.NoError(t, err)
	require.Contains(t, sqliteFrom, "CROSS JOIN utxo ON")
	otherFrom, _, _, err := utxoOrderingClauses(query, false)
	require.NoError(t, err)
	require.NotContains(t, otherFrom, "CROSS JOIN")
	require.Contains(t, otherFrom, "JOIN utxo ON utxo.id = utxo_address.utxo_id")
}
