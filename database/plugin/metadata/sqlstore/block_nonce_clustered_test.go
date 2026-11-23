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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestBlockNonceIsClusteredBySlotAndHash(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	require.Equal(
		t,
		[]string{"slot", "hash", "ordinal", "nonce", "is_checkpoint"},
		tableColumnNames(t, store, "block_nonce"),
		"block_nonce keeps no rowid surrogate",
	)
	require.Empty(
		t, tableIndexNames(t, store, "block_nonce"),
		"the primary key is the only b-tree: no secondary index",
	)
	var ddl string
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'block_nonce'`,
	).Scan(&ddl))
	require.True(
		t, strings.Contains(ddl, "WITHOUT ROWID"),
		"block_nonce must be a clustered table: %s", ddl,
	)
}

// TestBlockNonceKeepsApplicationOrderWithinSlot pins the ordering the latest
// nonce and the slot range reads promise when several blocks share a slot:
// rows come back in the order they were first written, whether they were
// written together or one at a time, and rewriting a row keeps its place.
func TestBlockNonceKeepsApplicationOrderWithinSlot(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	hashes := [][]byte{{0x90}, {0x10}, {0x50}, {0x30}}
	require.NoError(t, store.SetBlockNonces([]models.BlockNonce{
		{Hash: hashes[0], Slot: 7, Nonce: []byte{1}},
		{Hash: []byte{0xAA}, Slot: 6, Nonce: []byte{9}},
		{Hash: hashes[1], Slot: 7, Nonce: []byte{2}},
	}, nil))
	require.NoError(t, store.SetBlockNonce(hashes[2], 7, []byte{3}, false, nil))
	require.NoError(t, store.SetBlockNonces([]models.BlockNonce{
		{Hash: hashes[3], Slot: 7, Nonce: []byte{4}},
	}, nil))

	order := func() ([][]byte, []uint) {
		rows, err := store.GetBlockNoncesInSlotRange(7, 8, nil)
		require.NoError(t, err)
		var got [][]byte
		var ids []uint
		for _, row := range rows {
			got = append(got, row.Hash)
			ids = append(ids, row.ID)
		}
		return got, ids
	}
	got, ids := order()
	require.Equal(t, hashes, got, "slot range returns application order")
	require.IsIncreasing(t, ids, "ids ascend with application order")
	latest, ok, err := store.GetLatestBlockNonce(nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, hashes[3], latest.Hash)
	require.Equal(t, ids[3], latest.ID)

	// Rewriting the first row replaces its nonce and keeps its place.
	require.NoError(t, store.SetBlockNonces([]models.BlockNonce{
		{Hash: hashes[0], Slot: 7, Nonce: []byte{5}, IsCheckpoint: true},
	}, nil))
	got, again := order()
	require.Equal(t, hashes, got)
	require.Equal(t, ids, again, "a rewrite keeps the row's id")
	stored, err := store.GetBlockNonce(ocommon.Point{Slot: 7, Hash: hashes[0]}, nil)
	require.NoError(t, err)
	require.Equal(t, []byte{5}, stored)
	latest, _, err = store.GetLatestBlockNonce(nil)
	require.NoError(t, err)
	require.Equal(t, hashes[3], latest.Hash, "a rewrite does not become latest")

	// Rollback to the point drops later slots and sibling blocks only.
	require.NoError(t, store.DeleteBlockNoncesAfterPoint(
		ocommon.Point{Slot: 7, Hash: hashes[1]}, nil,
	))
	got, _ = order()
	require.Equal(t, [][]byte{hashes[1]}, got)
}

// TestBlockNonceLatestReadsPrimaryKeyWithoutSort pins that the latest-nonce
// read descends the clustered key to the highest slot and orders only that
// slot's rows.
func TestBlockNonceLatestReadsPrimaryKeyWithoutSort(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	require.NoError(t, store.SetBlockNonces(blockNonceRows(0, 500), nil))
	plan := queryPlan(t, store.writeDB, latestBlockNonceQuery)
	require.Contains(t, plan, "PRIMARY KEY", plan)
	require.NotContains(t, plan, "SCAN block_nonce", plan)
}
