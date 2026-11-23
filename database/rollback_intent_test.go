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
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func rollbackIntentTestDB(t *testing.T) *Database {
	t.Helper()
	return rollbackIntentTestDBAs(t, "")
}

// rollbackIntentTestDBAs is rollbackIntentTestDB opened under role.
func rollbackIntentTestDBAs(t *testing.T, role string) *Database {
	t.Helper()
	dir := t.TempDir()
	db, err := newTestDatabaseAt(
		t, dir, dir, &Config{DataDir: dir, Role: role},
	)
	require.NoError(t, err)
	return db
}

func rolledBackBlockCount(t *testing.T, db *Database) int {
	t.Helper()
	keys, err := db.rolledBackBlockKeys(1 << 20)
	require.NoError(t, err)
	return len(keys)
}

func rollbackIntentTestBlock(id uint64) models.Block {
	return models.Block{
		ID:     id,
		Slot:   id * 10,
		Number: id,
		Hash:   bytes.Repeat([]byte{byte(id)}, 32),
		Cbor:   bytes.Repeat([]byte{0x80 + byte(id)}, 64),
		Type:   1,
	}
}

func TestRollbackIntentRoundTripsNamedBlocks(t *testing.T) {
	t.Parallel()
	db := rollbackIntentTestDB(t)
	_, pending, err := db.GetRollbackIntent()
	require.NoError(t, err)
	require.False(t, pending)

	intent := RollbackIntent{
		Point: ocommon.Point{Slot: 42, Hash: []byte{1, 2, 3}},
		Blocks: []RollbackIntentBlock{{
			Hash: []byte{4, 5, 6}, PrevHash: []byte{1, 2, 3},
			EndorserHash: []byte{7, 8}, EndorserSlot: 41,
			EndorserTxs: []RollbackIntentTxRange{
				{Start: 0, Count: 3}, {Start: 5, Count: 1},
			},
			ID: 9, Slot: 43, Number: 7, Type: 8,
		}},
	}
	require.NoError(t, db.SetRollbackIntent(intent))
	got, pending, err := db.GetRollbackIntent()
	require.NoError(t, err)
	require.True(t, pending)
	require.Equal(t, intent, got)

	require.Nil(t, db.Metadata(), "the ledger's database keeps no sync_state")
	require.ErrorIs(
		t,
		db.SetSyncState(RollbackIntentKey, "{}", nil),
		ErrBlockStoreChainState,
	)

	require.NoError(t, db.ClearRollbackIntent())
	_, pending, err = db.GetRollbackIntent()
	require.NoError(t, err)
	require.False(t, pending)
}

func TestSetRollbackIntentRefusesMoreThanARecordHolds(t *testing.T) {
	t.Parallel()
	db := rollbackIntentTestDB(t)
	blocks := make([]RollbackIntentBlock, MaxRollbackIntentBlocks+1)
	require.ErrorIs(
		t,
		db.SetRollbackIntent(RollbackIntent{Blocks: blocks}),
		ErrRollbackIntentTooLarge,
	)
	ranges := make([]RollbackIntentTxRange, maxRollbackIntentTxRanges+1)
	require.ErrorIs(
		t,
		db.SetRollbackIntent(RollbackIntent{
			Blocks: []RollbackIntentBlock{{EndorserTxs: ranges}},
		}),
		ErrRollbackIntentTooLarge,
	)
	require.ErrorIs(
		t,
		db.SetRollbackIntent(RollbackIntent{
			Blocks: []RollbackIntentBlock{{
				EndorserTxs: []RollbackIntentTxRange{{
					Count: maxRollbackIntentEndorserTxs + 1,
				}},
			}},
		}),
		ErrRollbackIntentTooLarge,
	)
	_, pending, err := db.GetRollbackIntent()
	require.NoError(t, err)
	require.False(t, pending, "a refused intent was stored")

	require.NoError(t, db.SetRollbackIntent(RollbackIntent{
		Blocks: blocks[:MaxRollbackIntentBlocks],
	}))
	got, pending, err := db.GetRollbackIntent()
	require.NoError(t, err)
	require.True(t, pending)
	require.Len(t, got.Blocks, MaxRollbackIntentBlocks)
}

// A restored block store can hold any value under the intent's name, so a
// record beyond what the writer produces is refused, not expanded.
func TestGetRollbackIntentRefusesAnUnreadableRecord(t *testing.T) {
	t.Parallel()
	record := func(blocks string) string {
		return `{"format_version":2,"slot":1,"hash":"01","blocks":` +
			blocks + `}`
	}
	longHash, err := json.Marshal(
		bytes.Repeat([]byte{1}, maxRollbackIntentHashBytes+1),
	)
	require.NoError(t, err)
	tests := map[string]string{
		"not json":        "{",
		"released format": `{"format_version":1,"slot":1,"hash":"01","blocks":[]}`,
		"unknown field":   `{"format_version":2,"slot":1,"hash":"01","blocks":[],"x":1}`,
		"block body":      record(`[{"hash":"AQ==","cbor":"gA=="}]`),
		"missing point":   `{"format_version":2,"blocks":[]}`,
		"empty hash above origin": `{"format_version":2,"slot":1,` +
			`"hash":"","blocks":[]}`,
		"too many blocks": record(
			"[" + strings.Repeat("{},", MaxRollbackIntentBlocks) + "{}]",
		),
		"long block hash": record(`[{"hash":` + string(longHash) + `}]`),
		"too many ranges": record(
			`[{"endorser_txs":[` +
				strings.Repeat("[0,1],", maxRollbackIntentTxRanges) +
				`[0,1]]}]`,
		),
		"too many endorser transactions": record(
			`[{"endorser_txs":[[0,1048577]]}]`,
		),
		"endorser range past the index space": record(
			`[{"endorser_txs":[[4294967295,2]]}]`,
		),
		"too large": strings.Repeat(" ", maxRollbackIntentEncodedBytes+1),
		"trailing":  record("[]") + "{}",
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := rollbackIntentTestDB(t)
			require.NoError(t, db.SetChainState(RollbackIntentKey, raw, nil))
			_, pending, err := db.GetRollbackIntent()
			require.ErrorIs(t, err, ErrInvalidRollbackIntent)
			require.False(t, pending)
			kept, err := db.RetainRollbackIntentBlocks(context.Background())
			require.NoError(t, err)
			require.Zero(t, kept)
		})
	}
}

// A retired block leaves the chain entirely -- it is found by neither point,
// hash nor index -- while its body stays readable as a rolled-back block
// until it is released.
func TestBlockRetireKeepsTheBodyOffTheChain(t *testing.T) {
	t.Parallel()
	db := rollbackIntentTestDB(t)
	kept := rollbackIntentTestBlock(1)
	retired := rollbackIntentTestBlock(2)
	require.NoError(t, db.BlockCreate(kept, nil))
	require.NoError(t, db.BlockCreate(retired, nil))
	point := ocommon.Point{Slot: retired.Slot, Hash: retired.Hash}

	onChain, found, err := db.RolledBackBlock(retired.Slot, retired.Hash)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, retired.Cbor, onChain)
	require.Zero(t, rolledBackBlockCount(t, db))

	txn := db.BlockBlobTxn()
	require.NoError(t, txn.Do(func(txn *Txn) error {
		return BlockRetireTxn(txn, retired)
	}))

	_, err = BlockByPoint(context.Background(), db, point)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	_, err = db.BlockByIndex(retired.ID, nil)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	_, err = db.BlockByIndex(kept.ID, nil)
	require.NoError(t, err)

	body, found, err := db.RolledBackBlock(retired.Slot, retired.Hash)
	require.NoError(t, err)
	require.True(t, found, "the retired body was not kept")
	require.Equal(t, retired.Cbor, body)
	require.Equal(t, 1, rolledBackBlockCount(t, db))

	// The same block added again is a chain block like any other, and
	// releasing the rolled-back body leaves it alone.
	require.NoError(t, db.BlockCreate(retired, nil))
	deleted, err := db.ReleaseRolledBackBlocks(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	require.Zero(t, rolledBackBlockCount(t, db))
	got, err := BlockByPoint(context.Background(), db, point)
	require.NoError(t, err)
	require.Equal(t, retired.Cbor, got.Cbor)

	deleted, err = db.ReleaseRolledBackBlocks(context.Background())
	require.NoError(t, err)
	require.Zero(t, deleted)
}

func TestReleaseRolledBackBlocksDeletesEveryKeptBody(t *testing.T) {
	t.Parallel()
	db := rollbackIntentTestDB(t)
	const kept = rolledBackBlockReleaseBatch + 3
	for i := range kept {
		hash := []byte{byte(i), byte(i >> 8), 1}
		require.NoError(
			t, db.RetainRolledBackBlock(uint64(i), hash, []byte{0x80}),
		)
	}
	require.Equal(t, kept, rolledBackBlockCount(t, db))
	_, found, err := db.RolledBackBlock(5, []byte{5, 0, 1})
	require.NoError(t, err)
	require.True(t, found)

	deleted, err := db.ReleaseRolledBackBlocks(context.Background())
	require.NoError(t, err)
	require.Equal(t, kept, deleted)
	require.Zero(t, rolledBackBlockCount(t, db))
	_, found, err = db.RolledBackBlock(5, []byte{5, 0, 1})
	require.NoError(t, err)
	require.False(t, found)

	// A body kept after a release is found by the next one.
	require.NoError(t, db.RetainRolledBackBlock(9, []byte{9}, []byte{0x80}))
	deleted, err = db.ReleaseRolledBackBlocks(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
}

func releasedRollbackIntentRow(t *testing.T, blocks ...models.Block) string {
	t.Helper()
	released := make([]releasedRollbackBlock, 0, len(blocks))
	for _, block := range blocks {
		released = append(released, releasedRollbackBlock{
			Hash: block.Hash, PrevHash: block.PrevHash, Cbor: block.Cbor,
			ID: block.ID, Slot: block.Slot, Number: block.Number,
			Type: block.Type,
		})
	}
	data, err := json.Marshal(map[string]any{
		"format_version": 1,
		"slot":           5,
		"hash":           "0a0b",
		"blocks":         released,
	})
	require.NoError(t, err)
	return string(data)
}

// A v0.94.2 database with a rollback pending carries the rolled-back bodies
// in its sync_state row; that release already deleted them from the block
// store. The open-time move puts each back as a rolled-back block and leaves
// a record that names them.
func TestOpenMovesAReleasedRollbackIntentIntoTheBlockStore(t *testing.T) {
	t.Parallel()
	db := rollbackIntentTestDBAs(t, RoleReleased)
	first, second := rollbackIntentTestBlock(3), rollbackIntentTestBlock(2)
	first.PrevHash = second.Hash
	require.NoError(t, db.Metadata().SetSyncState(
		RollbackIntentKey, releasedRollbackIntentRow(t, first, second), nil,
	))

	for range 2 {
		require.NoError(
			t, db.moveChainStateToBlockStore(context.Background()),
		)
		row, err := db.Metadata().GetSyncState(RollbackIntentKey, nil)
		require.NoError(t, err)
		require.Empty(t, row, "the released row was left behind")

		intent, pending, err := db.GetRollbackIntent()
		require.NoError(t, err)
		require.True(t, pending)
		require.Equal(
			t, ocommon.Point{Slot: 5, Hash: []byte{0x0a, 0x0b}}, intent.Point,
		)
		require.Equal(t, []RollbackIntentBlock{
			{
				Hash: first.Hash, PrevHash: first.PrevHash, ID: first.ID,
				Slot: first.Slot, Number: first.Number, Type: first.Type,
			},
			{
				Hash: second.Hash, ID: second.ID, Slot: second.Slot,
				Number: second.Number, Type: second.Type,
			},
		}, intent.Blocks)
		for _, block := range []models.Block{first, second} {
			body, found, err := db.RolledBackBlock(block.Slot, block.Hash)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, block.Cbor, body)
		}
	}
}

// An open that stopped after writing the block-store record and before
// removing the row must not replace the record, which the ledger may have
// rewritten since, with the row's older content.
func TestOpenKeepsTheBlockStoreRollbackIntentOverALeftoverRow(t *testing.T) {
	t.Parallel()
	db := rollbackIntentTestDBAs(t, RoleReleased)
	held := RollbackIntent{Point: ocommon.Point{Slot: 9, Hash: []byte{9}}}
	require.NoError(t, db.SetRollbackIntent(held))
	require.NoError(t, db.Metadata().SetSyncState(
		RollbackIntentKey,
		releasedRollbackIntentRow(t, rollbackIntentTestBlock(3)),
		nil,
	))
	require.NoError(t, db.moveChainStateToBlockStore(context.Background()))

	got, pending, err := db.GetRollbackIntent()
	require.NoError(t, err)
	require.True(t, pending)
	require.Equal(t, held.Point, got.Point)
	require.Empty(t, got.Blocks)
	require.Zero(t, rolledBackBlockCount(t, db))
	row, err := db.Metadata().GetSyncState(RollbackIntentKey, nil)
	require.NoError(t, err)
	require.Empty(t, row)
}

func TestOpenDiscardsAnUnreadableReleasedRollbackIntent(t *testing.T) {
	t.Parallel()
	rows := map[string]string{
		"not json": "{",
		"unknown field": `{"format_version":1,"slot":1,"hash":"01",` +
			`"blocks":[],"endorser":1}`,
		"other version": `{"format_version":2,"slot":1,"hash":"01","blocks":[]}`,
		"too many blocks": `{"format_version":1,"slot":1,"hash":"01","blocks":[` +
			strings.Repeat(
				"{},",
				MaxRollbackIntentBlocks,
			) + `{}]}`,
		"missing point": `{"format_version":1,"blocks":[]}`,
	}
	for name, row := range rows {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := rollbackIntentTestDBAs(t, RoleReleased)
			require.NoError(t, db.Metadata().SetSyncState(
				RollbackIntentKey, row, nil,
			))
			require.NoError(
				t, db.moveChainStateToBlockStore(context.Background()),
			)
			_, pending, err := db.GetRollbackIntent()
			require.NoError(t, err)
			require.False(t, pending)
			left, err := db.Metadata().GetSyncState(RollbackIntentKey, nil)
			require.NoError(t, err)
			require.Empty(t, left)
			require.Zero(t, rolledBackBlockCount(t, db))
		})
	}
}

func TestClearImportedChainStateDropsTheRollbackIntent(t *testing.T) {
	t.Parallel()
	db := rollbackIntentTestDB(t)
	require.NoError(t, db.SetRollbackIntent(RollbackIntent{
		Point: ocommon.Point{Slot: 9, Hash: []byte{9}},
	}))
	require.NoError(t, db.ClearImportedChainState())
	_, pending, err := db.GetRollbackIntent()
	require.NoError(t, err)
	require.False(t, pending)
}

func TestRolledBackBlockKeyRefusesAnUnboundedHash(t *testing.T) {
	t.Parallel()
	_, err := rolledBackBlockKey(1, nil)
	require.Error(t, err)
	_, err = rolledBackBlockKey(
		1, bytes.Repeat([]byte{1}, maxRollbackIntentHashBytes+1),
	)
	require.Error(t, err)
	key, err := rolledBackBlockKey(1, []byte{7})
	require.NoError(t, err)
	require.Equal(
		t,
		append(
			[]byte(types.RolledBackBlockKeyPrefix),
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			1,
			7,
		),
		key,
	)
}
