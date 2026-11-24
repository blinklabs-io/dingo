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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// A block stays held while a payload column in use names its bytes, and is
// released once what used the payload is gone.
func TestBlockHoldsActivePayloads(t *testing.T) {
	t.Parallel()
	store, db := newManagementTestStoreWithRawDB(t)
	const slot, epoch = uint64(5), uint64(10)
	blockHash := bytes.Repeat([]byte{0xb1}, 32)
	otherHash := bytes.Repeat([]byte{0xb2}, 32)
	held := func(slot uint64, hash []byte) bool {
		t.Helper()
		got, err := store.BlockHoldsActivePayloads(t.Context(), slot, hash, epoch)
		require.NoError(t, err)
		return got
	}
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := db.Exec(query, args...)
		require.NoError(t, err, query)
	}
	require.False(t, held(slot, blockHash), "an empty store holds nothing")

	// A witness datum the block carries, named by hash from a live output.
	datumHash := bytes.Repeat([]byte{0xd1}, 32)
	exec(
		"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, ?)",
		datumHash, []byte{0x01}, slot,
	)
	require.False(t, held(slot, blockHash), "no live output names the datum")
	txID := bytes.Repeat([]byte{0xa1}, 32)
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId: txID, OutputIdx: 0, AddedSlot: slot + 1, DatumHash: datumHash,
	}))
	require.True(t, held(slot, blockHash), "a live output names the datum")
	require.False(t, held(slot+1, otherHash), "another slot is not held")
	exec("UPDATE utxo SET deleted_slot = ?", slot+2)
	require.False(t, held(slot, blockHash), "the output was spent")

	// A reference script of a live output.
	scriptHash := bytes.Repeat([]byte{0x22}, 28)
	exec(
		"INSERT INTO script (hash, content, created_slot, type) VALUES (?, ?, ?, 1)",
		scriptHash, []byte{0x02}, slot,
	)
	exec("UPDATE utxo SET deleted_slot = 0")
	exec("UPDATE utxo SET datum_hash = NULL")
	require.False(t, held(slot, blockHash))
	var utxoID int64
	require.NoError(t, db.QueryRow("SELECT id FROM utxo").Scan(&utxoID))
	exec(
		"INSERT INTO utxo_reference_script (utxo_id, script_hash) VALUES (?, ?)",
		utxoID, scriptHash,
	)
	require.True(t, held(slot, blockHash), "a live output carries the script")
	exec("UPDATE utxo SET deleted_slot = ?", slot+2)
	require.False(t, held(slot, blockHash))

	// A protocol parameter update not yet past the current epoch.
	exec(
		"INSERT INTO pparam_update (genesis_hash, cbor, added_slot, epoch) VALUES (?, ?, ?, ?)",
		bytes.Repeat([]byte{0x07}, 28), []byte{0x03}, slot, epoch,
	)
	require.True(t, held(slot, blockHash), "the update is pending")
	got, err := store.BlockHoldsActivePayloads(t.Context(), slot, blockHash, epoch+1)
	require.NoError(t, err)
	require.False(t, got, "the update was enacted or discarded")

	// An unspent Midnight registration of the block.
	exec(
		`INSERT INTO midnight_registrations
(full_datum, tx_hash, output_index, block_number, block_hash, tx_index, block_timestamp_ms)
VALUES (?, ?, 0, 5, ?, 0, 0)`,
		[]byte{0x04}, txID, otherHash,
	)
	require.True(t, held(slot+100, otherHash), "an unspent registration")
	exec(
		`INSERT INTO midnight_deregistrations
(full_datum, tx_hash, utxo_tx_hash, utxo_index, block_number, block_hash, tx_index, block_timestamp_ms)
VALUES (?, ?, ?, 0, 6, ?, 0, 0)`,
		[]byte{0x04}, bytes.Repeat([]byte{0x12}, 32), txID, blockHash,
	)
	require.False(t, held(slot+100, otherHash), "the registration was spent")

	// The latest Midnight Ariadne parameters and the current governance datum
	// of a type name their block; earlier rows are history.
	offsetOf := func(hash []byte) []byte {
		var h [32]byte
		copy(h[:], hash)
		return (&types.CborOffset{BlockSlot: 1, BlockHash: h, ByteLength: 1}).Encode()
	}
	older := bytes.Repeat([]byte{0xc1}, 32)
	newer := bytes.Repeat([]byte{0xc2}, 32)
	exec("INSERT INTO midnight_ariadne_params (epoch, datum) VALUES (1, ?)", offsetOf(older))
	exec("INSERT INTO midnight_ariadne_params (epoch, datum) VALUES (2, ?)", offsetOf(newer))
	require.True(t, held(slot+200, newer), "the latest Ariadne parameters")
	require.False(t, held(slot+200, older), "earlier Ariadne parameters are history")
	exec("INSERT INTO midnight_ariadne_params (epoch, datum) VALUES (3, ?)", offsetOf(blockHash))
	require.False(t, held(slot+200, newer), "superseded Ariadne parameters")

	for i, hash := range [][]byte{older, newer} {
		exec(
			`INSERT INTO midnight_governance_datums
(datum_type, tx_hash, output_index, datum, block_number) VALUES ('council', ?, 0, ?, ?)`,
			bytes.Repeat([]byte{byte(0x30 + i)}, 32), offsetOf(hash), 10+i,
		)
	}
	exec(
		`INSERT INTO midnight_governance_datums
(datum_type, tx_hash, output_index, datum, block_number) VALUES ('technical', ?, 0, ?, 5)`,
		bytes.Repeat([]byte{0x31}, 32), offsetOf(older),
	)
	require.True(t, held(slot+300, newer), "the current council datum")
	require.True(t, held(slot+300, older), "the only technical committee datum")
	exec("DELETE FROM midnight_governance_datums WHERE datum_type = 'technical'")
	require.False(t, held(slot+300, older), "a superseded governance datum")
}

// Each lookup that scales with the utxo, script or registration tables must
// be planned on an index, or the pruner scans the table once per block.
func TestActivePayloadQueriesUseIndexes(t *testing.T) {
	t.Parallel()
	_, db := newManagementTestStoreWithRawDB(t)
	for name, index := range map[string]string{
		"datum named by a live output":      "idx_utxo_live_datum_hash",
		"reference script of a live output": "idx_script_created_slot",
		"unspent Midnight registration":     "idx_midnight_registrations_block_hash",
	} {
		var query string
		var bySlot bool
		for _, q := range activePayloadQueries {
			if q.name == name {
				query, bySlot = q.query, q.bySlot
			}
		}
		require.NotEmpty(t, query, name)
		args := []any{[]byte{1}}
		if bySlot {
			args = []any{int64(1)}
		}
		rows, err := db.Query("EXPLAIN QUERY PLAN "+query, args...)
		require.NoError(t, err)
		var plan string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
			plan += detail + "\n"
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Contains(t, plan, index, "%s is planned without its index:\n%s", name, plan)
	}
}
