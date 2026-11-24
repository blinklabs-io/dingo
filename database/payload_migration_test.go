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
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// MigratePayloadOffsets reads each row's block from the block stores: a
// released store's copies become offsets into the canonical block at the
// row's slot, and every reader returns the bytes the copy held.
func TestMigratePayloadOffsetsReadsBlocksFromTheBlockStores(t *testing.T) {
	t.Parallel()
	pair := newIndexerTestDB(t, nil)
	datum := []byte{0xd8, 0x79, 0x9f, 0x01, 0xff}
	script := []byte{0x49, 0x01, 0x00, 0x00, 0x22, 0x23, 0x32, 0x00}
	metadata := []byte{0xa1, 0x19, 0x02, 0xd1, 0x65, 'h', 'e', 'l', 'l', 'o'}
	block := models.Block{
		ID: 1, Slot: 100, Number: 1, Type: 1,
		Hash: bytes.Repeat([]byte{0xc1}, 32),
		Cbor: bytes.Join([][]byte{
			[]byte("header"), datum, script, metadata, []byte("trailer"),
		}, []byte{'|'}),
	}
	require.NoError(t, pair.ledger.BlockCreate(block, nil))

	raw := rawSQLiteMetadataFixture(t, pair.indexer)
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := raw.Exec(query, args...)
		require.NoError(t, err, query)
	}
	datumHash := lcommon.Blake2b256Hash(datum)
	scriptHash := bytes.Repeat([]byte{0x22}, 28)
	txHash := bytes.Repeat([]byte{0x11}, 32)
	exec(
		"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, 100)",
		datumHash.Bytes(), datum,
	)
	exec(
		"INSERT INTO script (hash, content, created_slot, type) VALUES (?, ?, 100, 1)",
		scriptHash, script,
	)
	exec(`INSERT INTO "transaction" (id, hash) VALUES (1, ?)`, txHash)
	exec(
		`INSERT INTO applied_transaction
(tx_id, block_hash, metadata, slot, type, fee, collateral_fee, ttl, block_index, valid)
VALUES (1, ?, ?, 100, 0, '0', '0', '0', 0, 1)`,
		block.Hash, metadata,
	)

	require.NoError(t, pair.indexer.MigratePayloadOffsets(t.Context()))

	gotDatum, err := pair.indexer.GetDatum(t.Context(), datumHash.Bytes(), nil)
	require.NoError(t, err)
	require.Equal(t, datum, gotDatum.RawDatum)
	gotMetadata, err := pair.indexer.GetTransactionMetadataByHash(
		t.Context(), txHash, nil,
	)
	require.NoError(t, err)
	require.Equal(t, metadata, gotMetadata)
	var column []byte
	require.NoError(t, raw.QueryRow("SELECT content FROM script").Scan(&column))
	offset, err := types.DecodeCborOffset(column)
	require.NoError(t, err, "the script column holds an offset")
	require.EqualValues(t, 100, offset.BlockSlot)
	require.Equal(t, block.Hash, offset.BlockHash[:])
	require.Equal(
		t, script,
		block.Cbor[offset.ByteOffset:offset.ByteOffset+offset.ByteLength],
	)

	// A second run converts nothing and still reads the same bytes.
	require.NoError(t, pair.indexer.MigratePayloadOffsets(t.Context()))
	gotDatum, err = pair.indexer.GetDatum(t.Context(), datumHash.Bytes(), nil)
	require.NoError(t, err)
	require.Equal(t, datum, gotDatum.RawDatum)
}

// A row whose block the stores do not hold cannot be converted: the migration
// stops with the slot named instead of keeping the copy.
func TestMigratePayloadOffsetsFailsWithoutTheRowsBlock(t *testing.T) {
	t.Parallel()
	pair := newIndexerTestDB(t, nil)
	raw := rawSQLiteMetadataFixture(t, pair.indexer)
	datum := []byte{0xd8, 0x79, 0x80}
	_, err := raw.Exec(
		"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, 999)",
		lcommon.Blake2b256Hash(datum).Bytes(), datum,
	)
	require.NoError(t, err)

	err = pair.indexer.MigratePayloadOffsets(t.Context())
	require.ErrorContains(t, err, "slot 999")
	var column []byte
	require.NoError(
		t, raw.QueryRow("SELECT raw_datum FROM datum").Scan(&column),
	)
	require.Equal(t, datum, column, "the datum copy was replaced")
}

// With no archive configured, the payload of a block that expired locally is
// not kept: the migration completes, the column holds a marker with no bytes,
// and a read fails naming the expired block instead of returning empty or
// wrong bytes. Rows of blocks that are still stored convert as usual.
func TestMigratePayloadOffsetsDropsPayloadsOfExpiredBlocksWithoutAnArchive(
	t *testing.T,
) {
	t.Parallel()
	pair := newIndexerTestDB(t, nil)
	expiredDatum := []byte{0xd8, 0x79, 0x9f, 0x01, 0xff}
	keptDatum := []byte{0xd8, 0x79, 0x9f, 0x02, 0xff}
	expired := models.Block{
		ID: 1, Slot: 100, Number: 1, Type: 1,
		Hash: bytes.Repeat([]byte{0xe1}, 32),
		Cbor: bytes.Join([][]byte{[]byte("old"), expiredDatum}, []byte{'|'}),
	}
	kept := models.Block{
		ID: 2, Slot: 200, Number: 2, Type: 1,
		Hash: bytes.Repeat([]byte{0xe2}, 32),
		Cbor: bytes.Join([][]byte{[]byte("new"), keptDatum}, []byte{'|'}),
	}
	require.NoError(t, pair.ledger.BlockCreate(expired, nil))
	require.NoError(t, pair.ledger.BlockCreate(kept, nil))
	_, err := pair.ledger.PruneBlock(t.Context(), expired.Slot, expired.Hash)
	require.NoError(t, err)

	raw := rawSQLiteMetadataFixture(t, pair.indexer)
	for _, row := range []struct {
		datum []byte
		slot  uint64
	}{{expiredDatum, 100}, {keptDatum, 200}} {
		_, err := raw.Exec(
			"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, ?)",
			lcommon.Blake2b256Hash(row.datum).Bytes(), row.datum, row.slot,
		)
		require.NoError(t, err)
	}

	require.NoError(t, pair.indexer.MigratePayloadOffsets(t.Context()))

	_, err = pair.indexer.GetDatum(
		t.Context(), lcommon.Blake2b256Hash(expiredDatum).Bytes(), nil,
	)
	require.ErrorIs(t, err, types.ErrHistoryExpired)
	var expiredErr *types.HistoryExpiredError
	require.ErrorAs(t, err, &expiredErr)
	require.EqualValues(t, 100, expiredErr.Slot)
	require.Equal(t, expired.Hash, expiredErr.Hash)
	var column []byte
	require.NoError(t, raw.QueryRow(
		"SELECT raw_datum FROM datum WHERE added_slot = 100",
	).Scan(&column))
	require.NotContains(t, string(column), string(expiredDatum))

	gotKept, err := pair.indexer.GetDatum(
		t.Context(), lcommon.Blake2b256Hash(keptDatum).Bytes(), nil,
	)
	require.NoError(t, err)
	require.Equal(t, keptDatum, gotKept.RawDatum)
}
