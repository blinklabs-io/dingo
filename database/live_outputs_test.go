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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func liveKey(txID []byte, index uint32) string {
	return string(txID) + string(rune('0'+index))
}

type fakeLiveOutputs struct {
	mu   sync.Mutex
	live map[string][]byte
}

func (f *fakeLiveOutputs) LiveUtxoCbor(
	_ types.Txn,
	txID []byte,
	index uint32,
) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cbor, ok := f.live[liveKey(txID, index)]
	return cbor, ok, nil
}

func (f *fakeLiveOutputs) spend(txID []byte, index uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.live, liveKey(txID, index))
}

// An offset into an expired block reads an active output, and the datum or
// script inside it, from the live set, and a spent output stays expired.
func TestExpiredBlockReadsLiveOutputFromLiveSet(t *testing.T) {
	t.Parallel()
	db := openTestDBWithStorageMode(t, types.StorageModeAPI)
	const slot = uint64(100)
	blockHash := bytes.Repeat([]byte{0xbb}, 32)
	var blockHashArr [32]byte
	copy(blockHashArr[:], blockHash)
	txID := bytes.Repeat([]byte{0xa1}, 32)
	output := []byte("output-cbor-with-INLINE-DATUM-inside")
	block := append([]byte("header-"), output...)
	block = append(block, []byte("-trailer")...)
	outOffset := CborOffset{
		BlockSlot: slot, BlockHash: blockHashArr,
		ByteOffset: uint32(len("header-")), ByteLength: uint32(len(output)),
	}
	datum := []byte("INLINE-DATUM")
	datumOffset := CborOffset{
		BlockSlot: slot, BlockHash: blockHashArr,
		ByteOffset: outOffset.ByteOffset +
			uint32(bytes.Index(output, datum)),
		ByteLength: uint32(len(datum)),
	}

	require.NoError(t, db.BlockCreate(models.Block{
		Slot: slot, Hash: blockHash, Number: 1, Type: 1, Cbor: block,
	}, nil))
	blobTxn := db.BlobTxn(true)
	require.NoError(t, blobTxn.Do(func(txn *Txn) error {
		return db.Blob().SetUtxo(
			txn.Blob(), txID, 0, EncodeUtxoOffset(&outOffset),
		)
	}))
	mdTxn := db.MetadataTxn(context.Background(), true)
	require.NoError(t, mdTxn.Do(func(txn *Txn) error {
		return db.Metadata().CreateUtxo(txn.Metadata(), &models.Utxo{
			TxId: txID, Address: []byte{0xa, 0xb},
			PaymentKey: bytes.Repeat([]byte{0x11}, 28),
			AddedSlot:  slot, Amount: types.Uint64(1),
		})
	}))

	live := &fakeLiveOutputs{live: map[string][]byte{
		liveKey(txID, 0): output,
	}}
	db.SetLiveOutputs(live)

	// Expire the block without the materialization PruneBlock does, so the
	// offsets still name it.
	expire := db.BlockBlobTxn()
	require.NoError(t, db.tombstoneBlockTxn(expire, slot, blockHash))
	require.NoError(t, expire.Commit())
	expire.Release()

	got, err := db.CborCache().ResolveUtxoCbor(txID, 0)
	require.NoError(t, err)
	require.Equal(t, output, got, "a live output is byte-identical")

	var id [32]byte
	copy(id[:], txID)
	ref := UtxoRef{TxId: id, OutputIdx: 0}
	batch, err := db.CborCache().ResolveUtxoCborBatch([]UtxoRef{ref})
	require.NoError(t, err)
	require.Equal(t, output, batch[ref])

	gotDatum, err := db.ResolvePayload(EncodeUtxoOffset(&datumOffset))
	require.NoError(t, err)
	require.Equal(t, datum, gotDatum, "an inline datum of a live output")

	// A payload outside any live output is inactive history.
	headerOffset := CborOffset{
		BlockSlot: slot, BlockHash: blockHashArr, ByteLength: 4,
	}
	_, err = db.ResolvePayload(EncodeUtxoOffset(&headerOffset))
	require.ErrorIs(t, err, types.ErrHistoryExpired)

	live.spend(txID, 0)
	_, err = db.CborCache().ResolveUtxoCbor(txID, 0)
	require.ErrorIs(t, err, types.ErrHistoryExpired)
	_, err = db.ResolvePayload(EncodeUtxoOffset(&datumOffset))
	require.ErrorIs(t, err, types.ErrHistoryExpired)
}

// A row whose block expired before the migration, with no archive, names its
// bytes inside the live output that carries them instead of becoming an
// expired-payload marker, so the payload of an active output stays readable.
func TestMigratePayloadOffsetsLocatesPayloadInLiveOutputOfExpiredBlock(
	t *testing.T,
) {
	t.Parallel()
	db := openTestDBWithStorageMode(t, types.StorageModeAPI)
	const slot = uint64(100)
	blockHash := bytes.Repeat([]byte{0xbb}, 32)
	var blockHashArr [32]byte
	copy(blockHashArr[:], blockHash)
	txID := bytes.Repeat([]byte{0xa1}, 32)
	datum := []byte("INLINE-DATUM")
	output := append([]byte("output-cbor-with-"), datum...)
	output = append(output, []byte("-inside")...)
	block := append([]byte("header-"), output...)
	outOffset := CborOffset{
		BlockSlot: slot, BlockHash: blockHashArr,
		ByteOffset: uint32(len("header-")), ByteLength: uint32(len(output)),
	}
	require.NoError(t, db.BlockCreate(models.Block{
		Slot: slot, Hash: blockHash, Number: 1, Type: 1, Cbor: block,
	}, nil))
	blobTxn := db.BlobTxn(true)
	require.NoError(t, blobTxn.Do(func(txn *Txn) error {
		return db.Blob().SetUtxo(
			txn.Blob(), txID, 0, EncodeUtxoOffset(&outOffset),
		)
	}))
	mdTxn := db.MetadataTxn(context.Background(), true)
	require.NoError(t, mdTxn.Do(func(txn *Txn) error {
		return db.Metadata().CreateUtxo(txn.Metadata(), &models.Utxo{
			TxId: txID, Address: []byte{0xa, 0xb},
			PaymentKey: bytes.Repeat([]byte{0x11}, 28),
			AddedSlot:  slot, Amount: types.Uint64(1),
		})
	}))
	db.SetLiveOutputs(&fakeLiveOutputs{live: map[string][]byte{
		liveKey(txID, 0): output,
	}})
	expire := db.BlockBlobTxn()
	require.NoError(t, db.tombstoneBlockTxn(expire, slot, blockHash))
	require.NoError(t, expire.Commit())
	expire.Release()

	raw := rawSQLiteMetadataFixture(t, db)
	datumHash := lcommon.Blake2b256Hash(datum)
	_, err := raw.Exec(
		"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, ?)",
		datumHash.Bytes(), datum, slot,
	)
	require.NoError(t, err)

	require.NoError(t, db.MigratePayloadOffsets(t.Context()))

	var column []byte
	require.NoError(t, raw.QueryRow("SELECT raw_datum FROM datum").Scan(&column))
	require.False(t, types.IsExpiredPayload(column), "an active payload was dropped")
	got, err := db.GetDatum(t.Context(), datumHash.Bytes(), nil)
	require.NoError(t, err)
	require.Equal(t, datum, got.RawDatum)
}

// seedMaterializedLiveOutput stores a block holding one unspent output whose
// block then expires through PruneBlock, as a released API store left it: the
// output's entry is raw CBOR and its position in the block is lost. It returns
// the output's CBOR.
func seedMaterializedLiveOutput(
	t *testing.T,
	db *Database,
	slot uint64,
	txID, datumHash, output []byte,
) {
	t.Helper()
	blockHash := bytes.Repeat([]byte{0xbc}, 32)
	var blockHashArr [32]byte
	copy(blockHashArr[:], blockHash)
	block := append([]byte("header-"), output...)
	block = append(block, []byte("-trailer")...)
	outOffset := CborOffset{
		BlockSlot: slot, BlockHash: blockHashArr,
		ByteOffset: uint32(len("header-")), ByteLength: uint32(len(output)),
	}
	require.NoError(t, db.BlockCreate(models.Block{
		Slot: slot, Hash: blockHash, Number: 1, Type: 1, Cbor: block,
	}, nil))
	blobTxn := db.BlobTxn(true)
	require.NoError(t, blobTxn.Do(func(txn *Txn) error {
		return db.Blob().SetUtxo(
			txn.Blob(), txID, 0, EncodeUtxoOffset(&outOffset),
		)
	}))
	mdTxn := db.MetadataTxn(context.Background(), true)
	require.NoError(t, mdTxn.Do(func(txn *Txn) error {
		return db.Metadata().CreateUtxo(txn.Metadata(), &models.Utxo{
			TxId: txID, Address: []byte{0xa, 0xb}, DatumHash: datumHash,
			PaymentKey: bytes.Repeat([]byte{0x11}, 28),
			AddedSlot:  slot, Amount: types.Uint64(1),
		})
	}))
	_, err := db.PruneBlock(t.Context(), slot, blockHash)
	require.NoError(t, err)
	blobRead := db.Blob().NewTransaction(false)
	defer blobRead.Rollback() //nolint:errcheck
	entry, err := db.Blob().GetUtxo(blobRead, txID, 0)
	require.NoError(t, err)
	require.Equal(t, output, entry, "PruneBlock materialized the output")
}

// An output of a block that expired before the migration, whose entry is raw
// CBOR, holds its payload: the column names the payload by its position inside
// the live output, and the payload still reads.
func TestMigratePayloadOffsetsLocatesPayloadInRawLiveOutput(t *testing.T) {
	t.Parallel()
	db := openTestDBWithStorageMode(t, types.StorageModeAPI)
	txID := bytes.Repeat([]byte{0xa2}, 32)
	datum := []byte("INLINE-DATUM")
	output := append([]byte("output-cbor-with-"), datum...)
	datumHash := lcommon.Blake2b256Hash(datum)
	seedMaterializedLiveOutput(t, db, 100, txID, datumHash.Bytes(), output)
	db.SetLiveOutputs(&fakeLiveOutputs{live: map[string][]byte{
		liveKey(txID, 0): output,
	}})
	raw := rawSQLiteMetadataFixture(t, db)
	_, err := raw.Exec(
		"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, 100)",
		datumHash.Bytes(), datum,
	)
	require.NoError(t, err)

	require.NoError(t, db.MigratePayloadOffsets(t.Context()))

	var column []byte
	require.NoError(t, raw.QueryRow("SELECT raw_datum FROM datum").Scan(&column))
	require.True(t, types.IsLiveOutputOffset(column), "an active payload was dropped")
	got, err := db.GetDatum(t.Context(), datumHash.Bytes(), nil)
	require.NoError(t, err)
	require.Equal(t, datum, got.RawDatum)
}

// An orphaned datum row whose only carrier is a live output of an expired
// block resolves through that output, not through an expired-payload marker.
func TestMigratePayloadOffsetsResolvesAnOrphanThroughALiveOutput(t *testing.T) {
	t.Parallel()
	db := openTestDBWithStorageMode(t, types.StorageModeAPI)
	txID := bytes.Repeat([]byte{0xa3}, 32)
	datum := []byte("INLINE-DATUM")
	output := append([]byte("output-cbor-with-"), datum...)
	datumHash := lcommon.Blake2b256Hash(datum)
	seedMaterializedLiveOutput(t, db, 100, txID, datumHash.Bytes(), output)
	db.SetLiveOutputs(&fakeLiveOutputs{live: map[string][]byte{
		liveKey(txID, 0): output,
	}})
	// The row names a slot of a block a rollback abandoned. The applied
	// transaction that produced the live output is the canonical carrier.
	raw := rawSQLiteMetadataFixture(t, db)
	_, err := raw.Exec(
		"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, 999)",
		datumHash.Bytes(), datum,
	)
	require.NoError(t, err)
	_, err = raw.Exec(`INSERT INTO applied_transaction
(tx_id, block_hash, slot, type, fee, collateral_fee, ttl, block_index, valid)
SELECT id, ?, 100, 0, '0', '0', '0', 0, 1 FROM "transaction" WHERE hash = ?`,
		bytes.Repeat([]byte{0xbc}, 32), txID)
	require.NoError(t, err)

	require.NoError(t, db.MigratePayloadOffsets(t.Context()))

	var column []byte
	require.NoError(t, raw.QueryRow("SELECT raw_datum FROM datum").Scan(&column))
	require.False(t, types.IsExpiredPayload(column), "an active payload was dropped")
	got, err := db.GetDatum(t.Context(), datumHash.Bytes(), nil)
	require.NoError(t, err)
	require.Equal(t, datum, got.RawDatum)
}

// A live-output offset reads its bytes from the output in the live set, and an
// output that has been spent is inactive history.
func TestResolvePayloadReadsLiveOutputOffset(t *testing.T) {
	t.Parallel()
	db := openTestDBWithStorageMode(t, types.StorageModeAPI)
	txID := bytes.Repeat([]byte{0xa4}, 32)
	output := []byte("output-cbor-with-INLINE-DATUM")
	live := &fakeLiveOutputs{live: map[string][]byte{liveKey(txID, 0): output}}
	db.SetLiveOutputs(live)
	offset := (&types.LiveOutputOffset{
		TxHash: [32]byte(txID), ByteOffset: uint32(bytes.Index(output, []byte("INLINE"))),
		ByteLength: 12,
	}).Encode()

	got, err := db.ResolvePayload(offset)
	require.NoError(t, err)
	require.Equal(t, []byte("INLINE-DATUM"), got)

	live.spend(txID, 0)
	_, err = db.ResolvePayload(offset)
	require.ErrorIs(t, err, types.ErrHistoryExpired)
}

// A datum row names the block that first carried the datum, which expired,
// while a later output that is still live carries the same datum inline. The
// payload is active, so the row is re-pointed to the live output and not
// marked expired.
func TestMigratePayloadOffsetsRelocatesAnExpiredRowToALaterLiveOutput(
	t *testing.T,
) {
	t.Parallel()
	db := openTestDBWithStorageMode(t, types.StorageModeAPI)
	txID := bytes.Repeat([]byte{0xa5}, 32)
	datum := []byte("INLINE-DATUM")
	output := append([]byte("output-cbor-with-"), datum...)
	datumHash := lcommon.Blake2b256Hash(datum)
	seedMaterializedLiveOutput(t, db, 200, txID, datumHash.Bytes(), output)
	db.SetLiveOutputs(&fakeLiveOutputs{live: map[string][]byte{
		liveKey(txID, 0): output,
	}})
	// The block that first carried the datum, as a witness, expired.
	firstHash := bytes.Repeat([]byte{0xbd}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID: 50, Slot: 100, Hash: firstHash, Number: 0, Type: 1,
		Cbor: append([]byte("witness-"), datum...),
	}, nil))
	_, err := db.PruneBlock(t.Context(), 100, firstHash)
	require.NoError(t, err)

	raw := rawSQLiteMetadataFixture(t, db)
	_, err = raw.Exec(
		"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, 100)",
		datumHash.Bytes(), datum,
	)
	require.NoError(t, err)
	_, err = raw.Exec(`INSERT INTO applied_transaction
(tx_id, block_hash, slot, type, fee, collateral_fee, ttl, block_index, valid)
SELECT id, ?, 200, 0, '0', '0', '0', 0, 1 FROM "transaction" WHERE hash = ?`,
		bytes.Repeat([]byte{0xbc}, 32), txID)
	require.NoError(t, err)

	require.NoError(t, db.MigratePayloadOffsets(t.Context()))

	var column []byte
	var added uint64
	require.NoError(t, raw.QueryRow(
		"SELECT raw_datum, added_slot FROM datum",
	).Scan(&column, &added))
	require.False(t, types.IsExpiredPayload(column), "an active payload was dropped")
	require.EqualValues(t, 200, added)
	got, err := db.GetDatum(t.Context(), datumHash.Bytes(), nil)
	require.NoError(t, err)
	require.Equal(t, datum, got.RawDatum)
}
