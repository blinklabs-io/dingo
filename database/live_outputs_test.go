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
