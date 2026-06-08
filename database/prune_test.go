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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// seedPrunableBlock writes a synthetic block plus offset-based blob entries
// for each (utxoIdx, payload) pair. The block CBOR is the concatenation of
// payloads in order, so each UTxO's offset within the block is the prefix
// sum of preceding payload lengths. Returns the block hash and the resolved
// offsets keyed by output index.
func seedPrunableBlock(
	t *testing.T,
	db *Database,
	slot uint64,
	txId []byte,
	payloads map[uint32][]byte,
) (blockHash []byte, blockCbor []byte) {
	t.Helper()
	blockHash = randomHash(t)

	// Concatenate payloads into the block CBOR in deterministic output-idx
	// order, recording each payload's offset.
	type entry struct {
		idx     uint32
		payload []byte
		offset  uint32
	}
	var entries []entry
	// Ordered iteration: idxs 0..N
	maxIdx := uint32(0)
	for idx := range payloads {
		if idx > maxIdx {
			maxIdx = idx
		}
	}
	var buf bytes.Buffer
	for i := uint32(0); i <= maxIdx; i++ {
		payload, ok := payloads[i]
		if !ok {
			continue
		}
		off := uint32(buf.Len()) //nolint:gosec
		buf.Write(payload)
		entries = append(entries, entry{idx: i, payload: payload, offset: off})
	}
	blockCbor = buf.Bytes()

	// Persist the block.
	insertTestBlock(t, db, slot, blockHash, blockCbor)

	// Persist each UTxO's blob entry as a CborOffset reference.
	blobTxn := db.BlobTxn(true)
	require.NoError(t, blobTxn.Do(func(txn *Txn) error {
		var blockHashArr [32]byte
		copy(blockHashArr[:], blockHash)
		for _, e := range entries {
			off := &CborOffset{
				BlockSlot:  slot,
				BlockHash:  blockHashArr,
				ByteOffset: e.offset,
				ByteLength: uint32(len(e.payload)), //nolint:gosec
			}
			if err := db.Blob().SetUtxo(
				txn.Blob(), txId, e.idx, EncodeUtxoOffset(off),
			); err != nil {
				return err
			}
		}
		return nil
	}))
	return blockHash, blockCbor
}

// seedUtxoMetadata inserts a UTxO row with the given added/deleted slot.
func seedUtxoMetadata(
	t *testing.T,
	db *Database,
	txId []byte,
	outputIdx uint32,
	addedSlot, deletedSlot uint64,
) {
	t.Helper()
	mdTxn := db.MetadataTxn(true)
	require.NoError(t, mdTxn.Do(func(txn *Txn) error {
		return db.Metadata().CreateUtxo(txn.Metadata(), &models.Utxo{
			TxId:        txId,
			OutputIdx:   outputIdx,
			AddedSlot:   addedSlot,
			DeletedSlot: deletedSlot,
			Amount:      types.Uint64(1),
		})
	}))
}

func TestPruneBlock_MaterializesLiveUtxoAndTombstonesBlock(t *testing.T) {
	db := newTestDB(t)

	const slot uint64 = 100
	txId := bytes.Repeat([]byte{0xAA}, 32)
	livePayload := []byte("LIVE-utxo-cbor-payload")
	spentPayload := []byte("spent-utxo-cbor-payload")

	hash, blockCbor := seedPrunableBlock(t, db, slot, txId, map[uint32][]byte{
		0: livePayload,
		1: spentPayload,
	})

	// Live UTxO at slot 100; spent UTxO at slot 100, consumed at slot 150
	// (still alive at the prune call so we can test it doesn't appear in
	// GetLiveUtxosBySlot).
	seedUtxoMetadata(t, db, txId, 0, slot, 0)
	seedUtxoMetadata(t, db, txId, 1, slot, 150)

	// Sanity: pre-prune both UTxO blob entries are offset-encoded and the
	// block is retrievable.
	blobTxn := db.BlobTxn(false)
	preBytes0, err := db.Blob().GetUtxo(blobTxn.Blob(), txId, 0)
	require.NoError(t, err)
	require.True(t, IsUtxoOffsetStorage(preBytes0))
	preBytes1, err := db.Blob().GetUtxo(blobTxn.Blob(), txId, 1)
	require.NoError(t, err)
	require.True(t, IsUtxoOffsetStorage(preBytes1))
	preBlock, _, err := db.Blob().GetBlock(blobTxn.Blob(), slot, hash)
	require.NoError(t, err)
	assert.Equal(t, blockCbor, preBlock)
	blobTxn.Release()

	// Prune the block.
	n, err := db.PruneBlock(slot, hash)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "exactly one live UTxO should be materialized")

	// Block CBOR has been replaced with an expiry marker; GetBlock
	// signals this with ErrHistoryExpired so a wrapping archive proxy
	// can intercept and resolve from the archive. The bp key still
	// exists (carrying just the marker bytes), and the bi/bh index
	// pointers and metadata remain so chain-iterator lookups still
	// translate id/hash into a block key.
	blobTxn = db.BlobTxn(false)
	defer blobTxn.Release()
	_, _, err = db.Blob().GetBlock(blobTxn.Blob(), slot, hash)
	assert.ErrorIs(t, err, types.ErrHistoryExpired)
	rawBp, err := db.Blob().Get(blobTxn.Blob(), types.BlockBlobKey(slot, hash))
	require.NoError(t, err,
		"bp key must still exist post-prune so bi/bh references stay valid")
	assert.True(t, types.IsBlockTombstone(rawBp),
		"bp value must be the expiry marker after prune")

	// Live UTxO entry is now raw CBOR matching the in-block slice.
	postLive, err := db.Blob().GetUtxo(blobTxn.Blob(), txId, 0)
	require.NoError(t, err)
	assert.False(
		t, IsUtxoOffsetStorage(postLive),
		"live UTxO blob entry must no longer be offset-encoded",
	)
	assert.Equal(t, livePayload, postLive)

	// Spent UTxO entry is untouched: GetLiveUtxosBySlot did not include
	// it, so PruneBlock left it as offset storage. (In a real run, the
	// blob entry would have been deleted at consume time; here we kept
	// it to verify materialization is gated on the metadata query.)
	postSpent, err := db.Blob().GetUtxo(blobTxn.Blob(), txId, 1)
	require.NoError(t, err)
	assert.True(
		t, IsUtxoOffsetStorage(postSpent),
		"spent UTxO blob entry should be left untouched by PruneBlock",
	)
}

func TestPruneBlock_ResolverReadsMaterializedUtxoAfterPrune(t *testing.T) {
	db := newTestDB(t)

	const slot uint64 = 100
	txId := bytes.Repeat([]byte{0xBB}, 32)
	payload := []byte("post-prune-resolution-payload")

	hash, _ := seedPrunableBlock(t, db, slot, txId, map[uint32][]byte{
		0: payload,
	})
	seedUtxoMetadata(t, db, txId, 0, slot, 0)

	_, err := db.PruneBlock(slot, hash)
	require.NoError(t, err)

	// Snapshot cold-extraction count to prove the resolver does NOT have to
	// fetch the (now-missing) block.
	pre := db.CborCache().Metrics().ColdExtractions.Load()

	cbor, err := db.CborCache().ResolveUtxoCbor(txId, 0)
	require.NoError(t, err)
	assert.Equal(t, payload, cbor)

	post := db.CborCache().Metrics().ColdExtractions.Load()
	assert.Equal(
		t, pre, post,
		"resolver should not perform a cold block extraction for a "+
			"materialized UTxO",
	)
}

// TestPruneBlock_LeavesChainIteratorAtHistoryExpired exercises the post-fix
// behavior for issue #2104. After prune the chain-iterator path resolves
// the (id|hash) → block-key mapping locally (bi/bh and metadata are kept)
// and reaches the GetBlock call inside blockByKey, which now surfaces
// ErrHistoryExpired. That sentinel is the explicit handoff point for a
// wrapping archive proxy (bark) to fetch from the archive. Without a
// proxy installed, the error propagates so the operator sees a clear
// "history expired" signal — not the silent chain-tip closure that
// BlockFetch produced before the fix.
func TestPruneBlock_LeavesChainIteratorAtHistoryExpired(t *testing.T) {
	db := newTestDB(t)

	const slot uint64 = 100
	txId := bytes.Repeat([]byte{0xEE}, 32)
	payload := []byte("tombstone-handoff-payload")

	hash, _ := seedPrunableBlock(t, db, slot, txId, map[uint32][]byte{
		0: payload,
	})
	seedUtxoMetadata(t, db, txId, 0, slot, 0)

	recent, err := BlocksRecent(db, 1)
	require.NoError(t, err)
	require.Len(t, recent, 1)
	blockID := recent[0].ID
	require.NotZero(t, blockID, "block id must be set before prune")

	_, err = db.PruneBlock(slot, hash)
	require.NoError(t, err)

	// BlockByIndex is the path the chain iterator walks. After prune the
	// id→key indirection still resolves (bi is preserved), so the lookup
	// reaches blob.GetBlock; that now returns ErrHistoryExpired, which
	// blockByKey propagates verbatim. A bark wrapper would intercept this
	// error and proxy to the archive. Without a wrapper the error reaches
	// the iterator as a clear, actionable signal.
	_, err = db.BlockByIndex(blockID, nil)
	assert.ErrorIs(t, err, types.ErrHistoryExpired,
		"BlockByIndex must reach the GetBlock handoff (history expired) so a "+
			"bark archive proxy can intercept and resolve")
	assert.NotErrorIs(t, err, models.ErrBlockNotFound,
		"the lookup must not collapse to ErrBlockNotFound — that is the "+
			"silent chain-tip dead end issue #2104 describes")

	// BlockByHash exercises the parallel hash-keyed path; same handoff.
	_, err = BlockByHash(db, hash)
	assert.ErrorIs(t, err, types.ErrHistoryExpired,
		"BlockByHash must also reach the history-expired handoff post-prune")
	assert.NotErrorIs(t, err, models.ErrBlockNotFound)
}

// TestPruneBlock_APIModeMaterializesSpentUtxos verifies that in API
// storage mode the pruner materializes CBOR bytes for retained spent
// UTxOs at the slot as well as live ones. This is the counterpart fix
// to skipping cleanupConsumedUtxos in API mode: with spent rows
// retained for historical transaction queries, the source block can
// still be expired, but the spent UTxO blob entries — which hold
// offset references into the expired block — must be rewritten
// to raw CBOR up front so resolution does not require a wrapping
// archive proxy to fetch the source block.
func TestPruneBlock_APIModeMaterializesSpentUtxos(t *testing.T) {
	db := newTestDBWithMode(t, types.StorageModeAPI)

	const slot uint64 = 100
	txId := bytes.Repeat([]byte{0xA1}, 32)
	livePayload := []byte("live-cbor-payload-api-mode")
	spentPayload := []byte("spent-cbor-payload-api-mode")

	hash, _ := seedPrunableBlock(t, db, slot, txId, map[uint32][]byte{
		0: livePayload,
		1: spentPayload,
	})

	// Live UTxO at slot 100; spent UTxO consumed at slot 150. In API mode
	// the spent row is retained past the stability window, and its blob
	// entry — still an offset reference to slot 100 — must be materialized
	// before the block is expired.
	seedUtxoMetadata(t, db, txId, 0, slot, 0)
	seedUtxoMetadata(t, db, txId, 1, slot, 150)

	n, err := db.PruneBlock(slot, hash)
	require.NoError(t, err)
	assert.Equal(t, 2, n,
		"API mode must materialize both live and retained spent UTxOs")

	blobTxn := db.BlobTxn(false)
	defer blobTxn.Release()

	postLive, err := db.Blob().GetUtxo(blobTxn.Blob(), txId, 0)
	require.NoError(t, err)
	assert.False(t, IsUtxoOffsetStorage(postLive),
		"live UTxO blob entry must be rewritten to raw CBOR")
	assert.Equal(t, livePayload, postLive)

	postSpent, err := db.Blob().GetUtxo(blobTxn.Blob(), txId, 1)
	require.NoError(t, err)
	assert.False(t, IsUtxoOffsetStorage(postSpent),
		"spent UTxO blob entry must also be rewritten to raw CBOR in API mode")
	assert.Equal(t, spentPayload, postSpent)
}

// TestPruneBlock_CoreModeLeavesSpentUtxos verifies the existing core-mode
// invariant: spent UTxOs at the slot are NOT materialized. Core mode
// hard-deletes consumed UTxOs in the stability-window cleanup, so by the
// time the pruner runs the spent row is gone — but until then any leftover
// spent blob entry must remain as the original offset reference (and is
// then deleted by the next cleanup pass).
func TestPruneBlock_CoreModeLeavesSpentUtxos(t *testing.T) {
	db := newTestDBWithMode(t, types.StorageModeCore)

	const slot uint64 = 100
	txId := bytes.Repeat([]byte{0xA2}, 32)
	livePayload := []byte("live-cbor-payload-core-mode")
	spentPayload := []byte("spent-cbor-payload-core-mode")

	hash, _ := seedPrunableBlock(t, db, slot, txId, map[uint32][]byte{
		0: livePayload,
		1: spentPayload,
	})
	seedUtxoMetadata(t, db, txId, 0, slot, 0)
	seedUtxoMetadata(t, db, txId, 1, slot, 150)

	n, err := db.PruneBlock(slot, hash)
	require.NoError(t, err)
	assert.Equal(t, 1, n,
		"core mode must materialize only the live UTxO at the slot")

	blobTxn := db.BlobTxn(false)
	defer blobTxn.Release()

	postSpent, err := db.Blob().GetUtxo(blobTxn.Blob(), txId, 1)
	require.NoError(t, err)
	assert.True(t, IsUtxoOffsetStorage(postSpent),
		"core mode must leave spent UTxO blob entry as offset storage")
}

func TestPruneBlock_SkipsAlreadyMaterializedUtxo(t *testing.T) {
	db := newTestDB(t)

	const slot uint64 = 100
	txId := bytes.Repeat([]byte{0xCC}, 32)
	payload := []byte("already-raw-payload")

	hash, _ := seedPrunableBlock(t, db, slot, txId, map[uint32][]byte{
		0: payload,
	})
	// Overwrite the offset entry with raw CBOR before prune to simulate a
	// UTxO that was previously materialized (or stored by a legacy code
	// path).
	rawWriteTxn := db.BlobTxn(true)
	require.NoError(t, rawWriteTxn.Do(func(txn *Txn) error {
		return db.Blob().SetUtxo(txn.Blob(), txId, 0, payload)
	}))
	seedUtxoMetadata(t, db, txId, 0, slot, 0)

	n, err := db.PruneBlock(slot, hash)
	require.NoError(t, err)
	assert.Equal(t, 0, n,
		"already-raw UTxO should not be counted as materialized")
}
