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
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// PruneBlock removes the given block from the blob store after materializing
// any active UTxOs that still reference it.
//
// UTxO blob entries are stored as 52-byte CborOffset references that point
// into a source block's CBOR. Deleting a block while live UTxOs still
// reference it would leave those UTxOs unresolvable. To preserve them,
// PruneBlock first finds every live UTxO with added_slot equal to the
// pruned block's slot, decodes its offset, slices the underlying CBOR out
// of the block, and rewrites the UTxO blob entry as raw CBOR (which the
// resolver treats as the legacy non-offset format). The block delete and
// all UTxO rewrites happen in a single blob transaction, so the block is
// never deleted while live UTxOs still depend on it.
//
// Returns the number of UTxOs that were materialized.
func (d *Database) PruneBlock(slot uint64, hash []byte) (int, error) {
	// Read live UTxO refs for this slot from metadata. This is a separate
	// transaction so the blob write txn below has a single, simple commit
	// scope. A UTxO consumed between this read and the blob write is
	// handled below: GetUtxo will return ErrBlobKeyNotFound and the entry
	// is skipped.
	mdTxn := d.MetadataTxn(false)
	defer mdTxn.Release()
	liveUtxos, err := d.metadata.GetLiveUtxosBySlot(slot, mdTxn.Metadata())
	if err != nil {
		return 0, fmt.Errorf(
			"prune block (slot=%d): list live utxos: %w",
			slot,
			err,
		)
	}

	var materialized int
	blobTxn := d.BlobTxn(true)
	if err := blobTxn.Do(func(txn *Txn) error {
		blockCbor, meta, err := d.blob.GetBlock(txn.Blob(), slot, hash)
		if err != nil {
			return fmt.Errorf(
				"prune block (slot=%d): get block: %w",
				slot,
				err,
			)
		}
		for _, ref := range liveUtxos {
			n, err := d.materializeUtxo(txn.Blob(), slot, hash, blockCbor, ref)
			if err != nil {
				return err
			}
			materialized += n
		}
		if err := d.blob.DeleteBlock(txn.Blob(), slot, hash, meta.ID); err != nil {
			return fmt.Errorf(
				"prune block (slot=%d): delete block: %w",
				slot,
				err,
			)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return materialized, nil
}

// materializeUtxo rewrites a single UTxO blob entry from offset storage to
// raw CBOR by extracting the bytes from the given block CBOR. Returns 1
// if the entry was rewritten, 0 if it was already raw, missing, or
// references a different block.
func (d *Database) materializeUtxo(
	blobTxn types.Txn,
	slot uint64,
	hash []byte,
	blockCbor []byte,
	ref models.UtxoId,
) (int, error) {
	utxoData, err := d.blob.GetUtxo(blobTxn, ref.Hash, ref.Idx)
	if err != nil {
		// Raced with a consume that already deleted the blob entry. The
		// metadata row may also be in flight to deleted_slot != 0; either
		// way, nothing to materialize.
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf(
			"prune block (slot=%d): get utxo %x#%d: %w",
			slot,
			ref.Hash,
			ref.Idx,
			err,
		)
	}
	if !IsUtxoOffsetStorage(utxoData) {
		// Already raw CBOR (legacy or previously materialized).
		return 0, nil
	}
	offset, err := DecodeUtxoOffset(utxoData)
	if err != nil {
		return 0, fmt.Errorf(
			"prune block (slot=%d): decode utxo offset %x#%d: %w",
			slot,
			ref.Hash,
			ref.Idx,
			err,
		)
	}
	// Defensive: the metadata query keys on added_slot, but the blob
	// offset is the authoritative source. If they disagree, the UTxO
	// is backed by a different block — leave it alone.
	if offset.BlockSlot != slot || !bytes.Equal(offset.BlockHash[:], hash) {
		d.logger.Warn(
			"prune block: utxo offset references unexpected block; skipping",
			"slot", slot,
			"hash", hex.EncodeToString(hash),
			"utxo_tx_id", hex.EncodeToString(ref.Hash),
			"utxo_output_idx", ref.Idx,
			"offset_slot", offset.BlockSlot,
			"offset_hash", hex.EncodeToString(offset.BlockHash[:]),
		)
		return 0, nil
	}
	end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
	if end > uint64(len(blockCbor)) {
		return 0, fmt.Errorf(
			"prune block (slot=%d): utxo %x#%d offset out of range (offset=%d length=%d block_size=%d)",
			slot,
			ref.Hash,
			ref.Idx,
			offset.ByteOffset,
			offset.ByteLength,
			len(blockCbor),
		)
	}
	utxoCbor := make([]byte, offset.ByteLength)
	copy(utxoCbor, blockCbor[offset.ByteOffset:end])
	if err := d.blob.SetUtxo(blobTxn, ref.Hash, ref.Idx, utxoCbor); err != nil {
		return 0, fmt.Errorf(
			"prune block (slot=%d): rewrite utxo %x#%d as raw cbor: %w",
			slot,
			ref.Hash,
			ref.Idx,
			err,
		)
	}
	return 1, nil
}
