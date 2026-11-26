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
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
)

// HistoryExpiryCursorKey names the block-store chain state holding the slot
// below which every block has already been expired. The history-expiry pruner
// resumes its scan there, so a round costs the newly eligible blocks rather
// than every tombstone left by earlier rounds. A rollback or truncate lowers
// it to the rollback point, the slot above which replay stores new blocks.
const HistoryExpiryCursorKey = "history_expiry_cursor"

// oldestRetainedScanLimit bounds how many consecutive expired blocks
// RefreshOldestRetainedSlot walks past the history-expiry cursor before giving
// up.
const oldestRetainedScanLimit = 4096

// oldestRetainedSlot scans for the slot of the oldest block whose body is
// still stored. It reports ok=false when history expiry has never run (no
// cursor) or when no retained block is found within limit expired blocks past
// the cursor.
//
// The cursor alone is not the answer: it is the slot below which every block
// is expired, and the pruner leaves it on the last block it expired, so that
// slot itself is usually expired too. A rollback also lowers it below blocks
// that were already expired. The cursor is therefore a lower bound, and this
// walks forward from it past tombstones to the first block that reads back.
func (d *Database) oldestRetainedSlot(
	ctx context.Context,
	limit int,
) (slot uint64, ok bool, err error) {
	raw, err := d.GetChainState(HistoryExpiryCursorKey, nil)
	if err != nil {
		return 0, false, fmt.Errorf("read history expiry cursor: %w", err)
	}
	if raw == "" {
		return 0, false, nil
	}
	cursor, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse history expiry cursor: %w", err)
	}
	iter := d.BlocksInRangeContext(ctx, cursor, ^uint64(0))
	defer iter.Close()
	skipped := 0
	for {
		next, err := iter.NextRaw()
		if err != nil {
			if errors.Is(err, types.ErrHistoryExpired) {
				if skipped >= limit {
					return 0, false, nil
				}
				skipped++
				continue
			}
			return 0, false, fmt.Errorf("scan retained history: %w", err)
		}
		if next == nil {
			return 0, false, nil
		}
		return next.Slot, true, nil
	}
}

// lowerHistoryExpiryCursor caps the history-expiry cursor at slot. A cursor
// at or below slot, or an absent one, is left unchanged: blocks at or below
// the rollback point survive it, so a routine rollback does not send the
// pruner back over every expired block.
//
// The write shares txn when it carries a blob handle and commits on its own
// otherwise. Committing ahead of the rollback is safe: the cursor is a lower
// bound, so a lowered cursor whose rollback never commits only sends the
// pruner back over blocks it has already expired.
//
// The read never shares txn. The pruner saves the cursor from a transaction
// of its own while a rollback is open, and a blob store that detects
// conflicts would fail the rollback's commit over a key it had read.
func (d *Database) lowerHistoryExpiryCursor(slot uint64, txn *Txn) error {
	if txn != nil && txn.Blob() == nil {
		txn = nil
	}
	raw, err := d.GetChainState(HistoryExpiryCursorKey, nil)
	if err != nil {
		return fmt.Errorf("read history expiry cursor: %w", err)
	}
	if raw == "" {
		return nil
	}
	if cursor, err := strconv.ParseUint(raw, 10, 64); err == nil &&
		cursor <= slot {
		return nil
	}
	if err := d.SetChainState(
		HistoryExpiryCursorKey,
		strconv.FormatUint(slot, 10),
		txn,
	); err != nil {
		return fmt.Errorf("lower history expiry cursor: %w", err)
	}
	return nil
}

// PruneBlock expires the given block's local CBOR in the blob store after
// materializing any active UTxOs that still reference it. The block's CBOR
// is replaced with a small expired-history marker; index pointers (bi, bh)
// and metadata are kept so local reads can return ErrHistoryExpired and an
// optional archive proxy can still resolve the block by (slot, hash).
//
// UTxO blob entries are stored as 52-byte CborOffset references that point
// into a source block's CBOR. Tombstoning a block while live UTxOs still
// reference it would leave those UTxOs unresolvable. To preserve them,
// PruneBlock first finds every UTxO with added_slot equal to the pruned
// block's slot, decodes its offset, slices the underlying CBOR out of the
// block, and rewrites the UTxO blob entry as raw CBOR (which the resolver
// treats as the legacy non-offset format). The block expiry marker and all
// UTxO rewrites happen in a single blob transaction, so the block is
// never expired while live UTxOs still depend on it. The cloud plugins apply
// a transaction's objects one request at a time and apply the marker last,
// so a concurrent reader or snapshot never sees it ahead of the rewrites.
//
// In core storage mode only live (deleted_slot = 0) UTxOs at the slot are
// considered, because spent UTxOs are hard-deleted by the periodic
// stability-window cleanup and need no historical resolution. In API
// storage mode the cleanup is skipped and spent UTxO rows are retained
// indefinitely for historical transaction queries; in that mode every
// UTxO at the slot (including spent rows whose blob entries still hold
// offset references) is materialized so CBOR resolution survives the
// expired block without requiring a wrapping archive proxy.
//
// Returns the number of UTxOs that were materialized.
func (d *Database) PruneBlock(
	ctx context.Context,
	slot uint64,
	hash []byte,
) (int, error) {
	utxoRefs, err := d.offsetUtxoRefsAtSlot(ctx, slot)
	if err != nil {
		return 0, err
	}
	var materialized int
	blobTxn := d.BlockBlobTxn()
	if err := blobTxn.Do(func(txn *Txn) error {
		n, err := d.materializeUtxosTxn(ctx, txn, slot, hash, utxoRefs)
		if err != nil {
			return err
		}
		materialized = n
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.tombstoneBlockTxn(txn, slot, hash); err != nil {
			return fmt.Errorf(
				"prune block (slot=%d): expire block: %w",
				slot,
				err,
			)
		}
		d.retainedBlockRemoved(txn, slot)
		return nil
	}); err != nil {
		return 0, err
	}
	return materialized, nil
}

// MaterializeBlockUtxos rewrites, as raw CBOR, every UTxO entry of d that is
// an offset into the block at (slot, hash), as PruneBlock does before it
// expires the block, without expiring it. An IndexerDB keeps its UTxO entries
// in its own metadata store, which the ledger's PruneBlock does not see, so
// they are materialized through it before the ledger expires the block. It
// returns the number of entries rewritten.
func (d *Database) MaterializeBlockUtxos(
	ctx context.Context,
	slot uint64,
	hash []byte,
) (int, error) {
	if d.metadata == nil || d.liveUtxo {
		return 0, ctx.Err()
	}
	refs, err := d.offsetUtxoRefsAtSlot(ctx, slot)
	if err != nil {
		return 0, err
	}
	var materialized int
	// An IndexerDB writes its entries in its metadata transaction.
	var blobTxn *Txn
	if d.isIndexer() {
		blobTxn = d.Transaction(ctx, true)
	} else {
		blobTxn = d.BlockBlobTxn()
	}
	if err := blobTxn.Do(func(txn *Txn) error {
		blobStore := txn.BlobStore()
		if blobStore == nil {
			return types.ErrBlobStoreUnavailable
		}
		blockCbor, blockMeta, err := blobStore.GetBlock(txn.Blob(), slot, hash)
		if err != nil {
			return fmt.Errorf(
				"materialize block UTxOs (slot=%d): get block: %w",
				slot,
				err,
			)
		}
		if blockMeta.Type == ledger.BlockTypeDijkstra || len(refs) == 0 {
			block, err := models.DecodeBlockCbor(blockMeta.Type, blockCbor)
			if err != nil {
				return fmt.Errorf(
					"materialize block UTxOs (slot=%d): decode block: %w",
					slot,
					err,
				)
			}
			offsets, err := NewBlockIndexer(slot, hash).ComputeOffsets(
				blockCbor,
				block,
			)
			if err != nil {
				return fmt.Errorf(
					"materialize block UTxOs (slot=%d): compute offsets: %w",
					slot,
					err,
				)
			}
			refs = make([]models.UtxoId, 0, len(offsets.UtxoOffsets))
			for ref := range offsets.UtxoOffsets {
				refs = append(refs, models.UtxoId{
					Hash: ref.TxId[:],
					Idx:  ref.OutputIdx,
				})
			}
		}
		n, err := d.materializeUtxosTxnBlock(
			ctx, txn, slot, hash, blockCbor, refs,
		)
		materialized = n
		return err
	}); err != nil {
		return 0, err
	}
	return materialized, nil
}

// offsetUtxoRefsAtSlot lists the UTxOs added at slot whose blob entries may
// be offsets into that slot's block. The read is a transaction of its own,
// released before the blob write that rewrites them; a UTxO consumed in
// between has no blob entry left and is skipped there.
func (d *Database) offsetUtxoRefsAtSlot(
	ctx context.Context,
	slot uint64,
) ([]models.UtxoId, error) {
	// With the live UTxO set holding the outputs, no UTxO entry is an offset
	// into a block.
	if d.metadata == nil || d.liveUtxo {
		return nil, ctx.Err()
	}
	var (
		utxoRefs []models.UtxoId
		err      error
	)
	mdTxn := d.MetadataTxn(ctx, false)
	if d.config.StorageMode == types.StorageModeAPI {
		utxoRefs, err = d.utxoStore().GetUtxosBySlot(slot, mdTxn.Metadata())
	} else {
		utxoRefs, err = d.utxoStore().GetLiveUtxosBySlot(
			slot, mdTxn.Metadata(),
		)
	}
	mdTxn.Release()
	if err != nil {
		return nil, fmt.Errorf(
			"prune block (slot=%d): list utxos: %w",
			slot,
			err,
		)
	}
	return utxoRefs, ctx.Err()
}

// materializeUtxosTxn rewrites refs' offset entries into the block at (slot,
// hash) as raw CBOR within txn.
func (d *Database) materializeUtxosTxn(
	ctx context.Context,
	txn *Txn,
	slot uint64,
	hash []byte,
	refs []models.UtxoId,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	blobStore := txn.BlobStore()
	if blobStore == nil {
		return 0, types.ErrBlobStoreUnavailable
	}
	blockCbor, _, err := blobStore.GetBlock(txn.Blob(), slot, hash)
	if err != nil {
		return 0, fmt.Errorf(
			"prune block (slot=%d): get block: %w",
			slot,
			err,
		)
	}
	return d.materializeUtxosTxnBlock(ctx, txn, slot, hash, blockCbor, refs)
}

func (d *Database) materializeUtxosTxnBlock(
	ctx context.Context,
	txn *Txn,
	slot uint64,
	hash []byte,
	blockCbor []byte,
	refs []models.UtxoId,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	blobStore := txn.BlobStore()
	if blobStore == nil {
		return 0, types.ErrBlobStoreUnavailable
	}
	materialized := 0
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return materialized, err
		}
		n, err := d.materializeUtxo(txn, slot, hash, blockCbor, ref)
		if err != nil {
			return materialized, err
		}
		materialized += n
	}
	return materialized, nil
}

// materializeUtxo rewrites a single UTxO blob entry from offset storage to
// raw CBOR by extracting the bytes from the given block CBOR. Returns 1
// if the entry was rewritten, 0 if it was already raw, missing, or
// references a different block.
//
// It takes the *Txn rather than a bare types.Txn so the store and the blob
// transaction handle it writes through come from the same place: reading the
// database's installed store separately could pair a handle from one store
// with a call into another after a concurrent SetBlobStore.
func (d *Database) materializeUtxo(
	txn *Txn,
	slot uint64,
	hash []byte,
	blockCbor []byte,
	ref models.UtxoId,
) (int, error) {
	blobStore := txn.BlobStore()
	if blobStore == nil {
		return 0, types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	utxoData, err := blobStore.GetUtxo(blobTxn, ref.Hash, ref.Idx)
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
		d.logger.Error(
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
	if err := blobStore.SetUtxo(
		blobTxn,
		ref.Hash,
		ref.Idx,
		utxoCbor,
	); err != nil {
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
