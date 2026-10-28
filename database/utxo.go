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
	"slices"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ErrUtxoNotFound signals that the metadata row for a UTxO does not
// exist (or was filtered out, e.g. by deleted_slot != 0 in the live
// view). Callers may use errors.Is to detect a genuinely-absent row.
var ErrUtxoNotFound = types.ErrUtxoNotFound

// ErrUtxoHistoryCandidateLimit reports that exact-address history matching
// exhausted its coarse-candidate budget before completing the query.
var ErrUtxoHistoryCandidateLimit = errors.New(
	"utxo history candidate limit exceeded",
)

// MaxUtxosByAddressResults is the default bound passed to UtxosByAddress
// for callers with no more specific limit of their own. It caps how many
// candidate rows a broad multi-address query (or a single address with an
// unusually large UTxO set) may force the database layer to materialize.
const (
	MaxUtxosByAddressResults = 100_000
	// DefaultPublicUtxoResultLimit matches the public UTxORPC key-list
	// limit. It bounds endpoints that must aggregate a complete UTxO set.
	DefaultPublicUtxoResultLimit = 1000
	// DefaultPublicUtxoCborBudget allows one current mainnet maximum-size
	// transaction per result. An output is only one part of that transaction.
	DefaultPublicUtxoCborBudget = DefaultPublicUtxoResultLimit * (16 << 10)
	// ExactAddressCandidateScanLimit is the existing cap used when a coarse
	// credential match must be confirmed from output CBOR.
	ExactAddressCandidateScanLimit = 10_000
)

// ErrUtxoCborUnavailable signals that the metadata row for a UTxO
// exists but its CBOR could not be loaded from the blob store and
// could not be recovered from any indexed block — typically because
// the row was inserted directly (e.g. fixture seeding) without a
// corresponding blob, or because the producing block is missing.
// This is distinct from ErrUtxoNotFound: the row IS present in the
// live UTxO set; only the on-the-wire bytes are unrecoverable. Callers
// that only need indexed metadata fields can ignore this error.
var ErrUtxoCborUnavailable = errors.New("utxo cbor unavailable")

const exactAddressCandidateScanLimit = ExactAddressCandidateScanLimit

var errExactAddressCandidateScanLimit = errors.New(
	"exact address candidate scan limit reached",
)

const utxoAddressBackfillSyncKey = "dingo:utxo-addresses:backfilled"

// deleteUtxoBlobs deletes blob data for the given [models.Utxo] entries.
// Metadata remains the authoritative source of truth; blob deletions are
// supplementary. A caller [*Txn] holding a blob handle stages the deletes in
// that transaction, so they commit or roll back with the metadata delete they
// accompany; without one, this function creates and commits its own blob-only
// batches via the [Database].
//
// Failures do not stop the remaining deletes, but they are counted and
// reported as [ErrBlobDeleteIncomplete]: the caller goes on to remove the
// metadata that names these objects, after which nothing can reach them
// again.
//
// txn is used only to time that count. An object is not stranded until the
// metadata naming it is durably gone, so the counter is incremented from an
// after-commit callback; if the enclosing transaction rolls back, the row
// still names the blob and nothing was orphaned. A nil txn has no commit to
// wait for and counts immediately.
func deleteUtxoBlobs(d *Database, utxos []models.Utxo, txn *Txn) error {
	const batchSize = 500
	// Report an absent blob store up front, so an empty utxos slice reports
	// it the same way a populated one does rather than silently succeeding
	// because the batch loop never ran.
	if d.Blob() == nil {
		return types.ErrBlobStoreUnavailable
	}

	var deleteErrors int
	deleteBatch := func(
		store blob.BlobStore,
		blobTxn types.Txn,
		batch []models.Utxo,
	) int {
		var batchDeleteErrors int
		for _, utxo := range batch {
			if err := store.DeleteUtxo(blobTxn, utxo.TxId, utxo.OutputIdx); err != nil {
				deleteErrors++
				batchDeleteErrors++
				d.logger.Warn(
					"failed to delete UTxO blob data",
					"txid", hex.EncodeToString(utxo.TxId),
					"output_idx", utxo.OutputIdx,
					"added_slot", utxo.AddedSlot,
					"deleted_slot", utxo.DeletedSlot,
					"error", err,
				)
			}
		}
		return batchDeleteErrors
	}

	// The store used for each delete comes from whichever transaction owns
	// the handle that delete runs through, so a concurrent SetBlobStore
	// cannot leave a handle from one store being deleted through another.
	if txn != nil && txn.Blob() != nil {
		// Stage the deletes in the caller's transaction so they commit or
		// roll back with the metadata delete they accompany. Committing
		// them separately let a successful blob delete survive the
		// caller's rollback, leaving metadata that points at blob data
		// which is already gone.
		blob := txn.BlobStore()
		if blob == nil {
			return types.ErrBlobStoreUnavailable
		}
		// Stage only what that transaction can still hold. Past its budget
		// the store rejects every further staged write, including the
		// commit timestamp Txn.Commit puts into this same transaction, so
		// an unbounded stage costs the caller its whole commit rather than
		// just the tail of this set. What is left
		// unstaged is counted with the deletes that failed and reported
		// through ErrBlobDeleteIncomplete below: the metadata naming these
		// objects goes away either way, so both are orphans rather than
		// silently dropped work.
		staged := len(utxos)
		if staged > 0 {
			staged = stagedBlobDeleteLimit(
				blob,
				txn.Blob(),
				len(types.UtxoBlobKey(utxos[0].TxId, utxos[0].OutputIdx)),
				staged,
			)
		}
		deleteBatch(blob, txn.Blob(), utxos[:staged])
		if skipped := len(utxos) - staged; skipped > 0 {
			deleteErrors += skipped
			d.logger.Warn(
				"UTxO blob deletes left unstaged to keep the transaction committable",
				"skipped", skipped,
				"staged", staged,
				"total", len(utxos),
			)
		}
	} else {
		for start := 0; start < len(utxos); start += batchSize {
			end := min(start+batchSize, len(utxos))
			batch := utxos[start:end]
			batchTxn := NewBlobOnlyTxn(d, true)
			// Take the store from the batch's own transaction rather than
			// from the database once up front: each batch commits
			// separately, so a replacement between batches would otherwise
			// leave later batches deleting through a store that no longer
			// owns their transaction handles.
			blob := batchTxn.BlobStore()
			if blob == nil {
				batchTxn.Release()
				return types.ErrBlobStoreUnavailable
			}
			batchBlobTxn := batchTxn.Blob()
			if batchBlobTxn == nil {
				batchTxn.Release()
				return types.ErrNilTxn
			}
			batchDeleteErrors := deleteBatch(blob, batchBlobTxn, batch)
			if err := batchTxn.Commit(); err != nil {
				deleteErrors += len(batch) - batchDeleteErrors
				_ = batchTxn.Rollback()
				d.logger.Warn(
					"UTxO blob delete batch commit failed",
					"batch_start", start,
					"batch_end", end,
					"batch_size", len(batch),
					"error", err,
				)
			}
		}
	}
	if deleteErrors > 0 {
		recordBlobOrphansOnCommit(txn, deleteErrors)
		d.logger.Warn(
			"UTxO blob deletion completed with errors",
			"failed",
			deleteErrors,
			"total",
			len(utxos),
		)
		return fmt.Errorf(
			"%w: %d of %d UTxO blobs",
			ErrBlobDeleteIncomplete,
			deleteErrors,
			len(utxos),
		)
	}

	return nil
}

func loadCbor(u *models.Utxo, txn *Txn) error {
	db := txn.DB()
	// Use tiered cache if available
	if db.cborCache != nil {
		// Pass the transaction so we can see uncommitted writes
		// (important for intra-batch UTxO lookups during validation).
		// The transaction, not its bare blob handle: the cold path has to
		// run against the store that handle was opened on.
		cbor, err := db.cborCache.ResolveUtxoCbor(u.TxId, u.OutputIdx, txn)
		if err != nil {
			if errors.Is(err, types.ErrBlobKeyNotFound) {
				recoveredCbor, recoverErr := recoverUtxoCbor(
					db,
					txn,
					u.TxId,
					u.OutputIdx,
				)
				if recoverErr == nil {
					u.Cbor = recoveredCbor
					return nil
				}
				return recoverErr
			}
			return fmt.Errorf(
				"resolve UTxO cbor tx=%x idx=%d: %w",
				u.TxId[:8],
				u.OutputIdx,
				err,
			)
		}
		u.Cbor = cbor
		return nil
	}

	// Fallback: direct blob access (for tests without cache)
	blob := txn.BlobStore()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	val, err := blob.GetUtxo(txn.Blob(), u.TxId, u.OutputIdx)
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			recoveredCbor, recoverErr := recoverUtxoCbor(
				db,
				txn,
				u.TxId,
				u.OutputIdx,
			)
			if recoverErr == nil {
				u.Cbor = recoveredCbor
				return nil
			}
			return recoverErr
		}
		return fmt.Errorf(
			"resolve UTxO cbor tx=%x idx=%d: %w",
			u.TxId[:8],
			u.OutputIdx,
			err,
		)
	}

	// Check if this is offset-based storage
	if IsUtxoOffsetStorage(val) {
		// Decode the offset reference
		offset, err := DecodeUtxoOffset(val)
		if err != nil {
			return fmt.Errorf("decode utxo offset: %w", err)
		}

		// Get the block CBOR from blob store
		blockCbor, _, err := blob.GetBlock(
			txn.Blob(),
			offset.BlockSlot,
			offset.BlockHash[:],
		)
		if err != nil {
			return fmt.Errorf("get block for utxo extraction: %w", err)
		}

		// Extract the UTxO CBOR from the block
		end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
		if end > uint64(len(blockCbor)) {
			return fmt.Errorf(
				"utxo offset out of bounds: offset=%d, length=%d, block_size=%d",
				offset.ByteOffset,
				offset.ByteLength,
				len(blockCbor),
			)
		}
		u.Cbor = blockCbor[offset.ByteOffset:end]
		return nil
	}

	// Legacy format: raw CBOR data
	u.Cbor = val
	return nil
}

// ResolveUtxoCborWithRecovery resolves a UTxO's CBOR via the tiered cache,
// falling back to reconstructing it from the producing block (the same
// recovery loadCbor performs for UtxoByRef and IterateLiveUtxos) when the
// blob has gone missing but is reconstructable. Returns
// ErrUtxoCborUnavailable when recovery itself confirms the CBOR cannot be
// reconstructed.
//
// Prefer this over CborCache().ResolveUtxoCbor for a caller that has only a
// bare ref (not a *models.Utxo already loaded via GetUtxo) and must not
// silently treat a recoverable missing blob as absent -- e.g.
// ledger.queryShelleyUtxoWhole, which answers GetUTxOWhole and would
// otherwise return an incomplete reply for a row IterateLiveUtxos'
// loadCbor-based path would have recovered.
//
// txn may be blob-only (Metadata() == nil): the hot path above only ever
// reaches the blob store (TieredCborCache.ResolveUtxoCbor), and a caller
// resolving many refs concurrently (queryShelleyUtxoWhole's worker pool)
// wants to avoid holding a metadata connection from the shared read pool
// for the whole resolve phase when it is needed only on the rare recovery
// branch below. Only recoverUtxoCbor's metadata-based fallback
// (utxoRecoveryBlockForTx, once the blob-based lookup misses) actually
// needs one, so a metadata-capable transaction is opened here, on demand,
// just for that branch. This is an
// explicit, self-contained guarantee: sqlstore.Store.GetTransactionByHash
// (what that fallback ultimately calls) separately tolerates a nil
// types.Txn by borrowing its own ad-hoc pooled connection for just that one
// query, so passing txn.Metadata() straight through would also work today
// -- but relying on that would make this function's own resource behavior
// depend on an incidental property of a different package's nil-handling
// rather than on something stated and tested here.
func (d *Database) ResolveUtxoCborWithRecovery(
	ctx context.Context,
	txId []byte,
	outputIdx uint32,
	txn *Txn,
) ([]byte, error) {
	// recoverUtxoCbor (unlike CborCache().ResolveUtxoCbor) requires a real
	// *Txn -- mirror UtxoByRef's nil handling here rather than passing a nil
	// txn through to it.
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	cbor, err := d.CborCache().ResolveUtxoCbor(txId, outputIdx, txn)
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			recoveryTxn := txn
			var cleanup func()
			switch {
			case txn.Metadata() == nil:
				recoveryTxn, cleanup, err = txn.withMetadataForRecovery(ctx)
				if err != nil {
					return nil, fmt.Errorf(
						"open metadata recovery transaction: %w",
						err,
					)
				}
			case txn.Blob() == nil:
				// A metadata-only caller: recoverUtxoCbor's block lookup
				// (utxoRecoveryBlockForTx -> BlockByPointTxn) needs a blob
				// handle to fetch the producing block's raw CBOR, which
				// txn doesn't have. Mirrors the metadata-missing case
				// above for the opposite gap.
				recoveryTxn, cleanup = txn.withBlobForRecovery()
			}
			if cleanup != nil {
				defer cleanup()
			}
			return recoverUtxoCbor(d, recoveryTxn, txId, outputIdx)
		}
		return nil, err
	}
	return cbor, nil
}

func recoverUtxoCbor(
	db *Database,
	txn *Txn,
	txId []byte,
	outputIdx uint32,
) ([]byte, error) {
	txHash, err := lcommon.NewBlake2b256Checked(txId)
	if err != nil {
		return nil, fmt.Errorf("utxo recovery transaction id: %w", err)
	}
	block, err := utxoRecoveryBlockForTx(db, txn, txId)
	if err != nil {
		return nil, err
	}
	if block == nil {
		// The producer block could not be located. This is a CBOR-
		// recovery failure (the metadata row may still be present);
		// use the dedicated sentinel so callers can distinguish it
		// from a missing metadata row.
		return nil, ErrUtxoCborUnavailable
	}

	// Decode the block once for both CBOR extraction and offset computation
	decodedBlock, err := block.Decode()
	if err != nil {
		return nil, fmt.Errorf(
			"decode producer block for utxo recovery at slot %d: %w",
			block.Slot,
			err,
		)
	}

	// Extract the UTxO CBOR from the decoded block
	recoveredCbor, err := utxoCborFromDecodedBlock(
		decodedBlock, txId, outputIdx,
	)
	if err != nil {
		return nil, err
	}

	// Compute the DOFF offset so the repair stores a proper offset reference
	indexer := NewBlockIndexer(block.Slot, block.Hash)
	offsets, indexErr := indexer.ComputeOffsets(block.Cbor, decodedBlock)
	if indexErr == nil {
		ref := UtxoRef{TxId: txHash, OutputIdx: outputIdx}
		if offset, ok := offsets.UtxoOffsets[ref]; ok {
			if repairErr := repairUtxoBlob(
				db, txn, txId, outputIdx, &offset,
			); repairErr != nil {
				db.logger.Debug(
					"failed to repair missing UTxO blob",
					"txid", hex.EncodeToString(txId),
					"output_idx", outputIdx,
					"error", repairErr,
				)
			}
		}
	}

	return recoveredCbor, nil
}

// utxoRecoverySlotForTx returns the producer block slot for txId without
// fetching the block itself. Returns (0, false, nil) when the producer tx
// cannot be located so callers can decide whether that is fatal.
func utxoRecoverySlotForTx(
	db *Database,
	txn *Txn,
	txId []byte,
) (uint64, bool, error) {
	slot, _, found, err := fetchTxBlobSlotAndHash(db, txn, txId)
	if err != nil {
		return 0, false, err
	}
	if found {
		return slot, true, nil
	}
	slot, found, err = db.metadata.GetTransactionSlotByHash(
		txId, txn.Metadata(),
	)
	if err != nil {
		return 0, false, fmt.Errorf(
			"lookup producer tx metadata for utxo recovery %x: %w",
			bytePrefix(txId),
			err,
		)
	}
	if !found {
		return 0, false, nil
	}
	return slot, true, nil
}

func fetchTxBlobSlotAndHash(
	db *Database,
	txn *Txn,
	txId []byte,
) (uint64, [32]byte, bool, error) {
	var blockHash [32]byte
	if db == nil || txn == nil {
		return 0, blockHash, false, nil
	}
	blob := txn.BlobStore()
	blobTxn := txn.Blob()
	if blob == nil || blobTxn == nil {
		return 0, blockHash, false, nil
	}
	txData, err := blob.GetTx(blobTxn, txId)
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return 0, blockHash, false, nil
		}
		return 0, blockHash, false, fmt.Errorf(
			"lookup tx blob for utxo recovery %x: %w",
			bytePrefix(txId),
			err,
		)
	}
	switch {
	case IsTxOffsetStorage(txData):
		offset, err := DecodeTxOffset(txData)
		if err != nil {
			return 0, blockHash, false, fmt.Errorf(
				"decode tx offset for utxo recovery %x: %w",
				bytePrefix(txId),
				err,
			)
		}
		return offset.BlockSlot, offset.BlockHash, true, nil
	case IsTxCborPartsStorage(txData):
		parts, err := DecodeTxCborParts(txData)
		if err != nil {
			return 0, blockHash, false, fmt.Errorf(
				"decode tx parts for utxo recovery %x: %w",
				bytePrefix(txId),
				err,
			)
		}
		return parts.BlockSlot, parts.BlockHash, true, nil
	default:
		return 0, blockHash, false, nil
	}
}

func utxoRecoveryBlockForTx(
	db *Database,
	txn *Txn,
	txId []byte,
) (*models.Block, error) {
	// Try the blob-based path when both the blob store and a blob
	// transaction handle are available; otherwise skip straight to the
	// metadata-based lookup below.
	slot, blockHash, found, err := fetchTxBlobSlotAndHash(db, txn, txId)
	if err != nil {
		return nil, err
	}
	if found {
		block, err := BlockByPointTxn(
			txn,
			ocommon.NewPoint(slot, blockHash[:]),
		)
		if err != nil {
			return nil, fmt.Errorf(
				"lookup producer block from tx blob %x: %w",
				bytePrefix(txId),
				err,
			)
		}
		return &block, nil
	}
	producerTx, err := db.metadata.GetTransactionByHash(txId, txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf(
			"lookup producer tx metadata for utxo recovery %x: %w",
			bytePrefix(txId),
			err,
		)
	}
	if producerTx == nil || len(producerTx.BlockHash) == 0 {
		return nil, nil
	}
	block, err := BlockByPointTxn(
		txn,
		ocommon.NewPoint(producerTx.Slot, producerTx.BlockHash),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"lookup producer block from tx metadata %x: %w",
			bytePrefix(txId),
			err,
		)
	}
	return &block, nil
}

func utxoCborFromDecodedBlock(
	decodedBlock ledger.Block,
	txId []byte,
	outputIdx uint32,
) ([]byte, error) {
	// These returns signal "the producing block was located but the
	// requested output's CBOR cannot be reconstructed from it" — a
	// CBOR-recovery failure, not a missing metadata row. Use the
	// dedicated sentinel so callers can distinguish the two via
	// errors.Is.
	for _, tx := range decodedBlock.Transactions() {
		if !bytes.Equal(tx.Hash().Bytes(), txId) {
			continue
		}
		for _, produced := range tx.Produced() {
			if produced.Id.Index() == outputIdx {
				return produced.Output.Cbor(), nil
			}
		}
		return nil, ErrUtxoCborUnavailable
	}
	return nil, ErrUtxoCborUnavailable
}

func repairUtxoBlob(
	db *Database,
	txn *Txn,
	txId []byte,
	outputIdx uint32,
	offset *CborOffset,
) error {
	offsetData := EncodeUtxoOffset(offset)

	// Use the caller's blob txn when it is write-capable
	if txn != nil && txn.Blob() != nil && txn.IsReadWrite() {
		blob := txn.BlobStore()
		if blob == nil {
			return nil
		}
		return blob.SetUtxo(txn.Blob(), txId, outputIdx, offsetData)
	}

	// The caller's txn is read-only (or has no blob handle at all), so a
	// separate write transaction is needed. When the caller already
	// pinned a blob store, open the write transaction directly on that
	// same store.BlobStore rather than NewBlobOnlyTxn, which re-pins
	// whatever store is *currently* installed: recoverUtxoCbor's read
	// above located this block through the caller's pinned store, and
	// re-pinning here would let a concurrent SetBlobStore swap repair a
	// different store than the one just read from, splitting the read
	// and the write-back non-atomically across two stores.
	// The caller's own pin, still held
	// for txn's lifetime (which spans this whole call), keeps that store
	// from being drained out from under this write.
	var store blob.BlobStore
	if txn != nil {
		store = txn.BlobStore()
	}
	if store == nil {
		writeTxn := NewBlobOnlyTxn(db, true)
		defer writeTxn.Release()
		store = writeTxn.BlobStore()
		if store == nil {
			return nil
		}
		if err := store.SetUtxo(
			writeTxn.Blob(), txId, outputIdx, offsetData,
		); err != nil {
			return err
		}
		return writeTxn.Commit()
	}
	writeBlobTxn := store.NewTransaction(true)
	if err := store.SetUtxo(
		writeBlobTxn, txId, outputIdx, offsetData,
	); err != nil {
		_ = writeBlobTxn.Rollback()
		return err
	}
	if err := writeBlobTxn.Commit(); err != nil {
		// A failed Commit does not itself discard the underlying provider
		// transaction (e.g. badgerTxn.Commit leaves finished=false on a
		// failed t.tx.Commit), so Rollback is still needed here to release
		// it -- recoverUtxoCbor only logs a repair failure and moves on,
		// so a caller that never rolls back would leak one of these per
		// failed repair attempt.
		_ = writeBlobTxn.Rollback()
		return err
	}
	return nil
}

func (d *Database) UtxoByRef(
	ctx context.Context,
	txId []byte,
	outputIdx uint32,
	txn *Txn,
) (*models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	utxo, err := d.utxoStore().GetUtxo(txId, outputIdx, txn.Metadata())
	if err != nil {
		return nil, err
	}
	if utxo == nil {
		return nil, ErrUtxoNotFound
	}
	if err := loadCbor(utxo, txn); err != nil {
		return nil, err
	}
	return utxo, nil
}

// UtxoExists reports whether a live UTxO is recorded for the reference, without
// materializing its CBOR.
//
// UtxoByRef resolves the output's bytes from the blob store and, on a miss,
// reconstructs them by decoding the producing block. A caller that only needs
// to know whether the output is still there pays for all of that, and — worse —
// turns a CBOR that cannot be recovered into a hard error about a UTxO that
// demonstrably exists. Replay recovery asks exactly that question of every
// referenced input of a failing transaction (see
// LedgerState.findReplayRecoveryCandidate), so it uses this instead.
func (d *Database) UtxoExists(
	ctx context.Context,
	txId []byte,
	outputIdx uint32,
	txn *Txn,
) (bool, error) {
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	utxo, err := d.utxoStore().GetUtxo(txId, outputIdx, txn.Metadata())
	if err != nil {
		return false, err
	}
	return utxo != nil, nil
}

// UtxosByRefs returns the live UTxOs matching the given references in a
// single batch. Refs with no matching live UTxO are simply absent from the
// result.
func (d *Database) UtxosByRefs(
	ctx context.Context,
	refs []models.UtxoId,
	txn *Txn,
) ([]models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	utxos, err := d.utxoStore().GetUtxosByRefs(refs, txn.Metadata())
	if err != nil {
		return nil, err
	}
	for i := range utxos {
		if err := loadCbor(&utxos[i], txn); err != nil {
			return nil, err
		}
	}
	return utxos, nil
}

// ErrUtxoBytesExceeded is returned by UtxosByRefsBounded when the CBOR of the
// matching UTxOs adds up to more than the byte limit it was given.
var ErrUtxoBytesExceeded = errors.New("utxo cbor exceeds byte limit")

// UtxosByRefsBounded is UtxosByRefs with a cap on the CBOR it will load. The
// CBOR size of an output is only known once it has been read, so it is counted
// as each output is loaded and the call stops with ErrUtxoBytesExceeded as
// soon as the running total passes maxBytes, holding at most maxBytes plus the
// output that crossed it. The first output always fits, so a limit smaller
// than one output still makes progress. ctx is checked before each output is
// loaded. A maxBytes of zero or less means no limit.
func (d *Database) UtxosByRefsBounded(
	ctx context.Context,
	refs []models.UtxoId,
	maxBytes int64,
	txn *Txn,
) ([]models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	utxos, err := d.utxoStore().GetUtxosByRefs(refs, txn.Metadata())
	if err != nil {
		return nil, err
	}
	var used int64
	for i := range utxos {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := loadCbor(&utxos[i], txn); err != nil {
			return nil, err
		}
		size := int64(len(utxos[i].Cbor))
		if maxBytes > 0 && i > 0 && used+size > maxBytes {
			return nil, ErrUtxoBytesExceeded
		}
		used += size
	}
	return utxos, nil
}

// UtxosByRefsAsOf returns the UTxOs matching refs as they stood at atSlot:
// a ref is included when it was created at-or-before atSlot and is either
// still live or was spent strictly after atSlot. As with UtxosByRefs, a
// ref with no matching row is simply absent from the result -- but unlike
// UtxosByRefs, that absence is ambiguous once atSlot is older than this
// node's spent-UTxO retention floor: it could mean "genuinely never live
// at atSlot" or "was live at atSlot but its spend record has since been
// hard-deleted by the periodic stability-window cleanup"
// (UtxosDeleteConsumed). Callers pinning a historical point (ledger.Query,
// node-parity) must reject that case themselves before
// calling this -- see ledger's checkUtxoRetentionWindow.
func (d *Database) UtxosByRefsAsOf(
	ctx context.Context,
	refs []models.UtxoId,
	atSlot uint64,
	txn *Txn,
) ([]models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	utxos, err := d.utxoStore().GetUtxosByRefsAsOf(refs, atSlot, txn.Metadata())
	if err != nil {
		return nil, err
	}
	for i := range utxos {
		if err := loadCbor(&utxos[i], txn); err != nil {
			return nil, err
		}
	}
	return utxos, nil
}

// CreateUtxo inserts a Utxo row directly. The normal block-application
// path uses AddUtxos with UtxoSlot inputs; this is the simple-insert
// variant for callers that already have a populated model. When txn
// is nil a write transaction is opened, committed on success and
// rolled back on error via Txn.Do.
func (d *Database) CreateUtxo(
	ctx context.Context,
	txn *Txn,
	utxo *models.Utxo,
) error {
	if txn != nil {
		return d.utxoStore().CreateUtxo(txn.Metadata(), utxo)
	}
	return d.MetadataTxn(ctx, true).Do(func(t *Txn) error {
		return d.utxoStore().CreateUtxo(t.Metadata(), utxo)
	})
}

// UtxoByRefIncludingSpent returns a Utxo by reference,
// including spent (consumed) UTxOs.
func (d *Database) UtxoByRefIncludingSpent(
	ctx context.Context,
	txId []byte,
	outputIdx uint32,
	txn *Txn,
) (*models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	utxo, err := d.utxoStore().GetUtxoIncludingSpent(
		txId,
		outputIdx,
		txn.Metadata(),
	)
	if err != nil {
		return nil, err
	}
	if utxo == nil {
		return nil, nil
	}
	if err := loadCbor(utxo, txn); err != nil {
		return nil, err
	}
	return utxo, nil
}

// UtxosByAddress returns all UTxOs belonging to any of the given addresses.
// maxResults is a required, positive bound on the number of candidate rows
// the query may materialize; callers with no more specific limit of their
// own should pass MaxUtxosByAddressResults. Exceeding the bound returns
// models.ErrTooManyUtxoResults.
func (d *Database) UtxosByAddress(
	ctx context.Context,
	addrs []ledger.Address,
	maxResults int,
	txn *Txn,
) ([]models.Utxo, error) {
	if len(addrs) == 0 {
		return nil, nil
	}
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	patterns := make([]models.UtxoAddressPattern, len(addrs))
	for i, addr := range addrs {
		pattern, err := models.ExactUtxoAddressPattern(addr)
		if err != nil {
			return nil, err
		}
		patterns[i] = pattern
	}
	utxos, err := d.utxoStore().
		GetUtxosByAddress(patterns, maxResults, txn.Metadata())
	if err != nil {
		return nil, err
	}
	for i := range utxos {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := loadCbor(&utxos[i], txn); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return filterUtxosByAddressPatterns(ctx, utxos, patterns)
}

// UtxosByAddressAsOf is UtxosByAddress as the outputs stood at atSlot (see
// the metadata store's GetUtxosByAddressAsOf). Rows spent at or before
// atSlot and later removed by consumed-UTxO cleanup are missing, so callers
// must reject a point below their retention floor first.
func (d *Database) UtxosByAddressAsOf(
	ctx context.Context,
	addrs []ledger.Address,
	atSlot uint64,
	maxResults int,
	txn *Txn,
) ([]models.Utxo, error) {
	if len(addrs) == 0 {
		return nil, nil
	}
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	patterns := make([]models.UtxoAddressPattern, len(addrs))
	for i, addr := range addrs {
		pattern, err := models.ExactUtxoAddressPattern(addr)
		if err != nil {
			return nil, err
		}
		patterns[i] = pattern
	}
	utxos, err := d.utxoStore().GetUtxosByAddressAsOf(
		patterns,
		atSlot,
		maxResults,
		txn.Metadata(),
	)
	if err != nil {
		return nil, err
	}
	for i := range utxos {
		if err := loadCbor(&utxos[i], txn); err != nil {
			return nil, err
		}
	}
	return filterUtxosByAddressPatterns(ctx, utxos, patterns)
}

// UtxosWithHistory returns retained live and spent outputs with their
// producing and spending chain positions. The metadata store performs the
// indexed coarse query; this coordinated layer resolves output CBOR and
// completes exact-address matching, which cannot be done from credential
// columns alone. For a bounded exact-address query, Limit applies to exact
// matches rather than coarse SQL candidates.
func (d *Database) UtxosWithHistory(
	q *models.UtxoHistoryQuery,
	txn *Txn,
) ([]models.UtxoWithHistory, error) {
	utxos, _, err := d.utxosWithHistory(q, -1, txn)
	return utxos, err
}

// UtxosWithHistoryBounded is UtxosWithHistory with a limit on the coarse
// candidates examined while completing exact-address matching. It returns the
// number of candidates read so callers can enforce one budget across pages.
func (d *Database) UtxosWithHistoryBounded(
	q *models.UtxoHistoryQuery,
	maxCandidates int,
	txn *Txn,
) ([]models.UtxoWithHistory, int, error) {
	if maxCandidates < 0 {
		return nil, 0, errors.New("maxCandidates must not be negative")
	}
	return d.utxosWithHistory(q, maxCandidates, txn)
}

func (d *Database) utxosWithHistory(
	q *models.UtxoHistoryQuery,
	maxCandidates int,
	txn *Txn,
) ([]models.UtxoWithHistory, int, error) {
	if q == nil {
		return nil, 0, models.ErrNilUtxoHistoryQuery
	}
	if txn == nil {
		txn = d.TransactionContext(context.Background(), false)
		defer txn.Release()
	}
	requiresExact := !q.MatchAllAddresses &&
		models.RequiresExactAddressFilter(q.AddressPatterns)
	if !requiresExact || q.Limit <= 0 {
		readQuery := q
		if maxCandidates >= 0 &&
			(q.Limit <= 0 || q.Limit > maxCandidates+1) {
			boundedQuery := *q
			boundedQuery.Limit = maxCandidates + 1
			readQuery = &boundedQuery
		}
		utxos, err := d.utxoStore().GetUtxosWithHistory(
			readQuery,
			txn.Metadata(),
		)
		if err != nil {
			return nil, 0, err
		}
		if maxCandidates >= 0 && len(utxos) > maxCandidates {
			return nil, len(utxos), ErrUtxoHistoryCandidateLimit
		}
		patterns := q.AddressPatterns
		if !requiresExact {
			patterns = nil
		}
		ret, err := d.loadAndFilterHistoricalUtxos(utxos, patterns, txn)
		return ret, len(utxos), err
	}

	// Exact address identity is only available in output CBOR. Scan coarse SQL
	// candidates in keyset order until Limit exact matches are collected, so a
	// page full of other address forms sharing the requested credential cannot
	// truncate the result. GetUtxosWithHistory uses the cursor's comparison
	// direction for both ascending and descending queries.
	scanQuery := *q
	// Keep each SQL query and decoded candidate set bounded without imposing a
	// total scan cap. The number of credential-sharing address forms before an
	// exact match is not a correctness boundary.
	scanQuery.Limit = min(max(q.Limit, 128), 1024)
	ret := make([]models.UtxoWithHistory, 0, q.Limit)
	candidates := 0
	for len(ret) < q.Limit {
		batchLimit := scanQuery.Limit
		if maxCandidates >= 0 {
			batchLimit = min(batchLimit, maxCandidates-candidates+1)
		}
		scanQuery.Limit = batchLimit
		batch, err := d.utxoStore().GetUtxosWithHistory(
			&scanQuery,
			txn.Metadata(),
		)
		if err != nil {
			return nil, candidates, err
		}
		candidates += len(batch)
		if maxCandidates >= 0 && candidates > maxCandidates {
			return nil, candidates, ErrUtxoHistoryCandidateLimit
		}
		filtered, err := d.loadAndFilterHistoricalUtxos(
			batch,
			q.AddressPatterns,
			txn,
		)
		if err != nil {
			return nil, candidates, err
		}
		remaining := q.Limit - len(ret)
		if len(filtered) > remaining {
			filtered = filtered[:remaining]
			// A paging caller resumes after the last returned match and reads
			// the rest of this batch again, so charging those rows here would
			// count them twice against a budget shared across pages.
			keep := filtered[len(filtered)-1]
			for i, u := range slices.Backward(batch) {
				if u.OutputIdx == keep.OutputIdx &&
					bytes.Equal(u.TxId, keep.TxId) {
					candidates -= len(batch) - 1 - i
					break
				}
			}
		}
		ret = append(ret, filtered...)
		if len(batch) < scanQuery.Limit || len(batch) == 0 {
			break
		}
		last := batch[len(batch)-1]
		scanQuery.After = &models.UtxoOrderingCursor{
			Slot:       last.TxSlot,
			BlockIndex: last.TxBlockIndex,
			OutputIdx:  last.OutputIdx,
			TxId:       last.TxId,
		}
	}
	return ret, candidates, nil
}

func (d *Database) loadAndFilterHistoricalUtxos(
	utxos []models.UtxoWithHistory,
	patterns []models.UtxoAddressPattern,
	txn *Txn,
) ([]models.UtxoWithHistory, error) {
	for i := range utxos {
		if err := loadCbor(&utxos[i].Utxo, txn); err != nil {
			return nil, err
		}
	}
	return filterUtxoViews(context.Background(), utxos, patterns, "historical UTxO",
		func(item *models.UtxoWithHistory) *models.Utxo { return &item.Utxo })
}

// GetControlledAmountByCredential returns the sum of live UTxO amounts
// controlled by the given stake credential.
func (d *Database) GetControlledAmountByCredential(
	ctx context.Context,
	credentialTag uint8,
	stakingKey []byte,
	txn *Txn,
) (uint64, error) {
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	total, err := d.utxoStore().GetControlledAmountByCredential(
		credentialTag,
		stakingKey,
		txn.Metadata(),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"get controlled amount by credential tag=%d key=%x: %w",
			credentialTag,
			stakingKey,
			err,
		)
	}
	return total, nil
}

// GetUtxoPaymentScriptByCredential returns, for the given bounded set of
// payment-key hashes previously observed under a stake credential, whether
// each payment credential is a script hash. See the metadata store
// interface doc comment for the full contract.
func (d *Database) GetUtxoPaymentScriptByCredential(
	ctx context.Context,
	credentialTag uint8,
	stakingKey []byte,
	paymentKeys [][]byte,
	txn *Txn,
) (map[string]bool, error) {
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	ret, err := d.utxoStore().GetUtxoPaymentScriptByCredential(
		credentialTag,
		stakingKey,
		paymentKeys,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get payment script by stake credential: %w",
			err,
		)
	}
	return ret, nil
}

func (d *Database) UtxosByAddressWithOrdering(
	ctx context.Context,
	q *models.UtxoWithOrderingQuery,
	txn *Txn,
) ([]models.UtxoWithOrdering, error) {
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	if q == nil {
		return nil, models.ErrNilUtxoWithOrderingQuery
	}
	if q.MatchAllAddresses ||
		!models.RequiresExactAddressFilter(q.AddressPatterns) ||
		q.Limit <= 0 {
		utxos, err := d.utxoStore().GetUtxosByAddressWithOrdering(
			q,
			txn.Metadata(),
		)
		if err != nil {
			return nil, err
		}
		return d.loadAndFilterOrderedUtxos(ctx, utxos, q.AddressPatterns, txn)
	}

	// Exact address identity is only available in output CBOR. Scan coarse SQL
	// candidates in keyset order until Limit exact matches are collected, so a
	// page full of enterprise/pointer siblings cannot truncate the result.
	scanQuery := *q
	scanQuery.Limit = max(q.Limit, 128)
	ret := make([]models.UtxoWithOrdering, 0, q.Limit)
	candidatesProcessed := 0
	for len(ret) < q.Limit {
		remainingCandidates := exactAddressCandidateScanLimit -
			candidatesProcessed
		if remainingCandidates <= 0 {
			return ret, errExactAddressCandidateScanLimit
		}
		scanQuery.Limit = min(scanQuery.Limit, remainingCandidates)
		batch, err := d.utxoStore().GetUtxosByAddressWithOrdering(
			&scanQuery,
			txn.Metadata(),
		)
		if err != nil {
			return nil, err
		}
		candidatesProcessed += len(batch)
		filtered, err := d.loadAndFilterOrderedUtxos(
			ctx,
			batch,
			q.AddressPatterns,
			txn,
		)
		if err != nil {
			return nil, err
		}
		remaining := q.Limit - len(ret)
		if len(filtered) > remaining {
			filtered = filtered[:remaining]
		}
		ret = append(ret, filtered...)
		if len(batch) < scanQuery.Limit || len(batch) == 0 {
			break
		}
		if len(ret) < q.Limit &&
			candidatesProcessed >= exactAddressCandidateScanLimit {
			return ret, errExactAddressCandidateScanLimit
		}
		last := batch[len(batch)-1]
		scanQuery.After = &models.UtxoOrderingCursor{
			Slot:       last.TxSlot,
			BlockIndex: last.TxBlockIndex,
			OutputIdx:  last.OutputIdx,
			TxId:       last.TxId,
		}
	}
	return ret, nil
}

const matchingUtxoRefBatchSize = 128

// MatchingUtxoRefsByAddressWithOrderingBounded scans exact-address candidates
// in one transaction until it has q.Limit matches, exhausts the result set, or
// reaches either work budget. exhausted reports whether the exact total is
// known from this scan.
func (d *Database) MatchingUtxoRefsByAddressWithOrderingBounded(
	ctx context.Context,
	q *models.UtxoWithOrderingQuery,
	txn *Txn,
	maxCandidates int,
	maxCborBytes int,
) ([]models.UtxoId, bool, error) {
	if ctx == nil {
		return nil, false, errors.New("bounded reference scan context is nil")
	}
	if q == nil {
		return nil, false, models.ErrNilUtxoWithOrderingQuery
	}
	if txn == nil || q.Limit <= 0 || maxCandidates <= 0 || maxCborBytes <= 0 ||
		q.MatchAllAddresses ||
		!models.RequiresExactAddressFilter(q.AddressPatterns) {
		return nil, false, errors.New("bounded reference scan requires an exact-address query and transaction")
	}
	scanQuery := *q
	scanQuery.Limit = min(matchingUtxoRefBatchSize, maxCandidates)
	scanQuery.SkipAssets = true
	scanQuery.Offset = 0
	refs := make([]models.UtxoId, 0, min(q.Limit, maxCandidates))
	candidates := 0
	cborBytes := 0
	for candidates < maxCandidates && len(refs) < q.Limit {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		scanQuery.Limit = min(matchingUtxoRefBatchSize, maxCandidates-candidates)
		batch, err := d.utxoStore().GetUtxosByAddressWithOrdering(
			&scanQuery,
			txn.Metadata(),
		)
		if err != nil {
			return nil, false, err
		}
		for i := range batch {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			if err := loadCbor(&batch[i].Utxo, txn); err != nil {
				return nil, false, err
			}
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			cborBytes += len(batch[i].Cbor)
			if cborBytes > maxCborBytes {
				return nil, false, models.ErrUtxoQueryBudgetExceeded
			}
			output, err := batch[i].Decode()
			if err != nil {
				return nil, false, fmt.Errorf(
					"decode UTxO %x#%d for exact address match: %w",
					batch[i].TxId,
					batch[i].OutputIdx,
					err,
				)
			}
			match, err := models.MatchesUtxoAddressPatterns(
				output.Address(),
				q.AddressPatterns,
			)
			if err != nil {
				return nil, false, err
			}
			candidates++
			if match {
				refs = append(refs, models.UtxoId{
					Hash: batch[i].TxId,
					Idx:  batch[i].OutputIdx,
				})
				if len(refs) == q.Limit {
					return refs, false, nil
				}
			}
		}
		if len(batch) < scanQuery.Limit {
			return refs, true, nil
		}
		last := batch[len(batch)-1]
		scanQuery.After = &models.UtxoOrderingCursor{
			Slot:       last.TxSlot,
			BlockIndex: last.TxBlockIndex,
			OutputIdx:  last.OutputIdx,
			TxId:       last.TxId,
		}
	}
	return refs, false, nil
}

// UtxoRefsByIndexedAddressPage returns an exact-address page and total from
// the API-mode address index. indexed is false when the index is unavailable
// or its startup backfill has not completed, allowing callers to use their
// bounded CBOR scan instead.
func (d *Database) UtxoRefsByIndexedAddressPage(
	ctx context.Context,
	address lcommon.Address,
	descending bool,
	offset int,
	limit int,
	txn *Txn,
) ([]models.UtxoId, int, bool, error) {
	if ctx == nil {
		return nil, 0, false, errors.New("indexed address page context is nil")
	}
	if offset < 0 || limit <= 0 {
		return nil, 0, false, errors.New("indexed address page bounds must be positive")
	}
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	if d.StorageMode() != types.StorageModeAPI {
		return nil, 0, false, nil
	}
	complete, err := d.GetSyncState(utxoAddressBackfillSyncKey, txn)
	if err != nil {
		return nil, 0, false, err
	}
	if complete != "true" {
		return nil, 0, false, nil
	}
	query := &models.UtxoWithOrderingQuery{
		IndexedExactAddress: []byte(address.String()),
		Limit:               limit,
		Offset:              offset,
		Descending:          descending,
		SkipAssets:          true,
	}
	rows, err := d.utxoStore().GetUtxosByAddressWithOrdering(
		query,
		txn.Metadata(),
	)
	if err != nil {
		return nil, 0, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, false, err
	}
	countQuery := *query
	countQuery.Limit = 0
	countQuery.Offset = 0
	total, err := d.utxoStore().CountUtxosByAddressWithOrdering(
		&countQuery,
		txn.Metadata(),
	)
	if err != nil {
		return nil, 0, false, err
	}
	refs := make([]models.UtxoId, len(rows))
	for i := range rows {
		refs[i] = models.UtxoId{Hash: rows[i].TxId, Idx: rows[i].OutputIdx}
	}
	return refs, total, true, nil
}

// CountUtxosByAddressWithOrdering returns the number of live UTxOs matching
// q's SQL predicate. Exact-address patterns that require CBOR filtering are
// rejected; callers can use IndexedExactAddress when the API address index is
// complete.
func (d *Database) CountUtxosByAddressWithOrdering(
	ctx context.Context,
	q *models.UtxoWithOrderingQuery,
	txn *Txn,
) (int, error) {
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	if q == nil {
		return 0, models.ErrNilUtxoWithOrderingQuery
	}
	count, err := d.utxoStore().
		CountUtxosByAddressWithOrdering(q, txn.Metadata())
	if err != nil {
		return 0, fmt.Errorf("count utxos by address: %w", err)
	}
	return count, nil
}

// AssetHolders returns one exact metadata-aggregated holder page.
func (d *Database) AssetHolders(
	ctx context.Context,
	query models.AssetHolderQuery,
	txn *Txn,
) ([]models.AssetHolder, int, error) {
	if ctx == nil {
		return nil, 0, errors.New("asset holders: context must not be nil")
	}
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	return d.utxoStore().GetAssetHolders(query, txn.Metadata())
}

func (d *Database) UtxoBalanceByAddressAtSlot(
	ctx context.Context,
	address lcommon.Address,
	slot uint64,
	txn *Txn,
) (models.AddressBalance, error) {
	if ctx == nil {
		return models.AddressBalance{}, errors.New("UTxO balance context must not be nil")
	}
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	if d.StorageMode() != types.StorageModeAPI {
		return models.AddressBalance{}, errors.New(
			"historical address balance requires API storage mode",
		)
	}
	complete, err := d.GetSyncState(utxoAddressBackfillSyncKey, txn)
	if err != nil {
		return models.AddressBalance{}, err
	}
	if complete != "true" {
		return models.AddressBalance{}, errors.New(
			"historical address index is incomplete",
		)
	}
	return d.utxoStore().GetUtxoBalanceByAddressAtSlot(
		[]byte(address.String()), slot, ExactAddressCandidateScanLimit,
		txn.Metadata(),
	)
}

func (d *Database) backfillUtxoAddresses(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.StorageMode() != types.StorageModeAPI {
		return nil
	}
	complete, err := d.GetSyncState(utxoAddressBackfillSyncKey, nil)
	if err != nil {
		return err
	}
	if complete == "true" {
		return nil
	}

	const batchSize = 128
	var afterID uint
	var processed, lastLogged int
	started := time.Now()
	d.logger.Info("starting UTxO address backfill")
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		nextID, count, err := d.backfillUtxoAddressBatch(
			ctx, afterID, batchSize,
		)
		if err != nil {
			return err
		}
		afterID = nextID
		if count == 0 {
			break
		}
		processed += count
		if processed-lastLogged >= 10_000 {
			d.logger.Info(
				"UTxO address backfill progress",
				"utxos", processed,
				"last_utxo_id", afterID,
			)
			lastLogged = processed
		}
	}
	if err := d.SetSyncState(utxoAddressBackfillSyncKey, "true", nil); err != nil {
		return err
	}
	d.logger.Info(
		"completed UTxO address backfill",
		"utxos", processed,
		"duration", time.Since(started),
	)
	return nil
}

type utxoAddressRow struct {
	id      uint
	address []byte
}

func (d *Database) resolveUtxoAddresses(
	ctx context.Context,
	utxos []models.Utxo,
	txn *Txn,
) ([]utxoAddressRow, error) {
	rows := make([]utxoAddressRow, 0, len(utxos))
	for i := range utxos {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := loadCbor(&utxos[i], txn); err != nil {
			return nil, fmt.Errorf(
				"load UTxO %x#%d: %w",
				utxos[i].TxId,
				utxos[i].OutputIdx,
				err,
			)
		}
		output, err := utxos[i].Decode()
		if err != nil {
			return nil, fmt.Errorf(
				"decode UTxO %x#%d: %w",
				utxos[i].TxId,
				utxos[i].OutputIdx,
				err,
			)
		}
		address := []byte(output.Address().String())
		if len(address) == 0 {
			return nil, errors.New("decoded UTxO address is empty")
		}
		rows = append(rows, utxoAddressRow{
			id:      utxos[i].ID,
			address: address,
		})
	}
	return rows, nil
}

func (d *Database) backfillUtxoAddressBatch(
	ctx context.Context,
	afterID uint,
	limit int,
) (uint, int, error) {
	var rows []utxoAddressRow
	err := d.Transaction(ctx, false).Do(func(txn *Txn) error {
		utxos, err := d.utxoStore().GetUtxosMissingAddress(
			afterID,
			limit,
			txn.Metadata(),
		)
		if err != nil {
			return err
		}
		rows, err = d.resolveUtxoAddresses(ctx, utxos, txn)
		return err
	})
	if err != nil {
		return afterID, 0, err
	}
	if len(rows) == 0 {
		return afterID, 0, nil
	}
	err = d.Transaction(ctx, true).Do(func(txn *Txn) error {
		for _, row := range rows {
			if err := d.utxoStore().SetUtxoAddress(
				row.id,
				row.address,
				txn.Metadata(),
			); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return afterID, 0, err
	}
	return rows[len(rows)-1].id, len(rows), nil
}

func filterUtxosByAddressPatterns(
	ctx context.Context,
	utxos []models.Utxo,
	patterns []models.UtxoAddressPattern,
) ([]models.Utxo, error) {
	return filterUtxoViews(ctx, utxos, patterns, "UTxO",
		func(item *models.Utxo) *models.Utxo { return item })
}

func filterUtxoViews[T any](
	ctx context.Context,
	utxos []T,
	patterns []models.UtxoAddressPattern,
	description string,
	utxo func(*T) *models.Utxo,
) ([]T, error) {
	if !models.RequiresExactAddressFilter(patterns) {
		return utxos, nil
	}
	result := make([]T, 0, len(utxos))
	// A filter of only exact addresses is matched by keyed lookup; a linear
	// scan would cost candidates x patterns.
	var exactSet map[string]struct{}
	if exactOnly := onlyExactPatterns(patterns); exactOnly {
		exactSet = make(map[string]struct{}, len(patterns))
		for i := range patterns {
			exactSet[string(patterns[i].ExactAddress)] = struct{}{}
		}
	}
	for i := range utxos {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		item := utxo(&utxos[i])
		output, err := item.Decode()
		if err != nil {
			return nil, fmt.Errorf(
				"decode %s %x#%d for exact address match: %w",
				description, item.TxId, item.OutputIdx, err,
			)
		}
		if exactSet != nil {
			addrBytes, err := output.Address().Bytes()
			if err != nil {
				return nil, fmt.Errorf("encode output address: %w", err)
			}
			if _, ok := exactSet[string(addrBytes)]; ok {
				result = append(result, utxos[i])
			}
			continue
		}
		match, err := models.MatchesUtxoAddressPatterns(output.Address(), patterns)
		if err != nil {
			return nil, err
		}
		if match {
			result = append(result, utxos[i])
		}
	}
	return result, nil
}

// onlyExactPatterns reports whether every pattern is a bare exact address,
// the only shape a set lookup answers identically to the pattern contract.
func onlyExactPatterns(patterns []models.UtxoAddressPattern) bool {
	for i := range patterns {
		p := patterns[i]
		if len(p.ExactAddress) == 0 ||
			len(p.PaymentPart) > 0 ||
			len(p.DelegationPart) > 0 {
			return false
		}
	}
	return len(patterns) > 0
}

func (d *Database) loadAndFilterOrderedUtxos(
	ctx context.Context,
	utxos []models.UtxoWithOrdering,
	patterns []models.UtxoAddressPattern,
	txn *Txn,
) ([]models.UtxoWithOrdering, error) {
	for i := range utxos {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := loadCbor(&utxos[i].Utxo, txn); err != nil {
			return nil, err
		}
	}
	return filterUtxoViews(ctx, utxos, patterns, "UTxO",
		func(item *models.UtxoWithOrdering) *models.Utxo { return &item.Utxo })
}

// UtxosByAssets returns UTxOs that contain the specified assets
// policyId: the policy ID of the asset (required)
// assetName: the asset name (pass nil to match all assets under the policy, or empty []byte{} to match assets with empty names)
func (d *Database) UtxosByAssets(
	ctx context.Context,
	policyId []byte,
	assetName []byte,
	txn *Txn,
) ([]models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	utxos, err := d.utxoStore().GetUtxosByAssets(
		policyId,
		assetName,
		txn.Metadata(),
	)
	if err != nil {
		return nil, err
	}
	for i := range utxos {
		if err := loadCbor(&utxos[i], txn); err != nil {
			return nil, err
		}
	}
	return utxos, nil
}

// ConsumedUtxoPruneFloorSyncKey is the durable sync_state marker key
// recording the highest slot the periodic consumed-UTxO cleanup
// (ledger.LedgerState's cleanupConsumedUtxos, which calls
// UtxosDeleteConsumed below) has ever begun hard-deleting spent UTxO rows
// up to. Both ledger (which writes it via persistConsumedUtxoPruneFloor)
// and database/lifecycle's disaster-recovery Truncate (which reads it to
// refuse a target older than what routine cleanup has already destroyed)
// need to agree on this key, so it is exported from here as the single
// source of truth for its name rather than duplicated as a string literal
// in both packages. UtxosDeleteConsumed removes rows outright, while
// TruncateAfterSlot can only restore rows that still exist; a rollback below
// this marker would therefore leave the live UTxO set incomplete.
const ConsumedUtxoPruneFloorSyncKey = "consumed_utxo_prune_floor"

// CountUtxosConsumedUpTo counts, up to limit, the spent UTxO rows that
// UtxosDeleteConsumed with the same slot would remove. It reads at most limit
// rows, so a result equal to limit means at least that many.
func (d *Database) CountUtxosConsumedUpTo(
	ctx context.Context,
	slot uint64,
	limit int,
) (int, error) {
	txn := d.Transaction(ctx, false)
	defer txn.Rollback() //nolint:errcheck
	utxos, err := d.utxoStore().GetUtxosDeletedBeforeSlot(
		slot,
		limit,
		txn.Metadata(),
	)
	if err != nil {
		return 0, fmt.Errorf("count consumed UTxOs: %w", err)
	}
	return len(utxos), nil
}

func (d *Database) UtxosDeleteConsumed(
	ctx context.Context,
	slot uint64,
	limit int,
	txn *Txn,
) (int, error) {
	owned := false
	if txn == nil {
		txn = d.Transaction(ctx, true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	// Get UTxOs that are marked as deleted and older than our slot window
	utxos, err := d.utxoStore().GetUtxosDeletedBeforeSlot(
		slot,
		limit,
		txn.Metadata(),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"failed to query consumed UTxOs during cleanup: %w",
			err,
		)
	}
	utxoCount := len(utxos)
	var pruneFloor uint64
	if utxoCount > 0 {
		// Read the floor before deleting anything. A malformed or unreadable
		// value must not follow irreversible blob deletes. No-op sweeps skip
		// this read so they do not fail on an unused corrupt floor.
		pruneFloor, err = d.ConsumedUtxoPruneFloor(txn)
		if err != nil {
			return 0, err
		}
	}
	deleteUtxos := make([]models.UtxoId, utxoCount)
	for idx, utxo := range utxos {
		deleteUtxos[idx] = models.UtxoId{Hash: utxo.TxId, Idx: utxo.OutputIdx}
	}

	// Delete blob data first. A failure here does not stop the metadata
	// delete below: metadata is the source of truth, and leaving a consumed
	// UTxO in the live set to keep its blob reachable would be the worse
	// outcome. The objects it strands are counted and logged rather than
	// passed over, because nothing reclaims them afterwards.
	if blobErr := deleteUtxoBlobs(d, utxos, txn); blobErr != nil {
		d.logger.Error(
			"consumed UTxO blob delete left unreachable objects",
			"error", blobErr,
			"utxos", len(utxos),
		)
	}

	// Then delete metadata (source of truth)
	err = d.utxoStore().DeleteUtxos(deleteUtxos, txn.Metadata())
	if err != nil {
		return 0, err
	}

	// Record how deep spent rows have been removed, in the same transaction
	// that removes them. TruncateAfterSlot restores spent UTxOs with an
	// UPDATE, which cannot reach a row that no longer exists, so a rollback
	// below this slot silently leaves the live set short of every output
	// consumed above it. The floor only ever moves up: a sweep
	// at a lower slot does not make rows an earlier, higher sweep removed
	// restorable again.
	if utxoCount > 0 && slot > pruneFloor {
		if err := d.writeConsumedUtxoPruneFloor(slot, txn); err != nil {
			return 0, err
		}
	}

	if owned {
		if err := txn.Commit(); err != nil {
			return 0, err
		}
		owned = false
	}

	return utxoCount, nil
}

// UtxosDeleteSpentInBatch removes the rows of outputs that one trusted batch
// both created and spent, inside the transaction that applied it, so they are
// never committed. Every id must name a row the same transaction has already
// marked spent. slot is the last slot the transaction applied; the
// consumed-UTxO prune floor is raised to it, as UtxosDeleteConsumed does,
// because a rollback below it could not restore the removed rows.
func (d *Database) UtxosDeleteSpentInBatch(
	ids []models.UtxoId,
	slot uint64,
	txn *Txn,
) error {
	return d.utxosDropSpentInBatch(ids, slot, txn, true)
}

// UtxosDropSpentInBatchBlobs is UtxosDeleteSpentInBatch for a batch whose
// metadata rows a batch UTxO overlay already handled: it deletes the blob
// offset keys of ids and raises the consumed-UTxO prune floor to slot the
// same way, but deletes no metadata row.
func (d *Database) UtxosDropSpentInBatchBlobs(
	ids []models.UtxoId,
	slot uint64,
	txn *Txn,
) error {
	return d.utxosDropSpentInBatch(ids, slot, txn, false)
}

func (d *Database) utxosDropSpentInBatch(
	ids []models.UtxoId,
	slot uint64,
	txn *Txn,
	deleteRows bool,
) error {
	if len(ids) == 0 {
		return nil
	}
	if txn == nil {
		return types.ErrNilTxn
	}
	pruneFloor, err := d.ConsumedUtxoPruneFloor(txn)
	if err != nil {
		return err
	}
	utxos := make([]models.Utxo, len(ids))
	for i, id := range ids {
		utxos[i] = models.Utxo{TxId: id.Hash, OutputIdx: id.Idx}
	}
	if blobErr := deleteUtxoBlobs(d, utxos, txn); blobErr != nil {
		d.logger.Error(
			"trusted batch UTxO blob delete left unreachable objects",
			"error", blobErr,
			"utxos", len(utxos),
		)
	}
	if deleteRows {
		if err := d.utxoStore().DeleteUtxos(ids, txn.Metadata()); err != nil {
			return err
		}
	}
	if slot > pruneFloor {
		return d.writeConsumedUtxoPruneFloor(slot, txn)
	}
	return nil
}

func (d *Database) UtxosDeleteRolledback(
	ctx context.Context,
	slot uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.Transaction(ctx, true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	d.noteUtxoRollback(slot, txn)
	utxos, err := d.utxoStore().GetUtxosAddedAfterSlot(slot, txn.Metadata())
	if err != nil {
		return err
	}

	// Delete blob data first. As above, a failure must not stop the metadata
	// delete: a rolled-back UTxO cannot stay in the live set. The stranded
	// objects are counted and logged instead of ignored.
	if blobErr := deleteUtxoBlobs(d, utxos, txn); blobErr != nil {
		d.logger.Error(
			"rolled-back UTxO blob delete left unreachable objects",
			"error", blobErr,
			"slot", slot,
			"utxos", len(utxos),
		)
	}

	// Then delete metadata (source of truth)
	err = d.utxoStore().DeleteUtxosAfterSlot(slot, txn.Metadata())
	if err != nil {
		return err
	}

	if owned {
		if err := txn.Commit(); err != nil {
			return err
		}
		owned = false
	}

	return nil
}

func (d *Database) restoreUtxoAddressesAfterSlot(
	ctx context.Context,
	slot uint64,
	txn *Txn,
) error {
	if d.StorageMode() != types.StorageModeAPI {
		return nil
	}
	const batchSize = 128
	loadTxn := txn
	if txn.Blob() == nil {
		var cleanup func()
		loadTxn, cleanup = txn.withBlobForRecovery()
		defer cleanup()
	}
	var afterID uint
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		utxos, err := d.utxoStore().GetSpentUtxosMissingAddressAfterSlot(
			slot, afterID, batchSize, txn.Metadata(),
		)
		if err != nil {
			return err
		}
		rows, err := d.resolveUtxoAddresses(ctx, utxos, loadTxn)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := d.utxoStore().SetUtxoAddress(
				row.id, row.address, txn.Metadata(),
			); err != nil {
				return err
			}
			afterID = row.id
		}
		if len(utxos) < batchSize {
			return nil
		}
	}
}

func (d *Database) UtxosUnspend(
	ctx context.Context,
	slot uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = NewMetadataOnlyTxn(ctx, d, true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	d.noteUtxoRollback(slot, txn)
	if err := d.utxoStore().SetUtxosNotDeletedAfterSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return err
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return err
		}
		owned = false
	}
	return nil
}

// IterateLiveUtxos invokes fn once for each live UTxO row
// (DeletedSlot == 0). The callback receives a pointer to a row whose
// Cbor field has been populated from blob storage (or recovered from
// the producing block) — copy out anything you intend to retain
// because the underlying buffer is reused between callbacks.
// Returning a non-nil error from fn aborts iteration and that error
// is propagated up; CBOR-loading failures are also propagated.
// When txn is nil a read transaction is opened internally.
func (d *Database) IterateLiveUtxos(
	ctx context.Context,
	txn *Txn,
	fn func(*models.Utxo) error,
) error {
	withCbor := func(t *Txn) func(*models.Utxo) error {
		return func(u *models.Utxo) error {
			if err := loadCbor(u, t); err != nil {
				return fmt.Errorf(
					"load utxo cbor tx=%x idx=%d: %w",
					u.TxId[:8], u.OutputIdx, err,
				)
			}
			return fn(u)
		}
	}
	if txn != nil {
		return d.utxoStore().IterateLiveUtxos(txn.Metadata(), withCbor(txn))
	}
	return d.Transaction(ctx, false).Do(func(t *Txn) error {
		return d.utxoStore().IterateLiveUtxos(t.Metadata(), withCbor(t))
	})
}

// IterateLiveUtxoRefs invokes fn once for each live UTxO row (DeletedSlot ==
// 0), like IterateLiveUtxos, but does not resolve u.Cbor -- it is left as
// the store's raw stored value (a CborOffset reference, not the referenced
// output CBOR). For a caller that needs every live UTxO's CBOR and wants to
// resolve it itself (e.g. across a worker pool, rather than serially via
// IterateLiveUtxos' inline loadCbor -- see ledger.queryShelleyUtxoWhole).
// As with IterateLiveUtxos, the callback receives a pointer to a row whose
// underlying buffer is reused between callbacks -- copy out anything (e.g.
// u.TxId) you intend to retain past the current call.
// When txn is nil a read transaction is opened internally.
func (d *Database) IterateLiveUtxoRefs(
	ctx context.Context,
	txn *Txn,
	fn func(*models.Utxo) error,
) error {
	iterator := d.utxoStore()
	if txn != nil {
		return iterator.IterateLiveUtxosContext(ctx, txn.Metadata(), fn)
	}
	return d.Transaction(ctx, false).Do(func(t *Txn) error {
		return iterator.IterateLiveUtxosContext(ctx, t.Metadata(), fn)
	})
}

// IterateUtxoRefsAsOf is IterateLiveUtxoRefs' historical counterpart: it
// invokes fn once for each UTxO row live as of atSlot rather than live
// right now, and likewise does not resolve u.Cbor (see
// ledger.queryShelleyUtxoWhole, its only caller as of this writing, which
// resolves CBOR itself across a worker pool). As with IterateLiveUtxoRefs,
// the callback receives a pointer to a row whose underlying buffer is
// reused between callbacks -- copy out anything you intend to retain past
// the current call. When txn is nil a read transaction is opened
// internally.
//
// Callers pinning atSlot should check checkUtxoRetentionWindow (or the
// equivalent guarantee for their own use) first: a slot older than this
// node's spent-UTxO retention floor cannot be answered correctly by this
// query, since the rows it would need may already be pruned -- see
// IterateUtxosAsOf's doc comment on the MetadataStore interface.
func (d *Database) IterateUtxoRefsAsOf(
	ctx context.Context,
	atSlot uint64,
	txn *Txn,
	fn func(*models.Utxo) error,
) error {
	iterator := d.utxoStore()
	if txn != nil {
		return iterator.IterateUtxosAsOfContext(
			ctx, atSlot, txn.Metadata(), fn,
		)
	}
	return d.Transaction(ctx, false).Do(func(t *Txn) error {
		return iterator.IterateUtxosAsOfContext(ctx, atSlot, t.Metadata(), fn)
	})
}

// ErrUtxoHistoryRolledBack reports that a paged as-of UTxO read overlapped a
// rollback below its slot. Rows the rollback removed or unspent may already
// have been read or skipped, so the pages no longer describe one state; the
// caller retries or fails the read.
var ErrUtxoHistoryRolledBack = errors.New(
	"UTxO history rolled back below the slot of a paged read",
)

// utxoAsOfRead is one paged as-of read in progress. disturbed is set when a
// rollback below atSlot may have changed the rows it walks.
type utxoAsOfRead struct {
	atSlot    uint64
	disturbed atomic.Bool
}

// noteUtxoRollback marks every paged read below whose slot a rollback to slot
// rewrites UTxO history. A rollback to slot deletes rows added after it and
// unspends rows spent after it, which leaves the set live as of any slot at or
// below it unchanged. It marks before the rollback writes and again when txn
// finishes, so a read that starts while the rollback is in flight is caught by
// the second mark.
func (d *Database) noteUtxoRollback(slot uint64, txn *Txn) {
	mark := func() {
		d.utxoAsOfReadsMu.Lock()
		defer d.utxoAsOfReadsMu.Unlock()
		for r := range d.utxoAsOfReads {
			if slot < r.atSlot {
				r.disturbed.Store(true)
			}
		}
	}
	mark()
	if txn != nil {
		txn.OnFinish(mark)
	}
}

// IterateUtxoRefsAsOfPaged invokes fn once for each UTxO live as of atSlot,
// like IterateUtxoRefsAsOf, but reads pageSize rows per short read
// transaction instead of holding one for the whole walk. Over SQLite a
// transaction held for a whole-UTxO walk -- minutes on a large chain -- keeps
// the WAL from restarting, and one walk after another keeps it growing.
//
// The pages agree with one another without sharing a snapshot because of
// how UTxO rows change. Blocks applied after atSlot add rows with a later
// added_slot and spend rows at a later slot, and the as-of predicate already
// excludes the first and includes the second. Spent-row pruning removes only
// rows spent before the prune floor, which an acquired-point pin holds at or
// below atSlot. Rows keep their id, so paging on it neither repeats nor skips
// a row. The one change that alters the as-of set is a rollback below atSlot,
// which noteUtxoRollback reports: this returns ErrUtxoHistoryRolledBack and
// the caller decides whether to retry. Callers must hold pruning at or below
// atSlot themselves.
func (d *Database) IterateUtxoRefsAsOfPaged(
	ctx context.Context,
	atSlot uint64,
	pageSize int,
	fn func(UtxoRef) error,
) error {
	if pageSize <= 0 {
		return errors.New("iterate UTxO refs as of slot: page size must be positive")
	}
	read := &utxoAsOfRead{atSlot: atSlot}
	d.utxoAsOfReadsMu.Lock()
	if d.utxoAsOfReads == nil {
		d.utxoAsOfReads = make(map[*utxoAsOfRead]struct{})
	}
	d.utxoAsOfReads[read] = struct{}{}
	d.utxoAsOfReadsMu.Unlock()
	defer func() {
		d.utxoAsOfReadsMu.Lock()
		delete(d.utxoAsOfReads, read)
		d.utxoAsOfReadsMu.Unlock()
	}()

	var afterID uint
	for {
		var page []models.Utxo
		err := d.MetadataTxn(ctx, false).Do(func(t *Txn) error {
			var err error
			page, err = d.utxoStore().UtxoRefsAsOfAfterID(
				ctx, atSlot, afterID, pageSize, t.Metadata(),
			)
			return err
		})
		if err != nil {
			return err
		}
		if read.disturbed.Load() {
			return ErrUtxoHistoryRolledBack
		}
		for i := range page {
			if len(page[i].TxId) != len(UtxoRef{}.TxId) {
				return fmt.Errorf(
					"iterate UTxO refs as of slot: row %d has a %d-byte tx id",
					page[i].ID, len(page[i].TxId),
				)
			}
			var ref UtxoRef
			copy(ref.TxId[:], page[i].TxId)
			ref.OutputIdx = page[i].OutputIdx
			if err := fn(ref); err != nil {
				return err
			}
			afterID = page[i].ID
		}
		if len(page) < pageSize {
			return nil
		}
	}
}

// MarkUtxosDeletedAtSlot marks every live UTxO row matching one of
// refs as deleted at atSlot. Refs that don't match any live row are
// silently ignored; rollback un-deletion is handled by the existing
// rollback path (SetUtxosNotDeletedAfterSlot). When txn is nil a
// write transaction is opened, committed on success and rolled back
// on error via Txn.Do.
func (d *Database) MarkUtxosDeletedAtSlot(
	ctx context.Context,
	txn *Txn,
	refs []types.UtxoKey,
	atSlot uint64,
) error {
	if len(refs) == 0 {
		return nil
	}
	if txn != nil {
		return d.utxoStore().MarkUtxosDeletedAtSlot(
			txn.Metadata(), refs, atSlot,
		)
	}
	return d.MetadataTxn(ctx, true).Do(func(t *Txn) error {
		return d.utxoStore().MarkUtxosDeletedAtSlot(
			t.Metadata(), refs, atSlot,
		)
	})
}
