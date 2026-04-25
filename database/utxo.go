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
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ErrUtxoNotFound signals that the metadata row for a UTxO does not
// exist (or was filtered out, e.g. by deleted_slot != 0 in the live
// view). Callers may use errors.Is to detect a genuinely-absent row.
var ErrUtxoNotFound = errors.New("utxo not found")

// ErrUtxoCborUnavailable signals that the metadata row for a UTxO
// exists but its CBOR could not be loaded from the blob store and
// could not be recovered from any indexed block — typically because
// the row was inserted directly (e.g. fixture seeding) without a
// corresponding blob, or because the producing block is missing.
// This is distinct from ErrUtxoNotFound: the row IS present in the
// live UTxO set; only the on-the-wire bytes are unrecoverable. Callers
// that only need indexed metadata fields can ignore this error.
var ErrUtxoCborUnavailable = errors.New("utxo cbor unavailable")

// deleteUtxoBlobs performs best-effort deletion of blob data for the given
// [models.Utxo] entries. Metadata remains the authoritative source of truth;
// blob deletions are supplementary. The caller [*Txn] is ignored — this
// function always creates and commits its own blob-only batches via the
// [Database], so callers should not expect blob deletes to participate in any
// outer transaction.
func deleteUtxoBlobs(d *Database, utxos []models.Utxo, _ *Txn) error {
	const batchSize = 500
	blob := d.Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}

	var deleteErrors int
	for start := 0; start < len(utxos); start += batchSize {
		end := min(start+batchSize, len(utxos))
		batchTxn := NewBlobOnlyTxn(d, true)
		for _, utxo := range utxos[start:end] {
			if err := blob.DeleteUtxo(batchTxn.Blob(), utxo.TxId, utxo.OutputIdx); err != nil {
				deleteErrors++
				d.logger.Debug(
					"failed to delete UTxO blob data",
					"txid", hex.EncodeToString(utxo.TxId),
					"output_idx", utxo.OutputIdx,
					"error", err,
				)
			}
		}
		if err := batchTxn.Commit(); err != nil {
			_ = batchTxn.Rollback()
			d.logger.Debug("blob delete batch commit failed", "error", err)
		}
	}
	if deleteErrors > 0 {
		d.logger.Debug(
			"blob deletion completed with errors",
			"failed",
			deleteErrors,
			"total",
			len(utxos),
		)
	}

	return nil
}

func loadCbor(u *models.Utxo, txn *Txn) error {
	db := txn.DB()
	// Use tiered cache if available
	if db.cborCache != nil {
		// Pass the blob transaction so we can see uncommitted writes
		// (important for intra-batch UTxO lookups during validation)
		blobTxn := txn.Blob()
		cbor, err := db.cborCache.ResolveUtxoCbor(u.TxId, u.OutputIdx, blobTxn)
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
	blob := db.Blob()
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
		blockCbor, _, err := blob.GetBlock(txn.Blob(), offset.BlockSlot, offset.BlockHash[:])
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

func recoverUtxoCbor(
	db *Database,
	txn *Txn,
	txId []byte,
	outputIdx uint32,
) ([]byte, error) {
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
		var txHashArray [32]byte
		copy(txHashArray[:], txId)
		ref := UtxoRef{TxId: txHashArray, OutputIdx: outputIdx}
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

func utxoRecoveryBlockForTx(
	db *Database,
	txn *Txn,
	txId []byte,
) (*models.Block, error) {
	// Try the blob-based path when both the blob store and a blob
	// transaction handle are available; otherwise skip straight to the
	// metadata-based lookup below.
	blob := db.Blob()
	blobTxn := txn.Blob()
	if blob != nil && blobTxn != nil {
		if txData, err := blob.GetTx(blobTxn, txId); err == nil {
			switch {
			case IsTxOffsetStorage(txData):
				offset, err := DecodeTxOffset(txData)
				if err != nil {
					return nil, fmt.Errorf(
						"decode tx offset for utxo recovery %x: %w",
						txId[:8],
						err,
					)
				}
				block, err := BlockByPointTxn(
					txn,
					ocommon.NewPoint(offset.BlockSlot, offset.BlockHash[:]),
				)
				if err != nil {
					return nil, fmt.Errorf(
						"lookup producer block from tx offset %x: %w",
						txId[:8],
						err,
					)
				}
				return &block, nil
			case IsTxCborPartsStorage(txData):
				parts, err := DecodeTxCborParts(txData)
				if err != nil {
					return nil, fmt.Errorf(
						"decode tx parts for utxo recovery %x: %w",
						txId[:8],
						err,
					)
				}
				block, err := BlockByPointTxn(
					txn,
					ocommon.NewPoint(parts.BlockSlot, parts.BlockHash[:]),
				)
				if err != nil {
					return nil, fmt.Errorf(
						"lookup producer block from tx parts %x: %w",
						txId[:8],
						err,
					)
				}
				return &block, nil
			}
		} else if !errors.Is(err, types.ErrBlobKeyNotFound) {
			return nil, fmt.Errorf(
				"lookup tx blob for utxo recovery %x: %w",
				txId[:8],
				err,
			)
		}
	}
	producerTx, err := db.metadata.GetTransactionByHash(txId, txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf(
			"lookup producer tx metadata for utxo recovery %x: %w",
			txId[:8],
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
			txId[:8],
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
	blob := db.Blob()
	if blob == nil {
		return nil
	}

	offsetData := EncodeUtxoOffset(offset)

	// Use the caller's blob txn when it is write-capable
	if txn != nil && txn.Blob() != nil && txn.IsReadWrite() {
		return blob.SetUtxo(txn.Blob(), txId, outputIdx, offsetData)
	}

	// Open a dedicated write transaction when the caller txn is
	// nil or its blob handle is read-only / absent.
	writeTxn := NewBlobOnlyTxn(db, true)
	if err := blob.SetUtxo(
		writeTxn.Blob(), txId, outputIdx, offsetData,
	); err != nil {
		_ = writeTxn.Rollback()
		return err
	}
	if err := writeTxn.Commit(); err != nil {
		_ = writeTxn.Rollback()
		return err
	}
	return nil
}

func (d *Database) UtxoByRef(
	txId []byte,
	outputIdx uint32,
	txn *Txn,
) (*models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	utxo, err := d.metadata.GetUtxo(txId, outputIdx, txn.Metadata())
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

// CreateUtxo inserts a Utxo row directly. The normal block-application
// path uses AddUtxos with UtxoSlot inputs; this is the simple-insert
// variant for callers that already have a populated model. When txn
// is nil a write transaction is opened, committed on success and
// rolled back on error via Txn.Do.
func (d *Database) CreateUtxo(txn *Txn, utxo *models.Utxo) error {
	if txn != nil {
		return d.metadata.CreateUtxo(txn.Metadata(), utxo)
	}
	return d.MetadataTxn(true).Do(func(t *Txn) error {
		return d.metadata.CreateUtxo(t.Metadata(), utxo)
	})
}

// UtxoByRefIncludingSpent returns a Utxo by reference,
// including spent (consumed) UTxOs.
func (d *Database) UtxoByRefIncludingSpent(
	txId []byte,
	outputIdx uint32,
	txn *Txn,
) (*models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	utxo, err := d.metadata.GetUtxoIncludingSpent(
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

func (d *Database) UtxosByAddress(
	addr ledger.Address,
	txn *Txn,
) ([]models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	utxos, err := d.metadata.GetUtxosByAddress(addr, txn.Metadata())
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

func (d *Database) UtxosByAddressWithOrdering(
	q *models.UtxoWithOrderingQuery,
	txn *Txn,
) ([]models.UtxoWithOrdering, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	utxos, err := d.metadata.GetUtxosByAddressWithOrdering(
		q,
		txn.Metadata(),
	)
	if err != nil {
		return nil, err
	}
	for i := range utxos {
		if err := loadCbor(&utxos[i].Utxo, txn); err != nil {
			return nil, err
		}
	}
	return utxos, nil
}

func (d *Database) UtxosByAddressAtSlot(
	addr lcommon.Address,
	slot uint64,
	txn *Txn,
) ([]models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	utxos, err := d.metadata.GetUtxosByAddressAtSlot(
		addr,
		slot,
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

// UtxosByAssets returns UTxOs that contain the specified assets
// policyId: the policy ID of the asset (required)
// assetName: the asset name (pass nil to match all assets under the policy, or empty []byte{} to match assets with empty names)
func (d *Database) UtxosByAssets(
	policyId []byte,
	assetName []byte,
	txn *Txn,
) ([]models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	utxos, err := d.metadata.GetUtxosByAssets(
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

func (d *Database) UtxosDeleteConsumed(
	slot uint64,
	limit int,
	txn *Txn,
) (int, error) {
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	// Get UTxOs that are marked as deleted and older than our slot window
	utxos, err := d.metadata.GetUtxosDeletedBeforeSlot(
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
	deleteUtxos := make([]models.UtxoId, utxoCount)
	for idx, utxo := range utxos {
		deleteUtxos[idx] = models.UtxoId{Hash: utxo.TxId, Idx: utxo.OutputIdx}
	}

	// Delete blob data first (best effort)
	_ = deleteUtxoBlobs(d, utxos, txn)

	// Then delete metadata (source of truth)
	err = d.metadata.DeleteUtxos(deleteUtxos, txn.Metadata())
	if err != nil {
		return 0, err
	}

	if owned {
		if err := txn.Commit(); err != nil {
			return 0, err
		}
		owned = false
	}

	return utxoCount, nil
}

func (d *Database) UtxosDeleteRolledback(
	slot uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	utxos, err := d.metadata.GetUtxosAddedAfterSlot(slot, txn.Metadata())
	if err != nil {
		return err
	}

	// Delete blob data first (best effort)
	_ = deleteUtxoBlobs(d, utxos, txn)

	// Then delete metadata (source of truth)
	err = d.metadata.DeleteUtxosAfterSlot(slot, txn.Metadata())
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

func (d *Database) UtxosUnspend(
	slot uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = NewMetadataOnlyTxn(d, true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	if err := d.metadata.SetUtxosNotDeletedAfterSlot(slot, txn.Metadata()); err != nil {
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

// RemoveByronAvvmUtxos applies the Shelley→Allegra HARDFORK rule
// (cardano-ledger Allegra/Translation.hs, returnRedeemAddrsToReserves):
// marks every live UTxO at a Byron redeem (AVVM) address as deleted at
// atSlot and returns the count and total lovelace reclaimed. The
// DeletedSlot bookkeeping lets a rollback past atSlot un-delete the rows
// via the existing SetUtxosNotDeletedAfterSlot path. Callers are
// responsible for adding totalLovelace to reserves; this wrapper does
// not touch NetworkState because reserves tracking is broader than the
// AVVM rule.
func (d *Database) RemoveByronAvvmUtxos(
	atSlot uint64,
	txn *Txn,
) (int, uint64, error) {
	owned := false
	if txn == nil {
		txn = NewMetadataOnlyTxn(d, true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	count, total, err := d.metadata.RemoveByronAvvmUtxos(
		atSlot,
		txn.Metadata(),
	)
	if err != nil {
		return 0, 0, fmt.Errorf(
			"remove Byron AVVM UTxOs at slot %d: %w",
			atSlot,
			err,
		)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return 0, 0, fmt.Errorf("commit transaction: %w", err)
		}
		owned = false
	}
	return count, total, nil
}
