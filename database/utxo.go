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
var ErrUtxoNotFound = types.ErrUtxoNotFound

// ErrUtxoCborUnavailable signals that the metadata row for a UTxO
// exists but its CBOR could not be loaded from the blob store and
// could not be recovered from any indexed block — typically because
// the row was inserted directly (e.g. fixture seeding) without a
// corresponding blob, or because the producing block is missing.
// This is distinct from ErrUtxoNotFound: the row IS present in the
// live UTxO set; only the on-the-wire bytes are unrecoverable. Callers
// that only need indexed metadata fields can ignore this error.
var ErrUtxoCborUnavailable = errors.New("utxo cbor unavailable")

const exactAddressCandidateScanLimit = 10_000

var errExactAddressCandidateScanLimit = errors.New(
	"exact address candidate scan limit reached",
)

// deleteUtxoBlobs deletes blob data for the given [models.Utxo] entries.
// Metadata remains the authoritative source of truth; blob deletions are
// supplementary. The caller [*Txn] is ignored — this function always creates
// and commits its own blob-only batches via the [Database], so callers should
// not expect blob deletes to participate in any outer transaction.
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
	for start := 0; start < len(utxos); start += batchSize {
		end := min(start+batchSize, len(utxos))
		batchTxn := NewBlobOnlyTxn(d, true)
		// Take the store from the batch's own transaction rather than
		// from the database once up front: each batch commits separately,
		// so a replacement between batches would otherwise leave later
		// batches deleting through a store that no longer owns their
		// transaction handles.
		blob := batchTxn.BlobStore()
		if blob == nil {
			batchTxn.Release()
			return types.ErrBlobStoreUnavailable
		}
		var batchDeleteErrors int
		for _, utxo := range utxos[start:end] {
			if err := blob.DeleteUtxo(batchTxn.Blob(), utxo.TxId, utxo.OutputIdx); err != nil {
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
		if err := batchTxn.Commit(); err != nil {
			deleteErrors += (end - start) - batchDeleteErrors
			_ = batchTxn.Rollback()
			d.logger.Warn(
				"UTxO blob delete batch commit failed",
				"batch_start", start,
				"batch_end", end,
				"batch_size", end-start,
				"error", err,
			)
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

	// Open a dedicated write transaction when the caller txn is
	// nil or its blob handle is read-only / absent.
	writeTxn := NewBlobOnlyTxn(db, true)
	blob := writeTxn.BlobStore()
	if blob == nil {
		writeTxn.Release()
		return nil
	}
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
	txId []byte,
	outputIdx uint32,
	txn *Txn,
) (bool, error) {
	if txn == nil {
		txn = d.Transaction(false)
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
	refs []models.UtxoId,
	txn *Txn,
) ([]models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(false)
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

// CreateUtxo inserts a Utxo row directly. The normal block-application
// path uses AddUtxos with UtxoSlot inputs; this is the simple-insert
// variant for callers that already have a populated model. When txn
// is nil a write transaction is opened, committed on success and
// rolled back on error via Txn.Do.
func (d *Database) CreateUtxo(txn *Txn, utxo *models.Utxo) error {
	if txn != nil {
		return d.utxoStore().CreateUtxo(txn.Metadata(), utxo)
	}
	return d.MetadataTxn(true).Do(func(t *Txn) error {
		return d.utxoStore().CreateUtxo(t.Metadata(), utxo)
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
func (d *Database) UtxosByAddress(
	addrs []ledger.Address,
	txn *Txn,
) ([]models.Utxo, error) {
	if len(addrs) == 0 {
		return nil, nil
	}
	if txn == nil {
		txn = d.Transaction(false)
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
	utxos, err := d.utxoStore().GetUtxosByAddress(patterns, txn.Metadata())
	if err != nil {
		return nil, err
	}
	for i := range utxos {
		if err := loadCbor(&utxos[i], txn); err != nil {
			return nil, err
		}
	}
	return filterUtxosByAddressPatterns(utxos, patterns)
}

// GetControlledAmountByCredential returns the sum of live UTxO amounts
// controlled by the given stake credential.
func (d *Database) GetControlledAmountByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn *Txn,
) (uint64, error) {
	if txn == nil {
		txn = d.Transaction(false)
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
	credentialTag uint8,
	stakingKey []byte,
	paymentKeys [][]byte,
	txn *Txn,
) (map[string]bool, error) {
	if txn == nil {
		txn = d.Transaction(false)
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
	q *models.UtxoWithOrderingQuery,
	txn *Txn,
) ([]models.UtxoWithOrdering, error) {
	if txn == nil {
		txn = d.Transaction(false)
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
		return d.loadAndFilterOrderedUtxos(utxos, q.AddressPatterns, txn)
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

// MatchingUtxoRefsByAddressWithOrdering returns the (TxId, OutputIdx)
// references of every live UTxO matching q's address patterns, in ascending
// producing-transaction-position order, without loading assets or
// retaining full rows. Unlike CountUtxosByAddressWithOrdering, this works
// for exact-address patterns too: it scans coarse SQL candidates in keyset
// batches (see UtxosByAddressWithOrdering's identical loop) and CBOR-decodes
// each to confirm the match, which is the same per-candidate cost the
// coarse predicate alone cannot avoid, but skips the asset loading and full
// UtxoWithOrdering retention a straight fetch would pay for every candidate
// instead of only the page a caller goes on to request via UtxosByRefs.
//
// The result both is the accurate total (its length) and can be sliced for
// a page's worth of references to pass to UtxosByRefs, letting a caller
// avoid materializing more than one page of an address's UTxO history.
func (d *Database) MatchingUtxoRefsByAddressWithOrdering(
	q *models.UtxoWithOrderingQuery,
	txn *Txn,
) ([]models.UtxoId, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	if q == nil {
		return nil, models.ErrNilUtxoWithOrderingQuery
	}
	if q.MatchAllAddresses ||
		!models.RequiresExactAddressFilter(q.AddressPatterns) {
		scanQuery := *q
		scanQuery.SkipAssets = true
		utxos, err := d.utxoStore().GetUtxosByAddressWithOrdering(
			&scanQuery,
			txn.Metadata(),
		)
		if err != nil {
			return nil, err
		}
		refs := make([]models.UtxoId, len(utxos))
		for i := range utxos {
			refs[i] = models.UtxoId{
				Hash: utxos[i].TxId,
				Idx:  utxos[i].OutputIdx,
			}
		}
		return refs, nil
	}

	// Unlike UtxosByAddressWithOrdering's page-fill scan, this loop must
	// visit every coarse candidate to produce an accurate total and cannot
	// stop early once a page's worth of matches is found, so it does not
	// apply exactAddressCandidateScanLimit: that cap bounds work spent
	// filling one bounded page, and applying it here would turn a valid
	// high-cardinality address listing into a server error instead of
	// bounding cost, which SkipAssets and the reference-only result
	// already do.
	scanQuery := *q
	scanQuery.Limit = 1024
	scanQuery.SkipAssets = true
	scanQuery.Offset = 0
	scanQuery.Descending = false
	refs := []models.UtxoId{}
	for {
		batch, err := d.utxoStore().GetUtxosByAddressWithOrdering(
			&scanQuery,
			txn.Metadata(),
		)
		if err != nil {
			return nil, err
		}
		for i := range batch {
			if err := loadCbor(&batch[i].Utxo, txn); err != nil {
				return nil, err
			}
			output, err := batch[i].Decode()
			if err != nil {
				return nil, fmt.Errorf(
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
				return nil, err
			}
			if match {
				refs = append(refs, models.UtxoId{
					Hash: batch[i].TxId,
					Idx:  batch[i].OutputIdx,
				})
			}
		}
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
	return refs, nil
}

// CountUtxosByAddressWithOrdering returns the number of live UTxOs matching
// q's coarse SQL predicate. See MetadataStore.CountUtxosByAddressWithOrdering:
// it errors if q's address patterns require CBOR-based exact-address
// filtering, since Dingo has no cheap way to compute an exact-address total
// without decoding every coarse candidate's output CBOR.
func (d *Database) CountUtxosByAddressWithOrdering(
	q *models.UtxoWithOrderingQuery,
	txn *Txn,
) (int, error) {
	if txn == nil {
		txn = d.Transaction(false)
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

func (d *Database) UtxosByAddressAtSlot(
	addr lcommon.Address,
	slot uint64,
	txn *Txn,
) ([]models.Utxo, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	pattern, err := models.ExactUtxoAddressPattern(addr)
	if err != nil {
		return nil, err
	}
	utxos, err := d.utxoStore().GetUtxosByAddressAtSlot(
		pattern,
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
	return filterUtxosByAddressPatterns(utxos, []models.UtxoAddressPattern{
		pattern,
	})
}

func filterUtxosByAddressPatterns(
	utxos []models.Utxo,
	patterns []models.UtxoAddressPattern,
) ([]models.Utxo, error) {
	if !models.RequiresExactAddressFilter(patterns) {
		return utxos, nil
	}
	ret := make([]models.Utxo, 0, len(utxos))
	for i := range utxos {
		output, err := utxos[i].Decode()
		if err != nil {
			return nil, fmt.Errorf(
				"decode UTxO %x#%d for exact address match: %w",
				utxos[i].TxId,
				utxos[i].OutputIdx,
				err,
			)
		}
		match, err := models.MatchesUtxoAddressPatterns(
			output.Address(),
			patterns,
		)
		if err != nil {
			return nil, err
		}
		if match {
			ret = append(ret, utxos[i])
		}
	}
	return ret, nil
}

func (d *Database) loadAndFilterOrderedUtxos(
	utxos []models.UtxoWithOrdering,
	patterns []models.UtxoAddressPattern,
	txn *Txn,
) ([]models.UtxoWithOrdering, error) {
	for i := range utxos {
		if err := loadCbor(&utxos[i].Utxo, txn); err != nil {
			return nil, err
		}
	}
	if !models.RequiresExactAddressFilter(patterns) {
		return utxos, nil
	}
	ret := make([]models.UtxoWithOrdering, 0, len(utxos))
	for i := range utxos {
		output, err := utxos[i].Decode()
		if err != nil {
			return nil, fmt.Errorf(
				"decode UTxO %x#%d for exact address match: %w",
				utxos[i].TxId,
				utxos[i].OutputIdx,
				err,
			)
		}
		match, err := models.MatchesUtxoAddressPatterns(
			output.Address(),
			patterns,
		)
		if err != nil {
			return nil, err
		}
		if match {
			ret = append(ret, utxos[i])
		}
	}
	return ret, nil
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
	return d.Transaction(false).Do(func(t *Txn) error {
		return d.utxoStore().IterateLiveUtxos(t.Metadata(), withCbor(t))
	})
}

// MarkUtxosDeletedAtSlot marks every live UTxO row matching one of
// refs as deleted at atSlot. Refs that don't match any live row are
// silently ignored; rollback un-deletion is handled by the existing
// rollback path (SetUtxosNotDeletedAfterSlot). When txn is nil a
// write transaction is opened, committed on success and rolled back
// on error via Txn.Do.
func (d *Database) MarkUtxosDeletedAtSlot(
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
	return d.MetadataTxn(true).Do(func(t *Txn) error {
		return d.utxoStore().MarkUtxosDeletedAtSlot(
			t.Metadata(), refs, atSlot,
		)
	})
}
