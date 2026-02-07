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
	"encoding/hex"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func (d *Database) SetTransaction(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	updateEpoch uint64,
	pparamUpdates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
	certDeposits map[int]uint64,
	offsets *BlockIngestionResult,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer txn.Rollback() //nolint:errcheck
	}

	blob := txn.DB().Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}

	// Store transaction CBOR offset - offsets MUST be available
	txHash := tx.Hash()
	var txHashArray [32]byte
	copy(txHashArray[:], txHash.Bytes())

	if offsets == nil {
		return fmt.Errorf(
			"missing offsets for transaction %s at slot %d: offsets must be computed",
			hex.EncodeToString(txHash.Bytes()[:8]),
			point.Slot,
		)
	}
	txOffset, ok := offsets.TxOffsets[txHashArray]
	if !ok {
		return fmt.Errorf(
			"missing TX offset for %s at slot %d: offset must be computed by block indexer",
			hex.EncodeToString(txHash.Bytes()[:8]),
			point.Slot,
		)
	}
	// Store offset reference
	offsetData := EncodeTxOffset(&txOffset)
	if err := blob.SetTx(blobTxn, txHash.Bytes(), offsetData); err != nil {
		return fmt.Errorf("set tx offset: %w", err)
	}

	// Store all produced UTxOs - tx.Produced() returns correct indices for both
	// valid transactions (regular outputs at indices 0, 1, ...) and invalid
	// transactions (collateral return at index len(Outputs()))
	// UTxO offsets MUST be available - no fallback to full CBOR storage
	produced := tx.Produced()
	if len(produced) == 0 {
		d.logger.Warn(
			"transaction has no produced outputs",
			"txHash", hex.EncodeToString(txHash.Bytes()[:8]),
			"slot", point.Slot,
		)
	}
	for _, utxo := range produced {
		txId := utxo.Id.Id().Bytes()
		outputIdx := utxo.Id.Index()

		ref := UtxoRef{
			TxId:      txHashArray,
			OutputIdx: outputIdx,
		}
		offset, ok := offsets.UtxoOffsets[ref]
		if !ok {
			return fmt.Errorf(
				"missing UTxO offset for %s#%d at slot %d: offset must be computed by block indexer",
				hex.EncodeToString(txId[:8]),
				outputIdx,
				point.Slot,
			)
		}
		// Store offset reference
		offsetData := EncodeUtxoOffset(&offset)
		if err := blob.SetUtxo(blobTxn, txId, outputIdx, offsetData); err != nil {
			return fmt.Errorf("set utxo offset %x#%d: %w", txId[:8], outputIdx, err)
		}
	}

	if err := d.metadata.SetTransaction(tx, point, idx, certDeposits, txn.Metadata()); err != nil {
		return fmt.Errorf("set transaction metadata: %w", err)
	}

	if updateEpoch > 0 && tx.IsValid() {
		for genesisHash, update := range pparamUpdates {
			if err := d.SetPParamUpdate(genesisHash.Bytes(), update.Cbor(), point.Slot, updateEpoch, txn); err != nil {
				return fmt.Errorf("set pparam update: %w", err)
			}
		}
	}

	if owned {
		if err := txn.Commit(); err != nil {
			return err
		}
	}

	return nil
}

// SetGenesisTransaction stores a genesis transaction with its UTxO outputs.
// Genesis transactions have no inputs, witnesses, or fees - just outputs.
// The offsets map contains pre-computed byte offsets into the synthetic genesis block.
func (d *Database) SetGenesisTransaction(
	txHash []byte,
	blockHash []byte,
	outputs []lcommon.Utxo,
	offsets map[UtxoRef]CborOffset,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer txn.Rollback() //nolint:errcheck
	}

	blob := txn.DB().Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}

	// Store UTxO CBOR in blob store using offset references
	var txHashArray [32]byte
	copy(txHashArray[:], txHash)

	utxoModels := make([]models.Utxo, len(outputs))
	for i, utxo := range outputs {
		txId := utxo.Id.Id().Bytes()
		outputIdx := utxo.Id.Index()

		ref := UtxoRef{
			TxId:      txHashArray,
			OutputIdx: outputIdx,
		}

		offset, ok := offsets[ref]
		if !ok {
			return fmt.Errorf("missing offset for genesis utxo %x:%d", txId[:8], outputIdx)
		}

		// Store offset reference
		offsetData := EncodeUtxoOffset(&offset)
		if err := blob.SetUtxo(blobTxn, txId, outputIdx, offsetData); err != nil {
			return fmt.Errorf("set genesis utxo offset %x#%d: %w", txId[:8], outputIdx, err)
		}

		// Build model for metadata store
		utxoModels[i] = models.UtxoLedgerToModel(utxo, 0)
	}

	// Store transaction in metadata
	if err := d.metadata.SetGenesisTransaction(txHash, blockHash, utxoModels, txn.Metadata()); err != nil {
		return fmt.Errorf(
			"SetGenesisTransaction failed for tx %x block %x: %w",
			txHash[:8],
			blockHash[:8],
			err,
		)
	}

	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
	}

	return nil
}

func (d *Database) GetTransactionByHash(
	hash []byte,
	txn *Txn,
) (*models.Transaction, error) {
	if len(hash) == 0 {
		return nil, nil
	}
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	return d.metadata.GetTransactionByHash(hash, txn.Metadata())
}

// deleteTxBlobs attempts to delete blob data for the given transaction hashes.
// This is a best-effort operation; metadata remains the source of truth. If the
// provided transaction does not include a blob handle, a temporary blob-only
// transaction is used instead.
func deleteTxBlobs(d *Database, txHashes [][]byte, txn *Txn) error {
	blob := d.Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}

	useTxn := txn
	owned := false
	if useTxn == nil || useTxn.Blob() == nil {
		useTxn = NewBlobOnlyTxn(d, true)
		owned = true
		defer func() {
			if owned {
				useTxn.Rollback() //nolint:errcheck
			}
		}()
	}

	var deleteErrors int
	for _, txHash := range txHashes {
		if err := blob.DeleteTx(useTxn.Blob(), txHash); err != nil {
			deleteErrors++
			d.logger.Debug(
				"failed to delete TX blob data",
				"txHash", hex.EncodeToString(txHash),
				"error", err,
			)
		}
	}
	if deleteErrors > 0 {
		d.logger.Debug(
			"tx blob deletion completed with errors",
			"failed",
			deleteErrors,
			"total",
			len(txHashes),
		)
	}

	if owned {
		owned = false // prevent deferred rollback
		if err := useTxn.Commit(); err != nil {
			_ = useTxn.Rollback() // explicit rollback on commit failure
			d.logger.Debug("tx blob delete commit failed", "error", err)
		}
	}

	return nil
}

// TransactionsDeleteRolledback deletes transaction offset blobs and metadata
// for transactions added after the given slot. This is used during rollback
// to clean up both blob storage and metadata for rolled-back transactions.
func (d *Database) TransactionsDeleteRolledback(
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

	// Get transaction hashes that will be deleted
	txHashes, err := d.metadata.GetTransactionHashesAfterSlot(slot, txn.Metadata())
	if err != nil {
		return fmt.Errorf(
			"failed to get transaction hashes after slot %d: %w",
			slot,
			err,
		)
	}

	// Delete blob data first (best effort)
	_ = deleteTxBlobs(d, txHashes, txn)

	// Then delete metadata (source of truth)
	err = d.metadata.DeleteTransactionsAfterSlot(slot, txn.Metadata())
	if err != nil {
		return fmt.Errorf(
			"failed to delete transactions after slot %d: %w",
			slot,
			err,
		)
	}

	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
		owned = false
	}

	return nil
}
