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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// BatchAccumulator is an opaque accumulator owned by the active metadata
// plugin.
type BatchAccumulator = types.MetadataBatchAccumulator

// NewBatchAccumulator creates an accumulator for the configured metadata
// plugin.
func (d *Database) NewBatchAccumulator() BatchAccumulator {
	return d.metadata.NewBatchAccumulator()
}

// FlushBatch writes accumulated metadata rows for the active metadata plugin.
func (d *Database) FlushBatch(
	acc BatchAccumulator,
	txn *Txn,
) error {
	if acc == nil {
		return nil
	}
	var metadataTxn types.Txn
	if txn != nil {
		metadataTxn = txn.Metadata()
		if metadataTxn == nil {
			return types.ErrNilTxn
		}
	}
	return d.metadata.FlushBatch(acc, metadataTxn)
}

// SetTransactionBatched stores transaction blob offsets and immediate
// metadata, while accumulating bulk metadata rows into acc for a later
// FlushBatch.
func (d *Database) SetTransactionBatched(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	updateEpoch uint64,
	pparamUpdates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
	certDeposits map[int]uint64,
	offsets *BlockIngestionResult,
	acc BatchAccumulator,
	txn *Txn,
) (retErr error) {
	if acc == nil {
		return errors.New("batch accumulator must not be nil")
	}
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer func() {
			if txn == nil {
				return
			}
			if retErr != nil {
				acc.Reset()
			}
			_ = txn.Rollback()
		}()
	}

	blob := txn.DB().Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}

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
	offsetData := EncodeTxOffset(&txOffset)
	if err := blob.SetTx(blobTxn, txHash.Bytes(), offsetData); err != nil {
		return fmt.Errorf("set tx offset: %w", err)
	}

	for _, utxo := range tx.Produced() {
		txID := utxo.Id.Id().Bytes()
		outputIdx := utxo.Id.Index()
		ref := UtxoRef{
			TxId:      txHashArray,
			OutputIdx: outputIdx,
		}
		offset, ok := offsets.UtxoOffsets[ref]
		if !ok {
			return fmt.Errorf(
				"missing UTxO offset for %s#%d at slot %d: offset must be computed by block indexer",
				hex.EncodeToString(txID[:8]),
				outputIdx,
				point.Slot,
			)
		}
		offsetData := EncodeUtxoOffset(&offset)
		if err := blob.SetUtxo(blobTxn, txID, outputIdx, offsetData); err != nil {
			return fmt.Errorf(
				"set utxo offset %x#%d: %w",
				txID[:8],
				outputIdx,
				err,
			)
		}
	}

	if err := d.ensureTransactionConsumedUtxos(tx, point, txn); err != nil {
		return err
	}
	metadataTxn := txn.Metadata()
	if metadataTxn == nil {
		return types.ErrNilTxn
	}
	if err := d.metadata.SetTransactionBatched(
		tx, point, idx, certDeposits, acc, metadataTxn,
	); err != nil {
		return fmt.Errorf("set transaction metadata: %w", err)
	}

	if updateEpoch > 0 && tx.IsValid() {
		for genesisHash, update := range pparamUpdates {
			if err := d.SetPParamUpdate(
				genesisHash.Bytes(),
				update.Cbor(),
				point.Slot,
				updateEpoch,
				txn,
			); err != nil {
				return fmt.Errorf("set pparam update: %w", err)
			}
		}
	}

	if owned {
		if err := d.FlushBatch(acc, txn); err != nil {
			return err
		}
		if err := txn.Commit(); err != nil {
			return err
		}
		txn = nil
		acc.Reset()
	}

	return nil
}
