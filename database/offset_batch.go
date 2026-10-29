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
)

// PersistTransactionOffsets writes the transaction offset and produced-UTxO
// offset blob entries of txs from offsets, for a caller that then applies the
// same transactions with BatchedTxIngestOpts.OffsetsPersisted.
//
// A dense Leios endorser block stages thousands of entries, more than one
// Badger write transaction accepts, so the entries are committed in blob-only
// transactions of their own, each before it fills (the same budget-sized
// batching as the block inventory index rebuild). A store that reports no
// per-transaction budget has none to exhaust and receives the entries in txn's
// own blob transaction.
//
// Batches commit ahead of txn and are not undone by its rollback. That is safe
// because every entry is keyed by transaction hash, metadata is the source of
// truth for which transactions exist, and a re-apply of the same transactions
// rewrites identical values. The offsets must point at a blob already
// committed. Reads of the written keys through txn use a fresh snapshot.
//
// afterBatch, when non-nil, runs after each batch commits; an error from it
// stops the write. It is the seam for a crash between batches.
func (d *Database) PersistTransactionOffsets(
	txn *Txn,
	txs []lcommon.Transaction,
	offsets *BlockIngestionResult,
	afterBatch func() error,
) error {
	if txn == nil {
		return types.ErrNilTxn
	}
	store := txn.BlobStore()
	if store == nil {
		return types.ErrBlobStoreUnavailable
	}
	if txn.Blob() == nil {
		return types.ErrNilTxn
	}
	if offsets == nil {
		return errors.New("persist transaction offsets: offsets must be computed")
	}
	writer := newBlockInventoryIndexWriter(store, txn.Blob())
	defer writer.abort()
	var staged [][]byte
	commit := func() error {
		if err := writer.flush(); err != nil {
			return err
		}
		txn.markOffsetsCommittedSeparately(staged)
		staged = staged[:0]
		if afterBatch != nil {
			return afterBatch()
		}
		return nil
	}
	// stage writes one entry, committing the batch it would overflow first.
	// Keys are marked as soon as their batch commits, before any caller can
	// read them.
	stage := func(key []byte, write func(types.Txn) error, valueLen int) error {
		wasBatch := writer.cur
		wtxn, err := writer.txnFor(len(key) + valueLen)
		if err != nil {
			return err
		}
		if wasBatch != nil && wtxn != wasBatch {
			// txnFor committed the full batch and opened a new one.
			txn.markOffsetsCommittedSeparately(staged)
			staged = staged[:0]
			if afterBatch != nil {
				if err := afterBatch(); err != nil {
					return err
				}
			}
		}
		if err := write(wtxn); err != nil {
			return err
		}
		if writer.batched() {
			staged = append(staged, key)
		}
		return nil
	}
	for _, tx := range txs {
		txHashBytes := ledgerHashBytes(tx.Hash())
		var txHashArray [32]byte
		copy(txHashArray[:], txHashBytes)
		txOffset, ok := offsets.TxOffsets[txHashArray]
		if !ok {
			return fmt.Errorf(
				"missing TX offset for %s: offset must be computed",
				hex.EncodeToString(ledgerHashPrefix(tx.Hash())),
			)
		}
		txOffsetData := EncodeTxOffset(&txOffset)
		if err := stage(
			types.TxBlobKey(txHashBytes),
			func(w types.Txn) error {
				if err := store.SetTx(w, txHashBytes, txOffsetData); err != nil {
					return fmt.Errorf("set tx offset: %w", err)
				}
				return nil
			},
			len(txOffsetData),
		); err != nil {
			return err
		}
		for _, utxo := range tx.Produced() {
			txId := ledgerInputIDBytes(utxo.Id)
			outputIdx := utxo.Id.Index()
			var ref UtxoRef
			copy(ref.TxId[:], txId)
			ref.OutputIdx = outputIdx
			offset, ok := offsets.UtxoOffsets[ref]
			if !ok {
				return fmt.Errorf(
					"missing UTxO offset for %s#%d: offset must be computed",
					hex.EncodeToString(bytePrefix(txId)),
					outputIdx,
				)
			}
			utxoOffsetData := EncodeUtxoOffset(&offset)
			if err := stage(
				types.UtxoBlobKey(txId, outputIdx),
				func(w types.Txn) error {
					if err := store.SetUtxo(w, txId, outputIdx, utxoOffsetData); err != nil {
						return fmt.Errorf(
							"set utxo offset %x#%d: %w",
							bytePrefix(txId),
							outputIdx,
							err,
						)
					}
					return nil
				},
				len(utxoOffsetData),
			); err != nil {
				return err
			}
		}
	}
	if !writer.batched() {
		return nil
	}
	return commit()
}
