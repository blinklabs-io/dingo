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
	"time"

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

// BatchedTxIngestOpts toggles optional behaviors of SetTransactionBatched.
//
// Defaults preserve the original full-write behavior; setters opt callers
// (currently API-mode Mithril backfill) into write-elision when the offsets
// are known to already be present.
type BatchedTxIngestOpts struct {
	// SkipProducedUtxoOffsetWrites elides blob.SetUtxo calls for produced
	// outputs. Use when the produced-UTxO offset references for this block
	// have already been written (e.g. by the Mithril immutable-copy phase
	// reflected in the immutable_utxo_offsets_tip sync-state key). Offsets
	// must still be computed and present in the BlockIngestionResult — the
	// guarantee is verified, only the redundant blob write is dropped.
	// TX offset writes, metadata writes, and consumed-input handling are
	// unaffected.
	SkipProducedUtxoOffsetWrites bool

	// SkipConsumedInputRecovery elides the per-input GetUtxoIncludingSpent
	// recovery checks in ensureTransactionConsumedUtxos. Use when replaying
	// immutable blocks in slot order during Mithril historical backfill,
	// where consumed inputs are guaranteed to already exist in the metadata
	// store from earlier producer transactions. The in-flight producer lookup
	// optimization (same-batch provenance) remains active. Do NOT enable for
	// gap blocks, resumed backfill with potential missing rows, or normal
	// replay paths where producer rows may be absent.
	SkipConsumedInputRecovery bool

	// Stats receives hot-path timings and row-ish counts for operator
	// visibility during API-mode Mithril backfill. It is optional and is
	// intentionally updated only at coarse stage boundaries.
	Stats *types.BackfillHotPathStats
}

type batchStatsSetter interface {
	SetBackfillStats(*types.BackfillHotPathStats)
}

// inFlightProducerLookup is implemented by accumulators that index the
// outputs produced earlier in the current batch but not yet flushed. When
// available, consumed-input recovery can skip blob/metadata recovery for a
// producer that will be created (and spent) by the pending FlushBatch.
type inFlightProducerLookup interface {
	HasInFlightProducer(txId []byte, outputIdx uint32) bool
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
	return d.SetTransactionBatchedWithOpts(
		tx, point, idx, updateEpoch, pparamUpdates,
		certDeposits, offsets, acc, txn,
		BatchedTxIngestOpts{},
	)
}

// SetTransactionBatchedWithOpts is the option-aware form of
// SetTransactionBatched. See BatchedTxIngestOpts for the available toggles.
func (d *Database) SetTransactionBatchedWithOpts(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	updateEpoch uint64,
	pparamUpdates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
	certDeposits map[int]uint64,
	offsets *BlockIngestionResult,
	acc BatchAccumulator,
	txn *Txn,
	opts BatchedTxIngestOpts,
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
	txHashBytes := ledgerHashBytes(txHash)
	var txHashArray [32]byte
	copy(txHashArray[:], txHashBytes)

	if offsets == nil {
		return fmt.Errorf(
			"missing offsets for transaction %s at slot %d: offsets must be computed",
			hex.EncodeToString(ledgerHashPrefix(txHash)),
			point.Slot,
		)
	}
	txOffset, ok := offsets.TxOffsets[txHashArray]
	if !ok {
		return fmt.Errorf(
			"missing TX offset for %s at slot %d: offset must be computed by block indexer",
			hex.EncodeToString(ledgerHashPrefix(txHash)),
			point.Slot,
		)
	}

	produced := tx.Produced()
	if opts.Stats != nil {
		// Record interval density so timings can be compared across eras.
		opts.Stats.Txs++
		opts.Stats.Utxos += uint64(len(produced))
		opts.Stats.InputRefs += uint64(len(tx.Inputs()) +
			len(tx.Collateral()) + len(tx.ReferenceInputs()))
	}

	blobStart := time.Now()
	offsetData := EncodeTxOffset(&txOffset)
	if err := blob.SetTx(blobTxn, txHashBytes, offsetData); err != nil {
		return fmt.Errorf("set tx offset: %w", err)
	}
	if opts.Stats != nil {
		// Count tx offset writes separately from produced UTxO offset writes.
		opts.Stats.BlobTxOffsetWrites++
	}

	for _, utxo := range produced {
		txID := ledgerInputIDBytes(utxo.Id)
		outputIdx := utxo.Id.Index()
		ref := UtxoRef{
			TxId:      txHashArray,
			OutputIdx: outputIdx,
		}
		offset, ok := offsets.UtxoOffsets[ref]
		if !ok {
			return fmt.Errorf(
				"missing UTxO offset for %s#%d at slot %d: offset must be computed by block indexer",
				hex.EncodeToString(bytePrefix(txID)),
				outputIdx,
				point.Slot,
			)
		}
		if opts.SkipProducedUtxoOffsetWrites {
			// Offset is required to be computed (validated above) so a
			// regression in the indexer is still caught, but the blob
			// write itself is elided: the immutable-copy phase already
			// persisted this exact reference.
			continue
		}
		offsetData := EncodeUtxoOffset(&offset)
		if err := blob.SetUtxo(blobTxn, txID, outputIdx, offsetData); err != nil {
			return fmt.Errorf(
				"set utxo offset %x#%d: %w",
				bytePrefix(txID),
				outputIdx,
				err,
			)
		}
		if opts.Stats != nil {
			// Count produced UTxO offset writes that were not elided.
			opts.Stats.BlobUtxoOffsetWrites++
		}
	}
	if opts.Stats != nil {
		if opts.SkipProducedUtxoOffsetWrites {
			// Count saved blob writes from the Mithril immutable-copy phase.
			opts.Stats.SkippedUtxoOffsets += uint64(len(produced))
		}
		// Time spent writing offset refs to the blob plugin.
		opts.Stats.BlobOffsetWrites += time.Since(blobStart)
	}

	recoverStart := time.Now()
	if err := d.ensureTransactionConsumedUtxos(tx, point, txn, acc, opts); err != nil {
		return err
	}
	if opts.Stats != nil {
		// Time consumed-input lookup/recovery before metadata batching.
		opts.Stats.ConsumedInputRecovery += time.Since(recoverStart)
	}
	metadataTxn := txn.Metadata()
	if metadataTxn == nil {
		return types.ErrNilTxn
	}
	if setter, ok := acc.(batchStatsSetter); ok {
		setter.SetBackfillStats(opts.Stats)
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
		if opts.Stats != nil {
			// Count pparam updates written from valid update transactions.
			opts.Stats.PParamUpdates += uint64(len(pparamUpdates))
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
