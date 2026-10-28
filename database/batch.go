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
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
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
	return d.transactionStore().NewBatchAccumulator()
}

// NewLeiosClosureBatchAccumulator creates an API-detail accumulator when the
// metadata provider supports ordered closure application; nil selects the
// existing immediate path.
func (d *Database) NewLeiosClosureBatchAccumulator() BatchAccumulator {
	store, ok := d.transactionStore().(metadata.LeiosClosureBatcher)
	if !ok {
		return nil
	}
	return store.NewLeiosClosureBatchAccumulator()
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
	return d.transactionStore().FlushBatch(acc, metadataTxn)
}

// EnableBatchUtxoOverlay attaches a batch UTxO overlay to acc for the rest
// of txn when the metadata plugin supports one (see
// metadata.BatchUtxoOverlay). It reports false, and changes nothing, when it
// does not.
func (d *Database) EnableBatchUtxoOverlay(
	acc BatchAccumulator,
	txn *Txn,
	inputs []models.UtxoId,
	deleteOnSpend []models.UtxoId,
) (bool, error) {
	if acc == nil || txn == nil {
		return false, nil
	}
	overlay, ok := d.transactionStore().(metadata.BatchUtxoOverlay)
	if !ok {
		return false, nil
	}
	metadataTxn := txn.Metadata()
	if metadataTxn == nil {
		return false, types.ErrNilTxn
	}
	return overlay.EnableBatchUtxoOverlay(
		acc, metadataTxn, inputs, deleteOnSpend,
	)
}

// WithBatchUtxoOverlayError joins err with the error acc's batch UTxO overlay
// recorded. A read barrier inside QueryRowContext cannot return its error, so
// the statement and those after it run against tables the barrier failed to
// write, and the failure a caller then reports does not name the overlay. A
// caller that answers an overlay conflict with a per-row retry passes the
// errors of everything it ran under the overlay through this. err is
// returned unchanged when it is nil or acc records nothing.
func WithBatchUtxoOverlayError(acc BatchAccumulator, err error) error {
	recorder, ok := acc.(interface{ BatchUtxoOverlayError() error })
	if err == nil || !ok {
		return err
	}
	overlayErr := recorder.BatchUtxoOverlayError()
	if overlayErr == nil || errors.Is(err, overlayErr) {
		return err
	}
	return errors.Join(overlayErr, err)
}

// BatchedTxIngestOpts toggles optional behaviors of SetTransactionBatched.
//
// Defaults preserve the original full-write behavior; setters opt callers
// (currently API-mode Mithril backfill) into write-elision when the offsets
// are known to already be present.
type BatchedTxIngestOpts struct {
	// ProtocolMajor is the protocol version in force for certificate
	// transitions. Zero means the caller has no version context.
	ProtocolMajor uint64

	// LedgerContextSlot is the unticked parent slot for a prototype closure.
	// Point still identifies its certifying block for rollback.
	LedgerContextSlot *uint64

	// SkipProducedUtxoOffsetWrites elides blob.SetUtxo calls for produced
	// outputs. Use when the produced-UTxO offset references for this block
	// have already been written (e.g. by the Mithril immutable-copy phase
	// reflected in the immutable_utxo_offsets_tip sync-state key). Offsets
	// must still be computed and present in the BlockIngestionResult — the
	// guarantee is verified, only the redundant blob write is dropped.
	// TX offset writes, metadata writes, and consumed-input handling are
	// unaffected.
	SkipProducedUtxoOffsetWrites bool

	// OffsetsPersisted elides both the transaction offset and the
	// produced-UTxO offset blob writes. Use when the caller has written
	// this transaction's offsets itself, in bounded blob transactions of
	// its own (Database.PersistTransactionOffsets), because staging them in
	// the enclosing transaction could exceed the blob store's
	// per-transaction budget. Offsets must still be present in the
	// BlockIngestionResult.
	OffsetsPersisted bool

	// SkipConsumedInputRecovery elides the per-input GetUtxoIncludingSpent
	// recovery checks in ensureTransactionConsumedUtxos. Use when replaying
	// immutable blocks in slot order during Mithril historical backfill,
	// where consumed inputs are guaranteed to already exist in the metadata
	// store from earlier producer transactions. The in-flight producer lookup
	// optimization (same-batch provenance) remains active. Do NOT enable for
	// gap blocks, resumed backfill with potential missing rows, or normal
	// replay paths where producer rows may be absent.
	SkipConsumedInputRecovery bool

	// TrustedImmutableReplay elides consumed-input blob recovery while
	// preserving normal transaction conflict validation. Use only when replaying
	// a complete, trusted immutable history in slot order, so each consumed
	// output was written by an earlier block in the same replay.
	TrustedImmutableReplay bool

	// StrictAppliedInputConservation marks the steady-state, at-tip, validated
	// path. Past the Mithril trust boundary, a missing producer row is recovered
	// only when the producer block is still on the applied primary chain. This
	// allows rollback recovery after core-mode cleanup removed a spent row,
	// while refusing recovery from a retained abandoned-fork block.
	// The option is retained for callers that identify this path;
	// the primary-chain check also protects validated catch-up paths. It takes
	// effect only when StrictUtxoValidation is enabled on the Database.
	StrictAppliedInputConservation bool

	// Stats receives hot-path timings and row-ish counts for operator
	// visibility during API-mode Mithril backfill. It is optional and is
	// intentionally updated only at coarse stage boundaries.
	Stats *types.BackfillHotPathStats

	// SkipWithdrawalWitnessWrite elides the CIP-0163 account_withdrawal_witness
	// insert for each reward withdrawal. That table is only ever read by the
	// delegator-inactivity gate's rollback/renewal paths
	// (MetadataStore.AccountsWitnessedAfterSlot, AccountLastWitnessSlots); with
	// the gate off -- the default on every node not running CIP-0163 -- the
	// insert is pure write amplification on a table nothing reads (see
	// DelegatorInactivityEnabled). The ledger sets this to
	// !DelegatorInactivityEnabled on the live- apply path (ledger/delta.go),
	// and internal/node.Backfill derives it the same way from its own
	// delegatorInactivityEnabled field for the batched historical-replay path
	// -- see that field's doc comment for why the gate can genuinely be on
	// there too and why the value must always be set explicitly rather than
	// assumed. Defaults to false here, preserving the unconditional write for
	// any caller that does not opt in.
	SkipWithdrawalWitnessWrite bool

	// HistoricalBackfill records already-ledger-validated historical
	// withdrawals without replaying account state over the imported snapshot.
	// Live ledger ingestion leaves this false and enforces balance sufficiency.
	HistoricalBackfill bool
}

type batchStatsSetter interface {
	SetBackfillStats(*types.BackfillHotPathStats)
}

type transactionStoreHistoricalBackfill interface {
	SetTransactionBatchedHistorical(
		lcommon.Transaction,
		ocommon.Point,
		uint32,
		map[int]uint64,
		bool,
		bool,
		BatchAccumulator,
		types.Txn,
		uint64,
	) error
}

// inFlightProducerLookup is implemented by accumulators that index the
// outputs produced earlier in the current batch but not yet flushed. When
// available, consumed-input recovery can skip blob/metadata recovery for a
// producer that will be created (and spent) by the pending FlushBatch.
type inFlightProducerLookup interface {
	HasInFlightProducer(txId []byte, outputIdx uint32) bool
}

type transactionStoreStakeDeltaFlusher interface {
	FlushBatchStakeDeltas(types.MetadataBatchAccumulator, types.Txn) error
}

// FlushBatchStakeDeltas applies pending live-stake changes while leaving the
// accumulator's other batched rows queued for FlushBatch.
func (d *Database) FlushBatchStakeDeltas(
	acc BatchAccumulator,
	txn *Txn,
) error {
	if acc == nil {
		return nil
	}
	flusher, ok := d.transactionStore().(transactionStoreStakeDeltaFlusher)
	if !ok {
		return nil
	}
	var metadataTxn types.Txn
	if txn != nil {
		metadataTxn = txn.Metadata()
		if metadataTxn == nil {
			return types.ErrNilTxn
		}
	}
	return flusher.FlushBatchStakeDeltas(acc, metadataTxn)
}

// SetTransactionBatched stores transaction blob offsets and immediate
// metadata, while accumulating bulk metadata rows into acc for a later
// FlushBatch.
func (d *Database) SetTransactionBatched(
	ctx context.Context,
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	updateEpoch uint64,
	pparamUpdates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
	certDeposits map[int]uint64,
	offsets *BlockIngestionResult,
	acc BatchAccumulator,
	txn *Txn,
	protocolMajor uint64,
) (retErr error) {
	return d.SetTransactionBatchedWithOpts(
		ctx,
		tx, point, idx, updateEpoch, pparamUpdates,
		certDeposits, offsets, acc, txn,
		BatchedTxIngestOpts{ProtocolMajor: protocolMajor},
	)
}

// SetTransactionBatchedWithOpts is the option-aware form of
// SetTransactionBatched. See BatchedTxIngestOpts for the available toggles.
func (d *Database) SetTransactionBatchedWithOpts(
	ctx context.Context,
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
	closureAcc, markedClosure := acc.(metadata.LeiosClosureAccumulator)
	closureBatch := markedClosure && closureAcc.LeiosClosureRowsOnly()
	if opts.LedgerContextSlot != nil && !closureBatch {
		return errors.New("prototype closure context requires unbatched ingestion")
	}
	if closureBatch && !opts.SkipConsumedInputRecovery {
		return errors.New("closure accumulator requires closure input semantics")
	}
	if acc == nil {
		return errors.New("batch accumulator must not be nil")
	}
	owned := false
	if txn == nil {
		txn = d.Transaction(ctx, true)
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

	blob := txn.BlobStore()
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
	if !opts.OffsetsPersisted {
		if err := blob.SetTx(blobTxn, txHashBytes, offsetData); err != nil {
			return fmt.Errorf("set tx offset: %w", err)
		}
		if opts.Stats != nil {
			// Count tx offset writes separately from produced UTxO offset writes.
			opts.Stats.BlobTxOffsetWrites++
		}
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
		if opts.SkipProducedUtxoOffsetWrites || opts.OffsetsPersisted {
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
	var metadataErr error
	if closureBatch {
		store, ok := d.transactionStore().(metadata.LeiosClosureBatcher)
		if !ok {
			return errors.New("metadata provider does not support closure batching")
		}
		metadataErr = store.SetTransactionLeiosClosureBatched(
			tx, point, idx, certDeposits, opts.SkipWithdrawalWitnessWrite,
			opts.LedgerContextSlot, acc, metadataTxn, opts.ProtocolMajor,
		)
	} else if store, ok := d.transactionStore().(transactionStoreHistoricalBackfill); ok {
		metadataErr = store.SetTransactionBatchedHistorical(
			tx, point, idx, certDeposits,
			opts.SkipWithdrawalWitnessWrite,
			opts.HistoricalBackfill,
			acc, metadataTxn, opts.ProtocolMajor,
		)
	} else {
		metadataErr = d.transactionStore().SetTransactionBatched(
			tx, point, idx, certDeposits,
			opts.SkipWithdrawalWitnessWrite, acc, metadataTxn,
			opts.ProtocolMajor)
	}
	if err := metadataErr; err != nil {
		return fmt.Errorf(
			"set transaction metadata for tx %s (batch idx %d, slot %d): %w",
			tx.Hash(), idx, point.Slot, err,
		)
	}

	if len(pparamUpdates) > 0 && tx.IsValid() {
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
