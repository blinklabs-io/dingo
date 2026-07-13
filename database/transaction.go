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
	"math/big"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// mithrilLedgerSlotSyncKey mirrors the sync-state key written by the ledger
// and mithril packages when a Mithril snapshot is imported: blocks at or
// below this slot are Mithril-verified, so the node is not expected to have
// complete pre-boundary UTxO history. Duplicated here (rather than imported)
// because the database package cannot depend on ledger, which depends on it.
const mithrilLedgerSlotSyncKey = "mithril_ledger_slot"

type metadataOnlyTransaction struct {
	lcommon.Transaction
}

func (tx metadataOnlyTransaction) Inputs() []lcommon.TransactionInput {
	return nil
}

func (tx metadataOnlyTransaction) Outputs() []lcommon.TransactionOutput {
	return nil
}

func (tx metadataOnlyTransaction) ReferenceInputs() []lcommon.TransactionInput {
	return nil
}

func (tx metadataOnlyTransaction) Collateral() []lcommon.TransactionInput {
	return nil
}

func (tx metadataOnlyTransaction) CollateralReturn() lcommon.TransactionOutput {
	return nil
}

func (tx metadataOnlyTransaction) Withdrawals() map[*lcommon.Address]*big.Int {
	return nil
}

func (tx metadataOnlyTransaction) Consumed() []lcommon.TransactionInput {
	return nil
}

func (tx metadataOnlyTransaction) Produced() []lcommon.Utxo {
	return nil
}

// mithrilTrustBoundarySlot returns the recorded Mithril trust boundary slot,
// or 0 if none is recorded (genesis sync, or a non-genesis chainsync
// intersect point with no snapshot import). A failure to read the sync
// state is logged and also treated as 0 (the caller cannot distinguish it
// from "no boundary recorded" by return value alone), but the log lets an
// operator tell a transient storage problem apart from a genuinely
// unrecoverable UTxO when StrictUtxoValidation turns the latter into an
// ingest error.
func (d *Database) mithrilTrustBoundarySlot(txn *Txn) uint64 {
	val, err := d.GetSyncState(mithrilLedgerSlotSyncKey, txn)
	if err != nil {
		d.logger.Warn(
			"failed to read Mithril trust boundary from sync state; "+
				"treating consumed-utxo recovery failures as past the boundary",
			"error", err,
		)
		return 0
	}
	if val == "" {
		return 0
	}
	slot, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		d.logger.Warn(
			"malformed mithril_ledger_slot sync state value, ignoring",
			"value", val,
			"error", err,
		)
		return 0
	}
	return slot
}

func ledgerHashBytes(hash lcommon.Blake2b256) []byte {
	return hash[:]
}

func ledgerHashPrefix(hash lcommon.Blake2b256) []byte {
	return hash[:8]
}

func ledgerInputIDBytes(input lcommon.TransactionInput) []byte {
	id := input.Id()
	return id[:]
}

func bytePrefix(data []byte) []byte {
	const count = 8
	if len(data) < count {
		return data
	}
	return data[:count]
}

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
	// Store offset reference
	offsetData := EncodeTxOffset(&txOffset)
	if err := blob.SetTx(blobTxn, txHashBytes, offsetData); err != nil {
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
			"txHash", hex.EncodeToString(ledgerHashPrefix(txHash)),
			"slot", point.Slot,
		)
	}
	for _, utxo := range produced {
		txId := ledgerInputIDBytes(utxo.Id)
		outputIdx := utxo.Id.Index()

		ref := UtxoRef{
			TxId:      txHashArray,
			OutputIdx: outputIdx,
		}
		offset, ok := offsets.UtxoOffsets[ref]
		if !ok {
			return fmt.Errorf(
				"missing UTxO offset for %s#%d at slot %d: offset must be computed by block indexer",
				hex.EncodeToString(bytePrefix(txId)),
				outputIdx,
				point.Slot,
			)
		}
		// Store offset reference
		offsetData := EncodeUtxoOffset(&offset)
		if err := blob.SetUtxo(blobTxn, txId, outputIdx, offsetData); err != nil {
			return fmt.Errorf(
				"set utxo offset %x#%d: %w",
				bytePrefix(txId),
				outputIdx,
				err,
			)
		}
	}

	if err := d.ensureTransactionConsumedUtxos(tx, point, txn, nil, BatchedTxIngestOpts{}); err != nil {
		return err
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

// SetTransactionMetadataOnly records transaction metadata, certificates, and
// other non-UTxO metadata without writing blob offsets, produced outputs, spent
// inputs, collateral, reference inputs, reward withdrawals, or pparam updates.
//
// This is used for Leios/Musashi endorser-block transactions whose certificate
// and governance data must be visible to the metadata-backed ledger queries
// even though their UTxO effects are not applied by Dingo's Musashi path.
func (d *Database) SetTransactionMetadataOnly(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	certDeposits map[int]uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer txn.Rollback() //nolint:errcheck
	}
	metadataTxn := txn.Metadata()
	if metadataTxn == nil {
		return types.ErrNilTxn
	}
	if err := d.metadata.SetTransaction(
		metadataOnlyTransaction{Transaction: tx},
		point,
		idx,
		certDeposits,
		metadataTxn,
	); err != nil {
		return fmt.Errorf("set transaction metadata only: %w", err)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// SetGapBlockTransaction stores a transaction from a mithril gap block.
// It records blob offsets (TX and UTxO) for CBOR resolution and creates
// a minimal metadata record, but does NOT look up or consume input
// UTxOs because the mithril snapshot already reflects the correct
// spent/unspent state.
func (d *Database) SetGapBlockTransaction(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
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

	txHash := tx.Hash()
	txHashBytes := ledgerHashBytes(txHash)
	var txHashArray [32]byte
	copy(txHashArray[:], txHashBytes)

	if offsets == nil {
		return fmt.Errorf(
			"missing offsets for gap block transaction %s at slot %d",
			hex.EncodeToString(ledgerHashPrefix(txHash)),
			point.Slot,
		)
	}
	txOffset, ok := offsets.TxOffsets[txHashArray]
	if !ok {
		return fmt.Errorf(
			"missing TX offset for gap block %s at slot %d",
			hex.EncodeToString(ledgerHashPrefix(txHash)),
			point.Slot,
		)
	}
	offsetData := EncodeTxOffset(&txOffset)
	if err := blob.SetTx(blobTxn, txHashBytes, offsetData); err != nil {
		return fmt.Errorf("set gap block tx offset: %w", err)
	}

	// Store UTxO offsets for produced outputs
	for _, utxo := range tx.Produced() {
		txId := ledgerInputIDBytes(utxo.Id)
		outputIdx := utxo.Id.Index()
		ref := UtxoRef{
			TxId:      txHashArray,
			OutputIdx: outputIdx,
		}
		offset, ok := offsets.UtxoOffsets[ref]
		if !ok {
			return fmt.Errorf(
				"missing UTxO offset for gap block %s#%d at slot %d",
				hex.EncodeToString(bytePrefix(txId)),
				outputIdx,
				point.Slot,
			)
		}
		offsetData := EncodeUtxoOffset(&offset)
		if err := blob.SetUtxo(blobTxn, txId, outputIdx, offsetData); err != nil {
			return fmt.Errorf(
				"set gap block utxo offset %x#%d: %w",
				bytePrefix(txId), outputIdx, err,
			)
		}
	}

	if err := d.metadata.SetGapBlockTransaction(
		tx, point, idx, txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"set gap block transaction metadata: %w", err,
		)
	}
	if err := d.ensureGapConsumedUtxos(
		tx,
		point,
		txn,
	); err != nil {
		return err
	}

	if owned {
		if err := txn.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func (d *Database) ensureTransactionConsumedUtxos(
	tx lcommon.Transaction,
	point ocommon.Point,
	txn *Txn,
	acc BatchAccumulator,
	opts BatchedTxIngestOpts,
) error {
	consumed := tx.Consumed()
	if len(consumed) == 0 {
		return nil
	}

	// During Mithril historical backfill, immutable blocks are replayed in
	// slot order against a metadata store being populated from the same
	// history. Consumed inputs are guaranteed to already exist in the store
	// from earlier producer transactions, so the per-input recovery checks
	// are redundant. The in-flight producer lookup optimization (same-batch
	// provenance) remains valuable and is preserved below.
	if opts.SkipConsumedInputRecovery {
		if opts.Stats != nil {
			// Count inputs that would have triggered GetUtxoIncludingSpent
			// lookups. The in-flight check below is cheap and would have
			// avoided recovery anyway, so count the full consumed set.
			opts.Stats.SkippedInputRecovery += uint64(len(consumed))
		}
		// Return early: skip GetUtxoIncludingSpent, SetUtxoDeletedAtSlot,
		// recoverConsumedUtxo, and ImportUtxos. The in-flight producer
		// optimization is moot when the recovery path is disabled.
		return nil
	}

	inFlight, _ := acc.(inFlightProducerLookup)
	spenderTxHash := ledgerHashBytes(tx.Hash())
	recoveredUtxos := make([]models.Utxo, 0, len(consumed))
	seen := make(map[string]struct{}, len(consumed))
	for _, input := range consumed {
		inputTxId := ledgerInputIDBytes(input)
		inputKey := fmt.Sprintf("%x:%d", inputTxId, input.Index())
		if _, ok := seen[inputKey]; ok {
			continue
		}
		seen[inputKey] = struct{}{}
		existingUtxo, err := d.metadata.GetUtxoIncludingSpent(
			inputTxId,
			input.Index(),
			txn.Metadata(),
		)
		if err != nil {
			return fmt.Errorf(
				"check transaction input utxo %s: %w",
				input.String(),
				err,
			)
		}
		if existingUtxo != nil {
			// Backfill the spender link on a same-slot row that was
			// marked deleted by an earlier code path (e.g., a previous
			// partial run) without recording the consumer tx hash.
			// Without this, metadata.SetTransaction's batch consume
			// would fail with ErrUtxoConflict and rollback cleanup
			// could not restore the row.
			if existingUtxo.SpentAtTxId == nil &&
				existingUtxo.DeletedSlot == point.Slot {
				if err := d.metadata.SetUtxoDeletedAtSlot(
					input,
					point.Slot,
					spenderTxHash,
					txn.Metadata(),
				); err != nil &&
					!errors.Is(err, types.ErrUtxoNotFound) &&
					!errors.Is(err, types.ErrUtxoConflict) {
					return fmt.Errorf(
						"backfill spender for input utxo %s at slot %d: %w",
						input.String(),
						point.Slot,
						err,
					)
				}
			}
			continue
		}
		// The row is absent from the store. If it was produced earlier in
		// this same batch it has not been flushed yet: FlushBatch creates the
		// producer row before applying spends, and SetTransactionBatched
		// records the spend independently, so skip the expensive blob
		// recovery rather than reconstructing a row the flush will write.
		// This check is deliberately after the existing-row repair above so a
		// partially-written row from a resumed backfill (DeletedSlot ==
		// point.Slot, SpentAtTxId == nil) still gets its spender link
		// backfilled — batchSpendUtxos only updates rows where deleted_slot
		// = 0 and would not fix it later.
		if inFlight != nil &&
			inFlight.HasInFlightProducer(inputTxId, input.Index()) {
			continue
		}
		recoveredUtxo, err := d.recoverConsumedUtxo(
			input,
			txn,
		)
		if err != nil {
			// Past the Mithril trust boundary the node should have complete
			// producer history for every input it is asked to spend, so an
			// unrecoverable UTxO there indicates real corruption or a bug
			// rather than an expected gap. Below the boundary (or when none
			// is recorded and we did not sync from genesis) the UTxO may
			// legitimately predate the data we imported.
			if d.config.StrictUtxoValidation &&
				point.Slot > d.mithrilTrustBoundarySlot(txn) {
				return fmt.Errorf(
					"consumed utxo %s not found at slot %d and could not be recovered: %w",
					input.String(),
					point.Slot,
					err,
				)
			}
			d.logger.Debug(
				"skipping unrecoverable transaction input utxo repair",
				"input",
				input.String(),
				"error",
				err,
			)
			continue
		}
		recoveredUtxos = append(recoveredUtxos, *recoveredUtxo)
	}
	if len(recoveredUtxos) == 0 {
		return nil
	}
	if err := d.metadata.ImportUtxos(
		recoveredUtxos,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"import recovered transaction input utxos: %w",
			err,
		)
	}
	return nil
}

func (d *Database) ensureGapConsumedUtxos(
	tx lcommon.Transaction,
	point ocommon.Point,
	txn *Txn,
) error {
	consumed := tx.Consumed()
	if len(consumed) == 0 {
		return nil
	}
	spenderTxHash := ledgerHashBytes(tx.Hash())
	recoveredUtxos := make([]models.Utxo, 0, len(consumed))
	seen := make(map[string]struct{}, len(consumed))
	for _, input := range consumed {
		inputTxId := ledgerInputIDBytes(input)
		inputKey := fmt.Sprintf("%x:%d", inputTxId, input.Index())
		if _, ok := seen[inputKey]; ok {
			continue
		}
		seen[inputKey] = struct{}{}
		existingUtxo, err := d.metadata.GetUtxoIncludingSpent(
			inputTxId,
			input.Index(),
			txn.Metadata(),
		)
		if err != nil {
			return fmt.Errorf(
				"check gap input utxo %s at slot %d: %w",
				input.String(),
				point.Slot,
				err,
			)
		}
		if existingUtxo != nil {
			// Already spent by this same transaction: idempotent
			// re-processing of the same gap block is a no-op.
			if existingUtxo.SpentAtTxId != nil &&
				bytes.Equal(existingUtxo.SpentAtTxId, spenderTxHash) {
				continue
			}
			// Live rows from an earlier gap block or snapshot need to
			// be consumed now. Same-slot deleted rows with a missing
			// SpentAtTxId need their consumer link backfilled.
			if existingUtxo.SpentAtTxId == nil &&
				(existingUtxo.DeletedSlot == 0 ||
					existingUtxo.DeletedSlot == point.Slot) {
				if err := d.metadata.SetUtxoDeletedAtSlot(
					input,
					point.Slot,
					spenderTxHash,
					txn.Metadata(),
				); err != nil {
					// ErrUtxoConflict can occur if another path raced
					// the row into a different state between our read
					// and the update; treat it like NotFound so the
					// recover-from-blob path runs (which is a no-op for
					// any row that actually still exists thanks to
					// ImportUtxos' ON CONFLICT DO NOTHING).
					switch {
					case errors.Is(err, types.ErrUtxoNotFound),
						errors.Is(err, types.ErrUtxoConflict):
						existingUtxo = nil
					default:
						return fmt.Errorf(
							"mark gap input utxo %s spent at slot %d: %w",
							input.String(),
							point.Slot,
							err,
						)
					}
				}
				if existingUtxo != nil {
					continue
				}
			} else {
				// Already spent by a different tx (e.g. the Mithril
				// snapshot import already recorded this spend): leave the
				// existing row alone.
				continue
			}
		}
		recoveredUtxo, err := d.recoverConsumedUtxo(input, txn)
		if err != nil {
			return fmt.Errorf(
				"recover gap input utxo %s at slot %d: %w",
				input.String(),
				point.Slot,
				err,
			)
		}
		recoveredUtxo.DeletedSlot = point.Slot
		recoveredUtxo.SpentAtTxId = append(
			[]byte(nil),
			spenderTxHash...,
		)
		recoveredUtxos = append(recoveredUtxos, *recoveredUtxo)
	}
	if len(recoveredUtxos) == 0 {
		return nil
	}
	if err := d.metadata.ImportUtxos(
		recoveredUtxos,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"import recovered gap input utxos at slot %d: %w",
			point.Slot,
			err,
		)
	}
	return nil
}

func (d *Database) recoverConsumedUtxo(
	input lcommon.TransactionInput,
	txn *Txn,
) (*models.Utxo, error) {
	blob := txn.DB().Blob()
	if blob == nil {
		return nil, types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return nil, types.ErrNilTxn
	}
	utxoData, err := blob.GetUtxo(
		blobTxn,
		ledgerInputIDBytes(input),
		input.Index(),
	)
	if err != nil && !errors.Is(err, types.ErrBlobKeyNotFound) {
		return nil, fmt.Errorf("lookup blob data: %w", err)
	}
	addedSlot := uint64(0)
	outputCbor := utxoData
	switch {
	case err == nil && IsUtxoOffsetStorage(utxoData):
		offset, err := DecodeUtxoOffset(utxoData)
		if err != nil {
			return nil, fmt.Errorf("decode utxo offset: %w", err)
		}
		blockCbor, _, err := blob.GetBlock(
			blobTxn,
			offset.BlockSlot,
			offset.BlockHash[:],
		)
		if err != nil {
			return nil, fmt.Errorf("load producer block: %w", err)
		}
		end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
		if end > uint64(len(blockCbor)) {
			return nil, fmt.Errorf(
				"utxo offset out of bounds: offset=%d, length=%d, block_size=%d",
				offset.ByteOffset,
				offset.ByteLength,
				len(blockCbor),
			)
		}
		outputCbor = blockCbor[offset.ByteOffset:end]
		addedSlot = offset.BlockSlot
	case err == nil:
		// Legacy format: raw output CBOR is already present in utxoData.
		// Resolve the producer slot so addedSlot reflects when the UTxO
		// was actually created; otherwise recovered legacy rows would look
		// like genesis entries (added_slot = 0) and be invisible to
		// slot-bounded queries and rollback cleanup. We only need the slot
		// here, so the metadata-driven slot lookup avoids a full block
		// fetch.
		slot, found, slotErr := utxoRecoverySlotForTx(
			txn.DB(),
			txn,
			ledgerInputIDBytes(input),
		)
		if slotErr != nil {
			return nil, fmt.Errorf(
				"lookup producer slot for legacy utxo recovery: %w",
				slotErr,
			)
		}
		if !found {
			return nil, ErrUtxoNotFound
		}
		addedSlot = slot
	default:
		block, err := utxoRecoveryBlockForTx(
			txn.DB(),
			txn,
			ledgerInputIDBytes(input),
		)
		if err != nil {
			return nil, fmt.Errorf("lookup producer block: %w", err)
		}
		if block == nil {
			return nil, ErrUtxoNotFound
		}
		decodedBlock, err := block.Decode()
		if err != nil {
			return nil, fmt.Errorf(
				"decode producer block for input recovery at slot %d: %w",
				block.Slot,
				err,
			)
		}
		outputCbor, err = utxoCborFromDecodedBlock(
			decodedBlock,
			ledgerInputIDBytes(input),
			input.Index(),
		)
		if err != nil {
			return nil, err
		}
		addedSlot = block.Slot
		indexer := NewBlockIndexer(block.Slot, block.Hash)
		offsets, indexErr := indexer.ComputeOffsets(block.Cbor, decodedBlock)
		if indexErr == nil {
			var txHashArray [32]byte
			copy(txHashArray[:], ledgerInputIDBytes(input))
			ref := UtxoRef{TxId: txHashArray, OutputIdx: input.Index()}
			if offset, ok := offsets.UtxoOffsets[ref]; ok {
				if repairErr := repairUtxoBlob(
					txn.DB(),
					txn,
					ledgerInputIDBytes(input),
					input.Index(),
					&offset,
				); repairErr != nil {
					d.logger.Debug(
						"failed to repair missing consumed input utxo blob",
						"input",
						input.String(),
						"error",
						repairErr,
					)
				}
			}
		}
	}
	output, err := gledger.NewTransactionOutputFromCbor(outputCbor)
	if err != nil {
		return nil, fmt.Errorf("decode transaction output: %w", err)
	}
	ret := models.UtxoLedgerToModel(
		lcommon.Utxo{
			Id:     input,
			Output: output,
		},
		addedSlot,
	)
	// Populate the producer transaction FK so that joins on
	// utxo.transaction_id and Preload("Outputs") from the producer
	// Transaction see this row after a rollback reanimates it. The
	// producer tx record may genuinely be absent (the very condition
	// that drove recovery in some branches); in that case we leave
	// the FK nil and the row stays unjoinable until backfilled by a
	// later path that has the producer.
	producerID, found, lookupErr := d.metadata.GetTransactionIDByHash(
		ledgerInputIDBytes(input),
		txn.Metadata(),
	)
	if lookupErr != nil {
		d.logger.Debug(
			"failed to resolve producer transaction id for recovered utxo",
			"input",
			input.String(),
			"error",
			lookupErr,
		)
	} else if found {
		ret.TransactionID = &producerID
	}
	return &ret, nil
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
		txId := ledgerInputIDBytes(utxo.Id)
		outputIdx := utxo.Id.Index()

		ref := UtxoRef{
			TxId:      txHashArray,
			OutputIdx: outputIdx,
		}

		offset, ok := offsets[ref]
		if !ok {
			return fmt.Errorf(
				"missing offset for genesis utxo %x:%d",
				bytePrefix(txId),
				outputIdx,
			)
		}

		// Store offset reference
		offsetData := EncodeUtxoOffset(&offset)
		if err := blob.SetUtxo(blobTxn, txId, outputIdx, offsetData); err != nil {
			return fmt.Errorf(
				"set genesis utxo offset %x#%d: %w",
				bytePrefix(txId),
				outputIdx,
				err,
			)
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

// SetGenesisStaking stores genesis pool registrations and stake
// delegations. This is metadata-only (no blob operations needed).
func (d *Database) SetGenesisStaking(
	pools map[string]lcommon.PoolRegistrationCertificate,
	stakeDelegations map[string]string,
	blockHash []byte,
	txn *Txn,
) error {
	if txn == nil {
		if err := d.metadata.SetGenesisStaking(
			pools,
			stakeDelegations,
			blockHash,
			nil,
		); err != nil {
			return fmt.Errorf("set genesis staking: %w", err)
		}
		return nil
	}
	if err := d.metadata.SetGenesisStaking(
		pools,
		stakeDelegations,
		blockHash,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf("set genesis staking: %w", err)
	}
	return nil
}

// SetGenesisGovernance stores initial DReps and delegations from the
// Conway genesis bootstrap section. This is metadata-only.
func (d *Database) SetGenesisGovernance(
	initialDReps conway.ConwayGenesisInitialDReps,
	delegs conway.ConwayGenesisDelegs,
	blockHash []byte,
	txn *Txn,
) error {
	if txn == nil {
		if err := d.metadata.SetGenesisGovernance(
			initialDReps,
			delegs,
			blockHash,
			nil,
		); err != nil {
			return fmt.Errorf("set genesis governance: %w", err)
		}
		return nil
	}
	if err := d.metadata.SetGenesisGovernance(
		initialDReps,
		delegs,
		blockHash,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf("set genesis governance: %w", err)
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

// GetTransactionMetadataByHash returns only the stored metadata blob for the
// transaction with the given hash, without loading any associations. Returns
// (nil, nil) when no such transaction exists or it carries no metadata.
func (d *Database) GetTransactionMetadataByHash(
	hash []byte,
	txn *Txn,
) ([]byte, error) {
	if len(hash) == 0 {
		return nil, nil
	}
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	return d.metadata.GetTransactionMetadataByHash(hash, txn.Metadata())
}

// GetTransactionsByHashes returns transactions for the provided hashes.
func (d *Database) GetTransactionsByHashes(
	hashes [][]byte,
	txn *Txn,
) ([]models.Transaction, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	txs, err := d.metadata.GetTransactionsByHashes(hashes, txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf("get txs by hashes: %w", err)
	}
	return txs, nil
}

// GetTransactionsByBlockHash returns all transactions for a given
// block hash, ordered by their position within the block.
func (d *Database) GetTransactionsByBlockHash(
	blockHash []byte,
	txn *Txn,
) ([]models.Transaction, error) {
	if len(blockHash) == 0 {
		return nil, nil
	}
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	txs, err := d.metadata.GetTransactionsByBlockHash(
		blockHash,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get txs by block hash: %w", err,
		)
	}
	return txs, nil
}

// GetTransactionsByAddress returns transactions that involve a given
// address as either a sender (input) or receiver (output).
// Results are returned in descending on-chain order.
func (d *Database) GetTransactionsByAddress(
	addr lcommon.Address,
	limit int,
	offset int,
	txn *Txn,
) ([]models.Transaction, error) {
	zeroHash := lcommon.NewBlake2b224(nil)
	var paymentKey []byte
	var credentialTag uint8
	var stakingKey []byte
	if pkh := addr.PaymentKeyHash(); pkh != zeroHash {
		paymentKey = pkh.Bytes()
	}
	if skh := addr.StakeKeyHash(); skh != zeroHash {
		var ok bool
		credentialTag, ok = models.StakeCredentialTagFromAddress(addr)
		if !ok {
			return nil, errors.New("derive stake credential tag from address")
		}
		stakingKey = skh.Bytes()
	}
	return d.GetTransactionsByAddressKeys(
		paymentKey,
		credentialTag,
		stakingKey,
		limit,
		offset,
		"desc",
		txn,
	)
}

// GetTransactionsByAddressWithOrder returns transactions
// involving a given address with explicit ordering.
func (d *Database) GetTransactionsByAddressWithOrder(
	addr lcommon.Address,
	limit int,
	offset int,
	order string,
	txn *Txn,
) ([]models.Transaction, error) {
	zeroHash := lcommon.NewBlake2b224(nil)
	var paymentKey []byte
	var credentialTag uint8
	var stakingKey []byte
	if pkh := addr.PaymentKeyHash(); pkh != zeroHash {
		paymentKey = pkh.Bytes()
	}
	if skh := addr.StakeKeyHash(); skh != zeroHash {
		var ok bool
		credentialTag, ok = models.StakeCredentialTagFromAddress(addr)
		if !ok {
			return nil, errors.New("derive stake credential tag from address")
		}
		stakingKey = skh.Bytes()
	}
	return d.GetTransactionsByAddressKeys(
		paymentKey,
		credentialTag,
		stakingKey,
		limit,
		offset,
		order,
		txn,
	)
}

// GetTransactionsByAddressKeys returns transactions for a payment/staking
// credential tuple with pagination and explicit order (asc|desc).
func (d *Database) GetTransactionsByAddressKeys(
	paymentKey []byte,
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	txn *Txn,
) ([]models.Transaction, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	txs, err := d.metadata.GetTransactionsByAddress(
		paymentKey,
		credentialTag,
		stakingKey,
		limit,
		offset,
		order,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get txs by address payment=%x staking=%x limit=%d offset=%d order=%s: %w",
			paymentKey,
			stakingKey,
			limit,
			offset,
			order,
			err,
		)
	}
	return txs, nil
}

// CountTransactionsByAddress returns the total number of
// transactions involving a given address.
func (d *Database) CountTransactionsByAddress(
	addr lcommon.Address,
	txn *Txn,
) (int, error) {
	zeroHash := lcommon.NewBlake2b224(nil)
	var paymentKey []byte
	var credentialTag uint8
	var stakingKey []byte
	if pkh := addr.PaymentKeyHash(); pkh != zeroHash {
		paymentKey = pkh.Bytes()
	}
	if skh := addr.StakeKeyHash(); skh != zeroHash {
		var ok bool
		credentialTag, ok = models.StakeCredentialTagFromAddress(addr)
		if !ok {
			return 0, errors.New("derive stake credential tag from address")
		}
		stakingKey = skh.Bytes()
	}
	return d.CountTransactionsByAddressKeys(
		paymentKey,
		credentialTag,
		stakingKey,
		txn,
	)
}

// CountTransactionsByAddressKeys returns the total number
// of transactions for a payment/staking credential tuple.
func (d *Database) CountTransactionsByAddressKeys(
	paymentKey []byte,
	credentialTag uint8,
	stakingKey []byte,
	txn *Txn,
) (int, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	count, err := d.metadata.CountTransactionsByAddress(
		paymentKey,
		credentialTag,
		stakingKey,
		txn.Metadata(),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"count txs by address payment=%x staking=%x: %w",
			paymentKey,
			stakingKey,
			err,
		)
	}
	return count, nil
}

// GetAddressesByCredential returns distinct address mappings for a stake credential.
func (d *Database) GetAddressesByCredential(
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	txn *Txn,
) ([]models.AddressTransaction, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	addresses, err := d.metadata.GetAddressesByCredential(
		credentialTag,
		stakingKey,
		limit,
		offset,
		order,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get addresses by credential tag=%d key=%x limit=%d offset=%d: %w",
			credentialTag,
			stakingKey,
			limit,
			offset,
			err,
		)
	}
	return addresses, nil
}

// CountAddressesByCredential returns the total number of distinct address mappings for a stake credential.
func (d *Database) CountAddressesByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn *Txn,
) (int, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	count, err := d.metadata.CountAddressesByCredential(
		credentialTag,
		stakingKey,
		txn.Metadata(),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"count addresses by credential tag=%d key=%x: %w",
			credentialTag,
			stakingKey,
			err,
		)
	}
	return count, nil
}

// GetTransactionsByMetadataLabel returns transactions that include metadata
// for a given label key.
func (d *Database) GetTransactionsByMetadataLabel(
	label uint64,
	limit int,
	offset int,
	descending bool,
	txn *Txn,
) ([]models.Transaction, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	txs, err := d.metadata.GetTransactionsByMetadataLabel(
		label,
		limit,
		offset,
		descending,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get txs by metadata label %d limit=%d offset=%d descending=%t: %w",
			label,
			limit,
			offset,
			descending,
			err,
		)
	}
	return txs, nil
}

// CountTransactionsByMetadataLabel returns the total number of transactions
// that include metadata for a given label key.
func (d *Database) CountTransactionsByMetadataLabel(
	label uint64,
	txn *Txn,
) (int, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	count, err := d.metadata.CountTransactionsByMetadataLabel(
		label,
		txn.Metadata(),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"count txs by metadata label %d: %w",
			label,
			err,
		)
	}
	return count, nil
}

// DeleteTransactionMetadataLabelsAfterSlot removes transaction metadata
// label index records added after the given slot.
func (d *Database) DeleteTransactionMetadataLabelsAfterSlot(
	slot uint64,
	txn *Txn,
) error {
	if txn == nil {
		txn = d.MetadataTxn(true)
		defer txn.Rollback() //nolint:errcheck
		if err := d.metadata.DeleteTransactionMetadataLabelsAfterSlot(slot, txn.Metadata()); err != nil {
			return fmt.Errorf(
				"delete transaction metadata labels after slot %d: %w",
				slot,
				err,
			)
		}
		return txn.Commit()
	}
	if err := d.metadata.DeleteTransactionMetadataLabelsAfterSlot(slot, txn.Metadata()); err != nil {
		return fmt.Errorf(
			"delete transaction metadata labels after slot %d: %w",
			slot,
			err,
		)
	}
	return nil
}

// deleteTxBlobs attempts to delete blob data for the given transaction hashes.
// This is a best-effort operation; metadata remains the source of truth. When
// the caller provides a blob transaction, deletions stay coupled to that outer
// commit. A temporary blob-only transaction is used only as a fallback when no
// blob handle is available.
func deleteTxBlobs(d *Database, txHashes [][]byte, txn *Txn) error {
	const batchSize = 500
	blob := d.Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}

	var deleteErrors int
	deleteBatch := func(blobTxn types.Txn, batch [][]byte) int {
		var batchDeleteErrors int
		for _, txHash := range batch {
			if err := blob.DeleteTx(blobTxn, txHash); err != nil {
				deleteErrors++
				batchDeleteErrors++
				d.logger.Warn(
					"failed to delete TX blob data",
					"txHash", hex.EncodeToString(txHash),
					"error", err,
				)
			}
		}
		return batchDeleteErrors
	}

	if txn != nil && txn.Blob() != nil {
		deleteBatch(txn.Blob(), txHashes)
	} else {
		for start := 0; start < len(txHashes); start += batchSize {
			end := min(start+batchSize, len(txHashes))
			batch := txHashes[start:end]
			batchTxn := NewBlobOnlyTxn(d, true)
			batchBlobTxn := batchTxn.Blob()
			if batchBlobTxn == nil {
				return types.ErrNilTxn
			}
			batchDeleteErrors := deleteBatch(batchBlobTxn, batch)
			if err := batchTxn.Commit(); err != nil {
				deleteErrors += len(batch) - batchDeleteErrors
				_ = batchTxn.Rollback()
				d.logger.Warn(
					"TX blob delete batch commit failed",
					"batch_start", start,
					"batch_end", end,
					"batch_size", len(batch),
					"error", err,
				)
			}
		}
	}
	if deleteErrors > 0 {
		d.logger.Warn(
			"TX blob deletion completed with errors",
			"failed",
			deleteErrors,
			"total",
			len(txHashes),
		)
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
	txHashes, err := d.metadata.GetTransactionHashesAfterSlot(
		slot,
		txn.Metadata(),
	)
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
	if err := d.metadata.DeleteAddressTransactionsAfterSlot(slot, txn.Metadata()); err != nil {
		return fmt.Errorf(
			"failed to delete address transaction mappings after slot %d: %w",
			slot,
			err,
		)
	}
	if err := d.metadata.DeleteTransactionMetadataLabelsAfterSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to delete transaction metadata labels after slot %d: %w",
			slot,
			err,
		)
	}

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
