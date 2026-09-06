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

// MithrilTrustBoundarySlot returns the recorded Mithril trust boundary slot,
// or 0 if none is recorded (genesis sync, or a non-genesis chainsync
// intersect point with no snapshot import). A failure to read the sync
// state is logged and also treated as 0 (the caller cannot distinguish it
// from "no boundary recorded" by return value alone), but the log lets an
// operator tell a transient storage problem apart from a genuinely
// unrecoverable UTxO when StrictUtxoValidation turns the latter into an
// ingest error.
//
// This fail-open behavior is intentional for that caller (a best-effort
// recovery heuristic), but wrong for a caller enforcing a safety check —
// see MithrilTrustBoundarySlotStrict, used by database/lifecycle.Truncate,
// where treating a failed read as "no boundary recorded" would silently
// let a truncate proceed past a boundary that could not actually be
// verified, rather than merely under-informing a heuristic.
func (d *Database) MithrilTrustBoundarySlot(txn *Txn) uint64 {
	slot, err := d.MithrilTrustBoundarySlotStrict(txn)
	if err != nil {
		d.logger.Warn(
			"failed to read Mithril trust boundary from sync state; "+
				"treating consumed-utxo recovery failures as past the boundary",
			"error", err,
		)
		return 0
	}
	return slot
}

// MithrilTrustBoundarySlotStrict is MithrilTrustBoundarySlot, but returns
// the underlying read error instead of swallowing it as "no boundary
// recorded" — for a caller that must fail closed (refuse the operation)
// rather than fail open when the boundary can't be verified. A malformed
// stored value is also propagated as an error here (unlike
// MithrilTrustBoundarySlot, which still treats it as absent): a corrupted
// persisted boundary must not be indistinguishable from "no snapshot was
// ever imported" for a caller enforcing a safety check, or the check is
// defeated exactly when it matters most.
func (d *Database) MithrilTrustBoundarySlotStrict(txn *Txn) (uint64, error) {
	val, err := d.GetSyncState(mithrilLedgerSlotSyncKey, txn)
	if err != nil {
		return 0, fmt.Errorf("read Mithril trust boundary: %w", err)
	}
	if val == "" {
		return 0, nil
	}
	slot, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf(
			"parse Mithril trust boundary %q: %w",
			val,
			err,
		)
	}
	return slot, nil
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
	return d.SetTransactionWithOpts(
		tx,
		point,
		idx,
		updateEpoch,
		pparamUpdates,
		certDeposits,
		offsets,
		txn,
		BatchedTxIngestOpts{},
	)
}

// SetTransactionWithOpts is SetTransaction with control over UTxO ingest
// behavior via opts. Leios endorser-block application on the Musashi/
// Haskell-conformant path passes SkipConsumedInputRecovery so a transaction's
// effects are applied without the consumed-utxo recovery/repair pass: produced
// outputs and input spends are written, but a consumed input that is absent from
// the store is left as a no-op instead of triggering blob recovery. This matches
// the reference ledger's endorser-closure apply (ruleApplyTxValidation
// ValidateNone), which folds the closure's transactions onto the ledger state
// without validation or recovery.
func (d *Database) SetTransactionWithOpts(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	updateEpoch uint64,
	pparamUpdates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
	certDeposits map[int]uint64,
	offsets *BlockIngestionResult,
	txn *Txn,
	opts BatchedTxIngestOpts,
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
	// Producing no UTxOs is a legal shape: a valid transaction can spend its
	// whole input on deposits plus the fee and return no change (issue #3932),
	// and an invalid transaction without a collateral return produces nothing
	// either. Produced() is Outputs() for a valid transaction and the
	// collateral return for an invalid one, so an empty set means outputs were
	// dropped before storage only when the matching declaration is non-empty.
	// Each branch reports its own declaration: the two are different fields, so
	// one shared message would name the wrong one for half the warnings.
	if len(produced) == 0 {
		// Each accessor is read once into a local: every era's Outputs()
		// allocates a fresh slice per call, and reading CollateralReturn()
		// once closes the gap between the nil test and the dereference.
		outputs := tx.Outputs()
		collateralReturn := tx.CollateralReturn()
		txHashHex := hex.EncodeToString(ledgerHashPrefix(txHash))
		switch {
		case tx.IsValid() && len(outputs) > 0:
			d.logger.Warn(
				"valid transaction produced no UTxOs despite declaring outputs",
				"txHash", txHashHex,
				"outputs", len(outputs),
				"slot", point.Slot,
			)
		case !tx.IsValid() && collateralReturn != nil:
			d.logger.Warn(
				"invalid transaction produced no UTxOs despite "+
					"declaring a collateral return",
				"txHash", txHashHex,
				"collateralReturnLovelace", collateralReturn.Amount().String(),
				"slot", point.Slot,
			)
		}
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

	if err := d.ensureTransactionConsumedUtxos(tx, point, txn, nil, opts); err != nil {
		return err
	}
	// On the Leios endorser-block closure path (SkipConsumedInputRecovery), a
	// consumed input already spent by a different certified endorser-block
	// transaction is a no-op, matching the reference ledger's applyLeiosClosure
	// (ValidateNone), rather than wedging the pipeline with ErrUtxoConflict on a
	// legitimate cross-EB double-consume. Ranking-block application keeps the
	// hard conflict check.
	setTxErr := error(nil)
	if opts.SkipConsumedInputRecovery {
		setTxErr = d.transactionStore().SetTransactionLeiosClosure(
			tx, point, idx, certDeposits,
			opts.SkipWithdrawalWitnessWrite,
			txn.Metadata(),
		)
	} else {
		setTxErr = d.transactionStore().SetTransaction(
			tx, point, idx, certDeposits,
			opts.SkipWithdrawalWitnessWrite,
			txn.Metadata(),
		)
	}
	if setTxErr != nil {
		return fmt.Errorf(
			"set transaction metadata for tx %s (block idx %d, slot %d): %w",
			tx.Hash(), idx, point.Slot, setTxErr,
		)
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
// This is a general primitive for recording a transaction's certificate and
// governance data without applying its UTxO effects. It is no longer on the
// Leios endorser-block apply path: the Musashi path now applies endorser
// transactions with their full effects (see ledger/leios_apply.go and
// SetTransactionWithOpts), matching the reference ledger.
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
	if err := d.transactionStore().SetTransaction(
		metadataOnlyTransaction{Transaction: tx},
		point,
		idx,
		certDeposits,
		// skipWithdrawalWitness: value is moot since
		// metadataOnlyTransaction.Withdrawals() is always empty, so the loop
		// it would gate never runs either way. false (rather than true) for
		// readability: it reads as the honest "do it normally" default
		// instead of implying real gate logic applies here.
		false,
		metadataTxn,
	); err != nil {
		return fmt.Errorf(
			"set transaction metadata only for tx %s (block idx %d, slot %d): %w",
			tx.Hash(),
			idx,
			point.Slot,
			err,
		)
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

	if err := d.transactionStore().SetGapBlockTransaction(
		tx, point, idx, certDeposits, txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"set gap block transaction metadata: %w", err,
		)
	}
	// ensureGapConsumedUtxos must run after SetGapBlockTransaction: it marks
	// the consumed inputs spent by this tx (utxo.spent_at_tx_id is a FK to
	// transaction.hash), so the transaction row has to exist first.
	if err := d.ensureGapConsumedUtxos(
		tx,
		point,
		txn,
	); err != nil {
		return err
	}
	// For a phase-2-invalid transaction the consumed set is its collateral
	// inputs. SetGapBlockTransaction above computed the collateral fee before
	// ensureGapConsumedUtxos recovered those inputs from the blob store, so
	// when the tx declares no total collateral the fee was computed from an
	// incomplete UTxO view and undercounts. Recompute it now that the inputs
	// are materialized so the epoch fee pot is correct.
	if err := d.transactionStore().RecomputeGapCollateralFee(
		tx, point, txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"recompute gap block collateral fee: %w", err,
		)
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
	// Read the Mithril trust boundary once: below it, absent producer rows are
	// legitimately expected (the snapshot does not carry pre-boundary history);
	// past it the node should hold complete producer history.
	mithrilBoundarySlot := d.MithrilTrustBoundarySlot(txn)
	for _, input := range consumed {
		inputTxId := ledgerInputIDBytes(input)
		inputKey := fmt.Sprintf("%x:%d", inputTxId, input.Index())
		if _, ok := seen[inputKey]; ok {
			continue
		}
		seen[inputKey] = struct{}{}
		existingUtxo, err := d.utxoStore().GetUtxoIncludingSpent(
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
				if err := d.utxoStore().SetUtxoDeletedAtSlot(
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
		// For a validated block past the Mithril trust boundary, recover a
		// missing producer only when its block is still on the applied primary
		// chain (issue #3005). Core-mode cleanup can remove a spent row before a
		// rollback needs to restore it, even though the producer itself remains
		// canonical (issue #3170). The primary-chain check preserves the
		// input-conservation guard: an abandoned-fork producer is still refused.
		recoveredUtxo, err := d.recoverConsumedUtxo(
			input,
			txn,
			d.config.StrictUtxoValidation && point.Slot > mithrilBoundarySlot,
		)
		if err != nil {
			// Past the Mithril trust boundary the node should have complete
			// producer history for every input it is asked to spend, so an
			// unrecoverable UTxO there indicates real corruption or a bug
			// rather than an expected gap. Below the boundary (or when none
			// is recorded and we did not sync from genesis) the UTxO may
			// legitimately predate the data we imported.
			if d.config.StrictUtxoValidation &&
				point.Slot > mithrilBoundarySlot {
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
	if err := d.utxoStore().ImportUtxos(
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
		existingUtxo, err := d.utxoStore().GetUtxoIncludingSpent(
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
				if err := d.utxoStore().SetUtxoDeletedAtSlot(
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
		// Mithril gap-closure recovers producers from imported history that
		// legitimately has no block-index entry, so the primary-chain
		// membership check is not applied on this path.
		recoveredUtxo, err := d.recoverConsumedUtxo(input, txn, false)
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
	if err := d.utxoStore().ImportUtxos(
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

// recoveredProducerOnPrimaryChain reports whether the block that produced a
// blob-recovered UTxO is the block currently indexed on the applied primary
// chain at that height. The append-only blob store retains blocks from
// abandoned forks, so a producer found in the blob is not necessarily on the
// applied chain. Mirrors LedgerState.primaryChainContainsPoint at the database
// layer: BlockByIndex reveals which block is canonical at the producer's
// height, and a hash mismatch means the producer was abandoned.
//
// The producer's block ID is supplied by the caller rather than resolved here.
// Every recovery path has already loaded the producer -- from the blob's block
// metadata in the offset case, or as a *models.Block in the others -- so
// looking it up again by point would download the same full block CBOR from
// cold cloud storage a second time, once per recovered input, on exactly the
// Mithril catch-up path this check runs on.
func (d *Database) recoveredProducerOnPrimaryChain(
	txn *Txn,
	producerID uint64,
	hash []byte,
) (bool, error) {
	indexed, err := d.BlockByIndex(producerID, txn)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return false, nil
		}
		return false, err
	}
	return bytes.Equal(indexed.Hash, hash), nil
}

// refuseOffPrimaryChainProducer returns a wrapped ErrUtxoNotFound when the
// producer block of a blob-recovered consumed input is not on the applied
// primary chain. Recovering such a producer would splice in a UTxO the applied
// chain never produced (issue #3005 cross-fork input-conservation violation).
// It is enforced for validated blocks past the Mithril trust boundary, where
// the producer must be a live, applied, on-chain UTxO, so an abandoned-fork
// producer is never legitimate. Below the boundary and on the Mithril
// gap-closure path the check is not applied, because imported history need not
// carry a block-index entry for the producer.
func (d *Database) refuseOffPrimaryChainProducer(
	txn *Txn,
	producerID uint64,
	slot uint64,
	hash []byte,
	input lcommon.TransactionInput,
) error {
	onChain, err := d.recoveredProducerOnPrimaryChain(txn, producerID, hash)
	if err != nil {
		return fmt.Errorf(
			"check producer primary-chain membership for %s: %w",
			input.String(),
			err,
		)
	}
	if !onChain {
		return fmt.Errorf(
			"producer block %x at slot %d for consumed utxo %s is not on the "+
				"applied primary chain: refusing abandoned-fork blob recovery "+
				"that would persist an input-conservation violation "+
				"(issue #3005): %w",
			hash,
			slot,
			input.String(),
			ErrUtxoNotFound,
		)
	}
	return nil
}

func (d *Database) recoverConsumedUtxo(
	input lcommon.TransactionInput,
	txn *Txn,
	enforcePrimaryChain bool,
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
		blockCbor, producerMeta, err := blob.GetBlock(
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
		if enforcePrimaryChain {
			if err := d.refuseOffPrimaryChainProducer(
				txn,
				producerMeta.ID,
				offset.BlockSlot,
				offset.BlockHash[:],
				input,
			); err != nil {
				return nil, err
			}
		}
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
		if enforcePrimaryChain {
			// The legacy metadata slot lookup yields neither the producer
			// block hash nor its ID, so resolve the producer block to check
			// primary-chain membership. Legacy raw-CBOR blob entries do not
			// occur past the Mithril boundary in practice, so this extra
			// lookup is exceptional.
			prodBlock, bErr := utxoRecoveryBlockForTx(
				txn.DB(), txn, ledgerInputIDBytes(input),
			)
			if bErr != nil {
				return nil, fmt.Errorf(
					"resolve producer block for primary-chain check: %w", bErr,
				)
			}
			if prodBlock == nil {
				return nil, ErrUtxoNotFound
			}
			if err := d.refuseOffPrimaryChainProducer(
				txn, prodBlock.ID, prodBlock.Slot, prodBlock.Hash, input,
			); err != nil {
				return nil, err
			}
		}
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
		if enforcePrimaryChain {
			if err := d.refuseOffPrimaryChainProducer(
				txn, block.ID, block.Slot, block.Hash, input,
			); err != nil {
				return nil, err
			}
		}
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
	ret, err := models.UtxoLedgerToModel(
		lcommon.Utxo{
			Id:     input,
			Output: output,
		},
		addedSlot,
	)
	if err != nil {
		return nil, fmt.Errorf("convert recovered utxo: %w", err)
	}
	// Populate the producer transaction FK so that joins on
	// utxo.transaction_id and Preload("Outputs") from the producer
	// Transaction see this row after a rollback reanimates it. The
	// producer tx record may genuinely be absent (the very condition
	// that drove recovery in some branches); in that case we leave
	// the FK nil and the row stays unjoinable until backfilled by a
	// later path that has the producer.
	producerID, found, lookupErr := d.transactionStore().GetTransactionIDByHash(
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
		model, err := models.UtxoLedgerToModel(utxo, 0)
		if err != nil {
			return fmt.Errorf(
				"convert genesis utxo %x:%d: %w",
				bytePrefix(txId),
				outputIdx,
				err,
			)
		}
		utxoModels[i] = model
	}

	// Store transaction in metadata
	if err := d.transactionStore().SetGenesisTransaction(
		txHash,
		blockHash,
		utxoModels,
		txn.Metadata(),
	); err != nil {
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
	keyDeposit uint64,
	blockHash []byte,
	txn *Txn,
) error {
	if txn == nil {
		if err := d.metadata.SetGenesisStaking(
			pools,
			stakeDelegations,
			keyDeposit,
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
		keyDeposit,
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
	return d.transactionStore().GetTransactionByHash(hash, txn.Metadata())
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
	return d.transactionStore().
		GetTransactionMetadataByHash(hash, txn.Metadata())
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
	txs, err := d.transactionStore().GetTransactionsByHashes(
		hashes,
		txn.Metadata(),
	)
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
	txs, err := d.transactionStore().GetTransactionsByBlockHash(
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
	return d.getTransactionsByExactAddress(
		addr,
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
	return d.getTransactionsByExactAddress(
		addr,
		limit,
		offset,
		order,
		txn,
	)
}

func addressTransactionKeys(
	addr lcommon.Address,
) ([]byte, uint8, []byte, error) {
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
			return nil, 0, nil, errors.New(
				"derive stake credential tag from address",
			)
		}
		stakingKey = skh.Bytes()
	}
	return paymentKey, credentialTag, stakingKey, nil
}

func (d *Database) getTransactionsByExactAddress(
	addr lcommon.Address,
	limit int,
	offset int,
	order string,
	txn *Txn,
) ([]models.Transaction, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	paymentKey, credentialTag, stakingKey, err := addressTransactionKeys(addr)
	if err != nil {
		return nil, err
	}
	exactAddress, err := addr.Bytes()
	if err != nil {
		return nil, fmt.Errorf("encode exact transaction address: %w", err)
	}

	const candidateBatchSize = 128
	initialCapacity := min(max(limit, 0), candidateBatchSize)
	ret := make([]models.Transaction, 0, initialCapacity)
	candidateOffset := 0
	candidatesProcessed := 0
	matchesSkipped := 0
	for {
		remainingCandidates := exactAddressCandidateScanLimit -
			candidatesProcessed
		if remainingCandidates <= 0 {
			return ret, errExactAddressCandidateScanLimit
		}
		batchSize := min(candidateBatchSize, remainingCandidates)
		candidates, err := d.transactionStore().GetTransactionsByAddress(
			paymentKey,
			credentialTag,
			stakingKey,
			batchSize,
			candidateOffset,
			order,
			txn.Metadata(),
		)
		if err != nil {
			return nil, fmt.Errorf(
				"get exact-address transaction candidates: %w",
				err,
			)
		}
		candidatesProcessed += len(candidates)
		for i := range candidates {
			match, err := transactionContainsExactAddress(
				&candidates[i],
				exactAddress,
				txn,
			)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
			if matchesSkipped < offset {
				matchesSkipped++
				continue
			}
			ret = append(ret, candidates[i])
			if limit > 0 && len(ret) == limit {
				return ret, nil
			}
		}
		if len(candidates) < batchSize {
			return ret, nil
		}
		if candidatesProcessed >= exactAddressCandidateScanLimit {
			return ret, errExactAddressCandidateScanLimit
		}
		candidateOffset += len(candidates)
	}
}

func transactionContainsExactAddress(
	tx *models.Transaction,
	exactAddress []byte,
	txn *Txn,
) (bool, error) {
	utxos := make([]*models.Utxo, 0,
		len(tx.Inputs)+len(tx.Outputs)+len(tx.Collateral)+
			len(tx.ReferenceInputs)+1,
	)
	for i := range tx.Inputs {
		utxos = append(utxos, &tx.Inputs[i])
	}
	for i := range tx.Outputs {
		utxos = append(utxos, &tx.Outputs[i])
	}
	for i := range tx.Collateral {
		utxos = append(utxos, &tx.Collateral[i])
	}
	for i := range tx.ReferenceInputs {
		utxos = append(utxos, &tx.ReferenceInputs[i])
	}
	if tx.CollateralReturn != nil {
		utxos = append(utxos, tx.CollateralReturn)
	}
	for _, utxo := range utxos {
		if err := loadCbor(utxo, txn); err != nil {
			return false, fmt.Errorf(
				"load transaction UTxO %x#%d for exact address match: %w",
				utxo.TxId,
				utxo.OutputIdx,
				err,
			)
		}
		output, err := utxo.Decode()
		if err != nil {
			return false, fmt.Errorf(
				"decode transaction UTxO %x#%d for exact address match: %w",
				utxo.TxId,
				utxo.OutputIdx,
				err,
			)
		}
		addressBytes, err := output.Address().Bytes()
		if err != nil {
			return false, fmt.Errorf("encode transaction UTxO address: %w", err)
		}
		if bytes.Equal(addressBytes, exactAddress) {
			return true, nil
		}
	}
	return false, nil
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
	txs, err := d.transactionStore().GetTransactionsByAddress(
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
	txs, err := d.getTransactionsByExactAddress(
		addr,
		0,
		0,
		"desc",
		txn,
	)
	if err != nil {
		return 0, err
	}
	return len(txs), nil
}

// HasTransactionsByAddress reports whether at least one transaction involves
// the given exact address.
func (d *Database) HasTransactionsByAddress(
	addr lcommon.Address,
	txn *Txn,
) (bool, error) {
	txs, err := d.getTransactionsByExactAddress(
		addr,
		1,
		0,
		"desc",
		txn,
	)
	if err != nil {
		return false, err
	}
	return len(txs) > 0, nil
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
	count, err := d.transactionStore().CountTransactionsByAddress(
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

// CountTransactionsByPaymentCred returns the total number of transactions
// involving a payment credential across every address that carries it,
// regardless of staking part.
func (d *Database) CountTransactionsByPaymentCred(
	paymentKey []byte,
	txn *Txn,
) (int, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	count, err := d.transactionStore().CountTransactionsByPaymentCred(
		paymentKey,
		txn.Metadata(),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"count txs by payment cred %x: %w",
			paymentKey,
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
	addresses, err := d.transactionStore().GetAddressesByCredential(
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
	count, err := d.transactionStore().CountAddressesByCredential(
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
	txs, err := d.transactionStore().GetTransactionsByMetadataLabel(
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
	count, err := d.transactionStore().CountTransactionsByMetadataLabel(
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
		if err := d.transactionStore().DeleteTransactionMetadataLabelsAfterSlot(
			slot,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"delete transaction metadata labels after slot %d: %w",
				slot,
				err,
			)
		}
		return txn.Commit()
	}
	if err := d.transactionStore().DeleteTransactionMetadataLabelsAfterSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"delete transaction metadata labels after slot %d: %w",
			slot,
			err,
		)
	}
	return nil
}

// deleteTxBlobs deletes blob data for the given transaction hashes. Metadata
// remains the source of truth. When the caller provides a blob transaction,
// deletions stay coupled to that outer commit. A temporary blob-only
// transaction is used only as a fallback when no blob handle is available.
//
// Failures do not stop the remaining deletes, but they are counted and
// reported as [ErrBlobDeleteIncomplete]: the caller goes on to remove the
// metadata that names these objects, after which nothing can reach them
// again. The count is deferred to the enclosing transaction's commit for the
// reason given on deleteUtxoBlobs.
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
		recordBlobOrphansOnCommit(txn, deleteErrors)
		d.logger.Warn(
			"TX blob deletion completed with errors",
			"failed",
			deleteErrors,
			"total",
			len(txHashes),
		)
		return fmt.Errorf(
			"%w: %d of %d transaction blobs",
			ErrBlobDeleteIncomplete,
			deleteErrors,
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
	txHashes, err := d.transactionStore().GetTransactionHashesAfterSlot(
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
	// A blob delete failure must not stop the metadata cleanup below: a
	// rolled-back transaction cannot stay addressable. The objects it strands
	// are counted and logged rather than dropped.
	if blobErr := deleteTxBlobs(d, txHashes, txn); blobErr != nil {
		d.logger.Error(
			"rolled-back transaction blob delete left unreachable objects",
			"error", blobErr,
			"slot", slot,
			"transactions", len(txHashes),
		)
	}

	// Then delete metadata (source of truth)
	if err := d.transactionStore().DeleteAddressTransactionsAfterSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to delete address transaction mappings after slot %d: %w",
			slot,
			err,
		)
	}
	if err := d.transactionStore().DeleteTransactionMetadataLabelsAfterSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to delete transaction metadata labels after slot %d: %w",
			slot,
			err,
		)
	}

	err = d.transactionStore().DeleteTransactionsAfterSlot(slot, txn.Metadata())
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
