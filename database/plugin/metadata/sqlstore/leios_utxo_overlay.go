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

package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

const (
	maxLeiosUtxoOverlayTransactions = 8192
	maxLeiosUtxoOverlayOutputBytes  = 8 * 1024 * 1024
)

// EnableLeiosClosureUtxoOverlay stages plain API closure UTxOs while retaining
// every output and its spend history. Ineligible closures keep immediate writes.
func (s *Store) EnableLeiosClosureUtxoOverlay(
	accumulator types.MetadataBatchAccumulator,
	txn types.Txn,
	transactions []lcommon.Transaction,
) (bool, error) {
	acc, ok := accumulator.(*transactionBatchAccumulator)
	if !ok || !acc.leiosAPIRowsOnly || s.storageMode != types.StorageModeAPI ||
		s.dialect.Name() != "sqlite" || len(transactions) < 2 ||
		len(transactions) > maxLeiosUtxoOverlayTransactions {
		return false, nil
	}
	transaction, ok := txn.(*sqlTxn)
	if !ok || transaction.owner != s {
		return false, nil
	}
	if acc.overlay != nil {
		return false, errors.New("closure UTxO overlay already enabled")
	}
	produced := make(map[overlayKey]struct{}, len(transactions))
	hashes := make([][]byte, 0, len(transactions))
	outputs := make([]models.UtxoId, 0, len(transactions))
	external := make([]models.UtxoId, 0, len(transactions))
	allowed := make(map[string]struct{}, len(transactions))
	spent := make(map[overlayKey]struct{}, len(transactions))
	var outputBytes uint64
	for _, tx := range transactions {
		if !plainLeiosOverlayTransaction(tx) {
			return false, nil
		}
		hash := tx.Hash().Bytes()
		if _, exists := allowed[string(hash)]; exists {
			return false, nil
		}
		allowed[string(hash)] = struct{}{}
		hashes = append(hashes, hash)
		consumed, outputLevels := tx.Consumed(), tx.Produced()
		if len(consumed) != 1 || len(outputLevels) != 1 || consumed[0] == nil || outputLevels[0].Id == nil || outputLevels[0].Output == nil {
			return false, nil
		}
		input := consumed[0]
		key := newOverlayKey(input.Id().Bytes(), input.Index())
		if _, repeated := spent[key]; repeated {
			return false, nil
		}
		spent[key] = struct{}{}
		if _, internal := produced[key]; !internal {
			external = append(
				external,
				models.UtxoId{Hash: input.Id().Bytes(), Idx: input.Index()},
			)
		}
		out := outputLevels[0]
		size, err := lcommon.TransactionOutputCborSize(out.Output)
		if err != nil {
			return false, fmt.Errorf("size closure output: %w", err)
		}
		if size > maxLeiosUtxoOverlayOutputBytes-outputBytes {
			return false, nil
		}
		outputBytes += size
		if !bytes.Equal(out.Id.Id().Bytes(), hash) {
			return false, nil
		}
		key = newOverlayKey(hash, out.Id.Index())
		if _, exists := produced[key]; exists {
			return false, nil
		}
		produced[key] = struct{}{}
		outputs = append(
			outputs,
			models.UtxoId{Hash: hash, Idx: out.Id.Index()},
		)
	}
	existing, err := s.GetTransactionsByHashes(hashes, txn)
	if err != nil {
		return false, err
	}
	if len(existing) != 0 {
		return false, nil
	}
	existingOutputs, err := s.utxosByExactRefs(txn, outputs)
	if err != nil {
		return false, err
	}
	if len(existingOutputs) != 0 {
		return false, nil
	}
	inputs, err := s.utxosByExactRefs(txn, external)
	if err != nil {
		return false, err
	}
	external = dedupeUtxoIDs(external)
	if len(inputs) != len(external) {
		return false, nil
	}
	// Pointer and reference-script relations are not hydrated by the exact
	// UTxO metadata reader. Exclude them explicitly before enabling staging.
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return false, err
	}
	for start := 0; start < len(inputs); start += 200 {
		batch := inputs[start:min(start+200, len(inputs))]
		args := make([]any, 0, len(batch)*2)
		for range 2 {
			for _, input := range batch {
				args = append(args, input.ID)
			}
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(batch)), ",")
		query := `-- name: LeiosOverlayInputHasRelations :one
SELECT EXISTS(SELECT 1 FROM utxo_pointer WHERE utxo_id IN (` + placeholders + `))
 OR EXISTS(SELECT 1 FROM utxo_reference_script WHERE utxo_id IN (` + placeholders + `))`
		var related bool
		if err := db.QueryRowContext(ctx, query, args...).Scan(&related); err != nil {
			return false, err
		}
		if related {
			return false, nil
		}
	}
	overlay := &utxoOverlay{
		retainHistory:   true,
		apiInputs:       make(map[overlayKey]models.Utxo, len(inputs)),
		apiTransactions: allowed,
		created:         make(map[overlayKey]*overlayOutput, len(transactions)),
		inputs:          make(map[overlayKey]*overlayInput, len(inputs)),
		absent:          make(map[overlayKey]struct{}),
	}
	for _, in := range inputs {
		if in.DeletedSlot != 0 || len(in.SpentAtTxId) != 0 ||
			!plainLeiosOverlayOutput(in) {
			return false, nil
		}
		id, err := checkedInt64(uint64(in.ID))
		if err != nil {
			return false, fmt.Errorf("closure input ID: %w", err)
		}
		key := newOverlayKey(in.TxId, in.OutputIdx)
		overlay.apiInputs[key] = in
		overlay.inputs[key] = &overlayInput{
			id: id, tag: int64(in.CredentialTag), stakingKey: in.StakingKey,
			amount: sql.NullString{
				String: decimalUint64(in.Amount),
				Valid:  true,
			},
		}
	}
	err = s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			var err error
			overlay.utxoSeq, overlay.utxoSeqRow, err = readSQLiteSequence(
				ctx,
				db,
				"utxo",
			)
			if err != nil {
				return err
			}
			overlay.utxoWritten = overlay.utxoSeq
			overlay.assetSeq, overlay.assetSeqRow, err = readSQLiteSequence(
				ctx,
				db,
				"asset",
			)
			overlay.assetWritten = overlay.assetSeq
			return err
		},
	)
	if err != nil {
		return false, fmt.Errorf("prepare closure UTxO overlay: %w", err)
	}
	if err := transaction.bindBatch(acc); err != nil {
		return false, err
	}
	acc.overlay = overlay
	transaction.setOverlayAccumulator(acc)
	return true, nil
}

func plainLeiosOverlayTransaction(tx lcommon.Transaction) bool {
	if tx == nil || !tx.IsValid() || len(tx.Inputs()) != 1 || len(tx.Consumed()) != 1 ||
		len(tx.Outputs()) != 1 || len(tx.Produced()) != 1 ||
		len(tx.Collateral()) != 0 || tx.CollateralReturn() != nil ||
		len(tx.ReferenceInputs()) != 0 || len(tx.Certificates()) != 0 ||
		len(tx.Withdrawals()) != 0 || tx.AssetMint() != nil ||
		len(tx.VotingProcedures()) != 0 ||
		len(tx.ProposalProcedures()) != 0 ||
		len(lcommon.SubTransactionBodiesFromTransaction(tx)) != 0 ||
		len(lcommon.SubTransactionWitnessSetsFromTransaction(tx)) != 0 {
		return false
	}
	_, updates := tx.ProtocolParameterUpdates()
	if len(updates) != 0 {
		return false
	}
	input, consumed := tx.Inputs()[0], tx.Consumed()[0]
	if input.Id() != consumed.Id() || input.Index() != consumed.Index() {
		return false
	}
	out := tx.Produced()[0]
	if out.Output.Datum() != nil || out.Output.DatumHash() != nil ||
		out.Output.ScriptRef() != nil {
		return false
	}
	model, err := models.UtxoLedgerToModel(out, 0)
	if err != nil || !plainLeiosOverlayOutput(model) {
		return false
	}
	witnesses := tx.Witnesses()
	if witnesses != nil {
		if len(witnesses.NativeScripts()) != 0 || len(witnesses.PlutusData()) != 0 ||
			len(witnesses.PlutusV1Scripts()) != 0 || len(witnesses.PlutusV2Scripts()) != 0 ||
			len(witnesses.PlutusV3Scripts()) != 0 ||
			len(lcommon.PlutusV4ScriptsFromWitnessSet(witnesses)) != 0 {
			return false
		}
		if redeemers := witnesses.Redeemers(); redeemers != nil {
			for range redeemers.Iter() {
				return false
			}
		}
	}
	return true
}

func plainLeiosOverlayOutput(model models.Utxo) bool {
	return len(model.StakingKey) == 0 && len(model.Assets) == 0 &&
		!model.PaymentScript && model.Pointer == nil && len(model.DatumHash) == 0 &&
		len(model.Datum) == 0 && len(model.ScriptRef) == 0 &&
		len(model.ReferencedByTxId) == 0 && len(model.CollateralByTxId) == 0
}

func retainedOverlayFromQueryer(db queryer) *utxoOverlay {
	for {
		switch q := db.(type) {
		case barrierQueryer:
			o := q.accumulator.overlay
			if o != nil && o.retainHistory {
				return o
			}
			return nil
		case countingQueryer:
			db = q.queryer
		case dialectQueryer:
			db = q.queryer
		default:
			return nil
		}
	}
}

func (o *utxoOverlay) retainedUtxo(key overlayKey) (*models.Utxo, bool) {
	var model models.Utxo
	if out, ok := o.created[key]; ok {
		model = out.model
	} else if in, ok := o.apiInputs[key]; ok {
		model = in
		state := o.inputs[key]
		if state == nil {
			return nil, false
		}
		model.DeletedSlot = state.deletedSlot
		model.SpentAtTxId = state.spentBy
	} else {
		return nil, false
	}
	// Match the metadata reader's SQL projection. Convenience CBOR and address
	// fields are loaded by the database layer, not by a metadata point lookup.
	model.Cbor = nil
	model.Address = nil
	model.Datum = nil
	model.ScriptRef = nil
	model.ReferenceScriptHash = nil
	model.Pointer = nil
	model.Assets = []models.Asset{}
	model.TxId = bytes.Clone(model.TxId)
	model.PaymentKey = bytes.Clone(model.PaymentKey)
	model.Address = bytes.Clone(model.Address)
	model.Cbor = bytes.Clone(model.Cbor)
	model.SpentAtTxId = bytes.Clone(model.SpentAtTxId)
	if model.TransactionID != nil {
		id := *model.TransactionID
		model.TransactionID = &id
	}
	return &model, true
}
