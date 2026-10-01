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
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/labelcodec"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func (s *Store) applyTransactionMetadataLabels(
	rows *rowBatch,
	transactionID int64,
	slot uint64,
	labels []labelcodec.Entry,
) {
	if s.storageMode != types.StorageModeAPI {
		return
	}
	// A multi-row upsert may not name one (transaction_id, label) key twice,
	// so a repeated label keeps only its last value, as one upsert per label
	// did.
	last := make(map[uint64]int, len(labels))
	for i, label := range labels {
		last[label.Label] = i
	}
	for i, label := range labels {
		if last[label.Label] != i {
			continue
		}
		var jsonValue any
		if label.JSONError == nil {
			jsonValue = label.JsonValue
		}
		rows.add(
			metadataLabelShape,
			transactionID,
			decimalUint64(types.Uint64(label.Label)),
			slot,
			label.CborValue,
			jsonValue,
		)
	}
}

func (s *Store) applyTransactionAssetMintBurn(
	ctx context.Context,
	db queryer,
	transaction lcommon.Transaction,
	hash []byte,
	slot uint64,
	index uint32,
) error {
	if s.storageMode != types.StorageModeAPI || !transaction.IsValid() {
		return nil
	}
	for _, asset := range models.ConvertMintToAssetMintBurnModels(
		transaction.AssetMint(),
		hash,
		slot,
		index,
	) {
		if _, err := db.ExecContext(ctx, `
INSERT INTO asset_mint_burn (
    tx_hash, policy_id, name, fingerprint, slot, quantity, tx_index
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (tx_hash, policy_id, name) DO NOTHING`,
			asset.TxHash,
			asset.PolicyId,
			asset.Name,
			asset.Fingerprint,
			asset.Slot,
			asset.Quantity,
			asset.TxIndex,
		); err != nil {
			return fmt.Errorf("record asset mint/burn: %w", err)
		}
	}
	return nil
}

func (s *Store) applyTransactionAPIDetails(
	ctx context.Context,
	db queryer,
	transactionID int64,
	transaction lcommon.Transaction,
	slot uint64,
	index uint32,
	produced []models.Utxo,
	rows *rowBatch,
) error {
	if s.storageMode != types.StorageModeAPI {
		return nil
	}
	hash := transaction.Hash().Bytes()
	if err := markTransactionUtxoReferences(
		ctx,
		db,
		transaction.Collateral(),
		"collateral_by_tx_id",
		hash,
	); err != nil {
		return fmt.Errorf("mark collateral inputs: %w", err)
	}
	if err := markTransactionUtxoReferences(
		ctx,
		db,
		transaction.ReferenceInputs(),
		"referenced_by_tx_id",
		hash,
	); err != nil {
		return fmt.Errorf("mark reference inputs: %w", err)
	}
	if err := indexTransactionAddresses(
		ctx,
		db,
		rows,
		transactionID,
		transaction,
		slot,
		index,
		produced,
		s.dialect.ParameterLimit(),
	); err != nil {
		return err
	}
	if err := storeTransactionWitnesses(
		ctx,
		db,
		rows,
		transactionID,
		transaction,
		slot,
	); err != nil {
		return err
	}
	return storeTransactionDatumIndex(rows, transaction, slot)
}

func markTransactionUtxoReferences(
	ctx context.Context,
	db queryer,
	inputs []lcommon.TransactionInput,
	column string,
	hash []byte,
) error {
	if column != "collateral_by_tx_id" &&
		column != "referenced_by_tx_id" {
		return fmt.Errorf("unsupported UTxO reference column %q", column)
	}
	for _, input := range inputs {
		associationTable := "utxo_collateral_input"
		if column == "referenced_by_tx_id" {
			associationTable = "utxo_reference_input"
		}
		if _, err := db.ExecContext(
			ctx,
			`INSERT INTO `+associationTable+` (utxo_id, transaction_hash)
SELECT u.id, ? FROM utxo AS u
WHERE u.tx_id = ? AND u.output_idx = ?
  AND NOT EXISTS (
      SELECT 1 FROM `+associationTable+` AS r
      WHERE r.utxo_id = u.id AND r.transaction_hash = ?
  )`,
			hash,
			input.Id().Bytes(),
			input.Index(),
			hash,
		); err != nil {
			return err
		}
		query := "UPDATE utxo SET " + column +
			" = ? WHERE tx_id = ? AND output_idx = ?"
		if _, err := db.ExecContext(
			ctx,
			query,
			hash,
			input.Id().Bytes(),
			input.Index(),
		); err != nil {
			return err
		}
	}
	return nil
}

type addressIndexKey struct {
	payment string
	tag     uint8
	staking string
}

func indexTransactionAddresses(
	ctx context.Context,
	db queryer,
	rows *rowBatch,
	transactionID int64,
	transaction lcommon.Transaction,
	slot uint64,
	index uint32,
	produced []models.Utxo,
	parameterLimit int,
) error {
	if _, err := db.ExecContext(ctx, `
DELETE FROM address_transaction WHERE transaction_id = ?`,
		transactionID,
	); err != nil {
		return fmt.Errorf("delete existing address transactions: %w", err)
	}
	addresses := make(map[addressIndexKey]struct{})
	add := func(payment []byte, tag uint8, staking []byte) {
		if len(payment) == 0 && len(staking) == 0 {
			return
		}
		addresses[addressIndexKey{
			payment: string(payment),
			tag:     tag,
			staking: string(staking),
		}] = struct{}{}
	}
	for _, output := range produced {
		add(output.PaymentKey, output.CredentialTag, output.StakingKey)
	}
	allInputs := make(
		[]lcommon.TransactionInput,
		0,
		len(transaction.Inputs())+
			len(transaction.Collateral())+
			len(transaction.ReferenceInputs()),
	)
	allInputs = append(allInputs, transaction.Inputs()...)
	allInputs = append(allInputs, transaction.Collateral()...)
	allInputs = append(allInputs, transaction.ReferenceInputs()...)
	type inputKey struct {
		txID  string
		index uint32
	}
	keys := make([]inputKey, 0, len(allInputs))
	seen := make(map[inputKey]struct{}, len(allInputs))
	for _, input := range allInputs {
		key := inputKey{txID: string(input.Id().Bytes()), index: input.Index()}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	if parameterLimit < 2 {
		parameterLimit = 2
	}
	for start := 0; start < len(keys); start += parameterLimit / 2 {
		end := start + parameterLimit/2
		end = min(end, len(keys))
		predicates := make([]string, 0, end-start)
		args := make([]any, 0, (end-start)*2)
		for _, key := range keys[start:end] {
			predicates = append(predicates, "(tx_id = ? AND output_idx = ?)")
			args = append(args, []byte(key.txID), key.index)
		}
		inputRows, err := db.QueryContext(ctx, `
SELECT tx_id, output_idx, payment_key, credential_tag, staking_key
FROM utxo WHERE `+strings.Join(predicates, " OR "), args...)
		if err != nil {
			return fmt.Errorf(
				"lookup input addresses for transaction %d: %w",
				transactionID,
				err,
			)
		}
		if err := func() (runErr error) {
			defer func() {
				runErr = errors.Join(runErr, inputRows.Close())
			}()
			for inputRows.Next() {
				var (
					txID    []byte
					output  uint32
					payment []byte
					staking []byte
					tag     uint8
				)
				if err := inputRows.Scan(&txID, &output, &payment, &tag, &staking); err != nil {
					return fmt.Errorf("scan input address for transaction %d: %w", transactionID, err)
				}
				add(payment, tag, staking)
			}
			if err := inputRows.Err(); err != nil {
				return fmt.Errorf("iterate input addresses for transaction %d: %w", transactionID, err)
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	for address := range addresses {
		rows.add(
			addressTransactionShape,
			[]byte(address.payment),
			[]byte(address.staking),
			address.tag,
			transactionID,
			slot,
			index,
		)
	}
	return nil
}

// transactionWitnessTables are the per-transaction detail tables
// storeTransactionWitnesses rewrites wholesale. Every SetTransaction clears
// the rows the previous attempt left behind before re-inserting, so a
// re-processed block cannot accumulate duplicate witnesses.
var transactionWitnessTables = []string{
	"key_witness",
	"witness_scripts",
	"redeemer",
	"plutus_data",
}

// TransactionWitnessTables lists the tables TransactionWitnessCleanupSQL is
// issued against, in the order storeTransactionWitnesses clears them.
func TransactionWitnessTables() []string {
	return slices.Clone(transactionWitnessTables)
}

// TransactionWitnessCleanupSQL is the idempotency delete storeTransactionWitnesses
// runs against one witness table on every API-mode SetTransaction.
//
// Exported so a test can pin its query plan against the statement the store
// actually runs. The predicate column must stay indexed through bulk load:
// unindexed, each of these deletes degrades into a full scan of a table that
// grows with every transaction written, which makes historical backfill
// quadratic.
func TransactionWitnessCleanupSQL(table string) string {
	return "DELETE FROM " + table + " WHERE transaction_id = ?"
}

func storeTransactionWitnesses(
	ctx context.Context,
	db queryer,
	rows *rowBatch,
	transactionID int64,
	transaction lcommon.Transaction,
	slot uint64,
) error {
	for _, table := range transactionWitnessTables {
		if _, err := db.ExecContext(
			ctx,
			TransactionWitnessCleanupSQL(table),
			transactionID,
		); err != nil {
			return fmt.Errorf("delete existing %s rows: %w", table, err)
		}
	}
	witnesses := transaction.Witnesses()
	if witnesses == nil {
		return nil
	}
	for _, witness := range witnesses.Vkey() {
		rows.add(
			vkeyWitnessShape,
			witness.Vkey,
			witness.Signature,
			transactionID,
			models.KeyWitnessTypeVkey,
		)
	}
	for _, witness := range witnesses.Bootstrap() {
		rows.add(
			bootstrapWitnessShape,
			witness.Signature,
			witness.PublicKey,
			witness.ChainCode,
			witness.Attributes,
			transactionID,
			models.KeyWitnessTypeBootstrap,
		)
	}
	if err := storeWitnessScripts(
		ctx, db, rows, transactionID,
		uint8(lcommon.ScriptRefTypeNativeScript),
		witnesses.NativeScripts(), slot,
	); err != nil {
		return err
	}
	if err := storeWitnessScripts(
		ctx, db, rows, transactionID,
		uint8(lcommon.ScriptRefTypePlutusV1),
		witnesses.PlutusV1Scripts(), slot,
	); err != nil {
		return err
	}
	if err := storeWitnessScripts(
		ctx, db, rows, transactionID,
		uint8(lcommon.ScriptRefTypePlutusV2),
		witnesses.PlutusV2Scripts(), slot,
	); err != nil {
		return err
	}
	if err := storeWitnessScripts(
		ctx, db, rows, transactionID,
		uint8(lcommon.ScriptRefTypePlutusV3),
		witnesses.PlutusV3Scripts(), slot,
	); err != nil {
		return err
	}
	if transaction.IsValid() {
		for _, datum := range witnesses.PlutusData() {
			rows.add(
				plutusDataShape,
				datum.Cbor(),
				transactionID,
			)
		}
	}
	if witnesses.Redeemers() != nil {
		for key, value := range witnesses.Redeemers().Iter() {
			rows.add(
				redeemerShape,
				value.Data.Cbor(),
				transactionID,
				uint64(max(0, value.ExUnits.Memory)),
				uint64(max(0, value.ExUnits.Steps)),
				key.Index,
				uint8(key.Tag),
			)
		}
	}
	return nil
}

func storeWitnessScripts[T lcommon.Script](
	ctx context.Context,
	db queryer,
	rows *rowBatch,
	transactionID int64,
	scriptType uint8,
	scripts []T,
	slot uint64,
) error {
	for _, script := range scripts {
		hash := script.Hash().Bytes()
		rows.add(
			witnessScriptShape,
			hash,
			transactionID,
			scriptType,
		)
		if _, err := db.ExecContext(ctx, `
INSERT INTO script (hash, content, created_slot, type)
VALUES (?, ?, ?, ?)
ON CONFLICT (hash) DO NOTHING`,
			hash,
			script.RawScriptBytes(),
			slot,
			scriptType,
		); err != nil {
			return fmt.Errorf("create script content: %w", err)
		}
	}
	return nil
}

func storeTransactionDatumIndex(
	rows *rowBatch,
	transaction lcommon.Transaction,
	slot uint64,
) error {
	for _, output := range transaction.Produced() {
		if err := storeDatumIndexRow(rows, output.Output.Datum(), slot); err != nil {
			return err
		}
	}
	witnesses := transaction.Witnesses()
	if witnesses == nil || !transaction.IsValid() {
		return nil
	}
	for _, datum := range witnesses.PlutusData() {
		copy := datum
		if err := storeDatumIndexRow(rows, &copy, slot); err != nil {
			return err
		}
	}
	return nil
}

func storeDatumIndexRow(
	rows *rowBatch,
	datum *lcommon.Datum,
	slot uint64,
) error {
	if datum == nil {
		return nil
	}
	raw := datum.Cbor()
	if len(raw) == 0 {
		var err error
		raw, err = datum.MarshalCBOR()
		if err != nil {
			return fmt.Errorf("marshal datum: %w", err)
		}
	}
	if len(raw) == 0 {
		return nil
	}
	hash := lcommon.Blake2b256Hash(raw)
	rows.add(datumShape, hash.Bytes(), raw, slot)
	return nil
}
