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
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/labelcodec"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func (s *Store) applyTransactionMetadataLabels(
	db queryer,
	transactionID int64,
	slot uint64,
	labels []labelcodec.Entry,
) error {
	if s.storageMode != types.StorageModeAPI {
		return nil
	}
	for _, label := range labels {
		if _, err := db.ExecContext(context.Background(), `
INSERT INTO transaction_metadata_label (
    transaction_id, label, slot, cbor_value, json_value
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT (transaction_id, label) DO UPDATE SET
    slot = excluded.slot,
    cbor_value = excluded.cbor_value,
    json_value = excluded.json_value`,
			transactionID,
			decimalUint64(types.Uint64(label.Label)),
			slot,
			label.CborValue,
			label.JsonValue,
		); err != nil {
			return fmt.Errorf(
				"create transaction metadata label %d: %w",
				label.Label,
				err,
			)
		}
	}
	return nil
}

func (s *Store) applyTransactionAssetMintBurn(
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
		if _, err := db.ExecContext(context.Background(), `
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
	db queryer,
	transactionID int64,
	transaction lcommon.Transaction,
	slot uint64,
	index uint32,
	produced []models.Utxo,
) error {
	if s.storageMode != types.StorageModeAPI {
		return nil
	}
	hash := transaction.Hash().Bytes()
	if err := markTransactionUtxoReferences(
		db,
		transaction.Collateral(),
		"collateral_by_tx_id",
		hash,
	); err != nil {
		return fmt.Errorf("mark collateral inputs: %w", err)
	}
	if err := markTransactionUtxoReferences(
		db,
		transaction.ReferenceInputs(),
		"referenced_by_tx_id",
		hash,
	); err != nil {
		return fmt.Errorf("mark reference inputs: %w", err)
	}
	if err := indexTransactionAddresses(
		db,
		transactionID,
		transaction,
		slot,
		index,
		produced,
	); err != nil {
		return err
	}
	if err := storeTransactionWitnesses(
		db,
		transactionID,
		transaction,
		slot,
	); err != nil {
		return err
	}
	if err := storeTransactionDatumIndex(db, transaction, slot); err != nil {
		return err
	}
	return nil
}

func markTransactionUtxoReferences(
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
		if column == "referenced_by_tx_id" {
			if _, err := db.ExecContext(
				context.Background(),
				`INSERT INTO utxo_reference_input (utxo_id, transaction_hash)
SELECT u.id, ? FROM utxo AS u
WHERE u.tx_id = ? AND u.output_idx = ?
  AND NOT EXISTS (
      SELECT 1 FROM utxo_reference_input AS r
      WHERE r.utxo_id = u.id AND r.transaction_hash = ?
  )`,
				hash,
				input.Id().Bytes(),
				input.Index(),
				hash,
			); err != nil {
				return err
			}
		}
		query := "UPDATE utxo SET " + column +
			" = ? WHERE tx_id = ? AND output_idx = ?"
		if _, err := db.ExecContext(
			context.Background(),
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
	db queryer,
	transactionID int64,
	transaction lcommon.Transaction,
	slot uint64,
	index uint32,
	produced []models.Utxo,
) error {
	if _, err := db.ExecContext(context.Background(), `
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
	for _, input := range allInputs {
		var (
			payment []byte
			staking []byte
			tag     uint8
		)
		err := db.QueryRowContext(context.Background(), `
SELECT payment_key, credential_tag, staking_key
FROM utxo WHERE tx_id = ? AND output_idx = ?`,
			input.Id().Bytes(),
			input.Index(),
		).Scan(&payment, &tag, &staking)
		if err != nil {
			// Missing input history is valid during gap ingestion.
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return fmt.Errorf("lookup input address for transaction %d: %w", transactionID, err)
		}
		add(payment, tag, staking)
	}
	for address := range addresses {
		if _, err := db.ExecContext(context.Background(), `
INSERT INTO address_transaction (
    payment_key, staking_key, credential_tag, transaction_id, slot, tx_index
) VALUES (?, ?, ?, ?, ?, ?)`,
			[]byte(address.payment),
			[]byte(address.staking),
			address.tag,
			transactionID,
			slot,
			index,
		); err != nil {
			return fmt.Errorf("create address transaction: %w", err)
		}
	}
	return nil
}

func storeTransactionWitnesses(
	db queryer,
	transactionID int64,
	transaction lcommon.Transaction,
	slot uint64,
) error {
	for _, table := range []string{
		"key_witness",
		"witness_scripts",
		"redeemer",
		"plutus_data",
	} {
		if _, err := db.ExecContext(
			context.Background(),
			"DELETE FROM "+table+" WHERE transaction_id = ?",
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
		if _, err := db.ExecContext(context.Background(), `
INSERT INTO key_witness (
    vkey, signature, transaction_id, type
) VALUES (?, ?, ?, ?)`,
			witness.Vkey,
			witness.Signature,
			transactionID,
			models.KeyWitnessTypeVkey,
		); err != nil {
			return fmt.Errorf("create vkey witness: %w", err)
		}
	}
	for _, witness := range witnesses.Bootstrap() {
		if _, err := db.ExecContext(context.Background(), `
INSERT INTO key_witness (
    signature, public_key, chain_code, attributes, transaction_id, type
) VALUES (?, ?, ?, ?, ?, ?)`,
			witness.Signature,
			witness.PublicKey,
			witness.ChainCode,
			witness.Attributes,
			transactionID,
			models.KeyWitnessTypeBootstrap,
		); err != nil {
			return fmt.Errorf("create bootstrap witness: %w", err)
		}
	}
	if err := storeWitnessScripts(
		db,
		transactionID,
		uint8(lcommon.ScriptRefTypeNativeScript),
		witnesses.NativeScripts(),
		slot,
	); err != nil {
		return err
	}
	if err := storeWitnessScripts(
		db,
		transactionID,
		uint8(lcommon.ScriptRefTypePlutusV1),
		witnesses.PlutusV1Scripts(),
		slot,
	); err != nil {
		return err
	}
	if err := storeWitnessScripts(
		db,
		transactionID,
		uint8(lcommon.ScriptRefTypePlutusV2),
		witnesses.PlutusV2Scripts(),
		slot,
	); err != nil {
		return err
	}
	if err := storeWitnessScripts(
		db,
		transactionID,
		uint8(lcommon.ScriptRefTypePlutusV3),
		witnesses.PlutusV3Scripts(),
		slot,
	); err != nil {
		return err
	}
	if transaction.IsValid() {
		for _, datum := range witnesses.PlutusData() {
			if _, err := db.ExecContext(context.Background(), `
INSERT INTO plutus_data (data, transaction_id) VALUES (?, ?)`,
				datum.Cbor(),
				transactionID,
			); err != nil {
				return fmt.Errorf("create Plutus data: %w", err)
			}
		}
	}
	if witnesses.Redeemers() != nil {
		for key, value := range witnesses.Redeemers().Iter() {
			if _, err := db.ExecContext(context.Background(), `
INSERT INTO redeemer (
    data, transaction_id, ex_units_memory, ex_units_cpu, "index", tag
) VALUES (?, ?, ?, ?, ?, ?)`,
				value.Data.Cbor(),
				transactionID,
				uint64(max(0, value.ExUnits.Memory)),
				uint64(max(0, value.ExUnits.Steps)),
				key.Index,
				uint8(key.Tag),
			); err != nil {
				return fmt.Errorf("create redeemer: %w", err)
			}
		}
	}
	return nil
}

func storeWitnessScripts[T lcommon.Script](
	db queryer,
	transactionID int64,
	scriptType uint8,
	scripts []T,
	slot uint64,
) error {
	for _, script := range scripts {
		hash := script.Hash().Bytes()
		if _, err := db.ExecContext(context.Background(), `
INSERT INTO witness_scripts (script_hash, transaction_id, type)
VALUES (?, ?, ?)`,
			hash,
			transactionID,
			scriptType,
		); err != nil {
			return fmt.Errorf("create witness script: %w", err)
		}
		if _, err := db.ExecContext(context.Background(), `
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
	db queryer,
	transaction lcommon.Transaction,
	slot uint64,
) error {
	for _, output := range transaction.Produced() {
		if err := storeDatumIndexRow(db, output.Output.Datum(), slot); err != nil {
			return err
		}
	}
	witnesses := transaction.Witnesses()
	if witnesses == nil || !transaction.IsValid() {
		return nil
	}
	for _, datum := range witnesses.PlutusData() {
		copy := datum
		if err := storeDatumIndexRow(db, &copy, slot); err != nil {
			return err
		}
	}
	return nil
}

func storeDatumIndexRow(
	db queryer,
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
	if _, err := db.ExecContext(context.Background(), `
INSERT INTO datum (hash, raw_datum, added_slot)
VALUES (?, ?, ?)
ON CONFLICT (hash) DO NOTHING`,
		hash.Bytes(),
		raw,
		slot,
	); err != nil {
		return fmt.Errorf(
			"store datum %s: %w",
			hex.EncodeToString(hash.Bytes()),
			err,
		)
	}
	return nil
}
