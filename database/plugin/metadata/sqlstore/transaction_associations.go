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

//nolint:sqlclosecheck // Cursors are explicitly closed before dependent queries.
package sqlstore

import (
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
)

func (s *Store) hydrateTransactionSlice(
	db queryer,
	transactions []models.Transaction,
) error {
	for i := range transactions {
		if err := s.hydrateTransaction(db, &transactions[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) hydrateTransaction(
	db queryer,
	transaction *models.Transaction,
) error {
	var err error
	transaction.Outputs, err = s.transactionUtxos(
		db,
		"transaction_id = ?",
		transaction.ID,
	)
	if err != nil {
		return err
	}
	collateralReturn, err := s.transactionUtxos(
		db,
		"collateral_return_for_tx_id = ?",
		transaction.ID,
	)
	if err != nil {
		return err
	}
	transaction.CollateralReturn = nil
	if len(collateralReturn) > 0 {
		transaction.CollateralReturn = &collateralReturn[0]
	}
	transaction.Inputs, err = s.transactionUtxos(
		db,
		"spent_at_tx_id = ?",
		transaction.Hash,
	)
	if err != nil {
		return err
	}
	transaction.Collateral, err = s.transactionUtxos(
		db,
		"collateral_by_tx_id = ?",
		transaction.Hash,
	)
	if err != nil {
		return err
	}
	transaction.ReferenceInputs, err = s.transactionUtxos(
		db,
		"referenced_by_tx_id = ?",
		transaction.Hash,
	)
	if err != nil {
		return err
	}
	if err := loadTransactionCertificates(db, transaction); err != nil {
		return err
	}
	if err := loadTransactionKeyWitnesses(db, transaction); err != nil {
		return err
	}
	if err := loadTransactionWitnessScripts(db, transaction); err != nil {
		return err
	}
	if err := s.loadTransactionRedeemers(db, transaction); err != nil {
		return err
	}
	return loadTransactionPlutusData(db, transaction)
}

func (s *Store) transactionUtxos(
	db queryer,
	predicate string,
	arg any,
) ([]models.Utxo, error) {
	rows, err := db.QueryContext(
		context.Background(),
		"SELECT "+sqliteUtxoColumns+
			" FROM utxo WHERE "+predicate+" ORDER BY id",
		arg,
	)
	if err != nil {
		return nil, err
	}
	ret := []models.Utxo{}
	for rows.Next() {
		row, err := scanSQLiteUtxo(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		item, err := utxoFromSQLite(row)
		if err != nil {
			rows.Close()
			return nil, err
		}
		ret = append(ret, *item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	pointers := make([]*models.Utxo, len(ret))
	for i := range ret {
		pointers[i] = &ret[i]
	}
	if err := s.loadUtxoAssets(db, pointers); err != nil {
		return nil, err
	}
	return ret, nil
}

func loadTransactionCertificates(
	db queryer,
	transaction *models.Transaction,
) error {
	rows, err := db.QueryContext(context.Background(), `
SELECT block_hash, id, transaction_id, certificate_id, slot,
       cert_index, cert_type
FROM certs WHERE transaction_id = ? ORDER BY id`,
		transaction.ID,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	transaction.Certificates = []models.Certificate{}
	for rows.Next() {
		var item models.Certificate
		if err := rows.Scan(
			&item.BlockHash,
			&item.ID,
			&item.TransactionID,
			&item.CertificateID,
			&item.Slot,
			&item.CertIndex,
			&item.CertType,
		); err != nil {
			return err
		}
		transaction.Certificates = append(transaction.Certificates, item)
	}
	return rows.Err()
}

func loadTransactionKeyWitnesses(
	db queryer,
	transaction *models.Transaction,
) error {
	rows, err := db.QueryContext(context.Background(), `
SELECT vkey, signature, public_key, chain_code, attributes, id,
       transaction_id, type
FROM key_witness WHERE transaction_id = ? ORDER BY id`,
		transaction.ID,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	transaction.KeyWitnesses = []models.KeyWitness{}
	for rows.Next() {
		var item models.KeyWitness
		if err := rows.Scan(
			&item.Vkey,
			&item.Signature,
			&item.PublicKey,
			&item.ChainCode,
			&item.Attributes,
			&item.ID,
			&item.TransactionID,
			&item.Type,
		); err != nil {
			return err
		}
		transaction.KeyWitnesses = append(transaction.KeyWitnesses, item)
	}
	return rows.Err()
}

func loadTransactionWitnessScripts(
	db queryer,
	transaction *models.Transaction,
) error {
	rows, err := db.QueryContext(context.Background(), `
SELECT script_hash, id, transaction_id, type
FROM witness_scripts WHERE transaction_id = ? ORDER BY id`,
		transaction.ID,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	transaction.WitnessScripts = []models.WitnessScripts{}
	for rows.Next() {
		var item models.WitnessScripts
		if err := rows.Scan(
			&item.ScriptHash,
			&item.ID,
			&item.TransactionID,
			&item.Type,
		); err != nil {
			return err
		}
		transaction.WitnessScripts = append(
			transaction.WitnessScripts,
			item,
		)
	}
	return rows.Err()
}

func (s *Store) loadTransactionRedeemers(
	db queryer,
	transaction *models.Transaction,
) error {
	rows, err := db.QueryContext(context.Background(), `
SELECT data, id, transaction_id, ex_units_memory, ex_units_cpu,
	       `+s.dialect.QuoteIdentifier("index")+`, tag
FROM redeemer WHERE transaction_id = ? ORDER BY id`,
		transaction.ID,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	transaction.Redeemers = []models.Redeemer{}
	for rows.Next() {
		var item models.Redeemer
		if err := rows.Scan(
			&item.Data,
			&item.ID,
			&item.TransactionID,
			&item.ExUnitsMemory,
			&item.ExUnitsCPU,
			&item.Index,
			&item.Tag,
		); err != nil {
			return err
		}
		transaction.Redeemers = append(transaction.Redeemers, item)
	}
	return rows.Err()
}

func loadTransactionPlutusData(
	db queryer,
	transaction *models.Transaction,
) error {
	rows, err := db.QueryContext(context.Background(), `
SELECT data, id, transaction_id
FROM plutus_data WHERE transaction_id = ? ORDER BY id`,
		transaction.ID,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	transaction.PlutusData = []models.PlutusData{}
	for rows.Next() {
		var item models.PlutusData
		if err := rows.Scan(
			&item.Data,
			&item.ID,
			&item.TransactionID,
		); err != nil {
			return err
		}
		transaction.PlutusData = append(transaction.PlutusData, item)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("load transaction Plutus data: %w", err)
	}
	return nil
}
