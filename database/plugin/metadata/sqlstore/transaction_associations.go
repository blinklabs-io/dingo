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
	"database/sql"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
)

func (s *Store) hydrateTransactionSlice(
	db queryer,
	transactions []models.Transaction,
) error {
	if len(transactions) == 0 {
		return nil
	}
	ids := make([]any, len(transactions))
	hashes := make([]any, len(transactions))
	for i := range transactions {
		ids[i] = transactions[i].ID
		hashes[i] = transactions[i].Hash
		transactions[i].Outputs = nil
		transactions[i].CollateralReturn = nil
		transactions[i].Inputs = nil
		transactions[i].Collateral = nil
		transactions[i].ReferenceInputs = nil
	}
	outputs, err := s.transactionUtxosBatch(db, "transaction_id", ids, func(u *models.Utxo) string {
		if u.TransactionID == nil {
			return ""
		}
		return strconv.FormatUint(uint64(*u.TransactionID), 10)
	})
	if err != nil {
		return err
	}
	returns, err := s.transactionUtxosBatch(db, "collateral_return_for_tx_id", ids, func(u *models.Utxo) string {
		if u.CollateralReturnForTxID == nil {
			return ""
		}
		return strconv.FormatUint(uint64(*u.CollateralReturnForTxID), 10)
	})
	if err != nil {
		return err
	}
	inputs, err := s.transactionUtxosBatch(db, "spent_at_tx_id", hashes, func(u *models.Utxo) string { return string(u.SpentAtTxId) })
	if err != nil {
		return err
	}
	collateral, err := s.transactionUtxosBatch(db, "collateral_by_tx_id", hashes, func(u *models.Utxo) string { return string(u.CollateralByTxId) })
	if err != nil {
		return err
	}
	references, err := s.referenceInputsBatch(db, hashes)
	if err != nil {
		return err
	}
	if err := s.loadUtxoAssetsBatch(db, outputs, returns, inputs, collateral, references); err != nil {
		return err
	}
	certs, err := s.loadTransactionCertificatesBatch(db, ids)
	if err != nil {
		return err
	}
	witnesses, err := s.loadTransactionKeyWitnessesBatch(db, ids)
	if err != nil {
		return err
	}
	scripts, err := s.loadTransactionWitnessScriptsBatch(db, ids)
	if err != nil {
		return err
	}
	redeemers, err := s.loadTransactionRedeemersBatch(db, ids)
	if err != nil {
		return err
	}
	plutus, err := s.loadTransactionPlutusDataBatch(db, ids)
	if err != nil {
		return err
	}
	for i := range transactions {
		key := strconv.FormatUint(uint64(transactions[i].ID), 10)
		transactions[i].Outputs = outputs[key]
		if got := returns[key]; len(got) > 0 {
			transactions[i].CollateralReturn = &got[0]
		}
		transactions[i].Inputs = inputs[string(transactions[i].Hash)]
		transactions[i].Collateral = collateral[string(transactions[i].Hash)]
		transactions[i].ReferenceInputs = references[string(transactions[i].Hash)]
		transactions[i].Certificates = certs[key]
		transactions[i].KeyWitnesses = witnesses[key]
		transactions[i].WitnessScripts = scripts[key]
		transactions[i].Redeemers = redeemers[key]
		transactions[i].PlutusData = plutus[key]
		if transactions[i].Outputs == nil {
			transactions[i].Outputs = []models.Utxo{}
		}
		if transactions[i].Inputs == nil {
			transactions[i].Inputs = []models.Utxo{}
		}
		if transactions[i].Collateral == nil {
			transactions[i].Collateral = []models.Utxo{}
		}
		if transactions[i].ReferenceInputs == nil {
			transactions[i].ReferenceInputs = []models.Utxo{}
		}
		if transactions[i].Certificates == nil {
			transactions[i].Certificates = []models.Certificate{}
		}
		if transactions[i].KeyWitnesses == nil {
			transactions[i].KeyWitnesses = []models.KeyWitness{}
		}
		if transactions[i].WitnessScripts == nil {
			transactions[i].WitnessScripts = []models.WitnessScripts{}
		}
		if transactions[i].Redeemers == nil {
			transactions[i].Redeemers = []models.Redeemer{}
		}
		if transactions[i].PlutusData == nil {
			transactions[i].PlutusData = []models.PlutusData{}
		}
	}
	return nil
}

// referenceInputsBatch reads the durable many-to-many reference-input edges.
// The legacy UTxO column remains populated for compatibility, but cannot
// represent two transactions referencing the same output.
func (s *Store) referenceInputsBatch(db queryer, hashes []any) (map[string][]models.Utxo, error) {
	result := make(map[string][]models.Utxo)
	if len(hashes) == 0 {
		return result, nil
	}
	for start := 0; start < len(hashes); start += s.dialect.ParameterLimit() {
		end := min(start+s.dialect.ParameterLimit(), len(hashes))
		query := `SELECT ` + qualifiedSQLiteUtxoColumns + `, r.transaction_hash
FROM utxo AS utxo
JOIN utxo_reference_input AS r ON r.utxo_id = utxo.id
WHERE r.transaction_hash IN (` + bindPlaceholders(end-start) + `) ORDER BY utxo.id`
		rows, err := db.QueryContext(context.Background(), s.dialect.Rebind(query), hashes[start:end]...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			row, err := scanSQLiteUtxoWithReference(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			item, err := utxoFromSQLite(row.utxo)
			if err != nil {
				rows.Close()
				return nil, err
			}
			result[string(row.reference)] = append(result[string(row.reference)], *item)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return result, nil
}

type sqliteUtxoReferenceRow struct {
	utxo      sqlitequery.Utxo
	reference []byte
}

func scanSQLiteUtxoWithReference(rows *sql.Rows) (sqliteUtxoReferenceRow, error) {
	var row sqliteUtxoReferenceRow
	if err := rows.Scan(
		&row.utxo.TransactionID,
		&row.utxo.CollateralReturnForTxID,
		&row.utxo.TxID,
		&row.utxo.PaymentKey,
		&row.utxo.StakingKey,
		&row.utxo.CredentialTag,
		&row.utxo.DatumHash,
		&row.utxo.SpentAtTxID,
		&row.utxo.ReferencedByTxID,
		&row.utxo.CollateralByTxID,
		&row.utxo.ID,
		&row.utxo.AddedSlot,
		&row.utxo.DeletedSlot,
		&row.utxo.Amount,
		&row.utxo.OutputIdx,
		&row.utxo.PaymentScript,
		&row.reference,
	); err != nil {
		return row, err
	}
	return row, nil
}

func (s *Store) transactionIDRows(db queryer, columns, table string, ids []any, fn func(*sql.Rows) error) error {
	if len(ids) == 0 {
		return nil
	}
	for start := 0; start < len(ids); start += s.dialect.ParameterLimit() {
		end := min(start+s.dialect.ParameterLimit(), len(ids))
		query := "SELECT " + columns + " FROM " + table + " WHERE transaction_id IN (" + bindPlaceholders(end-start) + ") ORDER BY id"
		rows, err := db.QueryContext(context.Background(), s.dialect.Rebind(query), ids[start:end]...)
		if err != nil {
			return err
		}
		err = fn(rows)
		rowsErr := rows.Err()
		closeErr := rows.Close()
		if err != nil {
			return err
		}
		if rowsErr != nil {
			return rowsErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func (s *Store) loadTransactionCertificatesBatch(db queryer, ids []any) (map[string][]models.Certificate, error) {
	ret := make(map[string][]models.Certificate)
	err := s.transactionIDRows(db, "block_hash, id, transaction_id, certificate_id, slot, cert_index, cert_type", "certs", ids, func(rows *sql.Rows) error {
		for rows.Next() {
			var item models.Certificate
			if err := rows.Scan(&item.BlockHash, &item.ID, &item.TransactionID, &item.CertificateID, &item.Slot, &item.CertIndex, &item.CertType); err != nil {
				return err
			}
			ret[strconv.FormatUint(uint64(item.TransactionID), 10)] = append(ret[strconv.FormatUint(uint64(item.TransactionID), 10)], item)
		}
		return rows.Err()
	})
	return ret, err
}

func (s *Store) loadTransactionKeyWitnessesBatch(db queryer, ids []any) (map[string][]models.KeyWitness, error) {
	ret := make(map[string][]models.KeyWitness)
	err := s.transactionIDRows(db, "vkey, signature, public_key, chain_code, attributes, id, transaction_id, type", "key_witness", ids, func(rows *sql.Rows) error {
		for rows.Next() {
			var item models.KeyWitness
			if err := rows.Scan(&item.Vkey, &item.Signature, &item.PublicKey, &item.ChainCode, &item.Attributes, &item.ID, &item.TransactionID, &item.Type); err != nil {
				return err
			}
			key := strconv.FormatUint(uint64(item.TransactionID), 10)
			ret[key] = append(ret[key], item)
		}
		return rows.Err()
	})
	return ret, err
}

func (s *Store) loadTransactionWitnessScriptsBatch(db queryer, ids []any) (map[string][]models.WitnessScripts, error) {
	ret := make(map[string][]models.WitnessScripts)
	err := s.transactionIDRows(db, "script_hash, id, transaction_id, type", "witness_scripts", ids, func(rows *sql.Rows) error {
		for rows.Next() {
			var item models.WitnessScripts
			if err := rows.Scan(&item.ScriptHash, &item.ID, &item.TransactionID, &item.Type); err != nil {
				return err
			}
			key := strconv.FormatUint(uint64(item.TransactionID), 10)
			ret[key] = append(ret[key], item)
		}
		return rows.Err()
	})
	return ret, err
}

func (s *Store) loadTransactionRedeemersBatch(db queryer, ids []any) (map[string][]models.Redeemer, error) {
	ret := make(map[string][]models.Redeemer)
	columns := "data, id, transaction_id, ex_units_memory, ex_units_cpu, " + s.dialect.QuoteIdentifier("index") + ", tag"
	err := s.transactionIDRows(db, columns, "redeemer", ids, func(rows *sql.Rows) error {
		for rows.Next() {
			var item models.Redeemer
			if err := rows.Scan(&item.Data, &item.ID, &item.TransactionID, &item.ExUnitsMemory, &item.ExUnitsCPU, &item.Index, &item.Tag); err != nil {
				return err
			}
			key := strconv.FormatUint(uint64(item.TransactionID), 10)
			ret[key] = append(ret[key], item)
		}
		return rows.Err()
	})
	return ret, err
}

func (s *Store) loadTransactionPlutusDataBatch(db queryer, ids []any) (map[string][]models.PlutusData, error) {
	ret := make(map[string][]models.PlutusData)
	err := s.transactionIDRows(db, "data, id, transaction_id", "plutus_data", ids, func(rows *sql.Rows) error {
		for rows.Next() {
			var item models.PlutusData
			if err := rows.Scan(&item.Data, &item.ID, &item.TransactionID); err != nil {
				return err
			}
			key := strconv.FormatUint(uint64(item.TransactionID), 10)
			ret[key] = append(ret[key], item)
		}
		return rows.Err()
	})
	return ret, err
}

func (s *Store) hydrateTransaction(
	db queryer,
	transaction *models.Transaction,
) error {
	items := []models.Transaction{*transaction}
	if err := s.hydrateTransactionSlice(db, items); err != nil {
		return err
	}
	*transaction = items[0]
	return nil
}

// transactionUtxosBatch loads all UTxO associations for a transaction slice
// in parameter-limited batches. The result key is either a transaction ID or
// hash, as selected by key.
func (s *Store) transactionUtxosBatch(db queryer, column string, args []any, key func(*models.Utxo) string) (map[string][]models.Utxo, error) {
	result := make(map[string][]models.Utxo)
	if len(args) == 0 {
		return result, nil
	}
	for start := 0; start < len(args); start += s.dialect.ParameterLimit() {
		end := min(start+s.dialect.ParameterLimit(), len(args))
		query := "SELECT " + sqliteUtxoColumns + " FROM utxo WHERE " + column + " IN (" + bindPlaceholders(end-start) + ") ORDER BY id"
		rows, err := db.QueryContext(context.Background(), s.dialect.Rebind(query), args[start:end]...)
		if err != nil {
			return nil, err
		}
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
			k := key(item)
			if k != "" {
				result[k] = append(result[k], *item)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return result, nil
}
