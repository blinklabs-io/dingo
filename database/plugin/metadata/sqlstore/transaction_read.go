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

//nolint:gosec,sqlclosecheck // SQL INTEGER mappings preserve the unsigned domain API; cursors are explicitly closed before dependent queries.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
)

const sqliteTransactionColumns = `hash, block_hash, metadata, slot, type, id,
fee, collateral_fee, ttl, block_index, valid`

func (s *Store) GetTransactionByHash(
	hash []byte,
	txn types.Txn,
) (*models.Transaction, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return nil, err
	}
	row, err := q.GetTransactionByHash(context.Background(), hash)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ret, err := transactionFromSQLite(row)
	if err != nil {
		return nil, err
	}
	if err := s.hydrateTransaction(db, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Store) GetTransactionSlotByHash(
	hash []byte,
	txn types.Txn,
) (uint64, bool, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, false, err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return 0, false, err
	}
	slot, err := q.GetTransactionSlotByHash(context.Background(), hash)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return uint64(slot.Int64), true, nil
}

func (s *Store) GetTransactionIDByHash(
	hash []byte,
	txn types.Txn,
) (uint, bool, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, false, err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return 0, false, err
	}
	id, err := q.GetTransactionIDByHash(context.Background(), hash)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return uint(id), true, nil
}

func (s *Store) GetTransactionMetadataByHash(
	hash []byte,
	txn types.Txn,
) ([]byte, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return nil, err
	}
	metadata, err := q.GetTransactionMetadataByHash(
		context.Background(),
		hash,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return metadata, err
}

func (s *Store) SumTransactionFeesInSlotRange(
	startSlot uint64,
	endSlot uint64,
	txn types.Txn,
) (uint64, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	start, err := checkedInt64(startSlot)
	if err != nil {
		return 0, err
	}
	end, err := checkedInt64(endSlot)
	if err != nil {
		return 0, err
	}
	total, err := sumUint64Rows(db, s.dialect.Rebind(`
SELECT CASE WHEN valid THEN fee ELSE collateral_fee END
FROM "transaction"
WHERE slot >= ? AND slot <= ?`), validInt64(start), validInt64(end))
	if err != nil {
		return 0, fmt.Errorf("sum transaction fees in slot range: %w", err)
	}
	return uint64(total), nil
}

func (s *Store) GetTransactionsByBlockHash(
	blockHash []byte,
	txn types.Txn,
) ([]models.Transaction, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return nil, err
	}
	rows, err := q.GetTransactionsByBlockHash(
		context.Background(),
		blockHash,
	)
	if err != nil {
		return nil, fmt.Errorf("get txs by block %x: %w", blockHash, err)
	}
	ret, err := transactionsFromSQLite(rows)
	if err != nil {
		return nil, err
	}
	if err := s.hydrateTransactionSlice(db, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Store) GetTransactionsByHashes(
	hashes [][]byte,
	txn types.Txn,
) ([]models.Transaction, error) {
	ret := []models.Transaction{}
	if len(hashes) == 0 {
		return ret, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	for start := 0; start < len(hashes); start += s.dialect.ParameterLimit() {
		end := min(start+s.dialect.ParameterLimit(), len(hashes))
		chunk := hashes[start:end]
		args := make([]any, len(chunk))
		for i := range chunk {
			args[i] = chunk[i]
		}
		rows, err := db.QueryContext(
			context.Background(),
			s.dialect.Rebind(
				`SELECT `+sqliteTransactionColumns+`
FROM "transaction" WHERE hash IN (`+bindPlaceholders(len(chunk))+`)`,
			),
			args...,
		)
		if err != nil {
			return nil, fmt.Errorf("get txs by hashes: %w", err)
		}
		for rows.Next() {
			row, err := scanSQLiteTransaction(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			item, err := transactionFromSQLite(row)
			if err != nil {
				rows.Close()
				return nil, err
			}
			ret = append(ret, *item)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	if err := s.hydrateTransactionSlice(db, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Store) GetTransactionHashesAfterSlot(
	slot uint64,
	txn types.Txn,
) ([][]byte, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return nil, err
	}
	value, err := checkedInt64(slot)
	if err != nil {
		return nil, err
	}
	ret, err := q.GetTransactionHashesAfterSlot(
		context.Background(),
		validInt64(value),
	)
	if err != nil {
		return nil, fmt.Errorf("query transaction hashes: %w", err)
	}
	return ret, nil
}

func (s *Store) CountTransactionsByPaymentCred(
	paymentKey []byte,
	txn types.Txn,
) (int, error) {
	if len(paymentKey) == 0 {
		return 0, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return 0, err
	}
	count, err := q.CountTransactionsByPaymentCred(
		context.Background(),
		paymentKey,
	)
	if err != nil {
		return 0, fmt.Errorf("count txs by payment cred: %w", err)
	}
	return int(count), nil
}

func (s *Store) CountTransactionsByMetadataLabel(
	label uint64,
	txn types.Txn,
) (int, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return 0, err
	}
	count, err := q.CountTransactionsByMetadataLabel(
		context.Background(),
		validString(strconv.FormatUint(label, 10)),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"count txs by metadata label %d: %w",
			label,
			err,
		)
	}
	return int(count), nil
}

func (s *Store) CountTransactionsInSlotRange(
	startSlot uint64,
	endSlot uint64,
	txn types.Txn,
) (int, error) {
	if endSlot < startSlot {
		return 0, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return 0, err
	}
	start, err := checkedInt64(startSlot)
	if err != nil {
		return 0, err
	}
	end, err := checkedInt64(endSlot)
	if err != nil {
		return 0, err
	}
	count, err := q.CountTransactionsInSlotRange(
		context.Background(),
		sqlitequery.CountTransactionsInSlotRangeParams{
			Slot:   sql.NullInt64{Int64: start, Valid: true},
			Slot_2: sql.NullInt64{Int64: end, Valid: true},
		},
	)
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

func (s *Store) GetBlockSlotRangeStats(
	startSlot uint64,
	endSlot uint64,
	txn types.Txn,
) (metadata.SlotRangeStats, error) {
	if endSlot < startSlot {
		return metadata.SlotRangeStats{}, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return metadata.SlotRangeStats{}, err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return metadata.SlotRangeStats{}, err
	}
	start, err := checkedInt64(startSlot)
	if err != nil {
		return metadata.SlotRangeStats{}, err
	}
	end, err := checkedInt64(endSlot)
	if err != nil {
		return metadata.SlotRangeStats{}, err
	}
	row, err := q.GetBlockSlotRangeStats(
		context.Background(),
		sqlitequery.GetBlockSlotRangeStatsParams{
			Slot:   sql.NullInt64{Int64: start, Valid: true},
			Slot_2: sql.NullInt64{Int64: end, Valid: true},
		},
	)
	if err != nil {
		return metadata.SlotRangeStats{}, err
	}
	if row.Count == 0 {
		return metadata.SlotRangeStats{}, nil
	}
	return metadata.SlotRangeStats{
		Count:     int(row.Count),
		FirstSlot: uint64(row.FirstSlot),
		LastSlot:  uint64(row.LastSlot),
	}, nil
}

func (s *Store) DeleteAddressTransactionsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return err
	}
	value, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	if err := q.DeleteAddressTransactionsAfterSlot(
		context.Background(),
		validInt64(value),
	); err != nil {
		return fmt.Errorf("delete address transactions after slot: %w", err)
	}
	return nil
}

func (s *Store) DeleteTransactionMetadataLabelsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := s.dbFromTxn(txn)
	if err != nil {
		return err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return err
	}
	value, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	if err := q.DeleteTransactionMetadataLabelsAfterSlot(
		context.Background(),
		validInt64(value),
	); err != nil {
		return fmt.Errorf(
			"delete transaction metadata labels after slot %d: %w",
			slot,
			err,
		)
	}
	return nil
}

func (s *Store) CountTransactionsByAddress(
	paymentKey []byte,
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (int, error) {
	predicate, args := addressTransactionPredicate(
		paymentKey,
		credentialTag,
		stakingKey,
	)
	if predicate == "" {
		return 0, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	var count int64
	err = db.QueryRowContext(
		context.Background(),
		s.dialect.Rebind(`
SELECT COUNT(DISTINCT transaction_id)
FROM address_transaction WHERE `+predicate),
		args...,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count txs by address: %w", err)
	}
	return int(count), nil
}

func (s *Store) GetTransactionsByAddress(
	paymentKey []byte,
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.Transaction, error) {
	ret := []models.Transaction{}
	predicate, args := addressTransactionPredicate(
		paymentKey,
		credentialTag,
		stakingKey,
	)
	if predicate == "" {
		return ret, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	direction := "DESC"
	if strings.EqualFold(order, "asc") {
		direction = "ASC"
	}
	query := `SELECT ` + sqliteTransactionColumns + `
FROM "transaction"
WHERE id IN (
    SELECT DISTINCT transaction_id FROM address_transaction WHERE ` +
		predicate + `
)
ORDER BY slot ` + direction + `, block_index ` + direction + `,
         id ` + direction
	query, args = appendLimitOffset(query, args, limit, offset)
	return s.queryTransactions(db, query, args)
}

func (s *Store) GetTransactionsByMetadataLabel(
	label uint64,
	limit int,
	offset int,
	descending bool,
	txn types.Txn,
) ([]models.Transaction, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	direction := "ASC"
	if descending {
		direction = "DESC"
	}
	query := `SELECT ` + sqliteTransactionColumns + `
FROM "transaction"
WHERE id IN (
    SELECT transaction_id FROM transaction_metadata_label WHERE label = ?
)
ORDER BY slot ` + direction + `, block_index ` + direction + `,
         id ` + direction
	args := []any{strconv.FormatUint(label, 10)}
	query, args = appendLimitOffset(query, args, limit, offset)
	ret, err := s.queryTransactions(db, query, args)
	if err != nil {
		return nil, fmt.Errorf("get txs by metadata label %d: %w", label, err)
	}
	return ret, nil
}

func (s *Store) GetAddressesByCredential(
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.AddressTransaction, error) {
	ret := []models.AddressTransaction{}
	if len(stakingKey) == 0 {
		return ret, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	direction := "ASC"
	if strings.EqualFold(order, "desc") {
		direction = "DESC"
	}
	query := `
SELECT MIN(id), payment_key, credential_tag, staking_key
FROM address_transaction
WHERE credential_tag = ? AND staking_key = ? AND LENGTH(payment_key) > 0
GROUP BY payment_key, credential_tag, staking_key
ORDER BY payment_key ` + direction
	args := []any{credentialTag, stakingKey}
	query, args = appendLimitOffset(query, args, limit, offset)
	rows, err := db.QueryContext(
		context.Background(),
		s.dialect.Rebind(query),
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get addresses by stake credential: %w",
			err,
		)
	}
	defer rows.Close()
	for rows.Next() {
		var row models.AddressTransaction
		var tag int64
		if err := rows.Scan(
			&row.ID,
			&row.PaymentKey,
			&tag,
			&row.StakingKey,
		); err != nil {
			return nil, err
		}
		row.CredentialTag = uint8(tag)
		ret = append(ret, row)
	}
	return ret, rows.Err()
}

func (s *Store) CountAddressesByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (int, error) {
	if len(stakingKey) == 0 {
		return 0, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	var count int64
	err = db.QueryRowContext(context.Background(), `
SELECT COUNT(DISTINCT payment_key)
FROM address_transaction
WHERE credential_tag = ? AND staking_key = ?
  AND LENGTH(payment_key) > 0`,
		credentialTag,
		stakingKey,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf(
			"count addresses by stake credential: %w",
			err,
		)
	}
	return int(count), nil
}

func (s *Store) GetAddressTransactionsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	from *models.AddressTransactionPosition,
	to *models.AddressTransactionPosition,
	txn types.Txn,
) ([]models.AccountTransactionAssociationRow, error) {
	ret := make([]models.AccountTransactionAssociationRow, 0)
	if len(stakingKey) == 0 {
		return ret, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	query, args := addressTransactionRangeQuery(
		credentialTag,
		stakingKey,
		from,
		to,
	)
	if strings.EqualFold(order, "asc") {
		query += " ORDER BY at.slot ASC, at.tx_index ASC, at.payment_key ASC"
	} else {
		query += " ORDER BY at.slot DESC, at.tx_index DESC, at.payment_key DESC"
	}
	query, args = appendLimitOffset(query, args, limit, offset)
	rows, err := db.QueryContext(
		context.Background(),
		s.dialect.Rebind(query),
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get address transactions by credential: %w",
			err,
		)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			row     models.AccountTransactionAssociationRow
			txSlot  int64
			txIndex int64
		)
		if err := rows.Scan(
			&row.PaymentKey,
			&row.TxHash,
			&txSlot,
			&txIndex,
			&row.BlockHash,
		); err != nil {
			return nil, err
		}
		row.TxSlot = uint64(txSlot)
		row.TxIndex = uint32(txIndex)
		ret = append(ret, row)
	}
	return ret, rows.Err()
}

func (s *Store) CountAddressTransactionsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	from *models.AddressTransactionPosition,
	to *models.AddressTransactionPosition,
	txn types.Txn,
) (int, error) {
	if len(stakingKey) == 0 {
		return 0, nil
	}
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	query, args := addressTransactionRangeQuery(
		credentialTag,
		stakingKey,
		from,
		to,
	)
	var count int
	err = db.QueryRowContext(
		context.Background(),
		s.dialect.Rebind(
			"SELECT COUNT(*) FROM ("+query+") address_transaction_range",
		),
		args...,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf(
			"count address transactions by credential: %w",
			err,
		)
	}
	return count, nil
}

func addressTransactionRangeQuery(
	credentialTag uint8,
	stakingKey []byte,
	from *models.AddressTransactionPosition,
	to *models.AddressTransactionPosition,
) (string, []any) {
	query := `
SELECT at.payment_key, tx.hash, at.slot, at.tx_index, tx.block_hash
FROM address_transaction at
JOIN "transaction" tx ON tx.id = at.transaction_id
WHERE at.credential_tag = ? AND at.staking_key = ?`
	args := []any{credentialTag, stakingKey}
	if from != nil {
		query += " AND (at.slot, at.tx_index) >= (?, ?)"
		args = append(args, from.Slot, from.TxIndex)
	}
	if to != nil {
		query += " AND (at.slot, at.tx_index) <= (?, ?)"
		args = append(args, to.Slot, to.TxIndex)
	}
	return query, args
}

func (s *Store) queryTransactions(
	db queryer,
	query string,
	args []any,
) ([]models.Transaction, error) {
	rows, err := db.QueryContext(
		context.Background(),
		s.dialect.Rebind(query),
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []models.Transaction{}
	for rows.Next() {
		row, err := scanSQLiteTransaction(rows)
		if err != nil {
			return nil, err
		}
		item, err := transactionFromSQLite(row)
		if err != nil {
			return nil, err
		}
		ret = append(ret, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := s.hydrateTransactionSlice(db, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func addressTransactionPredicate(
	paymentKey []byte,
	credentialTag uint8,
	stakingKey []byte,
) (string, []any) {
	switch {
	case len(paymentKey) > 0 && len(stakingKey) > 0:
		return "payment_key = ? AND credential_tag = ? AND staking_key = ?",
			[]any{paymentKey, credentialTag, stakingKey}
	case len(paymentKey) > 0:
		return "payment_key = ? AND (staking_key IS NULL OR LENGTH(staking_key) = 0)",
			[]any{
				paymentKey,
			}
	case len(stakingKey) > 0:
		return "credential_tag = ? AND staking_key = ?",
			[]any{credentialTag, stakingKey}
	default:
		return "", nil
	}
}

func appendLimitOffset(
	query string,
	args []any,
	limit int,
	offset int,
) (string, []any) {
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
		if offset > 0 {
			query += " OFFSET ?"
			args = append(args, offset)
		}
	} else if offset > 0 {
		query += " LIMIT -1 OFFSET ?"
		args = append(args, offset)
	}
	return query, args
}

func transactionsFromSQLite(
	rows []sqlitequery.Transaction,
) ([]models.Transaction, error) {
	ret := make([]models.Transaction, len(rows))
	for i := range rows {
		item, err := transactionFromSQLite(rows[i])
		if err != nil {
			return nil, err
		}
		ret[i] = *item
	}
	return ret, nil
}

func transactionFromSQLite(
	row sqlitequery.Transaction,
) (*models.Transaction, error) {
	fee, err := parseNullUint64("transaction fee", row.Fee)
	if err != nil {
		return nil, err
	}
	collateralFee, err := parseNullUint64(
		"transaction collateral fee",
		row.CollateralFee,
	)
	if err != nil {
		return nil, err
	}
	ttl, err := parseNullUint64("transaction ttl", row.Ttl)
	if err != nil {
		return nil, err
	}
	return &models.Transaction{
		PlutusData:      []models.PlutusData{},
		Certificates:    []models.Certificate{},
		Outputs:         []models.Utxo{},
		Hash:            row.Hash,
		Collateral:      []models.Utxo{},
		BlockHash:       row.BlockHash,
		KeyWitnesses:    []models.KeyWitness{},
		WitnessScripts:  []models.WitnessScripts{},
		Inputs:          []models.Utxo{},
		Redeemers:       []models.Redeemer{},
		ReferenceInputs: []models.Utxo{},
		Metadata:        row.Metadata,
		Slot:            uint64(row.Slot.Int64),
		Type:            int(row.Type.Int64),
		ID:              uint(row.ID),
		Fee:             types.Uint64(fee),
		CollateralFee:   types.Uint64(collateralFee),
		TTL:             types.Uint64(ttl),
		BlockIndex:      uint32(row.BlockIndex.Int64),
		Valid:           row.Valid.Bool,
	}, nil
}

func scanSQLiteTransaction(
	rows *sql.Rows,
) (sqlitequery.Transaction, error) {
	var row sqlitequery.Transaction
	err := rows.Scan(
		&row.Hash,
		&row.BlockHash,
		&row.Metadata,
		&row.Slot,
		&row.Type,
		&row.ID,
		&row.Fee,
		&row.CollateralFee,
		&row.Ttl,
		&row.BlockIndex,
		&row.Valid,
	)
	return row, err
}

func parseNullUint64(name string, value sql.NullString) (uint64, error) {
	if !value.Valid || value.String == "" {
		return 0, nil
	}
	return parseUint64(name, value.String)
}
