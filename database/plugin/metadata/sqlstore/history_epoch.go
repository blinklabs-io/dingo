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

//nolint:rowserrcheck,sqlclosecheck // Cursors are explicitly closed and close errors are propagated before dependent queries.
package sqlstore

import (
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// orderDirectionFromString normalizes a pagination order to an SQL keyword. Anything
// but "desc" sorts ascending, so a caller cannot inject a clause through it.
func orderDirectionFromString(order string) string {
	if strings.EqualFold(order, "desc") {
		return "DESC"
	}
	return "ASC"
}

// SumTransactionOutputsInSlotRange totals the lovelace each transaction in
// the slot range put on chain: the outputs of a valid transaction, or the
// collateral return of a phase-2 invalid one. A Leios closure transaction is
// stored at its certifying block's slot but counts at the slot it executes
// in (leios_transaction_context), as SumTransactionFeesInSlotRange counts its
// fee, so an epoch's outputs and fees cover the same transactions. Output flow
// has no supply bound: the same lovelace can be transferred repeatedly.
func (s *Store) SumTransactionOutputsInSlotRange(
	startSlot uint64,
	endSlot uint64,
	txn types.Txn,
) (*big.Int, error) {
	if endSlot < startSlot {
		return new(big.Int), nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	start, err := checkedInt64(startSlot)
	if err != nil {
		return nil, err
	}
	end, err := checkedInt64(endSlot)
	if err != nil {
		return nil, err
	}
	total := new(big.Int)
	for _, query := range []string{
		`-- name: SumValidTransactionOutputsInSlotRange :one
SELECT ` + s.wideSumExpr("u.amount") + `
FROM utxo u
JOIN "transaction" tx ON tx.id = u.transaction_id
LEFT JOIN leios_transaction_context lc ON lc.transaction_id = tx.id
WHERE COALESCE(lc.slot, tx.slot) >= ? AND COALESCE(lc.slot, tx.slot) <= ?
 AND tx.valid = TRUE`,
		`-- name: SumCollateralReturnsInSlotRange :one
SELECT ` + s.wideSumExpr("u.amount") + `
FROM utxo u
JOIN "transaction" tx ON tx.id = u.collateral_return_for_tx_id
LEFT JOIN leios_transaction_context lc ON lc.transaction_id = tx.id
WHERE COALESCE(lc.slot, tx.slot) >= ? AND COALESCE(lc.slot, tx.slot) <= ?
 AND tx.valid = FALSE`,
	} {
		value, err := s.sumWide(ctx, db, query, start, end)
		if err != nil {
			return nil, fmt.Errorf(
				"sum transaction outputs in slot range: %w",
				err,
			)
		}
		if value.Sign() < 0 {
			return nil, errors.New("transaction output sum is negative")
		}
		total.Add(total, value)
	}
	return total, nil
}

func (s *Store) GetBlockHashesInSlotRange(
	startSlot uint64,
	endSlot uint64,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([][]byte, int, error) {
	return s.blockHashesInSlotRange(
		"", nil, startSlot, endSlot, limit, offset, order, txn,
	)
}

func (s *Store) GetPoolBlockHashesInSlotRange(
	poolKeyHash []byte,
	startSlot uint64,
	endSlot uint64,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([][]byte, int, error) {
	if len(poolKeyHash) == 0 {
		return [][]byte{}, 0, nil
	}
	return s.blockHashesInSlotRange(
		"JOIN pool_opcert_sequence pos ON pos.slot = bn.slot AND pos.pool_key_hash = ?",
		[]any{poolKeyHash},
		startSlot,
		endSlot,
		limit,
		offset,
		order,
		txn,
	)
}

func (s *Store) GetEarliestNonCheckpointBlockSlot(
	fromSlot uint64,
	txn types.Txn,
) (uint64, bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, false, err
	}
	from, err := checkedInt64(fromSlot)
	if err != nil {
		return 0, false, err
	}
	var slot sql.NullInt64
	if err := db.QueryRowContext(
		ctx,
		"SELECT MIN(slot) FROM block_nonce WHERE slot >= ? AND is_checkpoint = FALSE",
		from,
	).Scan(&slot); err != nil {
		return 0, false, fmt.Errorf("earliest non-checkpoint block: %w", err)
	}
	if !slot.Valid {
		return 0, false, nil
	}
	ret, err := checkedUint64(slot.Int64)
	if err != nil {
		return 0, false, err
	}
	return ret, true, nil
}

func (s *Store) blockHashesInSlotRange(
	join string,
	joinArgs []any,
	startSlot uint64,
	endSlot uint64,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([][]byte, int, error) {
	ret := [][]byte{}
	if endSlot < startSlot {
		return ret, 0, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	start, err := checkedInt64(startSlot)
	if err != nil {
		return nil, 0, err
	}
	end, err := checkedInt64(endSlot)
	if err != nil {
		return nil, 0, err
	}
	from := " FROM block_nonce bn " + join +
		" WHERE bn.slot >= ? AND bn.slot <= ?"
	args := append(append([]any{}, joinArgs...), start, end)
	var total int
	if err := db.QueryRowContext(
		ctx, "SELECT COUNT(*)"+from, args...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count blocks in slot range: %w", err)
	}
	if total == 0 {
		return ret, 0, nil
	}
	dir := orderDirectionFromString(order)
	query, queryArgs := addLimitOffset(
		"SELECT bn.hash"+from+" ORDER BY bn.slot "+dir+", bn.id "+dir,
		args, limit, offset,
	)
	rows, err := db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query block hashes in slot range: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var hash []byte
		if err := rows.Scan(&hash); err != nil {
			return nil, 0, err
		}
		ret = append(ret, hash)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return ret, total, nil
}

func (s *Store) GetEpochStakes(
	epoch uint64,
	poolKeyHash []byte,
	limit int,
	offset int,
	txn types.Txn,
) ([]models.EpochStakeRow, int, error) {
	ret := []models.EpochStakeRow{}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	sqlEpoch, err := checkedInt64(epoch)
	if err != nil {
		return nil, 0, err
	}
	where := " FROM reward_stake_input WHERE epoch = ? AND registered = TRUE"
	args := []any{sqlEpoch}
	if len(poolKeyHash) > 0 {
		where += " AND pool_key_hash = ?"
		args = append(args, poolKeyHash)
	}
	var total int
	if err := db.QueryRowContext(
		ctx, "SELECT COUNT(*)"+where, args...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count epoch stakes: %w", err)
	}
	if total == 0 {
		return ret, 0, nil
	}
	query, queryArgs := addLimitOffset(
		"SELECT pool_key_hash, credential_tag, staking_key, stake"+where+
			" ORDER BY pool_key_hash ASC, credential_tag ASC, staking_key ASC",
		args, limit, offset,
	)
	rows, err := db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query epoch stakes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row models.EpochStakeRow
		var stake string
		if err := rows.Scan(
			&row.PoolKeyHash, &row.CredentialTag, &row.StakingKey, &stake,
		); err != nil {
			return nil, 0, err
		}
		row.Stake, err = parseUint64("epoch stake", stake)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return ret, total, nil
}
