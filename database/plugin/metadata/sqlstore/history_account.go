// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//nolint:rowserrcheck,sqlclosecheck // Cursors are explicitly closed and close errors are propagated before dependent queries.
package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

func (s *Store) GetAccountStakeHistory(
	credentialTag uint8,
	stakingKey []byte,
	throughLabel uint64,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.AccountStakeHistoryRow, int, error) {
	ret := []models.AccountStakeHistoryRow{}
	if len(stakingKey) == 0 {
		return ret, 0, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	through, err := checkedInt64(throughLabel)
	if err != nil {
		return nil, 0, err
	}
	const where = ` FROM reward_stake_input
WHERE credential_tag = ? AND staking_key = ? AND registered = TRUE
  AND epoch < ?`
	args := []any{credentialTag, stakingKey, through}
	var total int
	if err := db.QueryRowContext(
		ctx, "SELECT COUNT(*)"+where, args...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count account stake history: %w", err)
	}
	if total == 0 {
		return ret, 0, nil
	}
	query, queryArgs := addLimitOffset(
		"SELECT epoch, pool_key_hash, stake"+where+
			" ORDER BY epoch "+orderDirectionFromString(order),
		args, limit, offset,
	)
	rows, err := db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query account stake history: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row models.AccountStakeHistoryRow
		var stake string
		if err := rows.Scan(&row.Epoch, &row.PoolKeyHash, &stake); err != nil {
			return nil, 0, err
		}
		if row.Stake, err = parseUint64("account stake history", stake); err != nil {
			return nil, 0, err
		}
		ret = append(ret, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return ret, total, nil
}

// GetEarliestRewardStakeInputEpoch returns the oldest snapshot label that
// still has per-credential stake rows. ok is false when none remain.
func (s *Store) GetEarliestRewardStakeInputEpoch(
	txn types.Txn,
) (epoch uint64, ok bool, err error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, false, err
	}
	var earliest sql.NullInt64
	if err := db.QueryRowContext(
		ctx, `SELECT MIN(epoch) FROM reward_stake_input`,
	).Scan(&earliest); err != nil {
		return 0, false, fmt.Errorf(
			"get earliest reward stake input epoch: %w",
			err,
		)
	}
	if !earliest.Valid || earliest.Int64 < 0 {
		return 0, false, nil
	}
	return uint64(earliest.Int64), true, nil
}

func (s *Store) GetAccountMIRHistory(
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.AccountMIRRow, int, error) {
	ret := []models.AccountMIRRow{}
	if len(stakingKey) == 0 {
		return ret, 0, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	const from = ` FROM move_instantaneous_rewards_reward reward
JOIN move_instantaneous_rewards mir ON mir.id = reward.mir_id
JOIN certs ON certs.id = mir.certificate_id
JOIN "transaction" tx ON tx.id = certs.transaction_id
WHERE reward.credential_tag = ? AND reward.credential = ?`
	args := []any{credentialTag, stakingKey}
	var total int
	if err := db.QueryRowContext(
		ctx, "SELECT COUNT(*)"+from, args...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count account MIR history: %w", err)
	}
	if total == 0 {
		return ret, 0, nil
	}
	dir := orderDirectionFromString(order)
	query, queryArgs := addLimitOffset(
		"SELECT tx.hash, reward.amount, tx.slot, tx.block_index, tx.block_hash"+
			from+" ORDER BY mir.added_slot "+dir+", tx.block_index "+dir+
			", certs.cert_index "+dir+", reward.id "+dir,
		args, limit, offset,
	)
	rows, err := db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query account MIR history: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row models.AccountMIRRow
		var raw any
		if err := rows.Scan(
			&row.TxHash, &raw, &row.TxSlot, &row.BlockIndex, &row.BlockHash,
		); err != nil {
			return nil, 0, err
		}
		text, err := signedAmountText(raw)
		if err != nil {
			return nil, 0, err
		}
		amount, ok := new(big.Int).SetString(text, 10)
		if !ok {
			return nil, 0, fmt.Errorf("decode account MIR amount %q", text)
		}
		row.Amount = amount
		ret = append(ret, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return ret, total, nil
}

// accountAssetRows streams the native-asset holdings of a credential's UTxOs,
// ordered by asset, and reports whether each UTxO has been spent. Only live
// UTxOs are read when liveOnly is set. More than maxRows holdings, when
// positive, fail with models.ErrAccountAggregateLimit. The cap applies to an
// inner query so the ordering sorts at most maxRows+1 rows.
func accountAssetRows(
	ctx context.Context,
	db queryer,
	credentialTag uint8,
	stakingKey []byte,
	liveOnly bool,
	order string,
	maxRows int,
	visit func(policyID, name []byte, quantity *big.Int, spent bool) error,
) error {
	query := `SELECT a.policy_id AS policy_id, a.name AS name,
a.amount AS amount, u.deleted_slot AS deleted_slot
FROM asset a
JOIN utxo u ON u.id = a.utxo_id
WHERE u.credential_tag = ? AND u.staking_key = ?`
	if liveOnly {
		query += " AND u.deleted_slot = 0"
	}
	args := []any{credentialTag, stakingKey}
	if maxRows > 0 {
		// OFFSET prevents SQLite from flattening the capped subquery and
		// sorting every matching holding before applying the limit.
		query += " LIMIT ? OFFSET 0"
		args = append(args, maxRows+1)
	}
	dir := orderDirectionFromString(order)
	query = "SELECT policy_id, name, amount, deleted_slot FROM (" + query +
		") capped ORDER BY policy_id " + dir + ", name " + dir
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	read := 0
	for rows.Next() {
		read++
		if maxRows > 0 && read > maxRows {
			return fmt.Errorf(
				"more than %d asset holdings: %w",
				maxRows,
				models.ErrAccountAggregateLimit,
			)
		}
		var policyID, name []byte
		var amount string
		var deletedSlot int64
		if err := rows.Scan(&policyID, &name, &amount, &deletedSlot); err != nil {
			return err
		}
		quantity, ok := new(big.Int).SetString(amount, 10)
		if !ok {
			return fmt.Errorf("decode asset amount %q", amount)
		}
		if err := visit(policyID, name, quantity, deletedSlot != 0); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *Store) GetAccountAssetBalances(
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	limits models.AccountAggregateLimits,
	txn types.Txn,
) ([]models.AssetQuantity, int, error) {
	ret := []models.AssetQuantity{}
	if len(stakingKey) == 0 {
		return ret, 0, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	// The cursor is ordered by asset, so equal assets are adjacent and one
	// pass folds them while holding only the requested page in memory.
	var (
		total   int
		current *models.AssetQuantity
	)
	flush := func() {
		if current == nil {
			return
		}
		index := total
		total++
		if index >= offset && (limit <= 0 || len(ret) < limit) {
			ret = append(ret, *current)
		}
		current = nil
	}
	err = accountAssetRows(
		ctx, db, credentialTag, stakingKey, true, order, limits.MaxAssetRows,
		func(policyID, name []byte, quantity *big.Int, _ bool) error {
			if current != nil &&
				bytes.Equal(current.PolicyID, policyID) &&
				bytes.Equal(current.Name, name) {
				current.Quantity.Add(current.Quantity, quantity)
				return nil
			}
			flush()
			if limits.MaxAssets > 0 && total >= limits.MaxAssets {
				return fmt.Errorf(
					"more than %d distinct assets: %w",
					limits.MaxAssets,
					models.ErrAccountAggregateLimit,
				)
			}
			current = &models.AssetQuantity{
				PolicyID: policyID,
				Name:     name,
				Quantity: new(big.Int).Set(quantity),
			}
			return nil
		},
	)
	if err != nil {
		return nil, 0, fmt.Errorf("query account asset balances: %w", err)
	}
	flush()
	return ret, total, nil
}

func (s *Store) GetAccountAddressTotals(
	credentialTag uint8,
	stakingKey []byte,
	limits models.AccountAggregateLimits,
	txn types.Txn,
) (models.AccountAddressTotals, error) {
	var ret models.AccountAddressTotals
	if len(stakingKey) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return ret, err
	}
	for _, total := range []struct {
		dest  **big.Int
		query string
	}{
		{&ret.ReceivedLovelace, `SELECT amount FROM utxo WHERE credential_tag = ? AND staking_key = ?`},
		{&ret.SentLovelace, `SELECT amount FROM utxo WHERE credential_tag = ? AND staking_key = ? AND deleted_slot <> 0`},
	} {
		value, err := sumSignedRowsBounded(
			ctx,
			db,
			total.query,
			limits.MaxUtxos,
			models.ErrAccountAggregateLimit,
			credentialTag,
			stakingKey,
		)
		if err != nil {
			return ret, fmt.Errorf("sum account lovelace totals: %w", err)
		}
		*total.dest = value
	}
	received := map[string]*models.AssetQuantity{}
	sent := map[string]*models.AssetQuantity{}
	add := func(into map[string]*models.AssetQuantity, policyID, name []byte, quantity *big.Int) error {
		key := string(policyID) + "\x00" + string(name)
		if held, ok := into[key]; ok {
			held.Quantity.Add(held.Quantity, quantity)
			return nil
		}
		if limits.MaxAssets > 0 && len(into) >= limits.MaxAssets {
			return fmt.Errorf(
				"more than %d distinct assets: %w",
				limits.MaxAssets,
				models.ErrAccountAggregateLimit,
			)
		}
		into[key] = &models.AssetQuantity{
			PolicyID: policyID,
			Name:     name,
			Quantity: new(big.Int).Set(quantity),
		}
		return nil
	}
	err = accountAssetRows(
		ctx, db, credentialTag, stakingKey, false, "asc", limits.MaxAssetRows,
		func(policyID, name []byte, quantity *big.Int, spent bool) error {
			if err := add(received, policyID, name, quantity); err != nil {
				return err
			}
			if spent {
				return add(sent, policyID, name, quantity)
			}
			return nil
		},
	)
	if err != nil {
		return ret, fmt.Errorf("sum account asset totals: %w", err)
	}
	ret.ReceivedAssets = sortedAssetQuantities(received)
	ret.SentAssets = sortedAssetQuantities(sent)
	// Each output the lovelace sums admitted is created by one transaction
	// and spent by at most one, so twice MaxUtxos covers those. A transaction
	// that only names an output as a reference or unspent collateral input is
	// recorded too and can push the count past it; that is refused, not
	// counted.
	maxTxs := 2 * limits.MaxUtxos
	countLimit := 0
	if maxTxs > 0 {
		countLimit = maxTxs + 1
	}
	txCount, err := countAccountTransactions(
		ctx, db, credentialTag, stakingKey, countLimit,
	)
	if err != nil {
		return ret, fmt.Errorf("count account transactions: %w", err)
	}
	if maxTxs > 0 && txCount > maxTxs {
		return ret, fmt.Errorf(
			"more than %d transactions: %w",
			maxTxs,
			models.ErrAccountAggregateLimit,
		)
	}
	ret.TxCount = txCount
	return ret, nil
}

// countAccountTransactions counts the distinct transactions recorded against
// a credential. A positive limit caps the distinct ids the database reads,
// so the result is then at most limit.
func countAccountTransactions(
	ctx context.Context,
	db queryer,
	credentialTag uint8,
	stakingKey []byte,
	limit int,
) (int, error) {
	query := `SELECT COUNT(*) FROM (SELECT DISTINCT transaction_id
FROM address_transaction
WHERE credential_tag = ? AND staking_key = ?`
	args := []any{credentialTag, stakingKey}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	query += ") distinct_transactions"
	var count int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func sortedAssetQuantities(
	held map[string]*models.AssetQuantity,
) []models.AssetQuantity {
	ret := make([]models.AssetQuantity, 0, len(held))
	for _, balance := range held {
		ret = append(ret, *balance)
	}
	slices.SortFunc(ret, func(a, b models.AssetQuantity) int {
		if c := bytes.Compare(a.PolicyID, b.PolicyID); c != 0 {
			return c
		}
		return bytes.Compare(a.Name, b.Name)
	})
	return ret
}
