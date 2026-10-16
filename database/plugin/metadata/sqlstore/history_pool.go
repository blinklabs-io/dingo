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
	"database/sql"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

func (s *Store) GetPoolHistory(
	poolKeyHash []byte,
	throughLabel uint64,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.PoolHistoryRow, int, error) {
	ret := []models.PoolHistoryRow{}
	if len(poolKeyHash) == 0 {
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
	// reward_pool_output is unique per (epoch, pool), so the join never
	// multiplies a snapshot row and the count can share the FROM clause.
	const from = ` FROM pool_stake_snapshot s
LEFT JOIN reward_pool_output o
       ON o.epoch = s.epoch AND o.pool_key_hash = s.pool_key_hash
WHERE s.pool_key_hash = ? AND s.snapshot_type = 'mark' AND s.epoch < ?`
	args := []any{poolKeyHash, through}
	var total int
	if err := db.QueryRowContext(
		ctx, "SELECT COUNT(*)"+from, args...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count pool history: %w", err)
	}
	if total == 0 {
		return ret, 0, nil
	}
	query, queryArgs := addLimitOffset(
		`SELECT s.epoch, s.total_stake, s.delegator_count,
       (SELECT es.total_active_stake FROM epoch_summary es
        WHERE es.epoch = s.epoch AND es.snapshot_ready = TRUE),
       o.member_reward_total, o.leader_reward`+from+
			" ORDER BY s.epoch "+orderDirectionFromString(order),
		args, limit, offset,
	)
	rows, err := db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query pool history: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row models.PoolHistoryRow
		var stake string
		var totalActive, member, leader sql.NullString
		if err := rows.Scan(
			&row.Epoch, &stake, &row.DelegatorCount,
			&totalActive, &member, &leader,
		); err != nil {
			return nil, 0, err
		}
		if row.Stake, err = parseUint64("pool history stake", stake); err != nil {
			return nil, 0, err
		}
		if totalActive.Valid {
			row.TotalActiveStakeKnown = true
			if row.TotalActiveStake, err = parseUint64(
				"pool history total active stake", totalActive.String,
			); err != nil {
				return nil, 0, err
			}
		}
		if member.Valid && leader.Valid {
			row.RewardsCalculated = true
			if row.MemberRewards, err = parseUint64(
				"pool history member rewards", member.String,
			); err != nil {
				return nil, 0, err
			}
			if row.LeaderRewards, err = parseUint64(
				"pool history leader rewards", leader.String,
			); err != nil {
				return nil, 0, err
			}
		}
		ret = append(ret, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return ret, total, nil
}

func (s *Store) GetPoolDelegators(
	poolKeyHash []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.PoolDelegatorRow, int, error) {
	ret := []models.PoolDelegatorRow{}
	if len(poolKeyHash) == 0 {
		return ret, 0, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	where := " FROM reward_live_stake WHERE pool_key_hash = ? AND registered = TRUE"
	args := []any{poolKeyHash}
	var total int
	if err := db.QueryRowContext(
		ctx, "SELECT COUNT(*)"+where, args...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count pool delegators: %w", err)
	}
	if total == 0 {
		return ret, 0, nil
	}
	dir := orderDirectionFromString(order)
	query, queryArgs := addLimitOffset(
		"SELECT credential_tag, staking_key, total_stake"+where+
			" ORDER BY pool_delegation_slot "+dir+
			", pool_delegation_block_index "+dir+
			", pool_delegation_cert_index "+dir+
			", credential_tag "+dir+", staking_key "+dir,
		args, limit, offset,
	)
	rows, err := db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query pool delegators: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row models.PoolDelegatorRow
		var stake string
		if err := rows.Scan(&row.CredentialTag, &row.StakingKey, &stake); err != nil {
			return nil, 0, err
		}
		if row.Stake, err = parseUint64("pool delegator stake", stake); err != nil {
			return nil, 0, err
		}
		ret = append(ret, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return ret, total, nil
}

const poolCertificateEventsQuery = `
SELECT item.added_slot AS added_slot, tx.block_index AS block_index,
       c.cert_index AS cert_index, tx.hash AS tx_hash, ? AS action
FROM pool_registration item
JOIN certs c ON c.id = item.certificate_id
JOIN "transaction" tx ON tx.id = c.transaction_id
WHERE item.pool_key_hash = ?
UNION ALL
SELECT item.added_slot, tx.block_index, c.cert_index, tx.hash, ?
FROM pool_retirement item
JOIN certs c ON c.id = item.certificate_id
JOIN "transaction" tx ON tx.id = c.transaction_id
WHERE item.pool_key_hash = ?`

func (s *Store) GetPoolCertificateEvents(
	poolKeyHash []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.PoolCertificateEvent, int, error) {
	ret := []models.PoolCertificateEvent{}
	if len(poolKeyHash) == 0 {
		return ret, 0, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	args := []any{"registered", poolKeyHash, "deregistered", poolKeyHash}
	var total int
	if err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM ("+poolCertificateEventsQuery+") events",
		args...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count pool certificate events: %w", err)
	}
	if total == 0 {
		return ret, 0, nil
	}
	dir := orderDirectionFromString(order)
	query, queryArgs := addLimitOffset(
		poolCertificateEventsQuery+" ORDER BY added_slot "+dir+
			", block_index "+dir+", cert_index "+dir+", tx_hash "+dir,
		args, limit, offset,
	)
	rows, err := db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query pool certificate events: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var event models.PoolCertificateEvent
		var addedSlot, blockIndex int64
		if err := rows.Scan(
			&addedSlot, &blockIndex, &event.CertIndex, &event.TxHash, &event.Action,
		); err != nil {
			return nil, 0, err
		}
		ret = append(ret, event)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return ret, total, nil
}
