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

package mysql

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// GetPoolsRetiringAtEpoch returns the pools whose effective retirement takes
// effect at the given epoch as of the boundary slot, together with the reward
// account and deposit from their active registration. A retirement is
// "effective" when it is the latest pool certificate (registration or
// retirement) before the boundary — i.e. it was not cancelled by a later
// re-registration. Same-slot ordering uses block_index then cert_index, the
// same disambiguation GetActivePoolKeyHashesAtSlot uses.
func (d *MetadataStoreMysql) GetPoolsRetiringAtEpoch(
	epoch uint64,
	boundarySlot uint64,
	txn types.Txn,
) ([]models.PoolRetirementRefund, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf("GetPoolsRetiringAtEpoch: resolve db: %w", err)
	}

	type row struct {
		PoolKeyHash   []byte
		RewardAccount []byte
		DepositAmount types.Uint64
	}
	var rows []row

	query := `
		WITH latest_reg AS (
			SELECT pr.pool_id, pr.added_slot, pr.reward_account, pr.deposit_amount,
				COALESCE(t.block_index, 0) as blk_idx,
				COALESCE(c.cert_index, 0) as cert_idx,
				ROW_NUMBER() OVER (
					PARTITION BY pr.pool_id
					ORDER BY pr.added_slot DESC, COALESCE(t.block_index, 0) DESC, COALESCE(c.cert_index, 0) DESC
				) as rn
			FROM pool_registration pr
			LEFT JOIN certs c ON c.id = pr.certificate_id
			LEFT JOIN ` + "`transaction`" + ` t ON t.id = c.transaction_id
			WHERE pr.added_slot < ?
		),
		latest_ret AS (
			SELECT rt.pool_id, rt.added_slot, rt.epoch,
				COALESCE(t.block_index, 0) as blk_idx,
				COALESCE(c.cert_index, 0) as cert_idx,
				ROW_NUMBER() OVER (
					PARTITION BY rt.pool_id
					ORDER BY rt.added_slot DESC, COALESCE(t.block_index, 0) DESC, COALESCE(c.cert_index, 0) DESC
				) as rn
			FROM pool_retirement rt
			LEFT JOIN certs c ON c.id = rt.certificate_id
			LEFT JOIN ` + "`transaction`" + ` t ON t.id = c.transaction_id
			WHERE rt.added_slot < ?
		)
		SELECT p.pool_key_hash, lr.reward_account, lr.deposit_amount
		FROM pool p
		INNER JOIN latest_reg lr ON lr.pool_id = p.id AND lr.rn = 1
		INNER JOIN latest_ret lrt ON lrt.pool_id = p.id AND lrt.rn = 1
		WHERE lrt.epoch = ?
			AND NOT (
				lrt.added_slot < lr.added_slot
				OR (lrt.added_slot = lr.added_slot AND lrt.blk_idx < lr.blk_idx)
				OR (lrt.added_slot = lr.added_slot AND lrt.blk_idx = lr.blk_idx AND lrt.cert_idx < lr.cert_idx)
			)`

	if err := db.Raw(
		query, boundarySlot, boundarySlot, epoch,
	).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf(
			"GetPoolsRetiringAtEpoch: query pools: %w", err,
		)
	}

	refunds := make([]models.PoolRetirementRefund, len(rows))
	for i, r := range rows {
		refunds[i] = models.PoolRetirementRefund{
			PoolKeyHash:   r.PoolKeyHash,
			RewardAccount: r.RewardAccount,
			DepositAmount: r.DepositAmount,
		}
	}
	return refunds, nil
}
