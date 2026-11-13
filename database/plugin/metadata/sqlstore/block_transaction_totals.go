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
	"math/big"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
)

var _ metadata.BlockTransactionTotalsStore = (*Store)(nil)

// A single projection keeps fees and outputs in one read snapshot. Summation
// stays in Go: SQLite SUM can overflow int64 and amounts can exceed uint64.
// The left join and correlated return lookup keep reads rooted at the block's
// transactions instead of allowing a join order that scans all output history.
const blockTransactionAmountRows = `-- name: ProjectBlockTransactionAmountRows
SELECT 0 AS is_output, tx.fee AS amount
FROM applied_transaction tx
WHERE tx.block_hash = ?
UNION ALL
SELECT 1 AS is_output, u.amount
FROM applied_transaction tx
LEFT JOIN utxo u ON u.tx_id = tx.tx_id AND u.collateral_return_for_tx_id IS NULL
WHERE tx.block_hash = ?
  AND (COALESCE(tx.valid, FALSE) OR NOT EXISTS (
    SELECT 1 FROM utxo cr WHERE cr.collateral_return_for_tx_id = tx.tx_id
  ))
UNION ALL
SELECT 1 AS is_output, (
  SELECT cr.amount FROM utxo cr WHERE cr.collateral_return_for_tx_id = tx.tx_id
) AS amount
FROM applied_transaction tx
WHERE tx.block_hash = ? AND NOT COALESCE(tx.valid, FALSE)`

// GetBlockTransactionTotals projects recorded fees and effective outputs.
func (s *Store) GetBlockTransactionTotals(
	blockHash []byte,
	txn types.Txn,
) (models.BlockTransactionTotals, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return models.BlockTransactionTotals{}, err
	}
	rows, err := db.QueryContext(
		ctx, s.dialect.Rebind(blockTransactionAmountRows),
		blockHash, blockHash, blockHash,
	)
	if err != nil {
		return models.BlockTransactionTotals{}, err
	}
	defer rows.Close()
	output, fees := new(big.Int), new(big.Int)
	var amount big.Int
	for rows.Next() {
		var isOutput int
		var value types.Uint64
		if err := rows.Scan(&isOutput, &value); err != nil {
			return models.BlockTransactionTotals{}, err
		}
		total := fees
		if isOutput == 1 {
			total = output
		}
		total.Add(total, amount.SetUint64(uint64(value)))
	}
	if err := rows.Err(); err != nil {
		return models.BlockTransactionTotals{}, err
	}
	return models.BlockTransactionTotals{
		Output: output.String(), Fees: fees.String(),
	}, nil
}
