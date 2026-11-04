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

package database

import (
	"context"
	"math/big"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

// GetBlockTransactionTotals returns recorded fees and effective output amounts
// without requiring full transaction associations from supporting providers.
// Providers without the optional projection retain the hydrated read path.
func (d *Database) GetBlockTransactionTotals(
	ctx context.Context,
	blockHash []byte,
	txn *Txn,
) (models.BlockTransactionTotals, error) {
	if len(blockHash) == 0 {
		return models.BlockTransactionTotals{Output: "0", Fees: "0"}, nil
	}
	if txn == nil {
		txn = d.TransactionContext(ctx, false)
		defer txn.Release()
	}
	store := d.transactionStore()
	if projection, ok := store.(metadata.BlockTransactionTotalsStore); ok {
		return projection.GetBlockTransactionTotals(blockHash, txn.Metadata())
	}
	txs, err := store.GetTransactionsByBlockHash(blockHash, txn.Metadata())
	if err != nil {
		return models.BlockTransactionTotals{}, err
	}
	output, fees := new(big.Int), new(big.Int)
	var amount big.Int
	for _, tx := range txs {
		fees.Add(fees, amount.SetUint64(uint64(tx.Fee)))
		outputs := tx.Outputs
		if !tx.Valid && tx.CollateralReturn != nil {
			outputs = []models.Utxo{*tx.CollateralReturn}
		}
		for _, out := range outputs {
			output.Add(output, amount.SetUint64(uint64(out.Amount)))
		}
	}
	return models.BlockTransactionTotals{
		Output: output.String(), Fees: fees.String(),
	}, nil
}
