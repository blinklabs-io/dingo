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
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// unspentGenesisUtxoQuery selects the row of one output that is still live: not
// spent, not marked deleted, not a collateral return, and referenced by no
// asset, reference input or collateral edge. The zero amount ties the row to
// a zero-coin output; core storage mode stores the amount as well.
const unspentGenesisUtxoQuery = `
SELECT u.id FROM utxo u
JOIN "transaction" t ON t.id = u.tx_id
WHERE t.hash = ? AND u.output_idx = ?
  AND u.amount = '0'
  AND u.spent_at_tx_id IS NULL
  AND COALESCE(u.deleted_slot, 0) = 0
  AND u.collateral_return_for_tx_id IS NULL
  AND NOT EXISTS (SELECT 1 FROM asset s WHERE s.utxo_id = u.id)
  AND NOT EXISTS (SELECT 1 FROM utxo_reference_edge e WHERE e.utxo_id = u.id)
  AND NOT EXISTS (SELECT 1 FROM utxo_collateral_edge e WHERE e.utxo_id = u.id)`

// unreferencedGenesisTransactionQuery selects the row of a genesis
// transaction, one applied at slot 0, that no output, spend or edge refers
// to.
const unreferencedGenesisTransactionQuery = `
SELECT t.id FROM "transaction" t
WHERE t.hash = ?
  AND EXISTS (
      SELECT 1 FROM applied_transaction a WHERE a.tx_id = t.id AND a.slot = 0)
  AND NOT EXISTS (SELECT 1 FROM utxo x WHERE x.tx_id = t.id)
  AND NOT EXISTS (SELECT 1 FROM utxo x WHERE x.spent_at_tx_id = t.id)
  AND NOT EXISTS (
      SELECT 1 FROM utxo x WHERE x.collateral_return_for_tx_id = t.id)
  AND NOT EXISTS (SELECT 1 FROM utxo_reference_edge e WHERE e.tx_id = t.id)
  AND NOT EXISTS (SELECT 1 FROM utxo_collateral_edge e WHERE e.tx_id = t.id)`

// DeleteUnspentGenesisUtxos removes the zero-coin outputs named by refs that
// are unspent, with their address, pointer and reference-script rows, and then
// the genesis transactions named by txHashes that nothing refers to any more:
// their address-index, application and identity rows. It returns the
// references it removed and how many application rows went with the
// transactions. An output that is spent, referenced by a transaction or
// carrying an asset stays, and so does the transaction of an output that
// stays.
//
// A store that keeps its UTxO set outside the metadata store has no utxo
// rows, so only the genesis transaction rows go.
func (s *Store) DeleteUnspentGenesisUtxos(
	refs []models.UtxoId,
	txHashes [][]byte,
	txn types.Txn,
) ([]models.UtxoId, uint64, error) {
	var removed []models.UtxoId
	var applied uint64
	err := s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			removed = removed[:0]
			applied = 0
			for _, ref := range refs {
				id, found, err := queryOneID(
					ctx, db, unspentGenesisUtxoQuery, ref.Hash, int64(ref.Idx),
				)
				if err != nil {
					return fmt.Errorf("read genesis output: %w", err)
				}
				if !found {
					continue
				}
				for _, table := range []string{
					"utxo_address", "utxo_pointer", "utxo_reference_script",
				} {
					if _, err := db.ExecContext(
						ctx, "DELETE FROM "+table+" WHERE utxo_id = ?", id,
					); err != nil {
						return fmt.Errorf("delete %s: %w", table, err)
					}
				}
				if _, err := db.ExecContext(
					ctx, "DELETE FROM utxo WHERE id = ?", id,
				); err != nil {
					return fmt.Errorf("delete utxo: %w", err)
				}
				removed = append(removed, ref)
			}
			for _, hash := range txHashes {
				id, found, err := queryOneID(
					ctx, db, unreferencedGenesisTransactionQuery, hash,
				)
				if err != nil {
					return fmt.Errorf("read genesis transaction: %w", err)
				}
				if !found {
					continue
				}
				// The application row goes before the identity row it
				// references.
				if _, err := db.ExecContext(
					ctx,
					"DELETE FROM address_transaction WHERE transaction_id = ?",
					id,
				); err != nil {
					return fmt.Errorf("delete genesis transaction: %w", err)
				}
				result, err := db.ExecContext(
					ctx, "DELETE FROM applied_transaction WHERE tx_id = ?", id,
				)
				if err != nil {
					return fmt.Errorf("delete genesis transaction: %w", err)
				}
				n, err := result.RowsAffected()
				if err != nil {
					return fmt.Errorf("delete genesis transaction: %w", err)
				}
				applied += uint64(n) //nolint:gosec // a row count
				if _, err := db.ExecContext(
					ctx, `DELETE FROM "transaction" WHERE id = ?`, id,
				); err != nil {
					return fmt.Errorf("delete genesis transaction: %w", err)
				}
			}
			return nil
		},
	)
	if err != nil {
		return nil, 0, err
	}
	return removed, applied, nil
}

// queryOneID returns the first id the query selects.
func queryOneID(
	ctx context.Context,
	db queryer,
	query string,
	args ...any,
) (int64, bool, error) {
	var id int64
	err := db.QueryRowContext(ctx, query, args...).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}
