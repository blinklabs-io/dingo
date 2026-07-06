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

//go:build dingo_extra_plugins

package mysql

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm/clause"
)

// AddEndorserTransactions records the given hashes as endorser-block
// (speculative) transactions applied under the ranking block at rbSlot. See the
// EndorserTransaction model for why. Idempotent via ON CONFLICT DO NOTHING.
func (d *MetadataStoreMysql) AddEndorserTransactions(
	hashes [][]byte,
	rbSlot uint64,
	txn types.Txn,
) error {
	if len(hashes) == 0 {
		return nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	rows := make([]models.EndorserTransaction, 0, len(hashes))
	for _, h := range hashes {
		rows = append(rows, models.EndorserTransaction{Hash: h, RbSlot: rbSlot})
	}
	if result := db.Clauses(clause.OnConflict{DoNothing: true}).
		CreateInBatches(rows, batchChunkSize); result.Error != nil {
		return fmt.Errorf("record endorser transactions: %w", result.Error)
	}
	return nil
}

// FilterEndorserTransactions returns the subset of the given hashes that are
// recorded endorser-block transactions.
func (d *MetadataStoreMysql) FilterEndorserTransactions(
	hashes [][]byte,
	txn types.Txn,
) ([][]byte, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	var out [][]byte
	for i := 0; i < len(hashes); i += batchChunkSize {
		end := min(i+batchChunkSize, len(hashes))
		var part [][]byte
		if result := db.Model(&models.EndorserTransaction{}).
			Where("hash IN ?", hashes[i:end]).
			Pluck("hash", &part); result.Error != nil {
			return nil, fmt.Errorf(
				"filter endorser transactions: %w",
				result.Error,
			)
		}
		out = append(out, part...)
	}
	return out, nil
}

// UtxoSpenders returns the distinct hashes of transactions that spent any output
// produced by the given producer transaction hashes. Used to expand the
// endorser-on-endorser revoke cascade.
func (d *MetadataStoreMysql) UtxoSpenders(
	producerHashes [][]byte,
	txn types.Txn,
) ([][]byte, error) {
	if len(producerHashes) == 0 {
		return nil, nil
	}
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	var out [][]byte
	for i := 0; i < len(producerHashes); i += batchChunkSize {
		end := min(i+batchChunkSize, len(producerHashes))
		var part [][]byte
		if result := db.Model(&models.Utxo{}).
			Where(
				"tx_id IN ? AND spent_at_tx_id IS NOT NULL",
				producerHashes[i:end],
			).
			Distinct().
			Pluck("spent_at_tx_id", &part); result.Error != nil {
			return nil, fmt.Errorf("query utxo spenders: %w", result.Error)
		}
		for _, h := range part {
			if len(h) == 0 {
				continue
			}
			k := string(h)
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			out = append(out, h)
		}
	}
	return out, nil
}

// RevokeEndorserTransactions undoes the ledger effect of the given endorser-block
// transactions: it restores (un-spends) the inputs they consumed, clears the
// hash-keyed collateral/reference links, deletes their address-index rows, and
// deletes the transactions themselves (produced outputs and other ID-keyed
// children go via ON DELETE CASCADE) along with their endorser-transaction
// provenance rows. The caller must pass the complete revoke set (the transitive
// endorser-on-endorser closure); this method does not expand the cascade.
func (d *MetadataStoreMysql) RevokeEndorserTransactions(
	hashes [][]byte,
	txn types.Txn,
) error {
	if len(hashes) == 0 {
		return nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	for i := 0; i < len(hashes); i += batchChunkSize {
		end := min(i+batchChunkSize, len(hashes))
		chunk := hashes[i:end]

		// Restore the inputs these transactions consumed to the unspent state,
		// mirroring the rollback un-spend (DeleteTransactionsAfterSlot).
		if result := db.Model(&models.Utxo{}).
			Where("spent_at_tx_id IN ?", chunk).
			Updates(map[string]any{
				"spent_at_tx_id": nil,
				"deleted_slot":   0,
			}); result.Error != nil {
			return fmt.Errorf("revoke: restore spent inputs: %w", result.Error)
		}
		if result := db.Model(&models.Utxo{}).
			Where("collateral_by_tx_id IN ?", chunk).
			Update("collateral_by_tx_id", nil); result.Error != nil {
			return fmt.Errorf(
				"revoke: clear collateral references: %w",
				result.Error,
			)
		}
		if result := db.Model(&models.Utxo{}).
			Where("referenced_by_tx_id IN ?", chunk).
			Update("referenced_by_tx_id", nil); result.Error != nil {
			return fmt.Errorf(
				"revoke: clear reference-input references: %w",
				result.Error,
			)
		}

		// Delete address-transaction index rows for the revoked transactions.
		// These are keyed by transaction row ID, so the transaction delete's
		// ID-keyed cascade does not reach them; remove them explicitly.
		var txIDs []uint
		if result := db.Model(&models.Transaction{}).
			Where("hash IN ?", chunk).
			Pluck("id", &txIDs); result.Error != nil {
			return fmt.Errorf("revoke: lookup transaction ids: %w", result.Error)
		}
		if len(txIDs) > 0 {
			if result := db.Where("transaction_id IN ?", txIDs).
				Delete(&models.AddressTransaction{}); result.Error != nil {
				return fmt.Errorf(
					"revoke: delete address transactions: %w",
					result.Error,
				)
			}
			if result := db.Where("transaction_id IN ?", txIDs).
				Delete(&models.TransactionMetadataLabel{}); result.Error != nil {
				return fmt.Errorf(
					"revoke: delete transaction metadata labels: %w",
					result.Error,
				)
			}
		}

		// Delete the outputs these transactions produced explicitly (keyed by
		// producer hash), rather than relying only on the transaction delete's
		// ID-keyed cascade: leaving an endorser output behind would keep a
		// never-confirmed UTxO spendable, defeating the fix.
		if result := db.Where("tx_id IN ?", chunk).
			Delete(&models.Utxo{}); result.Error != nil {
			return fmt.Errorf("revoke: delete produced outputs: %w", result.Error)
		}

		// Delete the transactions; remaining ID-keyed children (certificates,
		// witnesses, redeemers, ...) are removed by the ON DELETE CASCADE
		// constraints.
		if result := db.Where("hash IN ?", chunk).
			Delete(&models.Transaction{}); result.Error != nil {
			return fmt.Errorf("revoke: delete transactions: %w", result.Error)
		}
		if result := db.Where("hash IN ?", chunk).
			Delete(&models.EndorserTransaction{}); result.Error != nil {
			return fmt.Errorf(
				"revoke: delete endorser transaction rows: %w",
				result.Error,
			)
		}
	}
	return nil
}

// DeleteEndorserTransactionsAfterSlot removes endorser-transaction provenance
// rows whose referencing ranking block is after the given slot, so a rollback
// drops the provenance for the spends it is undoing.
func (d *MetadataStoreMysql) DeleteEndorserTransactionsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if result := db.Where("rb_slot > ?", slot).
		Delete(&models.EndorserTransaction{}); result.Error != nil {
		return fmt.Errorf(
			"delete endorser transactions after slot %d: %w",
			slot,
			result.Error,
		)
	}
	return nil
}
