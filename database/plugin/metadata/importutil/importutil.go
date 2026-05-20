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

// Package importutil provides shared helpers for metadata import
// operations across all database backends (sqlite, postgres, mysql).
package importutil

import (
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"gorm.io/gorm"
)

// RefetchChunkSize limits the number of tx_ids in a single
// WHERE IN clause to stay within database variable limits
// (e.g. SQLite's SQLITE_MAX_VARIABLE_NUMBER, PostgreSQL's
// 65535 parameter limit).
const RefetchChunkSize = 500

type utxoProvenanceUpdate struct {
	txID                    []byte
	transactionID           *uint
	collateralReturnForTxID *uint
	addedSlot               uint64
	outputIdx               uint32
}

// BatchRefetchUtxoIDs resolves IDs for UTxOs whose ID was not
// populated by GORM after an ON CONFLICT DO NOTHING insert. It
// replaces per-UTxO SELECT queries with chunked batch lookups.
func BatchRefetchUtxoIDs(
	db *gorm.DB,
	utxos []models.Utxo,
) error {
	// Collect unique tx_ids that need re-fetching
	txIdSet := make(map[string]struct{})
	for i := range utxos {
		if utxos[i].ID == 0 {
			txIdSet[string(utxos[i].TxId)] = struct{}{}
		}
	}
	if len(txIdSet) == 0 {
		return nil
	}
	txIds := make([][]byte, 0, len(txIdSet))
	for k := range txIdSet {
		txIds = append(txIds, []byte(k))
	}

	// Build lookup map: (tx_id, output_idx) -> ID
	type utxoKey struct {
		txId      string
		outputIdx uint32
	}
	lookup := make(map[utxoKey]uint, len(txIdSet))

	// Fetch in chunks to stay within variable limits
	for i := 0; i < len(txIds); i += RefetchChunkSize {
		end := min(i+RefetchChunkSize, len(txIds))
		chunk := txIds[i:end]
		var existing []models.Utxo
		if err := db.Select("id", "tx_id", "output_idx").
			Where("tx_id IN ?", chunk).
			Find(&existing).Error; err != nil {
			return fmt.Errorf(
				"querying existing UTxOs for batch re-fetch: %w",
				err,
			)
		}
		for _, e := range existing {
			lookup[utxoKey{
				string(e.TxId), e.OutputIdx,
			}] = e.ID
		}
	}

	// Map IDs back to the original slice
	var missing int
	for i := range utxos {
		if utxos[i].ID != 0 {
			continue
		}
		k := utxoKey{
			string(utxos[i].TxId),
			utxos[i].OutputIdx,
		}
		if id, ok := lookup[k]; ok {
			utxos[i].ID = id
		} else {
			missing++
		}
	}
	if missing > 0 {
		return fmt.Errorf(
			"%d UTxO(s) still have ID==0 after insert and "+
				"batch re-fetch",
			missing,
		)
	}
	return nil
}

// BatchHydrateUtxoProvenance attaches transaction provenance to UTxOs that
// were inserted earlier from a ledger-state snapshot. The chain backfill later
// replays the producing transaction with the same tx_id/output_idx pair, but
// the bulk import uses ON CONFLICT DO NOTHING, so conflicted rows need this
// second pass to gain their transaction foreign key and real added slot.
func BatchHydrateUtxoProvenance(
	db *gorm.DB,
	utxos []models.Utxo,
	chunkSize int,
) error {
	updates := make([]utxoProvenanceUpdate, 0, len(utxos))
	for i := range utxos {
		if utxos[i].TransactionID == nil &&
			utxos[i].CollateralReturnForTxID == nil {
			continue
		}
		updates = append(updates, utxoProvenanceUpdate{
			txID:                    utxos[i].TxId,
			outputIdx:               utxos[i].OutputIdx,
			transactionID:           utxos[i].TransactionID,
			collateralReturnForTxID: utxos[i].CollateralReturnForTxID,
			addedSlot:               utxos[i].AddedSlot,
		})
	}
	if len(updates) == 0 {
		return nil
	}
	if chunkSize <= 0 {
		chunkSize = RefetchChunkSize
	}

	for i := 0; i < len(updates); i += chunkSize {
		end := min(i+chunkSize, len(updates))
		if err := hydrateUtxoProvenanceChunk(
			db,
			updates[i:end],
		); err != nil {
			return err
		}
	}
	return nil
}

func hydrateUtxoProvenanceChunk(
	db *gorm.DB,
	updates []utxoProvenanceUpdate,
) error {
	txCases := make([]string, 0, len(updates))
	collateralCases := make([]string, 0, len(updates))
	slotCases := make([]string, 0, len(updates))
	where := make([]string, 0, len(updates))

	txArgs := make([]any, 0, len(updates)*3)
	collateralArgs := make([]any, 0, len(updates)*3)
	slotArgs := make([]any, 0, len(updates)*3)
	whereArgs := make([]any, 0, len(updates)*2)

	for i := range updates {
		u := updates[i]
		missingProvenance := make([]string, 0, 2)

		if u.transactionID != nil {
			missingProvenance = append(
				missingProvenance,
				"transaction_id IS NULL",
			)
			txCases = append(
				txCases,
				"WHEN tx_id = ? AND output_idx = ? AND transaction_id IS NULL THEN ?",
			)
			txArgs = append(txArgs, u.txID, u.outputIdx, *u.transactionID)
			slotCases = append(
				slotCases,
				"WHEN tx_id = ? AND output_idx = ? AND transaction_id IS NULL THEN ?",
			)
			slotArgs = append(slotArgs, u.txID, u.outputIdx, u.addedSlot)
		}
		if u.collateralReturnForTxID != nil {
			missingProvenance = append(
				missingProvenance,
				"collateral_return_for_tx_id IS NULL",
			)
			collateralCases = append(
				collateralCases,
				"WHEN tx_id = ? AND output_idx = ? AND collateral_return_for_tx_id IS NULL THEN ?",
			)
			collateralArgs = append(
				collateralArgs,
				u.txID,
				u.outputIdx,
				*u.collateralReturnForTxID,
			)
			slotCases = append(
				slotCases,
				"WHEN tx_id = ? AND output_idx = ? AND collateral_return_for_tx_id IS NULL THEN ?",
			)
			slotArgs = append(slotArgs, u.txID, u.outputIdx, u.addedSlot)
		}
		if len(missingProvenance) == 0 {
			continue
		}
		where = append(
			where,
			"(tx_id = ? AND output_idx = ? AND ("+
				strings.Join(missingProvenance, " OR ")+"))",
		)
		whereArgs = append(whereArgs, u.txID, u.outputIdx)
	}

	assignments := make([]string, 0, 3)
	args := make([]any, 0, len(updates)*8)
	if len(txCases) > 0 {
		assignments = append(
			assignments,
			"transaction_id = CASE "+
				strings.Join(txCases, " ")+" ELSE transaction_id END",
		)
		args = append(args, txArgs...)
	}
	if len(collateralCases) > 0 {
		assignments = append(
			assignments,
			"collateral_return_for_tx_id = CASE "+
				strings.Join(collateralCases, " ")+
				" ELSE collateral_return_for_tx_id END",
		)
		args = append(args, collateralArgs...)
	}
	if len(slotCases) > 0 {
		assignments = append(
			assignments,
			"added_slot = CASE "+
				strings.Join(slotCases, " ")+" ELSE added_slot END",
		)
		args = append(args, slotArgs...)
	}
	if len(assignments) == 0 {
		return nil
	}
	args = append(args, whereArgs...)

	result := db.Exec(
		"UPDATE utxo SET "+strings.Join(assignments, ", ")+
			" WHERE "+strings.Join(where, " OR "),
		args...,
	)
	if result.Error != nil {
		return fmt.Errorf("hydrating UTxO provenance: %w", result.Error)
	}
	return nil
}
