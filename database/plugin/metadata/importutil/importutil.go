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

	"github.com/blinklabs-io/dingo/database/models"
	"gorm.io/gorm"
)

// RefetchChunkSize limits the number of tx_ids in a single
// WHERE IN clause to stay within database variable limits
// (e.g. SQLite's SQLITE_MAX_VARIABLE_NUMBER, PostgreSQL's
// 65535 parameter limit).
const RefetchChunkSize = 500

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
