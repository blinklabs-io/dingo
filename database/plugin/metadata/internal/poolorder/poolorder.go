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

// Package poolorder centralizes the chain-ordered active-pool-key-hash
// query shared by the sqlite, postgres, and mysql metadata store backends,
// following the same rationale as poolcerthistory: the three backends
// differ only in how the "transaction" table is quoted
// (sqldialect.TransactionTableName), so keeping the query itself in one
// place prevents that single quoting difference from drifting into three
// independently-maintained copies.
package poolorder

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/sqldialect"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
)

// GetActivePoolKeyHashesOrdered returns the key hashes of pools active at
// slot, ordered oldest-first by each pool's EARLIEST registration
// certificate (added_slot ascending, then block_index and cert_index
// ascending to disambiguate certificates recorded in the same slot --
// block_index orders transactions within a block, cert_index orders
// certificates within a transaction). A final pool_key_hash tie-break
// makes the order fully deterministic even when two rows collapse to the
// same (added_slot, block_index, cert_index) key, e.g. registrations with
// no linked certificate/transaction (block_index/cert_index both default
// to 0 via COALESCE) such as those synthesized by the Mithril
// ledger-state import.
//
// "Oldest first" is deliberately keyed on each pool's FIRST on-chain
// registration, not its most recent parameter update: the Blockfrost
// pool_list schema documents order as "the ordering of items from the
// point of view of the blockchain... we return oldest first", which reads
// naturally as pool age (when it joined the chain), not freshness of its
// last registration certificate. A pool that re-registers years later to
// change its margin or relays keeps its original list position rather
// than jumping to the end of the list. This is a deliberate, reversible
// semantic choice, not one the schema pins -- it only says "oldest first"
// without defining "oldest" -- so a future change to key the sort on the
// LATEST registration instead would be a silent behavior reversal to
// evaluate on its own merits, not an obvious bug fix. Verified identical
// across sqlite, postgres, and mysql against the same fixture; see
// DATABASE.md's GetActivePoolKeyHashesOrdered section.
//
// A pool is considered active at slot under the same semantics as
// GetActivePoolKeyHashesAtSlot: it has a registration with added_slot <=
// slot, and either no retirement, or the latest retirement certificate
// precedes the latest registration certificate (a later re-registration
// cancels a pending retirement), or the retirement targets an epoch that
// had not started by slot. Determining the LATEST registration (for the
// active/retired comparison) requires a separate ranking from the
// FIRST registration (for ordering); both rankings are computed in a
// single reg_ranked CTE via two ROW_NUMBER window functions over the same
// joined rows, rather than scanning pool_registration/certs/transaction
// twice.
//
// Returns types.ErrNoEpochData (wrapped) if epoch data has not been
// synced for the requested slot, mirroring GetActivePoolKeyHashesAtSlot.
func GetActivePoolKeyHashesOrdered(
	db *gorm.DB,
	slot uint64,
) ([][]byte, error) {
	var epochAtSlot models.Epoch
	if res := db.Where(
		"start_slot <= ?",
		slot,
	).Order("start_slot DESC").First(&epochAtSlot); res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf(
				"GetActivePoolKeyHashesOrdered: %w",
				types.ErrNoEpochData,
			)
		}
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashesOrdered: get epoch at slot: %w",
			res.Error,
		)
	}

	// Verify the slot falls within the epoch's duration -- see
	// GetActivePoolKeyHashesAtSlot's identical check for the rationale
	// (a stale epoch ID could incorrectly treat retired pools as active).
	if slot >= epochAtSlot.StartSlot+uint64(epochAtSlot.LengthInSlots) {
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashesOrdered: %w",
			types.ErrNoEpochData,
		)
	}

	transactionTable := sqldialect.TransactionTableName(db)

	type poolResult struct {
		PoolKeyHash []byte
	}
	var results []poolResult

	query := fmt.Sprintf(`
		WITH reg_ranked AS (
			SELECT pr.pool_id, pr.added_slot,
				COALESCE(t.block_index, 0) AS blk_idx,
				COALESCE(c.cert_index, 0) AS cert_idx,
				ROW_NUMBER() OVER (
					PARTITION BY pr.pool_id
					ORDER BY pr.added_slot DESC,
						COALESCE(t.block_index, 0) DESC,
						COALESCE(c.cert_index, 0) DESC
				) AS rn_latest,
				ROW_NUMBER() OVER (
					PARTITION BY pr.pool_id
					ORDER BY pr.added_slot ASC,
						COALESCE(t.block_index, 0) ASC,
						COALESCE(c.cert_index, 0) ASC
				) AS rn_first
			FROM pool_registration pr
			LEFT JOIN certs c ON c.id = pr.certificate_id
			LEFT JOIN %[1]s t ON t.id = c.transaction_id
			WHERE pr.added_slot <= ?
		),
		latest_ret AS (
			SELECT rt.pool_id, rt.added_slot, rt.epoch,
				CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END AS synthetic_ret,
				COALESCE(t.block_index, 0) AS blk_idx,
				COALESCE(c.cert_index, 0) AS cert_idx,
				ROW_NUMBER() OVER (
					PARTITION BY rt.pool_id
					ORDER BY rt.added_slot DESC,
						CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END DESC,
						COALESCE(t.block_index, 0) DESC,
						COALESCE(c.cert_index, 0) DESC
				) AS rn
			FROM pool_retirement rt
			LEFT JOIN certs c ON c.id = rt.certificate_id
			LEFT JOIN %[1]s t ON t.id = c.transaction_id
			WHERE rt.added_slot <= ?
		)
		SELECT p.pool_key_hash
		FROM pool p
		INNER JOIN reg_ranked lr ON lr.pool_id = p.id AND lr.rn_latest = 1
		INNER JOIN reg_ranked fr ON fr.pool_id = p.id AND fr.rn_first = 1
		LEFT JOIN latest_ret lrt ON lrt.pool_id = p.id AND lrt.rn = 1
		WHERE lrt.pool_id IS NULL
			OR lrt.added_slot < lr.added_slot
			OR (lrt.added_slot = lr.added_slot AND lrt.synthetic_ret = 0 AND lrt.blk_idx < lr.blk_idx)
			OR (lrt.added_slot = lr.added_slot AND lrt.synthetic_ret = 0 AND lrt.blk_idx = lr.blk_idx AND lrt.cert_idx < lr.cert_idx)
			OR lrt.epoch > ?
		ORDER BY fr.added_slot ASC, fr.blk_idx ASC, fr.cert_idx ASC, p.pool_key_hash ASC`,
		transactionTable,
	)

	if err := db.Raw(query, slot, slot, epochAtSlot.EpochId).Scan(&results).Error; err != nil {
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashesOrdered: query pools: %w",
			err,
		)
	}

	poolKeyHashes := make([][]byte, len(results))
	for i, r := range results {
		poolKeyHashes[i] = r.PoolKeyHash
	}

	return poolKeyHashes, nil
}
