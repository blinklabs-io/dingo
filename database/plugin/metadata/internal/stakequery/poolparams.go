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

package stakequery

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"gorm.io/gorm"
)

// EffectivePoolRegistrationsForEpoch returns, per pool, the registration
// certificate whose parameters were in the ledger's pool-params map during
// the ended epoch spanning [epochStartSlot, snapshotSlot] — the params
// cardano-ledger's SNAP rule snapshots at the boundary ending that epoch.
//
// A re-registration by an already-registered pool is a future-params update:
// it promotes at the NEXT boundary (POOLREAP runs after SNAP), so a cert the
// pool submitted during the ended epoch must not be used. A fresh
// registration (pool absent from the params map: brand new, or registering
// again after an executed retirement) takes effect immediately, so for those
// pools the EARLIEST cert inside the epoch applies.
//
// Selection per pool: rank all registration certs plus executed-retirement
// candidates (retirement certs scheduled for an epoch at or before the ended
// epoch) that precede epochStartSlot by chain position. If the latest such
// event is a registration, it is the effective cert — any earlier retirement
// was either cancelled or followed by that fresh registration. If it is a
// retirement (or no event exists), the pool entered the params map during
// the ended epoch and its earliest in-epoch cert applies.
func EffectivePoolRegistrationsForEpoch(
	db *gorm.DB,
	pkhs [][]byte,
	epochStartSlot uint64,
	endedEpoch uint64,
	snapshotSlot uint64,
) ([]models.PoolRegistration, error) {
	if len(pkhs) == 0 {
		return []models.PoolRegistration{}, nil
	}
	txTable := transactionTableName(db)
	chunkSize := poolQueryChunkSize(db)
	// The pre-epoch event query binds the pool list twice (once for
	// registrations and once for retirements). SQLite's normal pool chunk is
	// sized for one IN clause, so halve it here to stay below the backend's
	// bind-variable limit. MySQL and Postgres use a larger driver-supported
	// limit and retain their normal chunk size.
	dialect := ""
	if db != nil {
		dialect = db.Name()
	}
	preEpochChunkSize := preEpochPoolQueryChunkSize(dialect, chunkSize)

	preEpochRegIDs := make([]uint, 0, len(pkhs))
	freshPools := make([][]byte, 0)
	lastEventQuery := fmt.Sprintf(`
WITH events AS (
	SELECT pr.pool_key_hash,
		pr.id AS reg_id,
		0 AS is_retirement,
		pr.added_slot,
		COALESCE(t.block_index, 0) AS bi,
		COALESCE(c.cert_index, 0) AS ci
	FROM pool_registration pr
	LEFT JOIN certs c ON c.id = pr.certificate_id
	LEFT JOIN %[1]s t ON t.id = c.transaction_id
	WHERE pr.pool_key_hash IN ? AND pr.added_slot < ?
	UNION ALL
	SELECT ret.pool_key_hash,
		0 AS reg_id,
		1 AS is_retirement,
		ret.added_slot,
		COALESCE(rt.block_index, 0) AS bi,
		COALESCE(rc.cert_index, 0) AS ci
	FROM pool_retirement ret
	LEFT JOIN certs rc ON rc.id = ret.certificate_id
	LEFT JOIN %[1]s rt ON rt.id = rc.transaction_id
	WHERE ret.pool_key_hash IN ? AND ret.added_slot < ? AND ret.epoch <= ?
), ranked AS (
	SELECT pool_key_hash, reg_id, is_retirement,
		ROW_NUMBER() OVER (
			PARTITION BY pool_key_hash
			ORDER BY added_slot DESC, bi DESC, ci DESC,
				is_retirement DESC, reg_id DESC
		) AS rn
	FROM events
)
SELECT pool_key_hash, reg_id, is_retirement FROM ranked WHERE rn = 1`,
		txTable,
	)
	for start := 0; start < len(pkhs); start += preEpochChunkSize {
		end := min(start+preEpochChunkSize, len(pkhs))
		chunk := pkhs[start:end]
		var rows []struct {
			PoolKeyHash  []byte
			RegID        uint
			IsRetirement int
		}
		if err := db.Raw(
			lastEventQuery,
			chunk, epochStartSlot,
			chunk, epochStartSlot, endedEpoch,
		).Scan(&rows).Error; err != nil {
			return nil, fmt.Errorf(
				"query pre-epoch pool cert events: %w", err,
			)
		}
		seen := make(map[string]struct{}, len(rows))
		for _, row := range rows {
			seen[string(row.PoolKeyHash)] = struct{}{}
			if row.IsRetirement == 0 && row.RegID != 0 {
				preEpochRegIDs = append(preEpochRegIDs, row.RegID)
			} else {
				freshPools = append(
					freshPools,
					append([]byte(nil), row.PoolKeyHash...),
				)
			}
		}
		for _, pkh := range chunk {
			if _, ok := seen[string(pkh)]; !ok {
				freshPools = append(freshPools, pkh)
			}
		}
	}

	earliestQuery := fmt.Sprintf(`
WITH ranked AS (
	SELECT pr.id, pr.pool_key_hash,
		ROW_NUMBER() OVER (
			PARTITION BY pr.pool_key_hash
			ORDER BY pr.added_slot ASC,
				COALESCE(t.block_index, 0) ASC,
				COALESCE(c.cert_index, 0) ASC,
				pr.id ASC
		) AS rn
	FROM pool_registration pr
	LEFT JOIN certs c ON c.id = pr.certificate_id
	LEFT JOIN %[1]s t ON t.id = c.transaction_id
	WHERE pr.pool_key_hash IN ? AND pr.added_slot >= ? AND pr.added_slot <= ?
)
SELECT id FROM ranked WHERE rn = 1`,
		txTable,
	)
	regIDs := preEpochRegIDs
	for start := 0; start < len(freshPools); start += chunkSize {
		end := min(start+chunkSize, len(freshPools))
		var ids []uint
		if err := db.Raw(
			earliestQuery,
			freshPools[start:end], epochStartSlot, snapshotSlot,
		).Scan(&ids).Error; err != nil {
			return nil, fmt.Errorf(
				"query in-epoch fresh pool registrations: %w", err,
			)
		}
		regIDs = append(regIDs, ids...)
	}

	registrations := make([]models.PoolRegistration, 0, len(regIDs))
	for start := 0; start < len(regIDs); start += chunkSize {
		end := min(start+chunkSize, len(regIDs))
		var chunkRegs []models.PoolRegistration
		if err := db.Preload("Owners").
			Where("id IN ?", regIDs[start:end]).
			Find(&chunkRegs).Error; err != nil {
			return nil, fmt.Errorf(
				"load effective pool registrations: %w", err,
			)
		}
		registrations = append(registrations, chunkRegs...)
	}
	return registrations, nil
}

func preEpochPoolQueryChunkSize(dialect string, chunkSize int) int {
	if dialect == "sqlite" {
		return max(1, chunkSize/2)
	}
	return chunkSize
}
