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

package importutil

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"gorm.io/gorm"
)

// poolRetirementBindVars is the number of bound columns per synthetic
// pool_retirement row inserted by RetirePools; it converts an IN-clause
// chunk size into a row count that stays within the same bind-var budget.
const poolRetirementBindVars = 5

// groupCredentialKeysByTag groups stake credential keys by credential tag so
// deactivation updates can run as one tag-scoped IN clause per tag.
func groupCredentialKeysByTag(
	creds []models.StakeCredentialRef,
) map[uint8][][]byte {
	ret := make(map[uint8][][]byte)
	for _, c := range creds {
		ret[c.Tag] = append(ret[c.Tag], c.Key)
	}
	return ret
}

// DeactivateAccounts marks the given accounts inactive, batching updates by
// credential tag in IN-clause chunks of chunkSize to stay within backend
// bind-variable limits.
func DeactivateAccounts(
	db *gorm.DB,
	creds []models.StakeCredentialRef,
	chunkSize int,
) error {
	for tag, keys := range groupCredentialKeysByTag(creds) {
		for start := 0; start < len(keys); start += chunkSize {
			end := min(start+chunkSize, len(keys))
			if res := db.
				Model(&models.Account{}).
				Where(
					"credential_tag = ? AND staking_key IN ? AND active = ?",
					tag, keys[start:end], true,
				).
				Update("active", false); res.Error != nil {
				return fmt.Errorf("DeactivateAccounts: %w", res.Error)
			}
		}
	}
	return nil
}

// DeactivateDreps marks the given DReps inactive, batching updates by
// credential tag in IN-clause chunks of chunkSize to stay within backend
// bind-variable limits.
func DeactivateDreps(
	db *gorm.DB,
	creds []models.StakeCredentialRef,
	chunkSize int,
) error {
	for tag, keys := range groupCredentialKeysByTag(creds) {
		for start := 0; start < len(keys); start += chunkSize {
			end := min(start+chunkSize, len(keys))
			if res := db.
				Model(&models.Drep{}).
				Where(
					"credential_tag = ? AND credential IN ? AND active = ?",
					tag, keys[start:end], true,
				).
				Update("active", false); res.Error != nil {
				return fmt.Errorf("DeactivateDreps: %w", res.Error)
			}
		}
	}
	return nil
}

// RetirePools records a synthetic retirement (certificate_id = 0) at the
// given epoch for each supplied pool key hash present in the pool table. Pool
// key hashes with no pool row are skipped. All lookups and inserts are
// chunked at chunkSize to stay within backend bind-variable limits.
//
// Pools that already carry an identical synthetic retirement (same epoch and
// added slot) are skipped: an interrupted catch-up retry recomputes the same
// stale pool set at the same tip, and pool_retirement has no unique index to
// reject the duplicate rows.
func RetirePools(
	db *gorm.DB,
	poolKeyHashes [][]byte,
	epoch uint64,
	addedSlot uint64,
	chunkSize int,
) error {
	type poolRow struct {
		ID          uint   `gorm:"column:id"`
		PoolKeyHash []byte `gorm:"column:pool_key_hash"`
	}
	var pools []poolRow
	for start := 0; start < len(poolKeyHashes); start += chunkSize {
		end := min(start+chunkSize, len(poolKeyHashes))
		var rows []poolRow
		if res := db.
			Model(&models.Pool{}).
			Select("id", "pool_key_hash").
			Where("pool_key_hash IN ?", poolKeyHashes[start:end]).
			Find(&rows); res.Error != nil {
			return fmt.Errorf("RetirePools: lookup pools: %w", res.Error)
		}
		pools = append(pools, rows...)
	}
	if len(pools) == 0 {
		return nil
	}

	poolIDs := make([]uint, 0, len(pools))
	for i := range pools {
		poolIDs = append(poolIDs, pools[i].ID)
	}
	existing := make(map[uint]struct{})
	for start := 0; start < len(poolIDs); start += chunkSize {
		end := min(start+chunkSize, len(poolIDs))
		var ids []uint
		if res := db.
			Model(&models.PoolRetirement{}).
			Where(
				"certificate_id = 0 AND epoch = ? AND added_slot = ? "+
					"AND pool_id IN ?",
				epoch, addedSlot, poolIDs[start:end],
			).
			Pluck("pool_id", &ids); res.Error != nil {
			return fmt.Errorf(
				"RetirePools: check existing retirements: %w", res.Error,
			)
		}
		for _, id := range ids {
			existing[id] = struct{}{}
		}
	}

	retirements := make([]models.PoolRetirement, 0, len(pools))
	queued := make(map[uint]struct{}, len(pools))
	for i := range pools {
		if _, ok := existing[pools[i].ID]; ok {
			continue
		}
		if _, ok := queued[pools[i].ID]; ok {
			continue
		}
		queued[pools[i].ID] = struct{}{}
		retirements = append(retirements, models.PoolRetirement{
			PoolID:      pools[i].ID,
			PoolKeyHash: pools[i].PoolKeyHash,
			Epoch:       epoch,
			AddedSlot:   addedSlot,
		})
	}
	if len(retirements) == 0 {
		return nil
	}
	if res := db.CreateInBatches(
		&retirements, max(1, chunkSize/poolRetirementBindVars),
	); res.Error != nil {
		return fmt.Errorf("RetirePools: create retirement: %w", res.Error)
	}
	return nil
}
