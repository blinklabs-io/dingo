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

package models

import (
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
)

// PoolStakeSnapshot captures aggregated pool stake at an epoch boundary.
// Used for leader election in Ouroboros Praos consensus.
type PoolStakeSnapshot struct {
	ID             uint         `gorm:"primarykey"`
	Epoch          uint64       `gorm:"uniqueIndex:idx_pool_stake_epoch_pool,priority:1;not null"`
	SnapshotType   string       `gorm:"type:varchar(4);uniqueIndex:idx_pool_stake_epoch_pool,priority:2;not null"` // "mark", "set", "go"
	PoolKeyHash    []byte       `gorm:"uniqueIndex:idx_pool_stake_epoch_pool,priority:3;size:28;not null"`
	TotalStake     types.Uint64 `gorm:"not null"`
	DelegatorCount uint64       `gorm:"not null"`
	CapturedSlot   uint64       `gorm:"not null"`
}

// TableName returns the table name
func (PoolStakeSnapshot) TableName() string {
	return "pool_stake_snapshot"
}

// EpochSummary captures network-wide aggregate statistics at epoch boundary.
type EpochSummary struct {
	ID               uint         `gorm:"primarykey"`
	Epoch            uint64       `gorm:"uniqueIndex;not null"`
	TotalActiveStake types.Uint64 `gorm:"not null"`
	TotalPoolCount   uint64       `gorm:"not null"`
	TotalDelegators  uint64       `gorm:"not null"`
	EpochNonce       []byte       `gorm:"size:32"`
	BoundarySlot     uint64       `gorm:"not null"`
	SnapshotReady    bool         `gorm:"not null;default:false"`
}

// TableName returns the table name
func (EpochSummary) TableName() string {
	return "epoch_summary"
}

// DedupePoolStakeSnapshots removes duplicate rows from the
// pool_stake_snapshot table so that the unique index
// idx_pool_stake_epoch_pool (epoch, snapshot_type, pool_key_hash)
// can be created safely by AutoMigrate. This must be called before
// AutoMigrate for PoolStakeSnapshot.
//
// The function is a no-op when the table does not exist or contains
// no duplicates.
func DedupePoolStakeSnapshots(db *gorm.DB, logger *slog.Logger) error {
	// Check whether the table exists. If not, AutoMigrate will
	// create it fresh with the index — no dedup needed.
	if !db.Migrator().HasTable(&PoolStakeSnapshot{}) {
		return nil
	}

	// Detect duplicate (epoch, snapshot_type, pool_key_hash) groups.
	type dupGroup struct {
		Epoch        uint64
		SnapshotType string
		PoolKeyHash  []byte
		Cnt          int64
	}
	var dups []dupGroup
	err := db.Raw(`
		SELECT epoch, snapshot_type, pool_key_hash, COUNT(*) AS cnt
		FROM pool_stake_snapshot
		GROUP BY epoch, snapshot_type, pool_key_hash
		HAVING COUNT(*) > 1
	`).Scan(&dups).Error
	if err != nil {
		return fmt.Errorf(
			"query duplicate pool_stake_snapshot groups: %w", err,
		)
	}
	if len(dups) == 0 {
		return nil
	}

	logger.Info(
		"deduplicating pool_stake_snapshot rows before creating unique index",
		"duplicate_groups", len(dups),
	)

	// For each duplicate group, SELECT the MAX(id) to keep into
	// memory, then DELETE by id. This avoids the MySQL error 1093
	// ("can't specify target table for update in FROM clause")
	// that occurs when a DELETE subquery references the same table.
	for _, d := range dups {
		var keepID uint
		err := db.Raw(`
			SELECT MAX(id) FROM pool_stake_snapshot
			WHERE epoch = ?
			  AND snapshot_type = ?
			  AND pool_key_hash = ?
		`, d.Epoch, d.SnapshotType, d.PoolKeyHash,
		).Scan(&keepID).Error
		if err != nil {
			return fmt.Errorf(
				"select max id for pool_stake_snapshot "+
					"(epoch=%d, type=%s): %w",
				d.Epoch, d.SnapshotType, err,
			)
		}
		err = db.Exec(`
			DELETE FROM pool_stake_snapshot
			WHERE epoch = ?
			  AND snapshot_type = ?
			  AND pool_key_hash = ?
			  AND id != ?
		`, d.Epoch, d.SnapshotType, d.PoolKeyHash, keepID,
		).Error
		if err != nil {
			return fmt.Errorf(
				"delete duplicate pool_stake_snapshot "+
					"(epoch=%d, type=%s): %w",
				d.Epoch, d.SnapshotType, err,
			)
		}
	}

	logger.Info("pool_stake_snapshot deduplication complete")
	return nil
}
