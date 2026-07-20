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

// PoolRewardAccountAutoVote enumerates the CIP-1694 reward-account
// DRep-delegation outcomes that produce an implicit SPO vote when a
// pool did not cast an explicit vote on a proposal. Resolved and
// frozen at the epoch boundary that captures the stake snapshot so
// the governance tally uses snapshot-era state rather than live state.
const (
	// PoolRewardAccountAutoVoteNone means the pool's reward account is
	// unset, deregistered, or delegated to anything other than the
	// predefined Always{Abstain,NoConfidence} DRep options. Such a
	// pool contributes to SPOTotalStake only and counts as implicit
	// no under CIP-1694.
	PoolRewardAccountAutoVoteNone uint8 = 0
	// PoolRewardAccountAutoVoteAbstain means the pool's reward
	// account delegates to AlwaysAbstain at the snapshot epoch. Its
	// stake is bucketed into SPOAbstainStake (excluded from the
	// active denominator).
	PoolRewardAccountAutoVoteAbstain uint8 = 1
	// PoolRewardAccountAutoVoteNoConfidence means the pool's reward
	// account delegates to AlwaysNoConfidence at the snapshot epoch.
	// For NoConfidence actions the pool stake is bucketed Yes; for
	// any other action type it is bucketed No (mirrors the
	// AlwaysNoConfidence DRep handling).
	PoolRewardAccountAutoVoteNoConfidence uint8 = 2
)

const (
	// PoolStakeSnapshotTypeMark is the epoch-boundary mark snapshot used by
	// governance and by the normal Praos epoch-offset rotation.
	PoolStakeSnapshotTypeMark = "mark"
	// PoolStakeSnapshotTypeSet is the mark snapshot rotated forward one
	// epoch, per the Shelley set/go/mark rotation.
	PoolStakeSnapshotTypeSet = "set"
	// PoolStakeSnapshotTypeGo is the set snapshot rotated forward one more
	// epoch; this is the row consulted for live leader-election stake.
	PoolStakeSnapshotTypeGo = "go"
	// PoolStakeSnapshotTypeActive is the active consensus pool distribution
	// imported from a Mithril NewEpochState.pool-distr field. TotalStake
	// stores the fraction numerator and StakeDenominator stores the
	// denominator for the same row.
	PoolStakeSnapshotTypeActive = "actv"
)

// ValidPoolStakeSnapshotType reports whether snapshotType is one of the
// known pool_stake_snapshot.snapshot_type values. Callers that accept a
// snapshotType from outside the package should validate with this before
// querying, so a typo or stale value fails fast instead of silently
// returning zero rows.
func ValidPoolStakeSnapshotType(snapshotType string) bool {
	switch snapshotType {
	case PoolStakeSnapshotTypeMark,
		PoolStakeSnapshotTypeSet,
		PoolStakeSnapshotTypeGo,
		PoolStakeSnapshotTypeActive:
		return true
	default:
		return false
	}
}

// PoolStakeSnapshot captures pool stake for an epoch snapshot. Mark rows store
// lovelace totals at an epoch boundary. Active rows imported from Mithril store
// a consensus stake fraction as TotalStake/StakeDenominator.
type PoolStakeSnapshot struct {
	ID               uint         `gorm:"primarykey"`
	Epoch            uint64       `gorm:"uniqueIndex:idx_pool_stake_epoch_pool,priority:1;not null"`
	SnapshotType     string       `gorm:"type:varchar(4);uniqueIndex:idx_pool_stake_epoch_pool,priority:2;not null"` // "mark", "set", "go", "actv"
	PoolKeyHash      []byte       `gorm:"uniqueIndex:idx_pool_stake_epoch_pool,priority:3;size:28;not null"`
	TotalStake       types.Uint64 `gorm:"not null"`
	StakeDenominator types.Uint64 `gorm:"not null;default:0"`
	DelegatorCount   uint64       `gorm:"not null"`
	CapturedSlot     uint64       `gorm:"not null"`
	// RewardAccountAutoVote captures the CIP-1694 SPO auto-vote
	// outcome implied by the pool's reward-account DRep delegation at
	// the snapshot epoch. Values come from PoolRewardAccountAutoVote*.
	// This field is only meaningful when RewardAccountAutoVoteResolved
	// is true; otherwise its value is undefined and must not be read
	// by the tally.
	RewardAccountAutoVote uint8 `gorm:"not null;default:0"`
	// RewardAccountAutoVoteResolved disambiguates "resolved as none"
	// from "never resolved". The resolver sets this to true after it
	// has computed RewardAccountAutoVote against snapshot-era state.
	// Rows imported by Mithril for set/go rotations (which only have
	// live state available at import time and cannot be faithfully
	// resolved against historical boundaries) intentionally leave this
	// false; the tally treats them as PoolRewardAccountAutoVoteNone,
	// matching pre-CIP-1694 behaviour for those rows. Pre-CIP-1694
	// rows in upgraded databases also remain false until re-resolved.
	RewardAccountAutoVoteResolved bool `gorm:"not null;default:false"`
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
