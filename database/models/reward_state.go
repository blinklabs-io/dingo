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

// RewardStakeCalculationVersion identifies the stake-accounting algorithm
// used to produce persisted live stake and consensus snapshots. Bump it when
// changing that calculation so upgrades cannot trust older values.
const RewardStakeCalculationVersion uint = 1

// RewardAdaPots captures the reward-related ADA pots at an epoch boundary.
type RewardAdaPots struct {
	ID           uint         `gorm:"primarykey"`
	Epoch        uint64       `gorm:"uniqueIndex;not null"`
	Treasury     types.Uint64 `gorm:"not null"`
	Reserves     types.Uint64 `gorm:"not null"`
	Fees         types.Uint64 `gorm:"not null"`
	Rewards      types.Uint64 `gorm:"not null"`
	CapturedSlot uint64       `gorm:"index;not null"`
}

func (RewardAdaPots) TableName() string {
	return "reward_ada_pots"
}

// RewardSnapshot captures reward-calculation snapshot metadata for an epoch.
type RewardSnapshot struct {
	ID               uint         `gorm:"primarykey"`
	Epoch            uint64       `gorm:"uniqueIndex:idx_reward_snapshot_epoch_type,priority:1;not null"`
	SnapshotType     string       `gorm:"type:varchar(4);uniqueIndex:idx_reward_snapshot_epoch_type,priority:2;not null"`
	TotalActiveStake types.Uint64 `gorm:"not null"`
	TotalPoolCount   uint64       `gorm:"not null"`
	TotalDelegators  uint64       `gorm:"not null"`
	CapturedSlot     uint64       `gorm:"index;not null"`
	BoundarySlot     uint64       `gorm:"index;not null"`
	EpochNonce       []byte       `gorm:"size:32"`
	ProtocolVersion  uint         `gorm:"not null"`
	// Authoritative marks a snapshot captured inside the ledger epoch-rollover
	// write transaction at the SNAP point (CaptureEpochBoundarySnapshot). The
	// event-driven fallback capture (captureMarkSnapshot) never overwrites an
	// authoritative row: it either claims a fresh row or is superseded. Defaults
	// to false, so pre-existing rows and fallback captures read as provisional.
	Authoritative bool `gorm:"not null;default:false"`
	// CalculationVersion ties authoritative Mark metadata to the stake
	// calculation that produced its accompanying pool snapshots.
	CalculationVersion uint `gorm:"not null;default:0"`
}

func (RewardSnapshot) TableName() string {
	return "reward_snapshot"
}

// RewardPoolInput captures per-pool inputs needed by reward calculation.
type RewardPoolInput struct {
	Margin                     *types.Rat
	PoolKeyHash                []byte `gorm:"uniqueIndex:idx_reward_pool_input_epoch_pool,priority:2;size:28;not null"`
	RewardAccount              []byte `gorm:"size:28"`
	BlocksProduced             *uint64
	TotalBlocksInEpoch         *uint64
	ID                         uint         `gorm:"primarykey"`
	Epoch                      uint64       `gorm:"uniqueIndex:idx_reward_pool_input_epoch_pool,priority:1;not null"`
	Pledge                     types.Uint64 `gorm:"not null"`
	DelegatedStake             types.Uint64 `gorm:"not null"`
	OwnerStake                 types.Uint64 `gorm:"not null;default:0"`
	Cost                       types.Uint64 `gorm:"not null"`
	DelegatorCount             uint64       `gorm:"not null"`
	RewardAccountCredentialTag uint8        `gorm:"not null;default:0"`
	CapturedSlot               uint64       `gorm:"index;not null"`
	BoundarySlot               uint64       `gorm:"index;not null"`
}

func (RewardPoolInput) TableName() string {
	return "reward_pool_input"
}

// RewardStakeInput captures per-credential stake at the reward snapshot.
type RewardStakeInput struct {
	PoolKeyHash   []byte       `gorm:"uniqueIndex:idx_reward_stake_input_epoch_pool_cred,priority:2;size:28;not null"`
	StakingKey    []byte       `gorm:"uniqueIndex:idx_reward_stake_input_epoch_pool_cred,priority:4;size:28;not null"`
	ID            uint         `gorm:"primarykey"`
	Epoch         uint64       `gorm:"uniqueIndex:idx_reward_stake_input_epoch_pool_cred,priority:1;not null"`
	CredentialTag uint8        `gorm:"uniqueIndex:idx_reward_stake_input_epoch_pool_cred,priority:3;not null;default:0"`
	Stake         types.Uint64 `gorm:"not null"`
	Owner         bool         `gorm:"not null;default:false"`
	Registered    bool         `gorm:"not null"`
	CapturedSlot  uint64       `gorm:"index;not null"`
	BoundarySlot  uint64       `gorm:"index;not null"`
}

func (RewardStakeInput) TableName() string {
	return "reward_stake_input"
}

// RewardLiveStake is the live per-stake-credential aggregate maintained for a
// reward and leader-election snapshot consumers. UtxoStake and RewardStake are stored
// separately so rollback/account-reward repair can refresh only the affected
// credential while TotalStake remains directly queryable.
type RewardLiveStake struct {
	PoolKeyHash   []byte       `gorm:"index:idx_reward_live_stake_pool_order,priority:1;size:28"`
	StakingKey    []byte       `gorm:"uniqueIndex:idx_reward_live_stake_cred,priority:2;index:idx_reward_live_stake_pool_order,priority:3;size:28;not null"`
	ID            uint         `gorm:"primarykey"`
	CredentialTag uint8        `gorm:"uniqueIndex:idx_reward_live_stake_cred,priority:1;index:idx_reward_live_stake_pool_order,priority:2;not null;default:0"`
	UtxoStake     types.Uint64 `gorm:"not null"`
	RewardStake   types.Uint64 `gorm:"not null"`
	TotalStake    types.Uint64 `gorm:"not null"`
	Registered    bool         `gorm:"not null"`
	// PoolDelegation* records the certificate order used to derive PoolKeyHash.
	// It is rollback/rebuild bookkeeping; snapshot consumers select eligible
	// pools independently at the requested slot.
	PoolDelegationSlot       uint64 `gorm:"not null;default:0"`
	PoolDelegationBlockIndex uint64 `gorm:"not null;default:0"`
	PoolDelegationCertIndex  uint32 `gorm:"not null;default:0"`
	UpdatedSlot              uint64 `gorm:"index;not null"`
	// CalculationVersion is set by every rebuild and incremental update. Zero
	// denotes rows created before calculation provenance was introduced.
	CalculationVersion uint `gorm:"not null;default:0"`
}

func (RewardLiveStake) TableName() string {
	return "reward_live_stake"
}

// RewardPoolOutput captures per-pool reward calculation output for an epoch.
type RewardPoolOutput struct {
	ApparentPerformance *types.Rat
	PoolKeyHash         []byte       `gorm:"uniqueIndex:idx_reward_pool_output_epoch_pool,priority:2;size:28;not null"`
	ID                  uint         `gorm:"primarykey"`
	Epoch               uint64       `gorm:"uniqueIndex:idx_reward_pool_output_epoch_pool,priority:1;not null"`
	OptimalReward       types.Uint64 `gorm:"not null"`
	TotalReward         types.Uint64 `gorm:"not null"`
	LeaderReward        types.Uint64 `gorm:"not null"`
	MemberRewardTotal   types.Uint64 `gorm:"not null"`
	OwnerStake          types.Uint64 `gorm:"not null"`
	Undistributed       types.Uint64 `gorm:"not null"`
	Unspendable         types.Uint64 `gorm:"not null"`
	CapturedSlot        uint64       `gorm:"index;not null"`
	BoundarySlot        uint64       `gorm:"index;not null"`
}

func (RewardPoolOutput) TableName() string {
	return "reward_pool_output"
}

// RewardAccountOutput captures per-account reward calculation output.
//
// idx_reward_account_output_credential_spendable leads with (credential_tag,
// staking_key, spendable) so GetRewardAccountOutputsByCredential (the
// Blockfrost account reward-history endpoint, dingo #1875) is a pure index
// range scan over one credential's spendable rows rather than an index seek
// followed by a per-row filter: the epoch/pool_key_hash/reward_type tail
// matches that query's ORDER BY, so an ascending request is served directly
// from the index and a descending one only sorts the (typically tiny,
// single-credential) matched rows rather than the whole table. This matters
// specifically because dingo #1875 also makes API storage mode retain this
// table without bound (see the retention note in DATABASE.md), so the
// existing idx_reward_account_output_epoch_cred_pool_type index — which
// leads with epoch, not credential — cannot serve this query without
// scanning every retained row.
//
// spendable joined the index (superseding the original
// idx_reward_account_output_credential, which had no spendable column) once
// GetAccountOutputsByCredential started filtering on it: a credential
// deregistered before its reward's payout boundary keeps a permanent
// spendable=false row (see applyStakeRewardApplication /
// finalizePrecomputedRewardOutputs in ledger/reward_calculation.go), and
// that reward was never credited, so the reward-history endpoint must not
// report it. Without spendable in the index's equality prefix, the filter
// degrades the seek into a seek over every one of the credential's rows plus
// a per-row check. MigrateRewardAccountOutputCredentialIndex drops the
// superseded index once this one exists, so upgraded databases end up with
// exactly one credential-leading index rather than both.
//
// NOTE(dingo #1875 follow-up): a CIP-0163-guarded reward
// (rewardOutputGuarded in ledger/reward_calculation.go) is also skipped at
// crediting time, but that guard is never written back to Spendable, so a
// guarded row keeps spendable=true and this filter does not catch it. That
// only matters once CIP-0163 (delegator inactivity) activates; see the
// tracking note in DATABASE.md. Not fixed here — deliberately out of scope
// for this change.
type RewardAccountOutput struct {
	StakingKey    []byte       `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:3;size:28;not null;index:idx_reward_account_output_credential_spendable,priority:2"`
	PoolKeyHash   []byte       `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:4;size:28;not null;index:idx_reward_account_output_credential_spendable,priority:5"`
	RewardType    string       `gorm:"type:varchar(16);uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:5;not null;index:idx_reward_account_output_credential_spendable,priority:6"`
	ID            uint         `gorm:"primarykey"`
	Epoch         uint64       `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:1;not null;index:idx_reward_account_output_credential_spendable,priority:4"`
	CredentialTag uint8        `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:2;not null;default:0;index:idx_reward_account_output_credential_spendable,priority:1"`
	Amount        types.Uint64 `gorm:"not null"`
	Spendable     bool         `gorm:"not null;index:idx_reward_account_output_credential_spendable,priority:3"`
	CapturedSlot  uint64       `gorm:"index;not null"`
	BoundarySlot  uint64       `gorm:"index;not null"`
}

func (RewardAccountOutput) TableName() string {
	return "reward_account_output"
}

// MigrateRewardAccountOutputCredentialIndex drops the superseded
// idx_reward_account_output_credential index (credential_tag, staking_key,
// epoch, pool_key_hash, reward_type) once its replacement,
// idx_reward_account_output_credential_spendable, exists. The replacement
// adds spendable to the equality prefix so GetAccountOutputsByCredential's
// new spendable=true filter (dingo #1875 follow-up) stays a pure index range
// scan instead of degrading to a seek over every one of the credential's
// rows plus a per-row filter.
//
// Unlike MigrateRewardLiveStakePoolIndex and the other legacy-index
// migrations in this file, this one runs AFTER AutoMigrate rather than
// before. Those migrations drop a unique index that could otherwise reject
// valid rows or block a column-type change, so they must run first.
// idx_reward_account_output_credential is a plain non-unique secondary
// index: keeping it around a little longer is only wasted space, never a
// correctness problem, so there is no reason to risk a window with no
// credential-leading index at all. Calling this after AutoMigrate
// guarantees the replacement already exists before the old index is
// dropped.
func MigrateRewardAccountOutputCredentialIndex(
	db *gorm.DB,
	logger *slog.Logger,
) error {
	if logger == nil {
		logger = slog.Default()
	}
	if !db.Migrator().HasTable(&RewardAccountOutput{}) {
		return nil
	}
	if !db.Migrator().HasIndex(
		&RewardAccountOutput{},
		"idx_reward_account_output_credential",
	) {
		return nil
	}
	if !db.Migrator().HasIndex(
		&RewardAccountOutput{},
		"idx_reward_account_output_credential_spendable",
	) {
		// The replacement has not been created yet (should not happen when
		// called after AutoMigrate); leave the old index in place rather
		// than dropping the only credential-leading index available.
		return nil
	}
	logger.Info(
		"dropping superseded reward_account_output credential index",
	)
	if err := db.Migrator().DropIndex(
		&RewardAccountOutput{},
		"idx_reward_account_output_credential",
	); err != nil {
		return fmt.Errorf(
			"drop reward_account_output credential index: %w",
			err,
		)
	}
	return nil
}

// MigrateRewardLiveStakePoolIndex drops the legacy pool/total_stake index.
// Snapshot capture uses the replacement pool/credential ordering index declared
// on RewardLiveStake. Dropping the legacy index first lets MySQL change
// total_stake's numeric column type during AutoMigrate when an older schema
// represented it as TEXT.
func MigrateRewardLiveStakePoolIndex(db *gorm.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if !db.Migrator().HasTable(&RewardLiveStake{}) ||
		!db.Migrator().HasIndex(&RewardLiveStake{}, "idx_reward_live_stake_pool") {
		return nil
	}
	logger.Info(
		"dropping legacy reward_live_stake pool/total_stake index",
	)
	if err := db.Migrator().DropIndex(
		&RewardLiveStake{},
		"idx_reward_live_stake_pool",
	); err != nil {
		return fmt.Errorf("drop reward_live_stake pool index: %w", err)
	}
	return nil
}

// DedupeRewardLiveStake removes duplicate rows from the reward_live_stake
// table so that the unique index idx_reward_live_stake_cred
// (credential_tag, staking_key) can be created safely by AutoMigrate. This
// must be called before AutoMigrate for RewardLiveStake.
//
// The unique credential identity ensures one aggregate can contribute to only
// one reward pool input. Snapshot capture defensively applies the same identity,
// while this migration repairs pre-existing duplicates so AutoMigrate can
// install the constraint. Keeping only the lowest-id row per credential
// preserves the row selected by RefreshLiveStakeAggregate's First query before
// the unique index exists. This is important for upgraded databases: a refresh
// may have already repaired that row, while the missing-key backfill check will
// not rebuild a credential merely because another duplicate retained stale
// values.
//
// The function is a no-op when the table does not exist or contains no
// duplicates.
func DedupeRewardLiveStake(db *gorm.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if !db.Migrator().HasTable(&RewardLiveStake{}) {
		return nil
	}

	type dupGroup struct {
		CredentialTag uint8
		StakingKey    []byte
		Cnt           int64
	}
	var dups []dupGroup
	if err := db.Raw(`
		SELECT credential_tag, staking_key, COUNT(*) AS cnt
		FROM reward_live_stake
		GROUP BY credential_tag, staking_key
		HAVING COUNT(*) > 1
	`).Scan(&dups).Error; err != nil {
		return fmt.Errorf(
			"query duplicate reward_live_stake groups: %w", err,
		)
	}
	if len(dups) == 0 {
		return nil
	}

	logger.Info(
		"deduplicating reward_live_stake rows before creating unique index",
		"duplicate_groups", len(dups),
	)

	// For each duplicate group, SELECT the MIN(id) to keep, then DELETE by id.
	// RefreshLiveStakeAggregate uses GORM First, which orders by primary key and
	// updates this same row when duplicates predate the unique index.
	// This avoids the MySQL error 1093 ("can't specify target table for update
	// in FROM clause") that occurs when a DELETE subquery references the same
	// table.
	for _, d := range dups {
		var keepID uint
		if err := db.Raw(`
			SELECT MIN(id) FROM reward_live_stake
			WHERE credential_tag = ?
			  AND staking_key = ?
		`, d.CredentialTag, d.StakingKey,
		).Scan(&keepID).Error; err != nil {
			return fmt.Errorf(
				"select min id for reward_live_stake (tag=%d): %w",
				d.CredentialTag, err,
			)
		}
		if err := db.Exec(`
			DELETE FROM reward_live_stake
			WHERE credential_tag = ?
			  AND staking_key = ?
			  AND id != ?
		`, d.CredentialTag, d.StakingKey, keepID,
		).Error; err != nil {
			return fmt.Errorf(
				"delete duplicate reward_live_stake (tag=%d): %w",
				d.CredentialTag, err,
			)
		}
	}
	return nil
}
