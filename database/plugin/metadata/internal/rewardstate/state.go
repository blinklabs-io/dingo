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

package rewardstate

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// rewardSaveBatchSize bounds the rows per multi-row INSERT for reward pool
// inputs. A single Create binds rows*columns parameters; the
// widest reward row (RewardPoolInput, ~14 columns) at full delegator/account
// scale (~1M rows on mainnet) would bind millions of parameters and exceed
// every backend's bind limit (SQLite 32766, Postgres/MySQL 65535), rolling
// back the epoch reward transaction. 1000 rows * ~14 columns = ~14000 stays
// well under all three, matching the existing importAssetBatchSize precedent.
const rewardSaveBatchSize = 1000

// SaveAdaPots saves reward-related ADA pots for an epoch.
func SaveAdaPots(db *gorm.DB, pots *models.RewardAdaPots) error {
	if err := db.Clauses(
		clause.OnConflict{
			Columns: []clause.Column{{Name: "epoch"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"treasury",
				"reserves",
				"fees",
				"rewards",
				"captured_slot",
			}),
		},
	).Create(pots).Error; err != nil {
		return fmt.Errorf("save reward ADA pots: %w", err)
	}
	return nil
}

// GetAdaPots retrieves reward-related ADA pots for an epoch.
func GetAdaPots(
	db *gorm.DB,
	epoch uint64,
) (*models.RewardAdaPots, error) {
	var pots models.RewardAdaPots
	result := db.Where("epoch = ?", epoch).First(&pots)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &pots, nil
}

// SaveSnapshot saves reward snapshot metadata for an epoch. It overwrites any
// existing row for the (epoch, snapshot_type) pair, including its authoritative
// flag, so the authoritative epoch-rollover capture always wins over a
// provisional fallback row. The fallback path must use ClaimFallbackSnapshot
// instead, which refuses to overwrite an authoritative row.
func SaveSnapshot(db *gorm.DB, snapshot *models.RewardSnapshot) error {
	if err := db.Clauses(
		clause.OnConflict{
			Columns: []clause.Column{
				{Name: "epoch"},
				{Name: "snapshot_type"},
			},
			DoUpdates: clause.AssignmentColumns([]string{
				"total_active_stake",
				"total_pool_count",
				"total_delegators",
				"captured_slot",
				"boundary_slot",
				"epoch_nonce",
				"protocol_version",
				"authoritative",
			}),
		},
	).Create(snapshot).Error; err != nil {
		return fmt.Errorf("save reward snapshot: %w", err)
	}
	return nil
}

// ClaimFallbackSnapshot atomically reserves the (epoch, snapshot_type) reward
// snapshot marker for a fallback (non-authoritative) capture. snapshot must
// carry Authoritative=false. It returns proceed=false when an authoritative
// snapshot already occupies the slot, so the caller must abandon the fallback
// capture instead of overwriting it.
//
// The claim is an INSERT ... ON CONFLICT DO NOTHING followed, on conflict, by a
// locking (SELECT ... FOR UPDATE) recheck. The row lock is what a concurrent
// authoritative writer blocks on under MySQL/Postgres READ COMMITTED, closing
// the check-then-write race; SQLite drops the lock clause but its single-writer
// transaction semantics provide the same serialization. A prior non-authoritative
// row (e.g. a slot-clock provisional) is replaced in place under the held lock.
//
// That lock only survives between statements while a transaction is open, so the
// whole claim MUST run in one transaction. When the caller supplies an open
// transaction (txn != nil) db already carries it; otherwise the claim is wrapped
// in db.Transaction so a transactionless call cannot silently drop the lock after
// the recheck and clobber an authoritative row a concurrent SaveSnapshot
// committed in between. This mirrors the txn handling in DeleteInputsForEpoch and
// DeleteState{After,Before}* below.
func ClaimFallbackSnapshot(
	db *gorm.DB,
	snapshot *models.RewardSnapshot,
	txn types.Txn,
) (bool, error) {
	snapshot.Authoritative = false
	claim := func(tx *gorm.DB) (bool, error) {
		res := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "epoch"},
				{Name: "snapshot_type"},
			},
			DoNothing: true,
		}).Create(snapshot)
		if res.Error != nil {
			return false, fmt.Errorf("claim fallback reward snapshot: %w", res.Error)
		}
		if res.RowsAffected == 1 {
			// Won the slot outright: our row is the marker.
			return true, nil
		}
		// A row already exists. Lock it and inspect the authoritative flag.
		var existing models.RewardSnapshot
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where(
				"epoch = ? AND snapshot_type = ?",
				snapshot.Epoch,
				snapshot.SnapshotType,
			).First(&existing).Error; err != nil {
			return false, fmt.Errorf("lock existing reward snapshot: %w", err)
		}
		if existing.Authoritative {
			return false, nil
		}
		// Prior provisional (fallback) row: replace it in place while holding the
		// lock so the refreshed boundary/nonce/totals take effect.
		if err := tx.Model(&models.RewardSnapshot{}).
			Where(
				"epoch = ? AND snapshot_type = ?",
				snapshot.Epoch,
				snapshot.SnapshotType,
			).Updates(map[string]any{
			"total_active_stake": snapshot.TotalActiveStake,
			"total_pool_count":   snapshot.TotalPoolCount,
			"total_delegators":   snapshot.TotalDelegators,
			"captured_slot":      snapshot.CapturedSlot,
			"boundary_slot":      snapshot.BoundarySlot,
			"epoch_nonce":        snapshot.EpochNonce,
			"protocol_version":   snapshot.ProtocolVersion,
			"authoritative":      false,
		}).Error; err != nil {
			return false, fmt.Errorf("replace fallback reward snapshot: %w", err)
		}
		return true, nil
	}

	if txn != nil {
		return claim(db)
	}
	var proceed bool
	if err := db.Transaction(func(tx *gorm.DB) error {
		var claimErr error
		proceed, claimErr = claim(tx)
		return claimErr
	}); err != nil {
		return false, err
	}
	return proceed, nil
}

// ClaimFallbackSnapshotGuard serializes a fallback snapshot capture that has no
// reward-input bundle against the authoritative capture without leaving a
// reward_snapshot row behind. It claims the same unique (epoch, snapshot_type)
// key used by SaveSnapshot. When the key is absent, it inserts a temporary row
// and returns its ID; the caller must delete that row in the same transaction
// after its other snapshot writes are staged. When a provisional row already
// exists, it is locked and left untouched. An authoritative row refuses the
// fallback.
//
// The insert-or-lock sequence must remain inside the caller's open transaction:
// the temporary row is the lockable key that makes a concurrent authoritative
// SaveSnapshot wait until the fallback either commits or rolls back.
func ClaimFallbackSnapshotGuard(
	db *gorm.DB,
	epoch uint64,
	snapshotType string,
) (bool, uint, error) {
	guard := &models.RewardSnapshot{
		Epoch:        epoch,
		SnapshotType: snapshotType,
	}
	res := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "epoch"},
			{Name: "snapshot_type"},
		},
		DoNothing: true,
	}).Create(guard)
	if res.Error != nil {
		return false, 0, fmt.Errorf(
			"claim fallback reward snapshot guard: %w",
			res.Error,
		)
	}
	if res.RowsAffected == 1 {
		if guard.ID == 0 {
			if err := db.Where(
				"epoch = ? AND snapshot_type = ?",
				epoch,
				snapshotType,
			).First(guard).Error; err != nil {
				return false, 0, fmt.Errorf(
					"load fallback reward snapshot guard: %w",
					err,
				)
			}
		}
		return true, guard.ID, nil
	}

	var existing models.RewardSnapshot
	if err := db.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where(
			"epoch = ? AND snapshot_type = ?",
			epoch,
			snapshotType,
		).First(&existing).Error; err != nil {
		return false, 0, fmt.Errorf(
			"lock existing reward snapshot guard: %w",
			err,
		)
	}
	if existing.Authoritative {
		return false, 0, nil
	}
	return true, 0, nil
}

// ReleaseFallbackSnapshotGuard deletes a temporary guard row by primary key.
// The caller still holds the row lock in the same transaction, so no
// authoritative writer can replace the row between the claim and this delete.
func ReleaseFallbackSnapshotGuard(db *gorm.DB, guardID uint) error {
	if guardID == 0 {
		return nil
	}
	result := db.Where(
		"id = ? AND authoritative = ?",
		guardID,
		false,
	).Delete(&models.RewardSnapshot{})
	if result.Error != nil {
		return fmt.Errorf(
			"release fallback reward snapshot guard: %w",
			result.Error,
		)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf(
			"release fallback reward snapshot guard: expected 1 row, deleted %d",
			result.RowsAffected,
		)
	}
	return nil
}

// GetSnapshot retrieves reward snapshot metadata for an epoch.
func GetSnapshot(
	db *gorm.DB,
	epoch uint64,
	snapshotType string,
) (*models.RewardSnapshot, error) {
	var snapshot models.RewardSnapshot
	result := db.Where(
		"epoch = ? AND snapshot_type = ?",
		epoch,
		snapshotType,
	).First(&snapshot)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &snapshot, nil
}

// SavePoolInputs saves per-pool reward inputs for an epoch.
func SavePoolInputs(db *gorm.DB, inputs []*models.RewardPoolInput) error {
	if len(inputs) == 0 {
		return nil
	}
	if err := db.Clauses(
		clause.OnConflict{
			Columns: []clause.Column{
				{Name: "epoch"},
				{Name: "pool_key_hash"},
			},
			DoUpdates: clause.AssignmentColumns([]string{
				"blocks_produced",
				"total_blocks_in_epoch",
				"pledge",
				"delegated_stake",
				"owner_stake",
				"cost",
				"margin",
				"reward_account",
				"reward_account_credential_tag",
				"delegator_count",
				"captured_slot",
				"boundary_slot",
			}),
		},
	).CreateInBatches(inputs, rewardSaveBatchSize).Error; err != nil {
		return fmt.Errorf("save reward pool inputs: %w", err)
	}
	return nil
}

// GetPoolInputs retrieves all per-pool reward inputs for an epoch.
func GetPoolInputs(
	db *gorm.DB,
	epoch uint64,
) ([]*models.RewardPoolInput, error) {
	var inputs []*models.RewardPoolInput
	result := db.Where("epoch = ?", epoch).
		Order("pool_key_hash ASC").
		Find(&inputs)
	if result.Error != nil {
		return nil, result.Error
	}
	return inputs, nil
}

// DedupePoolKeyHashes returns poolKeyHashes with duplicates removed.
func DedupePoolKeyHashes(poolKeyHashes [][]byte) [][]byte {
	if len(poolKeyHashes) <= 1 {
		return poolKeyHashes
	}
	seen := make(map[string]struct{}, len(poolKeyHashes))
	ret := make([][]byte, 0, len(poolKeyHashes))
	for _, poolKeyHash := range poolKeyHashes {
		key := string(poolKeyHash)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ret = append(ret, poolKeyHash)
	}
	return ret
}

// StakeInputsForPools returns positive registered stake from the maintained
// live reward aggregate for the requested pools. The live aggregate is
// maintained transactionally, so the result reflects the caller's db/txn view
// rather than any historical slot.
func StakeInputsForPools(
	db *gorm.DB,
	poolKeyHashes [][]byte,
	chunkSize int,
) ([]*models.RewardStakeInput, error) {
	if len(poolKeyHashes) == 0 {
		return nil, nil
	}
	poolKeyHashes = DedupePoolKeyHashes(poolKeyHashes)
	query := fmt.Sprintf(`
		SELECT rls.*
		FROM reward_live_stake rls
		WHERE rls.pool_key_hash IN ?
			AND rls.registered = ?
			AND CAST(rls.total_stake AS %s) > ?
		ORDER BY rls.pool_key_hash ASC, rls.credential_tag ASC, rls.staking_key ASC
	`, integerCastType(db))

	rows := make([]models.RewardLiveStake, 0)
	for start := 0; start < len(poolKeyHashes); start += chunkSize {
		end := min(start+chunkSize, len(poolKeyHashes))
		var chunkRows []models.RewardLiveStake
		if err := db.Raw(
			query,
			poolKeyHashes[start:end],
			true,
			0,
		).Scan(&chunkRows).Error; err != nil {
			return nil, fmt.Errorf("query stake inputs: %w", err)
		}
		rows = append(rows, chunkRows...)
	}

	ret := make([]*models.RewardStakeInput, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, &models.RewardStakeInput{
			PoolKeyHash:   append([]byte(nil), row.PoolKeyHash...),
			StakingKey:    append([]byte(nil), row.StakingKey...),
			CredentialTag: row.CredentialTag,
			Stake:         row.TotalStake,
			Registered:    true,
		})
	}
	return ret, nil
}

// SaveStakeInputs saves per-credential reward snapshot inputs.
func SaveStakeInputs(db *gorm.DB, inputs []*models.RewardStakeInput) error {
	if len(inputs) == 0 {
		return nil
	}
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "epoch"},
			{Name: "pool_key_hash"},
			{Name: "credential_tag"},
			{Name: "staking_key"},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			"stake", "owner", "registered", "captured_slot", "boundary_slot",
		}),
	}).CreateInBatches(inputs, rewardSaveBatchSize).Error; err != nil {
		return fmt.Errorf("save reward stake inputs: %w", err)
	}
	return nil
}

// GetStakeInputs retrieves all per-credential reward inputs for an epoch.
func GetStakeInputs(db *gorm.DB, epoch uint64) ([]*models.RewardStakeInput, error) {
	var inputs []*models.RewardStakeInput
	result := db.Where("epoch = ?", epoch).
		Order("pool_key_hash ASC, credential_tag ASC, staking_key ASC").
		Find(&inputs)
	return inputs, result.Error
}

// DeleteInputsForEpoch deletes reward-calculation input rows for an epoch.
func DeleteInputsForEpoch(db *gorm.DB, epoch uint64, txn types.Txn) error {
	deleteRows := func(tx *gorm.DB) error {
		if err := tx.Where("epoch = ?", epoch).Delete(&models.RewardPoolInput{}).Error; err != nil {
			return fmt.Errorf("delete reward pool inputs for epoch %d: %w", epoch, err)
		}
		if err := tx.Where("epoch = ?", epoch).Delete(&models.RewardStakeInput{}).Error; err != nil {
			return fmt.Errorf("delete reward stake inputs for epoch %d: %w", epoch, err)
		}
		return nil
	}
	if txn != nil {
		return deleteRows(db)
	}
	return db.Transaction(deleteRows)
}

// DeleteOutputsForEpoch deletes reward-calculation output rows for an epoch.
func DeleteOutputsForEpoch(db *gorm.DB, epoch uint64, txn types.Txn) error {
	deleteRows := func(tx *gorm.DB) error {
		if err := tx.Where("epoch = ?", epoch).Delete(&models.RewardPoolOutput{}).Error; err != nil {
			return fmt.Errorf("delete reward pool outputs for epoch %d: %w", epoch, err)
		}
		if err := tx.Where("epoch = ?", epoch).Delete(&models.RewardAccountOutput{}).Error; err != nil {
			return fmt.Errorf("delete reward account outputs for epoch %d: %w", epoch, err)
		}
		return nil
	}
	if txn != nil {
		return deleteRows(db)
	}
	return db.Transaction(deleteRows)
}

// SavePoolOutputs saves per-pool reward calculation outputs.
func SavePoolOutputs(db *gorm.DB, outputs []*models.RewardPoolOutput) error {
	if len(outputs) == 0 {
		return nil
	}
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "epoch"}, {Name: "pool_key_hash"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"apparent_performance", "optimal_reward", "total_reward",
			"leader_reward", "member_reward_total", "owner_stake",
			"undistributed", "unspendable", "captured_slot", "boundary_slot",
		}),
	}).CreateInBatches(outputs, rewardSaveBatchSize).Error; err != nil {
		return fmt.Errorf("save reward pool outputs: %w", err)
	}
	return nil
}

// GetPoolOutputs retrieves per-pool reward calculation outputs.
func GetPoolOutputs(db *gorm.DB, epoch uint64) ([]*models.RewardPoolOutput, error) {
	var outputs []*models.RewardPoolOutput
	result := db.Where("epoch = ?", epoch).Order("pool_key_hash ASC").Find(&outputs)
	return outputs, result.Error
}

// SaveAccountOutputs saves per-account reward calculation outputs.
func SaveAccountOutputs(db *gorm.DB, outputs []*models.RewardAccountOutput) error {
	if len(outputs) == 0 {
		return nil
	}
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "epoch"},
			{Name: "credential_tag"},
			{Name: "staking_key"},
			{Name: "pool_key_hash"},
			{Name: "reward_type"},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			"amount", "spendable", "captured_slot", "boundary_slot",
		}),
	}).CreateInBatches(outputs, rewardSaveBatchSize).Error; err != nil {
		return fmt.Errorf("save reward account outputs: %w", err)
	}
	return nil
}

// GetAccountOutputs retrieves per-account reward calculation outputs.
func GetAccountOutputs(db *gorm.DB, epoch uint64) ([]*models.RewardAccountOutput, error) {
	var outputs []*models.RewardAccountOutput
	result := db.Where("epoch = ?", epoch).
		Order("credential_tag ASC, staking_key ASC, pool_key_hash ASC, reward_type ASC").
		Find(&outputs)
	return outputs, result.Error
}

// DeleteStateAfterSlot deletes reward-state rows captured from rolled-back
// blocks. When txn is non-nil, db is used as-is; otherwise the deletes are
// wrapped in their own transaction.
func DeleteStateAfterSlot(
	db *gorm.DB,
	slot uint64,
	txn types.Txn,
) error {
	deleteRows := func(tx *gorm.DB) error {
		if err := tx.Where(
			"captured_slot > ?",
			slot,
		).Delete(&models.RewardAdaPots{}).Error; err != nil {
			return fmt.Errorf("delete reward ADA pots after slot: %w", err)
		}
		if err := tx.Where(
			"captured_slot > ? OR boundary_slot > ?",
			slot,
			slot,
		).Delete(&models.RewardSnapshot{}).Error; err != nil {
			return fmt.Errorf("delete reward snapshots after slot: %w", err)
		}
		if err := tx.Where(
			"captured_slot > ? OR boundary_slot > ?",
			slot,
			slot,
		).Delete(&models.RewardPoolInput{}).Error; err != nil {
			return fmt.Errorf("delete reward pool inputs after slot: %w", err)
		}
		for _, model := range []any{
			&models.RewardStakeInput{},
			&models.RewardPoolOutput{},
			&models.RewardAccountOutput{},
		} {
			if err := tx.Where(
				"captured_slot > ? OR boundary_slot > ?", slot, slot,
			).Delete(model).Error; err != nil {
				return fmt.Errorf("delete reward state after slot: %w", err)
			}
		}
		return nil
	}

	if txn != nil {
		return deleteRows(db)
	}
	return db.Transaction(deleteRows)
}

// DeleteStateBeforeEpoch deletes reward-state rows older than the retained
// snapshot window. When txn is non-nil, db is used as-is; otherwise the
// deletes are wrapped in their own transaction.
func DeleteStateBeforeEpoch(
	db *gorm.DB,
	epoch uint64,
	txn types.Txn,
) error {
	deleteRows := func(tx *gorm.DB) error {
		if err := tx.Where("epoch < ?", epoch).
			Delete(&models.RewardAdaPots{}).Error; err != nil {
			return fmt.Errorf("delete reward ADA pots before epoch: %w", err)
		}
		if err := tx.Where("epoch < ?", epoch).
			Delete(&models.RewardSnapshot{}).Error; err != nil {
			return fmt.Errorf("delete reward snapshots before epoch: %w", err)
		}
		if err := tx.Where("epoch < ?", epoch).
			Delete(&models.RewardPoolInput{}).Error; err != nil {
			return fmt.Errorf("delete reward pool inputs before epoch: %w", err)
		}
		for _, model := range []any{
			&models.RewardStakeInput{},
			&models.RewardPoolOutput{},
			&models.RewardAccountOutput{},
		} {
			if err := tx.Where("epoch < ?", epoch).Delete(model).Error; err != nil {
				return fmt.Errorf("delete reward state before epoch: %w", err)
			}
		}
		return nil
	}

	if txn != nil {
		return deleteRows(db)
	}
	return db.Transaction(deleteRows)
}
