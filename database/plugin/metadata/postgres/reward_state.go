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

package postgres

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/rewardstate"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/stakequery"
	"github.com/blinklabs-io/dingo/database/types"
)

const rewardStakeInputPoolBatchSize = 1000

// SaveRewardAdaPots saves reward-related ADA pots for an epoch.
func (d *MetadataStorePostgres) SaveRewardAdaPots(
	pots *models.RewardAdaPots,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return rewardstate.SaveAdaPots(db, pots)
}

// GetRewardAdaPots retrieves reward-related ADA pots for an epoch.
func (d *MetadataStorePostgres) GetRewardAdaPots(
	epoch uint64,
	txn types.Txn,
) (*models.RewardAdaPots, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	return rewardstate.GetAdaPots(db, epoch)
}

// SaveRewardSnapshot saves reward snapshot metadata for an epoch.
func (d *MetadataStorePostgres) SaveRewardSnapshot(
	snapshot *models.RewardSnapshot,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return rewardstate.SaveSnapshot(db, snapshot)
}

// ClaimFallbackRewardSnapshot atomically reserves the reward snapshot marker
// for a fallback capture.
func (d *MetadataStorePostgres) ClaimFallbackRewardSnapshot(
	snapshot *models.RewardSnapshot,
	txn types.Txn,
) (bool, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return false, fmt.Errorf("ClaimFallbackRewardSnapshot: resolve db: %w", err)
	}
	return rewardstate.ClaimFallbackSnapshot(db, snapshot, txn)
}

// ClaimFallbackRewardSnapshotGuard serializes a fallback capture that has no
// reward-input bundle against the authoritative capture.
func (d *MetadataStorePostgres) ClaimFallbackRewardSnapshotGuard(
	epoch uint64,
	snapshotType string,
	txn types.Txn,
) (bool, uint, error) {
	if txn == nil {
		return false, 0, errors.New(
			"ClaimFallbackRewardSnapshotGuard: transaction is required",
		)
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return false, 0, fmt.Errorf(
			"ClaimFallbackRewardSnapshotGuard: resolve db: %w",
			err,
		)
	}
	return rewardstate.ClaimFallbackSnapshotGuard(db, epoch, snapshotType)
}

// ReleaseFallbackRewardSnapshotGuard removes a temporary guard row.
func (d *MetadataStorePostgres) ReleaseFallbackRewardSnapshotGuard(
	guardID uint,
	txn types.Txn,
) error {
	if txn == nil {
		return errors.New(
			"ReleaseFallbackRewardSnapshotGuard: transaction is required",
		)
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"ReleaseFallbackRewardSnapshotGuard: resolve db: %w",
			err,
		)
	}
	return rewardstate.ReleaseFallbackSnapshotGuard(db, guardID)
}

// GetRewardSnapshot retrieves reward snapshot metadata for an epoch.
func (d *MetadataStorePostgres) GetRewardSnapshot(
	epoch uint64,
	snapshotType string,
	txn types.Txn,
) (*models.RewardSnapshot, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	return rewardstate.GetSnapshot(db, epoch, snapshotType)
}

// SaveRewardPoolInputs saves per-pool reward inputs for an epoch.
func (d *MetadataStorePostgres) SaveRewardPoolInputs(
	inputs []*models.RewardPoolInput,
	txn types.Txn,
) error {
	if len(inputs) == 0 {
		return nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return rewardstate.SavePoolInputs(db, inputs)
}

// GetRewardPoolInputs retrieves all per-pool reward inputs for an epoch.
func (d *MetadataStorePostgres) GetRewardPoolInputs(
	epoch uint64,
	txn types.Txn,
) ([]*models.RewardPoolInput, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	return rewardstate.GetPoolInputs(db, epoch)
}

func (d *MetadataStorePostgres) GetRewardStakeInputsForPools(poolKeyHashes [][]byte, slot uint64, expiryEpoch uint64, inactivityPeriod uint64, txn types.Txn) ([]*models.RewardStakeInput, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf("GetRewardStakeInputsForPools: resolve db: %w", err)
	}
	// CIP-0163 gate on: reconstruct reward inputs at slot from the same
	// historical CTE the leader-election path uses so both halves of the
	// snapshot agree by construction. Gate off: read the live aggregate without
	// an account join or expiration predicate.
	if expiryEpoch > 0 {
		inputs, err := stakequery.GetRewardStakeInputsByPoolsAtSlot(
			db, poolKeyHashes, slot, expiryEpoch, inactivityPeriod,
		)
		if err != nil {
			return nil, fmt.Errorf("GetRewardStakeInputsForPools: %w", err)
		}
		return inputs, nil
	}
	inputs, err := rewardstate.StakeInputsForPools(
		db, poolKeyHashes, rewardStakeInputPoolBatchSize, expiryEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("GetRewardStakeInputsForPools: %w", err)
	}
	return inputs, nil
}

func (d *MetadataStorePostgres) GetLiveStakeInputsForPools(
	poolKeyHashes [][]byte,
	expiryEpoch uint64,
	txn types.Txn,
) ([]*models.RewardStakeInput, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetLiveStakeInputsForPools: resolve db: %w", err,
		)
	}
	inputs, err := rewardstate.LiveStakeInputsForPools(
		db, poolKeyHashes, rewardStakeInputPoolBatchSize, expiryEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("GetLiveStakeInputsForPools: %w", err)
	}
	return inputs, nil
}

func (d *MetadataStorePostgres) SaveRewardStakeInputs(inputs []*models.RewardStakeInput, txn types.Txn) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return rewardstate.SaveStakeInputs(db, inputs)
}

func (d *MetadataStorePostgres) GetRewardStakeInputs(epoch uint64, txn types.Txn) ([]*models.RewardStakeInput, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	return rewardstate.GetStakeInputs(db, epoch)
}

func (d *MetadataStorePostgres) DeleteRewardInputsForEpoch(epoch uint64, txn types.Txn) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("delete reward inputs for epoch: resolve db: %w", err)
	}
	return rewardstate.DeleteInputsForEpoch(db, epoch, txn)
}

func (d *MetadataStorePostgres) DeleteRewardOutputsForEpoch(epoch uint64, txn types.Txn) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("delete reward outputs for epoch: resolve db: %w", err)
	}
	return rewardstate.DeleteOutputsForEpoch(db, epoch, txn)
}

func (d *MetadataStorePostgres) SaveRewardPoolOutputs(outputs []*models.RewardPoolOutput, txn types.Txn) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return rewardstate.SavePoolOutputs(db, outputs)
}

func (d *MetadataStorePostgres) GetRewardPoolOutputs(epoch uint64, txn types.Txn) ([]*models.RewardPoolOutput, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	return rewardstate.GetPoolOutputs(db, epoch)
}

func (d *MetadataStorePostgres) SaveRewardAccountOutputs(outputs []*models.RewardAccountOutput, txn types.Txn) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return rewardstate.SaveAccountOutputs(db, outputs)
}

func (d *MetadataStorePostgres) GetRewardAccountOutputs(epoch uint64, txn types.Txn) ([]*models.RewardAccountOutput, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	return rewardstate.GetAccountOutputs(db, epoch)
}

// GetRewardAccountOutputsByCredential retrieves reward account output rows
// for a stake credential tag/hash pair, paginated and ordered by epoch.
func (d *MetadataStorePostgres) GetRewardAccountOutputsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]*models.RewardAccountOutput, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve read DB for reward account outputs by credential: %w",
			err,
		)
	}
	rows, err := rewardstate.GetAccountOutputsByCredential(
		db,
		credentialTag,
		stakingKey,
		limit,
		offset,
		order,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get reward account outputs by credential: %w",
			err,
		)
	}
	return rows, nil
}

// CountRewardAccountOutputsByCredential retrieves the total count of reward
// account output rows for a stake credential tag/hash pair.
func (d *MetadataStorePostgres) CountRewardAccountOutputsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (int, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return 0, fmt.Errorf(
			"resolve read DB for count reward account outputs by credential: %w",
			err,
		)
	}
	count, err := rewardstate.CountAccountOutputsByCredential(
		db,
		credentialTag,
		stakingKey,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"count reward account outputs by credential: %w",
			err,
		)
	}
	return count, nil
}

// DeleteRewardStateAfterSlot deletes reward-state rows captured from
// rolled-back blocks.
func (d *MetadataStorePostgres) DeleteRewardStateAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("delete reward state after slot: resolve db: %w", err)
	}
	return rewardstate.DeleteStateAfterSlot(db, slot, txn)
}

// DeleteRewardStateBeforeEpoch deletes reward-state rows older than the
// retained snapshot window.
func (d *MetadataStorePostgres) DeleteRewardStateBeforeEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("delete reward state before epoch: resolve db: %w", err)
	}
	return rewardstate.DeleteStateBeforeEpoch(db, epoch, txn)
}

// DeleteRewardStakeInputBeforeEpoch deletes only reward_stake_input rows
// older than the retained snapshot window, leaving reward_account_output
// intact. Used in API storage mode.
func (d *MetadataStorePostgres) DeleteRewardStakeInputBeforeEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"delete reward stake input before epoch: resolve db: %w",
			err,
		)
	}
	return rewardstate.DeleteStakeInputBeforeEpoch(db, epoch, txn)
}
