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

package mysql

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/rewardstate"
	"github.com/blinklabs-io/dingo/database/types"
)

// SaveRewardAdaPots saves reward-related ADA pots for an epoch.
func (d *MetadataStoreMysql) SaveRewardAdaPots(
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
func (d *MetadataStoreMysql) GetRewardAdaPots(
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
func (d *MetadataStoreMysql) SaveRewardSnapshot(
	snapshot *models.RewardSnapshot,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return rewardstate.SaveSnapshot(db, snapshot)
}

// GetRewardSnapshot retrieves reward snapshot metadata for an epoch.
func (d *MetadataStoreMysql) GetRewardSnapshot(
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
func (d *MetadataStoreMysql) SaveRewardPoolInputs(
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
func (d *MetadataStoreMysql) GetRewardPoolInputs(
	epoch uint64,
	txn types.Txn,
) ([]*models.RewardPoolInput, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	return rewardstate.GetPoolInputs(db, epoch)
}

// DeleteRewardStateAfterSlot deletes reward-state rows captured from
// rolled-back blocks.
func (d *MetadataStoreMysql) DeleteRewardStateAfterSlot(
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
func (d *MetadataStoreMysql) DeleteRewardStateBeforeEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("delete reward state before epoch: resolve db: %w", err)
	}
	return rewardstate.DeleteStateBeforeEpoch(db, epoch, txn)
}
