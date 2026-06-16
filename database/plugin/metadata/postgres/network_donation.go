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

package postgres

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm/clause"
)

// AddNetworkDonation records a block's total Conway treasury donation for the
// given slot and epoch. It is idempotent per slot so re-applying a block (e.g.
// after a rollback) overwrites rather than double-counts.
func (d *MetadataStorePostgres) AddNetworkDonation(
	slot, epoch, amount uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("add network donation: %w", err)
	}
	rec := &models.NetworkDonation{
		Slot:   slot,
		Epoch:  epoch,
		Amount: amount,
	}
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "slot"}},
		DoUpdates: clause.AssignmentColumns([]string{"epoch", "amount"}),
	}).Create(rec)
	if result.Error != nil {
		return fmt.Errorf("add network donation: %w", result.Error)
	}
	return nil
}

// SumNetworkDonationsForEpoch returns the total donation contributed by blocks
// in the given epoch.
func (d *MetadataStorePostgres) SumNetworkDonationsForEpoch(
	epoch uint64,
	txn types.Txn,
) (uint64, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return 0, fmt.Errorf("sum network donations: %w", err)
	}
	var total uint64
	result := db.Model(&models.NetworkDonation{}).
		Where("epoch = ?", epoch).
		Select("COALESCE(SUM(amount), 0)").
		Scan(&total)
	if result.Error != nil {
		return 0, fmt.Errorf(
			"sum network donations for epoch %d: %w", epoch, result.Error,
		)
	}
	return total, nil
}

// DeleteNetworkDonationsAfterSlot removes donation records added after the
// given slot. This is used during chain rollbacks.
func (d *MetadataStorePostgres) DeleteNetworkDonationsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("delete network donations after slot: %w", err)
	}
	result := db.Where("slot > ?", slot).
		Delete(&models.NetworkDonation{})
	if result.Error != nil {
		return fmt.Errorf(
			"delete network donations after slot %d: %w", slot, result.Error,
		)
	}
	return nil
}
