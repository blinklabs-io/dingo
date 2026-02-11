// Copyright 2025 Blink Labs Software
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

package database

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
)

// RestoreDrepStateAtSlot reverts DRep state to the given slot. DReps
// registered only after the slot are deleted; remaining DReps have their
// anchor and active status restored.
func (d *Database) RestoreDrepStateAtSlot(
	slot uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.MetadataTxn(true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	if err := d.metadata.RestoreDrepStateAtSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to restore DRep state at slot %d: %w",
			slot,
			err,
		)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
		owned = false
	}
	return nil
}

// GetDrep returns a drep by credential
func (d *Database) GetDrep(
	cred []byte,
	includeInactive bool,
	txn *Txn,
) (*models.Drep, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	ret, err := d.metadata.GetDrep(cred, includeInactive, txn.Metadata())
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, models.ErrDrepNotFound
	}
	return ret, nil
}

// GetActiveDreps returns all active DReps
func (d *Database) GetActiveDreps(
	txn *Txn,
) ([]*models.Drep, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	return d.metadata.GetActiveDreps(txn.Metadata())
}

// GetDRepVotingPower calculates the voting power for a DRep by summing
// the stake of all accounts delegated to it. Uses the current live
// UTxO set (deleted_slot = 0) for the calculation.
func (d *Database) GetDRepVotingPower(
	drepCredential []byte,
	txn *Txn,
) (uint64, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	return d.metadata.GetDRepVotingPower(
		drepCredential,
		txn.Metadata(),
	)
}

// UpdateDRepActivity updates the DRep's last activity epoch and
// recalculates the expiry epoch.
func (d *Database) UpdateDRepActivity(
	drepCredential []byte,
	activityEpoch uint64,
	inactivityPeriod uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.MetadataTxn(true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	if err := d.metadata.UpdateDRepActivity(
		drepCredential,
		activityEpoch,
		inactivityPeriod,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to update DRep activity: %w",
			err,
		)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
		owned = false
	}
	return nil
}

// GetExpiredDReps returns all active DReps whose expiry epoch is at
// or before the given epoch.
func (d *Database) GetExpiredDReps(
	epoch uint64,
	txn *Txn,
) ([]*models.Drep, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	return d.metadata.GetExpiredDReps(epoch, txn.Metadata())
}
