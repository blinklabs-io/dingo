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
// the current stake of all delegated accounts, approximated from live
// UTxO balance plus reward-account balance.
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

// GetDRepVotingPowerBatch is the batch form of GetDRepVotingPower; see
// the metadata-store interface for the contract.
func (d *Database) GetDRepVotingPowerBatch(
	drepCredentials [][]byte,
	txn *Txn,
) (map[string]uint64, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	result, err := d.metadata.GetDRepVotingPowerBatch(
		drepCredentials,
		txn.Metadata(),
	)
	if err != nil {
		return result, fmt.Errorf(
			"Database.GetDRepVotingPowerBatch: failed to get "+
				"voting power for %d credentials: %w",
			len(drepCredentials),
			err,
		)
	}
	return result, nil
}

// GetDRepVotingPowerByType returns voting power grouped by DRep
// delegation type.
func (d *Database) GetDRepVotingPowerByType(
	drepTypes []uint64,
	txn *Txn,
) (map[uint64]uint64, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	result, err := d.metadata.GetDRepVotingPowerByType(
		drepTypes,
		txn.Metadata(),
	)
	if err != nil {
		return result, fmt.Errorf(
			"Database.GetDRepVotingPowerByType: failed to get "+
				"voting power for types %v: %w",
			drepTypes,
			err,
		)
	}
	return result, nil
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
