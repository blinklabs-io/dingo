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

package database

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
)

// DeleteConstitutionsAfterSlot removes constitutions added after the given
// slot and clears deleted_slot for any that were soft-deleted after that
// slot. This is used during chain rollbacks.
func (d *Database) DeleteConstitutionsAfterSlot(
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
	if err := d.metadata.DeleteConstitutionsAfterSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to delete constitutions after slot %d: %w",
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

// GetConstitution returns the current constitution
func (d *Database) GetConstitution(txn *Txn) (*models.Constitution, error) {
	if txn == nil {
		return d.metadata.GetConstitution(nil)
	}
	return d.metadata.GetConstitution(txn.Metadata())
}

// SetConstitution saves the constitution
func (d *Database) SetConstitution(
	constitution *models.Constitution,
	txn *Txn,
) error {
	if txn == nil {
		return d.metadata.SetConstitution(constitution, nil)
	}
	return d.metadata.SetConstitution(constitution, txn.Metadata())
}
