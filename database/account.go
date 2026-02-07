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

// RestoreAccountStateAtSlot reverts account delegation state to the given
// slot. For accounts modified after the slot, this restores their Pool and
// Drep delegations to the state they had at the given slot, or deletes them
// if they were registered after that slot.
func (d *Database) RestoreAccountStateAtSlot(
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
	if err := d.metadata.RestoreAccountStateAtSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to restore account state at slot %d: %w",
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

// GetAccount returns an account by staking key
func (d *Database) GetAccount(
	stakeKey []byte,
	includeInactive bool,
	txn *Txn,
) (*models.Account, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	account, err := d.metadata.GetAccount(
		stakeKey,
		includeInactive,
		txn.Metadata(),
	)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, models.ErrAccountNotFound
	}
	return account, nil
}
