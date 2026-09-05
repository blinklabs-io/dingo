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

import "fmt"

// DeleteNetworkStateAfterSlot removes network state records added
// after the given slot. This is used during chain rollbacks.
func (d *Database) DeleteNetworkStateAfterSlot(
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
	if err := d.metadata.DeleteNetworkStateAfterSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to delete network state after slot %d: %w",
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

// GetSyncState retrieves a sync state value by key.
// Returns empty string if the key does not exist.
func (d *Database) GetSyncState(
	key string,
	txn *Txn,
) (string, error) {
	var (
		val string
		err error
	)
	if txn == nil {
		val, err = d.metadata.GetSyncState(key, nil)
	} else {
		val, err = d.metadata.GetSyncState(
			key, txn.Metadata(),
		)
	}
	if err != nil {
		return "", fmt.Errorf(
			"Database.GetSyncState(%q): %w",
			key, err,
		)
	}
	return val, nil
}

// SetSyncState stores or updates a sync state value.
func (d *Database) SetSyncState(
	key, value string,
	txn *Txn,
) error {
	var err error
	if txn == nil {
		err = d.metadata.SetSyncState(key, value, nil)
	} else {
		err = d.metadata.SetSyncState(
			key, value, txn.Metadata(),
		)
	}
	if err != nil {
		return fmt.Errorf(
			"Database.SetSyncState(%q): %w",
			key, err,
		)
	}
	return nil
}

// DeleteSyncState removes a sync state key.
func (d *Database) DeleteSyncState(
	key string,
	txn *Txn,
) error {
	var err error
	if txn == nil {
		err = d.metadata.DeleteSyncState(key, nil)
	} else {
		err = d.metadata.DeleteSyncState(
			key, txn.Metadata(),
		)
	}
	if err != nil {
		return fmt.Errorf(
			"Database.DeleteSyncState(%q): %w",
			key, err,
		)
	}
	return nil
}

// ClearSyncState removes all sync state entries.
func (d *Database) ClearSyncState(txn *Txn) error {
	var err error
	if txn == nil {
		err = d.metadata.ClearSyncState(nil)
	} else {
		err = d.metadata.ClearSyncState(txn.Metadata())
	}
	if err != nil {
		return fmt.Errorf(
			"Database.ClearSyncState: %w", err,
		)
	}
	return nil
}
