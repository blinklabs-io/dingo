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

// GetAccountDelegationHistoryByCredential returns delegation history rows for
// a stake credential.
func (d *Database) GetAccountDelegationHistoryByCredential(
	credentialTag uint8,
	stakeKey []byte,
	limit int,
	offset int,
	order string,
	txn *Txn,
) ([]models.AccountDelegationHistoryRow, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	rows, err := d.metadata.GetAccountDelegationHistoryByCredential(
		credentialTag,
		stakeKey,
		limit,
		offset,
		order,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get account delegation history: %w",
			err,
		)
	}
	return rows, nil
}

// CountAccountDelegationHistoryByCredential returns the total number of
// delegation history rows for a stake credential.
func (d *Database) CountAccountDelegationHistoryByCredential(
	credentialTag uint8,
	stakeKey []byte,
	txn *Txn,
) (int, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	count, err := d.metadata.CountAccountDelegationHistoryByCredential(
		credentialTag,
		stakeKey,
		txn.Metadata(),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"count account delegation history: %w",
			err,
		)
	}
	return count, nil
}

// GetAccountRegistrationHistoryByCredential returns registration history rows
// for a stake credential.
func (d *Database) GetAccountRegistrationHistoryByCredential(
	credentialTag uint8,
	stakeKey []byte,
	limit int,
	offset int,
	order string,
	txn *Txn,
) ([]models.AccountRegistrationHistoryRow, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	rows, err := d.metadata.GetAccountRegistrationHistoryByCredential(
		credentialTag,
		stakeKey,
		limit,
		offset,
		order,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get account registration history: %w",
			err,
		)
	}
	return rows, nil
}

// CountAccountRegistrationHistoryByCredential returns the total number of
// registration history rows for a stake credential.
func (d *Database) CountAccountRegistrationHistoryByCredential(
	credentialTag uint8,
	stakeKey []byte,
	txn *Txn,
) (int, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	count, err := d.metadata.CountAccountRegistrationHistoryByCredential(
		credentialTag,
		stakeKey,
		txn.Metadata(),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"count account registration history: %w",
			err,
		)
	}
	return count, nil
}
