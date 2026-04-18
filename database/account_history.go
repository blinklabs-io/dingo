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

// GetAccountDelegationHistory returns delegation history rows for a staking key.
func (d *Database) GetAccountDelegationHistory(
	stakeKey []byte,
	txn *Txn,
) ([]models.AccountDelegationHistoryRow, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	rows, err := d.metadata.GetAccountDelegationHistory(
		stakeKey,
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

// GetAccountRegistrationHistory returns registration history rows for a staking key.
func (d *Database) GetAccountRegistrationHistory(
	stakeKey []byte,
	txn *Txn,
) ([]models.AccountRegistrationHistoryRow, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	rows, err := d.metadata.GetAccountRegistrationHistory(
		stakeKey,
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
