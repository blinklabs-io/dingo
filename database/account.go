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
	"github.com/blinklabs-io/dingo/database/models"
)

// GetAccount returns an account by staking key
func (d *Database) GetAccount(
	stakeKey []byte,
	txn *Txn,
) (*models.Account, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	account, err := d.metadata.GetAccount(stakeKey, txn.Metadata())
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, models.ErrAccountNotFound
	}
	return account, nil
}
