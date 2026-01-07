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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// GetPool returns a pool by its key hash
func (d *Database) GetPool(
	pkh lcommon.PoolKeyHash,
	includeInactive bool,
	txn *Txn,
) (*models.Pool, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	ret, err := d.metadata.GetPool(pkh, includeInactive, txn.Metadata())
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, models.ErrPoolNotFound
	}
	return ret, nil
}

// GetActivePoolRelays returns all relays from currently active pools.
// This is used for ledger peer discovery.
func (d *Database) GetActivePoolRelays(txn *Txn) ([]models.PoolRegistrationRelay, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	return d.metadata.GetActivePoolRelays(txn.Metadata())
}
