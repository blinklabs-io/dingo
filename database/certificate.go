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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// Certificate persistence is handled by SetTransaction.
// The ledger layer calculates deposits and calls SetTransaction
// to persist transactions and certificates together in a single operation.

// GetPoolRegistrations returns pool registration certificates for the given pool key hash
func (d *Database) GetPoolRegistrations(
	poolKeyHash lcommon.PoolKeyHash,
	txn *Txn,
) ([]lcommon.PoolRegistrationCertificate, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	return d.metadata.GetPoolRegistrations(poolKeyHash, txn.Metadata())
}

// GetStakeRegistrations returns stake registration certificates for the given staking key
func (d *Database) GetStakeRegistrations(
	stakingKey []byte,
	txn *Txn,
) ([]lcommon.StakeRegistrationCertificate, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	return d.metadata.GetStakeRegistrations(stakingKey, txn.Metadata())
}
