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

// GetPoolRegistrations returns a list of pool registration certificates
func (d *Database) GetPoolRegistrations(
	poolKeyHash lcommon.PoolKeyHash,
	txn *Txn,
) ([]lcommon.PoolRegistrationCertificate, error) {
	return d.metadata.GetPoolRegistrations(poolKeyHash, txn.Metadata())
}

// GetStakeRegistrations returns a list of stake registration certificates
func (d *Database) GetStakeRegistrations(
	stakingKey []byte,
	txn *Txn,
) ([]lcommon.StakeRegistrationCertificate, error) {
	return d.metadata.GetStakeRegistrations(stakingKey, txn.Metadata())
}

// DeleteCertificatesAfterSlot removes all certificate records added after the given slot.
// This is used during chain rollbacks to undo certificate state changes.
func (d *Database) DeleteCertificatesAfterSlot(slot uint64, txn *Txn) error {
	owned := false
	if txn == nil {
		txn = d.Transaction(true)
		owned = true
		defer txn.Rollback() //nolint:errcheck
	}
	if err := d.metadata.DeleteCertificatesAfterSlot(slot, txn.Metadata()); err != nil {
		return err
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return err
		}
	}
	return nil
}
