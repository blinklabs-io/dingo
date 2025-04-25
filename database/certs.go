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

// SetPoolRegistration saves a pool registration certificate
func (d *Database) SetPoolRegistration(
	cert *lcommon.PoolRegistrationCertificate,
	slot, deposit uint64, // slot
	txn *Txn,
) error {
	return d.metadata.SetPoolRegistration(cert, slot, deposit, txn.Metadata())
}

// SetPoolRetirement saves a pool retirement certificate
func (d *Database) SetPoolRetirement(
	cert *lcommon.PoolRetirementCertificate,
	slot uint64,
	txn *Txn,
) error {
	return d.metadata.SetPoolRetirement(cert, slot, txn.Metadata())
}

// SetStakeDeregistration saves a stake deregistration certificate
func (d *Database) SetStakeDeregistration(
	cert *lcommon.StakeDeregistrationCertificate,
	slot uint64,
	txn *Txn,
) error {
	return d.metadata.SetStakeDeregistration(cert, slot, txn.Metadata())
}
