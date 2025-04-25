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

type Account struct {
	ID         uint   `gorm:"primarykey"`
	StakingKey []byte `gorm:"index,unique"`
	Pool       []byte `gorm:"index"`
	Drep       []byte `gorm:"index"`
	AddedSlot  uint64
	Active     bool
}

func (a *Account) TableName() string {
	return "account"
}

// GetAccount returns an account by staking key
func (d *Database) GetAccount(
	stakeKey []byte,
	txn *Txn,
) (Account, error) {
	tmpAccount := Account{}
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	account, err := d.metadata.GetAccount(stakeKey, txn.Metadata())
	if err != nil {
		return tmpAccount, err
	}
	tmpAccount = Account(account)
	return tmpAccount, nil
}

// SetAccount saves an account
func (d *Database) SetAccount(
	stakeKey, pkh, drep []byte,
	slot uint64,
	active bool,
	txn *Txn,
) error {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Commit() //nolint:errcheck
	}
	return d.metadata.SetAccount(
		stakeKey,
		pkh,
		drep,
		slot,
		active,
		txn.Metadata(),
	)
}

// SetRegistration saves a registration certificate
func (d *Database) SetRegistration(
	cert *lcommon.RegistrationCertificate,
	slot, deposit uint64,
	txn *Txn,
) error {
	return d.metadata.SetRegistration(
		cert,
		slot,
		deposit,
		txn.Metadata(),
	)
}

// SetStakeDelegation saves a stake delegation certificate
func (d *Database) SetStakeDelegation(
	cert *lcommon.StakeDelegationCertificate,
	slot uint64,
	txn *Txn,
) error {
	return d.metadata.SetStakeDelegation(
		cert,
		slot,
		txn.Metadata(),
	)
}

// SetStakeDeregistration saves a stake deregistration certificate
func (d *Database) SetStakeDeregistration(
	cert *lcommon.StakeDeregistrationCertificate,
	slot uint64,
	txn *Txn,
) error {
	return d.metadata.SetStakeDeregistration(
		cert,
		slot,
		txn.Metadata(),
	)
}

// SetStakeRegistration saves a stake registration certificate
func (d *Database) SetStakeRegistration(
	cert *lcommon.StakeRegistrationCertificate,
	slot, deposit uint64,
	txn *Txn,
) error {
	return d.metadata.SetStakeRegistration(
		cert,
		slot,
		deposit,
		txn.Metadata(),
	)
}
