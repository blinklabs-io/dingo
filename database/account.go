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
	"github.com/btcsuite/btcd/btcutil/bech32"
)

type Account struct {
	ID            uint   `gorm:"primarykey"`
	StakingKey    []byte `gorm:"index,unique"`
	Pool          []byte `gorm:"index"`
	Drep          []byte `gorm:"index"`
	DepositAmount uint64
	AddedSlot     uint64
	Active        bool
}

func (a *Account) TableName() string {
	return "account"
}

func (a *Account) String() string {
	// Convert data to base32 and encode as bech32
	convData, err := bech32.ConvertBits(a.StakingKey[:], 8, 5, true)
	if err != nil {
		return ""
	}
	encoded, err := bech32.Encode("stake", convData)
	if err != nil {
		return ""
	}
	return encoded
}

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

func (d *Database) SetAccount(
	stakeKey, pkh, drep []byte,
	slot, amount uint64,
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
		amount,
		txn.Metadata(),
	)
}
