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

//go:build dingo_extra_plugins

package mysql

import (
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
)

// CreateDrep inserts a Drep row directly. See the MetadataStore
// interface for semantics.
func (d *MetadataStoreMysql) CreateDrep(
	txn types.Txn,
	drep *models.Drep,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Create(drep).Error
}

// CreateAccount inserts an Account row directly. See the MetadataStore
// interface for semantics.
func (d *MetadataStoreMysql) CreateAccount(
	txn types.Txn,
	account *models.Account,
) error {
	create := func(db *gorm.DB) error {
		if err := db.Create(account).Error; err != nil {
			return err
		}
		return refreshRewardLiveStakeAggregate(
			db,
			models.NewStakeCredentialRef(
				account.CredentialTag,
				account.StakingKey,
			),
			account.AddedSlot,
		)
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if txn == nil {
		return db.Transaction(create)
	}
	return create(db)
}

// CreateUtxo inserts a Utxo row directly. See the MetadataStore
// interface for semantics.
func (d *MetadataStoreMysql) CreateUtxo(
	txn types.Txn,
	utxo *models.Utxo,
) error {
	create := func(db *gorm.DB) error {
		if err := db.Create(utxo).Error; err != nil {
			return err
		}
		return refreshRewardLiveStakeAggregates(
			db,
			rewardStakeRefsFromUtxos([]models.Utxo{*utxo}),
		)
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if txn == nil {
		return db.Transaction(create)
	}
	return create(db)
}
