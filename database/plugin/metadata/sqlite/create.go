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

package sqlite

import (
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// CreateDrep inserts a Drep row directly. See the MetadataStore
// interface for semantics.
func (d *MetadataStoreSqlite) CreateDrep(
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
func (d *MetadataStoreSqlite) CreateAccount(
	txn types.Txn,
	account *models.Account,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Create(account).Error
}

// CreateUtxo inserts a Utxo row directly. See the MetadataStore
// interface for semantics.
func (d *MetadataStoreSqlite) CreateUtxo(
	txn types.Txn,
	utxo *models.Utxo,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Create(utxo).Error
}
