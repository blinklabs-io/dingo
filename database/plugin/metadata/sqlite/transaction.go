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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language
// governing permissions and limitations under the License.

package sqlite

import (
	"errors"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/models"
	"gorm.io/gorm"
)

// GetTransactionByHash returns a transaction by its hash
func (d *MetadataStoreSqlite) GetTransactionByHash(
	hash []byte,
	txn *gorm.DB,
) (models.Transaction, error) {
	var tx models.Transaction
	var result *gorm.DB

	query := d.DB()
	if txn != nil {
		query = txn
	}

	result = query.Where("hash = ?", hash).First(&tx)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return models.Transaction{}, nil
		}
		return models.Transaction{}, result.Error
	}
	return tx, nil
}

// SetTransaction adds a new transaction to the database
func (d *MetadataStoreSqlite) SetTransaction(
	hash []byte,
	txType string,
	blockHash []byte,
	blockIndex uint32,
	inputs []byte,
	outputs []byte,
	txn *gorm.DB,
) (models.Transaction, error) {
	var tx models.Transaction
	var result *gorm.DB

	query := d.DB()
	if txn != nil {
		query = txn
	}

	tx = models.Transaction{
		Hash:       hash,
		Type:       txType,
		BlockHash:  blockHash,
		BlockIndex: blockIndex,
		Inputs:     inputs,
		Outputs:    outputs,
	}

	result = query.Create(&tx)
	if result.Error != nil {
		return models.Transaction{}, result.Error
	}

	return tx, nil
}
