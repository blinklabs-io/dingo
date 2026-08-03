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
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/importutil"
	"github.com/blinklabs-io/dingo/database/types"
)

// GetActiveAccountCredentials returns the stake credentials of every active
// account. See metadata.MetadataStore for the contract.
func (d *MetadataStoreMysql) GetActiveAccountCredentials(
	txn types.Txn,
) ([]models.StakeCredentialRef, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf("GetActiveAccountCredentials: resolve db: %w", err)
	}
	var rows []struct {
		CredentialTag uint8  `gorm:"column:credential_tag"`
		StakingKey    []byte `gorm:"column:staking_key"`
	}
	if res := db.
		Model(&models.Account{}).
		Where("active = ?", true).
		Select("credential_tag", "staking_key").
		Find(&rows); res.Error != nil {
		return nil, fmt.Errorf("GetActiveAccountCredentials: %w", res.Error)
	}
	out := make([]models.StakeCredentialRef, 0, len(rows))
	for _, r := range rows {
		out = append(out, models.StakeCredentialRef{
			Tag: r.CredentialTag,
			Key: r.StakingKey,
		})
	}
	return out, nil
}

// DeactivateAccounts marks the given accounts inactive. See
// metadata.MetadataStore for the contract.
func (d *MetadataStoreMysql) DeactivateAccounts(
	txn types.Txn,
	creds []models.StakeCredentialRef,
) error {
	if len(creds) == 0 {
		return nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("DeactivateAccounts: resolve db: %w", err)
	}
	return importutil.DeactivateAccounts(db, creds, mysqlBatchChunkSize)
}

// DeactivateDreps marks the given DReps inactive. See
// metadata.MetadataStore for the contract.
func (d *MetadataStoreMysql) DeactivateDreps(
	txn types.Txn,
	creds []models.StakeCredentialRef,
) error {
	if len(creds) == 0 {
		return nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("DeactivateDreps: resolve db: %w", err)
	}
	return importutil.DeactivateDreps(db, creds, mysqlBatchChunkSize)
}

// RetirePools records a retirement at the given epoch for each supplied pool
// key hash. See metadata.MetadataStore for the contract.
func (d *MetadataStoreMysql) RetirePools(
	txn types.Txn,
	poolKeyHashes [][]byte,
	epoch uint64,
	addedSlot uint64,
) error {
	if len(poolKeyHashes) == 0 {
		return nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("RetirePools: resolve db: %w", err)
	}
	return importutil.RetirePools(
		db, poolKeyHashes, epoch, addedSlot, mysqlBatchChunkSize,
	)
}
