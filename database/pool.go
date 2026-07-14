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
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// RestorePoolStateAtSlot reverts pool state to the given slot. Pools
// registered only after the slot are deleted; remaining pools have their
// denormalized fields restored from the most recent registration at or
// before the slot.
func (d *Database) RestorePoolStateAtSlot(
	slot uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.MetadataTxn(true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	if err := d.metadata.RestorePoolStateAtSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to restore pool state at slot %d: %w",
			slot,
			err,
		)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
		owned = false
	}
	return nil
}

// GetPool returns a pool by its key hash
func (d *Database) GetPool(
	pkh lcommon.PoolKeyHash,
	includeInactive bool,
	txn *Txn,
) (*models.Pool, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
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

// ImportPool upserts a pool and creates a registration record. A supplied txn
// must be writable and include a metadata handle. When txn is nil a write
// transaction is opened, committed on success and rolled back on error via
// Txn.Do.
func (d *Database) ImportPool(
	txn *Txn,
	pool *models.Pool,
	reg *models.PoolRegistration,
) error {
	if txn != nil {
		if txn.Metadata() == nil {
			return fmt.Errorf("import pool: %w", types.ErrNilTxn)
		}
		if !txn.IsReadWrite() {
			return fmt.Errorf("import pool: %w", types.ErrTxnWrongType)
		}
		return d.metadata.ImportPool(pool, reg, txn.Metadata())
	}
	return d.MetadataTxn(true).Do(func(t *Txn) error {
		return d.metadata.ImportPool(pool, reg, t.Metadata())
	})
}

// UpdatePoolOpCertSequence records an observed op-cert sequence for a pool
// and updates the pool's denormalized maximum.
func (d *Database) UpdatePoolOpCertSequence(
	pkh lcommon.PoolKeyHash,
	sequence uint64,
	slot uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.MetadataTxn(true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	if err := d.metadata.UpdatePoolOpCertSequence(
		pkh,
		sequence,
		slot,
		txn.Metadata(),
	); err != nil {
		return err
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
		owned = false
	}
	return nil
}

// LatestPoolOpCertSequence returns the highest observed op-cert sequence for
// a pool.
func (d *Database) LatestPoolOpCertSequence(
	pkh lcommon.PoolKeyHash,
	txn *Txn,
) (uint64, bool, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	return d.metadata.LatestPoolOpCertSequence(pkh, txn.Metadata())
}

// GetPools returns pools by key hash.
func (d *Database) GetPools(
	pkhs []lcommon.PoolKeyHash,
	txn *Txn,
) ([]models.Pool, error) {
	if len(pkhs) == 0 {
		return []models.Pool{}, nil
	}
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	return d.metadata.GetPools(pkhs, txn.Metadata())
}

// GetPoolByVrfKeyHash retrieves an active pool by its VRF key hash.
// Returns nil if no active pool uses this VRF key.
func (d *Database) GetPoolByVrfKeyHash(
	vrfKeyHash []byte,
	txn *Txn,
) (*models.Pool, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	return d.metadata.GetPoolByVrfKeyHash(vrfKeyHash, txn.Metadata())
}

// GetActivePoolRelays returns all relays from currently active pools.
// This is used for ledger peer discovery.
func (d *Database) GetActivePoolRelays(
	txn *Txn,
) ([]models.PoolRegistrationRelay, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	return d.metadata.GetActivePoolRelays(txn.Metadata())
}

// GetActivePoolKeyHashes returns the key hashes of all currently active
// (registered, non-retired) stake pools. This backs the GetStakePools
// local-state-query.
func (d *Database) GetActivePoolKeyHashes(
	txn *Txn,
) ([][]byte, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	return d.metadata.GetActivePoolKeyHashes(txn.Metadata())
}
