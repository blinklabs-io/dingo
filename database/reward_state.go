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

package database

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// RebuildRewardLiveStake rebuilds the live reward stake aggregate from
// canonical account and live UTxO metadata.
func (d *Database) RebuildRewardLiveStake(slot uint64, txn *Txn) error {
	if txn == nil {
		return d.MetadataTxn(true).Do(func(t *Txn) error {
			return d.metadata.RebuildRewardLiveStake(slot, t.Metadata())
		})
	}
	if txn.db != d || txn.Metadata() == nil {
		return fmt.Errorf(
			"rebuild reward live stake: %w",
			types.ErrTxnWrongType,
		)
	}
	if !txn.IsReadWrite() {
		return fmt.Errorf(
			"rebuild reward live stake: %w",
			types.ErrTxnWrongType,
		)
	}
	if err := d.metadata.RebuildRewardLiveStake(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf("rebuild reward live stake at slot %d: %w", slot, err)
	}
	return nil
}

// DeleteRewardStateAfterSlot deletes reward-state rows captured from
// rolled-back blocks.
func (d *Database) DeleteRewardStateAfterSlot(
	slot uint64,
	txn *Txn,
) error {
	if txn == nil {
		return d.metadata.DeleteRewardStateAfterSlot(slot, nil)
	}
	if err := txn.db.metadata.DeleteRewardStateAfterSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf("delete reward state after slot %d: %w", slot, err)
	}
	return nil
}

// GetRewardAccountOutputsByCredential returns reward account output rows for
// a stake credential across every epoch that has not yet been pruned,
// paginated and ordered by epoch. Used by the Blockfrost account
// reward-history endpoint (GET /accounts/{stake_address}/rewards).
func (d *Database) GetRewardAccountOutputsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	txn *Txn,
) ([]*models.RewardAccountOutput, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	rows, err := d.metadata.GetRewardAccountOutputsByCredential(
		credentialTag,
		stakingKey,
		limit,
		offset,
		order,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get reward account outputs by credential: %w",
			err,
		)
	}
	return rows, nil
}

// CountRewardAccountOutputsByCredential returns the total count of reward
// account output rows for a stake credential.
func (d *Database) CountRewardAccountOutputsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn *Txn,
) (int, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	count, err := d.metadata.CountRewardAccountOutputsByCredential(
		credentialTag,
		stakingKey,
		txn.Metadata(),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"count reward account outputs by credential: %w",
			err,
		)
	}
	return count, nil
}
