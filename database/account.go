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
)

// ClearDanglingDRepDelegations applies the cardano-ledger Conway HARDFORK
// STS rule for protocol major version 10 (Plomin, mainnet January 2025): any
// account with a credential-backed DRep delegation (DrepType 0 or 1) whose
// target DRep credential is not currently registered as an active DRep has
// its delegation cleared. Account.AddedSlot is updated to atSlot so the
// rewritten row is excluded from a rollback restore targeting any slot
// before atSlot (the restore filters on `added_slot <= targetSlot` and picks
// up the prior certificate history instead). Pseudo-DRep delegations
// (AlwaysAbstain, AlwaysNoConfidence) are preserved. Returns the number of
// accounts updated.
//
// See cardano-ledger Conway/Rules/HardFork.hs (updateDRepDelegations).
func (d *Database) ClearDanglingDRepDelegations(
	atSlot uint64,
	txn *Txn,
) (int, error) {
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
	n, err := d.metadata.ClearDanglingDRepDelegations(
		atSlot,
		txn.Metadata(),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"clear dangling drep delegations at slot %d: %w",
			atSlot,
			err,
		)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return 0, fmt.Errorf("commit transaction: %w", err)
		}
		owned = false
	}
	return n, nil
}

// RestoreAccountStateAtSlot reverts account delegation state to the given
// slot. For accounts modified after the slot, this restores their Pool and
// Drep delegations to the state they had at the given slot, or deletes them
// if they were registered after that slot.
func (d *Database) RestoreAccountStateAtSlot(
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
	if err := d.metadata.RestoreAccountStateAtSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to restore account state at slot %d: %w",
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

// GetAccount returns an account by staking key
func (d *Database) GetAccount(
	stakeKey []byte,
	includeInactive bool,
	txn *Txn,
) (*models.Account, error) {
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}
	account, err := d.metadata.GetAccount(
		stakeKey,
		includeInactive,
		txn.Metadata(),
	)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, models.ErrAccountNotFound
	}
	return account, nil
}

// AddAccountReward credits the reward balance for a registered account.
func (d *Database) AddAccountReward(
	stakeKey []byte,
	amount uint64,
	slot uint64,
	txn *Txn,
) error {
	if amount == 0 {
		return nil
	}
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
	if err := d.metadata.AddAccountReward(
		stakeKey,
		amount,
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf("failed to add account reward: %w", err)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
		owned = false
	}
	return nil
}

// DeleteAccountRewardsAfterSlot reverts reward-account credits recorded after
// the given slot. Used during chain rollback for governance deposit refunds
// and treasury withdrawals.
func (d *Database) DeleteAccountRewardsAfterSlot(
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
	if err := d.metadata.DeleteAccountRewardsAfterSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to delete account reward deltas after slot %d: %w",
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
