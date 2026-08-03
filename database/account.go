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

// CreateAccount inserts an Account row directly. See the MetadataStore
// interface for the difference between this and ImportAccount. When txn
// is nil a write transaction is opened, committed on success and rolled
// back on error via Txn.Do; pass an existing write txn to participate
// in a wider unit of work.
func (d *Database) CreateAccount(txn *Txn, account *models.Account) error {
	if txn != nil {
		return d.metadata.CreateAccount(txn.Metadata(), account)
	}
	return d.MetadataTxn(true).Do(func(t *Txn) error {
		return d.metadata.CreateAccount(t.Metadata(), account)
	})
}

// RenewAccountExpirations sets the CIP-0163 expirationEpoch for the given
// reward-account credentials. See the metadata store implementation for
// semantics (refs with no matching account row are ignored). When txn is
// nil a write transaction is opened, committed on success and rolled back
// on error via Txn.Do; pass an existing write txn to participate in a
// wider unit of work.
func (d *Database) RenewAccountExpirations(
	refs []models.StakeCredentialRef,
	expirationEpoch uint64,
	txn *Txn,
) error {
	if txn != nil {
		return d.metadata.RenewAccountExpirations(refs, expirationEpoch, txn.Metadata())
	}
	return d.MetadataTxn(true).Do(func(t *Txn) error {
		return d.metadata.RenewAccountExpirations(refs, expirationEpoch, t.Metadata())
	})
}

// AccountLastWitnessSlots returns, per requested credential, the greatest
// CIP-0163 witnessing slot <= maxSlot across the stake-witnessing certificate
// tables and the reward-withdrawal history, keyed by
// StakeCredentialRef.MapKey(). A credential with no witness <= maxSlot is
// absent from the map. When txn is nil a read transaction is opened for the
// query; pass an existing txn to read within a wider unit of work.
func (d *Database) AccountLastWitnessSlots(
	refs []models.StakeCredentialRef,
	maxSlot uint64,
	txn *Txn,
) (map[string]uint64, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	return d.metadata.AccountLastWitnessSlots(refs, maxSlot, txn.Metadata())
}

// AccountsWitnessedAfterSlot returns the distinct reward-account credentials
// witnessed (via a stake-witnessing certificate or a reward withdrawal) at a
// slot greater than the given slot — the CIP-0163 rollback affected set. It
// must be called before rolled-back certificate and reward-delta rows are
// deleted. When txn is nil a read transaction is opened for the query; pass an
// existing txn to read within a wider unit of work.
func (d *Database) AccountsWitnessedAfterSlot(
	slot uint64,
	txn *Txn,
) ([]models.StakeCredentialRef, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	return d.metadata.AccountsWitnessedAfterSlot(slot, txn.Metadata())
}

// StampAllActiveAccountExpirations sets the CIP-0163 expirationEpoch for every
// active account row. Used once at activation to give every pre-existing
// account a full inactivity window from the activation epoch, including
// accounts witnessed before activation. Returns the number of rows stamped.
// When txn is nil a write transaction is opened, committed on success and
// rolled back on error via Txn.Do; pass an existing write txn to participate
// in a wider unit of work.
func (d *Database) StampAllActiveAccountExpirations(
	expirationEpoch uint64,
	txn *Txn,
) (int64, error) {
	if txn != nil {
		return d.metadata.StampAllActiveAccountExpirations(expirationEpoch, txn.Metadata())
	}
	var rows int64
	err := d.MetadataTxn(true).Do(func(t *Txn) error {
		var doErr error
		rows, doErr = d.metadata.StampAllActiveAccountExpirations(expirationEpoch, t.Metadata())
		return doErr
	})
	return rows, err
}

// AccountInactivityActivationMembership returns the requested credentials that
// were included in the one-time CIP-0163 activation stamp.
func (d *Database) AccountInactivityActivationMembership(
	refs []models.StakeCredentialRef,
	txn *Txn,
) (map[string]struct{}, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	return d.metadata.AccountInactivityActivationMembership(
		refs,
		txn.Metadata(),
	)
}

// ResetAccountExpirationActivation clears expiration from every account in the
// durable activation-membership set, deletes that set, and returns the affected
// credentials so the ledger can reconstruct any overwritten pre-activation
// witness expiration. When txn is nil, the operation runs in its own write
// transaction.
func (d *Database) ResetAccountExpirationActivation(
	txn *Txn,
) ([]models.StakeCredentialRef, error) {
	if txn != nil {
		return d.metadata.ResetAccountExpirationActivation(txn.Metadata())
	}
	var refs []models.StakeCredentialRef
	err := d.MetadataTxn(true).Do(func(t *Txn) error {
		var doErr error
		refs, doErr = d.metadata.ResetAccountExpirationActivation(
			t.Metadata(),
		)
		return doErr
	})
	return refs, err
}

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

// GetAccountByCredential returns an account by staking credential tag and key.
func (d *Database) GetAccountByCredential(
	credentialTag uint8,
	stakeKey []byte,
	includeInactive bool,
	txn *Txn,
) (*models.Account, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	account, err := d.metadata.GetAccountByCredential(
		credentialTag,
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

// GetAccountsByCredential returns accounts for the given staking credentials in
// a single query, keyed by StakeCredentialRef.MapKey().
func (d *Database) GetAccountsByCredential(
	refs []models.StakeCredentialRef,
	includeInactive bool,
	txn *Txn,
) (map[string]*models.Account, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	return d.metadata.GetAccountsByCredential(
		refs,
		includeInactive,
		txn.Metadata(),
	)
}

// AddAccountRewardByCredential credits the reward balance for a registered
// account identified by stake credential tag and key. sourceHash uniquely
// identifies the credit event (refunded proposal identity hash, reaped pool
// key hash, or synthetic MIR event discriminator); it makes each
// epoch-boundary credit a distinct rollback-aware journal row while letting a
// crash-replayed boundary map onto the existing row and skip idempotently.
// Pass nil when no per-event discriminator is available.
func (d *Database) AddAccountRewardByCredential(
	credentialTag uint8,
	stakeKey []byte,
	amount uint64,
	slot uint64,
	sourceHash []byte,
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
	if err := d.metadata.AddAccountRewardByCredential(
		credentialTag,
		stakeKey,
		amount,
		slot,
		sourceHash,
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

// DeleteAccountRewardsAfterSlot reverts reward-account balance changes recorded
// after the given slot. Used during chain rollback for governance credits and
// transaction withdrawals.
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
