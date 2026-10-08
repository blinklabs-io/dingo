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
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
)

// CreateDrep inserts a Drep row directly. See the MetadataStore
// interface for the difference between this and ImportDrep. When txn
// is nil a write transaction is opened, committed on success and
// rolled back on error via Txn.Do; pass an existing write txn to
// participate in a wider unit of work.
func (d *Database) CreateDrep(
	ctx context.Context,
	txn *Txn,
	drep *models.Drep,
) error {
	if txn != nil {
		return d.governanceStore().CreateDrep(txn.Metadata(), drep)
	}
	return d.MetadataTxn(ctx, true).Do(func(t *Txn) error {
		return d.governanceStore().CreateDrep(t.Metadata(), drep)
	})
}

// RestoreDrepStateAtSlot reverts DRep state to the given slot. DReps
// registered only after the slot are deleted; remaining DReps have their
// anchor and active status restored.
func (d *Database) RestoreDrepStateAtSlot(
	ctx context.Context,
	slot uint64,
	txn *Txn,
) error {
	return d.withMetadataWriteTxn(ctx, txn, func(txn *Txn) error {
		if err := d.governanceStore().RestoreDrepStateAtSlot(
			slot,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"failed to restore DRep state at slot %d: %w",
				slot,
				err,
			)
		}
		return nil
	})
}

// GetDrep returns a drep by credential hash only (no tag filter).
// Use for the protocol validation path where only a hash is available.
func (d *Database) GetDrep(
	ctx context.Context,
	cred []byte,
	includeInactive bool,
	txn *Txn,
) (*models.Drep, error) {
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	ret, err := d.governanceStore().
		GetDrep(cred, includeInactive, txn.Metadata())
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, models.ErrDrepNotFound
	}
	return ret, nil
}

// GetDrepByCredential returns a drep by the full credential identity (tag + hash).
func (d *Database) GetDrepByCredential(
	ctx context.Context,
	credentialTag uint8,
	cred []byte,
	includeInactive bool,
	txn *Txn,
) (*models.Drep, error) {
	if txn == nil {
		txn = d.Transaction(ctx, false)
		defer txn.Release()
	}
	ret, err := d.governanceStore().GetDrepByCredential(
		credentialTag, cred, includeInactive, txn.Metadata(),
	)
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, models.ErrDrepNotFound
	}
	return ret, nil
}

// GetActiveDreps returns all active DReps
func (d *Database) GetActiveDreps(
	ctx context.Context,
	txn *Txn,
) ([]*models.Drep, error) {
	if txn == nil {
		txn = d.MetadataTxn(ctx, false)
		defer txn.Release()
	}
	return d.governanceStore().GetActiveDreps(txn.Metadata())
}

// GetDrepLastRegistrationDeposit returns the deposit amount recorded
// against the most recent registration certificate for the DRep
// credential, or nil when no recorded deposit exists.
func (d *Database) GetDrepLastRegistrationDeposit(
	ctx context.Context,
	credentialTag uint8,
	credential []byte,
	txn *Txn,
) (*uint64, error) {
	if txn == nil {
		txn = d.MetadataTxn(ctx, false)
		defer txn.Release()
	}
	return d.governanceStore().GetDrepLastRegistrationDeposit(
		credentialTag,
		credential,
		txn.Metadata(),
	)
}

// InsertDrepIfAbsent inserts a minimal DRep row when no record exists
// for the given credential. Existing rows are left untouched so real
// registration metadata (added_slot, anchor_url, anchor_hash, active)
// is never overwritten by the vote-replay recovery path.
func (d *Database) InsertDrepIfAbsent(
	ctx context.Context,
	credentialTag uint8,
	cred []byte,
	slot uint64,
	url string,
	hash []byte,
	active bool,
	txn *Txn,
) error {
	return d.withMetadataWriteTxn(ctx, txn, func(txn *Txn) error {
		if err := d.governanceStore().InsertDrepIfAbsent(
			credentialTag,
			cred,
			slot,
			url,
			hash,
			active,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf("failed to insert DRep if absent: %w", err)
		}
		return nil
	})
}

// GetDRepVotingPower calculates the voting power for a DRep by summing
// the current stake of all delegated accounts, approximated from live
// UTxO balance plus reward-account balance. credentialTag distinguishes
// key (0) from script (1) DRep credentials sharing the same 28-byte hash.
// expiryEpoch is the CIP-0163 reward-account inactivity gate: 0 = off
// (byte-identical to the pre-CIP query), >0 = exclude accounts whose
// expiration_epoch is nonzero and less than expiryEpoch.
func (d *Database) GetDRepVotingPower(
	ctx context.Context,
	credentialTag uint8,
	drepCredential []byte,
	expiryEpoch uint64,
	txn *Txn,
) (uint64, error) {
	if txn == nil {
		txn = d.MetadataTxn(ctx, false)
		defer txn.Release()
	}
	return d.governanceStore().GetDRepVotingPower(
		credentialTag,
		drepCredential,
		expiryEpoch,
		txn.Metadata(),
	)
}

// GetDRepDelegators returns the stake credentials currently delegating their
// voting power to the given DRep, in canonical (tag, hash) order. This is the
// `delegators` member of the GetDRepState ledger query result. credentialTag
// distinguishes key (0) from script (1) DRep credentials sharing the same hash.
func (d *Database) GetDRepDelegators(
	ctx context.Context,
	credentialTag uint8,
	drepCredential []byte,
	txn *Txn,
) ([]models.StakeCredentialRef, error) {
	if txn == nil {
		txn = d.MetadataTxn(ctx, false)
		defer txn.Release()
	}
	return d.governanceStore().GetDRepDelegators(
		credentialTag,
		drepCredential,
		txn.Metadata(),
	)
}

// GetDRepDelegatorsAtSlot returns the stake credentials delegating to each
// of dreps (every DRep when empty) at slot, keyed by the DRep's
// StakeCredentialRef.MapKey().
func (d *Database) GetDRepDelegatorsAtSlot(
	ctx context.Context,
	dreps []models.StakeCredentialRef,
	slot uint64,
	txn *Txn,
) (map[string][]models.StakeCredentialRef, error) {
	if txn == nil {
		txn = d.MetadataTxn(ctx, false)
		defer txn.Release()
	}
	return d.governanceStore().GetDRepDelegatorsAtSlot(
		dreps,
		slot,
		txn.Metadata(),
	)
}

// GetDrepsAtSlot returns the given DReps (every DRep when refs is empty)
// that were registered at slot, as they stood there.
func (d *Database) GetDrepsAtSlot(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	slot uint64,
	txn *Txn,
) ([]*models.Drep, error) {
	if txn == nil {
		txn = d.MetadataTxn(ctx, false)
		defer txn.Release()
	}
	return d.governanceStore().GetDrepsAtSlot(refs, slot, txn.Metadata())
}

// GetDrepRegistrationDepositsAtSlot returns the deposit recorded against
// the latest registration at or before slot of each of refs (every DRep when
// empty), keyed by models.DrepDepositKey.
func (d *Database) GetDrepRegistrationDepositsAtSlot(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	slot uint64,
	txn *Txn,
) (map[string]uint64, error) {
	if txn == nil {
		txn = d.MetadataTxn(ctx, false)
		defer txn.Release()
	}
	return d.governanceStore().GetDrepRegistrationDepositsAtSlot(
		refs,
		slot,
		txn.Metadata(),
	)
}

// GetDRepVotingPowerBatch is the batch form of GetDRepVotingPower; see
// the metadata-store interface for the contract. expiryEpoch is the
// CIP-0163 gate; see GetDRepVotingPower.
func (d *Database) GetDRepVotingPowerBatch(
	ctx context.Context,
	drepCredentials []models.StakeCredentialRef,
	expiryEpoch uint64,
	txn *Txn,
) (map[string]uint64, error) {
	if txn == nil {
		txn = d.MetadataTxn(ctx, false)
		defer txn.Release()
	}
	result, err := d.governanceStore().GetDRepVotingPowerBatch(
		drepCredentials,
		expiryEpoch,
		txn.Metadata(),
	)
	if err != nil {
		return result, fmt.Errorf(
			"Database.GetDRepVotingPowerBatch: failed to get "+
				"voting power for %d credentials: %w",
			len(drepCredentials),
			err,
		)
	}
	return result, nil
}

// GetDRepVotingPowerByType returns voting power grouped by DRep
// delegation type. expiryEpoch is the CIP-0163 gate; see
// GetDRepVotingPower.
func (d *Database) GetDRepVotingPowerByType(
	ctx context.Context,
	drepTypes []uint64,
	expiryEpoch uint64,
	txn *Txn,
) (map[uint64]uint64, error) {
	if txn == nil {
		txn = d.MetadataTxn(ctx, false)
		defer txn.Release()
	}
	result, err := d.governanceStore().GetDRepVotingPowerByType(
		drepTypes,
		expiryEpoch,
		txn.Metadata(),
	)
	if err != nil {
		return result, fmt.Errorf(
			"Database.GetDRepVotingPowerByType: failed to get "+
				"voting power for types %v: %w",
			drepTypes,
			err,
		)
	}
	return result, nil
}

// UpdateDRepActivity updates the DRep's last activity epoch and
// recalculates the expiry epoch.
func (d *Database) UpdateDRepActivity(
	ctx context.Context,
	credentialTag uint8,
	drepCredential []byte,
	activityEpoch uint64,
	inactivityPeriod uint64,
	slot uint64,
	txn *Txn,
) error {
	return d.withMetadataWriteTxn(ctx, txn, func(txn *Txn) error {
		if err := d.governanceStore().UpdateDRepActivity(
			credentialTag,
			drepCredential,
			activityEpoch,
			inactivityPeriod,
			slot,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"failed to update DRep activity: %w",
				err,
			)
		}
		return nil
	})
}

// BumpDormantDRepExpiries extends active DRep expiries at an empty Conway
// governance boundary. Replaying the same boundary slot is idempotent.
func (d *Database) BumpDormantDRepExpiries(
	ctx context.Context,
	slot uint64,
	txn *Txn,
) (int, error) {
	var affected int
	err := d.withMetadataWriteTxn(ctx, txn, func(txn *Txn) error {
		var err error
		affected, err = d.governanceStore().BumpDormantDRepExpiries(
			slot,
			txn.Metadata(),
		)
		if err != nil {
			return fmt.Errorf("failed to bump dormant DRep expiries: %w", err)
		}
		return nil
	})
	return affected, err
}

func (d *Database) GetDormantDRepEpochs(ctx context.Context, txn *Txn) (uint64, error) {
	if txn == nil {
		txn = d.MetadataTxn(ctx, false)
		defer txn.Release()
	}
	return d.governanceStore().GetDormantDRepEpochs(txn.Metadata())
}

func (d *Database) ResetDormantDRepEpochs(ctx context.Context, slot uint64, txn *Txn) error {
	return d.withMetadataWriteTxn(ctx, txn, func(txn *Txn) error {
		if err := d.governanceStore().ResetDormantDRepEpochs(
			slot,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf("failed to reset dormant DRep epoch count: %w", err)
		}
		return nil
	})
}

func (d *Database) SetImportedDormantDRepEpochs(
	ctx context.Context,
	dormantEpochs uint64,
	txn *Txn,
) error {
	return d.withMetadataWriteTxn(ctx, txn, func(txn *Txn) error {
		if err := d.governanceStore().SetImportedDormantDRepEpochs(
			dormantEpochs,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf("failed to import dormant DRep epoch count: %w", err)
		}
		return nil
	})
}

// RecordDRepActivityEpoch sets a DRep's last activity epoch without touching
// its expiry. Historical replay below a Mithril anchor uses it: the snapshot's
// DRepState expiry already reflects the dormant-epoch rules replay does not
// run, so recomputing expiry from a historical vote or certificate would
// replace a correct value with a stale one.
func (d *Database) RecordDRepActivityEpoch(
	ctx context.Context,
	credentialTag uint8,
	drepCredential []byte,
	activityEpoch uint64,
	txn *Txn,
) error {
	return d.withMetadataWriteTxn(ctx, txn, func(txn *Txn) error {
		if err := d.governanceStore().RecordDRepActivityEpoch(
			credentialTag,
			drepCredential,
			activityEpoch,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"failed to record DRep activity epoch: %w",
				err,
			)
		}
		return nil
	})
}

// GetExpiredDReps returns all active DReps whose expiry epoch is at
// or before the given epoch.
func (d *Database) GetExpiredDReps(
	ctx context.Context,
	epoch uint64,
	txn *Txn,
) ([]*models.Drep, error) {
	if txn == nil {
		txn = d.MetadataTxn(ctx, false)
		defer txn.Release()
	}
	return d.governanceStore().GetExpiredDReps(epoch, txn.Metadata())
}

// GetDrepLastRegistrationDeposits returns the most recent registration
// deposit of every active DRep, keyed by models.DrepDepositKey, so a caller
// listing all active DReps does not need one query per DRep. Credentials
// with no registration_drep row are absent from the map.
func (d *Database) GetDrepLastRegistrationDeposits(
	ctx context.Context,
	txn *Txn,
) (map[string]uint64, error) {
	if txn == nil {
		txn = d.MetadataTxn(ctx, false)
		defer txn.Release()
	}
	return d.governanceStore().GetDrepLastRegistrationDeposits(txn.Metadata())
}
