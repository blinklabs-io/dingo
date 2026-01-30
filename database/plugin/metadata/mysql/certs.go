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

package mysql

import (
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

// GetStakeRegistrations returns stake registration certificates
func (d *MetadataStoreMysql) GetStakeRegistrations(
	stakingKey []byte,
	txn types.Txn,
) ([]lcommon.StakeRegistrationCertificate, error) {
	ret := []lcommon.StakeRegistrationCertificate{}
	certs := []models.StakeRegistration{}
	db, err := d.resolveDB(txn)
	if err != nil {
		return ret, err
	}
	result := db.Where("staking_key = ?", stakingKey).
		Order("id DESC").
		Find(&certs)
	if result.Error != nil {
		return ret, result.Error
	}
	var tmpCert lcommon.StakeRegistrationCertificate
	for _, cert := range certs {
		tmpCert = lcommon.StakeRegistrationCertificate{
			CertType: uint(lcommon.CertificateTypeStakeRegistration),
			StakeCredential: lcommon.Credential{
				// TODO: determine correct type
				// CredType: lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.CredentialHash(cert.StakingKey),
			},
		}
		ret = append(ret, tmpCert)
	}
	return ret, nil
}

// DeleteCertificatesAfterSlot removes all certificate records added after the given slot.
// This is used during chain rollbacks to undo certificate state changes.
// All deletions are performed atomically within a transaction to ensure consistency.
func (d *MetadataStoreMysql) DeleteCertificatesAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	// Delete all specialized certificate tables.
	// We explicitly delete child records for PoolRegistration (Owners, Relays) and
	// MoveInstantaneousRewards (Rewards) before deleting their parents.
	deleteCerts := func(tx *gorm.DB) error {
		// Delete PoolRegistrationOwner and PoolRegistrationRelay records
		// that belong to PoolRegistration records being deleted.
		poolRegIDsSubquery := tx.Model(&models.PoolRegistration{}).
			Select("id").
			Where("added_slot > ?", slot)
		if result := tx.Where(
			"pool_registration_id IN (?)",
			poolRegIDsSubquery,
		).Delete(&models.PoolRegistrationOwner{}); result.Error != nil {
			return result.Error
		}
		if result := tx.Where(
			"pool_registration_id IN (?)",
			poolRegIDsSubquery,
		).Delete(&models.PoolRegistrationRelay{}); result.Error != nil {
			return result.Error
		}

		// Delete MoveInstantaneousRewardsReward records that belong to
		// MoveInstantaneousRewards records being deleted.
		mirIDsSubquery := tx.Model(&models.MoveInstantaneousRewards{}).
			Select("id").
			Where("added_slot > ?", slot)
		if result := tx.Where(
			"mir_id IN (?)",
			mirIDsSubquery,
		).Delete(&models.MoveInstantaneousRewardsReward{}); result.Error != nil {
			return result.Error
		}

		certTables := []any{
			&models.StakeRegistration{},
			&models.StakeDelegation{},
			&models.StakeDeregistration{},
			&models.StakeRegistrationDelegation{},
			&models.StakeVoteDelegation{},
			&models.StakeVoteRegistrationDelegation{},
			&models.VoteDelegation{},
			&models.VoteRegistrationDelegation{},
			&models.Registration{},
			&models.Deregistration{},
			&models.PoolRegistration{},
			&models.PoolRetirement{},
			&models.RegistrationDrep{},
			&models.DeregistrationDrep{},
			&models.UpdateDrep{},
			&models.AuthCommitteeHot{},
			&models.ResignCommitteeCold{},
			&models.MoveInstantaneousRewards{},
		}

		for _, table := range certTables {
			if result := tx.Where("added_slot > ?", slot).Delete(table); result.Error != nil {
				return result.Error
			}
		}

		// Delete unified certificate records (Certificate table uses 'slot' not 'added_slot')
		if result := tx.Where("slot > ?", slot).Delete(&models.Certificate{}); result.Error != nil {
			return result.Error
		}

		return nil
	}

	// If caller provided a transaction, use it directly.
	// Otherwise, wrap in a transaction for atomicity.
	if txn != nil {
		return deleteCerts(db)
	}
	return db.Transaction(deleteCerts)
}
