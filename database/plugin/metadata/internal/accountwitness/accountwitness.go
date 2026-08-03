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

package accountwitness

import (
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// RecordWithdrawal persists a rollback-aware CIP-0163 withdrawal witness.
func RecordWithdrawal(db *gorm.DB, ref models.StakeCredentialRef, txHash []byte, slot uint64) error {
	if txHash == nil {
		txHash = []byte{}
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(
		&models.AccountWithdrawalWitness{
			StakingKey: ref.Key, CredentialTag: ref.Tag, TxHash: txHash, AddedSlot: slot,
		},
	).Error
}

// ActivationMembership returns the requested credentials that were included in
// the one-time CIP-0163 activation stamp.
func ActivationMembership(
	db *gorm.DB,
	refs []models.StakeCredentialRef,
	batchSize int,
) (map[string]struct{}, error) {
	ret := make(map[string]struct{}, len(refs))
	if len(refs) == 0 {
		return ret, nil
	}
	requested := make(map[string]struct{}, len(refs))
	seenHash := make(map[string]struct{}, len(refs))
	stakingKeys := make([][]byte, 0, len(refs))
	for _, ref := range refs {
		requested[ref.MapKey()] = struct{}{}
		hash := string(ref.Key)
		if _, ok := seenHash[hash]; ok {
			continue
		}
		seenHash[hash] = struct{}{}
		stakingKeys = append(stakingKeys, ref.Key)
	}
	for keyChunk := range slices.Chunk(stakingKeys, batchSize) {
		var rows []models.AccountInactivityActivation
		if err := db.Where("staking_key IN ?", keyChunk).
			Find(&rows).Error; err != nil {
			return nil, err
		}
		for _, row := range rows {
			key := models.NewStakeCredentialRef(
				row.CredentialTag,
				row.StakingKey,
			).MapKey()
			if _, ok := requested[key]; ok {
				ret[key] = struct{}{}
			}
		}
	}
	return ret, nil
}

// CertTableNames returns the certificate tables that witness reward accounts
// under CIP-0163.
func CertTableNames() []string {
	return []string{
		models.StakeRegistration{}.TableName(),
		models.StakeRegistrationDelegation{}.TableName(),
		models.StakeVoteRegistrationDelegation{}.TableName(),
		models.VoteRegistrationDelegation{}.TableName(),
		models.Registration{}.TableName(),
		models.StakeDeregistration{}.TableName(),
		models.Deregistration{}.TableName(),
		models.StakeDelegation{}.TableName(),
		models.StakeVoteDelegation{}.TableName(),
		models.VoteDelegation{}.TableName(),
	}
}

type witnessRow struct {
	CredentialTag uint8
	StakingKey    []byte
	LastSlot      uint64
}

// LastSlots returns the latest surviving witness slot for each requested
// credential. db carries the caller's resolved transaction context.
func LastSlots(
	db *gorm.DB,
	refs []models.StakeCredentialRef,
	maxSlot uint64,
	batchSize int,
) (map[string]uint64, error) {
	result := make(map[string]uint64, len(refs))
	if len(refs) == 0 {
		return result, nil
	}
	requested := make(map[string]struct{}, len(refs))
	seenHash := make(map[string]struct{}, len(refs))
	stakingKeys := make([][]byte, 0, len(refs))
	for _, ref := range refs {
		requested[ref.MapKey()] = struct{}{}
		h := string(ref.Key)
		if _, ok := seenHash[h]; !ok {
			seenHash[h] = struct{}{}
			stakingKeys = append(stakingKeys, ref.Key)
		}
	}
	merge := func(rows []witnessRow) {
		for _, row := range rows {
			key := models.NewStakeCredentialRef(row.CredentialTag, row.StakingKey).MapKey()
			if _, ok := requested[key]; !ok {
				continue
			}
			if current, ok := result[key]; !ok || row.LastSlot > current {
				result[key] = row.LastSlot
			}
		}
	}
	for keyChunk := range slices.Chunk(stakingKeys, batchSize) {
		for _, table := range CertTableNames() {
			var rows []witnessRow
			if err := db.Table(table).
				Select("credential_tag, staking_key, MAX(added_slot) AS last_slot").
				Where("staking_key IN ? AND added_slot <= ?", keyChunk, maxSlot).
				Group("credential_tag, staking_key").
				Scan(&rows).Error; err != nil {
				return nil, err
			}
			merge(rows)
		}
		var rows []witnessRow
		if err := db.Table(models.AccountWithdrawalWitness{}.TableName()).
			Select("credential_tag, staking_key, MAX(added_slot) AS last_slot").
			Where("staking_key IN ? AND added_slot <= ?", keyChunk, maxSlot).
			Group("credential_tag, staking_key").Scan(&rows).Error; err != nil {
			return nil, err
		}
		merge(rows)
		rows = nil
		if err := db.Table(models.AccountRewardDelta{}.TableName()).
			Select("credential_tag, staking_key, MAX(added_slot) AS last_slot").
			Where("withdrawal = ? AND staking_key IN ? AND added_slot <= ?", true, keyChunk, maxSlot).
			Group("credential_tag, staking_key").
			Scan(&rows).Error; err != nil {
			return nil, err
		}
		merge(rows)
	}
	return result, nil
}

// AfterSlot returns the distinct credentials witnessed after slot. db carries
// the caller's resolved transaction context.
func AfterSlot(db *gorm.DB, slot uint64) ([]models.StakeCredentialRef, error) {
	seen := make(map[string]struct{})
	var refs []models.StakeCredentialRef
	collect := func(rows []witnessRow) {
		for _, row := range rows {
			ref := models.NewStakeCredentialRef(row.CredentialTag, row.StakingKey)
			key := ref.MapKey()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			refs = append(refs, ref)
		}
	}
	for _, table := range CertTableNames() {
		var rows []witnessRow
		if err := db.Table(table).
			Select("credential_tag, staking_key").
			Where("added_slot > ?", slot).
			Group("credential_tag, staking_key").
			Scan(&rows).Error; err != nil {
			return nil, err
		}
		collect(rows)
	}
	var rows []witnessRow
	if err := db.Table(models.AccountWithdrawalWitness{}.TableName()).
		Select("credential_tag, staking_key").Where("added_slot > ?", slot).
		Group("credential_tag, staking_key").Scan(&rows).Error; err != nil {
		return nil, err
	}
	collect(rows)
	rows = nil
	if err := db.Table(models.AccountRewardDelta{}.TableName()).
		Select("credential_tag, staking_key").
		Where("withdrawal = ? AND added_slot > ?", true, slot).
		Group("credential_tag, staking_key").
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	collect(rows)
	return refs, nil
}
