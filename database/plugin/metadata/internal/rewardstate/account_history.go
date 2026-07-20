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

package rewardstate

import (
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
)

const rewardAccountQueryChunkSize = 400

// AccountCertificateHistory exposes the registration state needed to decide
// whether a reward account was active at a slot. Metadata backends implement
// this on their private certificate cache types.
type AccountCertificateHistory interface {
	RewardRegistrationState(key string) (
		hasRegistration bool,
		registrationActive bool,
		hasDeregistration bool,
	)
}

type rewardFallbackAccountState struct {
	createdSlot uint64
	active      bool
}

// GetAccountsActiveAtSlot returns credentials active according to registration
// and deregistration certificate history at or before slot.
func GetAccountsActiveAtSlot[C AccountCertificateHistory](
	refs []models.StakeCredentialRef,
	slot uint64,
	txn types.Txn,
	resolveReadDB func(types.Txn) (*gorm.DB, error),
	fetchCertificateHistory func(
		*gorm.DB,
		[]models.StakeCredentialRef,
		uint64,
	) (C, error),
) (map[string]struct{}, error) {
	ret := make(map[string]struct{}, len(refs))
	if len(refs) == 0 {
		return ret, nil
	}
	db, err := resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	history, err := fetchCertificateHistory(db, refs, slot)
	if err != nil {
		return nil, err
	}
	fallbackRefs := make([]models.StakeCredentialRef, 0)
	for _, ref := range refs {
		key := ref.MapKey()
		hasRegistration, registrationActive, _ := history.RewardRegistrationState(key)
		if !hasRegistration {
			fallbackRefs = append(fallbackRefs, ref)
			continue
		}
		if registrationActive {
			ret[key] = struct{}{}
		}
	}
	fallbackStates, err := rewardFallbackAccountStates(db, fallbackRefs)
	if err != nil {
		return nil, err
	}
	everDeregistered, err := rewardCredentialsEverDeregistered(db, fallbackRefs)
	if err != nil {
		return nil, err
	}
	for _, ref := range fallbackRefs {
		key := ref.MapKey()
		state, exists := fallbackStates[key]
		if !exists || state.createdSlot > slot {
			continue
		}
		_, wasDeregistered := everDeregistered[key]
		if !state.active && !wasDeregistered {
			continue
		}
		_, _, hasDeregistration := history.RewardRegistrationState(key)
		if hasDeregistration {
			continue
		}
		ret[key] = struct{}{}
	}
	return ret, nil
}

func rewardFallbackAccountStates(
	db *gorm.DB,
	refs []models.StakeCredentialRef,
) (map[string]rewardFallbackAccountState, error) {
	ret := make(map[string]rewardFallbackAccountState, len(refs))
	pairs := rewardAccountCredentialPairs(refs)
	for chunk := range slices.Chunk(pairs, rewardAccountQueryChunkSize) {
		var rows []models.Account
		if result := db.Model(&models.Account{}).
			Select("credential_tag", "staking_key", "created_slot", "active").
			Where("(credential_tag, staking_key) IN ?", chunk).
			Find(&rows); result.Error != nil {
			return nil, result.Error
		}
		for _, row := range rows {
			ret[models.NewStakeCredentialRef(
				row.CredentialTag,
				row.StakingKey,
			).MapKey()] = rewardFallbackAccountState{
				createdSlot: row.CreatedSlot,
				active:      row.Active,
			}
		}
	}
	return ret, nil
}

func rewardCredentialsEverDeregistered(
	db *gorm.DB,
	refs []models.StakeCredentialRef,
) (map[string]struct{}, error) {
	ret := make(map[string]struct{}, len(refs))
	pairs := rewardAccountCredentialPairs(refs)
	scan := func(model any) error {
		for chunk := range slices.Chunk(pairs, rewardAccountQueryChunkSize) {
			var rows []struct {
				CredentialTag uint8
				StakingKey    []byte
			}
			if err := db.Model(model).
				Select("credential_tag", "staking_key").
				Where("(credential_tag, staking_key) IN ?", chunk).
				Find(&rows).Error; err != nil {
				return err
			}
			for _, row := range rows {
				ret[models.NewStakeCredentialRef(
					row.CredentialTag,
					row.StakingKey,
				).MapKey()] = struct{}{}
			}
		}
		return nil
	}
	if err := scan(&models.StakeDeregistration{}); err != nil {
		return nil, err
	}
	if err := scan(&models.Deregistration{}); err != nil {
		return nil, err
	}
	return ret, nil
}

func rewardAccountCredentialPairs(refs []models.StakeCredentialRef) [][]any {
	pairs := make([][]any, 0, len(refs))
	for _, ref := range refs {
		pairs = append(pairs, []any{ref.Tag, ref.Key})
	}
	return pairs
}
