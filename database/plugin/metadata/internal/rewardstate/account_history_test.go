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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type testRewardRegistrationState struct {
	hasRegistration    bool
	registrationActive bool
	hasDeregistration  bool
}

type testAccountCertificateHistory map[string]testRewardRegistrationState

func (h testAccountCertificateHistory) RewardRegistrationState(
	key string,
) (bool, bool, bool) {
	state := h[key]
	return state.hasRegistration,
		state.registrationActive,
		state.hasDeregistration
}

func TestGetAccountsActiveAtSlot(t *testing.T) {
	db := newAccountHistoryTestDB(t)
	registeredActive := models.NewStakeCredentialRef(0, []byte("registered-active"))
	registeredInactive := models.NewStakeCredentialRef(0, []byte("registered-inactive"))
	fallbackActive := models.NewStakeCredentialRef(0, []byte("fallback-active"))
	fallbackFuture := models.NewStakeCredentialRef(0, []byte("fallback-future"))
	legacyInactive := models.NewStakeCredentialRef(0, []byte("legacy-inactive"))
	previouslyDeregistered := models.NewStakeCredentialRef(0, []byte("previously-deregistered"))
	fallbackDeregistered := models.NewStakeCredentialRef(0, []byte("fallback-deregistered"))
	sharedHashKey := []byte("shared-hash")
	sharedHashKeyCredential := models.NewStakeCredentialRef(0, sharedHashKey)
	sharedHashScriptCredential := models.NewStakeCredentialRef(1, sharedHashKey)

	accounts := []models.Account{
		{CredentialTag: fallbackActive.Tag, StakingKey: fallbackActive.Key, CreatedSlot: 10, Active: true},
		{CredentialTag: fallbackFuture.Tag, StakingKey: fallbackFuture.Key, CreatedSlot: 30, Active: true},
		{CredentialTag: legacyInactive.Tag, StakingKey: legacyInactive.Key, CreatedSlot: 10, Active: false},
		{CredentialTag: previouslyDeregistered.Tag, StakingKey: previouslyDeregistered.Key, CreatedSlot: 10, Active: false},
		{CredentialTag: fallbackDeregistered.Tag, StakingKey: fallbackDeregistered.Key, CreatedSlot: 10, Active: true},
		{CredentialTag: sharedHashKeyCredential.Tag, StakingKey: sharedHashKey, CreatedSlot: 10, Active: true},
		{CredentialTag: sharedHashScriptCredential.Tag, StakingKey: sharedHashKey, CreatedSlot: 30, Active: true},
	}
	require.NoError(t, db.Create(&accounts).Error)
	require.NoError(t, db.Model(&models.Account{}).
		Where("credential_tag = ? AND staking_key IN ?", 0, [][]byte{
			legacyInactive.Key,
			previouslyDeregistered.Key,
		}).
		Update("active", false).Error)
	require.NoError(t, db.Create(&models.StakeDeregistration{
		CredentialTag: previouslyDeregistered.Tag,
		StakingKey:    previouslyDeregistered.Key,
		AddedSlot:     30,
	}).Error)

	history := testAccountCertificateHistory{
		registeredActive.MapKey(): {
			hasRegistration:    true,
			registrationActive: true,
		},
		registeredInactive.MapKey(): {
			hasRegistration:   true,
			hasDeregistration: true,
		},
		fallbackDeregistered.MapKey(): {
			hasDeregistration: true,
		},
	}
	refs := []models.StakeCredentialRef{
		registeredActive,
		registeredInactive,
		fallbackActive,
		fallbackFuture,
		legacyInactive,
		previouslyDeregistered,
		fallbackDeregistered,
		sharedHashKeyCredential,
		sharedHashScriptCredential,
	}
	got, err := GetAccountsActiveAtSlot(
		refs,
		20,
		nil,
		func(types.Txn) (*gorm.DB, error) { return db, nil },
		func(
			*gorm.DB,
			[]models.StakeCredentialRef,
			uint64,
		) (testAccountCertificateHistory, error) {
			return history, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{
		registeredActive.MapKey():        {},
		fallbackActive.MapKey():          {},
		previouslyDeregistered.MapKey():  {},
		sharedHashKeyCredential.MapKey(): {},
	}, got)
}

func TestGetAccountsActiveAtSlotEmptyDoesNotResolveDB(t *testing.T) {
	resolved := false
	got, err := GetAccountsActiveAtSlot(
		nil,
		42,
		nil,
		func(types.Txn) (*gorm.DB, error) {
			resolved = true
			return nil, nil
		},
		func(
			*gorm.DB,
			[]models.StakeCredentialRef,
			uint64,
		) (testAccountCertificateHistory, error) {
			t.Fatal("certificate history fetch should not run")
			return nil, nil
		},
	)
	require.NoError(t, err)
	require.Empty(t, got)
	require.False(t, resolved)
}

func newAccountHistoryTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&models.Account{},
		&models.StakeDeregistration{},
		&models.Deregistration{},
	))
	return db
}
