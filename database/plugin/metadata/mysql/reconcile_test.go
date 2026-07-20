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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// TestMysqlReconcileRoundTrip exercises the shared importutil reconcile
// helpers against a real MySQL: chunked tag-grouped deactivation and
// duplicate-safe synthetic pool retirement.
func TestMysqlReconcileRoundTrip(t *testing.T) {
	store := newTestMysqlStore(t)
	defer store.Close() //nolint:errcheck

	db := store.DB()
	db.Where("1 = 1").Delete(&models.PoolRetirement{})
	db.Where("1 = 1").Delete(&models.Account{})
	db.Where("1 = 1").Delete(&models.Drep{})
	db.Where("1 = 1").Delete(&models.Pool{})

	stakeKey := bytes.Repeat([]byte{0xaa}, 28)
	drepCred := bytes.Repeat([]byte{0xbb}, 28)
	pkh := bytes.Repeat([]byte{0xcd}, 28)

	require.NoError(t, db.Create(&models.Account{
		StakingKey: stakeKey, CredentialTag: 0, Active: true, AddedSlot: 100,
	}).Error)
	require.NoError(t, db.Create(&models.Drep{
		Credential: drepCred, CredentialTag: 0, Active: true, AddedSlot: 100,
	}).Error)
	require.NoError(t, db.Create(&models.Pool{
		PoolKeyHash: pkh, VrfKeyHash: make([]byte, 32),
	}).Error)

	creds := []models.StakeCredentialRef{{Tag: 0, Key: stakeKey}}
	require.NoError(t, store.DeactivateAccounts(nil, creds))
	drepCreds := []models.StakeCredentialRef{{Tag: 0, Key: drepCred}}
	require.NoError(t, store.DeactivateDreps(nil, drepCreds))

	var activeAccounts, activeDreps int64
	require.NoError(t, db.Model(&models.Account{}).
		Where("active = ?", true).Count(&activeAccounts).Error)
	require.NoError(t, db.Model(&models.Drep{}).
		Where("active = ?", true).Count(&activeDreps).Error)
	require.Zero(t, activeAccounts)
	require.Zero(t, activeDreps)

	// RetirePools must be idempotent for the same epoch and tip slot.
	for range 2 {
		require.NoError(t, store.RetirePools(nil, [][]byte{pkh}, 42, 5000))
	}
	var retirements int64
	require.NoError(t, db.Model(&models.PoolRetirement{}).
		Where("pool_key_hash = ?", pkh).Count(&retirements).Error)
	require.EqualValues(t, 1, retirements)
}
