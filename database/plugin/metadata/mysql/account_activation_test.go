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

// TestStampAllActiveAccountExpirationsMysql verifies that activation gives all
// active accounts the same full inactivity window. In particular, a witness
// before activation may have already set expiration_epoch, but that earlier
// value must be replaced by the activation expiration.
func TestStampAllActiveAccountExpirationsMysql(t *testing.T) {
	store := newTestMysqlStore(t)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	db := store.DB()

	keyUnset := bytes.Repeat([]byte{0xD1}, 28)
	keyWitnessed := bytes.Repeat([]byte{0xD2}, 28)
	keyInactive := bytes.Repeat([]byte{0xD3}, 28)
	keys := [][]byte{keyUnset, keyWitnessed, keyInactive}
	cleanup := func() {
		for _, key := range keys {
			require.NoError(t, db.Where("staking_key = ?", key).
				Delete(&models.AccountInactivityActivation{}).Error)
			require.NoError(t, db.Where("staking_key = ?", key).
				Delete(&models.Account{}).Error)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	accounts := []models.Account{
		{
			StakingKey: keyUnset, CredentialTag: 0, Active: true,
		},
		{
			StakingKey: keyWitnessed, CredentialTag: 0, Active: true,
			ExpirationEpoch: 149,
		},
		{
			StakingKey: keyInactive, CredentialTag: 0, Active: true,
			ExpirationEpoch: 149,
		},
	}
	for i := range accounts {
		require.NoError(t, db.Create(&accounts[i]).Error)
	}
	require.NoError(t, db.Model(&models.Account{}).
		Where("staking_key = ?", keyInactive).
		Update("active", false).Error)

	rows, err := store.StampAllActiveAccountExpirations(150, nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, rows)

	refs := []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, keyUnset),
		models.NewStakeCredentialRef(0, keyWitnessed),
		models.NewStakeCredentialRef(0, keyInactive),
	}
	membership, err := store.AccountInactivityActivationMembership(refs, nil)
	require.NoError(t, err)
	require.Contains(t, membership, refs[0].MapKey())
	require.Contains(t, membership, refs[1].MapKey())
	require.NotContains(t, membership, refs[2].MapKey())

	var gotUnset models.Account
	require.NoError(t, db.First(&gotUnset, accounts[0].ID).Error)
	require.Equal(t, uint64(150), gotUnset.ExpirationEpoch)

	var gotWitnessed models.Account
	require.NoError(t, db.First(&gotWitnessed, accounts[1].ID).Error)
	require.Equal(t, uint64(150), gotWitnessed.ExpirationEpoch)

	var gotInactive models.Account
	require.NoError(t, db.First(&gotInactive, accounts[2].ID).Error)
	require.Equal(t, uint64(149), gotInactive.ExpirationEpoch)
}
