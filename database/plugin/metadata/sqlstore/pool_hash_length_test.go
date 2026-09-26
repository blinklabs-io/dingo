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

package sqlstore

import (
	"context"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestGetPoolRegistrationsRejectsMalformedStoredHashes(t *testing.T) {
	t.Parallel()
	testGetPoolRegistrationsRejectsMalformedStoredHashes(
		t,
		newManagementTestStore,
	)
}

func TestGetStakeRegistrationsByCredentialRejectsMalformedStoredKey(
	t *testing.T,
) {
	t.Parallel()
	testGetStakeRegistrationsByCredentialRejectsMalformedStoredKey(
		t,
		newManagementTestStore(t),
	)
}

// testGetPoolRegistrationsRejectsMalformedStoredHashes writes a valid pool
// registration, overwrites one hash column with a value one byte short, and
// requires the certificate reconstruction to fail rather than zero-pad it.
func testGetPoolRegistrationsRejectsMalformedStoredHashes(
	t *testing.T,
	newStore func(*testing.T) *Store,
) {
	t.Helper()
	for _, field := range []string{"VRF key hash", "reward account", "owner key hash"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			store := newStore(t)
			poolKey := make([]byte, lcommon.Blake2b224Size)
			poolKey[0] = 1
			vrfKey := make([]byte, lcommon.Blake2b256Size)
			vrfKey[0] = 2
			rewardAccount := make([]byte, lcommon.Blake2b224Size)
			rewardAccount[0] = 3
			ownerKey := make([]byte, lcommon.Blake2b224Size)
			ownerKey[0] = 4
			require.NoError(t, store.ImportPool(
				&models.Pool{
					PoolKeyHash:   poolKey,
					VrfKeyHash:    vrfKey,
					RewardAccount: rewardAccount,
				},
				&models.PoolRegistration{
					PoolKeyHash:   poolKey,
					VrfKeyHash:    vrfKey,
					RewardAccount: rewardAccount,
					Margin:        &dbtypes.Rat{Rat: big.NewRat(1, 10)},
					AddedSlot:     1,
					Owners: []models.PoolRegistrationOwner{{
						KeyHash: ownerKey,
					}},
				},
				nil,
			))

			badHash := make([]byte, lcommon.Blake2b224Size-1)
			statement := ""
			args := []any{badHash}
			switch field {
			case "VRF key hash":
				badHash = make([]byte, lcommon.Blake2b256Size-1)
				args = []any{badHash, poolKey}
				statement = "UPDATE pool_registration SET vrf_key_hash = ? WHERE pool_key_hash = ?"
			case "reward account":
				args = []any{badHash, poolKey}
				statement = "UPDATE pool_registration SET reward_account = ? WHERE pool_key_hash = ?"
			case "owner key hash":
				statement = "UPDATE pool_registration_owner SET key_hash = ?"
			}
			_, err := store.writeDB.ExecContext(
				context.Background(),
				store.dialect.Rebind(statement),
				args...,
			)
			require.NoError(t, err)

			_, err = store.GetPoolRegistrations(
				lcommon.NewBlake2b224(poolKey),
				nil,
			)
			require.Error(t, err)
			require.Contains(t, err.Error(), "get pool registrations")
			require.Contains(t, err.Error(), "invalid blake2b-")
		})
	}
}

// testGetStakeRegistrationsByCredentialRejectsMalformedStoredKey covers a
// stake registration row whose staking key is one byte short of a
// credential hash. The array conversion this replaced panicked on it.
func testGetStakeRegistrationsByCredentialRejectsMalformedStoredKey(
	t *testing.T,
	store *Store,
) {
	t.Helper()
	short := make([]byte, lcommon.Blake2b224Size-1)
	short[0] = 5
	_, err := store.writeDB.ExecContext(
		context.Background(),
		store.dialect.Rebind(
			"INSERT INTO stake_registration "+
				"(staking_key, credential_tag, added_slot, deposit_amount) "+
				"VALUES (?, 0, 1, '0')",
		),
		short,
	)
	require.NoError(t, err)

	var certs []lcommon.StakeRegistrationCertificate
	require.NotPanics(t, func() {
		certs, err = store.GetStakeRegistrationsByCredential(0, short, nil)
	})
	require.ErrorContains(t, err, "stake registration credential")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Nil(t, certs)
}
