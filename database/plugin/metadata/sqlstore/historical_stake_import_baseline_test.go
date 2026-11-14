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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

const (
	importBaselineSlot  = 100
	importBaselineStake = 7_000
	importBoundarySlot  = 1_500
)

// An imported account has no certificate history, so the historical boundary
// reconstruction cannot read it from certificates. A certificate after the
// boundary slot re-stamps the live account row; the boundary read must still
// see the imported state, taken from the import baseline.
func TestEpochBoundaryStakeKeepsImportedStateAfterLaterCertificate(
	t *testing.T,
) {
	t.Parallel()
	testEpochBoundaryStakeKeepsImportedState(t, func(t *testing.T) *Store {
		store := newDepositHeldStore(t)
		depositHeldEpochs(t, store, 5)
		return store
	})
}

// testEpochBoundaryStakeKeepsImportedState runs the scenarios against a fresh
// store per case so each dialect's integration test can share them.
func testEpochBoundaryStakeKeepsImportedState(
	t *testing.T,
	newStore func(t *testing.T) *Store,
) {
	t.Helper()
	for _, tc := range []struct {
		name string
		// baseline is false for a legacy row with no account_import_baseline.
		baseline       bool
		importedActive bool
		later          func(t *testing.T, store *Store, c lcommon.Credential, pool lcommon.PoolKeyHash)
		wantCounted    bool
	}{
		{
			name:           "no_later_cert",
			baseline:       true,
			importedActive: true,
			wantCounted:    true,
		},
		{
			name:           "later_redelegation",
			baseline:       true,
			importedActive: true,
			later: func(t *testing.T, store *Store, c lcommon.Credential, pool lcommon.PoolKeyHash) {
				writeDepositHeldCert(t, store, 2_000, 0,
					&lcommon.StakeDelegationCertificate{
						CertType: uint(
							lcommon.CertificateTypeStakeDelegation,
						),
						StakeCredential: &c,
						PoolKeyHash:     depositHeldPoolKey(0xee),
					}, 0)
			},
			wantCounted: true,
		},
		{
			name:           "later_deregistration",
			baseline:       true,
			importedActive: true,
			later: func(t *testing.T, store *Store, c lcommon.Credential, pool lcommon.PoolKeyHash) {
				writeDepositHeldCert(t, store, 2_000, 0,
					&lcommon.StakeDeregistrationCertificate{
						CertType: uint(
							lcommon.CertificateTypeStakeDeregistration,
						),
						StakeCredential: c,
					}, 0)
			},
			wantCounted: true,
		},
		{
			name:           "later_reregistration",
			baseline:       true,
			importedActive: true,
			later: func(t *testing.T, store *Store, c lcommon.Credential, pool lcommon.PoolKeyHash) {
				writeDepositHeldCert(t, store, 2_000, 0,
					&lcommon.StakeDeregistrationCertificate{
						CertType: uint(
							lcommon.CertificateTypeStakeDeregistration,
						),
						StakeCredential: c,
					}, 0)
				writeDepositHeldCert(t, store, 2_100, 0,
					&lcommon.StakeRegistrationCertificate{
						CertType: uint(
							lcommon.CertificateTypeStakeRegistration,
						),
						StakeCredential: c,
					}, 0)
			},
			wantCounted: true,
		},
		{
			name:           "baseline_inactive",
			baseline:       true,
			importedActive: false,
			wantCounted:    false,
		},
		{
			name:           "legacy_without_baseline",
			baseline:       false,
			importedActive: true,
			wantCounted:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newStore(t)
			pool := depositHeldPoolKey(0xd1)
			credential := stakeCredential(0xd2)

			account := &models.Account{
				StakingKey:    credential.Credential.Bytes(),
				CredentialTag: 0,
				Pool:          pool.Bytes(),
				Active:        tc.importedActive,
				AddedSlot:     importBaselineSlot,
				CreatedSlot:   importBaselineSlot,
				Reward:        types.Uint64(importBaselineStake),
			}
			if tc.baseline {
				require.NoError(t, store.ImportAccount(account, nil))
			} else {
				_, err := store.writeDB.ExecContext(t.Context(), store.dialect.Rebind(`
INSERT INTO account (staking_key, credential_tag, pool, added_slot, created_slot, active, reward)
VALUES (?, 0, ?, ?, ?, TRUE, ?)`),
					account.StakingKey, account.Pool,
					importBaselineSlot, importBaselineSlot,
					fmt.Sprint(importBaselineStake),
				)
				require.NoError(t, err)
			}
			if tc.later != nil {
				tc.later(t, store, credential, pool)
			}

			stake, delegators, err := store.GetEpochBoundaryStakeByPools(
				[][]byte{pool.Bytes()}, importBoundarySlot, 0, nil, 0, 0, nil,
			)
			require.NoError(t, err)
			inputs, err := store.GetEpochBoundaryRewardStakeInputsForPools(
				[][]byte{pool.Bytes()}, importBoundarySlot, 0, nil, 0, 0, nil,
			)
			require.NoError(t, err)
			pools, err := store.GetEpochBoundaryDelegatedPoolKeyHashes(
				importBoundarySlot, 0, nil,
			)
			require.NoError(t, err)
			// The scope CTE binds untyped parameters, which PostgreSQL
			// resolves as text, so the scoped form is exercised on SQLite
			// only; the other dialects read the same credential unscoped.
			var scopeSQL string
			var scopeArgs []any
			if store.dialect.Name() == "sqlite" {
				scopeSQL, scopeArgs = credentialScopeSQL(credential)
			}
			scoped := scopedActiveDelegationRows(
				t, store, importBoundarySlot, scopeSQL, scopeArgs,
			)

			key := string(pool.Bytes())
			if !tc.wantCounted {
				require.Zero(t, delegators[key])
				require.Zero(t, stake[key])
				require.Empty(t, inputs)
				require.NotContains(t, pools, pool.Bytes())
				require.Empty(t, scoped)
				return
			}
			require.Equal(t, uint64(1), delegators[key],
				"imported delegation at slot 100 must count at slot 1500")
			require.Equal(t, uint64(importBaselineStake), stake[key],
				"imported reward balance must be in the pool total")
			require.Contains(t, pools, pool.Bytes())
			require.Len(t, inputs, 1)
			require.Equal(
				t,
				credential.Credential.Bytes(),
				inputs[0].StakingKey,
			)
			require.Equal(t, pool.Bytes(), inputs[0].PoolKeyHash)
			require.True(t, inputs[0].Registered)
			require.Equal(t, types.Uint64(importBaselineStake), inputs[0].Stake)
			require.Equal(
				t,
				[]string{fmt.Sprintf("%x %d %x", pool.Bytes(),
					credential.CredType, credential.Credential.Bytes())},
				scoped,
				"the credential-scoped read the pointer overlay uses must agree",
			)
		})
	}
}

// scopedActiveDelegationRows is activeDelegationRows with the placeholders
// rebound for the store's dialect.
func scopedActiveDelegationRows(
	t *testing.T,
	store *Store,
	slot uint64,
	scopeSQL string,
	scopeArgs []any,
) []string {
	t.Helper()
	query, args := activeDelegationScopedSQL(slot, slot, scopeSQL, scopeArgs)
	query += `
SELECT pool_key_hash, credential_tag, staking_key FROM active_delegation
ORDER BY pool_key_hash, credential_tag, staking_key`
	rows, err := store.writeDB.QueryContext(
		t.Context(), store.dialect.Rebind(query), args...,
	)
	require.NoError(t, err)
	defer rows.Close()
	var ret []string
	for rows.Next() {
		var pool, key []byte
		var tag int64
		require.NoError(t, rows.Scan(&pool, &tag, &key))
		ret = append(ret, fmt.Sprintf("%x %d %x", pool, tag, key))
	}
	require.NoError(t, rows.Err())
	return ret
}
