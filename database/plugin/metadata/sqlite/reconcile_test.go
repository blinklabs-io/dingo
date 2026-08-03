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

package sqlite

import (
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// TestRetirePoolsIdempotent pins the duplicate guard on synthetic
// pool_retirement rows: an interrupted catch-up retry re-runs RetirePools
// with the same stale pool set, epoch, and tip slot, and pool_retirement has
// no unique index to reject the second insert. The re-run must not add a
// second identical row.
func TestRetirePoolsIdempotent(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	pkh := make([]byte, 28)
	for i := range pkh {
		pkh[i] = 0xCD
	}
	require.NoError(t, store.DB().Create(&models.Pool{
		PoolKeyHash: pkh,
		VrfKeyHash:  make([]byte, 32),
	}).Error)

	for range 2 {
		require.NoError(
			t, store.RetirePools(nil, [][]byte{pkh}, 42, 5000),
		)
	}

	var count int64
	require.NoError(t, store.DB().
		Model(&models.PoolRetirement{}).
		Where("pool_key_hash = ?", pkh).
		Count(&count).Error)
	require.EqualValues(
		t, 1, count,
		"re-running RetirePools with the same epoch and slot must not "+
			"insert duplicate synthetic retirement rows",
	)

	// A retirement at a different tip is a genuine new row, not a duplicate.
	require.NoError(t, store.RetirePools(nil, [][]byte{pkh}, 43, 6000))
	require.NoError(t, store.DB().
		Model(&models.PoolRetirement{}).
		Where("pool_key_hash = ?", pkh).
		Count(&count).Error)
	require.EqualValues(t, 2, count)
}

// TestRetirePoolsDeduplicatesInputAcrossChunks verifies idempotence within a
// single call: the same pool key hash can appear in multiple lookup chunks and
// must still produce only one synthetic retirement.
func TestRetirePoolsDeduplicatesInputAcrossChunks(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	pkh := make([]byte, 28)
	for i := range pkh {
		pkh[i] = 0xCD
	}
	require.NoError(t, store.DB().Create(&models.Pool{
		PoolKeyHash: pkh,
		VrfKeyHash:  make([]byte, 32),
	}).Error)

	keyHashes := make([][]byte, 0, sqliteBindVarLimit+1)
	keyHashes = append(keyHashes, pkh)
	for i := 1; i < sqliteBindVarLimit; i++ {
		unknown := make([]byte, 28)
		binary.BigEndian.PutUint32(unknown, uint32(i)) // #nosec G115
		keyHashes = append(keyHashes, unknown)
	}
	keyHashes = append(keyHashes, pkh)

	require.NoError(t, store.RetirePools(nil, keyHashes, 42, 5000))

	var count int64
	require.NoError(t, store.DB().
		Model(&models.PoolRetirement{}).
		Where("pool_key_hash = ?", pkh).
		Count(&count).Error)
	require.EqualValues(t, 1, count)
}

// TestRetirePoolsSkipsUnknownPools ensures pool key hashes with no pool row
// are skipped rather than erroring, matching the previous per-row behavior.
func TestRetirePoolsSkipsUnknownPools(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	unknown := make([]byte, 28)
	for i := range unknown {
		unknown[i] = 0xEF
	}
	require.NoError(
		t, store.RetirePools(nil, [][]byte{unknown}, 42, 5000),
	)
	var count int64
	require.NoError(t, store.DB().
		Model(&models.PoolRetirement{}).
		Count(&count).Error)
	require.Zero(t, count)
}

// TestDeactivateAccountsChunked exercises the chunked tag-grouped update
// with more credentials than one IN-clause chunk (sqliteBindVarLimit), so the
// loop boundary is covered, and verifies only the targeted tag is touched.
func TestDeactivateAccountsChunked(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	const total = sqliteBindVarLimit + 50
	accounts := make([]models.Account, 0, total+1)
	creds := make([]models.StakeCredentialRef, 0, total)
	for i := range total {
		key := make([]byte, 28)
		binary.BigEndian.PutUint32(key, uint32(i)) // #nosec G115
		accounts = append(accounts, models.Account{
			StakingKey: key, CredentialTag: 0, Active: true,
		})
		creds = append(creds, models.StakeCredentialRef{Tag: 0, Key: key})
	}
	// Same key bytes under a different tag must remain active.
	otherTagKey := make([]byte, 28)
	accounts = append(accounts, models.Account{
		StakingKey: otherTagKey, CredentialTag: 1, Active: true,
	})
	require.NoError(
		t, store.DB().CreateInBatches(&accounts, 100).Error,
	)

	require.NoError(t, store.DeactivateAccounts(nil, creds))

	var activeCount int64
	require.NoError(t, store.DB().
		Model(&models.Account{}).
		Where("active = ?", true).
		Count(&activeCount).Error)
	require.EqualValues(
		t, 1, activeCount,
		"all tag-0 credentials must be deactivated across chunk "+
			"boundaries; the tag-1 row must remain active",
	)
	var activeOtherTagCount int64
	require.NoError(t, store.DB().
		Model(&models.Account{}).
		Where(
			"credential_tag = ? AND staking_key = ? AND active = ?",
			1, otherTagKey, true,
		).
		Count(&activeOtherTagCount).Error)
	require.EqualValues(
		t, 1, activeOtherTagCount,
		"the row with the same key bytes under credential tag 1 must remain active",
	)
	var activeTag0Count int64
	require.NoError(t, store.DB().
		Model(&models.Account{}).
		Where("credential_tag = ? AND active = ?", 0, true).
		Count(&activeTag0Count).Error)
	require.Zero(t, activeTag0Count, "no tag-0 rows should remain active")
}

// TestDeactivateDrepsChunked mirrors TestDeactivateAccountsChunked for the
// DRep reconcile path.
func TestDeactivateDrepsChunked(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	const total = sqliteBindVarLimit + 50
	dreps := make([]models.Drep, 0, total)
	creds := make([]models.StakeCredentialRef, 0, total)
	for i := range total {
		key := make([]byte, 28)
		binary.BigEndian.PutUint32(key, uint32(i)) // #nosec G115
		dreps = append(dreps, models.Drep{
			Credential: key, CredentialTag: 0, Active: true,
		})
		creds = append(creds, models.StakeCredentialRef{Tag: 0, Key: key})
	}
	require.NoError(t, store.DB().CreateInBatches(&dreps, 100).Error)

	require.NoError(t, store.DeactivateDreps(nil, creds))

	var activeCount int64
	require.NoError(t, store.DB().
		Model(&models.Drep{}).
		Where("active = ?", true).
		Count(&activeCount).Error)
	require.Zero(t, activeCount)
}
