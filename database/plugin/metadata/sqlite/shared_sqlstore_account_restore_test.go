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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSharedSQLStoreRestoreImportedAccountStates pins which rows the
// post-backfill restore rewrites: only accounts whose import baseline was
// recorded at or after the anchor, and only their registration and delegation.
func TestSharedSQLStoreRestoreImportedAccountStates(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)
	const anchor = uint64(500)
	key := func(b byte) []byte { return bytes.Repeat([]byte{b}, 28) }
	stale := key(0x01)      // replay re-pointed the pool and DRep
	abstain := key(0x02)    // imported AlwaysAbstain, replay set a credential
	matching := key(0x03)   // replay left the imported state unchanged
	genesis := key(0x04)    // baseline recorded before the anchor
	replayOnly := key(0x05) // no baseline: created by certificate replay

	for _, account := range []*models.Account{
		{StakingKey: stale, AddedSlot: anchor, Reward: 700, Active: true},
		{
			StakingKey: abstain,
			AddedSlot:  anchor,
			DrepType:   models.DrepTypeAlwaysAbstain,
			Active:     true,
		},
		{StakingKey: matching, AddedSlot: anchor, Pool: key(0x10), Active: true},
		{StakingKey: genesis, Pool: key(0x11), Active: true},
	} {
		require.NoError(t, store.ImportAccount(account, nil))
	}
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: replayOnly,
		Pool:       key(0x12),
		AddedSlot:  100,
	}))
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := raw.Exec(query, args...)
		require.NoError(t, err)
	}
	exec(
		"UPDATE account SET pool = ?, drep = ?, drep_type = 0, added_slot = 300 "+
			"WHERE staking_key = ?",
		key(0x20), key(0x21), stale,
	)
	exec(
		"UPDATE account SET drep = ?, drep_type = 0, added_slot = 310 "+
			"WHERE staking_key = ?",
		key(0x22), abstain,
	)
	exec(
		"UPDATE account SET pool = ?, added_slot = 320 WHERE staking_key = ?",
		key(0x23), genesis,
	)

	restored, err := store.RestoreImportedAccountStates(anchor, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, restored)

	get := func(stakeKey []byte) *models.Account {
		t.Helper()
		account, err := store.GetAccountByCredential(0, stakeKey, true, nil)
		require.NoError(t, err)
		require.NotNil(t, account)
		return account
	}
	account := get(stale)
	assert.Empty(t, account.Pool)
	assert.Empty(t, account.Drep)
	assert.Equal(t, models.DrepTypeAddrKeyHash, account.DrepType)
	assert.True(t, account.Active)
	assert.Equal(t, anchor, account.AddedSlot)
	assert.Equal(t, uint64(700), uint64(account.Reward))

	account = get(abstain)
	assert.Empty(t, account.Drep)
	assert.Equal(t, models.DrepTypeAlwaysAbstain, account.DrepType)

	account = get(matching)
	assert.Equal(t, key(0x10), account.Pool)
	assert.Equal(t, anchor, account.AddedSlot)

	account = get(genesis)
	assert.Equal(t, key(0x23), account.Pool)
	assert.Equal(t, uint64(320), account.AddedSlot)

	account = get(replayOnly)
	assert.Equal(t, key(0x12), account.Pool)
	assert.False(t, account.Active)
	assert.Equal(t, uint64(100), account.AddedSlot)

	restored, err = store.RestoreImportedAccountStates(anchor, nil)
	require.NoError(t, err)
	assert.Zero(t, restored)
}
