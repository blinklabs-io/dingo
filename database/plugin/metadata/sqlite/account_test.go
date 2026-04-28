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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetAccount_ExcludesInactiveWhenIncludeInactiveFalse pins the
// includeInactive=false branch of GetAccount: a row with active=false must
// not be returned. This is the semantic the
// queryShelleyFilteredDelegationAndRewardAccounts handler relies on when
// it passes includeInactive=false; testing it here keeps the assertion
// next to the SQL clause it actually tests, without forcing the ledger
// layer to import this package or to depend on the GORM default-tag
// behavior of CreateAccount.
func TestGetAccount_ExcludesInactiveWhenIncludeInactiveFalse(t *testing.T) {
	store := setupTestStore(t)

	stakeKey := make([]byte, 28)
	for i := range stakeKey {
		stakeKey[i] = 0xAB
	}

	// Insert active, then deactivate via UPDATE. GORM's `default:true`
	// on Account.Active swallows the false value on INSERT (whether via
	// Create, Save, or Select("*").Create — the DEFAULT clause applies
	// at the SQL level). UPDATE has no analogous fallback, so flipping
	// Active in a follow-up statement is the reliable path.
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeKey,
		Reward:     types.Uint64(123_456),
		Active:     true,
	}).Error)
	require.NoError(t, store.DB().
		Model(&models.Account{}).
		Where("staking_key = ?", stakeKey).
		Update("active", false).Error)

	got, err := store.GetAccount(stakeKey, false /* includeInactive */, nil)
	require.NoError(t, err)
	assert.Nil(t, got, "inactive account must not be returned when includeInactive=false")

	got, err = store.GetAccount(stakeKey, true /* includeInactive */, nil)
	require.NoError(t, err)
	require.NotNil(t, got, "inactive account must be returned when includeInactive=true")
	assert.False(t, got.Active, "returned account should have Active=false")
}
