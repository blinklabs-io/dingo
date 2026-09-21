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
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// TestHistoricalRewardsAtBoundaryResolvesReconciliationToCorrectedAmount pins
// the fix for the reconstruction-semantics half of dingo #4529
// ("node-parity: persistent small stake-distribution excess across many
// pools"): historicalRewardsAtBoundary (the reader GetStakeByPoolsAtSlot and
// friends use to reconstruct a credential's reward balance as of a past
// slot) used to walk a ReconcileAccountRewardBalance correction's
// withdrawal-shaped delta row exactly like a real withdrawal, resolving any
// boundary before the correction's slot to previous_reward -- the very
// pre-correction balance the correction proved wrong. That silently
// reproduced the same small understatement in every historical/
// epoch-boundary stake read older than the correction. This test builds a
// concrete before/after/later scenario with real numbers around one
// reconciliation and checks all three boundaries.
func TestHistoricalRewardsAtBoundaryResolvesReconciliationToCorrectedAmount(
	t *testing.T,
) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytes.Repeat([]byte{0x7c}, 28)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, CredentialTag: 0, Active: true,
	}))

	// slot 100: a normal accrual reaches the balance dingo's own (buggy)
	// computation held going into the mismatch: 5_000_000.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 5_000_000, 100, bytes.Repeat([]byte{0xd1}, 32), nil,
	))

	// slot 150: a ReconcileAccountRewardBalance correction proves the true
	// balance was 5_000_014, not 5_000_000 -- the same shape of small,
	// persistent understatement described in dingo #4529.
	require.NoError(t, store.ReconcileAccountRewardBalance(
		0, key, 5_000_014, 150, bytes.Repeat([]byte{0xd2}, 32), nil,
	))

	// slot 200: normal accrual resumes after the correction.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 1_000_000, 200, bytes.Repeat([]byte{0xd3}, 32), nil,
	))

	selected := map[historicalRewardKey]struct{}{
		{tag: 0, key: string(key)}: {},
	}
	ref := historicalRewardKey{tag: 0, key: string(key)}

	// A boundary strictly before the correction's slot must resolve to the
	// corrected balance, not the pre-correction value the correction proved
	// wrong.
	before, err := historicalRewardsAtBoundary(
		context.Background(), store.writeDB, 120, 0, selected,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(5_000_014),
		before[ref],
		"a boundary before the correction must resolve to the corrected "+
			"balance (5_000_014), not the pre-correction value (5_000_000) "+
			"the correction proved wrong",
	)

	// A boundary between the correction and the next accrual must resolve to
	// exactly the corrected balance.
	between, err := historicalRewardsAtBoundary(
		context.Background(), store.writeDB, 160, 0, selected,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(5_000_014), between[ref])

	// A boundary after the later accrual sees it on top of the corrected
	// balance -- the branch this fix must not disturb.
	after, err := historicalRewardsAtBoundary(
		context.Background(), store.writeDB, 250, 0, selected,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(6_000_014), after[ref])
}
