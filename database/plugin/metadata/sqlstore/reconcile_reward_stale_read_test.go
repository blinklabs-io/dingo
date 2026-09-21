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

// These four tests pin the fix for four call sites that read a
// ReconcileAccountRewardBalance correction's synthetic withdrawal-shaped
// account_reward_delta row (see that method's doc comment in account.go) the
// same naive way historicalRewardsBatch was already fixed to avoid: treating
// it as if it were a real witnessed withdrawal event or a real withdrawn
// amount, rather than a system-generated balance correction that happens to
// reuse the withdrawal-shaped row for rollback-inversion bookkeeping.
// withdrawalHistoryQuery (account_history.go, ~line 372) is the reference for
// "already correct": its INNER JOIN to "transaction" naturally excludes a
// reconciliation row because its tx_hash is a synthetic discriminator, never
// a real transaction hash.

// TestAccountLastWitnessSlotsIgnoresReconciliation pins the fix for
// account_history.go's mergeWitnessSlots, called from AccountLastWitnessSlots
// with withdrawalsOnly=true against account_reward_delta: before this fix, it
// counted a ReconcileAccountRewardBalance correction as a witnessed
// withdrawal, which would wrongly renew a CIP-0163 delegator-inactivity
// clock the credential never actually witnessed.
func TestAccountLastWitnessSlotsIgnoresReconciliation(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytes.Repeat([]byte{0xb1}, 28)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, CredentialTag: 0, Active: true,
	}))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 1_000_000, 50, bytes.Repeat([]byte{0xc1}, 32), nil,
	))
	// The only account_reward_delta row with withdrawal=TRUE for this
	// credential is a reconciliation, not a real withdrawal.
	require.NoError(t, store.ReconcileAccountRewardBalance(
		0, key, 999_998, 500, bytes.Repeat([]byte{0xc2}, 32), nil,
	))

	ref := models.NewStakeCredentialRef(0, key)
	got, err := store.AccountLastWitnessSlots(
		[]models.StakeCredentialRef{ref}, 1_000, nil,
	)
	require.NoError(t, err)
	_, found := got[ref.MapKey()]
	require.False(
		t,
		found,
		"a reconciliation correction must not count as a witnessed "+
			"withdrawal event",
	)
}

// TestAccountsWitnessedAfterSlotIgnoresReconciliation pins the same fix in
// AccountsWitnessedAfterSlot's own account_reward_delta UNION arm.
func TestAccountsWitnessedAfterSlotIgnoresReconciliation(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytes.Repeat([]byte{0xb2}, 28)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, CredentialTag: 0, Active: true,
	}))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 1_000_000, 50, bytes.Repeat([]byte{0xc3}, 32), nil,
	))
	require.NoError(t, store.ReconcileAccountRewardBalance(
		0, key, 999_998, 500, bytes.Repeat([]byte{0xc4}, 32), nil,
	))

	got, err := store.AccountsWitnessedAfterSlot(10, nil)
	require.NoError(t, err)
	for _, ref := range got {
		require.False(
			t,
			ref.Tag == 0 && bytes.Equal(ref.Key, key),
			"a reconciliation correction must not count as witnessing "+
				"activity after the slot",
		)
	}
}

// TestHistoricalExpirationSQLIgnoresReconciliation pins the fix for
// historical_stake.go's historicalExpirationSQL: before this fix, a
// reconciliation row's added_slot fed historical_witness_epoch exactly like a
// real withdrawal, wrongly renewing the CIP-0163 inactivity-expiry clock (and
// so the voting power it gates) past what the credential's real activity
// justified.
//
// This calls historicalExpirationSQL directly and embeds its returned
// fragment in a minimal standalone query, rather than exercising it through
// the full GetStakeByPoolsAtSlot machinery, mirroring
// TestHistoricalRewardsAtBoundaryResolvesReconciliationToCorrectedAmount's
// direct-unit-call style for the equivalent historicalRewardsBatch fix.
func TestHistoricalExpirationSQLIgnoresReconciliation(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	ctx := context.Background()
	key := bytes.Repeat([]byte{0xb3}, 28)
	pool := bytes.Repeat([]byte{0xb4}, 28)

	const epochLength = 1_000
	for epoch := range uint64(6) {
		require.NoError(t, store.SetEpoch(
			epoch*epochLength, epoch, nil, nil, nil, nil, 6, 1, epochLength, nil,
		))
	}

	// The account's own baseline (no witness activity at all) is epoch 1 --
	// a stand-in for whatever RenewAccountExpirations last stamped. Any
	// value fed here that is not epoch 1 proves some source other than this
	// baseline drove the result.
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, CredentialTag: 0, Active: true,
		ExpirationEpoch: 1,
	}))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 1_000_000, 60, bytes.Repeat([]byte{0xc5}, 32), nil,
	))
	// A reconciliation at slot 2_500 (epoch 2) is the only
	// withdrawal-shaped account_reward_delta row for this credential.
	require.NoError(t, store.ReconcileAccountRewardBalance(
		0, key, 999_998, 2_500, bytes.Repeat([]byte{0xc6}, 32), nil,
	))

	query, args, err := historicalExpirationSQL(
		ctx, store.writeDB, 4_000, 4, 2,
	)
	require.NoError(t, err)
	full := `WITH active_delegation AS (
 SELECT ? credential_tag, ? staking_key, ? pool_key_hash
)` + query + `
SELECT expiration_epoch FROM historical_expiration`
	fullArgs := append([]any{uint8(0), key, pool}, args...)

	var expirationEpoch uint64
	require.NoError(
		t,
		store.writeDB.QueryRowContext(ctx, full, fullArgs...).
			Scan(&expirationEpoch),
	)
	require.Equal(
		t,
		uint64(1),
		expirationEpoch,
		"a reconciliation correction must not renew the inactivity-expiry "+
			"clock: the account's real (unwitnessed) baseline of epoch 1 "+
			"must survive, not the reconciliation slot's epoch (2) plus "+
			"the inactivity period (2)",
	)
}

// TestGetAccountSumsByCredentialExcludesReconciliationFromWithdrawalsSum pins
// the fix for account.go's GetAccountSumsByCredential: before this fix, a
// reconciliation row's "amount" (the pre-correction balance the correction
// proved wrong, not coin any delegator withdrew) was summed into
// Blockfrost's withdrawals_sum alongside real withdrawals.
func TestGetAccountSumsByCredentialExcludesReconciliationFromWithdrawalsSum(
	t *testing.T,
) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytes.Repeat([]byte{0xb5}, 28)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, CredentialTag: 0, Active: true,
	}))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 5_000_000, 50, bytes.Repeat([]byte{0xc7}, 32), nil,
	))
	// A real withdrawal that must still be summed.
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, key, 2_000_000, 100, bytes.Repeat([]byte{0xc8}, 32), nil,
	))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 5_000_000, 150, bytes.Repeat([]byte{0xc9}, 32), nil,
	))
	// A reconciliation, whose synthetic "amount" (5_000_000) must not be
	// mistaken for a second real withdrawal.
	require.NoError(t, store.ReconcileAccountRewardBalance(
		0, key, 4_999_998, 200, bytes.Repeat([]byte{0xca}, 32), nil,
	))

	sums, err := store.GetAccountSumsByCredential(0, key, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(2_000_000),
		sums.WithdrawalsSum,
		"withdrawals_sum must reflect only the real withdrawal "+
			"(2_000_000), not the reconciliation row's synthetic amount "+
			"(5_000_000)",
	)
}
