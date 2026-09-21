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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// TestDeleteAccountRewardsAfterSlotSurvivesReconciliation pins the fix for a
// bug caught live while trying to truncate a database that had exercised
// TrustCanonicalWithdrawalOnRewardMismatch: ReconcileAccountRewardBalance
// used to overwrite account.reward with no account_reward_delta row at all,
// so a later rollback/truncate crossing that slot walked the delta chain as
// if the account's history were unbroken, and failed with "account reward
// rollback underflow" once it tried to undo an earlier, legitimate accrual
// against a balance the reconciliation had silently changed out from under
// it.
func TestDeleteAccountRewardsAfterSlotSurvivesReconciliation(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytes.Repeat([]byte{0x9a}, 28)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, CredentialTag: 0, Active: true,
	}))

	// A normal accrual, well before the reconciliation.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 3_000_000, 100, bytes.Repeat([]byte{0xb1}, 32), nil,
	))

	// TrustCanonicalWithdrawalOnRewardMismatch overwrites the balance to
	// match a canonical peer's claim, at slot 150, in response to a specific
	// failing transaction.
	require.NoError(t, store.ReconcileAccountRewardBalance(
		0, key, 2_999_998, 150, bytes.Repeat([]byte{0xb2}, 32), nil,
	))

	// Normal accrual resumes afterward.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 1_000_000, 200, bytes.Repeat([]byte{0xb3}, 32), nil,
	))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3_999_998), uint64(got.Reward))

	// Rolling back to before the reconciliation must not fail with
	// "account reward rollback underflow" -- it must walk back through the
	// post-reconciliation accrual, then invert the reconciliation itself
	// (restoring the pre-reconciliation balance), leaving the balance
	// exactly as it was right after the original accrual.
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(120, nil))

	got, err = store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(3_000_000),
		uint64(got.Reward),
		"rollback across a reconciliation must restore the pre-reconciliation "+
			"balance, not underflow",
	)
}

// TestDeleteAccountRewardsAfterSlotIgnoresReconciledAmountOnRollback proves
// that adding reconciled_amount (the column historicalRewardsBatch now
// consults to resolve a boundary before a ReconcileAccountRewardBalance
// correction; see historical_reward_reconcile_test.go) left
// DeleteAccountRewardsAfterSlot's own rollback path untouched: it must keep
// restoring previous_reward verbatim, never reconciled_amount, even when the
// two differ by an amount far too large to occur by coincidence.
func TestDeleteAccountRewardsAfterSlotIgnoresReconciledAmountOnRollback(
	t *testing.T,
) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytes.Repeat([]byte{0x9c}, 28)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, CredentialTag: 0, Active: true,
	}))

	// A normal accrual, well before the reconciliation.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 3_000_000, 100, bytes.Repeat([]byte{0xe1}, 32), nil,
	))

	// The correction jumps the balance from 3_000_000 to 9_999_999 -- a gap
	// far larger than any plausible reconciliation, chosen so that restoring
	// reconciled_amount instead of previous_reward would be unmistakable.
	require.NoError(t, store.ReconcileAccountRewardBalance(
		0, key, 9_999_999, 150, bytes.Repeat([]byte{0xe2}, 32), nil,
	))

	// Normal accrual resumes afterward.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 1_000_000, 200, bytes.Repeat([]byte{0xe3}, 32), nil,
	))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(10_999_999), uint64(got.Reward))

	require.NoError(t, store.DeleteAccountRewardsAfterSlot(120, nil))

	got, err = store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(3_000_000),
		uint64(got.Reward),
		"rollback across a reconciliation must restore previous_reward "+
			"(3_000_000), never reconciled_amount (9_999_999)",
	)
}

// TestDeleteAccountRewardsAfterSlotClampsUnexplainableUnderflow pins the
// fallback for a gap the delta chain cannot explain at all -- e.g. a
// database old enough to carry a ReconcileAccountRewardBalance overwrite
// from before this fix existed, with no way to reconstruct the missing
// delta. `dingo database truncate`'s own doc comment promises a
// disaster-recovery rollback of arbitrary depth; a hard failure here would
// make that promise false for any database old enough to carry such a gap.
// Clamping to zero and continuing is the documented, deliberate trade-off.
func TestDeleteAccountRewardsAfterSlotClampsUnexplainableUnderflow(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytes.Repeat([]byte{0x9b}, 28)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, CredentialTag: 0, Active: true,
	}))

	// A normal accrual delta exists in history, but the account's current
	// balance is smaller than it -- as if some earlier, un-recorded event
	// (an old-format reconciliation, or any other silent overwrite) had
	// lowered the balance without leaving a trace for this rollback to
	// invert.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 5_000_000, 100, bytes.Repeat([]byte{0xc1}, 32), nil,
	))
	require.NoError(t, store.ReconcileAccountRewardBalance(
		0, key, 500_000, 150, bytes.Repeat([]byte{0xc2}, 32), nil,
	))

	// Simulate an old-format gap by deleting the very delta row that would
	// have made the rollback below invertible, leaving only the original
	// 5,000,000 accrual to walk back through against a much smaller balance.
	_, err := store.writeDB.Exec(
		`DELETE FROM account_reward_delta WHERE added_slot = 150`,
	)
	require.NoError(t, err)

	require.NoError(t, store.DeleteAccountRewardsAfterSlot(80, nil))

	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(0),
		uint64(got.Reward),
		"an unexplainable underflow must clamp to zero rather than fail "+
			"the whole rollback",
	)
}

// TestApplyAccountRewardWithdrawalAppliesAfterReconciliationForSameTxHash
// pins the fix for a bug caught by review on the reconciliation mechanism
// itself: TrustCanonicalWithdrawalOnRewardMismatch deliberately corrects the
// balance to the withdrawal's own claimed pre-withdrawal amount rather than
// to zero, on the assumption that the same transaction's real withdrawal
// gets re-validated and applied normally on the very next retry (see
// ReconcileAccountRewardBalance's doc comment). Before this fix, the
// reconciliation row was keyed on the real transaction's own hash, which is
// the exact (tx_hash, credential_tag, staking_key) triple
// ApplyAccountRewardWithdrawal's idempotency check looks up -- so the real
// withdrawal, replayed for the same transaction, found the reconciliation
// row, believed itself already applied, and returned without ever zeroing
// the balance it was supposed to withdraw. The balance would then stay
// permanently stuck at the reconciled pre-withdrawal amount.
func TestApplyAccountRewardWithdrawalAppliesAfterReconciliationForSameTxHash(
	t *testing.T,
) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytes.Repeat([]byte{0x9b}, 28)
	txHash := bytes.Repeat([]byte{0xd1}, 32)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, CredentialTag: 0, Active: true,
	}))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 3_000_000, 100, bytes.Repeat([]byte{0xd0}, 32), nil,
	))

	// The withdrawal transaction's own validation disagrees with dingo's
	// locally computed balance (2_999_998, the chain-agreed pre-withdrawal
	// amount, versus dingo's 3_000_000); the operator has opted in to
	// trusting the canonical figure, which corrects the balance so replay
	// recovery's ordinary rewind-and-retry can re-validate this exact
	// transaction normally afterward.
	require.NoError(t, store.ReconcileAccountRewardBalance(
		0, key, 2_999_998, 150, txHash, nil,
	))
	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2_999_998), uint64(got.Reward),
		"reconciliation must correct the balance to the chain-agreed amount")

	// Replay recovery's retry re-validates and applies the same withdrawal
	// transaction now that the balance agrees with what it expects.
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, key, 2_999_998, 150, txHash, nil,
	))
	got, err = store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(0),
		uint64(got.Reward),
		"the real withdrawal for the reconciled transaction must actually "+
			"apply and zero the balance, not be short-circuited by the "+
			"reconciliation row's idempotency check",
	)
}
