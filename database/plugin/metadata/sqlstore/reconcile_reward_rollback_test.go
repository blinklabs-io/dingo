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
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// TestDeleteAccountRewardsAfterSlotSurvivesReconciliation pins the fix for a
// bug caught live while trying to truncate a database that had recorded a
// ReconcileAccountRewardBalance correction: ReconcileAccountRewardBalance
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

	// ReconcileAccountRewardBalance overwrites the balance to match a
	// canonical peer's claim, at slot 150, in response to a specific
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
func TestDeleteAccountRewardsAfterSlotClampsUnexplainableUnderflow(
	t *testing.T,
) {
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
// itself: ReconcileAccountRewardBalance deliberately corrects the balance to
// the withdrawal's own claimed pre-withdrawal amount rather than
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

// rewardDeficit reads the account table's reward_deficit column directly,
// bypassing the models.Account projection (which never exposes this
// internal bookkeeping column), so tests can assert on it precisely.
func rewardDeficit(t *testing.T, store *Store, key []byte) uint64 {
	t.Helper()
	var deficit sql.NullString
	err := store.writeDB.QueryRow(
		`SELECT reward_deficit FROM account WHERE staking_key = ?`,
		key,
	).Scan(&deficit)
	require.NoError(t, err)
	value, err := parseNullUint64("account reward deficit", deficit)
	require.NoError(t, err)
	return value
}

// TestAddAccountRewardByCredentialNetsRollbackDeficitInsteadOfDivergingForever
// pins the fix for a second, independent bug in the same underflow clamp
// TestDeleteAccountRewardsAfterSlotClampsUnexplainableUnderflow pins: that
// test only proves the clamp lets the rollback succeed. It says nothing
// about what happens to the account afterward, and before this fix, nothing
// good did: AddAccountRewardByCredential is purely additive (current +
// amount), with no way to know a past rollback silently discarded a
// shortfall, so a node that hit the clamp would credit every future reward
// on top of a balance permanently short by exactly the discarded amount --
// while a node that never saw the underlying gap (e.g. one that resynced
// clean after this codebase's own bugs were fixed) would credit the same
// future rewards on top of the correct balance. The two nodes diverge
// forever with no future event ever correcting it.
//
// This reproduces that at the unit level and shows the balance now
// converges to the mathematically correct figure -- the same value plain
// (non-clamping, allow-negative) arithmetic would have produced -- once
// enough future accrual offsets the recorded deficit, instead of
// permanently baking in the shortfall:
//
//	current=500,000, rollback amount=5,000,000 => true value would be
//	-4,500,000 if negative balances were representable.
//	+2,000,000 accrual => true value -2,500,000 (still negative: clamped
//	balance must stay 0, deficit shrinks to 2,500,000).
//	+3,000,000 accrual => true value +500,000 (deficit fully absorbed:
//	balance becomes 500,000, deficit clears to zero).
//
// Before this fix, the same sequence left the balance at 5,000,000
// (2,000,000 + 3,000,000 credited on top of the clamped zero) -- permanently
// 4,500,000 higher than the mathematically correct, canonical figure.
func TestAddAccountRewardByCredentialNetsRollbackDeficitInsteadOfDivergingForever(
	t *testing.T,
) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytes.Repeat([]byte{0x9d}, 28)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, CredentialTag: 0, Active: true,
	}))

	// A normal accrual that a later rollback will need to invert.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 5_000_000, 100, bytes.Repeat([]byte{0xf1}, 32), nil,
	))
	// A reconciliation lowers the balance out from under the accrual above.
	require.NoError(t, store.ReconcileAccountRewardBalance(
		0, key, 500_000, 150, bytes.Repeat([]byte{0xf2}, 32), nil,
	))
	// Simulate an old-format gap (as
	// TestDeleteAccountRewardsAfterSlotClampsUnexplainableUnderflow does) by
	// deleting the delta row that would otherwise make the rollback below
	// invertible without underflowing.
	_, err := store.writeDB.Exec(
		`DELETE FROM account_reward_delta WHERE added_slot = 150`,
	)
	require.NoError(t, err)

	// Rolling back past the original accrual underflows (current 500,000 <
	// rollback amount 5,000,000): the clamp fires, and the 4,500,000
	// shortfall must be recorded rather than discarded.
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(80, nil))
	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(got.Reward))
	require.Equal(t, uint64(4_500_000), rewardDeficit(t, store, key))

	// A real post-rollback accrual smaller than the deficit must be fully
	// absorbed: the visible balance stays zero, and the deficit shrinks.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 2_000_000, 200, bytes.Repeat([]byte{0xf3}, 32), nil,
	))
	got, err = store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(got.Reward))
	require.Equal(t, uint64(2_500_000), rewardDeficit(t, store, key))

	// A further accrual exceeding the remaining deficit must net out the
	// deficit entirely and credit only the remainder -- converging to
	// exactly the value plain, non-clamping arithmetic would have produced,
	// not the 5,000,000 a purely additive credit on top of the clamped zero
	// would otherwise leave permanently diverged from canonical.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 3_000_000, 300, bytes.Repeat([]byte{0xf4}, 32), nil,
	))
	got, err = store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(500_000),
		uint64(got.Reward),
		"the deficit must be netted out of later accrual, converging to the "+
			"canonical value instead of permanently diverging by the "+
			"discarded shortfall",
	)
	require.Equal(t, uint64(0), rewardDeficit(t, store, key))
}

// TestReconcileAccountRewardBalanceRedeliverySurvivesLaterAccrual pins the
// fix for a bug in ReconcileAccountRewardBalance's write ordering: the
// UPDATE that changes account.reward used to run unconditionally, before
// the idempotency-journal INSERT ... ON CONFLICT DO NOTHING that decides
// whether this call is a first delivery or a redelivery of an
// already-applied correction. A redelivered/retried identical reconcile
// call is an expected occurrence this method's own doc comment says the
// discriminator exists to absorb -- but because the UPDATE ran regardless
// of whether the INSERT actually inserted a new row, a redelivery clobbered
// any legitimate accrual that had landed between the original delivery and
// the redelivery, resetting the balance back to the stale reconciled value
// instead of leaving the later accrual untouched.
func TestReconcileAccountRewardBalanceRedeliverySurvivesLaterAccrual(
	t *testing.T,
) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytes.Repeat([]byte{0x9e}, 28)
	txHash := bytes.Repeat([]byte{0xa1}, 32)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, CredentialTag: 0, Active: true,
	}))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 3_000_000, 100, bytes.Repeat([]byte{0xa0}, 32), nil,
	))

	// The reconciliation corrects the balance.
	require.NoError(t, store.ReconcileAccountRewardBalance(
		0, key, 2_999_998, 150, txHash, nil,
	))
	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2_999_998), uint64(got.Reward))

	// A real, distinct accrual lands afterward.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 1_000_000, 200, bytes.Repeat([]byte{0xa2}, 32), nil,
	))
	got, err = store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3_999_998), uint64(got.Reward))

	// The exact same reconciliation event is redelivered (identical
	// credential, corrected amount, slot, and txHash -- e.g. a retried
	// replay-recovery step). It must be a complete no-op: the idempotency
	// insert no-ops on conflict, and the balance write must not run either.
	require.NoError(t, store.ReconcileAccountRewardBalance(
		0, key, 2_999_998, 150, txHash, nil,
	))
	got, err = store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(3_999_998),
		uint64(got.Reward),
		"a redelivered reconciliation must not clobber a legitimate later "+
			"accrual back to the stale reconciled value",
	)
}
