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

// TestCorrectAccountRewardCreditAdjustsLiveBalance is a dingo #4529
// regression: correcting a stake-reward credit that has no later withdrawal
// must both fix the historical row and adjust the live account.reward
// balance by the same delta, since account.reward is a pure running sum of
// every credit since the account's last reset.
func TestCorrectAccountRewardCreditAdjustsLiveBalance(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytes.Repeat([]byte{0x41}, 28)
	source := bytes.Repeat([]byte{0x51}, 32)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, CredentialTag: 0, Active: true,
	}))

	// A stake-reward credit computed with a stale-delegation-corrupted
	// totalActiveStake denominator (dingo #4529): too high by 2.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 100_002, 1_000, source, nil,
	))
	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100_002), uint64(got.Reward))

	// A later, unrelated accrual, exactly as a real chain would keep
	// crediting rewards after the corrupted one.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 5_000, 1_100, bytes.Repeat([]byte{0x52}, 32), nil,
	))
	got, err = store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(105_002), uint64(got.Reward))

	// Repair: correct the original credit down to what a fresh calculation
	// (with the corrected totalActiveStake) actually produces.
	require.NoError(t, store.CorrectAccountRewardCredit(
		0, key, 1_000, source, 100_000, nil,
	))
	got, err = store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(
		t, uint64(105_000), uint64(got.Reward),
		"account.reward must reflect the corrected credit when no later "+
			"withdrawal has reset the balance",
	)

	// Correcting to the same value already stored must be a safe no-op.
	require.NoError(t, store.CorrectAccountRewardCredit(
		0, key, 1_000, source, 100_000, nil,
	))
	got, err = store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(105_000), uint64(got.Reward))
}

// TestCorrectAccountRewardCreditLeavesPostWithdrawalBalanceAlone proves the
// other half of CorrectAccountRewardCredit's contract: once a real
// withdrawal has cleared an account's balance, the real on-chain withdrawal
// amount is already the source of truth for the account's current reward,
// so correcting an earlier, now-historical credit must leave the live
// account.reward untouched. The credit row's amount is still corrected (for
// audit purposes), but the withdrawal's own previous_reward -- which is
// what a boundary before the withdrawal's slot actually reads -- is
// unchanged, so the call reports ErrRewardCreditWithdrawnSince rather than
// claiming the correction is complete for every historical read.
func TestCorrectAccountRewardCreditLeavesPostWithdrawalBalanceAlone(
	t *testing.T,
) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytes.Repeat([]byte{0x42}, 28)
	source := bytes.Repeat([]byte{0x61}, 32)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, CredentialTag: 0, Active: true,
	}))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 100_002, 1_000, source, nil,
	))
	// A real withdrawal clears the balance the corrupted credit fed into --
	// the real chain's own withdrawal amount, which the ledger's withdrawal
	// validation already checked, is ground truth going forward regardless
	// of whether the credit that built up to it was itself exactly right.
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, key, 100_002, 1_050, bytes.Repeat([]byte{0x62}, 32), nil,
	))
	got, err := store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(got.Reward))

	err = store.CorrectAccountRewardCredit(
		0, key, 1_000, source, 100_000, nil,
	)
	require.ErrorIs(t, err, ErrRewardCreditWithdrawnSince)
	got, err = store.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(
		t, uint64(0), uint64(got.Reward),
		"a later real withdrawal already established the true live balance; "+
			"correcting the earlier historical row must not perturb it",
	)
}

// TestCorrectAccountRewardCreditNoMatchingRow proves the credit lookup is
// exact: correcting a (credentialTag, stakeKey, addedSlot, sourceHash)
// tuple with no matching credit row is reported as an error rather than
// silently doing nothing, so a repair tool cannot mistake "nothing to fix"
// for "found the wrong row and skipped it."
func TestCorrectAccountRewardCreditNoMatchingRow(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytes.Repeat([]byte{0x43}, 28)

	err := store.CorrectAccountRewardCredit(
		0, key, 1_000, bytes.Repeat([]byte{0x71}, 32), 100_000, nil,
	)
	require.Error(t, err)
}
