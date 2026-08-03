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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlite

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// TestAddAccountRewardCreditReplayIsIdempotent reproduces the epoch-rollover
// crash-replay collision: a governance proposal-deposit refund (or MIR /
// POOLREAP credit) is committed during epoch rollover in a transaction
// separate from the one that advances the persisted ledger tip past the
// boundary block. A crash between those commits leaves the credit persisted
// but the tip un-advanced, so on restart the boundary block — and this credit
// — is replayed with the same per-event source hash and boundary slot.
//
// Before the fix the second credit did a bare INSERT into account_reward_delta
// (with a NULL tx_hash that, under SQLite, did not even enforce uniqueness)
// and double-credited; with the empty-but-equal tx_hash seen in production it
// instead hit "UNIQUE constraint failed: account_reward_delta..." and looped
// forever. After the fix the replay is a no-op: no error, reward credited once.
func TestAddAccountRewardCreditReplayIsIdempotent(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	stakeKey := bytes.Repeat([]byte{0xAB}, 28)
	const baseReward = uint64(0)
	// The standard governance proposal deposit (100k ADA).
	const refund = uint64(100_000_000_000)
	// The epoch-rollover boundary slot from the live failure.
	const boundarySlot = uint64(115_689_600)
	// The refunded proposal's tx hash, used as the per-event discriminator.
	proposalHash := bytes.Repeat([]byte{0xF6}, 32)

	account := &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
		AddedSlot:     10,
		Reward:        types.Uint64(baseReward),
	}
	require.NoError(t, store.DB().Create(account).Error)

	// First application of the epoch-rollover credit (committed by a prior
	// run before the crash).
	require.NoError(
		t,
		store.AddAccountRewardByCredential(
			0, stakeKey, refund, boundarySlot, proposalHash, nil,
		),
	)

	// Replay of the same epoch-rollover block on restart: same proposal, same
	// boundary slot. This MUST NOT error and MUST NOT re-credit.
	require.NoError(
		t,
		store.AddAccountRewardByCredential(
			0, stakeKey, refund, boundarySlot, proposalHash, nil,
		),
		"replaying an already-applied epoch-rollover credit must be idempotent",
	)

	// The reward must have been credited exactly once.
	got, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		types.Uint64(baseReward+refund),
		got.Reward,
		"reward must be credited exactly once across replay",
	)

	// Exactly one delta row must exist for this credit.
	var deltas []models.AccountRewardDelta
	require.NoError(
		t,
		store.DB().Where(
			"credential_tag = ? AND staking_key = ? AND added_slot = ?",
			0, stakeKey, boundarySlot,
		).Find(&deltas).Error,
	)
	require.Len(t, deltas, 1, "replay must not create a duplicate delta row")
}

// TestAddAccountRewardCreditReplayWithPersistedEmptyTxHashDelta reproduces the
// exact live failure with a nil source hash (the default before a discriminator
// is supplied): a prior run committed the refund delta (with an empty, non-NULL
// tx_hash) but crashed before advancing the persisted ledger tip. On restart
// the epoch-rollover block is replayed. Before the fix this hit the
// "UNIQUE constraint failed: account_reward_delta..." error and looped forever;
// after the fix the pre-check finds the persisted row and skips.
func TestAddAccountRewardCreditReplayWithPersistedEmptyTxHashDelta(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	stakeKey := bytes.Repeat([]byte{0xEF}, 28)
	const refund = uint64(100_000_000_000)
	const boundarySlot = uint64(115_689_600)

	account := &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
		AddedSlot:     10,
		// The prior run already credited the reward before persisting the
		// delta and crashing; the on-disk reward reflects that credit.
		Reward: types.Uint64(refund),
	}
	require.NoError(t, store.DB().Create(account).Error)

	// Seed the delta row exactly as the prior (crashed) run left it: an
	// empty, non-NULL tx_hash credit at the boundary slot.
	persisted := &models.AccountRewardDelta{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		TxHash:        []byte{},
		Amount:        types.Uint64(refund),
		AddedSlot:     boundarySlot,
		Withdrawal:    false,
	}
	require.NoError(t, store.DB().Create(persisted).Error)

	// Replay the epoch-rollover credit with nil source hash (normalized to an
	// empty blob, matching the seeded row). Must not error on the unique index.
	require.NoError(
		t,
		store.AddAccountRewardByCredential(
			0, stakeKey, refund, boundarySlot, nil, nil,
		),
		"replay against a persisted empty-tx_hash credit delta must be idempotent",
	)

	// Reward unchanged (credited exactly once, by the prior run).
	got, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, types.Uint64(refund), got.Reward)

	var deltas []models.AccountRewardDelta
	require.NoError(
		t,
		store.DB().Where(
			"credential_tag = ? AND staking_key = ? AND added_slot = ?",
			0, stakeKey, boundarySlot,
		).Find(&deltas).Error,
	)
	require.Len(t, deltas, 1, "replay must not create a duplicate delta row")
}

func TestAccountRewardWithdrawalReplayWithEmptyTxHashIsIdempotent(
	t *testing.T,
) {
	t.Parallel()
	store := setupTestDB(t)

	stakeKey := bytes.Repeat([]byte{0xE1}, 28)
	account := &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
		AddedSlot:     10,
		Reward:        types.Uint64(20),
	}
	require.NoError(t, store.DB().Create(account).Error)

	// The first withdrawal has no transaction hash discriminator. A later
	// reward credit makes an accidental second clear observable.
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, stakeKey, 20, 100, nil, nil,
	))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, stakeKey, 7, 200, bytes.Repeat([]byte{0x44}, 32), nil,
	))
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, stakeKey, 20, 100, nil, nil,
	))

	got, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, types.Uint64(7), got.Reward)

	var withdrawals int64
	require.NoError(t, store.DB().Model(&models.AccountRewardDelta{}).
		Where(
			"withdrawal = ? AND credential_tag = ? AND staking_key = ?",
			true, 0, stakeKey,
		).Count(&withdrawals).Error)
	require.Equal(t, int64(1), withdrawals)
}

func TestAccountRewardWithdrawalReplayAtDifferentSlotIsIdempotent(
	t *testing.T,
) {
	t.Parallel()
	store := setupTestDB(t)

	stakeKey := bytes.Repeat([]byte{0xE2}, 28)
	txHash := bytes.Repeat([]byte{0x55}, 32)
	account := &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
		AddedSlot:     10,
		Reward:        types.Uint64(20),
	}
	require.NoError(t, store.DB().Create(account).Error)

	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, stakeKey, 20, 100, txHash, nil,
	))
	// Make a replay-induced second clear observable. The transaction hash is
	// the withdrawal identity, so replaying it at another slot must not consume
	// a reward credited after its first application.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, stakeKey, 7, 200, bytes.Repeat([]byte{0x66}, 32), nil,
	))
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, stakeKey, 20, 300, txHash, nil,
	))

	got, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, types.Uint64(7), got.Reward)

	var withdrawals int64
	require.NoError(t, store.DB().Model(&models.AccountRewardDelta{}).
		Where(
			"withdrawal = ? AND tx_hash = ? AND credential_tag = ? AND staking_key = ?",
			true, txHash, 0, stakeKey,
		).Count(&withdrawals).Error)
	require.Equal(t, int64(1), withdrawals)
}

// TestAddAccountRewardCreditDistinctEventsSameSlot proves the source-hash
// discriminator keeps two genuinely distinct credit events to the same account
// in the same epoch boundary (e.g. an enacted proposal's deposit refund and an
// orphaned child proposal's deposit refund, both returned at the boundary slot)
// as separate rows that each credit the account — they must NOT be collapsed.
func TestAddAccountRewardCreditDistinctEventsSameSlot(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	stakeKey := bytes.Repeat([]byte{0x5A}, 28)
	const slot = uint64(115_689_600)
	const depositA = uint64(30)
	const depositB = uint64(15)
	proposalA := bytes.Repeat([]byte{0x01}, 32)
	proposalB := bytes.Repeat([]byte{0x02}, 32)

	account := &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
		AddedSlot:     10,
		Reward:        types.Uint64(0),
	}
	require.NoError(t, store.DB().Create(account).Error)

	require.NoError(
		t,
		store.AddAccountRewardByCredential(
			0, stakeKey, depositA, slot, proposalA, nil,
		),
	)
	require.NoError(
		t,
		store.AddAccountRewardByCredential(
			0, stakeKey, depositB, slot, proposalB, nil,
		),
		"a distinct refund event at the same slot must not collide",
	)

	got, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		types.Uint64(depositA+depositB),
		got.Reward,
		"both distinct same-slot refunds must be credited",
	)

	var deltas []models.AccountRewardDelta
	require.NoError(
		t,
		store.DB().Where(
			"credential_tag = ? AND staking_key = ? AND added_slot = ?",
			0, stakeKey, slot,
		).Find(&deltas).Error,
	)
	require.Len(t, deltas, 2, "distinct events must each have their own row")
}

// TestAddAccountRewardCreditDistinctEpochs proves the over-strict unique key is
// fixed: the same account receiving a credit (deposit refund / MIR / POOLREAP)
// in two different epochs must produce two distinct delta rows and accumulate
// both credits. Before the fix, credit deltas were keyed without added_slot, so
// the second epoch's credit collided on a clean first pass and per-row rollback
// accounting was lost.
func TestAddAccountRewardCreditDistinctEpochs(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	stakeKey := bytes.Repeat([]byte{0xCD}, 28)
	const credit1 = uint64(100_000_000_000)
	const credit2 = uint64(42_000_000)
	const slot1 = uint64(115_689_600)
	const slot2 = uint64(115_905_600) // a later epoch boundary
	// Same source hash in both epochs, to show added_slot — not just the
	// source hash — distinguishes per-epoch credits.
	source := bytes.Repeat([]byte{0x07}, 32)

	account := &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
		AddedSlot:     10,
		Reward:        types.Uint64(0),
	}
	require.NoError(t, store.DB().Create(account).Error)

	require.NoError(
		t,
		store.AddAccountRewardByCredential(
			0, stakeKey, credit1, slot1, source, nil,
		),
	)
	require.NoError(
		t,
		store.AddAccountRewardByCredential(
			0, stakeKey, credit2, slot2, source, nil,
		),
		"a credit in a later epoch must not collide with an earlier one",
	)

	got, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		types.Uint64(credit1+credit2),
		got.Reward,
		"both per-epoch credits must accumulate",
	)

	var deltas []models.AccountRewardDelta
	require.NoError(
		t,
		store.DB().Where(
			"credential_tag = ? AND staking_key = ?",
			0, stakeKey,
		).Order("added_slot ASC").Find(&deltas).Error,
	)
	require.Len(t, deltas, 2, "each per-epoch credit must be its own delta row")

	// Per-row rollback accounting must reverse only the later epoch's credit.
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(slot1, nil))
	afterRollback, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		types.Uint64(credit1),
		afterRollback.Reward,
		"rollback must reverse only the credit recorded after the slot",
	)
}
