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

package governance

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// refundJournal reads the reward journal rows the refund writers produced at
// slot, the same rows the parity source reads back.
func refundJournal(
	t *testing.T,
	store *tallyTestStore,
	slot uint64,
) []*models.AccountRewardDelta {
	t.Helper()
	rows, err := store.raw.Query(`
SELECT credential_tag, staking_key, tx_hash, amount, added_slot
FROM account_reward_delta
WHERE withdrawal = FALSE AND post_snapshot = TRUE AND added_slot = ?`,
		slot,
	)
	require.NoError(t, err)
	defer rows.Close()
	var out []*models.AccountRewardDelta
	for rows.Next() {
		var d models.AccountRewardDelta
		require.NoError(t, rows.Scan(
			&d.CredentialTag, &d.StakingKey, &d.TxHash, &d.Amount,
			&d.AddedSlot,
		))
		out = append(out, &d)
	}
	require.NoError(t, rows.Err())
	return out
}

func droppedProposal(
	txByte byte,
	index uint32,
	deposit uint64,
	returnAddress []byte,
	epoch, slot uint64,
) *models.GovernanceProposal {
	return &models.GovernanceProposal{
		TxHash:        testBytes(32, txByte),
		ActionIndex:   index,
		Deposit:       deposit,
		ReturnAddress: returnAddress,
		DroppedEpoch:  &epoch,
		DroppedSlot:   &slot,
	}
}

func TestMatchProposalDepositRefundsPairsEachCreditedDeposit(t *testing.T) {
	t.Parallel()

	const (
		epoch = uint64(678)
		slot  = uint64(5_000)
	)
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 0x41)
	returnAddress := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Active:     true,
	}))

	// Two proposals, one return account, equal deposits: the Preview epoch
	// 677 shape, where Koios lists two identical proposal_refund rows.
	first := droppedProposal(0x01, 0, 100, returnAddress, epoch, slot)
	second := droppedProposal(0x02, 0, 100, returnAddress, epoch, slot)
	for _, p := range []*models.GovernanceProposal{first, second} {
		require.NoError(t, refundProposalDeposit(
			context.Background(), db, nil, p, slot,
		))
	}

	refunds, err := MatchProposalDepositRefunds(
		[]*models.GovernanceProposal{first, second},
		epoch,
		refundJournal(t, store, slot),
	)
	require.NoError(t, err)
	require.Len(t, refunds, 2)
	for _, r := range refunds {
		assert.Equal(t, uint8(0), r.CredentialTag)
		assert.Equal(t, stakeCred, r.StakingKey)
		assert.Equal(t, uint64(100), r.Amount)
	}
}

func TestMatchProposalDepositRefundsSkipsDepositReturnedToTreasury(
	t *testing.T,
) {
	t.Parallel()

	const (
		epoch = uint64(678)
		slot  = uint64(5_000)
	)
	db, store := newTallyTestDB(t)
	// The return account was never registered, so the deposit goes to the
	// treasury and no credit is journaled. Koios reports no refund either.
	returnAddress := buildRewardAddr(t, testBytes(28, 0x42))
	p := droppedProposal(0x03, 0, 100, returnAddress, epoch, slot)
	require.NoError(t, refundProposalDeposit(
		context.Background(), db, nil, p, slot,
	))

	refunds, err := MatchProposalDepositRefunds(
		[]*models.GovernanceProposal{p},
		epoch,
		refundJournal(t, store, slot),
	)
	require.NoError(t, err)
	assert.Empty(t, refunds)
}

// A credit to the return account at the same slot from a different source
// (POOLREAP, another proposal's withdrawal) must not be read as the refund.
func TestMatchProposalDepositRefundsIgnoresOtherCreditsToTheAccount(
	t *testing.T,
) {
	t.Parallel()

	const (
		epoch = uint64(678)
		slot  = uint64(5_000)
	)
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 0x43)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Active:     true,
	}))
	credited, err := CreditRegisteredRewardAccountAfterSnapshot(
		context.Background(), db, nil, 0, stakeCred, 100, slot,
		testBytes(32, 0xee),
	)
	require.NoError(t, err)
	require.True(t, credited)

	p := droppedProposal(
		0x04, 0, 100, buildRewardAddr(t, stakeCred), epoch, slot,
	)
	refunds, err := MatchProposalDepositRefunds(
		[]*models.GovernanceProposal{p},
		epoch,
		refundJournal(t, store, slot),
	)
	require.NoError(t, err)
	assert.Empty(t, refunds)
}

// An enacted treasury withdrawal that also pays its own return account
// journals the withdrawal under the proposal hash and the refund under the
// refund discriminator. Only the refund is a deposit refund.
func TestMatchProposalDepositRefundsSeparatesWithdrawalFromRefund(
	t *testing.T,
) {
	t.Parallel()

	const (
		epoch = uint64(678)
		slot  = uint64(5_000)
	)
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 0x44)
	returnAddress := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Active:     true,
	}))
	p := &models.GovernanceProposal{
		TxHash:        testBytes(32, 0x05),
		Deposit:       100,
		ReturnAddress: returnAddress,
		EnactedEpoch:  new(epoch),
		EnactedSlot:   new(slot),
	}
	credited, err := CreditRegisteredRewardAccountAfterSnapshot(
		context.Background(), db, nil, 0, stakeCred, 777, slot,
		proposalRewardSourceHash(p),
	)
	require.NoError(t, err)
	require.True(t, credited)
	require.NoError(t, refundProposalDepositFromSource(
		context.Background(), db, nil, p, slot,
		proposalDepositRefundSourceHash(p),
	))

	refunds, err := MatchProposalDepositRefunds(
		[]*models.GovernanceProposal{p},
		epoch,
		refundJournal(t, store, slot),
	)
	require.NoError(t, err)
	require.Len(t, refunds, 1)
	assert.Equal(t, uint64(100), refunds[0].Amount)
}

func TestMatchProposalDepositRefundsIgnoresCreditsAtOtherSlots(t *testing.T) {
	t.Parallel()

	const epoch = uint64(678)
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 0x45)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Active:     true,
	}))
	p := droppedProposal(
		0x06, 0, 100, buildRewardAddr(t, stakeCred), epoch, 5_000,
	)
	// Journaled at a different boundary than the proposal's recorded slot.
	require.NoError(t, refundProposalDeposit(
		context.Background(), db, nil, p, 4_000,
	))

	refunds, err := MatchProposalDepositRefunds(
		[]*models.GovernanceProposal{p},
		epoch,
		refundJournal(t, store, 4_000),
	)
	require.NoError(t, err)
	assert.Empty(t, refunds)
}

func TestProposalDepositRefundSlots(t *testing.T) {
	t.Parallel()

	enacted := &models.GovernanceProposal{
		EnactedEpoch: new(uint64(10)),
		EnactedSlot:  new(uint64(900)),
	}
	dropped := droppedProposal(1, 0, 1, nil, 10, 900)
	otherEpoch := droppedProposal(2, 0, 1, nil, 9, 400)
	other := droppedProposal(3, 0, 1, nil, 10, 901)
	assert.Equal(
		t,
		[]uint64{900, 901},
		ProposalDepositRefundSlots(
			[]*models.GovernanceProposal{enacted, dropped, otherEpoch, other},
			10,
		),
	)
}

func TestMatchProposalDepositRefundsRejectsBadReturnAddress(t *testing.T) {
	t.Parallel()

	p := droppedProposal(0x07, 0, 100, []byte{0x01}, 10, 900)
	_, err := MatchProposalDepositRefunds(
		[]*models.GovernanceProposal{p}, 10, nil,
	)
	require.Error(t, err)
}
