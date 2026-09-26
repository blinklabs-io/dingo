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
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessEpochSkipsPreConwayProtocolParameters(t *testing.T) {
	t.Parallel()

	pparams := &shelley.ShelleyProtocolParameters{}
	out, err := ProcessEpoch(&EpochInput{
		PParams: pparams,
	})
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Same(t, pparams, out.UpdatedPParams)
}

func TestProcessEpochExtendsDRepsWhenNoLiveProposals(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	credential := testBytes(28, 0x71)
	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		CredentialTag: 0,
		Credential:    credential,
		ExpiryEpoch:   20,
		Active:        true,
	}))

	processBoundary := func() {
		txn := db.MetadataTxn(true)
		defer txn.Release()
		_, err := ProcessEpoch(&EpochInput{
			DB:           db,
			Txn:          txn,
			PrevEpoch:    4,
			NewEpoch:     5,
			BoundarySlot: 500,
			PParams:      conwayPParamsFixture(10),
			UpdateFn: func(
				pparams lcommon.ProtocolParameters,
				_ any,
			) (lcommon.ProtocolParameters, error) {
				return pparams, nil
			},
		})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	}

	processBoundary()
	processBoundary()
	drep, err := store.GetDrepByCredential(0, credential, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(21), drep.ExpiryEpoch)
}

func TestRatificationPreconditionAcceptsZeroCommitteeQuorum(t *testing.T) {
	t.Parallel()

	action := &lcommon.UpdateCommitteeGovAction{
		Type:   uint(lcommon.GovActionTypeUpdateCommittee),
		Quorum: cbor.Rat{Rat: big.NewRat(0, 1)},
	}
	actionCbor, err := cbor.Encode(action)
	require.NoError(t, err)
	proposal := &models.GovernanceProposal{
		ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
		GovActionCbor: actionCbor,
	}

	remaining, err := ratificationEnactmentPrecondition(
		conwayPParamsFixture(10),
		nil,
		proposal,
		0,
	)
	require.NoError(t, err)
	assert.Zero(t, remaining)
}

func TestRefundProposalDepositCreditsRewardAccount(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 1)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	rewardAddrBytes, err := rewardAddr.Bytes()
	require.NoError(t, err)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(5),
		Active:     true,
	}))

	err = refundProposalDeposit(db, nil, &models.GovernanceProposal{
		Deposit:       7,
		ReturnAddress: rewardAddrBytes,
	}, 123)
	require.NoError(t, err)

	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(12), uint64(account.Reward))
}

func TestRefundProposalDeposit_DistinguishesSameTxActionIndex(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 0x31)
	rewardAddrBytes := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))

	txHash := testBytes(32, 0x32)
	first := &models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   0,
		Deposit:       7,
		ReturnAddress: rewardAddrBytes,
	}
	second := &models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   1,
		Deposit:       11,
		ReturnAddress: rewardAddrBytes,
	}
	require.NoError(t, refundProposalDeposit(db, nil, first, 123))
	require.NoError(t, refundProposalDeposit(db, nil, second, 123))

	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(18), uint64(account.Reward))

	rows, err := store.raw.Query(`
SELECT tx_hash, amount FROM account_reward_delta
WHERE credential_tag = ? AND staking_key = ? AND added_slot = ?`,
		0, stakeCred, uint64(123),
	)
	require.NoError(t, err)
	var deltas []models.AccountRewardDelta
	for rows.Next() {
		var delta models.AccountRewardDelta
		require.NoError(t, rows.Scan(&delta.TxHash, &delta.Amount))
		deltas = append(deltas, delta)
	}
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
	require.Len(t, deltas, 2)
	assert.NotEqual(
		t,
		proposalRewardSourceHash(first),
		proposalRewardSourceHash(second),
	)
	// Pin the caller contract: refundProposalDeposit must journal each refund
	// under its own per-proposal source hash, so two refunds sharing a tx hash
	// stay distinct replay-idempotent rows. Rows are matched by their stored
	// discriminator rather than by query order.
	bySourceHash := make(map[string]models.AccountRewardDelta, len(deltas))
	for _, delta := range deltas {
		bySourceHash[string(delta.TxHash)] = delta
	}
	require.Len(t, bySourceHash, 2, "journaled TxHash values must be distinct")
	for _, tc := range []struct {
		name     string
		proposal *models.GovernanceProposal
		amount   uint64
	}{
		{name: "first", proposal: first, amount: 7},
		{name: "second", proposal: second, amount: 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantHash := proposalRewardSourceHash(tc.proposal)
			delta, ok := bySourceHash[string(wantHash)]
			require.True(
				t,
				ok,
				"no reward delta journaled with proposalRewardSourceHash",
			)
			assert.Equal(t, wantHash, delta.TxHash)
			assert.Equal(t, tc.amount, uint64(delta.Amount))
		})
	}
}

// TestProcessEpochExpiresProposalWithoutRefundingDeposit pins the first half
// of the expire/drop split (dingo#4411): marking a proposal expired must not
// itself refund its deposit. cardano-ledger does not return an expired
// governance action's deposit until one full epoch after it is marked
// expired -- refunding it immediately inflated the very next mark snapshot's
// total active stake by the deposit amount whenever the return account was
// still delegated. See
// TestProcessEpochDropsExpiredProposalAndRefundsDepositNextEpoch for the
// drop half.
func TestProcessEpochExpiresProposalWithoutRefundingDeposit(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 2)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	rewardAddrBytes, err := rewardAddr.Bytes()
	require.NoError(t, err)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(5),
		Active:     true,
	}))
	txHash := testBytes(32, 3)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch: 1,
		ExpiresEpoch:  4,
		AnchorURL:     "https://example.invalid/expired",
		AnchorHash:    testBytes(32, 4),
		Deposit:       7,
		ReturnAddress: rewardAddrBytes,
		AddedSlot:     100,
	}, nil))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    4,
		NewEpoch:     5,
		BoundarySlot: 500,
		PParams:      conwayPParamsFixture(10),
		UpdateFn: func(
			pparams lcommon.ProtocolParameters,
			_ any,
		) (lcommon.ProtocolParameters, error) {
			return pparams, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	assert.Equal(t, 1, out.ExpiredCount)
	assert.Equal(t, 0, out.DroppedCount)
	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(
		t,
		uint64(5),
		uint64(account.Reward),
		"marking a proposal expired must not refund its deposit yet",
	)

	proposal, err := db.GetGovernanceProposal(txHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, proposal.ExpiredEpoch)
	require.NotNil(t, proposal.ExpiredSlot)
	assert.Equal(t, uint64(5), *proposal.ExpiredEpoch)
	assert.Equal(t, uint64(500), *proposal.ExpiredSlot)
	assert.Nil(t, proposal.DroppedEpoch)
	assert.Nil(t, proposal.DroppedSlot)
}

// TestProcessEpochDropsExpiredProposalAndRefundsDepositNextEpoch is the
// regression test for dingo#4411: a proposal marked expired at epoch E must
// have its deposit refunded only when ProcessEpoch runs for epoch E+1, not
// immediately.
func TestProcessEpochDropsExpiredProposalAndRefundsDepositNextEpoch(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 2)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	rewardAddrBytes, err := rewardAddr.Bytes()
	require.NoError(t, err)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(5),
		Active:     true,
	}))
	txHash := testBytes(32, 3)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch: 1,
		ExpiresEpoch:  4,
		AnchorURL:     "https://example.invalid/expired",
		AnchorHash:    testBytes(32, 4),
		Deposit:       7,
		ReturnAddress: rewardAddrBytes,
		AddedSlot:     100,
	}, nil))

	runEpoch := func(newEpoch, boundarySlot uint64) *EpochOutput {
		t.Helper()
		txn := db.MetadataTxn(true)
		defer txn.Release()
		out, err := ProcessEpoch(&EpochInput{
			DB:           db,
			Txn:          txn,
			PrevEpoch:    newEpoch - 1,
			NewEpoch:     newEpoch,
			BoundarySlot: boundarySlot,
			PParams:      conwayPParamsFixture(10),
			UpdateFn: func(
				pparams lcommon.ProtocolParameters,
				_ any,
			) (lcommon.ProtocolParameters, error) {
				return pparams, nil
			},
		})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
		return out
	}

	// Epoch 5: marked expired, no refund yet.
	out := runEpoch(5, 500)
	assert.Equal(t, 1, out.ExpiredCount)
	assert.Equal(t, 0, out.DroppedCount)
	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(5), uint64(account.Reward))

	// Epoch 6: dropped, deposit refunded now.
	out = runEpoch(6, 600)
	assert.Equal(t, 0, out.ExpiredCount)
	assert.Equal(t, 1, out.DroppedCount)
	account, err = store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(12), uint64(account.Reward))

	proposal, err := db.GetGovernanceProposal(txHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, proposal.ExpiredEpoch)
	assert.Equal(t, uint64(5), *proposal.ExpiredEpoch)
	require.NotNil(t, proposal.DroppedEpoch)
	require.NotNil(t, proposal.DroppedSlot)
	assert.Equal(t, uint64(6), *proposal.DroppedEpoch)
	assert.Equal(t, uint64(600), *proposal.DroppedSlot)
}

// TestProcessEpochReplayedExpireBoundaryDoesNotDropInSameEpoch covers the
// crash-replay half of dingo#4411. A boundary that is reprocessed reruns
// against the expiry the first pass already wrote, so the drop step's own
// `expired_epoch < NewEpoch` bound -- not merely its position ahead of the
// expiry step -- is what keeps the refund out of the epoch that expired it.
func TestProcessEpochReplayedExpireBoundaryDoesNotDropInSameEpoch(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 0x71)
	rewardAddrBytes := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(5),
		Active:     true,
	}))
	txHash := testBytes(32, 0x72)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch: 1,
		ExpiresEpoch:  4,
		AnchorURL:     "https://example.invalid/replay-expire",
		AnchorHash:    testBytes(32, 0x73),
		Deposit:       7,
		ReturnAddress: rewardAddrBytes,
		AddedSlot:     100,
	}, nil))

	runEpoch := func(newEpoch, boundarySlot uint64) *EpochOutput {
		t.Helper()
		txn := db.MetadataTxn(true)
		defer txn.Release()
		out, err := ProcessEpoch(&EpochInput{
			DB:           db,
			Txn:          txn,
			PrevEpoch:    newEpoch - 1,
			NewEpoch:     newEpoch,
			BoundarySlot: boundarySlot,
			PParams:      conwayPParamsFixture(10),
			UpdateFn: func(
				pparams lcommon.ProtocolParameters,
				_ any,
			) (lcommon.ProtocolParameters, error) {
				return pparams, nil
			},
		})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
		return out
	}

	out := runEpoch(5, 500)
	assert.Equal(t, 1, out.ExpiredCount)
	assert.Equal(t, 0, out.DroppedCount)

	// The same boundary reprocessed after a commit crash.
	out = runEpoch(5, 500)
	assert.Equal(t, 0, out.DroppedCount)

	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(
		t,
		uint64(5),
		uint64(account.Reward),
		"a reprocessed expire boundary must not refund the deposit",
	)
	proposal, err := db.GetGovernanceProposal(txHash, 0, nil)
	require.NoError(t, err)
	assert.Nil(t, proposal.DroppedEpoch)

	// The refund still arrives at the following epoch.
	out = runEpoch(6, 600)
	assert.Equal(t, 1, out.DroppedCount)
	account, err = store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(12), uint64(account.Reward))
}

// TestProcessEpochRefundsEnactmentOrphanInTheEnactingEpoch pins the half of
// the removal path that does not defer. cardano-ledger unions expiredActions,
// enactedActions and removedDueToEnactment and returns all of their deposits
// in one EPOCH tick, so a competing sibling removed because another action
// enacted is refunded alongside the winner rather than an epoch later.
func TestProcessEpochRefundsEnactmentOrphanInTheEnactingEpoch(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	winnerCred := testBytes(28, 0x91)
	winnerAddr := buildRewardAddr(t, winnerCred)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: winnerCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))
	siblingCred := testBytes(28, 0x92)
	siblingAddr := buildRewardAddr(t, siblingCred)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: siblingCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))

	ratifiedEpoch := uint64(4)
	ratifiedSlot := uint64(400)
	winnerHash := testBytes(32, 0x93)
	siblingHash := testBytes(32, 0x94)
	require.NoError(t, db.SetGovernanceProposal(buildNoConfidenceProposal(
		t, winnerHash, 0, 10, 30, winnerAddr, 100,
		nil, nil, &ratifiedEpoch, &ratifiedSlot,
	), nil))
	require.NoError(t, db.SetGovernanceProposal(buildNoConfidenceProposal(
		t, siblingHash, 0, 12, 25, siblingAddr, 101,
		nil, nil, nil, nil,
	), nil))

	runEpoch := func(newEpoch, boundarySlot uint64) *EpochOutput {
		t.Helper()
		txn := db.MetadataTxn(true)
		defer txn.Release()
		out, err := ProcessEpoch(&EpochInput{
			DB:           db,
			Txn:          txn,
			PrevEpoch:    newEpoch - 1,
			NewEpoch:     newEpoch,
			BoundarySlot: boundarySlot,
			PParams:      conwayPParamsFixture(10),
			UpdateFn: func(
				pparams lcommon.ProtocolParameters,
				_ any,
			) (lcommon.ProtocolParameters, error) {
				return pparams, nil
			},
		})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
		return out
	}

	out := runEpoch(5, 500)
	assert.Equal(t, 1, out.OrphanedCount)

	winner, err := store.GetAccountByCredential(0, winnerCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, winner)
	assert.Equal(t, uint64(30), uint64(winner.Reward))

	sibling, err := store.GetAccountByCredential(0, siblingCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, sibling)
	assert.Equal(
		t,
		uint64(25),
		uint64(sibling.Reward),
		"a sibling removed by an enactment is refunded in that same tick",
	)

	// The drop step must not refund it a second time next epoch.
	out = runEpoch(6, 600)
	assert.Equal(t, 0, out.DroppedCount)
	sibling, err = store.GetAccountByCredential(0, siblingCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, sibling)
	assert.Equal(t, uint64(25), uint64(sibling.Reward))
}

// TestProcessEpochReplaysBoundaryDroppedProposalAfterStakeRewardReset mirrors
// TestProcessEpochReplaysBoundaryTreasuryWithdrawalAfterStakeRewardReset for
// the drop step: a crash-replayed boundary must reapply the deposit refund's
// treasury/pot side effects without re-marking the proposal dropped twice.
func TestProcessEpochReplaysBoundaryDroppedProposalAfterStakeRewardReset(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 0x20)
	rewardAddrBytes := buildRewardAddr(t, stakeCred)
	txHash := testBytes(32, 0x21)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch: 1,
		ExpiresEpoch:  4,
		AnchorURL:     "https://example.invalid/replay-expired",
		AnchorHash:    testBytes(32, 0x22),
		Deposit:       25,
		ReturnAddress: rewardAddrBytes,
		AddedSlot:     100,
	}, nil))
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	runEpoch := func(newEpoch, boundarySlot uint64) *EpochOutput {
		t.Helper()
		txn := db.MetadataTxn(true)
		defer txn.Release()
		out, err := ProcessEpoch(&EpochInput{
			DB:           db,
			Txn:          txn,
			PrevEpoch:    newEpoch - 1,
			NewEpoch:     newEpoch,
			BoundarySlot: boundarySlot,
			PParams:      conwayPParamsFixture(10),
			UpdateFn: func(
				pparams lcommon.ProtocolParameters,
				_ any,
			) (lcommon.ProtocolParameters, error) {
				return pparams, nil
			},
		})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
		return out
	}

	// Epoch 5: marked expired, no refund/treasury effect yet.
	out := runEpoch(5, 500)
	assert.Equal(t, 1, out.ExpiredCount)
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(100), uint64(state.Treasury))

	// Epoch 6: dropped, deposit refunded to treasury (no reward account).
	out = runEpoch(6, 600)
	assert.Equal(t, 1, out.DroppedCount)
	state, err = store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(125), uint64(state.Treasury))

	// Crash replay re-applies stake rewards first, which resets the same
	// boundary NetworkState row before governance is run again for the same
	// (epoch 6) boundary.
	require.NoError(t, store.SetNetworkState(100, 20, 600, nil))

	out = runEpoch(6, 600)
	assert.Equal(t, 0, out.DroppedCount)
	state, err = store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(125), uint64(state.Treasury))
	assert.Equal(t, uint64(20), uint64(state.Reserves))
}

func TestProcessEpochReturnsMissingRewardAccountRefundToTreasury(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 2)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	rewardAddrBytes, err := rewardAddr.Bytes()
	require.NoError(t, err)
	txHash := testBytes(32, 3)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch: 1,
		ExpiresEpoch:  4,
		AnchorURL:     "https://example.invalid/expired",
		AnchorHash:    testBytes(32, 4),
		Deposit:       7,
		ReturnAddress: rewardAddrBytes,
		AddedSlot:     100,
	}, nil))
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	runEpoch := func(newEpoch, boundarySlot uint64) *EpochOutput {
		t.Helper()
		txn := db.MetadataTxn(true)
		defer txn.Release()
		out, err := ProcessEpoch(&EpochInput{
			DB:           db,
			Txn:          txn,
			PrevEpoch:    newEpoch - 1,
			NewEpoch:     newEpoch,
			BoundarySlot: boundarySlot,
			PParams:      conwayPParamsFixture(10),
			UpdateFn: func(
				pparams lcommon.ProtocolParameters,
				_ any,
			) (lcommon.ProtocolParameters, error) {
				return pparams, nil
			},
		})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
		return out
	}

	// Epoch 5: marked expired, no treasury effect yet.
	out := runEpoch(5, 500)
	assert.Equal(t, 1, out.ExpiredCount)
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(100), uint64(state.Treasury))

	// Epoch 6: dropped, unclaimed deposit routed to treasury now.
	out = runEpoch(6, 600)
	assert.Equal(t, 1, out.DroppedCount)

	active, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	assert.Nil(t, active, "refund must not create a reward account")
	account, err := store.GetAccountByCredential(0, stakeCred, true, nil)
	require.NoError(t, err)
	assert.Nil(t, account)
	state, err = store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(107), uint64(state.Treasury))
	assert.Equal(t, uint64(20), uint64(state.Reserves))

	proposal, err := db.GetGovernanceProposal(txHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, proposal.ExpiredEpoch)
	require.NotNil(t, proposal.ExpiredSlot)
	assert.Equal(t, uint64(5), *proposal.ExpiredEpoch)
	assert.Equal(t, uint64(500), *proposal.ExpiredSlot)
	require.NotNil(t, proposal.DroppedEpoch)
	assert.Equal(t, uint64(6), *proposal.DroppedEpoch)
}

// TestProcessEpochBootstrapParameterChangeWithoutCommitteeDoesNotRatify
// verifies that the epoch-boundary caller does not treat PV9's committee
// minimum-size exception as approval from an absent committee.
func TestProcessEpochBootstrapParameterChangeWithoutCommitteeDoesNotRatify(
	t *testing.T,
) {
	t.Parallel()

	db, _ := newTallyTestDB(t)
	poolDeposit := uint(1234)
	actionCbor, err := cbor.Encode(&conway.ConwayParameterChangeGovAction{
		Type: uint(lcommon.GovActionTypeParameterChange),
		ParamUpdate: conway.ConwayProtocolParameterUpdate{
			PoolDeposit: &poolDeposit,
		},
	})
	require.NoError(t, err)

	txHash := testBytes(32, 0x61)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeParameterChange),
		ProposedEpoch: stabilityTestEpoch - 1,
		ExpiresEpoch:  stabilityTestEpoch + 10,
		AnchorURL:     "https://example.invalid/bootstrap-boundary",
		AnchorHash:    testBytes(32, 0x62),
		ReturnAddress: testBytes(29, 0x63),
		GovActionCbor: actionCbor,
		AddedSlot:     100,
		Deposit:       1_000,
	}, nil))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    stabilityTestEpoch - 1,
		NewEpoch:     stabilityTestEpoch,
		BoundarySlot: 500,
		PParams:      conwayPParamsFixture(9),
		UpdateFn: func(
			pparams lcommon.ProtocolParameters,
			_ any,
		) (lcommon.ProtocolParameters, error) {
			return pparams, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	assert.Equal(t, 0, out.RatifiedCount)
	proposal, err := db.GetGovernanceProposal(txHash, 0, nil)
	require.NoError(t, err)
	assert.Nil(t, proposal.RatifiedEpoch)
	assert.Nil(t, proposal.RatifiedSlot)
}

func TestProcessEpochRatifiesConwayAndDijkstra(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pparams func() lcommon.ProtocolParameters
	}{
		{
			name: "Conway",
			pparams: func() lcommon.ProtocolParameters {
				return &conway.ConwayProtocolParameters{
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: 10,
					},
				}
			},
		},
		{
			name: "Dijkstra",
			pparams: func() lcommon.ProtocolParameters {
				return &gdijkstra.DijkstraProtocolParameters{
					ConwayProtocolParameters: conway.ConwayProtocolParameters{
						ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
							Major: gdijkstra.MinProtocolVersionDijkstra,
						},
					},
					MaxRefScriptSizePerBlock: 1_000,
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, _ := newTallyTestDB(t)
			actionCbor, err := cbor.Encode(&lcommon.NoConfidenceGovAction{
				Type: uint(lcommon.GovActionTypeNoConfidence),
			})
			require.NoError(t, err)

			txHash := testBytes(32, 0xA1)
			require.NoError(t, db.SetGovernanceProposal(
				&models.GovernanceProposal{
					TxHash:        txHash,
					ActionIndex:   0,
					ActionType:    uint8(lcommon.GovActionTypeNoConfidence),
					ProposedEpoch: 4,
					ExpiresEpoch:  10,
					AnchorURL:     "https://example.invalid/no-confidence",
					AnchorHash:    testBytes(32, 0xA2),
					ReturnAddress: testBytes(29, 0xA3),
					GovActionCbor: actionCbor,
					AddedSlot:     400,
				},
				nil,
			))

			txn := db.MetadataTxn(true)
			defer txn.Release()
			out, err := ProcessEpoch(&EpochInput{
				DB:           db,
				Txn:          txn,
				PrevEpoch:    4,
				NewEpoch:     5,
				BoundarySlot: 500,
				PParams:      test.pparams(),
				UpdateFn: func(
					pparams lcommon.ProtocolParameters,
					_ any,
				) (lcommon.ProtocolParameters, error) {
					return pparams, nil
				},
			})
			require.NoError(t, err)
			require.NoError(t, txn.Commit())

			require.Equal(
				t,
				1,
				out.RatifiedCount,
				"ProcessEpoch must run the governance ratification path",
			)
			proposal, err := db.GetGovernanceProposal(txHash, 0, nil)
			require.NoError(t, err)
			require.NotNil(t, proposal.RatifiedEpoch)
			assert.Equal(t, uint64(5), *proposal.RatifiedEpoch)
		})
	}
}

func TestProcessEpochRatifiesAndEnactsDijkstraOnlyParameterChanges(
	t *testing.T,
) {
	t.Parallel()

	maxBlock := uint32(2_000)
	maxTx := uint32(1_000)
	stride := uint32(128)
	multiplier := &cbor.Rat{Rat: new(big.Rat).SetFrac64(7, 4)}
	tests := []struct {
		name   string
		update gdijkstra.DijkstraProtocolParameterUpdate
		assert func(*testing.T, *gdijkstra.DijkstraProtocolParameters)
	}{
		{
			name: "key-34-max-ref-script-size-per-block",
			update: gdijkstra.DijkstraProtocolParameterUpdate{
				MaxRefScriptSizePerBlock: &maxBlock,
			},
			assert: func(t *testing.T, p *gdijkstra.DijkstraProtocolParameters) {
				require.Equal(t, maxBlock, p.MaxRefScriptSizePerBlock)
			},
		},
		{
			name: "key-35-max-ref-script-size-per-tx",
			update: gdijkstra.DijkstraProtocolParameterUpdate{
				MaxRefScriptSizePerTx: &maxTx,
			},
			assert: func(t *testing.T, p *gdijkstra.DijkstraProtocolParameters) {
				require.Equal(t, maxTx, p.MaxRefScriptSizePerTx)
			},
		},
		{
			name: "key-36-ref-script-cost-stride",
			update: gdijkstra.DijkstraProtocolParameterUpdate{
				RefScriptCostStride: &stride,
			},
			assert: func(t *testing.T, p *gdijkstra.DijkstraProtocolParameters) {
				require.Equal(t, stride, p.RefScriptCostStride)
			},
		},
		{
			name: "key-37-ref-script-cost-multiplier",
			update: gdijkstra.DijkstraProtocolParameterUpdate{
				RefScriptCostMultiplier: multiplier,
			},
			assert: func(t *testing.T, p *gdijkstra.DijkstraProtocolParameters) {
				require.Zero(t, p.RefScriptCostMultiplier.Cmp(multiplier.Rat))
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, store := newTallyTestDB(t)
			actionCbor, err := cbor.Encode(
				&gdijkstra.DijkstraParameterChangeGovAction{
					Type:        uint(lcommon.GovActionTypeParameterChange),
					ParamUpdate: test.update,
				},
			)
			require.NoError(t, err)

			txHash := testBytes(32, byte(0xB0+index))
			require.NoError(t, db.SetGovernanceProposal(
				&models.GovernanceProposal{
					TxHash:        txHash,
					ActionIndex:   0,
					ActionType:    uint8(lcommon.GovActionTypeParameterChange),
					ProposedEpoch: stabilityTestEpoch - 1,
					ExpiresEpoch:  stabilityTestEpoch + 10,
					AnchorURL:     "https://example.invalid/dijkstra-param",
					AnchorHash:    testBytes(32, byte(0xC0+index)),
					ReturnAddress: testBytes(29, byte(0xD0+index)),
					GovActionCbor: actionCbor,
					AddedSlot:     400,
				},
				nil,
			))
			proposal, err := db.GetGovernanceProposal(txHash, 0, nil)
			require.NoError(t, err)
			drepCred := seedDRepWithStake(t, db, 100)
			seedDRepYesVote(t, db, proposal.ID, drepCred)
			seedHardForkCommitteeAndSPOVotes(t, db, store, proposal)

			pparams := &gdijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: gdijkstra.MinProtocolVersionDijkstra,
					},
					MinCommitteeSize: 1,
					PoolVotingThresholds: conway.PoolVotingThresholds{
						PpSecurityGroup: newRat(1, 1),
					},
					DRepVotingThresholds: conway.DRepVotingThresholds{
						PpNetworkGroup: newRat(1, 1),
						PpGovGroup:     newRat(1, 1),
					},
				},
				MaxRefScriptSizePerBlock: 1_000,
				MaxRefScriptSizePerTx:    500,
				RefScriptCostStride:      64,
				RefScriptCostMultiplier:  testRatPtr(3, 2),
				CommitteeStakeCoverage:   testRatPtr(2, 3),
				QuorumStakeThreshold:     testRatPtr(3, 5),
			}

			runEpoch := func(
				prevEpoch uint64,
				newEpoch uint64,
				activePParams lcommon.ProtocolParameters,
			) *EpochOutput {
				t.Helper()
				txn := db.MetadataTxn(true)
				defer txn.Release()
				out, processErr := ProcessEpoch(&EpochInput{
					DB:           db,
					Txn:          txn,
					PrevEpoch:    prevEpoch,
					NewEpoch:     newEpoch,
					BoundarySlot: newEpoch * 100,
					PParams:      activePParams,
					UpdateFn:     eras.PParamsUpdateDijkstra,
				})
				require.NoError(t, processErr)
				require.NoError(t, txn.Commit())
				return out
			}

			ratification := runEpoch(
				stabilityTestEpoch-1,
				stabilityTestEpoch,
				pparams,
			)
			require.Equal(t, 1, ratification.RatifiedCount)
			stored, err := db.GetGovernanceProposal(txHash, 0, nil)
			require.NoError(t, err)
			require.NotNil(t, stored.RatifiedEpoch)
			require.Equal(t, stabilityTestEpoch, *stored.RatifiedEpoch)

			enactment := runEpoch(
				stabilityTestEpoch,
				stabilityTestEpoch+1,
				ratification.UpdatedPParams,
			)
			require.Equal(t, 1, enactment.EnactedCount)
			updated, ok := enactment.UpdatedPParams.(*gdijkstra.DijkstraProtocolParameters)
			require.True(t, ok)
			test.assert(t, updated)
			stored, err = db.GetGovernanceProposal(txHash, 0, nil)
			require.NoError(t, err)
			require.NotNil(t, stored.EnactedEpoch)
			require.Equal(t, stabilityTestEpoch+1, *stored.EnactedEpoch)
		})
	}
}

// TestProcessEpochEnactsConwayParameterChangeReportsPlutusV2CostModelWritten
// covers blinklabs-io/dingo#3825's PR review (wolf31o2): a real ratify+enact
// cycle through ProcessEpoch, not EnactProposal called in isolation, must
// still surface PlutusV2CostModelWritten on the resulting EpochOutput --
// proving applyEnactmentResult's OR into EpochOutput actually connects to
// EnactmentResult, the one hop between the two that was previously
// untested.
func TestProcessEpochEnactsConwayParameterChangeReportsPlutusV2CostModelWritten(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	actionCbor, err := cbor.Encode(
		&conway.ConwayParameterChangeGovAction{
			Type: uint(lcommon.GovActionTypeParameterChange),
			ParamUpdate: conway.ConwayProtocolParameterUpdate{
				CostModels: map[uint][]int64{1: {205665, 812, 1}},
			},
		},
	)
	require.NoError(t, err)

	txHash := testBytes(32, 0xE0)
	require.NoError(t, db.SetGovernanceProposal(
		&models.GovernanceProposal{
			TxHash:        txHash,
			ActionIndex:   0,
			ActionType:    uint8(lcommon.GovActionTypeParameterChange),
			ProposedEpoch: stabilityTestEpoch - 1,
			ExpiresEpoch:  stabilityTestEpoch + 10,
			AnchorURL:     "https://example.invalid/conway-cost-model-epoch",
			AnchorHash:    testBytes(32, 0xE1),
			ReturnAddress: testBytes(29, 0xE2),
			GovActionCbor: actionCbor,
			AddedSlot:     400,
		},
		nil,
	))
	proposal, err := db.GetGovernanceProposal(txHash, 0, nil)
	require.NoError(t, err)
	drepCred := seedDRepWithStake(t, db, 100)
	seedDRepYesVote(t, db, proposal.ID, drepCred)
	seedHardForkCommitteeAndSPOVotes(t, db, store, proposal)

	pparams := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: conway.MinProtocolVersionConway,
		},
		MinCommitteeSize: 1,
		CostModels:       map[uint][]int64{0: {1, 2, 3}},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			PpSecurityGroup: newRat(1, 1),
		},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			PpNetworkGroup: newRat(1, 1),
			PpGovGroup:     newRat(1, 1),
		},
	}

	runEpoch := func(
		prevEpoch uint64,
		newEpoch uint64,
		activePParams lcommon.ProtocolParameters,
	) *EpochOutput {
		t.Helper()
		txn := db.MetadataTxn(true)
		defer txn.Release()
		out, processErr := ProcessEpoch(&EpochInput{
			DB:           db,
			Txn:          txn,
			PrevEpoch:    prevEpoch,
			NewEpoch:     newEpoch,
			BoundarySlot: newEpoch * 100,
			PParams:      activePParams,
			UpdateFn:     eras.PParamsUpdateConway,
		})
		require.NoError(t, processErr)
		require.NoError(t, txn.Commit())
		return out
	}

	ratification := runEpoch(stabilityTestEpoch-1, stabilityTestEpoch, pparams)
	require.Equal(t, 1, ratification.RatifiedCount)
	require.False(t, ratification.PlutusV2CostModelWritten,
		"ratification alone must not report the write -- only enactment does")

	enactment := runEpoch(
		stabilityTestEpoch, stabilityTestEpoch+1, ratification.UpdatedPParams,
	)
	require.Equal(t, 1, enactment.EnactedCount)
	require.True(t, enactment.PlutusV2CostModelWritten,
		"the enacted ParamUpdate explicitly specified CostModels[1]")
	updated, ok := enactment.UpdatedPParams.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	require.Contains(t, updated.CostModels, uint(1))
}

func TestProcessEpochReplaysBoundaryTreasuryWithdrawalAfterStakeRewardReset(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 0x30)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	rewardAddrBytes, err := rewardAddr.Bytes()
	require.NoError(t, err)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))
	withdrawalCbor, err := cbor.Encode(
		&lcommon.TreasuryWithdrawalGovAction{
			Type:        2,
			Withdrawals: map[*lcommon.Address]uint64{&rewardAddr: 7},
		},
	)
	require.NoError(t, err)
	ratifiedEpoch := uint64(4)
	ratifiedSlot := uint64(400)
	txHash := testBytes(32, 0x31)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 3,
		ExpiresEpoch:  10,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
		AnchorURL:     "https://example.invalid/replay-withdrawal",
		AnchorHash:    testBytes(32, 0x32),
		Deposit:       0,
		ReturnAddress: rewardAddrBytes,
		GovActionCbor: withdrawalCbor,
		AddedSlot:     100,
	}, nil))
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	runEpoch := func() *EpochOutput {
		t.Helper()
		txn := db.MetadataTxn(true)
		defer txn.Release()
		out, err := ProcessEpoch(&EpochInput{
			DB:           db,
			Txn:          txn,
			PrevEpoch:    4,
			NewEpoch:     5,
			BoundarySlot: 500,
			PParams:      conwayPParamsFixture(10),
			UpdateFn: func(
				pparams lcommon.ProtocolParameters,
				_ any,
			) (lcommon.ProtocolParameters, error) {
				return pparams, nil
			},
		})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
		return out
	}

	out := runEpoch()
	assert.Equal(t, 1, out.EnactedCount)
	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(7), uint64(account.Reward))
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(93), uint64(state.Treasury))

	require.NoError(t, store.SetNetworkState(100, 20, 500, nil))

	out = runEpoch()
	assert.Equal(t, 0, out.EnactedCount)
	account, err = store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(7), uint64(account.Reward))
	state, err = store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(93), uint64(state.Treasury))
	assert.Equal(t, uint64(20), uint64(state.Reserves))
}

func TestProcessEpochUnclaimedDepositDoesNotIncreaseWithdrawalCapacity(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	missingStakeCred := testBytes(28, 5)
	missingReturnAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		missingStakeCred,
	)
	require.NoError(t, err)
	missingReturnAddrBytes, err := missingReturnAddr.Bytes()
	require.NoError(t, err)

	withdrawStakeCred := testBytes(28, 6)
	withdrawAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		withdrawStakeCred,
	)
	require.NoError(t, err)
	withdrawAddrBytes, err := withdrawAddr.Bytes()
	require.NoError(t, err)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: withdrawStakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))

	constitutionAction := &lcommon.NewConstitutionGovAction{Type: 5}
	constitutionAction.Constitution.Anchor.Url =
		"https://example.invalid/constitution"
	copy(
		constitutionAction.Constitution.Anchor.DataHash[:],
		testBytes(32, 7),
	)
	constitutionCbor, err := cbor.Encode(constitutionAction)
	require.NoError(t, err)
	withdrawalCbor, err := cbor.Encode(
		&lcommon.TreasuryWithdrawalGovAction{
			Type: 2,
			Withdrawals: map[*lcommon.Address]uint64{
				&withdrawAddr: 120,
			},
		},
	)
	require.NoError(t, err)

	ratifiedEpoch := uint64(4)
	ratifiedSlot := uint64(400)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 8),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeNewConstitution),
		ProposedEpoch: 3,
		ExpiresEpoch:  10,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
		AnchorURL:     "https://example.invalid/proposal-a",
		AnchorHash:    testBytes(32, 9),
		Deposit:       50,
		ReturnAddress: missingReturnAddrBytes,
		GovActionCbor: constitutionCbor,
		AddedSlot:     100,
	}, nil))
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 10),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 3,
		ExpiresEpoch:  10,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
		AnchorURL:     "https://example.invalid/proposal-b",
		AnchorHash:    testBytes(32, 11),
		Deposit:       0,
		ReturnAddress: withdrawAddrBytes,
		GovActionCbor: withdrawalCbor,
		AddedSlot:     101,
	}, nil))
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    4,
		NewEpoch:     5,
		BoundarySlot: 500,
		PParams:      conwayPParamsFixture(10),
		UpdateFn: func(
			pparams lcommon.ProtocolParameters,
			_ any,
		) (lcommon.ProtocolParameters, error) {
			return pparams, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())
	assert.Equal(t, 1, out.EnactedCount)
	constitution, err := db.GetGovernanceProposal(testBytes(32, 8), 0, nil)
	require.NoError(t, err)
	assert.NotNil(t, constitution.EnactedEpoch)
	withdrawal, err := db.GetGovernanceProposal(testBytes(32, 10), 0, nil)
	require.NoError(t, err)
	assert.Nil(t, withdrawal.EnactedEpoch)
	assert.Nil(t, withdrawal.RatifiedEpoch)
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(150), uint64(state.Treasury))
	account, err := store.GetAccountByCredential(
		0, withdrawStakeCred, false, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Zero(t, uint64(account.Reward))
}

func TestRefundProposalDepositReturnsInactiveRewardAccountToTreasury(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 5)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	rewardAddrBytes, err := rewardAddr.Bytes()
	require.NoError(t, err)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(5),
		Active:     true,
	}))
	_, err = store.raw.Exec(
		"UPDATE account SET active = FALSE WHERE staking_key = ?",
		stakeCred,
	)
	require.NoError(t, err)
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	err = refundProposalDeposit(db, nil, &models.GovernanceProposal{
		Deposit:       7,
		ReturnAddress: rewardAddrBytes,
	}, 123)
	require.NoError(t, err)

	active, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	assert.Nil(t, active, "refund must not reactivate the reward account")
	account, err := store.GetAccountByCredential(0, stakeCred, true, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.False(t, account.Active)
	assert.Equal(t, uint64(5), uint64(account.Reward))
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(107), uint64(state.Treasury))
	assert.Equal(t, uint64(20), uint64(state.Reserves))
}
func TestRewardCreditsRollbackBySlot(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 1)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	rewardAddrBytes, err := rewardAddr.Bytes()
	require.NoError(t, err)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(5),
		Active:     true,
	}))

	err = refundProposalDeposit(db, nil, &models.GovernanceProposal{
		Deposit:       7,
		ReturnAddress: rewardAddrBytes,
	}, 123)
	require.NoError(t, err)

	require.NoError(t, db.DeleteAccountRewardsAfterSlot(122, nil))
	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(5), uint64(account.Reward))
}

func TestCountActiveDRepsFiltersExpiredDReps(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	for _, drep := range []models.Drep{
		{
			Credential:  testBytes(28, 1),
			ExpiryEpoch: 0,
			Active:      true,
		},
		{
			Credential:  testBytes(28, 2),
			ExpiryEpoch: 10,
			Active:      true,
		},
		{
			Credential:  testBytes(28, 3),
			ExpiryEpoch: 11,
			Active:      true,
		},
	} {
		require.NoError(t, store.CreateDrep(nil, &drep))
	}

	count, err := countActiveDReps(db, nil, 10)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestCommitteeNoConfidenceStateUsesEnactedCommitteeRoot(t *testing.T) {
	t.Parallel()

	assert.False(t, committeeNoConfidenceState(nil))
	assert.False(t, committeeNoConfidenceState(&models.GovernanceProposal{
		ActionType: uint8(lcommon.GovActionTypeUpdateCommittee),
	}))
	assert.True(t, committeeNoConfidenceState(&models.GovernanceProposal{
		ActionType: uint8(lcommon.GovActionTypeNoConfidence),
	}))
}

func TestCommitteeAbsentUsesEnactedStateAndGenesis(t *testing.T) {
	t.Parallel()

	genesis := &conway.ConwayGenesis{
		Committee: conway.ConwayGenesisCommittee{
			Members: map[string]int{"keyHash-committee-member": 500},
		},
	}
	emptyGenesis := &conway.ConwayGenesis{
		Committee: conway.ConwayGenesisCommittee{
			Members: map[string]int{},
		},
	}
	require.True(t, committeeAbsent(nil, nil, false))
	require.False(t, committeeAbsent(nil, genesis, false))
	require.False(t, committeeAbsent(nil, emptyGenesis, false))
	require.False(t, committeeAbsent(nil, nil, true))
	require.False(t, committeeAbsent(&models.GovernanceProposal{
		ActionType: uint8(lcommon.GovActionTypeUpdateCommittee),
	}, nil, false))
	require.True(t, committeeAbsent(&models.GovernanceProposal{
		ActionType: uint8(lcommon.GovActionTypeNoConfidence),
	}, genesis, true))
}

func TestProcessEpochCommitteeTermLimit(t *testing.T) {
	t.Parallel()

	const currentEpoch = uint64(10)
	uintPtr := func(value uint64) *uint64 { return &value }
	tests := []struct {
		name          string
		termLimit     uint64
		memberExpiry  *uint64
		actionType    lcommon.GovActionType
		wantRatified  bool
		zeroQuorum    bool
		wantEnactment bool
	}{
		{
			name:         "within limit",
			termLimit:    5,
			memberExpiry: uintPtr(14),
			actionType:   lcommon.GovActionTypeUpdateCommittee,
			wantRatified: true,
		},
		{
			name:          "zero quorum ratifies and enacts",
			termLimit:     5,
			memberExpiry:  uintPtr(14),
			actionType:    lcommon.GovActionTypeUpdateCommittee,
			wantRatified:  true,
			zeroQuorum:    true,
			wantEnactment: true,
		},
		{
			name:         "exact boundary",
			termLimit:    5,
			memberExpiry: uintPtr(15),
			actionType:   lcommon.GovActionTypeUpdateCommittee,
			wantRatified: true,
		},
		{
			name:         "over limit remains pending",
			termLimit:    5,
			memberExpiry: uintPtr(16),
			actionType:   lcommon.GovActionTypeUpdateCommittee,
			wantRatified: false,
		},
		{
			name:         "overflowing bound",
			termLimit:    math.MaxUint64,
			memberExpiry: uintPtr(20),
			actionType:   lcommon.GovActionTypeUpdateCommittee,
			wantRatified: true,
		},
		{
			name:         "empty members",
			termLimit:    0,
			actionType:   lcommon.GovActionTypeUpdateCommittee,
			wantRatified: true,
		},
		{
			name:         "non committee action",
			termLimit:    0,
			actionType:   lcommon.GovActionTypeNoConfidence,
			wantRatified: true,
		},
	}

	for testIndex, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, _ := newTallyTestDB(t)
			var action lcommon.GovAction
			switch test.actionType {
			case lcommon.GovActionTypeUpdateCommittee:
				members := make(map[*lcommon.Credential]uint64)
				if test.memberExpiry != nil {
					credential := &lcommon.Credential{
						CredType: lcommon.CredentialTypeAddrKeyHash,
					}
					copy(
						credential.Credential[:],
						testBytes(28, byte(testIndex+1)),
					)
					members[credential] = *test.memberExpiry
				}
				quorum := newRat(2, 3)
				if test.zeroQuorum {
					quorum = newRat(0, 1)
				}
				action = &lcommon.UpdateCommitteeGovAction{
					Type:       uint(test.actionType),
					CredEpochs: members,
					Quorum:     quorum,
				}
			case lcommon.GovActionTypeNoConfidence:
				action = &lcommon.NoConfidenceGovAction{
					Type: uint(test.actionType),
				}
			default:
				t.Fatalf("unsupported action type %d", test.actionType)
			}
			actionCBOR, err := cbor.Encode(action)
			require.NoError(t, err)

			txHash := testBytes(32, byte(0x80+testIndex))
			require.NoError(t, db.SetGovernanceProposal(
				&models.GovernanceProposal{
					TxHash:        txHash,
					ActionIndex:   0,
					ActionType:    uint8(test.actionType),
					ProposedEpoch: currentEpoch - 1,
					ExpiresEpoch:  currentEpoch + 10,
					AnchorURL:     "https://example.invalid/committee-term",
					AnchorHash:    testBytes(32, byte(0x90+testIndex)),
					ReturnAddress: testBytes(29, byte(0xa0+testIndex)),
					GovActionCbor: actionCBOR,
					AddedSlot:     100,
				},
				nil,
			))

			pparams := conwayPParamsFixture(10)
			pparams.CommitteeTermLimit = test.termLimit
			pparams.DRepVotingThresholds.CommitteeNormal = newRat(0, 1)
			pparams.PoolVotingThresholds.CommitteeNormal = newRat(0, 1)
			pparams.DRepVotingThresholds.MotionNoConfidence = newRat(0, 1)
			pparams.PoolVotingThresholds.MotionNoConfidence = newRat(0, 1)

			txn := db.MetadataTxn(true)
			defer txn.Release()
			out, err := ProcessEpoch(&EpochInput{
				DB:           db,
				Txn:          txn,
				PrevEpoch:    currentEpoch - 1,
				NewEpoch:     currentEpoch,
				BoundarySlot: 500,
				PParams:      pparams,
				UpdateFn: func(
					pparams lcommon.ProtocolParameters,
					_ any,
				) (lcommon.ProtocolParameters, error) {
					return pparams, nil
				},
			})
			require.NoError(t, err)
			require.NoError(t, txn.Commit())

			if test.wantRatified {
				assert.Equal(t, 1, out.RatifiedCount)
			} else {
				assert.Equal(t, 0, out.RatifiedCount)
			}
			proposal, err := db.GetGovernanceProposal(txHash, 0, nil)
			require.NoError(t, err)
			if test.wantRatified {
				require.NotNil(t, proposal.RatifiedEpoch)
				assert.Equal(t, currentEpoch, *proposal.RatifiedEpoch)
				if test.wantEnactment {
					nextTxn := db.MetadataTxn(true)
					nextOut, nextErr := ProcessEpoch(&EpochInput{
						DB:           db,
						Txn:          nextTxn,
						PrevEpoch:    currentEpoch,
						NewEpoch:     currentEpoch + 1,
						BoundarySlot: 600,
						PParams:      out.UpdatedPParams,
						UpdateFn: func(
							pparams lcommon.ProtocolParameters,
							_ any,
						) (lcommon.ProtocolParameters, error) {
							return pparams, nil
						},
					})
					require.NoError(t, nextErr)
					require.NoError(t, nextTxn.Commit())
					nextTxn.Release()
					require.Equal(t, 1, nextOut.EnactedCount)
					proposal, err = db.GetGovernanceProposal(txHash, 0, nil)
					require.NoError(t, err)
					require.NotNil(t, proposal.EnactedEpoch)
					assert.Equal(t, currentEpoch+1, *proposal.EnactedEpoch)
				}
			} else {
				assert.Nil(t, proposal.RatifiedEpoch)
				assert.Nil(t, proposal.RatifiedSlot)
				assert.Nil(t, proposal.EnactedEpoch)
				assert.Nil(t, proposal.ExpiredEpoch)
				assert.Nil(t, proposal.DeletedSlot)
				active, err := db.GetActiveGovernanceProposals(currentEpoch, nil)
				require.NoError(t, err)
				require.Len(t, active, 1)
				assert.Equal(t, txHash, active[0].TxHash)
			}
		})
	}
}

// buildInfoProposal is a test helper that creates a GovernanceProposal with
// Info action CBOR set, ready for insertion via SetGovernanceProposal.
func buildInfoProposal(
	t *testing.T,
	txHash []byte,
	actionIndex uint32,
	expiresEpoch uint64,
	deposit uint64,
	returnAddress []byte,
	addedSlot uint64,
	parentTxHash []byte,
	parentActionIdx *uint32,
	ratifiedEpoch *uint64,
	ratifiedSlot *uint64,
) *models.GovernanceProposal {
	t.Helper()
	infoCbor, err := cbor.Encode(&lcommon.InfoGovAction{
		Type: uint(lcommon.GovActionTypeInfo),
	})
	require.NoError(t, err)
	return &models.GovernanceProposal{
		TxHash:          txHash,
		ActionIndex:     actionIndex,
		ActionType:      uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch:   3,
		ExpiresEpoch:    expiresEpoch,
		AnchorURL:       "https://example.invalid/proposal",
		AnchorHash:      testBytes(32, txHash[0]),
		Deposit:         deposit,
		ReturnAddress:   returnAddress,
		GovActionCbor:   infoCbor,
		AddedSlot:       addedSlot,
		ParentTxHash:    parentTxHash,
		ParentActionIdx: parentActionIdx,
		RatifiedEpoch:   ratifiedEpoch,
		RatifiedSlot:    ratifiedSlot,
	}
}

func buildNoConfidenceProposal(
	t *testing.T,
	txHash []byte,
	actionIndex uint32,
	expiresEpoch uint64,
	deposit uint64,
	returnAddress []byte,
	addedSlot uint64,
	parentTxHash []byte,
	parentActionIdx *uint32,
	ratifiedEpoch *uint64,
	ratifiedSlot *uint64,
) *models.GovernanceProposal {
	t.Helper()
	proposal := buildInfoProposal(
		t,
		txHash,
		actionIndex,
		expiresEpoch,
		deposit,
		returnAddress,
		addedSlot,
		parentTxHash,
		parentActionIdx,
		ratifiedEpoch,
		ratifiedSlot,
	)
	encoded, err := cbor.Encode(&lcommon.NoConfidenceGovAction{
		Type: uint(lcommon.GovActionTypeNoConfidence),
	})
	require.NoError(t, err)
	proposal.ActionType = uint8(lcommon.GovActionTypeNoConfidence)
	proposal.GovActionCbor = encoded
	return proposal
}

// buildRewardAddr returns a reward address byte slice for the given stake
// credential for use in proposal return-address fields.
func buildRewardAddr(t *testing.T, stakeCred []byte) []byte {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	b, err := addr.Bytes()
	require.NoError(t, err)
	return b
}

// TestProcessEpochEnactedChildPreserved verifies that enactment advances the
// purpose root without removing descendants that can validly follow it.
func TestProcessEpochEnactedChildPreserved(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)

	stakeCred := testBytes(28, 50)
	returnAddr := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))

	ratifiedEpoch := uint64(4)
	ratifiedSlot := uint64(400)
	parentHash := testBytes(32, 51)
	childHash := testBytes(32, 52)
	parentIdx := uint32(0)

	require.NoError(t, db.SetGovernanceProposal(
		buildNoConfidenceProposal(t, parentHash, 0, 10, 30, returnAddr, 100,
			nil, nil, &ratifiedEpoch, &ratifiedSlot),
		nil,
	))
	require.NoError(t, db.SetGovernanceProposal(
		buildNoConfidenceProposal(t, childHash, 0, 12, 15, returnAddr, 101,
			parentHash, &parentIdx, nil, nil),
		nil,
	))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    4,
		NewEpoch:     5,
		BoundarySlot: 500,
		PParams:      conwayPParamsFixture(10),
		UpdateFn: func(p lcommon.ProtocolParameters, _ any) (lcommon.ProtocolParameters, error) {
			return p, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	assert.Equal(t, 1, out.EnactedCount)
	assert.Equal(t, 0, out.OrphanedCount)

	child, err := db.GetGovernanceProposal(childHash, 0, nil)
	require.NoError(t, err)
	require.Nil(t, child.ExpiredEpoch)
	require.Nil(t, child.ExpiredSlot)

	// Only the enacted parent's deposit is returned; the child remains active.
	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(30), uint64(account.Reward))
}

// TestProcessEpochOrphanedSiblingMissingReturnAccountGoesToTreasury checks
// that when an orphaned proposal's return reward account is not registered,
// its deposit is routed to the treasury rather than credited to the account.
func TestProcessEpochOrphanedSiblingMissingReturnAccountGoesToTreasury(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	parentStakeCred := testBytes(28, 53)
	parentReturnAddr := buildRewardAddr(t, parentStakeCred)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: parentStakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))

	missingStakeCred := testBytes(28, 54)
	missingReturnAddr := buildRewardAddr(t, missingStakeCred)

	ratifiedEpoch := uint64(4)
	ratifiedSlot := uint64(400)
	parentHash := testBytes(32, 55)
	childHash := testBytes(32, 56)

	require.NoError(t, db.SetGovernanceProposal(
		buildNoConfidenceProposal(
			t,
			parentHash,
			0,
			10,
			30,
			parentReturnAddr,
			100,
			nil,
			nil,
			&ratifiedEpoch,
			&ratifiedSlot,
		),
		nil,
	))
	require.NoError(t, db.SetGovernanceProposal(
		buildNoConfidenceProposal(
			t,
			childHash,
			0,
			12,
			25,
			missingReturnAddr,
			101,
			nil,
			nil,
			nil,
			nil,
		),
		nil,
	))

	runEpoch := func(newEpoch, boundarySlot uint64) *EpochOutput {
		t.Helper()
		txn := db.MetadataTxn(true)
		defer txn.Release()
		out, err := ProcessEpoch(&EpochInput{
			DB:           db,
			Txn:          txn,
			PrevEpoch:    newEpoch - 1,
			NewEpoch:     newEpoch,
			BoundarySlot: boundarySlot,
			PParams:      conwayPParamsFixture(10),
			UpdateFn: func(p lcommon.ProtocolParameters, _ any) (lcommon.ProtocolParameters, error) {
				return p, nil
			},
		})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
		return out
	}

	out := runEpoch(5, 500)

	assert.Equal(t, 1, out.OrphanedCount)

	// The sibling lost to an enactment, which cardano-ledger settles in the
	// enacting tick, so its deposit is returned now rather than at the next
	// tick's drop step.
	child, err := db.GetGovernanceProposal(childHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, child.ExpiredEpoch)
	require.NotNil(t, child.DroppedEpoch)
	assert.Equal(t, uint64(5), *child.DroppedEpoch)

	missing, err := store.GetAccountByCredential(
		0,
		missingStakeCred,
		false,
		nil,
	)
	require.NoError(t, err)
	assert.Nil(t, missing, "orphan refund must not create a reward account")

	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(125), uint64(state.Treasury))

	// The drop step must not return the same deposit a second time.
	out = runEpoch(6, 600)
	assert.Equal(t, 0, out.DroppedCount)
	state, err = store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(125), uint64(state.Treasury))
}

// TestProcessEpochTransitiveOrphanRemoval verifies that orphan sweeps
// cascade: when a competing root-level sibling of an enacted proposal is
// orphaned, its own children are also swept and refunded in the same tick.
func TestProcessEpochTransitiveOrphanRemoval(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)

	stakeCred := testBytes(28, 57)
	returnAddr := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))

	ratifiedEpoch := uint64(4)
	ratifiedSlot := uint64(400)
	parentHash := testBytes(32, 58)
	childHash := testBytes(32, 59)
	grandchildHash := testBytes(32, 60)
	parentIdx := uint32(0)

	require.NoError(t, db.SetGovernanceProposal(
		buildNoConfidenceProposal(t, parentHash, 0, 10, 10, returnAddr, 100,
			nil, nil, &ratifiedEpoch, &ratifiedSlot),
		nil,
	))
	require.NoError(t, db.SetGovernanceProposal(
		buildNoConfidenceProposal(t, childHash, 0, 12, 20, returnAddr, 101,
			nil, nil, nil, nil),
		nil,
	))
	require.NoError(t, db.SetGovernanceProposal(
		buildNoConfidenceProposal(t, grandchildHash, 0, 14, 30, returnAddr, 102,
			childHash, &parentIdx, nil, nil),
		nil,
	))

	runEpoch := func(newEpoch, boundarySlot uint64) *EpochOutput {
		t.Helper()
		txn := db.MetadataTxn(true)
		defer txn.Release()
		out, err := ProcessEpoch(&EpochInput{
			DB:           db,
			Txn:          txn,
			PrevEpoch:    newEpoch - 1,
			NewEpoch:     newEpoch,
			BoundarySlot: boundarySlot,
			PParams:      conwayPParamsFixture(10),
			UpdateFn: func(p lcommon.ProtocolParameters, _ any) (lcommon.ProtocolParameters, error) {
				return p, nil
			},
		})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
		return out
	}

	out := runEpoch(5, 500)

	assert.Equal(t, 1, out.EnactedCount)
	assert.Equal(t, 2, out.OrphanedCount)

	for _, hash := range [][]byte{childHash, grandchildHash} {
		p, err := db.GetGovernanceProposal(hash, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, p.ExpiredEpoch,
			"proposal %x should be orphaned", hash)
		assert.Equal(t, uint64(5), *p.ExpiredEpoch)
		require.NotNil(t, p.DroppedEpoch,
			"proposal %x lost to an enactment, so it drops in this tick", hash)
		assert.Equal(t, uint64(5), *p.DroppedEpoch)
	}

	// cardano-ledger unions the enacted action with the siblings its
	// enactment removed before returning deposits, so all three land in the
	// enacting epoch: 10 + 20 + 30.
	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(60), uint64(account.Reward))

	// The drop step must not return the same deposits a second time.
	out = runEpoch(6, 600)
	assert.Equal(t, 0, out.DroppedCount)
	account, err = store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(60), uint64(account.Reward))
}

// TestProcessEpochOrphanExcludedFromActiveProposals verifies that after
// orphan removal, GetActiveGovernanceProposals no longer returns orphaned
// proposals (their expired_epoch field filters them out).
func TestProcessEpochOrphanExcludedFromActiveProposals(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)

	stakeCred := testBytes(28, 61)
	returnAddr := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))

	ratifiedEpoch := uint64(4)
	ratifiedSlot := uint64(400)
	parentHash := testBytes(32, 62)
	childHash := testBytes(32, 63)

	require.NoError(t, db.SetGovernanceProposal(
		buildNoConfidenceProposal(t, parentHash, 0, 10, 5, returnAddr, 100,
			nil, nil, &ratifiedEpoch, &ratifiedSlot),
		nil,
	))
	require.NoError(t, db.SetGovernanceProposal(
		buildNoConfidenceProposal(t, childHash, 0, 12, 5, returnAddr, 101,
			nil, nil, nil, nil),
		nil,
	))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	_, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    4,
		NewEpoch:     5,
		BoundarySlot: 500,
		PParams:      conwayPParamsFixture(10),
		UpdateFn: func(p lcommon.ProtocolParameters, _ any) (lcommon.ProtocolParameters, error) {
			return p, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	active, err := db.GetActiveGovernanceProposals(5, nil)
	require.NoError(t, err)
	for _, p := range active {
		assert.NotEqual(t, childHash, p.TxHash,
			"orphaned proposal must not appear in active pool")
	}
}

// TestProcessEpochOrphanedSiblingRestoredOnRollback verifies that rolling
// back to a slot before the boundary slot restores orphaned proposals
// (clears their expired_epoch/expired_slot) and reverses the reward credit.
func TestProcessEpochOrphanedSiblingRestoredOnRollback(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)

	stakeCred := testBytes(28, 64)
	returnAddr := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))

	ratifiedEpoch := uint64(4)
	ratifiedSlot := uint64(400)
	parentHash := testBytes(32, 65)
	childHash := testBytes(32, 66)

	require.NoError(t, db.SetGovernanceProposal(
		buildNoConfidenceProposal(t, parentHash, 0, 10, 10, returnAddr, 100,
			nil, nil, &ratifiedEpoch, &ratifiedSlot),
		nil,
	))
	require.NoError(t, db.SetGovernanceProposal(
		buildNoConfidenceProposal(t, childHash, 0, 12, 20, returnAddr, 101,
			nil, nil, nil, nil),
		nil,
	))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	_, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    4,
		NewEpoch:     5,
		BoundarySlot: 500,
		PParams:      conwayPParamsFixture(10),
		UpdateFn: func(p lcommon.ProtocolParameters, _ any) (lcommon.ProtocolParameters, error) {
			return p, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	child, err := db.GetGovernanceProposal(childHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(
		t,
		child.ExpiredEpoch,
		"child must be orphaned before rollback",
	)

	require.NoError(t, db.DeleteGovernanceProposalsAfterSlot(499, nil))
	require.NoError(t, db.DeleteAccountRewardsAfterSlot(499, nil))

	child, err = db.GetGovernanceProposal(childHash, 0, nil)
	require.NoError(t, err)
	assert.Nil(
		t,
		child.ExpiredEpoch,
		"orphaned status must be reversed by rollback",
	)
	assert.Nil(t, child.ExpiredSlot)

	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(0), uint64(account.Reward),
		"reward credit must be reversed by rollback")
}

// TestProcessEpochOrphanAfterExpiry verifies that a proposal whose parent
// expires naturally at this epoch boundary is also orphaned and refunded.
func TestProcessEpochOrphanAfterExpiry(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)

	stakeCred := testBytes(28, 67)
	returnAddr := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))

	parentHash := testBytes(32, 68)
	childHash := testBytes(32, 69)
	parentIdx := uint32(0)

	// Parent expires at epoch 4 (ExpiresEpoch < NewEpoch=5).
	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, parentHash, 0, 4, 10, returnAddr, 100,
			nil, nil, nil, nil),
		nil,
	))
	// Child expires at epoch 15 but references parent.
	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, childHash, 0, 15, 20, returnAddr, 101,
			parentHash, &parentIdx, nil, nil),
		nil,
	))

	runEpoch := func(newEpoch, boundarySlot uint64) *EpochOutput {
		t.Helper()
		txn := db.MetadataTxn(true)
		defer txn.Release()
		out, err := ProcessEpoch(&EpochInput{
			DB:           db,
			Txn:          txn,
			PrevEpoch:    newEpoch - 1,
			NewEpoch:     newEpoch,
			BoundarySlot: boundarySlot,
			PParams:      conwayPParamsFixture(10),
			UpdateFn: func(p lcommon.ProtocolParameters, _ any) (lcommon.ProtocolParameters, error) {
				return p, nil
			},
		})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
		return out
	}

	// Epoch 5: parent expires and orphans the child; neither deposit is
	// refunded yet.
	out := runEpoch(5, 500)
	assert.Equal(t, 1, out.ExpiredCount)
	assert.Equal(t, 1, out.OrphanedCount)

	child, err := db.GetGovernanceProposal(childHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, child.ExpiredEpoch)
	assert.Equal(t, uint64(5), *child.ExpiredEpoch)
	assert.Nil(t, child.DroppedEpoch)

	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(
		t,
		uint64(0),
		uint64(account.Reward),
		"neither deposit may be refunded in the epoch the proposals expire",
	)

	// Epoch 6: both deposits (parent + orphaned child) are dropped and
	// refunded together.
	out = runEpoch(6, 600)
	assert.Equal(t, 2, out.DroppedCount)
	account, err = store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(30), uint64(account.Reward))
}
