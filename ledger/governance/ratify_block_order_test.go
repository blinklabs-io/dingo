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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func chainTestWithdrawal(t *testing.T, seed byte, amount uint64) []byte {
	t.Helper()
	address, err := lcommon.NewAddressFromBytes(
		buildRewardAddr(t, testBytes(28, seed)),
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(&lcommon.TreasuryWithdrawalGovAction{
		Type:        uint(lcommon.GovActionTypeTreasuryWithdrawal),
		Withdrawals: map[*lcommon.Address]uint64{&address: amount},
	})
	require.NoError(t, err)
	return encoded
}

// sameBlockPair stores two same-slot proposals whose block order is the
// reverse of their transaction-hash order: the earlier transaction has the
// higher hash. It returns them in block order.
func sameBlockPair(
	t *testing.T,
	actionType lcommon.GovActionType,
	earlierCbor, laterCbor []byte,
) (*models.GovernanceProposal, *models.GovernanceProposal) {
	t.Helper()
	earlier := chainTestProposal(
		actionType, testBytes(32, 0xB2), nil, 400, 0, testBytes(29, 0),
		earlierCbor,
	)
	later := chainTestProposal(
		actionType, testBytes(32, 0xB1), nil, 400, 0, testBytes(29, 0),
		laterCbor,
	)
	earlierIndex, laterIndex := uint32(0), uint32(1)
	earlier.TxIndex = &earlierIndex
	later.TxIndex = &laterIndex
	return earlier, later
}

// TestProcessEpochOrdersSameBlockWithdrawalsByTransactionPosition is the
// dingo#4465 case: two TreasuryWithdrawals in one block each fit the running
// treasury budget alone but not together, and the one appearing later in the
// block has the lower transaction hash. Conway RATIFY evaluates them in
// submission order, so the earlier transaction's withdrawal is accepted and
// the later one no longer fits.
func TestProcessEpochOrdersSameBlockWithdrawalsByTransactionPosition(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
	earlier, later := sameBlockPair(
		t,
		lcommon.GovActionTypeTreasuryWithdrawal,
		chainTestWithdrawal(t, 0xB3, 6),
		chainTestWithdrawal(t, 0xB4, 6),
	)
	// Store the later transaction first as well, so neither row ID nor hash
	// order agrees with the block order.
	stored := chainTestStore(t, db, later, earlier)
	seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)

	out := chainTestRunEpoch(t, db, stabilityTestEpoch, chainTestPParams())
	assert.Equal(t, 1, out.RatifiedCount)
	assert.NotNil(t, chainTestReload(t, db, earlier).RatifiedEpoch)
	assert.Nil(t, chainTestReload(t, db, later).RatifiedEpoch)
}

// TestProcessEpochOrdersEveryEqualPriorityClassByTransactionPosition covers
// each action class whose same-slot tie changes the outcome: competing
// ParameterChanges with one parent, and two of each delaying action, where
// the first accepted ends the pass. The earlier transaction wins although it
// has the higher hash.
func TestProcessEpochOrdersEveryEqualPriorityClassByTransactionPosition(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		actionType lcommon.GovActionType
		encode     func(t *testing.T, marker int64) []byte
	}{
		{
			name:       "parameter change siblings",
			actionType: lcommon.GovActionTypeParameterChange,
			encode:     chainTestParameterChange,
		},
		{
			name:       "no confidence",
			actionType: lcommon.GovActionTypeNoConfidence,
			encode: func(t *testing.T, _ int64) []byte {
				return chainTestDelayingAction(
					t, lcommon.GovActionTypeNoConfidence,
				)
			},
		},
		{
			name:       "update committee",
			actionType: lcommon.GovActionTypeUpdateCommittee,
			encode: func(t *testing.T, _ int64) []byte {
				return chainTestDelayingAction(
					t, lcommon.GovActionTypeUpdateCommittee,
				)
			},
		},
		{
			name:       "new constitution",
			actionType: lcommon.GovActionTypeNewConstitution,
			encode: func(t *testing.T, _ int64) []byte {
				return chainTestDelayingAction(
					t, lcommon.GovActionTypeNewConstitution,
				)
			},
		},
		{
			name:       "hard fork initiation",
			actionType: lcommon.GovActionTypeHardForkInitiation,
			encode: func(t *testing.T, _ int64) []byte {
				return chainTestDelayingAction(
					t, lcommon.GovActionTypeHardForkInitiation,
				)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db, store := newTallyTestDB(t)
			require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
			earlier, later := sameBlockPair(
				t, test.actionType, test.encode(t, 61), test.encode(t, 62),
			)
			stored := chainTestStore(t, db, later, earlier)
			seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)

			out := chainTestRunEpoch(
				t, db, stabilityTestEpoch, chainTestPParams(),
			)
			assert.Equal(t, 1, out.RatifiedCount)
			assert.NotNil(t, chainTestReload(t, db, earlier).RatifiedEpoch)
			assert.Nil(t, chainTestReload(t, db, later).RatifiedEpoch)
		})
	}
}

// TestProcessEpochKeepsLegacyHashOrderWithoutTransactionPosition pins the
// upgrade contract: rows stored before the transaction position was recorded
// (TxIndex nil) keep the transaction-hash order they were evaluated in.
func TestProcessEpochKeepsLegacyHashOrderWithoutTransactionPosition(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
	higher, lower := sameBlockPair(
		t,
		lcommon.GovActionTypeTreasuryWithdrawal,
		chainTestWithdrawal(t, 0xB3, 6),
		chainTestWithdrawal(t, 0xB4, 6),
	)
	higher.TxIndex = nil
	lower.TxIndex = nil
	stored := chainTestStore(t, db, higher, lower)
	seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)

	out := chainTestRunEpoch(t, db, stabilityTestEpoch, chainTestPParams())
	assert.Equal(t, 1, out.RatifiedCount)
	assert.NotNil(t, chainTestReload(t, db, lower).RatifiedEpoch)
	assert.Nil(t, chainTestReload(t, db, higher).RatifiedEpoch)
}

// TestProcessProposalsRecordsTransactionPosition checks the ingestion side:
// every proposal procedure of a transaction records that transaction's block
// position.
func TestProcessProposalsRecordsTransactionPosition(t *testing.T) {
	t.Parallel()

	db, _ := newTallyTestDB(t)
	txHash := testBytes(32, 0xB5)
	address, err := lcommon.NewAddressFromBytes(
		buildRewardAddr(t, testBytes(28, 0xB6)),
	)
	require.NoError(t, err)
	anchor := lcommon.GovAnchor{Url: "https://example.invalid/position"}
	procedure := func() lcommon.ProposalProcedure {
		procedure, err := conway.NewConwayProposalProcedure(
			1_000,
			address,
			&lcommon.InfoGovAction{Type: uint(lcommon.GovActionTypeInfo)},
			anchor,
		)
		require.NoError(t, err)
		return procedure
	}
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(txHash)
	tx.WithProposalProcedures(procedure(), procedure())

	require.NoError(t, ProcessProposals(
		tx, ocommon.Point{Slot: 400}, 7, stabilityTestEpoch, 6, db, nil,
	))
	for actionIndex := range uint32(2) {
		stored, err := db.GetGovernanceProposal(txHash, actionIndex, nil)
		require.NoError(t, err)
		require.NotNil(t, stored.TxIndex)
		assert.Equal(t, uint32(7), *stored.TxIndex)
	}
}
