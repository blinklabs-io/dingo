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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedProposalReturnAccount creates a registered account and returns the
// reward-account address bytes for it, for use as a GovernanceProposal's
// ReturnAddress.
func seedProposalReturnAccount(
	t *testing.T,
	store *tallyTestStore,
	stakeCred []byte,
	account *models.Account,
) []byte {
	t.Helper()
	account.StakingKey = stakeCred
	require.NoError(t, store.CreateAccount(nil, account))
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	addrBytes, err := addr.Bytes()
	require.NoError(t, err)
	return addrBytes
}

func TestLoadDRepVotingStateIncludesActiveProposalDeposit(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	drepCred := testBytes(28, 1)
	drepStakeCred := testBytes(28, 2)
	returnStakeCred := testBytes(28, 3)

	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
	}))
	seedDRepStake(
		t, store, drepStakeCred, drepCred, models.DrepTypeAddrKeyHash, 100, 1,
	)

	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			Drep:      drepCred,
			DrepType:  models.DrepTypeAddrKeyHash,
			AddedSlot: 1,
			Active:    true,
		},
	)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 9),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 5,
		ExpiresEpoch:  10,
		Deposit:       50,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    testBytes(32, 0xAA),
		AddedSlot:     1,
	}, nil))

	state, err := LoadDRepVotingState(db, nil, 6, false)
	require.NoError(t, err)
	ref := models.StakeCredentialRef{Tag: 0, Key: drepCred}
	assert.Equal(t, uint64(150), state.Powers[ref.MapKey()])
}

func TestLoadDRepVotingStateAddsProposalDepositToAlwaysNoConfidence(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	returnStakeCred := testBytes(28, 1)

	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			DrepType:  models.DrepTypeAlwaysNoConfidence,
			AddedSlot: 1,
			Active:    true,
		},
	)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 10),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 5,
		ExpiresEpoch:  10,
		Deposit:       75,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    testBytes(32, 0xAB),
		AddedSlot:     1,
	}, nil))

	state, err := LoadDRepVotingState(db, nil, 6, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(75), state.NoConfidencePower)
}

func TestLoadDRepVotingStateExcludesAlwaysAbstainProposalDeposit(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	returnStakeCred := testBytes(28, 1)

	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			DrepType:  models.DrepTypeAlwaysAbstain,
			AddedSlot: 1,
			Active:    true,
		},
	)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 11),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 5,
		ExpiresEpoch:  10,
		Deposit:       75,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    testBytes(32, 0xAC),
		AddedSlot:     1,
	}, nil))

	state, err := LoadDRepVotingState(db, nil, 6, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), state.AbstainPower)
	assert.Equal(t, uint64(0), state.NoConfidencePower)
}

func TestLoadDRepVotingStateExcludesExpiredProposalDeposit(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	drepCred := testBytes(28, 1)
	returnStakeCred := testBytes(28, 2)

	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
	}))

	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			Drep:      drepCred,
			DrepType:  models.DrepTypeAddrKeyHash,
			AddedSlot: 1,
			Active:    true,
		},
	)
	// Expired as of epoch 6: expires_epoch (5) < currentEpoch (6), so
	// GetActiveGovernanceProposals must not return it.
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 12),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 1,
		ExpiresEpoch:  5,
		Deposit:       50,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    testBytes(32, 0xAD),
		AddedSlot:     1,
	}, nil))

	state, err := LoadDRepVotingState(db, nil, 6, false)
	require.NoError(t, err)
	ref := models.StakeCredentialRef{Tag: 0, Key: drepCred}
	assert.Equal(t, uint64(0), state.Powers[ref.MapKey()])
}

func TestLoadDRepVotingStateExcludesDeregisteredReturnAccountDeposit(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	drepCred := testBytes(28, 1)
	drepStakeCred := testBytes(28, 2)
	returnStakeCred := testBytes(28, 3)

	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
	}))
	seedDRepStake(
		t, store, drepStakeCred, drepCred, models.DrepTypeAddrKeyHash, 100, 1,
	)

	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			Drep:      drepCred,
			DrepType:  models.DrepTypeAddrKeyHash,
			AddedSlot: 1,
			Active:    false,
		},
	)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 13),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 5,
		ExpiresEpoch:  10,
		Deposit:       50,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    testBytes(32, 0xAE),
		AddedSlot:     1,
	}, nil))

	state, err := LoadDRepVotingState(db, nil, 6, false)
	require.NoError(t, err)
	ref := models.StakeCredentialRef{Tag: 0, Key: drepCred}
	assert.Equal(t, uint64(100), state.Powers[ref.MapKey()])
}

func TestLoadDRepVotingStateHonorsDelegatorInactivityGateOnDeposit(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	drepCred := testBytes(28, 1)
	drepStakeCred := testBytes(28, 2)
	returnStakeCred := testBytes(28, 3)

	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
	}))
	seedDRepStake(
		t, store, drepStakeCred, drepCred, models.DrepTypeAddrKeyHash, 100, 1,
	)

	// The return account's own CIP-0163 activity stamp is stale relative to
	// currentEpoch (6): ExpirationEpoch (1) < expiryEpoch (6).
	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			Drep:            drepCred,
			DrepType:        models.DrepTypeAddrKeyHash,
			AddedSlot:       1,
			Active:          true,
			ExpirationEpoch: 1,
		},
	)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 14),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 5,
		ExpiresEpoch:  10,
		Deposit:       50,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    testBytes(32, 0xAF),
		AddedSlot:     1,
	}, nil))

	ref := models.StakeCredentialRef{Tag: 0, Key: drepCred}

	// Gate off: byte-identical to pre-CIP-0163 behavior, deposit still
	// counts regardless of the stale ExpirationEpoch.
	offState, err := LoadDRepVotingState(db, nil, 6, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(150), offState.Powers[ref.MapKey()])

	// Gate on: the return account is inactive by the same rule
	// VotingPowerBatchSQL applies to its ordinary stake, so its deposit is
	// excluded too.
	onState, err := LoadDRepVotingState(db, nil, 6, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), onState.Powers[ref.MapKey()])
}

// TestLoadDRepVotingStateProposalDepositSumOverflow drives the per-return-
// account deposit accumulation past the uint64 boundary using three active
// proposals that all return to the same account: two deposits of MaxInt64
// (each safely representable in the sqlite INTEGER deposit column) plus one
// of 2 sum to MaxUint64+1. This targets the deposit-summation guard itself
// (ActiveProposalDepositDRepPower's `deposits[key], err = addUint64(...)`),
// not the later drep-power merge -- a single deposit anywhere near MaxUint64
// cannot round-trip through the database, since the sqlite INTEGER type is
// signed 64-bit.
func TestLoadDRepVotingStateProposalDepositSumOverflow(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	drepCred := testBytes(28, 1)
	returnStakeCred := testBytes(28, 2)
	const maxInt64 = uint64(1<<63 - 1)

	returnAddrBytes := seedProposalReturnAccount(
		t, store, returnStakeCred, &models.Account{
			Drep:      drepCred,
			DrepType:  models.DrepTypeAddrKeyHash,
			AddedSlot: 1,
			Active:    true,
		},
	)
	for i, deposit := range []uint64{maxInt64, maxInt64, 2} {
		require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
			TxHash:        testBytes(32, byte(0xC0+i)),
			ActionIndex:   0,
			ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
			ProposedEpoch: 5,
			ExpiresEpoch:  10,
			Deposit:       deposit,
			ReturnAddress: returnAddrBytes,
			AnchorURL:     "https://example.invalid/deposit",
			AnchorHash:    testBytes(32, byte(0xD0+i)),
			AddedSlot:     1,
		}, nil))
	}

	_, err := LoadDRepVotingState(db, nil, 6, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sum active proposal deposits")
}
