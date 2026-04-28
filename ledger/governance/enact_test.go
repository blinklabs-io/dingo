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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeGovAction_InfoRoundtrip(t *testing.T) {
	original := &lcommon.InfoGovAction{Type: 6}
	encoded, err := cbor.Encode(original)
	require.NoError(t, err)
	decoded, err := decodeGovAction(
		encoded, uint8(lcommon.GovActionTypeInfo),
	)
	require.NoError(t, err)
	_, ok := decoded.(*lcommon.InfoGovAction)
	assert.True(t, ok)
}

func TestDecodeGovAction_ParameterChangeRoundtrip(t *testing.T) {
	fee := uint(1234)
	original := &conway.ConwayParameterChangeGovAction{
		Type: 0,
		ParamUpdate: conway.ConwayProtocolParameterUpdate{
			MinFeeA: &fee,
		},
	}
	encoded, err := cbor.Encode(original)
	require.NoError(t, err)
	decoded, err := decodeGovAction(
		encoded, uint8(lcommon.GovActionTypeParameterChange),
	)
	require.NoError(t, err)
	concrete, ok := decoded.(*conway.ConwayParameterChangeGovAction)
	require.True(t, ok)
	require.NotNil(t, concrete.ParamUpdate.MinFeeA)
	assert.Equal(t, uint(1234), *concrete.ParamUpdate.MinFeeA)
}

func TestDecodeGovAction_HardForkRoundtrip(t *testing.T) {
	original := &lcommon.HardForkInitiationGovAction{Type: 1}
	original.ProtocolVersion.Major = 10
	original.ProtocolVersion.Minor = 0
	encoded, err := cbor.Encode(original)
	require.NoError(t, err)
	decoded, err := decodeGovAction(
		encoded, uint8(lcommon.GovActionTypeHardForkInitiation),
	)
	require.NoError(t, err)
	concrete, ok := decoded.(*lcommon.HardForkInitiationGovAction)
	require.True(t, ok)
	assert.Equal(t, uint(10), concrete.ProtocolVersion.Major)
}

func TestDecodeGovAction_TreasuryWithdrawalRoundtrip(t *testing.T) {
	original := &lcommon.TreasuryWithdrawalGovAction{
		Type:       2,
		PolicyHash: []byte{0xAB, 0xCD, 0xEF},
	}
	encoded, err := cbor.Encode(original)
	require.NoError(t, err)
	decoded, err := decodeGovAction(
		encoded, uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	)
	require.NoError(t, err)
	concrete, ok := decoded.(*lcommon.TreasuryWithdrawalGovAction)
	require.True(t, ok)
	assert.Equal(t, []byte{0xAB, 0xCD, 0xEF}, concrete.PolicyHash)
}

func TestDecodeGovAction_NoConfidenceRoundtrip(t *testing.T) {
	original := &lcommon.NoConfidenceGovAction{Type: 3}
	encoded, err := cbor.Encode(original)
	require.NoError(t, err)
	decoded, err := decodeGovAction(
		encoded, uint8(lcommon.GovActionTypeNoConfidence),
	)
	require.NoError(t, err)
	_, ok := decoded.(*lcommon.NoConfidenceGovAction)
	assert.True(t, ok)
}

func TestDecodeGovAction_UpdateCommitteeRoundtrip(t *testing.T) {
	original := &lcommon.UpdateCommitteeGovAction{
		Type:        4,
		Credentials: []lcommon.Credential{},
		Quorum:      newRat(2, 3),
	}
	encoded, err := cbor.Encode(original)
	require.NoError(t, err)
	decoded, err := decodeGovAction(
		encoded, uint8(lcommon.GovActionTypeUpdateCommittee),
	)
	require.NoError(t, err)
	_, ok := decoded.(*lcommon.UpdateCommitteeGovAction)
	assert.True(t, ok)
}

func TestDecodeGovAction_NewConstitutionRoundtrip(t *testing.T) {
	original := &lcommon.NewConstitutionGovAction{Type: 5}
	encoded, err := cbor.Encode(original)
	require.NoError(t, err)
	decoded, err := decodeGovAction(
		encoded, uint8(lcommon.GovActionTypeNewConstitution),
	)
	require.NoError(t, err)
	_, ok := decoded.(*lcommon.NewConstitutionGovAction)
	assert.True(t, ok)
}

func TestDecodeGovAction_EmptyCbor(t *testing.T) {
	_, err := decodeGovAction(
		nil, uint8(lcommon.GovActionTypeInfo),
	)
	assert.Error(t, err)
}

func TestDecodeGovAction_UnknownType(t *testing.T) {
	_, err := decodeGovAction(
		[]byte{0x00}, 99,
	)
	assert.Error(t, err)
}

func TestSetProtocolVersion_ConwayParams(t *testing.T) {
	pparams := &conway.ConwayProtocolParameters{}
	pparams.ProtocolVersion.Major = 9
	pparams.ProtocolVersion.Minor = 0
	updated, err := setProtocolVersion(pparams, 10, 0)
	require.NoError(t, err)
	concrete, ok := updated.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.Equal(t, uint(10), concrete.ProtocolVersion.Major)
	// Original must remain unmutated to preserve the previous epoch's
	// pparams for rollback safety.
	assert.Equal(t, uint(9), pparams.ProtocolVersion.Major)
}

func TestStakeEpochFor(t *testing.T) {
	tests := []struct {
		newEpoch uint64
		expected uint64
	}{
		{0, 0},
		{1, 0},
		{2, 0},
		{3, 1},
		{10, 8},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, stakeEpochFor(tt.newEpoch))
	}
}

// Expiry is exercised end-to-end via the DB query and ProcessEpoch
// integration rather than a split-in-memory helper, so there is no unit
// test for a partition function here.

func TestApplyUpdateCommittee_PersistsEnactedQuorum(t *testing.T) {
	db, _ := newTallyTestDB(t)

	action := &lcommon.UpdateCommitteeGovAction{
		Credentials: []lcommon.Credential{},
		CredEpochs:  map[*lcommon.Credential]uint{},
		Quorum:      cbor.Rat{Rat: big.NewRat(3, 5)},
	}
	err := applyUpdateCommittee(
		&EnactmentContext{DB: db, Slot: 4242},
		action,
	)
	require.NoError(t, err)

	got, err := db.GetCommitteeQuorum(nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 0, got.Cmp(big.NewRat(3, 5)))
}

func TestEnactProposal_NoConfidence_ClearsCommitteeQuorum(
	t *testing.T,
) {
	db, _ := newTallyTestDB(t)

	// Seed an enacted quorum from a prior UpdateCommittee.
	require.NoError(
		t,
		db.SetCommitteeQuorum(big.NewRat(3, 5), 1000, nil),
	)

	// Build a NoConfidence proposal with a zero deposit so the
	// reward-account refund path short-circuits.
	action := &lcommon.NoConfidenceGovAction{
		Type: uint(lcommon.GovActionTypeNoConfidence),
	}
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)
	proposal := &models.GovernanceProposal{
		TxHash:        testBytes(32, 7),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeNoConfidence),
		GovActionCbor: encoded,
		AddedSlot:     500,
		ExpiresEpoch:  100,
		AnchorURL:     "https://example.invalid/noconf",
		AnchorHash:    testBytes(32, 8),
		ReturnAddress: testBytes(29, 9),
		// Zero deposit keeps the refund path a no-op so this test
		// isolates the committee-quorum clear behavior.
		Deposit: 0,
	}

	_, err = EnactProposal(
		&EnactmentContext{DB: db, Slot: 2000, Epoch: 42},
		proposal,
	)
	require.NoError(t, err)

	got, err := db.GetCommitteeQuorum(nil)
	require.NoError(t, err)
	assert.Nil(t, got, "NoConfidence should clear the enacted quorum")
}

func TestApplyTreasuryWithdrawal_CreditsRewardsAndDebitsTreasury(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 1)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(5),
		Active:     true,
	}).Error)
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	a := &lcommon.TreasuryWithdrawalGovAction{
		Withdrawals: map[*lcommon.Address]uint64{&rewardAddr: 7},
	}
	err = applyTreasuryWithdrawal(&EnactmentContext{
		DB:   db,
		Slot: 123,
	}, a)
	require.NoError(t, err)

	account, err := store.GetAccount(stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(12), uint64(account.Reward))
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(93), uint64(state.Treasury))
	assert.Equal(t, uint64(20), uint64(state.Reserves))
}
