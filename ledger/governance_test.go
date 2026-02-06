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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
)

func TestMapVoterType(t *testing.T) {
	tests := []struct {
		name     string
		input    uint8
		expected uint8
	}{
		{
			name:     "CC hot key hash",
			input:    lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
			expected: models.VoterTypeCC,
		},
		{
			name:     "CC hot script hash",
			input:    lcommon.VoterTypeConstitutionalCommitteeHotScriptHash,
			expected: models.VoterTypeCC,
		},
		{
			name:     "DRep key hash",
			input:    lcommon.VoterTypeDRepKeyHash,
			expected: models.VoterTypeDRep,
		},
		{
			name:     "DRep script hash",
			input:    lcommon.VoterTypeDRepScriptHash,
			expected: models.VoterTypeDRep,
		},
		{
			name:     "SPO key hash",
			input:    lcommon.VoterTypeStakingPoolKeyHash,
			expected: models.VoterTypeSPO,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := mapVoterType(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractGovActionInfo_ParameterChange(t *testing.T) {
	parentId := &lcommon.GovActionId{
		TransactionId: [32]byte{1, 2, 3},
		GovActionIdx:  5,
	}
	action := &conway.ConwayParameterChangeGovAction{
		ActionId:   parentId,
		PolicyHash: []byte{0xAB, 0xCD},
	}

	actionType, parentTxHash, parentActionIdx, policyHash, err := extractGovActionInfo(action)
	assert.NoError(t, err)

	assert.Equal(
		t,
		uint8(lcommon.GovActionTypeParameterChange),
		actionType,
	)
	assert.Equal(t, parentId.TransactionId[:], parentTxHash)
	assert.NotNil(t, parentActionIdx)
	assert.Equal(t, uint32(5), *parentActionIdx)
	assert.Equal(t, []byte{0xAB, 0xCD}, policyHash)
}

func TestExtractGovActionInfo_ParameterChangeNoParent(t *testing.T) {
	action := &conway.ConwayParameterChangeGovAction{}

	actionType, parentTxHash, parentActionIdx, policyHash, err := extractGovActionInfo(action)
	assert.NoError(t, err)

	assert.Equal(
		t,
		uint8(lcommon.GovActionTypeParameterChange),
		actionType,
	)
	assert.Nil(t, parentTxHash)
	assert.Nil(t, parentActionIdx)
	assert.Nil(t, policyHash)
}

func TestExtractGovActionInfo_HardForkInitiation(t *testing.T) {
	parentId := &lcommon.GovActionId{
		TransactionId: [32]byte{4, 5, 6},
		GovActionIdx:  2,
	}
	action := &lcommon.HardForkInitiationGovAction{
		ActionId: parentId,
	}

	actionType, parentTxHash, parentActionIdx, _, err := extractGovActionInfo(action)
	assert.NoError(t, err)

	assert.Equal(
		t,
		uint8(lcommon.GovActionTypeHardForkInitiation),
		actionType,
	)
	assert.Equal(t, parentId.TransactionId[:], parentTxHash)
	assert.NotNil(t, parentActionIdx)
	assert.Equal(t, uint32(2), *parentActionIdx)
}

func TestExtractGovActionInfo_TreasuryWithdrawal(t *testing.T) {
	action := &lcommon.TreasuryWithdrawalGovAction{
		PolicyHash: []byte{0x01, 0x02, 0x03},
	}

	actionType, parentTxHash, parentActionIdx, policyHash, err := extractGovActionInfo(action)
	assert.NoError(t, err)

	assert.Equal(
		t,
		uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		actionType,
	)
	assert.Nil(t, parentTxHash)
	assert.Nil(t, parentActionIdx)
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, policyHash)
}

func TestExtractGovActionInfo_NoConfidence(t *testing.T) {
	parentId := &lcommon.GovActionId{
		TransactionId: [32]byte{7, 8, 9},
		GovActionIdx:  0,
	}
	action := &lcommon.NoConfidenceGovAction{
		ActionId: parentId,
	}

	actionType, parentTxHash, parentActionIdx, _, err := extractGovActionInfo(action)
	assert.NoError(t, err)

	assert.Equal(
		t,
		uint8(lcommon.GovActionTypeNoConfidence),
		actionType,
	)
	assert.Equal(t, parentId.TransactionId[:], parentTxHash)
	assert.NotNil(t, parentActionIdx)
	assert.Equal(t, uint32(0), *parentActionIdx)
}

func TestExtractGovActionInfo_UpdateCommittee(t *testing.T) {
	action := &lcommon.UpdateCommitteeGovAction{}

	actionType, parentTxHash, parentActionIdx, policyHash, err := extractGovActionInfo(action)
	assert.NoError(t, err)

	assert.Equal(
		t,
		uint8(lcommon.GovActionTypeUpdateCommittee),
		actionType,
	)
	assert.Nil(t, parentTxHash)
	assert.Nil(t, parentActionIdx)
	assert.Nil(t, policyHash)
}

func TestExtractGovActionInfo_NewConstitution(t *testing.T) {
	parentId := &lcommon.GovActionId{
		TransactionId: [32]byte{10, 11, 12},
		GovActionIdx:  3,
	}
	action := &lcommon.NewConstitutionGovAction{
		ActionId: parentId,
	}

	actionType, parentTxHash, parentActionIdx, _, err := extractGovActionInfo(action)
	assert.NoError(t, err)

	assert.Equal(
		t,
		uint8(lcommon.GovActionTypeNewConstitution),
		actionType,
	)
	assert.Equal(t, parentId.TransactionId[:], parentTxHash)
	assert.NotNil(t, parentActionIdx)
	assert.Equal(t, uint32(3), *parentActionIdx)
}

func TestExtractGovActionInfo_Info(t *testing.T) {
	action := &lcommon.InfoGovAction{}

	actionType, parentTxHash, parentActionIdx, policyHash, err := extractGovActionInfo(action)
	assert.NoError(t, err)

	assert.Equal(t, uint8(lcommon.GovActionTypeInfo), actionType)
	assert.Nil(t, parentTxHash)
	assert.Nil(t, parentActionIdx)
	assert.Nil(t, policyHash)
}
