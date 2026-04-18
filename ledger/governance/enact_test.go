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

func TestPartitionByExpiry(t *testing.T) {
	props := []*models.GovernanceProposal{
		{ExpiresEpoch: 5},
		{ExpiresEpoch: 10},
	}
	expired, active := partitionByExpiry(props, 7)
	require.Len(t, expired, 1)
	require.Len(t, active, 1)
	assert.Equal(t, uint64(5), expired[0].ExpiresEpoch)
	assert.Equal(t, uint64(10), active[0].ExpiresEpoch)
}

func TestPartitionByExpiry_EqualEpochStaysActive(t *testing.T) {
	props := []*models.GovernanceProposal{
		{ExpiresEpoch: 7},
	}
	expired, active := partitionByExpiry(props, 7)
	assert.Empty(t, expired)
	require.Len(t, active, 1)
}
