// Copyright 2025 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blockfrost

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rat(num, denom int64) *cbor.Rat {
	return &cbor.Rat{Rat: big.NewRat(num, denom)}
}

// TestProtocolParamsResponseConway verifies that Conway-era governance and
// reference-script parameters are mapped from the native ledger type and
// surfaced in the Blockfrost response.
func TestProtocolParamsResponseConway(t *testing.T) {
	pp := &conway.ConwayProtocolParameters{
		MinFeeA:            44,
		MinFeeB:            155381,
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		KeyDeposit:         2000000,
		PoolDeposit:        500000000,
		MaxEpoch:           18,
		NOpt:               150,
		A0:                 rat(3, 10),
		Rho:                rat(3, 1000),
		Tau:                rat(2, 10),
		ProtocolVersion: common.ProtocolParametersProtocolVersion{
			Major: 10,
			Minor: 0,
		},
		MinPoolCost:    170000000,
		AdaPerUtxoByte: 4310,
		CostModels: map[uint][]int64{
			0: {197209, 0, 1},
			1: {197209, 0, 2},
			2: {100, 200, 300},
		},
		ExecutionCosts: common.ExUnitPrice{
			MemPrice:  rat(577, 10000),
			StepPrice: rat(721, 10000000),
		},
		MaxTxExUnits:         common.ExUnits{Memory: 14000000, Steps: 10000000000},
		MaxBlockExUnits:      common.ExUnits{Memory: 62000000, Steps: 20000000000},
		MaxValueSize:         5000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    cbor.Rat{Rat: big.NewRat(51, 100)},
			CommitteeNormal:       cbor.Rat{Rat: big.NewRat(52, 100)},
			CommitteeNoConfidence: cbor.Rat{Rat: big.NewRat(53, 100)},
			HardForkInitiation:    cbor.Rat{Rat: big.NewRat(54, 100)},
			PpSecurityGroup:       cbor.Rat{Rat: big.NewRat(55, 100)},
		},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    cbor.Rat{Rat: big.NewRat(67, 100)},
			CommitteeNormal:       cbor.Rat{Rat: big.NewRat(68, 100)},
			CommitteeNoConfidence: cbor.Rat{Rat: big.NewRat(60, 100)},
			UpdateToConstitution:  cbor.Rat{Rat: big.NewRat(75, 100)},
			HardForkInitiation:    cbor.Rat{Rat: big.NewRat(61, 100)},
			PpNetworkGroup:        cbor.Rat{Rat: big.NewRat(62, 100)},
			PpEconomicGroup:       cbor.Rat{Rat: big.NewRat(63, 100)},
			PpTechnicalGroup:      cbor.Rat{Rat: big.NewRat(64, 100)},
			PpGovGroup:            cbor.Rat{Rat: big.NewRat(76, 100)},
			TreasuryWithdrawal:    cbor.Rat{Rat: big.NewRat(65, 100)},
		},
		MinCommitteeSize:           7,
		CommitteeTermLimit:         146,
		GovActionValidityPeriod:    6,
		GovActionDeposit:           100000000000,
		DRepDeposit:                500000000,
		DRepInactivityPeriod:       20,
		MinFeeRefScriptCostPerByte: rat(15, 1),
	}

	info, err := protocolParamsInfoFromNative(pp, 507)
	require.NoError(t, err)
	resp := protocolParamsResponse(info)

	// Pool voting thresholds.
	require.NotNil(t, resp.PvtMotionNoConfidence)
	assert.InDelta(t, 0.51, *resp.PvtMotionNoConfidence, 1e-9)
	require.NotNil(t, resp.PvtCommitteeNormal)
	assert.InDelta(t, 0.52, *resp.PvtCommitteeNormal, 1e-9)
	require.NotNil(t, resp.PvtCommitteeNoConfidence)
	assert.InDelta(t, 0.53, *resp.PvtCommitteeNoConfidence, 1e-9)
	require.NotNil(t, resp.PvtHardForkInitiation)
	assert.InDelta(t, 0.54, *resp.PvtHardForkInitiation, 1e-9)

	// Security-group threshold is exposed under both keys.
	require.NotNil(t, resp.PvtPPSecurityGroup)
	assert.InDelta(t, 0.55, *resp.PvtPPSecurityGroup, 1e-9)
	require.NotNil(t, resp.PvtppSecurityGroup)
	assert.InDelta(t, 0.55, *resp.PvtppSecurityGroup, 1e-9)

	// DRep voting thresholds.
	require.NotNil(t, resp.DvtMotionNoConfidence)
	assert.InDelta(t, 0.67, *resp.DvtMotionNoConfidence, 1e-9)
	require.NotNil(t, resp.DvtUpdateToConstitution)
	assert.InDelta(t, 0.75, *resp.DvtUpdateToConstitution, 1e-9)
	require.NotNil(t, resp.DvtPPNetworkGroup)
	assert.InDelta(t, 0.62, *resp.DvtPPNetworkGroup, 1e-9)
	require.NotNil(t, resp.DvtPPGovGroup)
	assert.InDelta(t, 0.76, *resp.DvtPPGovGroup, 1e-9)
	require.NotNil(t, resp.DvtTreasuryWithdrawal)
	assert.InDelta(t, 0.65, *resp.DvtTreasuryWithdrawal, 1e-9)

	// Governance scalar parameters serialized as strings.
	require.NotNil(t, resp.CommitteeMinSize)
	assert.Equal(t, "7", *resp.CommitteeMinSize)
	require.NotNil(t, resp.CommitteeMaxTermLength)
	assert.Equal(t, "146", *resp.CommitteeMaxTermLength)
	require.NotNil(t, resp.GovActionLifetime)
	assert.Equal(t, "6", *resp.GovActionLifetime)
	require.NotNil(t, resp.GovActionDeposit)
	assert.Equal(t, "100000000000", *resp.GovActionDeposit)
	require.NotNil(t, resp.DrepDeposit)
	assert.Equal(t, "500000000", *resp.DrepDeposit)
	require.NotNil(t, resp.DrepActivity)
	assert.Equal(t, "20", *resp.DrepActivity)

	// Reference-script cost.
	require.NotNil(t, resp.MinFeeRefScriptCostPerByte)
	assert.InDelta(t, 15.0, *resp.MinFeeRefScriptCostPerByte, 1e-9)

	// Raw cost models keyed by Plutus version name.
	require.NotNil(t, resp.CostModelsRaw)
	raw, ok := (*resp.CostModelsRaw).(map[string][]int64)
	require.True(t, ok)
	assert.Equal(t, []int64{197209, 0, 1}, raw["PlutusV1"])
	assert.Equal(t, []int64{197209, 0, 2}, raw["PlutusV2"])
	assert.Equal(t, []int64{100, 200, 300}, raw["PlutusV3"])

	// The full response must round-trip through JSON with the governance
	// keys present.
	data, err := json.Marshal(resp)
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))
	for _, key := range []string{
		"pvt_motion_no_confidence",
		"dvt_p_p_network_group",
		"dvt_p_p_gov_group",
		"committee_min_size",
		"gov_action_deposit",
		"drep_deposit",
		"drep_activity",
		"pvtpp_security_group",
		"pvt_p_p_security_group",
		"min_fee_ref_script_cost_per_byte",
		"cost_models_raw",
	} {
		_, present := decoded[key]
		assert.Truef(t, present, "expected key %q in response", key)
	}
}

// TestProtocolParamsResponseShelleyGovernanceNull verifies that an older-era
// (Shelley) parameter response carries the Conway governance fields as JSON
// null rather than omitting them or filling placeholders.
func TestProtocolParamsResponseShelleyGovernanceNull(t *testing.T) {
	pp := &shelley.ShelleyProtocolParameters{
		MinFeeA:            44,
		MinFeeB:            155381,
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		KeyDeposit:         2000000,
		PoolDeposit:        500000000,
		MaxEpoch:           18,
		NOpt:               150,
		A0:                 rat(3, 10),
		Rho:                rat(3, 1000),
		Tau:                rat(2, 10),
		ProtocolMajor:      2,
		ProtocolMinor:      0,
	}

	info, err := protocolParamsInfoFromNative(pp, 200)
	require.NoError(t, err)
	resp := protocolParamsResponse(info)

	assert.Nil(t, resp.PvtMotionNoConfidence)
	assert.Nil(t, resp.DvtPPNetworkGroup)
	assert.Nil(t, resp.PvtPPSecurityGroup)
	assert.Nil(t, resp.PvtppSecurityGroup)
	assert.Nil(t, resp.CommitteeMinSize)
	assert.Nil(t, resp.GovActionDeposit)
	assert.Nil(t, resp.DrepDeposit)
	assert.Nil(t, resp.MinFeeRefScriptCostPerByte)
	assert.Nil(t, resp.CostModelsRaw)

	// The keys must still be present in the JSON, serialized as null.
	data, err := json.Marshal(resp)
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))
	for _, key := range []string{
		"pvt_motion_no_confidence",
		"committee_min_size",
		"gov_action_deposit",
		"min_fee_ref_script_cost_per_byte",
		"cost_models_raw",
	} {
		value, present := decoded[key]
		require.Truef(t, present, "expected key %q in response", key)
		assert.Nilf(t, value, "expected key %q to be null", key)
	}
}
