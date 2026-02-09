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

package blockfrost

import (
	"encoding/json"
	"testing"

	bfgo "github.com/blockfrost/blockfrost-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompatBlockResponse verifies that our BlockResponse
// round-trips through blockfrost-go's Block type.
func TestCompatBlockResponse(t *testing.T) {
	output := "12345"
	fees := "678"
	blockVRF := "vrf_vk1abc"
	opCert := "opcert123"
	opCertCounter := "5"
	nextBlock := "next_abc"

	ours := BlockResponse{
		Time:          1700000000,
		Height:        67890,
		Hash:          "abc123",
		Slot:          12345,
		Epoch:         100,
		EpochSlot:     345,
		SlotLeader:    "pool1xyz",
		Size:          1024,
		TxCount:       5,
		Output:        &output,
		Fees:          &fees,
		BlockVRF:      &blockVRF,
		OPCert:        &opCert,
		OPCertCounter: &opCertCounter,
		PreviousBlock: "prev123",
		NextBlock:     &nextBlock,
		Confirmations: 10,
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs bfgo.Block
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	assert.Equal(t, 1700000000, theirs.Time)
	assert.Equal(t, 67890, theirs.Height)
	assert.Equal(t, "abc123", theirs.Hash)
	assert.Equal(t, 12345, theirs.Slot)
	assert.Equal(t, 100, theirs.Epoch)
	assert.Equal(t, 345, theirs.EpochSlot)
	assert.Equal(t, "pool1xyz", theirs.SlotLeader)
	assert.Equal(t, 1024, theirs.Size)
	assert.Equal(t, 5, theirs.TxCount)
	require.NotNil(t, theirs.Output)
	assert.Equal(t, "12345", *theirs.Output)
	require.NotNil(t, theirs.Fees)
	assert.Equal(t, "678", *theirs.Fees)
	require.NotNil(t, theirs.BlockVRF)
	assert.Equal(t, "vrf_vk1abc", *theirs.BlockVRF)
	require.NotNil(t, theirs.OPCert)
	assert.Equal(t, "opcert123", *theirs.OPCert)
	require.NotNil(t, theirs.OPCertCounter)
	assert.Equal(t, "5", *theirs.OPCertCounter)
	assert.Equal(t, "prev123", theirs.PreviousBlock)
	require.NotNil(t, theirs.NextBlock)
	assert.Equal(t, "next_abc", *theirs.NextBlock)
	assert.Equal(t, 10, theirs.Confirmations)
}

// TestCompatBlockResponseNilFields verifies that null
// pointer fields round-trip correctly.
func TestCompatBlockResponseNilFields(t *testing.T) {
	output := "0"
	fees := "0"
	ours := BlockResponse{
		Hash:   "abc",
		Output: &output,
		Fees:   &fees,
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs bfgo.Block
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	assert.Nil(t, theirs.BlockVRF)
	assert.Nil(t, theirs.OPCert)
	assert.Nil(t, theirs.OPCertCounter)
	assert.Nil(t, theirs.NextBlock)
}

// TestCompatEpochResponse verifies that our
// EpochResponse round-trips through blockfrost-go's
// Epoch type.
func TestCompatEpochResponse(t *testing.T) {
	activeStake := "1000000"
	ours := EpochResponse{
		Epoch:          100,
		StartTime:      1700000000,
		EndTime:        1700432000,
		FirstBlockTime: 1700000020,
		LastBlockTime:  1700431980,
		BlockCount:     21600,
		TxCount:        50000,
		Output:         "999",
		Fees:           "123",
		ActiveStake:    &activeStake,
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs bfgo.Epoch
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	assert.Equal(t, 100, theirs.Epoch)
	assert.Equal(t, 1700000000, theirs.StartTime)
	assert.Equal(t, 1700432000, theirs.EndTime)
	assert.Equal(t, 1700000020, theirs.FirstBlockTime)
	assert.Equal(t, 1700431980, theirs.LastBlockTime)
	assert.Equal(t, 21600, theirs.BlockCount)
	assert.Equal(t, 50000, theirs.TxCount)
	assert.Equal(t, "999", theirs.Output)
	assert.Equal(t, "123", theirs.Fees)
	require.NotNil(t, theirs.ActiveStake)
	assert.Equal(t, "1000000", *theirs.ActiveStake)
}

// TestCompatProtocolParamsResponse verifies that our
// ProtocolParamsResponse round-trips through
// blockfrost-go's EpochParameters type.
func TestCompatProtocolParamsResponse(t *testing.T) {
	coinsPerUtxoSize := "4310"
	priceMem := 0.0577
	priceStep := 0.0000721
	maxTxExMem := "10000000000"
	maxTxExSteps := "10000000000000"
	maxBlockExMem := "50000000000"
	maxBlockExSteps := "40000000000000"
	maxValSize := "5000"
	collateralPercent := 150
	maxCollateralInputs := 3

	ours := ProtocolParamsResponse{
		Epoch:                 100,
		MinFeeA:               44,
		MinFeeB:               155381,
		MaxBlockSize:          65536,
		MaxTxSize:             16384,
		MaxBlockHeaderSize:    1100,
		KeyDeposit:            "2000000",
		PoolDeposit:           "500000000",
		EMax:                  18,
		NOpt:                  150,
		A0:                    0.3,
		Rho:                   0.003,
		Tau:                   0.2,
		DecentralisationParam: 0,
		ExtraEntropy:          nil,
		ProtocolMajorVer:      8,
		ProtocolMinorVer:      0,
		MinUtxo:               "0",
		MinPoolCost:           "170000000",
		Nonce:                 "abc123",
		CoinsPerUtxoSize:      &coinsPerUtxoSize,
		CoinsPerUtxoWord:      "4310",
		CostModels:            nil,
		PriceMem:              &priceMem,
		PriceStep:             &priceStep,
		MaxTxExMem:            &maxTxExMem,
		MaxTxExSteps:          &maxTxExSteps,
		MaxBlockExMem:         &maxBlockExMem,
		MaxBlockExSteps:       &maxBlockExSteps,
		MaxValSize:            &maxValSize,
		CollateralPercent:     &collateralPercent,
		MaxCollateralInputs:   &maxCollateralInputs,
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs bfgo.EpochParameters
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	assert.Equal(t, 100, theirs.Epoch)
	assert.Equal(t, 44, theirs.MinFeeA)
	assert.Equal(t, 155381, theirs.MinFeeB)
	assert.Equal(t, 65536, theirs.MaxBlockSize)
	assert.Equal(t, 16384, theirs.MaxTxSize)
	assert.Equal(t, 1100, theirs.MaxBlockHeaderSize)
	assert.Equal(t, "2000000", theirs.KeyDeposit)
	assert.Equal(t, "500000000", theirs.PoolDeposit)
	assert.Equal(t, 18, theirs.EMax)
	assert.Equal(t, 150, theirs.NOpt)
	assert.InDelta(t, 0.3, float64(theirs.A0), 0.001)
	assert.InDelta(
		t, 0.003, float64(theirs.Rho), 0.0001,
	)
	assert.InDelta(t, 0.2, float64(theirs.Tau), 0.001)
	assert.Equal(t, 8, theirs.ProtocolMajorVer)
	assert.Equal(t, 0, theirs.ProtocolMinorVer)
	assert.Equal(t, "170000000", theirs.MinPoolCost)
	assert.Equal(t, "abc123", theirs.Nonce)
	require.NotNil(t, theirs.CoinsPerUTxOSize)
	assert.Equal(t, "4310", *theirs.CoinsPerUTxOSize)
	require.NotNil(t, theirs.PriceMem)
	assert.InDelta(
		t, 0.0577, float64(*theirs.PriceMem), 0.0001,
	)
	require.NotNil(t, theirs.PriceStep)
	assert.InDelta(
		t,
		0.0000721,
		float64(*theirs.PriceStep),
		0.00001,
	)
	require.NotNil(t, theirs.MaxTxExMem)
	assert.Equal(t, "10000000000", *theirs.MaxTxExMem)
	require.NotNil(t, theirs.MaxTxExSteps)
	assert.Equal(
		t, "10000000000000", *theirs.MaxTxExSteps,
	)
	require.NotNil(t, theirs.MaxBlockExMem)
	assert.Equal(
		t, "50000000000", *theirs.MaxBlockExMem,
	)
	require.NotNil(t, theirs.MaxBlockExSteps)
	assert.Equal(
		t, "40000000000000", *theirs.MaxBlockExSteps,
	)
	// NOTE: blockfrost-go v0.3.0 has a tab in the
	// MaxValSize JSON tag ("max_val_size\t"), so this
	// field cannot round-trip. Skip assertion.
	// See: blockfrost-go api_epochs.go:95
	require.NotNil(t, theirs.CollateralPercent)
	assert.Equal(t, 150, *theirs.CollateralPercent)
	require.NotNil(t, theirs.MaxCollateralInputs)
	assert.Equal(t, 3, *theirs.MaxCollateralInputs)
}

// TestCompatHealthResponse verifies that our
// HealthResponse round-trips through blockfrost-go's
// Health type.
func TestCompatHealthResponse(t *testing.T) {
	ours := HealthResponse{IsHealthy: true}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs bfgo.Health
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	assert.True(t, theirs.IsHealthy)
}

// TestCompatRootResponse verifies that our
// RootResponse round-trips through blockfrost-go's
// Info type.
func TestCompatRootResponse(t *testing.T) {
	ours := RootResponse{
		URL:     "https://blockfrost.io/",
		Version: "0.1.0",
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs bfgo.Info
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	assert.Equal(
		t, "https://blockfrost.io/", theirs.Url,
	)
	assert.Equal(t, "0.1.0", theirs.Version)
}

// TestCompatNetworkResponse verifies that our
// NetworkResponse round-trips through blockfrost-go's
// NetworkInfo type.
func TestCompatNetworkResponse(t *testing.T) {
	ours := NetworkResponse{
		Supply: NetworkSupply{
			Max:         "45000000000000000",
			Total:       "33000000000000000",
			Circulating: "32000000000000000",
			Locked:      "1000000000000",
			Treasury:    "500000000000",
			Reserves:    "12000000000000000",
		},
		Stake: NetworkStake{
			Live:   "23000000000000000",
			Active: "22000000000000000",
		},
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs bfgo.NetworkInfo
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	assert.Equal(
		t, "45000000000000000", theirs.Supply.Max,
	)
	assert.Equal(
		t, "33000000000000000", theirs.Supply.Total,
	)
	assert.Equal(
		t,
		"32000000000000000",
		theirs.Supply.Circulating,
	)
	assert.Equal(
		t, "1000000000000", theirs.Supply.Locked,
	)
	assert.Equal(
		t, "23000000000000000", theirs.Stake.Live,
	)
	assert.Equal(
		t, "22000000000000000", theirs.Stake.Active,
	)
}
