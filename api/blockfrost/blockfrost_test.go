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

package blockfrost

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	bfgo "github.com/blockfrost/blockfrost-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompatBlockResponse verifies that our BlockResponse
// round-trips through blockfrost-go's Block type.
func TestCompatBlockResponse(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

// TestCompatAssetResponse verifies that our AssetResponse
// round-trips through blockfrost-go's Asset type.
func TestCompatAssetResponse(t *testing.T) {
	t.Parallel()

	ours := AssetResponse{
		Asset:             "00112233445566778899aabbccddeeff00112233445566778899aabb746f6b656e",
		PolicyID:          "00112233445566778899aabbccddeeff00112233445566778899aabb",
		AssetName:         "746f6b656e",
		AssetNameASCII:    "token",
		Fingerprint:       "asset1test",
		Quantity:          "42",
		InitialMintTxHash: "",
		MintOrBurnCount:   0,
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs bfgo.Asset
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	assert.Equal(t, ours.Asset, theirs.Asset)
	assert.Equal(t, ours.PolicyID, theirs.PolicyId)
	assert.Equal(t, ours.AssetName, theirs.AssetName)
	assert.Equal(t, ours.Fingerprint, theirs.Fingerprint)
	assert.Equal(t, ours.Quantity, theirs.Quantity)
	assert.Equal(t, ours.InitialMintTxHash, theirs.InitialMintTxHash)
	assert.Equal(t, ours.MintOrBurnCount, theirs.MintOrBurnCount)
	assert.Nil(t, theirs.OnchainMetadata)
	assert.Nil(t, theirs.OnchainMetadataStandard)
	assert.Nil(t, theirs.OnchainMetadataExtra)
	assert.Nil(t, theirs.Metadata)
}

// TestCompatProtocolParamsResponse verifies that our
// ProtocolParamsResponse round-trips through
// blockfrost-go's EpochParameters type.
func TestCompatProtocolParamsResponse(t *testing.T) {
	t.Parallel()

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

func TestCompatMetadataTransactionJSONResponse(t *testing.T) {
	t.Parallel()

	ours := MetadataTransactionJSONResponse{
		TxHash: "abc123",
		JSONMetadata: json.RawMessage(
			`{"name":"nft-one","image":"ipfs://cid"}`,
		),
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs bfgo.MetadataTxContentInJSON
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	assert.Equal(t, "abc123", theirs.TxHash)
	require.NotNil(t, theirs.JSONMetadata)
	jsonMetadata, ok := (*theirs.JSONMetadata).(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "nft-one", jsonMetadata["name"])
	assert.Equal(t, "ipfs://cid", jsonMetadata["image"])
}

func TestCompatMetadataTransactionCBORResponse(t *testing.T) {
	t.Parallel()

	cborMetadata := "a1646e616d65676e66742d6f6e65"
	ours := MetadataTransactionCBORResponse{
		TxHash:       "abc123",
		CborMetadata: &cborMetadata,
		Metadata:     cborMetadata,
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs bfgo.MetadataTxContentInCBOR
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	assert.Equal(t, "abc123", theirs.TxHash)
	require.NotNil(t, theirs.Metadata)
	assert.Equal(t, cborMetadata, *theirs.Metadata)
}

func TestCompatTransactionResponse(t *testing.T) {
	t.Parallel()

	invalidBefore := "100"
	invalidHereafter := "200"
	ours := TransactionResponse{
		Hash:        "txhash1",
		Block:       "blockhash1",
		Slot:        123,
		BlockHeight: 45,
		BlockTime:   1700000000,
		Index:       2,
		OutputAmount: []AddressAmountResponse{
			{Unit: "lovelace", Quantity: "5000"},
		},
		Fees:             "170000",
		Deposit:          "0",
		Size:             512,
		UtxoCount:        3,
		StakeCertCount:   1,
		DelegationCount:  1,
		RedeemerCount:    2,
		ValidContract:    true,
		InvalidBefore:    &invalidBefore,
		InvalidHereafter: &invalidHereafter,
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs bfgo.TransactionContent
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	assert.Equal(t, "txhash1", theirs.Hash)
	assert.Equal(t, "blockhash1", theirs.Block)
	assert.Equal(t, 123, theirs.Slot)
	assert.Equal(t, 45, theirs.BlockHeight)
	assert.Equal(t, 1700000000, theirs.BlockTime)
	assert.Equal(t, 2, theirs.Index)
	assert.Equal(t, "170000", theirs.Fees)
	require.Len(t, theirs.OutputAmount, 1)
	assert.Equal(t, "lovelace", theirs.OutputAmount[0].Unit)
	assert.Equal(t, "5000", theirs.OutputAmount[0].Quantity)
	require.NotNil(t, theirs.InvalidBefore)
	assert.Equal(t, "100", *theirs.InvalidBefore)
	require.NotNil(t, theirs.InvalidHereafter)
	assert.Equal(t, "200", *theirs.InvalidHereafter)
}

func TestCompatAddressUTXOResponse(t *testing.T) {
	t.Parallel()

	dataHash := "datumhash1"
	inlineDatum := "d8799f"
	refScriptHash := "scripthash1"
	ours := AddressUTXOResponse{
		Address:     "addr1...",
		TxHash:      "txhash1",
		OutputIndex: 1,
		Amount: []AddressAmountResponse{
			{Unit: "lovelace", Quantity: "1000"},
			{Unit: "policyasset", Quantity: "5"},
		},
		Block:               "blockhash1",
		DataHash:            &dataHash,
		InlineDatum:         &inlineDatum,
		ReferenceScriptHash: &refScriptHash,
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs bfgo.AddressUTXO
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	assert.Equal(t, "addr1...", theirs.Address)
	assert.Equal(t, "txhash1", theirs.TxHash)
	assert.Equal(t, 1, theirs.OutputIndex)
	require.Len(t, theirs.Amount, 2)
	assert.Equal(t, "lovelace", theirs.Amount[0].Unit)
	assert.Equal(t, "1000", theirs.Amount[0].Quantity)
	assert.Equal(t, "blockhash1", theirs.Block)
	require.NotNil(t, theirs.DataHash)
	assert.Equal(t, "datumhash1", *theirs.DataHash)
	require.NotNil(t, theirs.InlineDatum)
	assert.Equal(t, "d8799f", *theirs.InlineDatum)
	require.NotNil(t, theirs.ReferenceScriptHash)
	assert.Equal(t, "scripthash1", *theirs.ReferenceScriptHash)
}

func TestCompatAddressTransactionResponse(t *testing.T) {
	t.Parallel()

	ours := AddressTransactionResponse{
		TxHash:      "txhash1",
		TxIndex:     3,
		BlockHeight: 99,
		BlockTime:   1700000000,
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs bfgo.AddressTransactions
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	assert.Equal(t, "txhash1", theirs.TxHash)
	assert.Equal(t, 3, theirs.TxIndex)
	assert.EqualValues(t, 99, theirs.BlockHeight)
	assert.Equal(t, 1700000000, theirs.BlockTime)
}

// TestCompatHealthResponse verifies that our
// HealthResponse round-trips through blockfrost-go's
// Health type.
func TestCompatHealthResponse(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

// TestCompatAccountResponse verifies that our
// AccountResponse round-trips through blockfrost-go's
// Account type.
func TestCompatAccountResponse(t *testing.T) {
	t.Parallel()

	activeEpoch := int64(42)
	poolID := "pool1xyz"
	drepID := "drep1y2v8nygzdll2krgvw5dzqpkncvj3y"
	ours := AccountResponse{
		StakeAddress:       "stake_test1upugeuz3jdy0a7hncusutadavzcetdzylgxcldz39hp9n0s0xy0n5",
		Active:             true,
		ActiveEpoch:        &activeEpoch,
		ControlledAmount:   "1000000",
		RewardsSum:         "123",
		WithdrawalsSum:     "45",
		ReservesSum:        "6",
		TreasurySum:        "7",
		WithdrawableAmount: "89",
		PoolID:             &poolID,
		DrepID:             &drepID,
		Registered:         true,
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs bfgo.Account
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	assert.Equal(t, ours.StakeAddress, theirs.StakeAddress)
	assert.Equal(t, ours.Active, theirs.Active)
	require.NotNil(t, theirs.ActiveEpoch)
	assert.Equal(t, activeEpoch, *theirs.ActiveEpoch)
	assert.Equal(
		t, ours.ControlledAmount, theirs.ControlledAmount,
	)
	assert.Equal(t, ours.RewardsSum, theirs.RewardsSum)
	assert.Equal(
		t, ours.WithdrawalsSum, theirs.WithdrawalsSum,
	)
	assert.Equal(t, ours.ReservesSum, theirs.ReservesSum)
	assert.Equal(t, ours.TreasurySum, theirs.TreasurySum)
	assert.Equal(
		t, ours.WithdrawableAmount, theirs.WithdrawableAmount,
	)
	require.NotNil(t, theirs.PoolID)
	assert.Equal(t, poolID, *theirs.PoolID)
	require.NotNil(t, theirs.DrepID)
	assert.Equal(t, drepID, *theirs.DrepID)
	assert.Equal(t, ours.Registered, theirs.Registered)
}

// TestCompatAccountAssociatedAddressesResponse verifies
// that our AccountAssociatedAddressResponse round-trips
// through blockfrost-go's AccountAssociatedAddress type.
func TestCompatAccountAssociatedAddressesResponse(t *testing.T) {
	t.Parallel()

	ours := []AccountAssociatedAddressResponse{
		{Address: "addr_test1qqqexample"},
		{Address: "addr_test1qqqexample2"},
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs []bfgo.AccountAssociatedAddress
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	require.Len(t, theirs, 2)
	assert.Equal(t, ours[0].Address, theirs[0].Address)
	assert.Equal(t, ours[1].Address, theirs[1].Address)
}

// TestCompatAccountDelegationHistoryResponse verifies
// that our AccountDelegationHistoryResponse round-trips
// through blockfrost-go's AccountDelegationHistory type.
func TestCompatAccountDelegationHistoryResponse(t *testing.T) {
	t.Parallel()

	ours := []AccountDelegationHistoryResponse{
		{
			ActiveEpoch: 7,
			TxHash:      "abc123",
			Amount:      "1000000",
			PoolID:      "pool1xyz",
			TxSlot:      123456,
			BlockTime:   1700000000,
			BlockHeight: 9876,
		},
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs []bfgo.AccountDelegationHistory
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	require.Len(t, theirs, 1)
	assert.Equal(
		t, int32(ours[0].ActiveEpoch), theirs[0].ActiveEpoch,
	)
	assert.Equal(t, ours[0].TxHash, theirs[0].TXHash)
	assert.Equal(t, ours[0].Amount, theirs[0].Amount)
	assert.Equal(t, ours[0].PoolID, theirs[0].PoolID)
	assert.Equal(t, int(ours[0].TxSlot), theirs[0].TxSlot)
	assert.Equal(t, int(ours[0].BlockTime), theirs[0].BlockTime)
	assert.Equal(t, int(ours[0].BlockHeight), theirs[0].BlockHeight)
}

// TestCompatAccountRegistrationHistoryResponse verifies
// that our AccountRegistrationHistoryResponse
// round-trips through blockfrost-go's
// AccountRegistrationHistory type.
func TestCompatAccountRegistrationHistoryResponse(
	t *testing.T,
) {
	t.Parallel()

	ours := []AccountRegistrationHistoryResponse{
		{
			TxHash:      "abc123",
			Action:      "registered",
			Deposit:     "2000000",
			TxSlot:      123456,
			BlockTime:   1700000000,
			BlockHeight: 9876,
		},
		{
			TxHash:      "def456",
			Action:      "deregistered",
			Deposit:     "2000000",
			TxSlot:      223456,
			BlockTime:   1700000100,
			BlockHeight: 9999,
		},
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs []bfgo.AccountRegistrationHistory
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	require.Len(t, theirs, 2)
	assert.Equal(t, ours[0].TxHash, theirs[0].TXHash)
	assert.Equal(t, ours[0].Action, theirs[0].Action)
	assert.Equal(t, int(ours[0].TxSlot), theirs[0].TxSlot)
	assert.Equal(t, int(ours[0].BlockTime), theirs[0].BlockTime)
	assert.Equal(t, int(ours[0].BlockHeight), theirs[0].BlockHeight)
	assert.Equal(t, ours[1].TxHash, theirs[1].TXHash)
	assert.Equal(t, ours[1].Action, theirs[1].Action)
	assert.Equal(t, int(ours[1].TxSlot), theirs[1].TxSlot)
	assert.Equal(t, int(ours[1].BlockTime), theirs[1].BlockTime)
	assert.Equal(t, int(ours[1].BlockHeight), theirs[1].BlockHeight)

	// bfgo.AccountRegistrationHistory has no Deposit field; assert via raw
	// JSON so a serialization regression on the "deposit" key is caught.
	var rawItems []map[string]any
	require.NoError(t, json.Unmarshal(data, &rawItems))
	require.Len(t, rawItems, 2)
	assert.Equal(t, ours[0].Deposit, rawItems[0]["deposit"])
	assert.Equal(t, ours[1].Deposit, rawItems[1]["deposit"])
}

// TestCompatAccountRewardHistoryResponse verifies that
// our AccountRewardHistoryResponse round-trips through
// blockfrost-go's AccountRewardsHistory type.
func TestCompatAccountRewardHistoryResponse(t *testing.T) {
	t.Parallel()

	ours := []AccountRewardHistoryResponse{
		{
			Epoch:  11,
			Amount: "500",
			PoolID: "pool1xyz",
		},
	}

	data, err := json.Marshal(ours)
	require.NoError(t, err)

	var theirs []bfgo.AccountRewardsHistory
	err = json.Unmarshal(data, &theirs)
	require.NoError(t, err)

	require.Len(t, theirs, 1)
	assert.Equal(t, int32(ours[0].Epoch), theirs[0].Epoch)
	assert.Equal(t, ours[0].Amount, theirs[0].Amount)
	assert.Equal(t, ours[0].PoolID, theirs[0].PoolID)
}

func TestRedeemerExecutionFee(t *testing.T) {
	t.Parallel()

	fee := redeemerExecutionFee(
		lcommon.ExUnitPrice{
			MemPrice:  &cbor.Rat{Rat: big.NewRat(577, 10000)},
			StepPrice: &cbor.Rat{Rat: big.NewRat(721, 10000000)},
		},
		245264,
		103956223,
	)
	assert.Equal(t, uint64(21647), fee)
}

func TestMetadataJSONUnavailableSerializesAsNull(t *testing.T) {
	payload, err := json.Marshal(MetadataTransactionJSONResponse{
		TxHash:       "deadbeef",
		JSONMetadata: nil,
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"tx_hash":"deadbeef","json_metadata":null}`, string(payload))
}

func TestMetadataTransactionsCollisionKeepsLabelAndRawCBOR(t *testing.T) {
	t.Parallel()
	adapter, raw, _ := newDBBackedAdapter(t)
	validHash := bytes.Repeat([]byte{0x41}, 32)
	ambiguousHash := bytes.Repeat([]byte{0x42}, 32)
	validMetadata, err := hex.DecodeString("a11902d1626f6b")
	require.NoError(t, err)
	ambiguousMetadata, err := hex.DecodeString("a11902d1a20163696e7461316474657874")
	require.NoError(t, err)
	validTx := &models.Transaction{Hash: validHash, Metadata: validMetadata}
	ambiguousTx := &models.Transaction{Hash: ambiguousHash, Metadata: ambiguousMetadata}
	insertAdapterTransaction(t, raw, validTx)
	insertAdapterTransaction(t, raw, ambiguousTx)
	ambiguousLabelCBOR, err := hex.DecodeString("a20163696e7461316474657874")
	require.NoError(t, err)
	_, err = raw.Exec(`INSERT INTO transaction_metadata_label (transaction_id, label, slot, cbor_value, json_value) VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)`,
		validTx.ID, "721", 1, []byte{0x62, 0x6f, 0x6b}, `"ok"`,
		ambiguousTx.ID, "721", 2, ambiguousLabelCBOR, nil)
	require.NoError(t, err)
	jsonRows, total, err := adapter.MetadataTransactions(
		context.Background(),
		721,
		PaginationParams{Count: 2, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, jsonRows, 2)
	require.Equal(t, `"ok"`, string(jsonRows[0].JSONMetadata))
	require.Nil(t, jsonRows[1].JSONMetadata)
	cborRows, _, err := adapter.MetadataTransactionsCBOR(
		context.Background(),
		721,
		PaginationParams{Count: 2, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Len(t, cborRows, 2)
	require.Equal(t, "a20163696e7461316474657874", cborRows[1].Metadata)
	transactionJSON, err := adapter.TransactionMetadata(context.Background(), ambiguousHash)
	require.NoError(t, err)
	require.Len(t, transactionJSON, 1)
	require.Equal(t, "", string(transactionJSON[0].JSONMetadata))
	transactionCBOR, err := adapter.TransactionMetadataCBOR(context.Background(), ambiguousHash)
	require.NoError(t, err)
	require.Len(t, transactionCBOR, 1)
	require.Equal(t, "a20163696e7461316474657874", transactionCBOR[0].CBORMetadata)
}

// TestMetadataEndpointsDeterministicAcrossRepeatsAndKeyOrders stores pairs of
// equivalent metadata under different top-level and nested key orders. One
// pair also distinguishes integer key 1 from text key "1". It checks that
// every metadata endpoint returns the same label set, JSON availability and
// per-label CBOR on every call. The vectors and expectations are written out
// by hand from the CBOR bytes.
func TestMetadataEndpointsDeterministicAcrossRepeatsAndKeyOrders(t *testing.T) {
	t.Parallel()
	adapter, raw, _ := newDBBackedAdapter(t)

	const (
		collideIntFirst  = "a20163696e7461316474657874"
		collideTextFirst = "a2613164746578740163696e74"
	)
	type metaTx struct {
		hashByte byte
		metadata string
		label721 string
	}
	txs := []metaTx{
		{0x51, "a21902d1" + collideIntFirst + "016161", collideIntFirst},
		{0x52, "a2016161" + "1902d1" + collideIntFirst, collideIntFirst},
		{0x53, "a21902d1" + collideTextFirst + "016161", collideTextFirst},
		{0x54, "a2016161" + "1902d1" + collideTextFirst, collideTextFirst},
	}
	for i, mt := range txs {
		metadataCbor, err := hex.DecodeString(mt.metadata)
		require.NoError(t, err)
		label721, err := hex.DecodeString(mt.label721)
		require.NoError(t, err)
		tx := &models.Transaction{
			Hash:     bytes.Repeat([]byte{mt.hashByte}, 32),
			Metadata: metadataCbor,
		}
		insertAdapterTransaction(t, raw, tx)
		_, err = raw.Exec(`INSERT INTO transaction_metadata_label (transaction_id, label, slot, cbor_value, json_value) VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)`,
			tx.ID, "1", i+1, []byte{0x61, 0x61}, `"a"`,
			tx.ID, "721", i+1, label721, nil)
		require.NoError(t, err)
	}

	for pass := range 16 {
		for i, mt := range txs {
			hash := bytes.Repeat([]byte{mt.hashByte}, 32)

			jsonLabels, err := adapter.TransactionMetadata(context.Background(), hash)
			require.NoError(t, err, "pass %d tx %d", pass, i)
			require.Len(t, jsonLabels, 2)
			require.Equal(t, "1", jsonLabels[0].Label)
			require.Equal(t, `"a"`, string(jsonLabels[0].JSONMetadata))
			require.Equal(t, "721", jsonLabels[1].Label)
			require.Empty(t, jsonLabels[1].JSONMetadata, "pass %d tx %d", pass, i)

			cborLabels, err := adapter.TransactionMetadataCBOR(context.Background(), hash)
			require.NoError(t, err)
			require.Len(t, cborLabels, 2)
			require.Equal(t, "1", cborLabels[0].Label)
			require.Equal(t, "6161", cborLabels[0].CBORMetadata)
			require.Equal(t, "721", cborLabels[1].Label)
			require.Equal(t, mt.label721, cborLabels[1].CBORMetadata)
		}

		params := PaginationParams{
			Count: 10,
			Page:  1,
			Order: PaginationOrderAsc,
		}
		jsonRows, total, err := adapter.MetadataTransactions(
			context.Background(),
			721,
			params,
		)
		require.NoError(t, err)
		require.Equal(t, len(txs), total)
		require.Len(t, jsonRows, len(txs))
		cborRows, cborTotal, err := adapter.MetadataTransactionsCBOR(
			context.Background(),
			721,
			params,
		)
		require.NoError(t, err)
		require.Equal(t, len(txs), cborTotal)
		require.Len(t, cborRows, len(txs))
		for i, mt := range txs {
			require.Equal(t, hex.EncodeToString(bytes.Repeat([]byte{mt.hashByte}, 32)), jsonRows[i].TxHash)
			require.Nil(t, jsonRows[i].JSONMetadata, "pass %d row %d", pass, i)
			require.Equal(t, jsonRows[i].TxHash, cborRows[i].TxHash)
			require.Equal(t, mt.label721, cborRows[i].Metadata)
		}

		// Representable label stays available alongside the colliding one.
		oneRows, oneTotal, err := adapter.MetadataTransactions(
			context.Background(),
			1,
			params,
		)
		require.NoError(t, err)
		require.Equal(t, len(txs), oneTotal)
		require.Len(t, oneRows, len(txs))
		for _, row := range oneRows {
			require.Equal(t, `"a"`, string(row.JSONMetadata))
		}

		// Pagination does not drop or repeat the unavailable-JSON rows.
		var seen []string
		for page := 1; page <= len(txs); page++ {
			rows, _, err := adapter.MetadataTransactions(
				context.Background(),
				721,
				PaginationParams{
					Count: 1,
					Page:  page,
					Order: PaginationOrderAsc,
				},
			)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			seen = append(seen, rows[0].TxHash)
		}
		require.Equal(t, []string{
			hex.EncodeToString(bytes.Repeat([]byte{0x51}, 32)),
			hex.EncodeToString(bytes.Repeat([]byte{0x52}, 32)),
			hex.EncodeToString(bytes.Repeat([]byte{0x53}, 32)),
			hex.EncodeToString(bytes.Repeat([]byte{0x54}, 32)),
		}, seen)
	}
}

// newMetadataLabelAdapter returns an adapter holding n transactions that all
// carry label 721.
func newMetadataLabelAdapter(t *testing.T, n int) (*NodeAdapter, *sql.DB) {
	t.Helper()
	adapter, raw, _ := newDBBackedAdapter(t)
	for i := range n {
		tx := &models.Transaction{
			Hash:     bytes.Repeat([]byte{byte(0x41 + i)}, 32),
			Metadata: []byte{0xa1, 0x19, 0x02, 0xd1, 0x62, 0x6f, 0x6b},
		}
		insertAdapterTransaction(t, raw, tx)
		_, err := raw.Exec(
			`INSERT INTO transaction_metadata_label (transaction_id, label, slot, cbor_value, json_value) VALUES (?, ?, ?, ?, ?)`,
			tx.ID,
			"721",
			i+1,
			[]byte{0x62, 0x6f, 0x6b},
			`"ok"`,
		)
		require.NoError(t, err)
	}
	return adapter, raw
}

func TestMetadataTransactionsPastEndSkipsPageQuery(t *testing.T) {
	t.Parallel()
	const total = 3
	adapter, raw := newMetadataLabelAdapter(t, total)
	// The count reads only the label table, while the page query reads
	// "transaction". Renaming that table makes any page query fail, so only a
	// skipped query can succeed.
	_, err := raw.Exec(`ALTER TABLE "transaction" RENAME TO transaction_hidden`)
	require.NoError(t, err)

	for _, order := range []string{PaginationOrderAsc, PaginationOrderDesc} {
		for _, tc := range []struct {
			name     string
			params   PaginationParams
			skipped  bool
			wantRows int
		}{
			{"offset just inside", PaginationParams{Count: 1, Page: total}, false, 0},
			{"offset equals total", PaginationParams{Count: 1, Page: total + 1}, true, 0},
			{"far past the end", PaginationParams{Count: 10, Page: 1000000}, true, 0},
			{"offset overflows int", PaginationParams{Count: 100, Page: math.MaxInt}, true, 0},
		} {
			t.Run(order+"/"+tc.name, func(t *testing.T) {
				tc.params.Order = order
				jsonRows, jsonTotal, jsonErr := adapter.MetadataTransactions(
					context.Background(), 721, tc.params,
				)
				cborRows, cborTotal, cborErr := adapter.MetadataTransactionsCBOR(
					context.Background(),
					721,
					tc.params,
				)
				if !tc.skipped {
					require.Error(t, jsonErr)
					require.Error(t, cborErr)
					return
				}
				require.NoError(t, jsonErr)
				require.NoError(t, cborErr)
				require.Equal(t, total, jsonTotal)
				require.Equal(t, total, cborTotal)
				require.Empty(t, jsonRows)
				require.Empty(t, cborRows)
			})
		}
	}
}

func TestMetadataTransactionsEmptyLabel(t *testing.T) {
	t.Parallel()
	adapter, _ := newMetadataLabelAdapter(t, 0)
	params := PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc}

	rows, total, err := adapter.MetadataTransactions(
		context.Background(),
		721,
		params,
	)
	require.NoError(t, err)
	require.Zero(t, total)
	require.Empty(t, rows)
	cborRows, total, err := adapter.MetadataTransactionsCBOR(
		context.Background(),
		721,
		params,
	)
	require.NoError(t, err)
	require.Zero(t, total)
	require.Empty(t, cborRows)
}

func TestMetadataTransactionsCancelledContext(t *testing.T) {
	t.Parallel()
	adapter, _ := newMetadataLabelAdapter(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	params := PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc}

	_, _, err := adapter.MetadataTransactions(ctx, 721, params)
	require.ErrorIs(t, err, context.Canceled)
	_, _, err = adapter.MetadataTransactionsCBOR(ctx, 721, params)
	require.ErrorIs(t, err, context.Canceled)
}

// mockNode implements BlockfrostNode for testing.
type mockNode struct {
	historyMock
	queryMock
	chainTip                      ChainTipInfo
	block                         BlockInfo
	blockByID                     BlockInfo
	txHashes                      []string
	epoch                         EpochInfo
	params                        ProtocolParamsInfo
	epochParams                   ProtocolParamsInfo
	network                       NetworkInfo
	networkEras                   []NetworkEraInfo
	genesis                       GenesisInfo
	pools                         []PoolExtendedInfo
	poolsList                     []string
	poolsListTotal                int
	poolsListParams               PaginationParams
	poolsRetiring                 []PoolRetiringInfo
	poolsRetiringTotal            int
	poolMetadata                  PoolMetadataInfo
	poolDetail                    PoolDetailInfo
	poolDetailErr                 error
	asset                         AssetInfo
	assetHolders                  []AssetHolderInfo
	assetHoldersTotal             int
	assetAddressesPolicyID        string
	assetAddressesAssetName       []byte
	assetAddressesParams          PaginationParams
	drep                          DRepInfo
	drepList                      []DRepListItemInfo
	drepListTotal                 int
	drepListParams                DRepListParams
	drepCredential                DRepCredential
	addressInfo                   AddressInfo
	addressUTXOs                  []AddressUTXOInfo
	addressTransactions           []AddressTransactionInfo
	metadataJSON                  []MetadataTransactionJSONInfo
	metadataCBOR                  []MetadataTransactionCBORInfo
	transaction                   TransactionInfo
	transactionCtx                context.Context
	transactionSubmitHash         string
	transactionEvaluation         TransactionEvaluationResponse
	transactionEvaluationCtx      context.Context
	transactionEvaluateCbor       []byte
	transactionCBOR               []byte
	transactionMetadata           []TransactionMetadataInfo
	transactionMetadataCBOR       []TransactionMetadataCBORInfo
	transactionUTXOs              TransactionUTXOsInfo
	transactionDelegations        []TransactionDelegationInfo
	transactionStakes             []TransactionStakeAddressInfo
	transactionWithdrawals        []TransactionWithdrawalInfo
	transactionMIRs               []TransactionMIRInfo
	transactionPoolUpdates        []TransactionPoolUpdateInfo
	transactionPoolRetires        []TransactionPoolRetireInfo
	transactionRedeemers          []TransactionRedeemerInfo
	transactionRequiredSigners    []TransactionRequiredSignerInfo
	addressUTXOsTotal             int
	addressTxsTotal               int
	metadataJSONTotal             int
	metadataCBORTotal             int
	account                       AccountInfo
	addresses                     []AccountAssociatedAddressInfo
	delegations                   []AccountDelegationHistoryInfo
	regs                          []AccountRegistrationHistoryInfo
	rewards                       []AccountRewardHistoryInfo
	accountUTXOs                  []AccountUTXOInfo
	accountWithdrawals            []AccountWithdrawalInfo
	accountTransactions           []AccountTransactionInfo
	lastAccountTransactionsParams TransactionRangeParams
	lastAddressTransactionsParams TransactionRangeParams
	chainTipErr                   error
	blockErr                      error
	blockByIDErr                  error
	txHashesErr                   error
	epochErr                      error
	paramsErr                     error
	epochParamsErr                error
	networkErr                    error
	networkErasErr                error
	genesisErr                    error
	poolsErr                      error
	poolsListErr                  error
	poolsRetiringErr              error
	poolMetadataErr               error
	assetErr                      error
	assetAddressesErr             error
	drepErr                       error
	drepListErr                   error
	addressInfoErr                error
	addressUTXOsErr               error
	addressTransactionsErr        error
	metadataJSONErr               error
	metadataCBORErr               error
	metadataCtx                   context.Context
	transactionErr                error
	transactionSubmitErr          error
	transactionEvaluationErr      error
	transactionCBORErr            error
	transactionMetadataErr        error
	transactionMetadataCBORErr    error
	transactionUTXOsErr           error
	transactionDelegationsErr     error
	transactionStakesErr          error
	transactionWithdrawalsErr     error
	transactionMIRsErr            error
	transactionPoolUpdatesErr     error
	transactionPoolRetiresErr     error
	transactionRedeemersErr       error
	transactionRequiredSignersErr error
	accountErr                    error
	addressesErr                  error
	delegationsErr                error
	regsErr                       error
	rewardsErr                    error
	accountUTXOsErr               error
	accountWithdrawalsErr         error
	accountTransactionsErr        error
}

func (m *mockNode) ChainTip() (
	ChainTipInfo, error,
) {
	return m.chainTip, m.chainTipErr
}

func (m *mockNode) LatestBlock(_ context.Context) (
	BlockInfo, error,
) {
	return m.block, m.blockErr
}

func (m *mockNode) BlockByHashOrNumber(
	_ context.Context,
	_ string,
) (BlockInfo, error) {
	return m.blockByID, m.blockByIDErr
}

func (m *mockNode) LatestBlockTxHashes(_ context.Context) (
	[]string, error,
) {
	return m.txHashes, m.txHashesErr
}

func (m *mockNode) CurrentEpoch(
	_ context.Context,
) (EpochInfo, error) {
	return m.epoch, m.epochErr
}

func (m *mockNode) CurrentProtocolParams() (
	ProtocolParamsInfo, error,
) {
	return m.params, m.paramsErr
}

func (m *mockNode) EpochProtocolParams(
	_ uint64,
) (ProtocolParamsInfo, error) {
	return m.epochParams, m.epochParamsErr
}

func (m *mockNode) Network(
	_ context.Context,
) (NetworkInfo, error) {
	return m.network, m.networkErr
}

func (m *mockNode) NetworkEras() (
	[]NetworkEraInfo, error,
) {
	return m.networkEras, m.networkErasErr
}

func (m *mockNode) Genesis() (
	GenesisInfo, error,
) {
	return m.genesis, m.genesisErr
}

func (m *mockNode) PoolsExtended(
	_ context.Context,
	params PaginationParams,
) (
	[]PoolExtendedInfo, int, error,
) {
	pools := slices.Clone(m.pools)
	slices.SortFunc(pools, func(a, b PoolExtendedInfo) int {
		return strings.Compare(a.Hex, b.Hex)
	})
	if params.Order == PaginationOrderDesc {
		slices.Reverse(pools)
	}
	offset, ok := paginationOffset(params)
	if !ok || offset >= len(pools) {
		return []PoolExtendedInfo{}, len(pools), m.poolsErr
	}
	return pools[offset:min(offset+params.Count, len(pools))], len(pools), m.poolsErr
}

func (m *mockNode) PoolsList(ctx context.Context,
	params PaginationParams,
) ([]string, int, error) {
	m.poolsListParams = params
	return m.poolsList, m.poolsListTotal, m.poolsListErr
}

func (m *mockNode) PoolsRetiring(
	_ context.Context,
	_ PaginationParams,
) ([]PoolRetiringInfo, int, error) {
	return m.poolsRetiring, m.poolsRetiringTotal, m.poolsRetiringErr
}

func (m *mockNode) PoolMetadata(
	_ string,
) (PoolMetadataInfo, error) {
	return m.poolMetadata, m.poolMetadataErr
}

func (m *mockNode) PoolDetail(ctx context.Context,
	_ string,
) (PoolDetailInfo, error) {
	return m.poolDetail, m.poolDetailErr
}

func (m *mockNode) Asset(
	_ context.Context,
	_ string,
	_ []byte,
) (AssetInfo, error) {
	return m.asset, m.assetErr
}

func (m *mockNode) AssetAddresses(
	_ context.Context,
	policyID string,
	assetName []byte,
	params PaginationParams,
) ([]AssetHolderInfo, int, error) {
	m.assetAddressesPolicyID = policyID
	m.assetAddressesAssetName = assetName
	m.assetAddressesParams = params
	return m.assetHolders, m.assetHoldersTotal, m.assetAddressesErr
}

func (m *mockNode) DRep(
	_ context.Context,
	credential DRepCredential,
) (DRepInfo, error) {
	m.drepCredential = credential
	return m.drep, m.drepErr
}

func (m *mockNode) DReps(
	_ context.Context,
	params DRepListParams,
) ([]DRepListItemInfo, int, error) {
	m.drepListParams = params
	return m.drepList, m.drepListTotal, m.drepListErr
}

func (m *mockNode) Address(
	_ context.Context,
	_ string,
) (AddressInfo, error) {
	return m.addressInfo, m.addressInfoErr
}

func (m *mockNode) AddressUTXOs(
	_ context.Context,
	_ string,
	_ PaginationParams,
) ([]AddressUTXOInfo, int, error) {
	return m.addressUTXOs, m.addressUTXOsTotal, m.addressUTXOsErr
}

func (m *mockNode) AddressTransactions(
	_ context.Context,
	_ string,
	params TransactionRangeParams,
) ([]AddressTransactionInfo, int, error) {
	m.lastAddressTransactionsParams = params
	return m.addressTransactions, m.addressTxsTotal, m.addressTransactionsErr
}

func (m *mockNode) MetadataTransactions(
	ctx context.Context,
	_ uint64,
	_ PaginationParams,
) ([]MetadataTransactionJSONInfo, int, error) {
	m.metadataCtx = ctx
	return m.metadataJSON, m.metadataJSONTotal, m.metadataJSONErr
}

func (m *mockNode) MetadataTransactionsCBOR(
	ctx context.Context,
	_ uint64,
	_ PaginationParams,
) ([]MetadataTransactionCBORInfo, int, error) {
	m.metadataCtx = ctx
	return m.metadataCBOR, m.metadataCBORTotal, m.metadataCBORErr
}

func (m *mockNode) Transaction(
	ctx context.Context,
	_ []byte,
) (TransactionInfo, error) {
	m.transactionCtx = ctx
	return m.transaction, m.transactionErr
}

func (m *mockNode) TransactionSubmit(
	_ context.Context,
	_ []byte,
) (string, error) {
	return m.transactionSubmitHash, m.transactionSubmitErr
}

func (m *mockNode) TransactionEvaluate(
	ctx context.Context,
	txCbor []byte,
) (TransactionEvaluationResponse, error) {
	m.transactionEvaluationCtx = ctx
	m.transactionEvaluateCbor = txCbor
	return m.transactionEvaluation, m.transactionEvaluationErr
}

func (m *mockNode) TransactionCBOR(
	_ context.Context,
	_ []byte,
) ([]byte, error) {
	return m.transactionCBOR, m.transactionCBORErr
}

func (m *mockNode) TransactionMetadata(
	_ context.Context,
	_ []byte,
) ([]TransactionMetadataInfo, error) {
	return m.transactionMetadata, m.transactionMetadataErr
}

func (m *mockNode) TransactionMetadataCBOR(
	_ context.Context,
	_ []byte,
) ([]TransactionMetadataCBORInfo, error) {
	return m.transactionMetadataCBOR, m.transactionMetadataCBORErr
}

func (m *mockNode) TransactionUTXOs(
	_ context.Context,
	_ []byte,
) (TransactionUTXOsInfo, error) {
	return m.transactionUTXOs, m.transactionUTXOsErr
}

func (m *mockNode) TransactionDelegations(
	_ context.Context,
	_ []byte,
) ([]TransactionDelegationInfo, error) {
	return m.transactionDelegations, m.transactionDelegationsErr
}

func (m *mockNode) TransactionStakeAddresses(
	_ context.Context,
	_ []byte,
) ([]TransactionStakeAddressInfo, error) {
	return m.transactionStakes, m.transactionStakesErr
}

func (m *mockNode) TransactionWithdrawals(
	_ context.Context,
	_ []byte,
) ([]TransactionWithdrawalInfo, error) {
	return m.transactionWithdrawals, m.transactionWithdrawalsErr
}

func (m *mockNode) TransactionMIRs(
	_ context.Context,
	_ []byte,
) ([]TransactionMIRInfo, error) {
	return m.transactionMIRs, m.transactionMIRsErr
}

func (m *mockNode) TransactionPoolUpdates(
	_ context.Context,
	_ []byte,
) ([]TransactionPoolUpdateInfo, error) {
	return m.transactionPoolUpdates, m.transactionPoolUpdatesErr
}

func (m *mockNode) TransactionPoolRetires(
	_ context.Context,
	_ []byte,
) ([]TransactionPoolRetireInfo, error) {
	return m.transactionPoolRetires, m.transactionPoolRetiresErr
}

func (m *mockNode) TransactionRedeemers(
	_ context.Context,
	_ []byte,
) ([]TransactionRedeemerInfo, error) {
	return m.transactionRedeemers, m.transactionRedeemersErr
}

func (m *mockNode) TransactionRequiredSigners(
	_ context.Context,
	_ []byte,
) ([]TransactionRequiredSignerInfo, error) {
	return m.transactionRequiredSigners, m.transactionRequiredSignersErr
}

func (m *mockNode) Account(
	_ context.Context,
	_ string,
) (AccountInfo, error) {
	return m.account, m.accountErr
}

func (m *mockNode) AccountAssociatedAddresses(ctx context.Context,
	_ string,
	params PaginationParams,
) ([]AccountAssociatedAddressInfo, int, error) {
	items := append([]AccountAssociatedAddressInfo(nil), m.addresses...)
	total := len(items)
	if params.Order == PaginationOrderDesc {
		for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
			items[i], items[j] = items[j], items[i]
		}
	}
	start := (params.Page - 1) * params.Count
	if start >= total {
		return []AccountAssociatedAddressInfo{}, total, m.addressesErr
	}
	end := min(start+params.Count, total)
	return items[start:end], total, m.addressesErr
}

func (m *mockNode) AccountDelegationHistory(ctx context.Context,
	_ string,
	params PaginationParams,
) ([]AccountDelegationHistoryInfo, int, error) {
	items := append([]AccountDelegationHistoryInfo(nil), m.delegations...)
	total := len(items)
	if params.Order == PaginationOrderDesc {
		for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
			items[i], items[j] = items[j], items[i]
		}
	}
	start := (params.Page - 1) * params.Count
	if start >= total {
		return []AccountDelegationHistoryInfo{}, total, m.delegationsErr
	}
	end := min(start+params.Count, total)
	return items[start:end], total, m.delegationsErr
}

func (m *mockNode) AccountRegistrationHistory(ctx context.Context,
	_ string,
	params PaginationParams,
) ([]AccountRegistrationHistoryInfo, int, error) {
	items := append([]AccountRegistrationHistoryInfo(nil), m.regs...)
	total := len(items)
	if params.Order == PaginationOrderDesc {
		for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
			items[i], items[j] = items[j], items[i]
		}
	}
	start := (params.Page - 1) * params.Count
	if start >= total {
		return []AccountRegistrationHistoryInfo{}, total, m.regsErr
	}
	end := min(start+params.Count, total)
	return items[start:end], total, m.regsErr
}

func (m *mockNode) AccountRewardHistory(ctx context.Context,
	_ string,
	params PaginationParams,
) ([]AccountRewardHistoryInfo, int, error) {
	items := append([]AccountRewardHistoryInfo(nil), m.rewards...)
	total := len(items)
	if params.Order == PaginationOrderDesc {
		for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
			items[i], items[j] = items[j], items[i]
		}
	}
	start := (params.Page - 1) * params.Count
	if start >= total {
		return []AccountRewardHistoryInfo{}, total, m.rewardsErr
	}
	end := min(start+params.Count, total)
	return items[start:end], total, m.rewardsErr
}

// mockPage applies the same in-memory ordering/pagination every
// mockNode paginated-list method needs: reverse for desc, then slice out
// the requested page. It only handles the shared count/page/order
// mechanics; each caller still owns picking its own backing slice and
// error field, since those differ per endpoint.
func mockPage[T any](items []T, order string, page, count int) ([]T, int) {
	items = append([]T(nil), items...)
	total := len(items)
	if order == PaginationOrderDesc {
		for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
			items[i], items[j] = items[j], items[i]
		}
	}
	start := (page - 1) * count
	if start >= total {
		return []T{}, total
	}
	end := min(start+count, total)
	return items[start:end], total
}

func (m *mockNode) AccountUTXOs(ctx context.Context,
	_ string,
	params PaginationParams,
) ([]AccountUTXOInfo, int, error) {
	rows, total := mockPage(
		m.accountUTXOs, params.Order, params.Page, params.Count,
	)
	return rows, total, m.accountUTXOsErr
}

func (m *mockNode) AccountWithdrawals(ctx context.Context,
	_ string,
	params PaginationParams,
) ([]AccountWithdrawalInfo, int, error) {
	rows, total := mockPage(
		m.accountWithdrawals, params.Order, params.Page, params.Count,
	)
	return rows, total, m.accountWithdrawalsErr
}

func (m *mockNode) AccountTransactions(ctx context.Context,
	_ string,
	params TransactionRangeParams,
) ([]AccountTransactionInfo, int, error) {
	m.lastAccountTransactionsParams = params
	rows, total := mockPage(
		m.accountTransactions,
		params.Pagination.Order,
		params.Pagination.Page,
		params.Pagination.Count,
	)
	return rows, total, m.accountTransactionsErr
}

func newTestBlockfrost(
	node BlockfrostNode,
) *Blockfrost {
	return New(
		BlockfrostConfig{
			ListenAddress: "127.0.0.1:0",
		},
		node,
		slog.Default(),
	)
}

func TestStartStop(t *testing.T) {
	t.Parallel()

	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	err := b.Start(t.Context())
	require.NoError(t, err)

	// Verify server is running
	assert.NotNil(t, b.listener.Server())

	// Stop the server
	stopCtx, stopCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer stopCancel()
	err = b.Stop(stopCtx)
	require.NoError(t, err)

	// Verify server is stopped
	assert.Nil(t, b.listener.Server())
}

func TestStartAlreadyStarted(t *testing.T) {
	t.Parallel()

	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	ctx := t.Context()
	err := b.Start(ctx)
	require.NoError(t, err)
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = b.Stop(stopCtx)
	}()

	// Starting again should error
	err = b.Start(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already started")
}

func TestRouterRootServesRootDocument(t *testing.T) {
	t.Parallel()

	mock := &mockNode{}
	b := newTestBlockfrost(mock)
	handler := b.handler()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp RootResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "https://blockfrost.io/", resp.URL)
	assert.Equal(t, "0.1.0", resp.Version)
}

func TestRouterUnknownRouteReturns404(t *testing.T) {
	t.Parallel()

	mock := &mockNode{}
	b := newTestBlockfrost(mock)
	handler := b.handler()

	paths := []string{
		"/does-not-exist",
		"/api/v0/not-an-operation",
		"/api/v0/scripts/hash/unknown",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			assert.Equal(t, http.StatusNotFound, w.Code)
			var resp ErrorResponse
			err := json.NewDecoder(w.Body).Decode(&resp)
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, resp.StatusCode)
			assert.Equal(t, "Not Found", resp.Error)
		})
	}
}

func TestHandleHealth(t *testing.T) {
	t.Parallel()

	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet, "/health", nil,
	)
	w := httptest.NewRecorder()
	b.handleHealth(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp HealthResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.IsHealthy)
}

func TestHandleBlockNotFound(t *testing.T) {
	t.Parallel()

	mock := &mockNode{blockByIDErr: ErrBlockNotFound}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/blocks/999999",
		nil,
	)
	req.SetPathValue("hash_or_number", "999999")
	w := httptest.NewRecorder()
	b.handleBlock(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleAssetInvalidIdentifier(t *testing.T) {
	t.Parallel()

	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/assets/not-hex",
		nil,
	)
	req.SetPathValue("asset", "not-hex")
	w := httptest.NewRecorder()
	b.handleAsset(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "Bad Request", resp.Error)
	assert.Equal(t, "Invalid asset identifier.", resp.Message)
}

func TestHandleAssetNotFound(t *testing.T) {
	t.Parallel()

	mock := &mockNode{
		assetErr: ErrAssetNotFound,
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/assets/00112233445566778899aabbccddeeff00112233445566778899aabb",
		nil,
	)
	req.SetPathValue(
		"asset",
		"00112233445566778899aabbccddeeff00112233445566778899aabb",
	)
	w := httptest.NewRecorder()
	b.handleAsset(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "Not Found", resp.Error)
	assert.Equal(t, "The requested asset could not be found.", resp.Message)
}

func TestHandleAssetAddresses(t *testing.T) {
	t.Parallel()

	const assetID = "00112233445566778899aabbccddeeff00112233445566778899aabb746f6b656e"
	mock := &mockNode{
		assetHolders: []AssetHolderInfo{
			{Address: "addr1holder1", Quantity: "1"},
			{Address: "addr1holder2", Quantity: "42"},
		},
		assetHoldersTotal: 12,
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/assets/"+assetID+"/addresses?count=2&page=3&order=desc",
		nil,
	)
	req.SetPathValue("asset", assetID)
	w := httptest.NewRecorder()
	b.handleAssetAddresses(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(
		t,
		"00112233445566778899aabbccddeeff00112233445566778899aabb",
		mock.assetAddressesPolicyID,
	)
	assert.Equal(t, []byte("token"), mock.assetAddressesAssetName)
	assert.Equal(
		t,
		PaginationParams{Count: 2, Page: 3, Order: "desc"},
		mock.assetAddressesParams,
	)
	assert.Equal(t, "12", w.Header().Get("X-Pagination-Count-Total"))
	assert.Equal(t, "6", w.Header().Get("X-Pagination-Page-Total"))

	var resp []AssetAddressResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 2)
	assert.Equal(t, "addr1holder1", resp[0].Address)
	assert.Equal(t, "1", resp[0].Quantity)
	assert.Equal(t, "addr1holder2", resp[1].Address)
	assert.Equal(t, "42", resp[1].Quantity)
}

func TestHandleAssetAddressesInvalidIdentifier(t *testing.T) {
	t.Parallel()

	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/assets/not-hex/addresses",
		nil,
	)
	req.SetPathValue("asset", "not-hex")
	w := httptest.NewRecorder()
	b.handleAssetAddresses(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "Bad Request", resp.Error)
	assert.Equal(t, "Invalid asset identifier.", resp.Message)
}

func TestHandleAssetAddressesNotFound(t *testing.T) {
	t.Parallel()

	const assetID = "00112233445566778899aabbccddeeff00112233445566778899aabb"
	mock := &mockNode{
		assetAddressesErr: ErrAssetNotFound,
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/assets/"+assetID+"/addresses",
		nil,
	)
	req.SetPathValue("asset", assetID)
	w := httptest.NewRecorder()
	b.handleAssetAddresses(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "Not Found", resp.Error)
	assert.Equal(t, "The requested asset could not be found.", resp.Message)
}

func TestHandleAssetAddressesNoHolders(t *testing.T) {
	t.Parallel()

	const assetID = "00112233445566778899aabbccddeeff00112233445566778899aabb"
	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/assets/"+assetID+"/addresses",
		nil,
	)
	req.SetPathValue("asset", assetID)
	w := httptest.NewRecorder()
	b.handleAssetAddresses(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "Not Found", resp.Error)
	assert.Equal(t, "The requested asset could not be found.", resp.Message)
}

func TestHandlePoolsRetiringInvalidPagination(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{})
	req := httptest.NewRequest(
		http.MethodGet, "/api/v0/pools/retiring?order=a", nil,
	)
	w := httptest.NewRecorder()
	b.handlePoolsRetiring(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(
		t,
		"querystring/order must be equal to one of the allowed values",
		resp.Message,
	)
}

func TestHandlePoolMetadataNoAnchor(t *testing.T) {
	t.Parallel()

	// A pool without registered metadata answers with an empty JSON
	// object, matching hosted Blockfrost.
	b := newTestBlockfrost(&mockNode{
		poolMetadata: PoolMetadataInfo{
			PoolID: "pool1vzqtn3mtfvvuy8ghksy34gs9g97tszj5f8mr3sn7asy5vk577ec",
			Hex:    "6080b9c76b4b19c21d17b4091aa205417cb80a5449f638c27eec0946",
		},
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/pools/pool1vzqtn3mtfvvuy8ghksy34gs9g97tszj5f8mr3sn7asy5vk577ec/metadata",
		nil,
	)
	req.SetPathValue(
		"pool_id",
		"pool1vzqtn3mtfvvuy8ghksy34gs9g97tszj5f8mr3sn7asy5vk577ec",
	)
	w := httptest.NewRecorder()
	b.handlePoolMetadata(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, "{}", w.Body.String())
}

func TestOffchainFetchErrorClassification(t *testing.T) {
	t.Parallel()

	url := "https://example.com/meta.json"
	expected := bytes.Repeat([]byte{0x01}, 32)
	stale := bytes.Repeat([]byte{0x02}, 32)

	// Latest attempt failed with 404; the stale BodyHash from an
	// earlier hash-mismatch attempt must not win.
	err404 := offchainFetchError(
		"Pool",
		url,
		expected,
		&models.OffchainMetadata{
			LastError:      "unexpected HTTP status 404",
			LastHTTPStatus: 404,
			BodyHash:       stale,
		},
	)
	assert.Equal(t, "HTTP_RESPONSE_ERROR", err404.Code)
	assert.Contains(t, err404.Message, `404 "Not Found"`)

	mismatch := offchainFetchError(
		"Pool",
		url,
		expected,
		&models.OffchainMetadata{
			LastError:      models.OffchainFetchErrHashMismatch,
			LastHTTPStatus: 200,
			BodyHash:       stale,
		},
	)
	assert.Equal(t, "HASH_MISMATCH", mismatch.Code)
	assert.Contains(t, mismatch.Message, hex.EncodeToString(stale))

	size := offchainFetchError("Pool", url, expected, &models.OffchainMetadata{
		LastError:      models.OffchainFetchErrBodyTooLargePrefix + " 1048576 bytes",
		LastHTTPStatus: 200,
	})
	assert.Equal(t, "SIZE_EXCEEDED", size.Code)

	conn := offchainFetchError("Pool", url, expected, &models.OffchainMetadata{
		LastError: "dial tcp: connection refused",
	})
	assert.Equal(t, "CONNECTION_ERROR", conn.Code)
}

func TestHandlePoolMetadataInvalidID(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{poolMetadataErr: ErrInvalidPoolID})
	req := httptest.NewRequest(
		http.MethodGet, "/api/v0/pools/pool1stonks/metadata", nil,
	)
	req.SetPathValue("pool_id", "pool1stonks")
	w := httptest.NewRecorder()
	b.handlePoolMetadata(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "Invalid or malformed pool id format.", resp.Message)
}

func TestHandlePoolMetadataNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{
		poolMetadataErr: fmt.Errorf("get pool: %w", models.ErrPoolNotFound),
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/pools/pool1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq8a7a2d/metadata",
		nil,
	)
	req.SetPathValue(
		"pool_id",
		"pool1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq8a7a2d",
	)
	w := httptest.NewRecorder()
	b.handlePoolMetadata(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestParsePoolID(t *testing.T) {
	t.Parallel()

	hash, err := parsePoolID(
		"pool1vzqtn3mtfvvuy8ghksy34gs9g97tszj5f8mr3sn7asy5vk577ec",
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		"6080b9c76b4b19c21d17b4091aa205417cb80a5449f638c27eec0946",
		hex.EncodeToString(hash),
	)

	hash2, err := parsePoolID(
		"6080b9c76b4b19c21d17b4091aa205417cb80a5449f638c27eec0946",
	)
	require.NoError(t, err)
	assert.Equal(t, hash, hash2)

	_, err = parsePoolID("pool1stonks")
	require.Error(t, err)

	_, err = parsePoolID(
		"drep1ygqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq7vlc9n",
	)
	require.Error(t, err)
}

func TestHandleDReps(t *testing.T) {
	t.Parallel()

	lastActive := uint64(680)
	mock := &mockNode{
		drepListTotal: 42,
		drepList: []DRepListItemInfo{
			{
				DRepID:          "drep1y2zpa7e9f23g7snuhfqss73ufeshsqhdt4qqfu74p9y4kfqcpsxym",
				Hex:             "22841efb254aa28f427cba41087a3c4e617802ed5d4004f3d509495b24",
				Amount:          "1000000",
				HasScript:       false,
				Retired:         false,
				Expired:         true,
				LastActiveEpoch: &lastActive,
				Metadata: &DRepMetadataInfo{
					URL:          "https://example.com/drep.json",
					Hash:         "aa",
					JSONMetadata: json.RawMessage(`{"a":1}`),
					Bytes:        "\\x7b2261223a317d",
				},
			},
			{
				DRepID: "drep1yvvxuvh64q9zdqgrjt76d42eclk5wgdxtnsun4808cwg0dquhj65s",
				Hex:    "23186e32faa80a26810392fda6d559c7ed4721a65ce1c9d4ef3e1c87b4",
				Amount: "0", HasScript: true, Retired: true,
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/governance/dreps?count=2&order_by=amount&order=desc&retired=false",
		nil,
	)
	w := httptest.NewRecorder()
	b.handleDReps(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "42", w.Header().Get("X-Pagination-Count-Total"))
	assert.True(t, mock.drepListParams.OrderByAmount)
	require.NotNil(t, mock.drepListParams.Retired)
	assert.False(t, *mock.drepListParams.Retired)
	assert.Nil(t, mock.drepListParams.Expired)
	assert.Equal(t, PaginationOrderDesc, mock.drepListParams.Pagination.Order)

	var resp []DRepListItemResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 2)
	assert.Equal(t, "1000000", resp[0].Amount)
	require.NotNil(t, resp[0].Metadata)
	assert.Equal(t, "https://example.com/drep.json", resp[0].Metadata.URL)
	assert.Nil(t, resp[1].Metadata)
	assert.True(t, resp[1].Retired)
}

func TestHandleDRepsInvalidParams(t *testing.T) {
	t.Parallel()

	cases := []struct {
		query   string
		message string
	}{
		{
			"order=a",
			"querystring/order must be equal to one of the allowed values",
		},
		{"page=x", "querystring/page must be integer"},
		{"page=0", "querystring/page must be >= 1"},
		{"page=99999999999999", "querystring/page must be <= 21474836"},
		{"count=x", "querystring/count must be integer"},
		{"count=999999999999999", "querystring/count must be <= 100"},
		{"count=0", "querystring/count must be >= 1"},
		{
			"order_by=bogus",
			"querystring/order_by must be equal to one of the allowed values",
		},
		{"retired=x", "querystring/retired must be boolean"},
		{"expired=x", "querystring/expired must be boolean"},
	}
	b := newTestBlockfrost(&mockNode{})
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			req := httptest.NewRequest(
				http.MethodGet,
				"/api/v0/governance/dreps?"+tc.query,
				nil,
			)
			w := httptest.NewRecorder()
			b.handleDReps(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code, tc.query)
			var resp ErrorResponse
			require.NoError(
				t,
				json.NewDecoder(w.Body).Decode(&resp),
				tc.query,
			)
			assert.Equal(t, tc.message, resp.Message, tc.query)
		})
	}
}

// TestNodeAdapterDRepsEpochForSlot pins the slot-to-epoch resolution behind a
// DRep's registration epoch, which is the reported last-active epoch when the
// DRep has no recorded activity: the epoch is the latest one whose start slot
// is at or before the registration slot, and a slot equal to a start slot
// belongs to the epoch it starts.
func TestNodeAdapterDRepsEpochForSlot(t *testing.T) {
	t.Parallel()

	adapter, store, _ := newDBBackedAdapter(t)

	for _, epoch := range []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 100},
		{EpochId: 1, StartSlot: 100, LengthInSlots: 100},
		{EpochId: 2, StartSlot: 200, LengthInSlots: 100},
	} {
		_, err := store.Exec(`
INSERT INTO epoch (epoch_id, start_slot, length_in_slots)
VALUES (?, ?, ?)`,
			epoch.EpochId,
			epoch.StartSlot,
			epoch.LengthInSlots,
		)
		require.NoError(t, err)
	}

	// LastActivityEpoch 0 makes the reported last-active epoch fall back
	// to the registration epoch derived from added_slot.
	for _, drep := range []models.Drep{
		{Credential: bytes.Repeat([]byte{0x01}, 28), AddedSlot: 150, Active: true},
		{Credential: bytes.Repeat([]byte{0x02}, 28), AddedSlot: 200, Active: true},
		{Credential: bytes.Repeat([]byte{0x03}, 28), AddedSlot: 250, Active: true},
	} {
		_, err := store.Exec(`
INSERT INTO drep (
    credential, credential_tag, added_slot, last_activity_epoch, active
) VALUES (?, ?, ?, ?, ?)`,
			drep.Credential,
			drep.CredentialTag,
			drep.AddedSlot,
			drep.LastActivityEpoch,
			drep.Active,
		)
		require.NoError(t, err)
	}

	items, total, err := adapter.DReps(context.Background(), DRepListParams{
		Pagination: PaginationParams{
			Count: 100,
			Page:  1,
			Order: PaginationOrderAsc,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	require.Len(t, items, 3)
	// Rows are ordered by first on-chain appearance: slots 150, 200, 250.
	wantEpochs := []uint64{1, 2, 2}
	for i, item := range items {
		require.NotNil(t, item.LastActiveEpoch, i)
		assert.Equal(t, wantEpochs[i], *item.LastActiveEpoch, i)
		assert.False(t, item.Retired, i)
		assert.False(t, item.Expired, i)
	}
}

func TestParseDRepIdentifierSpecial(t *testing.T) {
	t.Parallel()

	cred, err := parseDRepIdentifier("drep_always_abstain")
	require.NoError(t, err)
	require.NotNil(t, cred.Predefined)
	assert.Equal(t, models.DrepTypeAlwaysAbstain, *cred.Predefined)

	cred, err = parseDRepIdentifier("drep_always_no_confidence")
	require.NoError(t, err)
	require.NotNil(t, cred.Predefined)
	assert.Equal(t, models.DrepTypeAlwaysNoConfidence, *cred.Predefined)
}

// TestHandleDRepCIP129ScriptIdentifier verifies script DRep IDs keep their
// credential type when the HTTP handler passes them to the node adapter.
func TestHandleDRepCIP129ScriptIdentifier(t *testing.T) {
	t.Parallel()

	const drepID = "drep1yvqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq770f95"
	const drepHex = "00000000000000000000000000000000000000000000000000000000"

	mock := &mockNode{
		drep: DRepInfo{
			DRepID:    drepID,
			Hex:       drepHex,
			HasScript: true,
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/governance/dreps/"+drepID,
		nil,
	)
	req.SetPathValue("drep_id", drepID)
	w := httptest.NewRecorder()
	b.handleDRep(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, mock.drepCredential.HasScript)
	assert.True(t, mock.drepCredential.CredentialTagKnown)
	assert.Equal(
		t,
		make([]byte, drepCredentialHashLen),
		mock.drepCredential.Hash,
	)
}

// TestParseDRepIdentifierCIP129CredentialType verifies CIP-129 DRep IDs
// decode the credential type from the bech32 payload header.
func TestParseDRepIdentifierCIP129CredentialType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		id            string
		wantHasScript bool
	}{
		{
			name:          "key",
			id:            "drep1ygqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq7vlc9n",
			wantHasScript: false,
		},
		{
			name:          "script",
			id:            "drep1yvqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq770f95",
			wantHasScript: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			credential, err := parseDRepIdentifier(tc.id)
			require.NoError(t, err)
			assert.Equal(t, tc.id, credential.ID)
			assert.Equal(
				t,
				make([]byte, drepCredentialHashLen),
				credential.Hash,
			)
			assert.Equal(t, tc.wantHasScript, credential.HasScript)
			assert.True(t, credential.CredentialTagKnown)
		})
	}
}

// TestParseDRepIdentifierHexIsAmbiguous verifies raw hash identifiers remain
// untyped so lookup can deliberately fall back across key/script tags.
func TestParseDRepIdentifierHexIsAmbiguous(t *testing.T) {
	t.Parallel()

	const drepHex = "00000000000000000000000000000000000000000000000000000000"

	credential, err := parseDRepIdentifier(drepHex)
	require.NoError(t, err)
	assert.Equal(t, drepHex, credential.ID)
	assert.Equal(t, make([]byte, drepCredentialHashLen), credential.Hash)
	assert.False(t, credential.HasScript)
	assert.False(t, credential.CredentialTagKnown)
}

func TestHandleDRepInvalidIdentifier(t *testing.T) {
	t.Parallel()

	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/governance/dreps/not-a-drep",
		nil,
	)
	req.SetPathValue("drep_id", "not-a-drep")
	w := httptest.NewRecorder()
	b.handleDRep(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "Bad Request", resp.Error)
	assert.Equal(t, "Invalid DRep identifier.", resp.Message)
}

func TestHandleDRepNotFound(t *testing.T) {
	t.Parallel()

	mock := &mockNode{
		drepErr: ErrDRepNotFound,
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/governance/dreps/00000000000000000000000000000000000000000000000000000000",
		nil,
	)
	req.SetPathValue(
		"drep_id",
		"00000000000000000000000000000000000000000000000000000000",
	)
	w := httptest.NewRecorder()
	b.handleDRep(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "Not Found", resp.Error)
	assert.Equal(t, "The requested DRep could not be found.", resp.Message)
}

func TestHandleLatestBlockError(t *testing.T) {
	t.Parallel()

	mock := &mockNode{
		blockErr: assert.AnError,
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/blocks/latest",
		nil,
	)
	w := httptest.NewRecorder()
	b.handleLatestBlock(w, req)

	assert.Equal(
		t,
		http.StatusInternalServerError,
		w.Code,
	)

	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, 500, resp.StatusCode)
	assert.Equal(
		t,
		"Internal Server Error",
		resp.Error,
	)
}

func TestHandlePoolsExtendedInvalidPagination(t *testing.T) {
	t.Parallel()

	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/pools/extended?count=abc",
		nil,
	)
	w := httptest.NewRecorder()
	b.handlePoolsExtended(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, 400, resp.StatusCode)
	assert.Equal(t, "Bad Request", resp.Error)
	assert.Equal(
		t,
		"Invalid pagination parameters.",
		resp.Message,
	)
}

func TestHandlePoolsExtendedError(t *testing.T) {
	t.Parallel()

	mock := &mockNode{
		poolsErr: assert.AnError,
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/pools/extended",
		nil,
	)
	w := httptest.NewRecorder()
	b.handlePoolsExtended(w, req)

	assert.Equal(
		t,
		http.StatusInternalServerError,
		w.Code,
	)

	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, 500, resp.StatusCode)
	assert.Equal(
		t,
		"Internal Server Error",
		resp.Error,
	)
}

func TestHandleTransaction(t *testing.T) {
	t.Parallel()

	invalidBefore := "100"
	invalidHereafter := "200"
	mock := &mockNode{
		transaction: TransactionInfo{
			Hash:        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Block:       "blockhash1",
			Slot:        123,
			BlockHeight: 45,
			BlockTime:   1700000000,
			Index:       2,
			OutputAmount: []AddressAmountInfo{
				{Unit: "lovelace", Quantity: "5000"},
			},
			Fees:             "170000",
			Deposit:          "0",
			TreasuryDonation: "12345",
			Size:             512,
			UtxoCount:        3,
			StakeCertCount:   1,
			DelegationCount:  1,
			RedeemerCount:    2,
			ValidContract:    true,
			InvalidBefore:    &invalidBefore,
			InvalidHereafter: &invalidHereafter,
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/txs/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		nil,
	)
	req.SetPathValue(
		"hash",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	)
	requestCtx, cancel := context.WithCancel(req.Context())
	cancel()
	req = req.WithContext(requestCtx)
	w := httptest.NewRecorder()
	b.handleTransaction(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Same(t, requestCtx, mock.transactionCtx)

	var resp TransactionResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, mock.transaction.Hash, resp.Hash)
	assert.Equal(t, "blockhash1", resp.Block)
	assert.Equal(t, uint64(123), resp.Slot)
	assert.Equal(t, uint64(45), resp.BlockHeight)
	assert.Equal(t, int64(1700000000), resp.BlockTime)
	assert.Equal(t, 2, resp.Index)
	assert.Equal(t, "170000", resp.Fees)
	assert.Equal(t, "12345", resp.TreasuryDonation)
	require.Len(t, resp.OutputAmount, 1)
	assert.Equal(t, "lovelace", resp.OutputAmount[0].Unit)
	assert.Equal(t, "5000", resp.OutputAmount[0].Quantity)
	require.NotNil(t, resp.InvalidBefore)
	assert.Equal(t, "100", *resp.InvalidBefore)
	require.NotNil(t, resp.InvalidHereafter)
	assert.Equal(t, "200", *resp.InvalidHereafter)
}

func TestHandleTransactionInvalidHash(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/txs/not-a-hash",
		nil,
	)
	req.SetPathValue("hash", "not-a-hash")
	w := httptest.NewRecorder()
	b.handleTransaction(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "Invalid transaction hash.", resp.Message)
}

func TestHandleTransactionNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{
		transactionErr: ErrTransactionNotFound,
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/txs/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		nil,
	)
	req.SetPathValue(
		"hash",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	)
	w := httptest.NewRecorder()
	b.handleTransaction(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "Not Found", resp.Error)
}

func TestHandleTransactionSubmitMempoolUnavailable(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{
		transactionSubmitErr: ErrMempoolUnavailable,
	})
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v0/tx/submit",
		strings.NewReader("\x84\x00"),
	)
	req.Header.Set("Content-Type", "application/cbor")
	w := httptest.NewRecorder()
	b.handleTransactionSubmit(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, "Service Unavailable", resp.Error)
	assert.Equal(t, "mempool unavailable", resp.Message)
}

func TestHandleTransactionSubmitMempoolFull(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{
		transactionSubmitErr: ErrMempoolFull,
	})
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v0/tx/submit",
		strings.NewReader("\x84\x00"),
	)
	req.Header.Set("Content-Type", "application/cbor")
	w := httptest.NewRecorder()
	b.handleTransactionSubmit(w, req)

	assert.Equal(t, 425, w.Code)
	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, 425, resp.StatusCode)
	assert.Equal(t, "Mempool Full", resp.Error)
	assert.Equal(t, "mempool is full, try again later", resp.Message)
}

func TestHandleTransactionSubmitErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		body        string
		nodeErr     error
		wantStatus  int
		wantError   string
		wantMessage string
	}{
		{
			name:        "unsupported content type",
			contentType: "application/json",
			body:        "\x84\x00",
			wantStatus:  http.StatusUnsupportedMediaType,
			wantError:   "Unsupported Media Type",
			wantMessage: "Content-Type must be application/cbor.",
		},
		{
			name:        "oversized body",
			contentType: "application/cbor",
			body:        strings.Repeat("x", maxTxBodySize+1),
			wantStatus:  http.StatusRequestEntityTooLarge,
			wantError:   "Request Entity Too Large",
			wantMessage: "transaction body exceeds maximum allowed size",
		},
		{
			name:        "empty body",
			contentType: "application/cbor",
			body:        "",
			wantStatus:  http.StatusBadRequest,
			wantError:   "Bad Request",
			wantMessage: "transaction body is empty",
		},
		{
			name:        "invalid cbor",
			contentType: "application/cbor",
			body:        "not-cbor",
			nodeErr:     ErrInvalidTransaction,
			wantStatus:  http.StatusBadRequest,
			wantError:   "Bad Request",
			wantMessage: "Invalid transaction CBOR.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newTestBlockfrost(&mockNode{
				transactionSubmitErr: tt.nodeErr,
			})
			req := httptest.NewRequest(
				http.MethodPost,
				"/api/v0/tx/submit",
				strings.NewReader(tt.body),
			)
			req.Header.Set("Content-Type", tt.contentType)
			w := httptest.NewRecorder()
			b.handleTransactionSubmit(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			var resp ErrorResponse
			err := json.NewDecoder(w.Body).Decode(&resp)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
			assert.Equal(t, tt.wantError, resp.Error)
			assert.Equal(t, tt.wantMessage, resp.Message)
		})
	}
}

func TestHandleTransactionSubEndpointNotFound(t *testing.T) {
	t.Parallel()

	const hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	tests := []struct {
		name      string
		path      string
		configure func(*mockNode)
		handler   func(*Blockfrost, http.ResponseWriter, *http.Request)
	}{
		{
			name: "cbor",
			path: "/api/v0/txs/" + hash + "/cbor",
			configure: func(m *mockNode) {
				m.transactionCBORErr = ErrTransactionNotFound
			},
			handler: (*Blockfrost).handleTransactionCBOR,
		},
		{
			name: "metadata",
			path: "/api/v0/txs/" + hash + "/metadata",
			configure: func(m *mockNode) {
				m.transactionMetadataErr = ErrTransactionNotFound
			},
			handler: (*Blockfrost).handleTransactionMetadata,
		},
		{
			name: "metadata cbor",
			path: "/api/v0/txs/" + hash + "/metadata/cbor",
			configure: func(m *mockNode) {
				m.transactionMetadataCBORErr = ErrTransactionNotFound
			},
			handler: (*Blockfrost).handleTransactionMetadataCBOR,
		},
		{
			name: "utxos",
			path: "/api/v0/txs/" + hash + "/utxos",
			configure: func(m *mockNode) {
				m.transactionUTXOsErr = ErrTransactionNotFound
			},
			handler: (*Blockfrost).handleTransactionUTXOs,
		},
		{
			name: "delegations",
			path: "/api/v0/txs/" + hash + "/delegations",
			configure: func(m *mockNode) {
				m.transactionDelegationsErr = ErrTransactionNotFound
			},
			handler: (*Blockfrost).handleTransactionDelegations,
		},
		{
			name: "stakes",
			path: "/api/v0/txs/" + hash + "/stakes",
			configure: func(m *mockNode) {
				m.transactionStakesErr = ErrTransactionNotFound
			},
			handler: (*Blockfrost).handleTransactionStakeAddresses,
		},
		{
			name: "withdrawals",
			path: "/api/v0/txs/" + hash + "/withdrawals",
			configure: func(m *mockNode) {
				m.transactionWithdrawalsErr = ErrTransactionNotFound
			},
			handler: (*Blockfrost).handleTransactionWithdrawals,
		},
		{
			name: "mirs",
			path: "/api/v0/txs/" + hash + "/mirs",
			configure: func(m *mockNode) {
				m.transactionMIRsErr = ErrTransactionNotFound
			},
			handler: (*Blockfrost).handleTransactionMIRs,
		},
		{
			name: "pool updates",
			path: "/api/v0/txs/" + hash + "/pool_updates",
			configure: func(m *mockNode) {
				m.transactionPoolUpdatesErr = ErrTransactionNotFound
			},
			handler: (*Blockfrost).handleTransactionPoolUpdates,
		},
		{
			name: "pool retires",
			path: "/api/v0/txs/" + hash + "/pool_retires",
			configure: func(m *mockNode) {
				m.transactionPoolRetiresErr = ErrTransactionNotFound
			},
			handler: (*Blockfrost).handleTransactionPoolRetires,
		},
		{
			name: "redeemers",
			path: "/api/v0/txs/" + hash + "/redeemers",
			configure: func(m *mockNode) {
				m.transactionRedeemersErr = ErrTransactionNotFound
			},
			handler: (*Blockfrost).handleTransactionRedeemers,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockNode{}
			tt.configure(mock)
			b := newTestBlockfrost(mock)
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			req.SetPathValue("hash", hash)
			w := httptest.NewRecorder()
			tt.handler(b, w, req)

			assert.Equal(t, http.StatusNotFound, w.Code)
			var resp ErrorResponse
			err := json.NewDecoder(w.Body).Decode(&resp)
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, resp.StatusCode)
			assert.Equal(t, "Not Found", resp.Error)
			assert.Equal(
				t,
				"The requested transaction could not be found.",
				resp.Message,
			)
		})
	}
}

func TestHandleEpochParamsInvalidEpoch(t *testing.T) {
	t.Parallel()

	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/epochs/not-a-number/parameters",
		nil,
	)
	req.SetPathValue("number", "not-a-number")
	w := httptest.NewRecorder()
	b.handleEpochParams(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, 400, resp.StatusCode)
	assert.Equal(t, "Bad Request", resp.Error)
	assert.Equal(t, "Invalid epoch number.", resp.Message)
}

func TestHandleEpochParamsNotFound(t *testing.T) {
	t.Parallel()

	mock := &mockNode{
		epochParamsErr: ErrEpochNotFound,
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/epochs/42/parameters",
		nil,
	)
	req.SetPathValue("number", "42")
	w := httptest.NewRecorder()
	b.handleEpochParams(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, 404, resp.StatusCode)
	assert.Equal(t, "Not Found", resp.Error)
}

// assertJSONKeys marshals v and asserts that the resulting JSON object's keys
// exactly match want. It is used to pin Blockfrost response structs to the
// field set defined by the Blockfrost OpenAPI schema.
func assertJSONKeys(t *testing.T, v any, want []string) {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	var obj map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &obj))
	got := make([]string, 0, len(obj))
	for k := range obj {
		got = append(got, k)
	}
	assert.ElementsMatch(t, want, got)
}

func TestHandleAccountInvalidStakeAddress(t *testing.T) {
	t.Parallel()

	mock := &mockNode{
		accountErr: ErrInvalidStakeAddress,
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/accounts/invalid",
		nil,
	)
	req.SetPathValue("stake_address", "invalid")
	w := httptest.NewRecorder()
	b.handleAccount(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, 400, resp.StatusCode)
	assert.Equal(t, "Invalid stake address.", resp.Message)
}

func TestHandleAccountNotFound(t *testing.T) {
	t.Parallel()

	mock := &mockNode{
		accountErr: models.ErrAccountNotFound,
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/accounts/stake_missing",
		nil,
	)
	req.SetPathValue("stake_address", "stake_missing")
	w := httptest.NewRecorder()
	b.handleAccount(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestStopIdempotent(t *testing.T) {
	t.Parallel()

	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	// Stop without starting should not error
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()
	err := b.Stop(ctx)
	require.NoError(t, err)
}

func TestParseAddressOrPaymentCred(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		// wantType is asserted when non-nil.
		wantType *uint8
		// wantStakeAddress is asserted when non-empty.
		wantStakeAddress string
		wantPaymentCred  bool
		// wantNilStakeAddress asserts StakeAddress() is nil.
		wantNilStakeAddress bool
		wantErr             bool
	}{
		{
			name:                "CIP-5 payment key hash credential",
			input:               "addr_vkh1x0ph3nhyrvhpttyy3alk78f00q244vfdjwm38h5f3kz47kpgum5",
			wantPaymentCred:     true,
			wantType:            new(uint8(lcommon.AddressTypeKeyNone)),
			wantNilStakeAddress: true,
		},
		{
			name:            "CIP-5 script hash credential",
			input:           "script1tx3c5y302fupjrm0xs3skumkaw97h2le97rjgrfzw8spydzr5ej",
			wantPaymentCred: true,
			wantType:        new(uint8(lcommon.AddressTypeScriptNone)),
		},
		{
			// Full base address still parses through the normal path.
			name:             "full base address",
			input:            "addr_test1qprwyxzmswhtjstxvj7epjc28gskffsqcxuurx80qrjdy3uayerntq74us2hsgxymgk3f5nka58z46zcqgctv9c05ctq8g0qn7",
			wantPaymentCred:  false,
			wantStakeAddress: "stake_test1uzwjv3e4s027g9tcyrzd5tg56fmw6r32apvqyv9kzu86v9sqrx6ye",
		},
		{
			// Garbage input fails despite decoding as base58 bytes.
			name:    "garbage input",
			input:   "addr1stonks",
			wantErr: true,
		},
		{
			// Stake addresses have no payment part and are rejected.
			name:    "stake address has no payment part",
			input:   "stake_test1uzwjv3e4s027g9tcyrzd5tg56fmw6r32apvqyv9kzu86v9sqrx6ye",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			addr, isPaymentCred, err := parseAddressOrPaymentCred(
				tc.input,
			)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantPaymentCred, isPaymentCred)
			if tc.wantType != nil {
				assert.Equal(t, *tc.wantType, addr.Type())
			}
			if tc.wantNilStakeAddress {
				assert.Nil(t, addr.StakeAddress())
			}
			if tc.wantStakeAddress != "" {
				require.NotNil(t, addr.StakeAddress())
				assert.Equal(
					t,
					tc.wantStakeAddress,
					addr.StakeAddress().String(),
				)
			}
		})
	}
}

// TestNodeAdapterAddressRejectsInvalidInput covers NodeAdapter.Address's
// rejection paths: the network guard (queries match on bare credential hashes,
// so a foreign-network address must not answer with this network's balances)
// and the parse-failure path, which must keep ErrInvalidAddress for the HTTP
// 400 mapping while retaining the underlying parse diagnostics.
func TestNodeAdapterAddressRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	adapter, _, _ := newDBBackedAdapter(t)
	paymentKey := bytes.Repeat([]byte{0x11}, lcommon.AddressHashSize)

	t.Run("foreign network", func(t *testing.T) {
		// newDBBackedAdapter supplies no CardanoNodeConfig, so the
		// adapter's network is testnet; a mainnet-encoded address is
		// foreign to it.
		mainnetAddr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyNone,
			lcommon.AddressNetworkMainnet,
			paymentKey,
			nil,
		)
		require.NoError(t, err)

		_, err = adapter.Address(context.Background(), mainnetAddr.String())
		require.ErrorIs(t, err, ErrInvalidAddress)
		assert.ErrorContains(t, err, "network mismatch")
	})

	t.Run("parse failure keeps diagnostics", func(t *testing.T) {
		_, err := adapter.Address(context.Background(), "addr1stonks")
		require.ErrorIs(t, err, ErrInvalidAddress,
			"the HTTP layer maps this sentinel to 400")
		// Which diagnostic comes back depends on the first check in the
		// decoder that rejects the input, and that moves as the decoder
		// gains validation: gouroboros v0.191.x began rejecting the bad
		// network ID before the round-trip check could run. What has to
		// hold is that some diagnostic survives rather than being
		// flattened into the bare sentinel, so assert that instead of
		// pinning one decoder's wording.
		// Asserting the input name or a length difference would prove
		// nothing: Adapter.Address always wraps as `parse address %q: %w: %w`,
		// so both hold whether or not the inner error survives. What
		// distinguishes them is whatever follows that wrapper, so strip the
		// part the wrapper guarantees and require something left over.
		prefix := fmt.Sprintf(
			"parse address %q: %s: ", "addr1stonks", ErrInvalidAddress,
		)
		require.True(t, strings.HasPrefix(err.Error(), prefix),
			"unexpected wrapper shape: %s", err)
		assert.NotEmpty(
			t,
			strings.TrimSpace(strings.TrimPrefix(err.Error(), prefix)),
			"the underlying parse error is retained, not discarded",
		)
	})

	t.Run("this network resolves to not found", func(t *testing.T) {
		// The same credential encoded for this network passes both
		// guards and reaches the lookup.
		testnetAddr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyNone,
			lcommon.AddressNetworkTestnet,
			paymentKey,
			nil,
		)
		require.NoError(t, err)
		_, err = adapter.Address(context.Background(), testnetAddr.String())
		require.ErrorIs(t, err, ErrAddressNotFound)
	})
}

// TestNodeAdapterAddressPaymentCredTransactionFallback covers the
// CountTransactionsByPaymentCred fallback: a bare payment credential whose
// UTxOs are all spent must still resolve with a zero balance, because the
// synthetic enterprise address alone would not match base-address history.
func TestNodeAdapterAddressPaymentCredTransactionFallback(t *testing.T) {
	t.Parallel()

	adapter, store, _ := newDBBackedAdapter(t)

	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)
	credID, err := bech32EncodeData("addr_vkh", paymentKey)
	require.NoError(t, err)

	// No UTxOs and no transaction history: the credential is unknown.
	_, err = adapter.Address(context.Background(), credID)
	require.ErrorIs(t, err, ErrAddressNotFound)

	// Spent history recorded against the payment credential (via a base
	// address, so the synthetic enterprise form never matches) resolves
	// with a zero lovelace balance.
	_, err = store.Exec(`
INSERT INTO address_transaction (
    payment_key, payment_script, staking_key, transaction_id, slot
) VALUES (?, FALSE, ?, ?, ?)`,
		paymentKey,
		bytes.Repeat([]byte{0x33}, lcommon.AddressHashSize),
		1,
		10,
	)
	require.NoError(t, err)

	info, err := adapter.Address(context.Background(), credID)
	require.NoError(t, err)
	assert.Equal(t, credID, info.Address)
	require.Len(t, info.Amount, 1)
	assert.Equal(t, "lovelace", info.Amount[0].Unit)
	assert.Equal(t, "0", info.Amount[0].Quantity)
	assert.Equal(t, "shelley", info.Type)
	assert.False(t, info.Script)
	assert.Nil(t, info.StakeAddress)
}

func TestNodeAdapterKeyScriptStakeAddress(t *testing.T) {
	t.Parallel()

	adapter, store, db := newDBBackedAdapter(t)
	paymentHash := bytes.Repeat([]byte{0x01}, lcommon.AddressHashSize)
	stakeScriptHash := bytes.Repeat([]byte{0x02}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyScript,
		lcommon.AddressNetworkTestnet,
		paymentHash,
		stakeScriptHash,
	)
	require.NoError(t, err)
	utxo := models.Utxo{
		TxId:          fill32(0x31),
		PaymentKey:    paymentHash,
		StakingKey:    stakeScriptHash,
		CredentialTag: 1,
		AddedSlot:     10,
		Amount:        types.Uint64(1_000_000),
	}
	insertAdapterUtxo(t, store, &utxo)
	storePointerOutputCbor(t, db, utxo.TxId, 0, addr, 1_000_000)

	info, err := adapter.Address(context.Background(), addr.String())
	require.NoError(t, err)
	require.NotNil(t, info.StakeAddress)
	assert.True(t, strings.HasPrefix(*info.StakeAddress, "stake_test17"))

	stakeAddr, err := lcommon.NewAddress(*info.StakeAddress)
	require.NoError(t, err)
	assert.Equal(t, uint(lcommon.AddressNetworkTestnet), stakeAddr.NetworkId())
	assert.Equal(t, uint8(lcommon.AddressTypeNoneScript), stakeAddr.Type())
	assert.Equal(t, stakeScriptHash, stakeAddr.StakeKeyHash().Bytes())

	mainnetCfg, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		"mainnet/config.json",
	)
	require.NoError(t, err)
	mainnetAdapter, mainnetStore, mainnetDB := newDBBackedAdapter(t, mainnetCfg)
	mainnetAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyScript,
		lcommon.AddressNetworkMainnet,
		paymentHash,
		stakeScriptHash,
	)
	require.NoError(t, err)
	mainnetUtxo := utxo
	insertAdapterUtxo(t, mainnetStore, &mainnetUtxo)
	storePointerOutputCbor(t, mainnetDB, mainnetUtxo.TxId, 0, mainnetAddr, 1_000_000)

	mainnetInfo, err := mainnetAdapter.Address(
		context.Background(), mainnetAddr.String(),
	)
	require.NoError(t, err)
	require.NotNil(t, mainnetInfo.StakeAddress)
	assert.True(t, strings.HasPrefix(*mainnetInfo.StakeAddress, "stake17"))
	mainnetStakeAddr, err := lcommon.NewAddress(*mainnetInfo.StakeAddress)
	require.NoError(t, err)
	mainnetStakeBytes, err := mainnetStakeAddr.Bytes()
	require.NoError(t, err)
	assert.Equal(t, uint8(0xf1), mainnetStakeBytes[0])

	keyStakeHash := bytes.Repeat([]byte{0x03}, lcommon.AddressHashSize)
	keyAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		paymentHash,
		keyStakeHash,
	)
	require.NoError(t, err)
	keyUtxo := models.Utxo{
		TxId:          fill32(0x32),
		PaymentKey:    paymentHash,
		StakingKey:    keyStakeHash,
		CredentialTag: 0,
		AddedSlot:     10,
		Amount:        types.Uint64(1_000_000),
	}
	insertAdapterUtxo(t, store, &keyUtxo)
	storePointerOutputCbor(t, db, keyUtxo.TxId, 0, keyAddr, 1_000_000)

	keyInfo, err := adapter.Address(context.Background(), keyAddr.String())
	require.NoError(t, err)
	require.NotNil(t, keyInfo.StakeAddress)
	assert.True(t, strings.HasPrefix(*keyInfo.StakeAddress, "stake_test1u"))
	keyStakeAddr, err := lcommon.NewAddress(*keyInfo.StakeAddress)
	require.NoError(t, err)
	assert.Equal(t, uint8(lcommon.AddressTypeNoneKey), keyStakeAddr.Type())
	assert.Equal(t, keyStakeHash, keyStakeAddr.StakeKeyHash().Bytes())
}

func TestKeyScriptStakeAddressDerivation(t *testing.T) {
	// Keep direct coverage of the script-credential branch used when the
	// adapter derives a stake address from a base address.
	stakeScriptHash := bytes.Repeat([]byte{0x02}, lcommon.AddressHashSize)

	encoded, err := stakeAddressFromCredential(
		lcommon.Credential{
			CredType:   lcommon.CredentialTypeScriptHash,
			Credential: lcommon.CredentialHash(stakeScriptHash),
		},
		lcommon.AddressNetworkTestnet,
	)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(encoded, "stake_test17"))

	stakeAddr, err := lcommon.NewAddress(encoded)
	require.NoError(t, err)
	assert.Equal(t, stakeScriptHash, stakeAddr.StakeKeyHash().Bytes())
}

func TestHandleAddressNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{
		addressInfoErr: ErrAddressNotFound,
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/addresses/addr_test1vr8nl4...",
		nil,
	)
	req.SetPathValue("address", "addr_test1vr8nl4...")
	w := httptest.NewRecorder()
	b.handleAddress(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(
		t,
		"The requested component has not been found.",
		resp.Message,
	)
}

func TestHandleAddressInvalidAddress(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{
		addressInfoErr: ErrInvalidAddress,
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/addresses/not_an_address",
		nil,
	)
	req.SetPathValue("address", "not_an_address")
	w := httptest.NewRecorder()
	b.handleAddress(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAddressUTXOsOmitsUnknownTotals(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{addressUTXOsTotal: -1})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/addresses/addr_test1vr8nl4/utxos",
		nil,
	)
	req.SetPathValue("address", "addr_test1vr8nl4")
	w := httptest.NewRecorder()
	b.handleAddressUTXOs(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get("X-Pagination-Count-Total"))
	assert.Empty(t, w.Header().Get("X-Pagination-Page-Total"))
}

func TestHandleAddressUTXOsInvalidPagination(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/addresses/addr_test1vr8nl4.../utxos?count=abc",
		nil,
	)
	req.SetPathValue("address", "addr_test1vr8nl4...")
	w := httptest.NewRecorder()
	b.handleAddressUTXOs(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "Invalid pagination parameters.", resp.Message)
}

func TestHandleAddressUTXOsInvalidAddress(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{
		addressUTXOsErr: ErrInvalidAddress,
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/addresses/not_an_address/utxos",
		nil,
	)
	req.SetPathValue("address", "not_an_address")
	w := httptest.NewRecorder()
	b.handleAddressUTXOs(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleMetadataTransactionsPassRequestContext(t *testing.T) {
	t.Parallel()

	type ctxKey struct{}
	for _, tc := range []struct {
		name    string
		path    string
		handler func(*Blockfrost, http.ResponseWriter, *http.Request)
	}{
		{"json", "/api/v0/metadata/txs/labels/721", (*Blockfrost).handleMetadataTransactions},
		{"cbor", "/api/v0/metadata/txs/labels/721/cbor", (*Blockfrost).handleMetadataTransactionsCBOR},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mock := &mockNode{}
			b := newTestBlockfrost(mock)
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req = req.WithContext(
				context.WithValue(req.Context(), ctxKey{}, "request"),
			)
			req.SetPathValue("label", "721")
			w := httptest.NewRecorder()
			tc.handler(b, w, req)

			require.Equal(t, http.StatusOK, w.Code)
			require.NotNil(t, mock.metadataCtx)
			assert.Equal(t, "request", mock.metadataCtx.Value(ctxKey{}))
		})
	}
}

func TestHandleMetadataTransactionsClientCancelWritesNothing(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		path    string
		handler func(*Blockfrost, http.ResponseWriter, *http.Request)
	}{
		{"json", "/api/v0/metadata/txs/labels/721", (*Blockfrost).handleMetadataTransactions},
		{"cbor", "/api/v0/metadata/txs/labels/721/cbor", (*Blockfrost).handleMetadataTransactionsCBOR},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var logs bytes.Buffer
			b := newTestBlockfrost(&mockNode{
				metadataJSONErr: context.Canceled,
				metadataCBORErr: context.Canceled,
			})
			b.logger = slog.New(slog.NewTextHandler(&logs, nil))
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req.SetPathValue("label", "721")
			w := httptest.NewRecorder()
			tc.handler(b, w, req)

			assert.Empty(t, w.Body.String())
			assert.Empty(t, logs.String())
			assert.Empty(t, w.Header().Get("X-Pagination-Count-Total"))
		})
	}
}

func TestHandleMetadataTransactionsKeepsUnavailableJSONAsNull(t *testing.T) {
	t.Parallel()
	mock := &mockNode{metadataJSONTotal: 1, metadataJSON: []MetadataTransactionJSONInfo{{
		TxHash: "txhash-ambiguous", JSONMetadata: nil,
	}}}
	b := newTestBlockfrost(mock)
	req := httptest.NewRequest(http.MethodGet, "/api/v0/metadata/txs/labels/721", nil)
	req.SetPathValue("label", "721")
	w := httptest.NewRecorder()
	b.handleMetadataTransactions(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var resp []MetadataTransactionJSONResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.Len(t, resp, 1)
	assert.Equal(t, "null", string(resp[0].JSONMetadata))
}

func TestHandleMetadataTransactionsInvalidPagination(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/metadata/txs/labels/721?count=abc",
		nil,
	)
	req.SetPathValue("label", "721")
	w := httptest.NewRecorder()
	b.handleMetadataTransactions(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "Invalid pagination parameters.", resp.Message)
}

func TestHandleMetadataTransactionsInvalidLabel(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/metadata/txs/labels/not-a-number",
		nil,
	)
	req.SetPathValue("label", "not-a-number")
	w := httptest.NewRecorder()
	b.handleMetadataTransactions(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "Invalid metadata label.", resp.Message)
}

func TestHandleAddressTransactionsRange(t *testing.T) {
	t.Parallel()

	mock := &mockNode{}
	b := newTestBlockfrost(mock)
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/addresses/a/transactions?from=100:2&to=200",
		nil,
	)
	req.SetPathValue("address", "a")
	w := httptest.NewRecorder()
	b.handleAddressTransactions(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	got := mock.lastAddressTransactionsParams
	require.NotNil(t, got.From)
	require.NotNil(t, got.To)
	assert.EqualValues(t, 100, got.From.Block)
	require.NotNil(t, got.From.Index)
	assert.EqualValues(t, 2, *got.From.Index)
	assert.EqualValues(t, 200, got.To.Block)
	assert.Nil(t, got.To.Index)
}

func TestHandleAddressTransactionsRangeRejectsBadBounds(t *testing.T) {
	t.Parallel()

	for _, query := range []string{
		"from=abc", "to=1:x", "from=5&to=4", "from=5:3&to=5:2",
	} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()
			b := newTestBlockfrost(&mockNode{})
			req := httptest.NewRequest(
				http.MethodGet,
				"/api/v0/addresses/a/transactions?"+query,
				nil,
			)
			req.SetPathValue("address", "a")
			w := httptest.NewRecorder()
			b.handleAddressTransactions(w, req)
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}
