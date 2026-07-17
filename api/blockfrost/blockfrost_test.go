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
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:fix inline
func intPtr(v int) *int {
	return new(v)
}

func TestRedeemerExecutionFee(t *testing.T) {
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

// mockNode implements BlockfrostNode for testing.
type mockNode struct {
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
	asset                         AssetInfo
	assetHolders                  []AssetHolderInfo
	assetHoldersTotal             int
	assetAddressesPolicyID        string
	assetAddressesAssetName       []byte
	assetAddressesParams          PaginationParams
	drep                          DRepInfo
	drepCredential                DRepCredential
	addressUTXOs                  []AddressUTXOInfo
	addressTransactions           []AddressTransactionInfo
	metadataJSON                  []MetadataTransactionJSONInfo
	metadataCBOR                  []MetadataTransactionCBORInfo
	transaction                   TransactionInfo
	transactionSubmitHash         string
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
	assetErr                      error
	assetAddressesErr             error
	drepErr                       error
	addressUTXOsErr               error
	addressTransactionsErr        error
	metadataJSONErr               error
	metadataCBORErr               error
	transactionErr                error
	transactionSubmitErr          error
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
}

func (m *mockNode) ChainTip() (
	ChainTipInfo, error,
) {
	return m.chainTip, m.chainTipErr
}

func (m *mockNode) LatestBlock() (
	BlockInfo, error,
) {
	return m.block, m.blockErr
}

func (m *mockNode) BlockByHashOrNumber(
	_ string,
) (BlockInfo, error) {
	return m.blockByID, m.blockByIDErr
}

func (m *mockNode) LatestBlockTxHashes() (
	[]string, error,
) {
	return m.txHashes, m.txHashesErr
}

func (m *mockNode) CurrentEpoch() (
	EpochInfo, error,
) {
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

func (m *mockNode) Network() (
	NetworkInfo, error,
) {
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

func (m *mockNode) PoolsExtended() (
	[]PoolExtendedInfo, error,
) {
	return m.pools, m.poolsErr
}

func (m *mockNode) Asset(
	_ string,
	_ []byte,
) (AssetInfo, error) {
	return m.asset, m.assetErr
}

func (m *mockNode) AssetAddresses(
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
	credential DRepCredential,
) (DRepInfo, error) {
	m.drepCredential = credential
	return m.drep, m.drepErr
}

func (m *mockNode) AddressUTXOs(
	_ string,
	_ PaginationParams,
) ([]AddressUTXOInfo, int, error) {
	return m.addressUTXOs, m.addressUTXOsTotal, m.addressUTXOsErr
}

func (m *mockNode) AddressTransactions(
	_ string,
	_ PaginationParams,
) ([]AddressTransactionInfo, int, error) {
	return m.addressTransactions, m.addressTxsTotal, m.addressTransactionsErr
}

func (m *mockNode) MetadataTransactions(
	_ uint64,
	_ PaginationParams,
) ([]MetadataTransactionJSONInfo, int, error) {
	return m.metadataJSON, m.metadataJSONTotal, m.metadataJSONErr
}

func (m *mockNode) MetadataTransactionsCBOR(
	_ uint64,
	_ PaginationParams,
) ([]MetadataTransactionCBORInfo, int, error) {
	return m.metadataCBOR, m.metadataCBORTotal, m.metadataCBORErr
}

func (m *mockNode) Transaction(
	_ []byte,
) (TransactionInfo, error) {
	return m.transaction, m.transactionErr
}

func (m *mockNode) TransactionSubmit(
	_ []byte,
) (string, error) {
	return m.transactionSubmitHash, m.transactionSubmitErr
}

func (m *mockNode) TransactionCBOR(
	_ []byte,
) ([]byte, error) {
	return m.transactionCBOR, m.transactionCBORErr
}

func (m *mockNode) TransactionMetadata(
	_ []byte,
) ([]TransactionMetadataInfo, error) {
	return m.transactionMetadata, m.transactionMetadataErr
}

func (m *mockNode) TransactionMetadataCBOR(
	_ []byte,
) ([]TransactionMetadataCBORInfo, error) {
	return m.transactionMetadataCBOR, m.transactionMetadataCBORErr
}

func (m *mockNode) TransactionUTXOs(
	_ []byte,
) (TransactionUTXOsInfo, error) {
	return m.transactionUTXOs, m.transactionUTXOsErr
}

func (m *mockNode) TransactionDelegations(
	_ []byte,
) ([]TransactionDelegationInfo, error) {
	return m.transactionDelegations, m.transactionDelegationsErr
}

func (m *mockNode) TransactionStakeAddresses(
	_ []byte,
) ([]TransactionStakeAddressInfo, error) {
	return m.transactionStakes, m.transactionStakesErr
}

func (m *mockNode) TransactionWithdrawals(
	_ []byte,
) ([]TransactionWithdrawalInfo, error) {
	return m.transactionWithdrawals, m.transactionWithdrawalsErr
}

func (m *mockNode) TransactionMIRs(
	_ []byte,
) ([]TransactionMIRInfo, error) {
	return m.transactionMIRs, m.transactionMIRsErr
}

func (m *mockNode) TransactionPoolUpdates(
	_ []byte,
) ([]TransactionPoolUpdateInfo, error) {
	return m.transactionPoolUpdates, m.transactionPoolUpdatesErr
}

func (m *mockNode) TransactionPoolRetires(
	_ []byte,
) ([]TransactionPoolRetireInfo, error) {
	return m.transactionPoolRetires, m.transactionPoolRetiresErr
}

func (m *mockNode) TransactionRedeemers(
	_ []byte,
) ([]TransactionRedeemerInfo, error) {
	return m.transactionRedeemers, m.transactionRedeemersErr
}

func (m *mockNode) TransactionRequiredSigners(
	_ []byte,
) ([]TransactionRequiredSignerInfo, error) {
	return m.transactionRequiredSigners, m.transactionRequiredSignersErr
}

func (m *mockNode) Account(
	_ string,
) (AccountInfo, error) {
	return m.account, m.accountErr
}

func (m *mockNode) AccountAssociatedAddresses(
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

func (m *mockNode) AccountDelegationHistory(
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

func (m *mockNode) AccountRegistrationHistory(
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

func (m *mockNode) AccountRewardHistory(
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

func newTestBlockfrost(
	node BlockfrostNode,
) *Blockfrost {
	return New(
		BlockfrostConfig{
			ListenAddress: ":0",
		},
		node,
		slog.Default(),
	)
}

func TestStartStop(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	err := b.Start(t.Context())
	require.NoError(t, err)

	// Verify server is running
	b.mu.Lock()
	assert.NotNil(t, b.httpServer)
	b.mu.Unlock()

	// Stop the server
	stopCtx, stopCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer stopCancel()
	err = b.Stop(stopCtx)
	require.NoError(t, err)

	// Verify server is stopped
	b.mu.Lock()
	assert.Nil(t, b.httpServer)
	b.mu.Unlock()
}

func TestStartAlreadyStarted(t *testing.T) {
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

func TestHandleRoot(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet, "/", nil,
	)
	w := httptest.NewRecorder()
	b.handleRoot(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(
		t,
		"application/json",
		w.Header().Get("Content-Type"),
	)

	var resp RootResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(
		t,
		"https://blockfrost.io/",
		resp.URL,
	)
	assert.Equal(t, "0.1.0", resp.Version)
}

func TestRouterRootServesRootDocument(t *testing.T) {
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
}

func TestRouterUnimplementedRouteReturns404(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)
	handler := b.handler()

	paths := []string{
		"/api/v0/",
		"/api/v0/scripts",
		"/api/v0/pools",
		"/does-not-exist",
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

func TestRouterImplementedRouteStillWorks(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)
	handler := b.handler()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleHealth(t *testing.T) {
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

func TestHandleLatestBlock(t *testing.T) {
	blockVRF := "vrf_vk1abc"
	opCert := "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
	opCertCounter := "7"
	mock := &mockNode{
		block: BlockInfo{
			Hash:          "abc123",
			Slot:          12345,
			Epoch:         100,
			EpochSlot:     345,
			Height:        67890,
			Time:          1700000000,
			Size:          1024,
			TxCount:       5,
			SlotLeader:    "pool1xyz",
			PreviousBlock: "prev123",
			Confirmations: 10,
			Output:        "123456789",
			Fees:          "4321",
			BlockVRF:      &blockVRF,
			OPCert:        &opCert,
			OPCertCounter: &opCertCounter,
			// Latest block is the tip, so it has no successor.
			NextBlock: nil,
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/blocks/latest",
		nil,
	)
	w := httptest.NewRecorder()
	b.handleLatestBlock(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp BlockResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "abc123", resp.Hash)
	assert.Equal(t, uint64(12345), resp.Slot)
	assert.Equal(t, uint64(100), resp.Epoch)
	assert.Equal(t, uint64(345), resp.EpochSlot)
	assert.Equal(t, uint64(67890), resp.Height)
	assert.Equal(t, int64(1700000000), resp.Time)
	assert.Equal(t, uint64(1024), resp.Size)
	assert.Equal(t, 5, resp.TxCount)
	assert.Equal(t, "pool1xyz", resp.SlotLeader)
	assert.Equal(t, "prev123", resp.PreviousBlock)
	assert.Equal(t, uint64(10), resp.Confirmations)
	require.NotNil(t, resp.Output)
	assert.Equal(t, "123456789", *resp.Output)
	require.NotNil(t, resp.Fees)
	assert.Equal(t, "4321", *resp.Fees)
	require.NotNil(t, resp.BlockVRF)
	assert.Equal(t, "vrf_vk1abc", *resp.BlockVRF)
	require.NotNil(t, resp.OPCert)
	assert.Equal(t, opCert, *resp.OPCert)
	require.NotNil(t, resp.OPCertCounter)
	assert.Equal(t, "7", *resp.OPCertCounter)
	assert.Nil(t, resp.NextBlock)
}

func TestHandleBlockByHashOrNumber(t *testing.T) {
	nextBlock := "nexthash"
	mock := &mockNode{
		blockByID: BlockInfo{
			Hash:          "abc123",
			Slot:          12345,
			Epoch:         10,
			EpochSlot:     345,
			Height:        1000,
			Time:          1700000000,
			Size:          4096,
			TxCount:       5,
			SlotLeader:    "pool1...",
			PreviousBlock: "prevhash",
			Confirmations: 7,
			Output:        "999",
			Fees:          "10",
			// Historical (non-tip) block: it has a known successor.
			NextBlock: &nextBlock,
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/blocks/1000",
		nil,
	)
	req.SetPathValue("hash_or_number", "1000")
	w := httptest.NewRecorder()
	b.handleBlock(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp BlockResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "abc123", resp.Hash)
	assert.Equal(t, uint64(1000), resp.Height)
	assert.Equal(t, uint64(7), resp.Confirmations)
	require.NotNil(t, resp.Output)
	assert.Equal(t, "999", *resp.Output)
	require.NotNil(t, resp.Fees)
	assert.Equal(t, "10", *resp.Fees)
	require.NotNil(t, resp.NextBlock)
	assert.Equal(t, "nexthash", *resp.NextBlock)
}

func TestHandleBlockNotFound(t *testing.T) {
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

func TestHandleAsset(t *testing.T) {
	onchain := any(map[string]any{"name": "Test Token", "decimals": float64(6)})
	standard := "CIP25v2"
	mock := &mockNode{
		asset: AssetInfo{
			Asset:                   "00112233445566778899aabbccddeeff00112233445566778899aabb746f6b656e",
			PolicyID:                "00112233445566778899aabbccddeeff00112233445566778899aabb",
			AssetName:               "746f6b656e",
			AssetNameASCII:          "token",
			Fingerprint:             "asset1test",
			Quantity:                "42",
			InitialMintTxHash:       "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
			MintOrBurnCount:         3,
			OnchainMetadata:         &onchain,
			OnchainMetadataStandard: &standard,
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/assets/00112233445566778899aabbccddeeff00112233445566778899aabb746f6b656e",
		nil,
	)
	req.SetPathValue(
		"asset",
		"00112233445566778899aabbccddeeff00112233445566778899aabb746f6b656e",
	)
	w := httptest.NewRecorder()
	b.handleAsset(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp AssetResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, mock.asset.Asset, resp.Asset)
	assert.Equal(t, mock.asset.PolicyID, resp.PolicyID)
	assert.Equal(t, mock.asset.AssetName, resp.AssetName)
	assert.Equal(t, mock.asset.AssetNameASCII, resp.AssetNameASCII)
	assert.Equal(t, mock.asset.Fingerprint, resp.Fingerprint)
	assert.Equal(t, mock.asset.Quantity, resp.Quantity)
	assert.Equal(t, mock.asset.InitialMintTxHash, resp.InitialMintTxHash)
	assert.Equal(t, mock.asset.MintOrBurnCount, resp.MintOrBurnCount)
	require.NotNil(t, resp.OnchainMetadata)
	assert.Equal(t, onchain, *resp.OnchainMetadata)
	require.NotNil(t, resp.OnchainMetadataStandard)
	assert.Equal(t, "CIP25v2", *resp.OnchainMetadataStandard)
	assert.Nil(t, resp.Metadata)
}

func TestHandleAssetInvalidIdentifier(t *testing.T) {
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

func TestAssetHoldersFromUtxosPreservesPointerAddress(t *testing.T) {
	paymentHash := bytes.Repeat([]byte{0xab}, lcommon.AddressHashSize)
	policyID := bytes.Repeat([]byte{0xcd}, lcommon.AddressHashSize)
	assetName := []byte("TOKEN")
	addrBytes := []byte{
		(lcommon.AddressTypeKeyPointer << 4) |
			lcommon.AddressNetworkTestnet,
	}
	addrBytes = append(addrBytes, paymentHash...)
	addrBytes = append(addrBytes, 0x01, 0x00, 0x00)
	addr, err := lcommon.NewAddressFromBytes(addrBytes)
	require.NoError(t, err)

	output := shelley.ShelleyTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  1_000_000,
	}
	outputCbor, err := cbor.Encode(&output)
	require.NoError(t, err)
	holders, err := assetHoldersFromUtxos(
		policyID,
		assetName,
		[]models.Utxo{{
			TxId:      bytes.Repeat([]byte{0x01}, 32),
			OutputIdx: 0,
			Cbor:      outputCbor,
			Assets: []models.Asset{{
				PolicyId: policyID,
				Name:     assetName,
				Amount:   types.Uint64(7),
			}},
		}},
		PaginationParams{Count: 100, Page: 1, Order: "asc"},
	)
	require.NoError(t, err)
	require.Len(t, holders, 1)
	assert.Equal(t, addr.String(), holders[0].Address)
	assert.Equal(t, "7", holders[0].Quantity)
}

func TestHandleDRep(t *testing.T) {
	const drepHex = "00000000000000000000000000000000000000000000000000000000"
	const drepID = drepHex

	mock := &mockNode{
		drep: DRepInfo{
			DRepID:      drepID,
			Hex:         drepHex,
			HasScript:   false,
			Registered:  true,
			Epoch:       12,
			Amount:      "123456",
			Active:      true,
			ActiveEpoch: 14,
			LiveStake:   "123456",
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

	var resp DRepResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, mock.drep.DRepID, resp.DRepID)
	assert.Equal(t, mock.drep.Hex, resp.Hex)
	assert.Equal(t, mock.drep.HasScript, resp.HasScript)
	assert.Equal(t, mock.drep.Registered, resp.Registered)
	assert.Equal(t, mock.drep.Epoch, resp.Epoch)
	assert.Equal(t, mock.drep.Amount, resp.Amount)
	assert.Equal(t, mock.drep.Active, resp.Active)
	assert.Equal(t, mock.drep.ActiveEpoch, resp.ActiveEpoch)
	assert.Equal(t, mock.drep.LiveStake, resp.LiveStake)
}

// TestHandleDRepCIP129ScriptIdentifier verifies script DRep IDs keep their
// credential type when the HTTP handler passes them to the node adapter.
func TestHandleDRepCIP129ScriptIdentifier(t *testing.T) {
	const drepID = "drep1xvqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqhknfj5"
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
	assert.Equal(t, make([]byte, drepCredentialHashLen), mock.drepCredential.Hash)
}

// TestParseDRepIdentifierCIP129CredentialType verifies CIP-129 DRep IDs
// decode the credential type from the bech32 payload header.
func TestParseDRepIdentifierCIP129CredentialType(t *testing.T) {
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
			id:            "drep1xvqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqhknfj5",
			wantHasScript: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			credential, err := parseDRepIdentifier(tc.id)
			require.NoError(t, err)
			assert.Equal(t, tc.id, credential.ID)
			assert.Equal(t, make([]byte, drepCredentialHashLen), credential.Hash)
			assert.Equal(t, tc.wantHasScript, credential.HasScript)
			assert.True(t, credential.CredentialTagKnown)
		})
	}
}

// TestParseDRepIdentifierHexIsAmbiguous verifies raw hash identifiers remain
// untyped so lookup can deliberately fall back across key/script tags.
func TestParseDRepIdentifierHexIsAmbiguous(t *testing.T) {
	const drepHex = "00000000000000000000000000000000000000000000000000000000"

	credential, err := parseDRepIdentifier(drepHex)
	require.NoError(t, err)
	assert.Equal(t, drepHex, credential.ID)
	assert.Equal(t, make([]byte, drepCredentialHashLen), credential.Hash)
	assert.False(t, credential.HasScript)
	assert.False(t, credential.CredentialTagKnown)
}

func TestHandleDRepInvalidIdentifier(t *testing.T) {
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

func TestHandlePoolsExtended(t *testing.T) {
	mock := &mockNode{
		pools: []PoolExtendedInfo{
			{
				PoolID:         "pool1zzz",
				Hex:            "ff",
				VrfKey:         "vrf2",
				ActiveStake:    "200",
				LiveStake:      "300",
				DeclaredPledge: "400",
				FixedCost:      "500",
				MarginCost:     0.2,
				Relays: []PoolRelayInfo{
					{
						IPv4: "192.168.0.1",
						DNS:  "relay-two.example",
						Port: new(3002),
					},
				},
			},
			{
				PoolID:         "pool1aaa",
				Hex:            "01",
				VrfKey:         "vrf1",
				ActiveStake:    "20",
				LiveStake:      "30",
				DeclaredPledge: "40",
				FixedCost:      "50",
				MarginCost:     0.1,
				Relays: []PoolRelayInfo{
					{
						IPv6: "2001:db8::1",
						DNS:  "relay-one.example",
						Port: new(3001),
					},
					{
						DNS: "relay-no-port.example",
					},
				},
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/pools/extended?count=1&page=1&order=asc",
		nil,
	)
	w := httptest.NewRecorder()
	b.handlePoolsExtended(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(
		t,
		"2",
		w.Header().Get("X-Pagination-Count-Total"),
	)
	assert.Equal(
		t,
		"2",
		w.Header().Get("X-Pagination-Page-Total"),
	)

	var resp []PoolExtendedResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, "pool1aaa", resp[0].PoolID)
	assert.Equal(t, "01", resp[0].Hex)
	assert.Equal(t, "vrf1", resp[0].VrfKey)
	assert.Equal(t, "20", resp[0].ActiveStake)
	assert.Equal(t, "30", resp[0].LiveStake)
	assert.Equal(t, "40", resp[0].DeclaredPledge)
	assert.Equal(t, "50", resp[0].FixedCost)
	assert.InDelta(t, 0.1, resp[0].MarginCost, 0.0001)
	require.Len(t, resp[0].Relays, 2)
	assert.Nil(t, resp[0].Relays[0].IPv4)
	require.NotNil(t, resp[0].Relays[0].IPv6)
	assert.Equal(t, "2001:db8::1", *resp[0].Relays[0].IPv6)
	require.NotNil(t, resp[0].Relays[0].DNS)
	assert.Equal(t, "relay-one.example", *resp[0].Relays[0].DNS)
	require.NotNil(t, resp[0].Relays[0].Port)
	assert.Equal(t, 3001, *resp[0].Relays[0].Port)
	require.NotNil(t, resp[0].Relays[1].DNS)
	assert.Equal(t, "relay-no-port.example", *resp[0].Relays[1].DNS)
	assert.Nil(t, resp[0].Relays[1].Port)
}

func TestHandlePoolsExtendedInvalidPagination(t *testing.T) {
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

func TestHandleLatestBlockTxs(t *testing.T) {
	mock := &mockNode{
		txHashes: []string{"tx1", "tx2", "tx3"},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/blocks/latest/txs",
		nil,
	)
	w := httptest.NewRecorder()
	b.handleLatestBlockTxs(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp []string
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(
		t,
		[]string{"tx1", "tx2", "tx3"},
		resp,
	)
}

func TestHandleLatestBlockTxsEmpty(t *testing.T) {
	mock := &mockNode{
		txHashes: nil,
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/blocks/latest/txs",
		nil,
	)
	w := httptest.NewRecorder()
	b.handleLatestBlockTxs(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp []string
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	// Should return empty array, not null
	assert.NotNil(t, resp)
	assert.Empty(t, resp)
}

func TestHandleTransaction(t *testing.T) {
	invalidBefore := "100"
	invalidHereafter := "200"
	mock := &mockNode{
		transaction: TransactionInfo{
			Hash:             "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Block:            "blockhash1",
			Slot:             123,
			BlockHeight:      45,
			BlockTime:        1700000000,
			Index:            2,
			OutputAmount:     []AddressAmountInfo{{Unit: "lovelace", Quantity: "5000"}},
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
	w := httptest.NewRecorder()
	b.handleTransaction(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

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

func TestHandleTransactionCBOR(t *testing.T) {
	b := newTestBlockfrost(&mockNode{
		transactionCBOR: []byte{0x84, 0x01, 0x02, 0xf5, 0xf6},
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/txs/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/cbor",
		nil,
	)
	req.SetPathValue(
		"hash",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	)
	w := httptest.NewRecorder()
	b.handleTransactionCBOR(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp TransactionCBORResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "840102f5f6", resp.CBOR)
}

func TestHandleTransactionSubEndpointNotFound(t *testing.T) {
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
			assert.Equal(t, "The requested transaction could not be found.", resp.Message)
		})
	}
}

func TestHandleTransactionMetadata(t *testing.T) {
	b := newTestBlockfrost(&mockNode{
		transactionMetadata: []TransactionMetadataInfo{
			{
				Label:        "721",
				JSONMetadata: json.RawMessage(`{"name":"nft-one"}`),
			},
		},
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/txs/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/metadata",
		nil,
	)
	req.SetPathValue(
		"hash",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	)
	w := httptest.NewRecorder()
	b.handleTransactionMetadata(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp []TransactionMetadataResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, "721", resp[0].Label)
	assert.JSONEq(t, `{"name":"nft-one"}`, string(resp[0].JSONMetadata))
}

func TestHandleTransactionMetadataCBOR(t *testing.T) {
	b := newTestBlockfrost(&mockNode{
		transactionMetadataCBOR: []TransactionMetadataCBORInfo{
			{Label: "721", CBORMetadata: "a1646e616d65636e6674"},
		},
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/txs/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/metadata/cbor",
		nil,
	)
	req.SetPathValue(
		"hash",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	)
	w := httptest.NewRecorder()
	b.handleTransactionMetadataCBOR(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp []TransactionMetadataCBORResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, "721", resp[0].Label)
	assert.Equal(t, "a1646e616d65636e6674", resp[0].Metadata)
	require.NotNil(t, resp[0].CborMetadata)
	assert.Equal(t, resp[0].Metadata, *resp[0].CborMetadata)
}

func TestHandleTransactionUTXOs(t *testing.T) {
	ref := true
	b := newTestBlockfrost(&mockNode{
		transactionUTXOs: TransactionUTXOsInfo{
			Hash: "txhash1",
			Inputs: []TransactionInputInfo{
				{
					Address:     "addr_test1input",
					TxHash:      "prevtx",
					OutputIndex: 1,
					Amount:      []AddressAmountInfo{{Unit: "lovelace", Quantity: "10"}},
					Reference:   &ref,
				},
			},
			Outputs: []TransactionOutputInfo{
				{
					Address:     "addr_test1output",
					OutputIndex: 0,
					Amount:      []AddressAmountInfo{{Unit: "lovelace", Quantity: "5"}},
				},
			},
		},
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/txs/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/utxos",
		nil,
	)
	req.SetPathValue(
		"hash",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	)
	w := httptest.NewRecorder()
	b.handleTransactionUTXOs(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp TransactionUTXOsResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "txhash1", resp.Hash)
	require.Len(t, resp.Inputs, 1)
	assert.Equal(t, "prevtx", resp.Inputs[0].TxHash)
	assert.Equal(t, 1, resp.Inputs[0].OutputIndex)
	require.NotNil(t, resp.Inputs[0].Reference)
	assert.True(t, *resp.Inputs[0].Reference)
	require.Len(t, resp.Outputs, 1)
	assert.Equal(t, "addr_test1output", resp.Outputs[0].Address)
}

func TestHandleTransactionDelegations(t *testing.T) {
	b := newTestBlockfrost(&mockNode{
		transactionDelegations: []TransactionDelegationInfo{
			{
				Address:     "stake_test1...",
				PoolID:      "pool1...",
				Index:       2,
				CertIndex:   2,
				ActiveEpoch: 10,
			},
		},
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/txs/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/delegations",
		nil,
	)
	req.SetPathValue(
		"hash",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	)
	w := httptest.NewRecorder()
	b.handleTransactionDelegations(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp []TransactionDelegationResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, "stake_test1...", resp[0].Address)
	assert.Equal(t, "pool1...", resp[0].PoolID)
	assert.Equal(t, 2, resp[0].Index)
	assert.Equal(t, 2, resp[0].CertIndex)
	assert.Equal(t, uint64(10), resp[0].ActiveEpoch)
}

func TestHandleTransactionRedeemers(t *testing.T) {
	datumHash := "923918e403bf43c34b4ef6b48eb2ee04babed17320d8d1b9ff9ad086e86f44ec"
	b := newTestBlockfrost(&mockNode{
		transactionRedeemers: []TransactionRedeemerInfo{
			{
				DatumHash:        &datumHash,
				TxIndex:          0,
				Purpose:          "spend",
				ScriptHash:       "f00dbeef00112233445566778899aabbccddeeff0011223344556677",
				RedeemerDataHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
				UnitMem:          "1700",
				UnitSteps:        "200000",
				Fee:              "172345",
			},
		},
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/txs/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/redeemers",
		nil,
	)
	req.SetPathValue(
		"hash",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	)
	w := httptest.NewRecorder()
	b.handleTransactionRedeemers(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp []TransactionRedeemerResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	require.NotNil(t, resp[0].DatumHash)
	assert.Equal(t, datumHash, *resp[0].DatumHash)
	assert.Equal(t, 0, resp[0].TxIndex)
	assert.Equal(t, "spend", resp[0].Purpose)
	assert.Equal(t, "f00dbeef00112233445566778899aabbccddeeff0011223344556677", resp[0].ScriptHash)
	assert.Equal(t, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", resp[0].RedeemerDataHash)
	assert.Equal(t, "1700", resp[0].UnitMem)
	assert.Equal(t, "200000", resp[0].UnitSteps)
	assert.Equal(t, "172345", resp[0].Fee)
}

func TestHandleTransactionStakeAddresses(t *testing.T) {
	b := newTestBlockfrost(&mockNode{
		transactionStakes: []TransactionStakeAddressInfo{
			{
				Address:      "stake_test1...",
				CertIndex:    1,
				Registration: true,
			},
		},
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/txs/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/stakes",
		nil,
	)
	req.SetPathValue(
		"hash",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	)
	w := httptest.NewRecorder()
	b.handleTransactionStakeAddresses(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp []TransactionStakeAddressResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, "stake_test1...", resp[0].Address)
	assert.True(t, resp[0].Registration)
}

func TestHandleLatestEpoch(t *testing.T) {
	mock := &mockNode{
		epoch: EpochInfo{
			Epoch:          100,
			StartTime:      1700000000,
			EndTime:        1700432000,
			FirstBlockTime: 1700000020,
			LastBlockTime:  1700431980,
			BlockCount:     21600,
			TxCount:        50000,
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/epochs/latest",
		nil,
	)
	w := httptest.NewRecorder()
	b.handleLatestEpoch(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp EpochResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), resp.Epoch)
	assert.Equal(t, int64(1700000000), resp.StartTime)
	assert.Equal(t, int64(1700432000), resp.EndTime)
	assert.Equal(t, 21600, resp.BlockCount)
	assert.Equal(t, 50000, resp.TxCount)
}

func TestHandleLatestEpochParams(t *testing.T) {
	mock := &mockNode{
		params: ProtocolParamsInfo{
			Epoch:               100,
			MinFeeA:             44,
			MinFeeB:             155381,
			MaxBlockSize:        65536,
			MaxTxSize:           16384,
			MaxBlockHeaderSize:  1100,
			KeyDeposit:          "2000000",
			PoolDeposit:         "500000000",
			EMax:                18,
			NOpt:                150,
			A0:                  0.3,
			Rho:                 0.003,
			Tau:                 0.2,
			ProtocolMajorVer:    8,
			ProtocolMinorVer:    0,
			MinPoolCost:         "170000000",
			CoinsPerUtxoSize:    "4310",
			PriceMem:            0.0577,
			PriceStep:           0.0000721,
			MaxTxExMem:          "10000000000",
			MaxTxExSteps:        "10000000000000",
			MaxBlockExMem:       "50000000000",
			MaxBlockExSteps:     "40000000000000",
			MaxValSize:          "5000",
			CollateralPercent:   150,
			MaxCollateralInputs: 3,
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/epochs/latest/parameters",
		nil,
	)
	w := httptest.NewRecorder()
	b.handleLatestEpochParams(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp ProtocolParamsResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), resp.Epoch)
	assert.Equal(t, 44, resp.MinFeeA)
	assert.Equal(t, 155381, resp.MinFeeB)
	assert.Equal(t, 65536, resp.MaxBlockSize)
	assert.Equal(t, "2000000", resp.KeyDeposit)
	assert.Equal(t, "500000000", resp.PoolDeposit)
	require.NotNil(t, resp.CollateralPercent)
	assert.Equal(t, 150, *resp.CollateralPercent)
	require.NotNil(t, resp.MaxCollateralInputs)
	assert.Equal(t, 3, *resp.MaxCollateralInputs)
}

func TestHandleEpochParams(t *testing.T) {
	mock := &mockNode{
		epochParams: ProtocolParamsInfo{
			Epoch:               42,
			MinFeeA:             44,
			MinFeeB:             155381,
			MaxBlockSize:        65536,
			MaxTxSize:           16384,
			MaxBlockHeaderSize:  1100,
			KeyDeposit:          "2000000",
			PoolDeposit:         "500000000",
			EMax:                18,
			NOpt:                150,
			A0:                  0.3,
			Rho:                 0.003,
			Tau:                 0.2,
			ProtocolMajorVer:    8,
			ProtocolMinorVer:    0,
			MinPoolCost:         "170000000",
			CoinsPerUtxoSize:    "4310",
			PriceMem:            0.0577,
			PriceStep:           0.0000721,
			MaxTxExMem:          "10000000000",
			MaxTxExSteps:        "10000000000000",
			MaxBlockExMem:       "50000000000",
			MaxBlockExSteps:     "40000000000000",
			MaxValSize:          "5000",
			CollateralPercent:   150,
			MaxCollateralInputs: 3,
		},
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

	assert.Equal(t, http.StatusOK, w.Code)

	var resp ProtocolParamsResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), resp.Epoch)
	assert.Equal(t, 44, resp.MinFeeA)
	assert.Equal(t, 155381, resp.MinFeeB)
	assert.Equal(t, "4310", resp.CoinsPerUtxoWord)
}

func TestHandleEpochParamsInvalidEpoch(t *testing.T) {
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

func TestHandleAccount(t *testing.T) {
	drepID := "drep1xyz"
	mock := &mockNode{
		account: AccountInfo{
			StakeAddress:       "stake_test1",
			Active:             true,
			ControlledAmount:   "123",
			RewardsSum:         "10",
			WithdrawalsSum:     "45",
			ReservesSum:        "6",
			TreasurySum:        "7",
			WithdrawableAmount: "10",
			DrepID:             &drepID,
			Registered:         true,
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/accounts/stake_test1",
		nil,
	)
	req.SetPathValue("stake_address", "stake_test1")
	w := httptest.NewRecorder()
	b.handleAccount(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp AccountResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "stake_test1", resp.StakeAddress)
	assert.True(t, resp.Active)
	assert.Equal(t, "123", resp.ControlledAmount)
	assert.Equal(t, "10", resp.RewardsSum)
	assert.Equal(t, "45", resp.WithdrawalsSum)
	assert.Equal(t, "6", resp.ReservesSum)
	assert.Equal(t, "7", resp.TreasurySum)
	assert.Equal(t, "10", resp.WithdrawableAmount)
	require.NotNil(t, resp.DrepID)
	assert.Equal(t, "drep1xyz", *resp.DrepID)
	assert.True(t, resp.Registered)

	// The OpenAPI 0.1.88 account_content schema requires these field
	// names; assert the marshaled key set exposes them.
	assertJSONKeys(t, resp, []string{
		"stake_address",
		"active",
		"active_epoch",
		"controlled_amount",
		"rewards_sum",
		"withdrawals_sum",
		"reserves_sum",
		"treasury_sum",
		"withdrawable_amount",
		"pool_id",
		"drep_id",
		"registered",
	})
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

func TestHandleAccountAssociatedAddresses(t *testing.T) {
	mock := &mockNode{
		addresses: []AccountAssociatedAddressInfo{
			{Address: "addr_test1"},
			{Address: "addr_test2"},
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/accounts/stake_test1/addresses?count=1&page=2&order=asc",
		nil,
	)
	req.SetPathValue("stake_address", "stake_test1")
	w := httptest.NewRecorder()
	b.handleAccountAssociatedAddresses(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "2", w.Header().Get("X-Pagination-Count-Total"))
	assert.Equal(t, "2", w.Header().Get("X-Pagination-Page-Total"))

	var resp []AccountAssociatedAddressResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, "addr_test2", resp[0].Address)
}

func TestHandleAccountDelegationHistory(t *testing.T) {
	mock := &mockNode{
		delegations: []AccountDelegationHistoryInfo{
			{
				ActiveEpoch: 1,
				TxHash:      "tx1",
				Amount:      "0",
				PoolID:      "pool1",
				TxSlot:      100,
				BlockTime:   1000,
				BlockHeight: 10,
			},
			{
				ActiveEpoch: 2,
				TxHash:      "tx2",
				Amount:      "0",
				PoolID:      "pool2",
				TxSlot:      200,
				BlockTime:   2000,
				BlockHeight: 20,
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/accounts/stake_test1/delegations?count=1&page=1&order=desc",
		nil,
	)
	req.SetPathValue("stake_address", "stake_test1")
	w := httptest.NewRecorder()
	b.handleAccountDelegationHistory(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp []AccountDelegationHistoryResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, int32(2), resp[0].ActiveEpoch)
	assert.Equal(t, "tx2", resp[0].TxHash)
	assert.Equal(t, "pool2", resp[0].PoolID)
	assert.Equal(t, int64(200), resp[0].TxSlot)
	assert.Equal(t, int64(2000), resp[0].BlockTime)
	assert.Equal(t, int64(20), resp[0].BlockHeight)

	// OpenAPI 0.1.88 account_delegation_content required field names.
	assertJSONKeys(t, resp[0], []string{
		"active_epoch",
		"tx_hash",
		"amount",
		"pool_id",
		"tx_slot",
		"block_time",
		"block_height",
	})
}

func TestHandleAccountRegistrationHistory(t *testing.T) {
	mock := &mockNode{
		regs: []AccountRegistrationHistoryInfo{
			{
				TxHash:      "tx1",
				Action:      "registered",
				Deposit:     "2000000",
				TxSlot:      100,
				BlockTime:   1000,
				BlockHeight: 10,
			},
			{
				TxHash:      "tx2",
				Action:      "deregistered",
				Deposit:     "2000000",
				TxSlot:      200,
				BlockTime:   2000,
				BlockHeight: 20,
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/accounts/stake_test1/registrations?count=2&page=1&order=asc",
		nil,
	)
	req.SetPathValue("stake_address", "stake_test1")
	w := httptest.NewRecorder()
	b.handleAccountRegistrationHistory(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp []AccountRegistrationHistoryResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 2)
	assert.Equal(t, "registered", resp[0].Action)
	assert.Equal(t, "deregistered", resp[1].Action)
	assert.Equal(t, "2000000", resp[0].Deposit)
	assert.Equal(t, int64(100), resp[0].TxSlot)
	assert.Equal(t, int64(1000), resp[0].BlockTime)
	assert.Equal(t, int64(10), resp[0].BlockHeight)

	// OpenAPI 0.1.88 account_registration_content required field names.
	assertJSONKeys(t, resp[0], []string{
		"tx_hash",
		"action",
		"deposit",
		"tx_slot",
		"block_time",
		"block_height",
	})
}

func TestHandleAccountRewardHistory(t *testing.T) {
	mock := &mockNode{
		rewards: []AccountRewardHistoryInfo{
			{Epoch: 3, Amount: "12", PoolID: "pool1"},
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/accounts/stake_test1/rewards",
		nil,
	)
	req.SetPathValue("stake_address", "stake_test1")
	w := httptest.NewRecorder()
	b.handleAccountRewardHistory(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp []AccountRewardHistoryResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, int32(3), resp[0].Epoch)
	assert.Equal(t, "12", resp[0].Amount)
}

func TestHandleNetwork(t *testing.T) {
	mock := &mockNode{
		network: NetworkInfo{
			Supply: NetworkSupplyInfo{
				Max:         "45000000000000000",
				Total:       "33000000000000000",
				Circulating: "32000000000000000",
				Locked:      "1000000000000",
				Treasury:    "500000000000",
				Reserves:    "12000000000000000",
			},
			Stake: NetworkStakeInfo{
				Live:   "25000000000000000",
				Active: "24000000000000000",
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/network",
		nil,
	)
	w := httptest.NewRecorder()
	b.handleNetwork(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp NetworkResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(
		t,
		"45000000000000000",
		resp.Supply.Max,
	)
	assert.NotEmpty(t, resp.Stake.Live)
	assert.NotEmpty(t, resp.Stake.Active)
	assert.Equal(t, "500000000000", resp.Supply.Treasury)
	assert.Equal(t, "12000000000000000", resp.Supply.Reserves)
	assert.Equal(t, "1000000000000", resp.Supply.Locked)
}

func TestHandleNetworkEras(t *testing.T) {
	mock := &mockNode{
		networkEras: []NetworkEraInfo{
			{
				Era: "Byron",
				Start: NetworkEraBoundInfo{
					Time:  1506203091,
					Slot:  0,
					Epoch: 0,
				},
				End: &NetworkEraBoundInfo{
					Time:  1563999616,
					Slot:  4492800,
					Epoch: 208,
				},
				Params: NetworkEraParamsInfo{
					EpochLength: 21600,
					SlotLength:  20,
					SafeZone:    4320,
				},
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/network/eras",
		nil,
	)
	w := httptest.NewRecorder()
	b.handleNetworkEras(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Per OpenAPI 0.1.88, era items carry only start, end, and
	// parameters; the era name must not appear in the response.
	assert.NotContains(t, w.Body.String(), "\"era\"")

	var resp []NetworkEraResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, uint64(0), resp[0].Start.Epoch)
	require.NotNil(t, resp[0].End)
	assert.Equal(t, uint64(208), resp[0].End.Epoch)
	assert.Equal(t, uint64(21600), resp[0].Parameters.EpochLength)
	assert.Equal(t, uint64(4320), resp[0].Parameters.SafeZone)
}

func TestHandleGenesis(t *testing.T) {
	mock := &mockNode{
		genesis: GenesisInfo{
			ActiveSlotsCoefficient: 0.05,
			UpdateQuorum:           5,
			MaxLovelaceSupply:      "45000000000000000",
			NetworkMagic:           764824073,
			EpochLength:            432000,
			SystemStart:            1506203091,
			SlotsPerKESPeriod:      129600,
			SlotLength:             1,
			MaxKESEvolutions:       62,
			SecurityParam:          2160,
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/genesis",
		nil,
	)
	w := httptest.NewRecorder()
	b.handleGenesis(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp GenesisResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "45000000000000000", resp.MaxLovelaceSupply)
	assert.Equal(t, 764824073, resp.NetworkMagic)
	assert.Equal(t, 432000, resp.EpochLength)
}

func TestStopIdempotent(t *testing.T) {
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

func TestNilLogger(t *testing.T) {
	b := New(
		BlockfrostConfig{ListenAddress: ":0"},
		&mockNode{},
		nil,
	)
	assert.NotNil(t, b.logger)
}

func TestDefaultListenAddress(t *testing.T) {
	b := New(
		BlockfrostConfig{},
		&mockNode{},
		slog.Default(),
	)
	assert.Equal(t, ":3000", b.config.ListenAddress)
}

func TestHandleAddressUTXOs(t *testing.T) {
	mock := &mockNode{
		addressUTXOsTotal: 2,
		addressUTXOs: []AddressUTXOInfo{
			{
				Address:     "addr_test1vr8nl4...",
				TxHash:      "txhash1",
				OutputIndex: 1,
				Amount: []AddressAmountInfo{
					{Unit: "lovelace", Quantity: "1000"},
					{Unit: "policyasset", Quantity: "5"},
				},
				Block: "blockhash1",
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/addresses/addr_test1vr8nl4.../utxos?count=1&page=1&order=desc",
		nil,
	)
	req.SetPathValue("address", "addr_test1vr8nl4...")
	w := httptest.NewRecorder()
	b.handleAddressUTXOs(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "2", w.Header().Get("X-Pagination-Count-Total"))
	assert.Equal(t, "2", w.Header().Get("X-Pagination-Page-Total"))

	var resp []AddressUTXOResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, "addr_test1vr8nl4...", resp[0].Address)
	assert.Equal(t, "txhash1", resp[0].TxHash)
	assert.Equal(t, 1, resp[0].OutputIndex)
	assert.Equal(t, "lovelace", resp[0].Amount[0].Unit)
	assert.Equal(t, "1000", resp[0].Amount[0].Quantity)
	assert.Equal(t, "blockhash1", resp[0].Block)
}

func TestHandleAddressUTXOsInvalidPagination(t *testing.T) {
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

func TestHandleAddressTransactions(t *testing.T) {
	mock := &mockNode{
		addressTxsTotal: 3,
		addressTransactions: []AddressTransactionInfo{
			{
				TxHash:      "txhash1",
				TxIndex:     2,
				BlockHeight: 55,
				BlockTime:   1700000000,
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/addresses/addr_test1vr8nl4.../transactions?count=2&page=1",
		nil,
	)
	req.SetPathValue("address", "addr_test1vr8nl4...")
	w := httptest.NewRecorder()
	b.handleAddressTransactions(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "3", w.Header().Get("X-Pagination-Count-Total"))
	assert.Equal(t, "2", w.Header().Get("X-Pagination-Page-Total"))

	var resp []AddressTransactionResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, "txhash1", resp[0].TxHash)
	assert.Equal(t, 2, resp[0].TxIndex)
	assert.EqualValues(t, 55, resp[0].BlockHeight)
	assert.Equal(t, 1700000000, resp[0].BlockTime)
}

func TestHandleMetadataTransactions(t *testing.T) {
	mock := &mockNode{
		metadataJSONTotal: 2,
		metadataJSON: []MetadataTransactionJSONInfo{
			{
				TxHash:       "txhash1",
				JSONMetadata: json.RawMessage(`{"name":"nft-one"}`),
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/metadata/txs/labels/721?count=1&page=1&order=asc",
		nil,
	)
	req.SetPathValue("label", "721")
	w := httptest.NewRecorder()
	b.handleMetadataTransactions(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "2", w.Header().Get("X-Pagination-Count-Total"))
	assert.Equal(t, "2", w.Header().Get("X-Pagination-Page-Total"))

	var resp []MetadataTransactionJSONResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, "txhash1", resp[0].TxHash)
	assert.JSONEq(t, `{"name":"nft-one"}`, string(resp[0].JSONMetadata))
}

func TestHandleMetadataTransactionsCBOR(t *testing.T) {
	mock := &mockNode{
		metadataCBORTotal: 1,
		metadataCBOR: []MetadataTransactionCBORInfo{
			{
				TxHash:   "txhash2",
				Metadata: "a1646e616d65676e66742d74776f",
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/metadata/txs/labels/721/cbor?count=1&page=1&order=desc",
		nil,
	)
	req.SetPathValue("label", "721")
	w := httptest.NewRecorder()
	b.handleMetadataTransactionsCBOR(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "1", w.Header().Get("X-Pagination-Count-Total"))
	assert.Equal(t, "1", w.Header().Get("X-Pagination-Page-Total"))

	var resp []MetadataTransactionCBORResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, "txhash2", resp[0].TxHash)
	require.NotNil(t, resp[0].CborMetadata)
	assert.Equal(t, "a1646e616d65676e66742d74776f", *resp[0].CborMetadata)
	assert.Equal(t, "a1646e616d65676e66742d74776f", resp[0].Metadata)
}

func TestHandleMetadataTransactionsInvalidPagination(t *testing.T) {
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
