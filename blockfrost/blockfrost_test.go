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
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intPtr(v int) *int {
	return &v
}

// mockNode implements BlockfrostNode for testing.
type mockNode struct {
	chainTip               ChainTipInfo
	block                  BlockInfo
	txHashes               []string
	epoch                  EpochInfo
	params                 ProtocolParamsInfo
	epochParams            ProtocolParamsInfo
	pools                  []PoolExtendedInfo
	addressUTXOs           []AddressUTXOInfo
	addressTransactions    []AddressTransactionInfo
	metadataJSON           []MetadataTransactionJSONInfo
	metadataCBOR           []MetadataTransactionCBORInfo
	addressUTXOsTotal      int
	addressTxsTotal        int
	metadataJSONTotal      int
	metadataCBORTotal      int
	account                AccountInfo
	addresses              []AccountAssociatedAddressInfo
	delegations            []AccountDelegationHistoryInfo
	regs                   []AccountRegistrationHistoryInfo
	rewards                []AccountRewardHistoryInfo
	chainTipErr            error
	blockErr               error
	txHashesErr            error
	epochErr               error
	paramsErr              error
	epochParamsErr         error
	poolsErr               error
	addressUTXOsErr        error
	addressTransactionsErr error
	metadataJSONErr        error
	metadataCBORErr        error
	accountErr             error
	addressesErr           error
	delegationsErr         error
	regsErr                error
	rewardsErr             error
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

func (m *mockNode) PoolsExtended() (
	[]PoolExtendedInfo, error,
) {
	return m.pools, m.poolsErr
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
						Port: intPtr(3002),
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
						Port: intPtr(3001),
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
	mock := &mockNode{
		account: AccountInfo{
			StakeAddress:       "stake_test1",
			Active:             true,
			ControlledAmount:   "123",
			RewardsSum:         "10",
			WithdrawalsSum:     "0",
			ReservesSum:        "0",
			TreasurySum:        "0",
			WithdrawableAmount: "10",
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
	assert.Equal(t, "10", resp.WithdrawableAmount)
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
			},
			{
				ActiveEpoch: 2,
				TxHash:      "tx2",
				Amount:      "0",
				PoolID:      "pool2",
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
}

func TestHandleAccountRegistrationHistory(t *testing.T) {
	mock := &mockNode{
		regs: []AccountRegistrationHistoryInfo{
			{TxHash: "tx1", Action: "registered"},
			{TxHash: "tx2", Action: "deregistered"},
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
	mock := &mockNode{}
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
