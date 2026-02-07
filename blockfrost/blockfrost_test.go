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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockNode implements BlockfrostNode for testing.
type mockNode struct {
	chainTip    ChainTipInfo
	block       BlockInfo
	txHashes    []string
	epoch       EpochInfo
	params      ProtocolParamsInfo
	chainTipErr error
	blockErr    error
	txHashesErr error
	epochErr    error
	paramsErr   error
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
