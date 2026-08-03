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
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newAccountActivityRequest(
	t *testing.T,
	target string,
) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.SetPathValue("stake_address", "stake_test1")
	return req
}

// --- /accounts/{stake_address}/utxos ---

func TestHandleAccountUTXOs(t *testing.T) {
	dataHash := "dh1"
	inlineDatum := "19a6aa"
	refScript := "13a3efd8"
	mock := &mockNode{
		accountUTXOs: []AccountUTXOInfo{
			{
				Address:     "addr_test1",
				TxHash:      "tx1",
				TxIndex:     0,
				OutputIndex: 0,
				Amount: []AddressAmountInfo{
					{Unit: "lovelace", Quantity: "1000000"},
				},
				Block: "block1",
			},
			{
				Address:             "addr_test2",
				TxHash:              "tx2",
				TxIndex:             1,
				OutputIndex:         1,
				Amount:              []AddressAmountInfo{{Unit: "lovelace", Quantity: "2000000"}},
				Block:               "block2",
				DataHash:            &dataHash,
				InlineDatum:         &inlineDatum,
				ReferenceScriptHash: &refScript,
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/utxos?count=1&page=1&order=desc",
	)
	w := httptest.NewRecorder()
	b.handleAccountUTXOs(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "2", w.Header().Get("X-Pagination-Count-Total"))

	var resp []AccountUTXOResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.Len(t, resp, 1)
	assert.Equal(t, "addr_test2", resp[0].Address)
	assert.Equal(t, "tx2", resp[0].TxHash)
	assert.Equal(t, 1, resp[0].TxIndex)
	assert.Equal(t, 1, resp[0].OutputIndex)
	assert.Equal(t, "block2", resp[0].Block)
	require.NotNil(t, resp[0].DataHash)
	assert.Equal(t, "dh1", *resp[0].DataHash)
	require.NotNil(t, resp[0].InlineDatum)
	assert.Equal(t, "19a6aa", *resp[0].InlineDatum)
	require.NotNil(t, resp[0].ReferenceScriptHash)
	assert.Equal(t, "13a3efd8", *resp[0].ReferenceScriptHash)

	// OpenAPI 0.1.90 account_utxo_content: every field is required (data_hash,
	// inline_datum, and reference_script_hash are nullable, not absent).
	assertJSONKeys(t, resp[0], []string{
		"address",
		"tx_hash",
		"tx_index",
		"output_index",
		"amount",
		"block",
		"data_hash",
		"inline_datum",
		"reference_script_hash",
	})
}

func TestHandleAccountUTXOsNullableFieldsNull(t *testing.T) {
	mock := &mockNode{
		accountUTXOs: []AccountUTXOInfo{
			{
				Address:     "addr_test1",
				TxHash:      "tx1",
				TxIndex:     0,
				OutputIndex: 0,
				Amount:      []AddressAmountInfo{{Unit: "lovelace", Quantity: "1000000"}},
				Block:       "block1",
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(t, "/api/v0/accounts/stake_test1/utxos")
	w := httptest.NewRecorder()
	b.handleAccountUTXOs(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp []AccountUTXOResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.Len(t, resp, 1)
	assert.Nil(t, resp[0].DataHash)
	assert.Nil(t, resp[0].InlineDatum)
	assert.Nil(t, resp[0].ReferenceScriptHash)
}

func TestHandleAccountUTXOsEmpty(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(t, "/api/v0/accounts/stake_test1/utxos")
	w := httptest.NewRecorder()
	b.handleAccountUTXOs(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "0", w.Header().Get("X-Pagination-Count-Total"))
	var resp []AccountUTXOResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Empty(t, resp)
}

func TestHandleAccountUTXOsInvalidStakeAddress(t *testing.T) {
	mock := &mockNode{accountUTXOsErr: ErrInvalidStakeAddress}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(t, "/api/v0/accounts/stake_test1/utxos")
	w := httptest.NewRecorder()
	b.handleAccountUTXOs(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "Invalid stake address.", resp.Message)
}

func TestHandleAccountUTXOsNotFound(t *testing.T) {
	mock := &mockNode{accountUTXOsErr: models.ErrAccountNotFound}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(t, "/api/v0/accounts/stake_test1/utxos")
	w := httptest.NewRecorder()
	b.handleAccountUTXOs(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleAccountUTXOsQueryError(t *testing.T) {
	mock := &mockNode{accountUTXOsErr: errors.New("boom")}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(t, "/api/v0/accounts/stake_test1/utxos")
	w := httptest.NewRecorder()
	b.handleAccountUTXOs(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleAccountUTXOsInvalidPagination(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/utxos?count=notanumber",
	)
	w := httptest.NewRecorder()
	b.handleAccountUTXOs(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- /accounts/{stake_address}/withdrawals ---

func TestHandleAccountWithdrawals(t *testing.T) {
	mock := &mockNode{
		accountWithdrawals: []AccountWithdrawalInfo{
			{
				TxHash:      "tx1",
				Amount:      "454541212442",
				TxSlot:      45093580,
				BlockTime:   1646437200,
				BlockHeight: 6745358,
			},
			{
				TxHash:      "tx2",
				Amount:      "97846969",
				TxSlot:      48093580,
				BlockTime:   1649033600,
				BlockHeight: 7126896,
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/withdrawals?count=1&page=2&order=asc",
	)
	w := httptest.NewRecorder()
	b.handleAccountWithdrawals(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "2", w.Header().Get("X-Pagination-Count-Total"))
	assert.Equal(t, "2", w.Header().Get("X-Pagination-Page-Total"))

	var resp []AccountWithdrawalResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.Len(t, resp, 1)
	assert.Equal(t, "tx2", resp[0].TxHash)
	assert.Equal(t, "97846969", resp[0].Amount)
	assert.Equal(t, int64(48093580), resp[0].TxSlot)
	assert.Equal(t, int64(1649033600), resp[0].BlockTime)
	assert.Equal(t, int64(7126896), resp[0].BlockHeight)

	// OpenAPI 0.1.90 account_withdrawal_content required field names.
	assertJSONKeys(t, resp[0], []string{
		"tx_hash",
		"amount",
		"tx_slot",
		"block_time",
		"block_height",
	})
}

func TestHandleAccountWithdrawalsEmpty(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/withdrawals",
	)
	w := httptest.NewRecorder()
	b.handleAccountWithdrawals(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp []AccountWithdrawalResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Empty(t, resp)
}

func TestHandleAccountWithdrawalsInvalidStakeAddress(t *testing.T) {
	mock := &mockNode{accountWithdrawalsErr: ErrInvalidStakeAddress}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/withdrawals",
	)
	w := httptest.NewRecorder()
	b.handleAccountWithdrawals(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAccountWithdrawalsNotFound(t *testing.T) {
	mock := &mockNode{accountWithdrawalsErr: models.ErrAccountNotFound}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/withdrawals",
	)
	w := httptest.NewRecorder()
	b.handleAccountWithdrawals(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleAccountWithdrawalsQueryError(t *testing.T) {
	mock := &mockNode{accountWithdrawalsErr: errors.New("boom")}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/withdrawals",
	)
	w := httptest.NewRecorder()
	b.handleAccountWithdrawals(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --- /accounts/{stake_address}/transactions ---

func TestHandleAccountTransactions(t *testing.T) {
	mock := &mockNode{
		accountTransactions: []AccountTransactionInfo{
			{
				Address:     "addr_test1",
				TxHash:      "tx1",
				TxIndex:     34,
				BlockHeight: 7900364,
				BlockTime:   1666114079,
			},
			{
				Address:     "addr_test2",
				TxHash:      "tx2",
				TxIndex:     6,
				BlockHeight: 7900557,
				BlockTime:   1666118180,
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/transactions?count=1&page=1&order=desc",
	)
	w := httptest.NewRecorder()
	b.handleAccountTransactions(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "2", w.Header().Get("X-Pagination-Count-Total"))

	var resp []AccountTransactionResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.Len(t, resp, 1)
	assert.Equal(t, "addr_test2", resp[0].Address)
	assert.Equal(t, "tx2", resp[0].TxHash)
	assert.Equal(t, 6, resp[0].TxIndex)
	assert.Equal(t, uint64(7900557), resp[0].BlockHeight)
	assert.Equal(t, 1666118180, resp[0].BlockTime)

	// OpenAPI 0.1.90 account_transactions_content required field names.
	assertJSONKeys(t, resp[0], []string{
		"address",
		"tx_hash",
		"tx_index",
		"block_height",
		"block_time",
	})
}

func TestHandleAccountTransactionsEmpty(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/transactions",
	)
	w := httptest.NewRecorder()
	b.handleAccountTransactions(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp []AccountTransactionResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Empty(t, resp)
}

func TestHandleAccountTransactionsInvalidStakeAddress(t *testing.T) {
	mock := &mockNode{accountTransactionsErr: ErrInvalidStakeAddress}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/transactions",
	)
	w := httptest.NewRecorder()
	b.handleAccountTransactions(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAccountTransactionsNotFound(t *testing.T) {
	mock := &mockNode{accountTransactionsErr: models.ErrAccountNotFound}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/transactions",
	)
	w := httptest.NewRecorder()
	b.handleAccountTransactions(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleAccountTransactionsQueryError(t *testing.T) {
	mock := &mockNode{accountTransactionsErr: errors.New("boom")}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/transactions",
	)
	w := httptest.NewRecorder()
	b.handleAccountTransactions(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleAccountTransactionsInvalidPagination(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/transactions?page=notanumber",
	)
	w := httptest.NewRecorder()
	b.handleAccountTransactions(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAccountTransactionsFromToParsed(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t,
		"/api/v0/accounts/stake_test1/transactions?from=8929261&to=9999269:10",
	)
	w := httptest.NewRecorder()
	b.handleAccountTransactions(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, mock.lastAccountTransactionsParams.From)
	assert.Equal(
		t,
		uint64(8929261),
		mock.lastAccountTransactionsParams.From.Block,
	)
	assert.Nil(t, mock.lastAccountTransactionsParams.From.Index)
	require.NotNil(t, mock.lastAccountTransactionsParams.To)
	assert.Equal(
		t,
		uint64(9999269),
		mock.lastAccountTransactionsParams.To.Block,
	)
	require.NotNil(t, mock.lastAccountTransactionsParams.To.Index)
	assert.Equal(
		t,
		uint32(10),
		*mock.lastAccountTransactionsParams.To.Index,
	)
}

func TestHandleAccountTransactionsFromMalformed(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	for _, raw := range []string{"notanumber", "1:notanumber", "-1", "1:2:3"} {
		req := newAccountActivityRequest(
			t, "/api/v0/accounts/stake_test1/transactions?from="+raw,
		)
		w := httptest.NewRecorder()
		b.handleAccountTransactions(w, req)
		assert.Equal(
			t, http.StatusBadRequest, w.Code,
			"from=%q should be rejected", raw,
		)
	}
}

func TestHandleAccountTransactionsToMalformed(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/transactions?to=notanumber",
	)
	w := httptest.NewRecorder()
	b.handleAccountTransactions(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAccountTransactionsInvertedRange(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/transactions?from=100&to=50",
	)
	w := httptest.NewRecorder()
	b.handleAccountTransactions(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAccountTransactionsInvertedRangeSameBlock(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/transactions?from=100:5&to=100:2",
	)
	w := httptest.NewRecorder()
	b.handleAccountTransactions(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAccountTransactionsValidRangeSameBlockNotInverted(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)

	// "from" omits an index (defaults to the start of the block) and "to"
	// pins an explicit index within the same block: not inverted.
	req := newAccountActivityRequest(
		t, "/api/v0/accounts/stake_test1/transactions?from=100&to=100:5",
	)
	w := httptest.NewRecorder()
	b.handleAccountTransactions(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

// --- parseBlockRangePosition / blockRangeInverted unit coverage ---

func TestParseBlockRangePosition(t *testing.T) {
	pos, err := parseBlockRangePosition("100")
	require.NoError(t, err)
	assert.Equal(t, uint64(100), pos.Block)
	assert.Nil(t, pos.Index)

	pos, err = parseBlockRangePosition("100:5")
	require.NoError(t, err)
	assert.Equal(t, uint64(100), pos.Block)
	require.NotNil(t, pos.Index)
	assert.Equal(t, uint32(5), *pos.Index)

	_, err = parseBlockRangePosition("notanumber")
	assert.ErrorIs(t, err, ErrInvalidBlockRange)

	_, err = parseBlockRangePosition("100:notanumber")
	assert.ErrorIs(t, err, ErrInvalidBlockRange)
}

func TestBlockRangeInverted(t *testing.T) {
	idx := func(v uint32) *uint32 { return &v }

	assert.True(t, blockRangeInverted(
		BlockRangePosition{Block: 100},
		BlockRangePosition{Block: 50},
	))
	assert.False(t, blockRangeInverted(
		BlockRangePosition{Block: 50},
		BlockRangePosition{Block: 100},
	))
	assert.True(t, blockRangeInverted(
		BlockRangePosition{Block: 100, Index: idx(5)},
		BlockRangePosition{Block: 100, Index: idx(2)},
	))
	assert.False(t, blockRangeInverted(
		BlockRangePosition{Block: 100, Index: idx(2)},
		BlockRangePosition{Block: 100, Index: idx(5)},
	))
	// Ambiguous same-block comparisons with a missing index are not
	// treated as inverted.
	assert.False(t, blockRangeInverted(
		BlockRangePosition{Block: 100},
		BlockRangePosition{Block: 100, Index: idx(0)},
	))
}
