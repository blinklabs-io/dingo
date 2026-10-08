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
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/btcsuite/btcd/btcutil/bech32"
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
	t.Parallel()

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
				Address:     "addr_test2",
				TxHash:      "tx2",
				TxIndex:     1,
				OutputIndex: 1,
				Amount: []AddressAmountInfo{
					{Unit: "lovelace", Quantity: "2000000"},
				},
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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	mock := &mockNode{accountUTXOsErr: models.ErrAccountNotFound}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(t, "/api/v0/accounts/stake_test1/utxos")
	w := httptest.NewRecorder()
	b.handleAccountUTXOs(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleAccountUTXOsQueryError(t *testing.T) {
	t.Parallel()

	mock := &mockNode{accountUTXOsErr: errors.New("boom")}
	b := newTestBlockfrost(mock)

	req := newAccountActivityRequest(t, "/api/v0/accounts/stake_test1/utxos")
	w := httptest.NewRecorder()
	b.handleAccountUTXOs(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --- /accounts/{stake_address}/withdrawals ---

func TestHandleAccountWithdrawals(t *testing.T) {
	t.Parallel()

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

// --- /accounts/{stake_address}/transactions ---

func TestHandleAccountTransactions(t *testing.T) {
	t.Parallel()

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
		t,
		"/api/v0/accounts/stake_test1/transactions?count=1&page=1&order=desc",
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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

// TestSignedSumText pins the rendering of the account reserves_sum and
// treasury_sum fields. Both aggregate delta_coin rows, so a net negative total
// has to reach the response with its sign rather than as its magnitude, and an
// account with no MIR history has to render as "0" rather than an empty string.
func TestSignedSumText(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		value *big.Int
		want  string
	}{
		{name: "nil renders as zero", value: nil, want: "0"},
		{name: "zero", value: big.NewInt(0), want: "0"},
		{name: "positive", value: big.NewInt(1_200), want: "1200"},
		{name: "negative keeps its sign", value: big.NewInt(-200), want: "-200"},
		{
			name:  "beyond int64",
			value: new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 70)),
			want:  "-1180591620717411303424",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, signedSumText(test.value))
		})
	}
}

// vrfKey and hotVkey are arbitrary 32-byte values used to exercise the header
// field extraction. The exact bytes don't matter, only that they round-trip.
var (
	testVRFKey = mustHex(
		"00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
	)
	testHotVkey = mustHex(
		"ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100",
	)
)

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

// expectedVRFBech32 encodes the test VRF key exactly as praosHeaderFields
// should, giving the tests a reference value.
func expectedVRFBech32(t *testing.T) string {
	t.Helper()
	conv, err := bech32.ConvertBits(testVRFKey, 8, 5, true)
	require.NoError(t, err)
	encoded, err := bech32.Encode("vrf_vk", conv)
	require.NoError(t, err)
	return encoded
}

func TestPraosHeaderFieldsShelley(t *testing.T) {
	t.Parallel()

	var h shelley.ShelleyBlockHeader
	h.Body.VrfKey = testVRFKey
	h.Body.OpCertHotVkey = testHotVkey
	h.Body.OpCertSequenceNumber = 7

	vrf, opCert, counter := praosHeaderFields(&h)

	require.NotNil(t, vrf)
	assert.Equal(t, expectedVRFBech32(t), *vrf)
	assert.True(t, strings.HasPrefix(*vrf, "vrf_vk1"))
	require.NotNil(t, opCert)
	assert.Equal(t, hex.EncodeToString(testHotVkey), *opCert)
	require.NotNil(t, counter)
	assert.Equal(t, "7", *counter)
}

func TestPraosHeaderFieldsAllegraTPraos(t *testing.T) {
	t.Parallel()

	// Allegra embeds the Shelley header body (TPraos era).
	var h allegra.AllegraBlockHeader
	h.Body.VrfKey = testVRFKey
	h.Body.OpCertHotVkey = testHotVkey
	h.Body.OpCertSequenceNumber = 0

	vrf, opCert, counter := praosHeaderFields(&h)

	require.NotNil(t, vrf)
	assert.Equal(t, expectedVRFBech32(t), *vrf)
	require.NotNil(t, opCert)
	assert.Equal(t, hex.EncodeToString(testHotVkey), *opCert)
	require.NotNil(t, counter)
	assert.Equal(t, "0", *counter)
}

func TestPraosHeaderFieldsBabbage(t *testing.T) {
	t.Parallel()

	var h babbage.BabbageBlockHeader
	h.Body.VrfKey = testVRFKey
	h.Body.OpCert.HotVkey = testHotVkey
	h.Body.OpCert.SequenceNumber = 42

	vrf, opCert, counter := praosHeaderFields(&h)

	require.NotNil(t, vrf)
	assert.Equal(t, expectedVRFBech32(t), *vrf)
	require.NotNil(t, opCert)
	assert.Equal(t, hex.EncodeToString(testHotVkey), *opCert)
	require.NotNil(t, counter)
	assert.Equal(t, "42", *counter)
}

func TestPraosHeaderFieldsConway(t *testing.T) {
	t.Parallel()

	var h conway.ConwayBlockHeader
	h.Body.VrfKey = testVRFKey
	h.Body.OpCert.HotVkey = testHotVkey
	h.Body.OpCert.SequenceNumber = 123

	vrf, opCert, counter := praosHeaderFields(&h)

	require.NotNil(t, vrf)
	assert.Equal(t, expectedVRFBech32(t), *vrf)
	require.NotNil(t, opCert)
	assert.Equal(t, hex.EncodeToString(testHotVkey), *opCert)
	require.NotNil(t, counter)
	assert.Equal(t, "123", *counter)
}

// TestPraosHeaderFieldsByron covers the genesis/pre-Shelley edge case: Byron
// headers carry no VRF or operational certificate, so all three fields are nil.
func TestPraosHeaderFieldsByron(t *testing.T) {
	t.Parallel()

	var h byron.ByronMainBlockHeader
	// Sanity check that the Byron header satisfies the header interface used
	// by praosHeaderFields.
	var _ gledger.BlockHeader = &h

	vrf, opCert, counter := praosHeaderFields(&h)

	assert.Nil(t, vrf)
	assert.Nil(t, opCert)
	assert.Nil(t, counter)
}

// TestPraosHeaderFieldsEmptyVRFAndOpCert verifies that empty header byte slices
// (which can occur for malformed or partially populated headers) do not produce
// empty-string values; block_vrf/op_cert stay nil while the counter is always
// reported.
func TestPraosHeaderFieldsEmptyVRFAndOpCert(t *testing.T) {
	t.Parallel()

	var h conway.ConwayBlockHeader
	h.Body.OpCert.SequenceNumber = 5

	vrf, opCert, counter := praosHeaderFields(&h)

	assert.Nil(t, vrf)
	assert.Nil(t, opCert)
	require.NotNil(t, counter)
	assert.Equal(t, "5", *counter)
}

func TestBech32EncodeDataVRF(t *testing.T) {
	t.Parallel()

	encoded, err := bech32EncodeData("vrf_vk", testVRFKey)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(encoded, "vrf_vk1"))

	// Round-trip: decoding must recover the original bytes.
	hrp, data, err := bech32.Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, "vrf_vk", hrp)
	decoded, err := bech32.ConvertBits(data, 5, 8, false)
	require.NoError(t, err)
	assert.Equal(t, testVRFKey, decoded)
}

// depositReturnAddress builds reward-account address bytes for stakeCred,
// suitable for a GovernanceProposal's ReturnAddress.
func depositReturnAddress(t *testing.T, stakeCred []byte) []byte {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	addrBytes, err := addr.Bytes()
	require.NoError(t, err)
	return addrBytes
}

// TestPredefinedDRepAmountIncludesActiveProposalDeposit proves the
// Blockfrost single-DRep endpoint reports the same CIP-1694
// deposit-inclusive voting power ledger/governance.LoadDRepVotingState uses
// for real ratification, not the plain
// GetDRepVotingPowerByType figure alone, for the AlwaysNoConfidence
// predefined DRep.
//
// The credential-backed single-DRep endpoint (drepByCredentialTag) merges
// deposit power through the identical map lookup this test and
// TestDRepsListAmountsIncludeActiveProposalDeposit already exercise, but
// additionally resolves a registration epoch via the ledger's genesis
// hard-fork summary -- machinery a fresh, block-less test ledger state
// cannot produce -- so it is not separately exercised end-to-end here.
func TestPredefinedDRepAmountIncludesActiveProposalDeposit(t *testing.T) {
	t.Parallel()

	adapter, _, db := newDBBackedAdapter(t)
	returnStakeCred := []byte{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4}

	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: returnStakeCred,
			DrepType:   models.DrepTypeAlwaysNoConfidence,
			AddedSlot:  1,
			Active:     true,
		}),
	)
	require.NoError(
		t,
		db.SetGovernanceProposal(
			context.Background(),
			&models.GovernanceProposal{
				TxHash:        []byte("proposal-tx-hash-32-bytes-long2"),
				ActionIndex:   0,
				ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
				ProposedEpoch: 0,
				ExpiresEpoch:  100,
				Deposit:       75,
				ReturnAddress: depositReturnAddress(t, returnStakeCred),
				AnchorURL:     "https://example.invalid/deposit",
				AnchorHash:    []byte("anchor-hash-32-bytes-long-valu2"),
				AddedSlot:     1,
			},
			nil,
		),
	)

	drepType := models.DrepTypeAlwaysNoConfidence
	info, err := adapter.DRep(DRepCredential{
		ID:         "drep_always_no_confidence",
		Predefined: &drepType,
	})
	require.NoError(t, err)
	assert.Equal(t, "75", info.Amount)
}

// TestDRepsListAmountsIncludeActiveProposalDeposit is the batch-listing
// counterpart: the /governance/dreps page must report the same
// deposit-inclusive amount as the single-DRep endpoint.
func TestDRepsListAmountsIncludeActiveProposalDeposit(t *testing.T) {
	t.Parallel()

	adapter, _, db := newDBBackedAdapter(t)
	drepCred := []byte{5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5}
	returnStakeCred := []byte{6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}

	require.NoError(t, db.CreateDrep(context.Background(), nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
		AddedSlot:  1,
	}))
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: returnStakeCred,
			Drep:       drepCred,
			DrepType:   models.DrepTypeAddrKeyHash,
			AddedSlot:  1,
			Active:     true,
		}),
	)
	require.NoError(
		t,
		db.SetGovernanceProposal(
			context.Background(),
			&models.GovernanceProposal{
				TxHash:        []byte("proposal-tx-hash-32-bytes-long3"),
				ActionIndex:   0,
				ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
				ProposedEpoch: 0,
				ExpiresEpoch:  100,
				Deposit:       30,
				ReturnAddress: depositReturnAddress(t, returnStakeCred),
				AnchorURL:     "https://example.invalid/deposit",
				AnchorHash:    []byte("anchor-hash-32-bytes-long-valu3"),
				AddedSlot:     1,
			},
			nil,
		),
	)

	items, total, err := adapter.DReps(DRepListParams{
		Pagination: PaginationParams{
			Count: 100,
			Page:  1,
			Order: PaginationOrderAsc,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, items, 1)
	assert.Equal(t, "30", items[0].Amount)
}

type listPaginationNode struct {
	*mockNode
	calls  int
	params PaginationParams
}

func (n *listPaginationNode) PoolsExtended() ([]PoolExtendedInfo, error) {
	n.calls++
	return []PoolExtendedInfo{}, nil
}

func (n *listPaginationNode) AccountAssociatedAddresses(ctx context.Context,
	_ string,
	params PaginationParams,
) ([]AccountAssociatedAddressInfo, int, error) {
	n.calls++
	n.params = params
	return []AccountAssociatedAddressInfo{}, 0, nil
}

func (n *listPaginationNode) AccountDelegationHistory(ctx context.Context,
	_ string,
	params PaginationParams,
) ([]AccountDelegationHistoryInfo, int, error) {
	n.calls++
	n.params = params
	return []AccountDelegationHistoryInfo{}, 0, nil
}

func (n *listPaginationNode) AccountRegistrationHistory(ctx context.Context,
	_ string,
	params PaginationParams,
) ([]AccountRegistrationHistoryInfo, int, error) {
	n.calls++
	n.params = params
	return []AccountRegistrationHistoryInfo{}, 0, nil
}

func (n *listPaginationNode) AccountRewardHistory(ctx context.Context,
	_ string,
	params PaginationParams,
) ([]AccountRewardHistoryInfo, int, error) {
	n.calls++
	n.params = params
	return []AccountRewardHistoryInfo{}, 0, nil
}

func (n *listPaginationNode) AccountUTXOs(ctx context.Context,
	_ string,
	params PaginationParams,
) ([]AccountUTXOInfo, int, error) {
	n.calls++
	n.params = params
	return []AccountUTXOInfo{}, 0, nil
}

func (n *listPaginationNode) AccountWithdrawals(ctx context.Context,
	_ string,
	params PaginationParams,
) ([]AccountWithdrawalInfo, int, error) {
	n.calls++
	n.params = params
	return []AccountWithdrawalInfo{}, 0, nil
}

func (n *listPaginationNode) AccountTransactions(ctx context.Context,
	_ string,
	params AccountTransactionsParams,
) ([]AccountTransactionInfo, int, error) {
	n.calls++
	n.params = params.Pagination
	return []AccountTransactionInfo{}, 0, nil
}

// TestListRoutesRejectOutOfRangePagination is the only test that drives the
// six paginated account routes through the router, so it is the only guard on
// handlePaginatedAccountRequest's parse gate: removing that gate leaves every
// other test in this package green. It is also the only test that pins the
// parsed defaults (100/1/asc) reaching the adapter, since nothing else
// references DefaultPaginationCount or DefaultPaginationPage.
func TestListRoutesRejectOutOfRangePagination(t *testing.T) {
	routes := []string{
		"/api/v0/pools/extended",
		"/api/v0/accounts/stake_test1/addresses",
		"/api/v0/accounts/stake_test1/delegations",
		"/api/v0/accounts/stake_test1/registrations",
		"/api/v0/accounts/stake_test1/rewards",
		"/api/v0/accounts/stake_test1/utxos",
		"/api/v0/accounts/stake_test1/withdrawals",
		"/api/v0/accounts/stake_test1/transactions",
	}
	for _, route := range routes {
		t.Run(route, func(t *testing.T) {
			for _, query := range []string{
				"count=0", "count=-1", "count=101", "count=abc",
				"page=0", "page=-1", "page=21474837", "page=abc",
				"order=sideways",
			} {
				t.Run(query, func(t *testing.T) {
					node := &listPaginationNode{mockNode: &mockNode{}}
					b := newTestBlockfrost(node)
					recorder := httptest.NewRecorder()
					request := httptest.NewRequest(
						http.MethodGet, route+"?"+query, nil,
					)
					b.handler().ServeHTTP(recorder, request)
					require.Zero(
						t,
						node.calls,
						"invalid pagination must not reach the adapter",
					)
					require.Equal(t, http.StatusBadRequest, recorder.Code)
					require.JSONEq(
						t,
						`{"status_code":400,"error":"Bad Request","message":"Invalid pagination parameters."}`,
						recorder.Body.String(),
					)
				})
			}
			for _, tc := range []struct {
				name  string
				query string
				want  PaginationParams
			}{
				{"defaults", "", PaginationParams{Count: 100, Page: 1, Order: "asc"}},
				{"minimum", "count=1&page=1&order=asc", PaginationParams{Count: 1, Page: 1, Order: "asc"}},
				{"maximum", "count=100&page=21474836&order=desc", PaginationParams{Count: 100, Page: 21474836, Order: "desc"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					node := &listPaginationNode{mockNode: &mockNode{}}
					b := newTestBlockfrost(node)
					recorder := httptest.NewRecorder()
					request := httptest.NewRequest(
						http.MethodGet, route+"?"+tc.query, nil,
					)
					b.handler().ServeHTTP(recorder, request)
					require.Equal(t, http.StatusOK, recorder.Code)
					require.Equal(t, 1, node.calls)
					if route != "/api/v0/pools/extended" {
						require.Equal(t, tc.want, node.params)
					}
					require.JSONEq(t, `[]`, recorder.Body.String())
				})
			}
		})
	}
}

func startOnFreePort(
	t *testing.T,
	ctx context.Context,
	cfg BlockfrostConfig,
) (*Blockfrost, string) {
	t.Helper()
	var lastErr error
	for range testutil.BindAttempts {
		addr := testutil.FreePort(t)
		cfg.ListenAddress = addr
		srv := New(cfg, &mockNode{}, nil)
		attemptCtx, cancel := context.WithCancel(ctx)
		lastErr = srv.Start(attemptCtx)
		if lastErr == nil {
			t.Cleanup(cancel)
			return srv, addr
		}
		cancel()
	}
	t.Fatalf("could not start on a free loopback port: %v", lastErr)
	return nil, ""
}

// stopNow shuts srv down under a bounded context, matching the package's
// existing convention (tls_auth_test.go): a hang in the teardown path should
// fail the test rather than stall the suite until go test's own timeout.
func stopNow(t *testing.T, srv *Blockfrost) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	return srv.Stop(ctx)
}

// The shutdown protocol this test exercises is covered in depth, with the
// windows constructed rather than raced, in internal/apilistener. What is
// checked here is that this package is wired to it -- that a Blockfrost server
// keeps the promise its Stop makes.

// TestServerRebindsAfterStop is the production path this fix exists for: a
// live database restore or truncate quiesces the API capabilities and
// reinitializeAPIServers brings them back up on the same configured port (see
// node_lifecycle.go). A Stop that returned while the socket was still bound
// left that restart failing with EADDRINUSE. The constructed tests in
// internal/apilistener assert closure on the original listener object; dialing
// a released ephemeral address here could instead reach another package's
// listener when the suite runs concurrently.
func TestServerRebindsAfterStop(t *testing.T) {
	t.Parallel()

	srv, addr := startOnFreePort(t, t.Context(), BlockfrostConfig{})
	require.NoError(t, stopNow(t, srv))

	restarted := New(
		BlockfrostConfig{ListenAddress: addr}, &mockNode{}, nil,
	)
	require.NoError(
		t, restarted.Start(t.Context()),
		"a capability restart must rebind the port Stop released",
	)
	require.NoError(t, stopNow(t, restarted))
}

// TestStartIsRefusedWhileAnotherStartHoldsTheGate pins the start gate this
// package is wired to. TestStartAlreadyStarted asserts only the
// already-published server rejection, which Publish reports, so without this
// test the BeginStart/EndStart pair can be removed from Start with the suite
// green.
func TestStartIsRefusedWhileAnotherStartHoldsTheGate(t *testing.T) {
	t.Parallel()

	srv := New(
		BlockfrostConfig{ListenAddress: testutil.FreePort(t)},
		&mockNode{},
		nil,
	)

	held, err := srv.listener.BeginStart()
	require.NoError(t, err)

	err = srv.Start(t.Context())
	require.ErrorContains(
		t, err, "start already in progress",
		"Start must take the listener's start gate before publishing",
	)
	require.Nil(
		t, srv.listener.Server(),
		"a refused Start must not publish a server",
	)

	srv.listener.EndStart(held)
	require.NoError(
		t, srv.Start(t.Context()),
		"the gate must be available again once the holder releases it",
	)
	require.NoError(t, stopNow(t, srv))
}

// TestServerShutdownOnContextCancel asserts cancelling the context passed to
// Start releases the port, which is how the node stops this API during its own
// shutdown. Nothing else in the package reads the Start context, so without
// this test the listener.Watch call can be removed from Start with the suite
// green.
func TestServerShutdownOnContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	_, addr := startOnFreePort(t, ctx, BlockfrostConfig{})

	cancel()

	testutil.WaitForCondition(
		t,
		func() bool { return !portAccepts(addr) },
		5*time.Second,
		"listener still accepting after context cancel",
	)
}

// portAccepts reports whether a TCP connection to addr succeeds.
func portAccepts(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func TestHandlePoolDetail(t *testing.T) {
	t.Parallel()

	mock := &mockNode{
		poolDetail: PoolDetailInfo{
			PoolID:         "pool1vzqtn3mtfvvuy8ghksy34gs9g97tszj5f8mr3sn7asy5vk577ec",
			Hex:            "6080b9c76b4b19c21d17b4091aa205417cb80a5449f638c27eec0946",
			VrfKey:         "0b5245f9934ec2151116fb8ec00f35fd00e0aa3b075c4ed12cce440f999d823",
			BlocksMinted:   69,
			BlocksEpoch:    4,
			LiveStake:      "6900000000",
			LiveSize:       0.42,
			LiveSaturation: 0.93,
			LiveDelegators: 127,
			ActiveStake:    "4200000000",
			ActiveSize:     0.43,
			DeclaredPledge: "5000000000",
			LivePledge:     "5000000001",
			MarginCost:     0.05,
			FixedCost:      "340000000",
			RewardAccount:  "stake1uxkptsa4lkr55jleztw43t37vgdn88l6ghclfwuxld2eykgpgvg3f",
			Owners: []string{
				"stake1u98nnlkvkk23vtvf9273uq7cph5ww6u2yq2389psuqet90sv4xv9v",
			},
			Registration: []string{
				"9f83e5484f543e05b52e99988272a31da373f3aab4c064c76db96643a355d9dc",
			},
			Retirement: []string{},
			CalidusKey: nil,
		},
	}
	b := newTestBlockfrost(mock)
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/pools/pool1vzqtn3mtfvvuy8ghksy34gs9g97tszj5f8mr3sn7asy5vk577ec",
		nil,
	)
	req.SetPathValue(
		"pool_id",
		"pool1vzqtn3mtfvvuy8ghksy34gs9g97tszj5f8mr3sn7asy5vk577ec",
	)
	w := httptest.NewRecorder()
	b.handlePoolDetail(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	// Every field name and type must match the OpenAPI 0.1.90 pool schema
	// exactly: string amounts, float ratios, integer counts, and a
	// nullable calidus_key that must be present in the payload as null,
	// not omitted.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &raw))
	calidusKey, ok := raw["calidus_key"]
	require.True(t, ok, "calidus_key must be present in the response")
	assert.Nil(t, calidusKey)

	var resp PoolDetailResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, mock.poolDetail.PoolID, resp.PoolID)
	assert.Equal(t, mock.poolDetail.Hex, resp.Hex)
	assert.Equal(t, mock.poolDetail.VrfKey, resp.VrfKey)
	assert.Equal(t, uint64(69), resp.BlocksMinted)
	assert.Equal(t, uint64(4), resp.BlocksEpoch)
	assert.Equal(t, "6900000000", resp.LiveStake)
	assert.InDelta(t, 0.42, resp.LiveSize, 0.0001)
	assert.InDelta(t, 0.93, resp.LiveSaturation, 0.0001)
	assert.Equal(t, uint64(127), resp.LiveDelegators)
	assert.Equal(t, "4200000000", resp.ActiveStake)
	assert.InDelta(t, 0.43, resp.ActiveSize, 0.0001)
	assert.Equal(t, "5000000000", resp.DeclaredPledge)
	assert.Equal(t, "5000000001", resp.LivePledge)
	assert.InDelta(t, 0.05, resp.MarginCost, 0.0001)
	assert.Equal(t, "340000000", resp.FixedCost)
	assert.Equal(t, mock.poolDetail.RewardAccount, resp.RewardAccount)
	assert.Equal(t, mock.poolDetail.Owners, resp.Owners)
	assert.Equal(t, mock.poolDetail.Registration, resp.Registration)
	assert.Empty(t, resp.Retirement)
	assert.NotNil(t, resp.Retirement)
	assert.Nil(t, resp.CalidusKey)
}

// TestHandlePoolDetailEmptyArraysNotNull guards the non-nullable owners,
// registration, and retirement arrays: a zero-value PoolDetailInfo (nil
// slices) must still encode as "[]", never JSON null, since the OpenAPI
// schema marks them required arrays without nullable: true.
func TestHandlePoolDetailEmptyArraysNotNull(t *testing.T) {
	t.Parallel()

	mock := &mockNode{poolDetail: PoolDetailInfo{PoolID: "pool1empty"}}
	b := newTestBlockfrost(mock)
	req := httptest.NewRequest(http.MethodGet, "/api/v0/pools/pool1empty", nil)
	req.SetPathValue("pool_id", "pool1empty")
	w := httptest.NewRecorder()
	b.handlePoolDetail(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &raw))
	for _, field := range []string{"owners", "registration", "retirement"} {
		v, ok := raw[field]
		require.True(t, ok, "%s must be present", field)
		arr, isArray := v.([]any)
		require.True(
			t,
			isArray,
			"%s must encode as a JSON array, got %T",
			field,
			v,
		)
		assert.Empty(t, arr)
	}
}

func TestHandlePoolDetailInvalidID(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{poolDetailErr: ErrInvalidPoolID})
	req := httptest.NewRequest(http.MethodGet, "/api/v0/pools/pool1stonks", nil)
	req.SetPathValue("pool_id", "pool1stonks")
	w := httptest.NewRecorder()
	b.handlePoolDetail(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "Invalid or malformed pool id format.", resp.Message)
}

func TestHandlePoolDetailNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{
		poolDetailErr: fmt.Errorf("get pool: %w", models.ErrPoolNotFound),
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/pools/pool1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq8a7a2d",
		nil,
	)
	req.SetPathValue(
		"pool_id",
		"pool1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq8a7a2d",
	)
	w := httptest.NewRecorder()
	b.handlePoolDetail(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// TestHandlePoolDetailDatabaseFailure covers an opaque backing-store error
// (neither the invalid-ID nor not-found sentinels): it must surface as a
// generic 500 rather than being misclassified as a 400/404.
func TestHandlePoolDetailDatabaseFailure(t *testing.T) {
	t.Parallel()

	b := newTestBlockfrost(&mockNode{
		poolDetailErr: errors.New("database is closed"),
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/pools/pool1whatever",
		nil,
	)
	req.SetPathValue("pool_id", "pool1whatever")
	w := httptest.NewRecorder()
	b.handlePoolDetail(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "failed to retrieve pool detail", resp.Message)
}

// TestPoolsRouteOrderingPoolDetailDoesNotSwallowSiblings is the acceptance
// test for route registration: even though "/pools/{pool_id}" is
// registered, requests for the literal sibling paths "/pools/retiring" and
// "/pools/extended" must still resolve to their own handlers, not be
// captured by the pool-detail wildcard. This exercises the real
// http.ServeMux built by (*Blockfrost).handler(), not a direct method
// call, so it verifies Go's actual pattern-specificity resolution rather
// than assuming it.
func TestPoolsRouteOrderingPoolDetailDoesNotSwallowSiblings(t *testing.T) {
	t.Parallel()

	mock := &mockNode{
		poolsRetiringTotal: 1,
		poolsRetiring: []PoolRetiringInfo{
			{PoolID: "pool1retiring", Epoch: 10},
		},
		pools: []PoolExtendedInfo{
			{PoolID: "pool1extended"},
		},
		poolDetail: PoolDetailInfo{PoolID: "pool1detail"},
	}
	b := newTestBlockfrost(mock)
	handler := b.handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v0/pools/retiring", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var retiringResp []PoolRetiringResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&retiringResp))
	require.Len(t, retiringResp, 1)
	assert.Equal(t, "pool1retiring", retiringResp[0].PoolID)

	req = httptest.NewRequest(http.MethodGet, "/api/v0/pools/extended", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var extendedResp []PoolExtendedResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&extendedResp))
	require.Len(t, extendedResp, 1)
	assert.Equal(t, "pool1extended", extendedResp[0].PoolID)

	req = httptest.NewRequest(
		http.MethodGet, "/api/v0/pools/pool1notretiringnorextended", nil,
	)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var detailResp PoolDetailResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&detailResp))
	assert.Equal(t, "pool1detail", detailResp.PoolID)

	req = httptest.NewRequest(
		http.MethodGet, "/api/v0/pools/pool1detail/metadata", nil,
	)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestPoolSizeSaturation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                                             string
		liveStake, activeStake, totalLive, totalActive   uint64
		totalCirculation                                 uint64
		nOpt                                             int
		wantLiveSize, wantActiveSize, wantLiveSaturation float64
	}{
		{
			// totalCirculation is deliberately different from totalActive
			// here (as it always is in practice: circulating supply is
			// always larger than total staked), so this case would catch
			// live_saturation being computed against the wrong
			// denominator.
			name:               "normal pool",
			liveStake:          6_900_000_000,
			activeStake:        4_200_000_000,
			totalLive:          16_428_571_428,
			totalActive:        420_000_000_000,
			totalCirculation:   700_000_000_000,
			nOpt:               500,
			wantLiveSize:       6_900_000_000.0 / 16_428_571_428.0,
			wantActiveSize:     4_200_000_000.0 / 420_000_000_000.0,
			wantLiveSaturation: 6_900_000_000.0 / (700_000_000_000.0 / 500.0),
		},
		{
			name:               "zero total live stake",
			liveStake:          1000,
			totalLive:          0,
			totalActive:        1_000_000,
			activeStake:        500,
			totalCirculation:   2_000_000,
			nOpt:               100,
			wantLiveSize:       0,
			wantActiveSize:     500.0 / 1_000_000.0,
			wantLiveSaturation: 1000.0 / (2_000_000.0 / 100.0),
		},
		{
			// totalActive == 0 (no snapshot captured) no longer affects
			// live_saturation at all: it now depends solely on
			// totalCirculation and nOpt, so it comes out nonzero here even
			// though active_size is forced to zero by the totalActive
			// guard. PoolDetail itself errors before calling this function
			// when totalActive == 0 (see the active_size doc comment on
			// poolSizeSaturation in adapter_pool_detail.go); this case
			// only pins poolSizeSaturation's own defensive guard.
			name:               "zero total active stake",
			liveStake:          1000,
			totalLive:          10_000,
			activeStake:        0,
			totalActive:        0,
			totalCirculation:   5_000_000,
			nOpt:               100,
			wantLiveSize:       1000.0 / 10_000.0,
			wantActiveSize:     0,
			wantLiveSaturation: 1000.0 / (5_000_000.0 / 100.0),
		},
		{
			// Defensive zero-denominator guard only: PoolDetail never calls
			// this with nOpt == 0 in practice, since it now requires
			// CurrentProtocolParams to succeed before computing saturation
			// at all.
			name:               "nOpt is zero",
			liveStake:          1000,
			totalLive:          10_000,
			activeStake:        500,
			totalActive:        1_000_000,
			totalCirculation:   2_000_000,
			nOpt:               0,
			wantLiveSize:       1000.0 / 10_000.0,
			wantActiveSize:     500.0 / 1_000_000.0,
			wantLiveSaturation: 0,
		},
		{
			// Defensive zero-denominator guard only: PoolDetail never calls
			// this with totalCirculation == 0 in practice, since it now
			// requires totalCirculation to be computed successfully before
			// calling this function at all.
			name:               "zero total circulation",
			liveStake:          1000,
			totalLive:          10_000,
			activeStake:        500,
			totalActive:        1_000_000,
			totalCirculation:   0,
			nOpt:               100,
			wantLiveSize:       1000.0 / 10_000.0,
			wantActiveSize:     500.0 / 1_000_000.0,
			wantLiveSaturation: 0,
		},
		{
			name: "all zero",
		},
		{
			// Mainnet-shaped: circulating supply and total staked diverge
			// enough (~1.68x) that the two denominators disagree sharply.
			// A pool at 72M ADA live stake should land at ~1.0 saturation
			// against the correct denominator (circulating supply / nOpt =
			// 72.2M ADA), the shape the pre-fix formula (totalActive /
			// nOpt = 43M ADA) could not express: it would have reported
			// this same pool at ~1.674. Values are lovelace
			// (1 ADA = 1_000_000 lovelace).
			name:               "mainnet-shaped: circulating vs staked diverge",
			liveStake:          72_000_000_000_000,     // 72M ADA
			activeStake:        72_000_000_000_000,     // 72M ADA
			totalLive:          21_500_000_000_000_000, // 21.5B ADA
			totalActive:        21_500_000_000_000_000, // 21.5B ADA
			totalCirculation:   36_100_000_000_000_000, // 36.1B ADA
			nOpt:               500,
			wantLiveSize:       72_000_000_000_000.0 / 21_500_000_000_000_000.0,
			wantActiveSize:     72_000_000_000_000.0 / 21_500_000_000_000_000.0,
			wantLiveSaturation: 72_000_000_000_000.0 / (36_100_000_000_000_000.0 / 500.0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			liveSize, activeSize, liveSaturation := poolSizeSaturation(
				tt.liveStake, tt.activeStake, tt.totalLive, tt.totalActive,
				tt.totalCirculation, tt.nOpt,
			)
			assert.InDelta(t, tt.wantLiveSize, liveSize, 1e-9)
			assert.InDelta(t, tt.wantActiveSize, activeSize, 1e-9)
			assert.InDelta(t, tt.wantLiveSaturation, liveSaturation, 1e-6)
		})
	}
}

// A freshly constructed LedgerState has never loaded protocol parameters, so
// GetCurrentPParams returns nil — the same thing it genuinely reports during a
// Byron prefix, where there is no protocol-parameter CBOR to load.

// TestCurrentProtocolParams_ByronEraSentinel pins the adapter-level contract:
// Byron-era unavailability surfaces as a sentinel callers can branch on, not
// as an opaque string that every caller has to treat as an internal fault.
func TestCurrentProtocolParams_ByronEraSentinel(t *testing.T) {
	t.Parallel()

	adapter, _, _ := newDBBackedAdapter(t)
	require.Nil(
		t,
		adapter.ledgerState.GetCurrentPParams(),
		"precondition: ledger reports no current pparams",
	)

	info, err := adapter.CurrentProtocolParams()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrProtocolParamsUnavailable)
	assert.Equal(
		t,
		ProtocolParamsInfo{},
		info,
		"no Shelley-shaped substitute alongside the error",
	)
}

// TestHandleLatestEpochParams_ByronEraNotFound is the behavior change an
// operator sees. A Byron prefix is an expected point in a from-genesis sync,
// so GET /epochs/latest/parameters must not report 500 Internal Server Error
// — that reads as a node fault and trips alerting. 404 matches the
// ErrEpochNotFound precedent already established for absent epoch data.
func TestHandleLatestEpochParams_ByronEraNotFound(t *testing.T) {
	t.Parallel()

	mock := &mockNode{paramsErr: ErrProtocolParamsUnavailable}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/epochs/latest/parameters",
		nil,
	)
	w := httptest.NewRecorder()
	b.handleLatestEpochParams(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "Not Found", resp["error"])
}

// TestHandleLatestEpochParams_OtherErrorsStillInternal keeps the Byron carve-
// out narrow: a genuine conversion or storage failure must still be a 500.
func TestHandleLatestEpochParams_OtherErrorsStillInternal(t *testing.T) {
	t.Parallel()

	mock := &mockNode{paramsErr: assert.AnError}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/epochs/latest/parameters",
		nil,
	)
	w := httptest.NewRecorder()
	b.handleLatestEpochParams(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// TestProtocolParamsForSlot_ByronEraSentinel covers the certificate-deposit
// path's fallback. With no epoch row for the slot it consults the current
// pparams, and a Byron prefix leaves that nil.
func TestProtocolParamsForSlot_ByronEraSentinel(t *testing.T) {
	t.Parallel()

	adapter, _, _ := newDBBackedAdapter(t)

	pparams, err := adapter.protocolParamsForSlot(0)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrProtocolParamsUnavailable)
	assert.Nil(t, pparams)
}

// TestDrepInactivityPeriod_UnavailableIsNotZero is the silent-conversion fix.
// Returning a bare 0 for absent parameters is indistinguishable from a chain
// that genuinely configured drep_activity to 0, and drepStatus then derives
// expiry epochs from a value nobody set.
func TestDrepInactivityPeriod_UnavailableIsNotZero(t *testing.T) {
	t.Parallel()

	adapter, _, _ := newDBBackedAdapter(t)

	period, ok := adapter.drepInactivityPeriod()

	assert.False(
		t,
		ok,
		"absent protocol parameters must not report a configured value",
	)
	assert.Zero(t, period)
}

// TestDrepInactivityFromPParams_DistinguishesUnavailableFromZero proves the
// second return reports availability rather than merely restating the first:
// a Conway chain that genuinely sets drep_activity to 0 reports (0, true),
// which is what the old bare-uint64 signature could not express.
func TestDrepInactivityFromPParams_DistinguishesUnavailableFromZero(
	t *testing.T,
) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		pparams    lcommon.ProtocolParameters
		wantPeriod uint64
		wantOK     bool
	}{
		{
			name:    "byron reports unavailable",
			pparams: nil,
			wantOK:  false,
		},
		{
			name: "conway configured zero",
			pparams: &conway.ConwayProtocolParameters{
				DRepInactivityPeriod: 0,
			},
			wantPeriod: 0,
			wantOK:     true,
		},
		{
			name: "conway configured nonzero",
			pparams: &conway.ConwayProtocolParameters{
				DRepInactivityPeriod: 20,
			},
			wantPeriod: 20,
			wantOK:     true,
		},
		{
			name: "dijkstra configured nonzero",
			pparams: &dijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{
					DRepInactivityPeriod: 31,
				},
			},
			wantPeriod: 31,
			wantOK:     true,
		},
		{
			name:    "pre-conway era has no drep semantics",
			pparams: &shelley.ShelleyProtocolParameters{},
			wantOK:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			period, ok := drepInactivityFromPParams(tc.pparams)

			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantPeriod, period)
		})
	}
}

// TestDrepStatus_UsesAvailabilityNotZeroSentinel is the consequence of the
// signature change. drepStatus previously guarded expiry derivation on
// "inactivityPeriod > 0", which conflates two different chains: one whose era
// defines no drep_activity, and one that set it to 0 so DReps expire the epoch
// they last acted. Only the availability flag separates them.
func TestDrepStatus_UsesAvailabilityNotZeroSentinel(t *testing.T) {
	t.Parallel()

	const (
		lastActivity = uint64(10)
		currentEpoch = uint64(12)
	)

	t.Run("unavailable does not derive expiry", func(t *testing.T) {
		retired, expired, lastActive := drepStatus(
			true,         // active
			lastActivity, // lastActivityEpoch
			0,            // expiryEpoch: none recorded
			5,            // registrationEpoch
			currentEpoch,
			0,     // inactivityPeriod
			false, // inactivityKnown
		)

		assert.False(t, retired)
		assert.False(
			t,
			expired,
			"no drep_activity parameter means no derived expiry",
		)
		assert.Equal(t, lastActivity, lastActive)
	})

	t.Run("configured zero expires at last activity", func(t *testing.T) {
		retired, expired, lastActive := drepStatus(
			true,
			lastActivity,
			0,
			5,
			currentEpoch,
			0,    // drep_activity configured to 0
			true, // available
		)

		assert.False(t, retired)
		assert.True(
			t,
			expired,
			"drep_activity of 0 expires a DRep at its last active epoch",
		)
		assert.Equal(t, lastActivity, lastActive)
	})

	// The epoch-zero case: a DRep registered in epoch 0 that has never acted,
	// on a chain with drep_activity 0. Expiry epoch 0 is real, but that epoch
	// remains active; expiration starts in the following epoch.
	t.Run("configured zero at epoch zero expires after that epoch", func(t *testing.T) {
		for _, test := range []struct {
			current uint64
			expired bool
		}{{current: 0}, {current: 1, expired: true}, {current: 5, expired: true}} {
			retired, expired, lastActive := drepStatus(
				true, // active
				0,    // lastActivityEpoch: never acted
				0,    // expiryEpoch: none recorded
				0,    // registrationEpoch: genesis
				test.current,
				0,    // drep_activity configured to 0
				true, // available
			)

			assert.False(t, retired)
			assert.Equal(
				t,
				test.expired,
				expired,
				"expiry at epoch 0 is active through that epoch",
			)
			assert.Zero(t, lastActive)
		}
	})

	t.Run("stored expiry remains active through its epoch", func(t *testing.T) {
		for _, test := range []struct {
			current uint64
			expired bool
		}{{current: currentEpoch}, {current: currentEpoch + 1, expired: true}} {
			_, expired, _ := drepStatus(
				true, lastActivity, currentEpoch, 5, test.current, 0, false,
			)
			assert.Equal(t, test.expired, expired)
		}
	})

	t.Run("configured nonzero derives from last activity", func(t *testing.T) {
		_, expired, _ := drepStatus(
			true,
			lastActivity,
			0,
			5,
			currentEpoch,
			20, // expiry 10+20=30, beyond currentEpoch 12
			true,
		)

		assert.False(t, expired)
	})
}

// TestProtocolParamsForSlot_ByronEpochRowSentinel covers the branch a Byron
// slot actually takes once the chain has recorded epochs.
//
// The epoch_id=0 row exists with era_id 0 (Byron), so the lookup does not fall
// through to GetCurrentPParams; it reaches db.GetPParams, which returns
// (nil, nil) because Byron never writes a protocol-parameter row — the era
// defines no DecodePParamsFunc at all. That absence is the same Byron fact the
// nil-current-pparams branch reports, so it must carry the same sentinel
// rather than an untyped "decoded protocol parameters are nil".
func TestProtocolParamsForSlot_ByronEpochRowSentinel(t *testing.T) {
	t.Parallel()

	adapter, store, _ := newDBBackedAdapter(t)

	_, err := store.Exec(`
INSERT INTO epoch (epoch_id, start_slot, length_in_slots, era_id)
VALUES (?, ?, ?, ?)`,
		0, 0, 100, byron.EraIdByron,
	)
	require.NoError(t, err)

	pparams, err := adapter.protocolParamsForSlot(50)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrProtocolParamsUnavailable)
	assert.Nil(t, pparams)
}

// --- EpochProtocolParams: the Byron consumer that outlives the sync --------
//
// Unlike CurrentProtocolParams, this path stays reachable forever: GET
// /api/v0/epochs/0/parameters on a fully synced mainnet node still resolves
// the Byron epoch row and finds no parameter row. Raised by @wolf31o2 in
// review.

// insertByronEpoch records a Byron epoch row with no accompanying parameter
// row, which is how a synced node genuinely stores the Byron prefix.
func insertByronEpoch(t *testing.T, store *sql.DB, epochID uint64) {
	t.Helper()
	_, err := store.Exec(`
INSERT INTO epoch (epoch_id, start_slot, length_in_slots, era_id)
VALUES (?, ?, ?, ?)`,
		epochID, epochID*100, 100, byron.EraIdByron,
	)
	require.NoError(t, err)
}

// TestEpochProtocolParams_ByronEpochReportsParamsNotEpoch separates the two
// facts the old sentinel ran together. The epoch exists — the node holds it
// and will answer other queries about it — and only its parameters do not.
// Reporting "epoch not found" tells a caller something false about the node's
// contents.
func TestEpochProtocolParams_ByronEpochReportsParamsNotEpoch(t *testing.T) {
	t.Parallel()

	adapter, store, _ := newDBBackedAdapter(t)
	insertByronEpoch(t, store, 0)

	info, err := adapter.EpochProtocolParams(0)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrProtocolParamsUnavailable)
	assert.NotErrorIs(
		t,
		err,
		ErrEpochNotFound,
		"the epoch exists; only its parameters do not",
	)
	assert.Equal(t, ProtocolParamsInfo{}, info)
}

// TestEpochProtocolParams_MissingEpochStillNotFound keeps the distinction
// meaningful in the other direction: an epoch the node genuinely does not
// hold must still report ErrEpochNotFound.
func TestEpochProtocolParams_MissingEpochStillNotFound(t *testing.T) {
	t.Parallel()

	adapter, _, _ := newDBBackedAdapter(t)

	_, err := adapter.EpochProtocolParams(999)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEpochNotFound)
	assert.NotErrorIs(t, err, ErrProtocolParamsUnavailable)
}

// TestEpochProtocolParams_ByronRowDoesNotCallNilDecoder guards the decode
// call. ByronEraDesc defines no DecodePParamsFunc, so reaching the decoder
// with a Byron era would be a nil-func call. The empty-rows return covers
// that today, which makes this a guard against a future reordering rather
// than a live defect.
func TestEpochProtocolParams_ByronRowDoesNotCallNilDecoder(t *testing.T) {
	t.Parallel()

	adapter, store, _ := newDBBackedAdapter(t)
	insertByronEpoch(t, store, 0)
	// A Byron parameter row should never exist, but if one did the decode
	// call must not be reached with a nil decoder.
	_, err := store.Exec(`
INSERT INTO pparams (cbor, added_slot, epoch, era_id)
VALUES (?, ?, ?, ?)`,
		[]byte{0xa0}, 0, 0, byron.EraIdByron,
	)
	require.NoError(t, err)

	require.NotPanics(t, func() {
		_, err := adapter.EpochProtocolParams(0)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrProtocolParamsUnavailable)
	})
}

// TestHandleEpochParams_ByronEpochNotFoundNotLoggedAsError covers the
// operator-facing half. handleLatestEpochParams logs the same expected
// absence at Debug precisely so a from-genesis sync does not fill the log
// with errors; this sibling handler logged every Byron-epoch query at Error
// before reaching its not-found branch.
func TestHandleEpochParams_ByronEpochNotFoundNotLoggedAsError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "byron params", err: ErrProtocolParamsUnavailable},
		{name: "missing epoch", err: ErrEpochNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(
				&buf,
				&slog.HandlerOptions{Level: slog.LevelDebug},
			))
			b := New(
				BlockfrostConfig{ListenAddress: ":0"},
				&mockNode{epochParamsErr: tc.err},
				logger,
			)

			req := httptest.NewRequest(
				http.MethodGet,
				"/api/v0/epochs/0/parameters",
				nil,
			)
			req.SetPathValue("number", "0")
			w := httptest.NewRecorder()
			b.handleEpochParams(w, req)

			assert.Equal(t, http.StatusNotFound, w.Code)
			assert.NotContains(
				t,
				buf.String(),
				`"level":"ERROR"`,
				"an expected absence must not log at error level",
			)
		})
	}
}

// TestHandleEpochParams_RealFailureStillLogsError keeps that carve-out
// narrow: a genuine failure must still be a logged 500.
func TestHandleEpochParams_RealFailureStillLogsError(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(
		&buf,
		&slog.HandlerOptions{Level: slog.LevelDebug},
	))
	b := New(
		BlockfrostConfig{ListenAddress: ":0"},
		&mockNode{epochParamsErr: assert.AnError},
		logger,
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/epochs/5/parameters",
		nil,
	)
	req.SetPathValue("number", "5")
	w := httptest.NewRecorder()
	b.handleEpochParams(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, buf.String(), `"level":"ERROR"`)
}

func TestTransactionBodyReadDeadline(t *testing.T) {
	for _, route := range []struct {
		path        string
		contentType string
		body        string
	}{
		{"/api/v0/tx/submit", "application/cbor", "80"},
		{"/api/v0/utils/txs/evaluate", "application/cbor", "80"},
		{"/api/v0/utils/txs/evaluate/utxos", "application/json", `{"cbor":"80"}`},
	} {
		t.Run(route.path, func(t *testing.T) {
			for _, mode := range []string{"complete", "truncated", "stalled"} {
				t.Run(mode, func(t *testing.T) {
					b := New(BlockfrostConfig{}, &mockNode{}, nil)
					b.requestBodyTimeout = 100 * time.Millisecond
					server := httptest.NewServer(b.handler())
					defer server.Close()
					conn, err := net.DialTimeout(
						"tcp",
						strings.TrimPrefix(server.URL, "http://"),
						5*time.Second,
					)
					require.NoError(t, err)
					// Close the client before server teardown, including failed assertions.
					defer conn.Close()
					length := len(route.body)
					if mode != "complete" {
						length += 4096
					}
					_, err = fmt.Fprintf(
						conn,
						"POST %s HTTP/1.1\r\nHost: localhost\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n%s",
						route.path,
						route.contentType,
						length,
						route.body,
					)
					require.NoError(t, err)
					if mode == "truncated" {
						require.NoError(t, conn.(*net.TCPConn).CloseWrite())
					}
					require.NoError(
						t,
						conn.SetReadDeadline(time.Now().Add(3*time.Second)),
					)
					response, err := http.ReadResponse(
						bufio.NewReader(conn),
						nil,
					)
					require.NoError(
						t,
						err,
						"body reader did not return a response to the %s client",
						mode,
					)
					defer response.Body.Close()
					body, err := io.ReadAll(response.Body)
					require.NoError(t, err)
					if mode == "complete" {
						require.Equal(
							t,
							http.StatusOK,
							response.StatusCode,
							string(body),
						)
					} else {
						require.Equal(t, http.StatusBadRequest, response.StatusCode, string(body))
						require.Contains(t, string(body), "failed to read transaction body")
					}
				})
			}
		})
	}
}

func TestBlockfrostAnonymousPlaintextTLSAndCORS(t *testing.T) {
	t.Parallel()
	const origin = "https://wallet.example"
	cert, key := testutil.GenerateTestTLSCertKey(t)
	for _, tc := range []struct {
		name string
		tls  apiconfig.EffectiveTLS
		url  string
		cli  *http.Client
	}{
		{"plaintext", apiconfig.EffectiveTLS{}, "http://", http.DefaultClient},
		{"tls", apiconfig.EffectiveTLS{Enabled: true, CertFilePath: cert, KeyFilePath: key}, "https://", testutil.InsecureHTTPClient()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)
			client := *tc.cli
			client.Timeout = 5 * time.Second
			srv, addr := startOnFreePort(
				t,
				ctx,
				BlockfrostConfig{
					TLS:                tc.tls,
					CORSAllowedOrigins: []string{origin},
				},
			)
			t.Cleanup(func() {
				stopCtx, stopCancel := context.WithTimeout(
					context.Background(),
					5*time.Second,
				)
				defer stopCancel()
				require.NoError(t, srv.Stop(stopCtx))
			})
			req, err := http.NewRequestWithContext(
				ctx,
				http.MethodGet,
				tc.url+addr+"/health",
				nil,
			)
			require.NoError(t, err)
			req.Header.Set("Origin", origin)
			resp, err := client.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(
				t,
				origin,
				resp.Header.Get("Access-Control-Allow-Origin"),
			)
			preflight, err := http.NewRequestWithContext(
				ctx,
				http.MethodOptions,
				tc.url+addr+"/health",
				nil,
			)
			require.NoError(t, err)
			preflight.Header.Set("Origin", origin)
			preflight.Header.Set(
				"Access-Control-Request-Method",
				http.MethodGet,
			)
			corsResp, err := client.Do(preflight)
			require.NoError(t, err)
			defer corsResp.Body.Close()
			require.Equal(t, http.StatusNoContent, corsResp.StatusCode)
			require.Equal(
				t,
				origin,
				corsResp.Header.Get("Access-Control-Allow-Origin"),
			)
		})
	}
}
