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

package mesh

import (
	"errors"
	"net/http"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// balanceRequest builds an /account/balance request.
func balanceRequest(addr string) AccountBalanceRequest {
	req := AccountBalanceRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
	}
	if addr != "" {
		req.AccountIdentifier = &AccountIdentifier{Address: addr}
	}
	return req
}

// coinsRequest builds an /account/coins request.
func coinsRequest(addr string) AccountCoinsRequest {
	req := AccountCoinsRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
	}
	if addr != "" {
		req.AccountIdentifier = &AccountIdentifier{Address: addr}
	}
	return req
}

func TestAccountBalance(t *testing.T) {
	deps := newTestDeps()
	paymentKey := testKeyHash(0x03)
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, paymentKey, nil,
	)
	tipHash := testHash(0x99)
	deps.chain.tip = ochainsync.Tip{
		Point:       ocommon.NewPoint(500, tipHash),
		BlockNumber: 25,
	}
	deps.ledger.utxos = func(
		got lcommon.Address,
	) ([]models.Utxo, error) {
		require.Equal(t, addr, got.String())
		return []models.Utxo{
			testUtxo(
				testHash(0x01), 0, 2_000_000, paymentKey, nil,
			),
			testUtxo(
				testHash(0x02), 1, 3_500_000, paymentKey, nil,
			),
		}, nil
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/account/balance", balanceRequest(addr))

	resp := decodeResponse[AccountBalanceResponse](t, rec)
	require.Equal(
		t,
		&BlockIdentifier{Index: 25, Hash: hexString(tipHash)},
		resp.BlockIdentifier,
	)
	require.Len(t, resp.Balances, 1)
	require.Equal(t, "5500000", resp.Balances[0].Value)
	require.Equal(t, "ADA", resp.Balances[0].Currency.Symbol)
	require.Equal(t, int32(6), resp.Balances[0].Currency.Decimals)
}

// TestAccountBalanceEmptyAccount asserts an address with no UTxOs still
// reports an explicit zero ADA balance rather than an empty list, which
// Mesh clients treat as a malformed response.
func TestAccountBalanceEmptyAccount(t *testing.T) {
	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x04), nil,
	)
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/account/balance", balanceRequest(addr))

	resp := decodeResponse[AccountBalanceResponse](t, rec)
	require.Len(t, resp.Balances, 1)
	require.Equal(t, "0", resp.Balances[0].Value)
}

// TestAccountBalanceAggregatesAssets covers native asset totals summed
// across UTxOs and the deterministic policy/name ordering clients rely
// on for stable diffs.
func TestAccountBalanceAggregatesAssets(t *testing.T) {
	deps := newTestDeps()
	paymentKey := testKeyHash(0x05)
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, paymentKey, nil,
	)
	policyB := testKeyHash(0xbb)
	policyA := testKeyHash(0xaa)
	deps.ledger.utxos = func(
		lcommon.Address,
	) ([]models.Utxo, error) {
		return []models.Utxo{
			testUtxo(
				testHash(0x01), 0, 1_000_000, paymentKey,
				[]models.Asset{
					testAsset(policyB, []byte("zeta"), 5),
					testAsset(policyA, []byte("beta"), 7),
				},
			),
			testUtxo(
				testHash(0x02), 0, 1_000_000, paymentKey,
				[]models.Asset{
					testAsset(policyA, []byte("beta"), 3),
					testAsset(policyA, []byte("alpha"), 1),
				},
			),
		}, nil
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/account/balance", balanceRequest(addr))

	resp := decodeResponse[AccountBalanceResponse](t, rec)
	require.Len(t, resp.Balances, 4)
	require.Equal(t, "2000000", resp.Balances[0].Value)
	require.Equal(t, "ADA", resp.Balances[0].Currency.Symbol)
	// Sorted by policy then asset name, so policyA/alpha precedes
	// policyA/beta, which precedes policyB/zeta.
	require.Equal(
		t,
		hexString([]byte("alpha")),
		resp.Balances[1].Currency.Symbol,
	)
	require.Equal(t, "1", resp.Balances[1].Value)
	require.Equal(
		t,
		hexString([]byte("beta")),
		resp.Balances[2].Currency.Symbol,
	)
	require.Equal(t, "10", resp.Balances[2].Value)
	require.Equal(
		t,
		hexString([]byte("zeta")),
		resp.Balances[3].Currency.Symbol,
	)
	require.Equal(t, "5", resp.Balances[3].Value)
	require.Equal(
		t,
		hexString(policyA),
		resp.Balances[1].Currency.Metadata["policyId"],
	)
}

// TestAccountBalanceRejectsHistoricalLookup covers the Mesh contract
// that an unsupported operation fails explicitly: /network/options
// advertises historical_balance_lookup=false, so a request pinned to an
// older block must be refused rather than silently answered with the
// current tip balance.
func TestAccountBalanceRejectsHistoricalLookup(t *testing.T) {
	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x06), nil,
	)
	deps.ledger.utxos = func(
		lcommon.Address,
	) ([]models.Utxo, error) {
		t.Fatal("ledger must not be queried for a historical point")
		return nil, nil
	}
	h := newTestHandler(t, deps)

	req := balanceRequest(addr)
	req.BlockIdentifier = byIndex(10)
	rec := postJSON(t, h, "/account/balance", req)

	requireMeshError(
		t, rec, ErrNotImplemented, http.StatusNotImplemented,
	)
}

func TestAccountBalanceInvalidAccount(t *testing.T) {
	tests := map[string]string{
		"missing account identifier": "",
		"empty address":              "",
		"malformed address":          "not-an-address",
	}
	for name, addr := range tests {
		t.Run(name, func(t *testing.T) {
			h := newTestHandler(t, newTestDeps())

			rec := postJSON(
				t, h, "/account/balance", balanceRequest(addr),
			)

			requireMeshError(
				t, rec, ErrInvalidRequest,
				http.StatusBadRequest,
			)
		})
	}
}

func TestAccountBalanceLedgerError(t *testing.T) {
	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x07), nil,
	)
	deps.ledger.utxos = func(
		lcommon.Address,
	) ([]models.Utxo, error) {
		return nil, errors.New("ledger unavailable")
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/account/balance", balanceRequest(addr))

	got := requireMeshError(
		t, rec, ErrInternal, http.StatusInternalServerError,
	)
	require.Equal(t, "ledger unavailable", got.Details["error"])
}

func TestAccountCoins(t *testing.T) {
	deps := newTestDeps()
	paymentKey := testKeyHash(0x08)
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, paymentKey, nil,
	)
	txID := testHash(0x0a)
	policy := testKeyHash(0xcc)
	deps.chain.tip = ochainsync.Tip{
		Point:       ocommon.NewPoint(9, testHash(0x0b)),
		BlockNumber: 3,
	}
	deps.ledger.utxos = func(
		lcommon.Address,
	) ([]models.Utxo, error) {
		return []models.Utxo{
			testUtxo(
				txID, 2, 6_000_000, paymentKey,
				[]models.Asset{
					testAsset(policy, []byte("tok"), 42),
				},
			),
		}, nil
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/account/coins", coinsRequest(addr))

	resp := decodeResponse[AccountCoinsResponse](t, rec)
	require.Equal(t, int64(3), resp.BlockIdentifier.Index)
	require.Len(t, resp.Coins, 2)
	require.Equal(
		t,
		hexString(txID)+":2",
		resp.Coins[0].CoinIdentifier.Identifier,
	)
	require.Equal(t, "6000000", resp.Coins[0].Amount.Value)
	// Native assets are reported as sub-coins of the owning UTxO.
	require.Equal(
		t,
		hexString(txID)+":2:"+hexString(policy)+":"+
			hexString([]byte("tok")),
		resp.Coins[1].CoinIdentifier.Identifier,
	)
	require.Equal(t, "42", resp.Coins[1].Amount.Value)
}

func TestAccountCoinsEmptyAccount(t *testing.T) {
	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x09), nil,
	)
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/account/coins", coinsRequest(addr))

	resp := decodeResponse[AccountCoinsResponse](t, rec)
	require.NotNil(t, resp.Coins)
	require.Empty(t, resp.Coins)
}

func TestAccountCoinsInvalidAccount(t *testing.T) {
	h := newTestHandler(t, newTestDeps())

	rec := postJSON(
		t, h, "/account/coins", coinsRequest("not-an-address"),
	)

	requireMeshError(
		t, rec, ErrInvalidRequest, http.StatusBadRequest,
	)
}

func TestAccountCoinsLedgerError(t *testing.T) {
	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x0c), nil,
	)
	deps.ledger.utxos = func(
		lcommon.Address,
	) ([]models.Utxo, error) {
		return nil, errors.New("ledger unavailable")
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/account/coins", coinsRequest(addr))

	requireMeshError(
		t, rec, ErrInternal, http.StatusInternalServerError,
	)
}
