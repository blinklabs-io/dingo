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

package mesh

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
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
	t.Parallel()

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
		got []lcommon.Address,
	) ([]models.Utxo, error) {
		require.Len(t, got, 1)
		require.Equal(t, addr, got[0].String())
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
	t.Parallel()

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
	t.Parallel()

	deps := newTestDeps()
	paymentKey := testKeyHash(0x05)
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, paymentKey, nil,
	)
	policyB := testKeyHash(0xbb)
	policyA := testKeyHash(0xaa)
	deps.ledger.utxos = func(
		[]lcommon.Address,
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

// TestAccountBalanceHistoricalByIndex covers a balance pinned to an
// earlier block: the ledger must be queried at that block's slot, and
// the response must report the requested block rather than the tip, so
// a client can tell which point the balance belongs to.
func TestAccountBalanceHistoricalByIndex(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	paymentKey := testKeyHash(0x30)
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, paymentKey, nil,
	)
	histHash := testHash(0x31)
	deps.chain.tip = ochainsync.Tip{
		Point:       ocommon.NewPoint(9000, testHash(0x32)),
		BlockNumber: 900,
	}
	deps.database.blockByIndex = func(
		idx uint64,
	) (models.Block, error) {
		require.Equal(t, uint64(120), idx)
		return models.Block{
			Hash:   histHash,
			Number: 120,
			Slot:   1200,
		}, nil
	}
	deps.ledger.utxos = func(
		[]lcommon.Address,
	) ([]models.Utxo, error) {
		t.Fatal("historical request must not read the tip UTxO set")
		return nil, nil
	}
	deps.ledger.utxosAtSlot = func(
		got lcommon.Address,
		slot uint64,
	) ([]models.Utxo, error) {
		require.Equal(t, addr, got.String())
		require.Equal(t, uint64(1200), slot)
		return []models.Utxo{
			testUtxo(
				testHash(0x33), 0, 7_000_000, paymentKey, nil,
			),
		}, nil
	}
	h := newTestHandler(t, deps)

	req := balanceRequest(addr)
	req.BlockIdentifier = byIndex(120)
	rec := postJSON(t, h, "/account/balance", req)

	resp := decodeResponse[AccountBalanceResponse](t, rec)
	require.Equal(
		t,
		&BlockIdentifier{Index: 120, Hash: hexString(histHash)},
		resp.BlockIdentifier,
	)
	require.Len(t, resp.Balances, 1)
	require.Equal(t, "7000000", resp.Balances[0].Value)
}

// TestAccountBalanceHistoricalByHash covers pinning by block hash,
// which is the identifier a client holds after reading a block.
func TestAccountBalanceHistoricalByHash(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x34), nil,
	)
	histHash := testHash(0x35)
	deps.database.blockByHash = func(
		hash []byte,
	) (models.Block, error) {
		require.Equal(t, histHash, hash)
		return models.Block{
			Hash:   histHash,
			Number: 55,
			Slot:   550,
		}, nil
	}
	deps.ledger.utxosAtSlot = func(
		_ lcommon.Address,
		slot uint64,
	) ([]models.Utxo, error) {
		require.Equal(t, uint64(550), slot)
		return nil, nil
	}
	h := newTestHandler(t, deps)

	req := balanceRequest(addr)
	req.BlockIdentifier = byHash(hexString(histHash))
	rec := postJSON(t, h, "/account/balance", req)

	resp := decodeResponse[AccountBalanceResponse](t, rec)
	require.Equal(
		t,
		&BlockIdentifier{Index: 55, Hash: hexString(histHash)},
		resp.BlockIdentifier,
	)
	// An account with no UTxOs at that point still reports zero ADA.
	require.Len(t, resp.Balances, 1)
	require.Equal(t, "0", resp.Balances[0].Value)
}

// TestAccountBalanceHistoricalEmptyIdentifier asserts a block
// identifier carrying neither hash nor index is treated as absent, so
// clients that always send the field still get the tip balance.
func TestAccountBalanceHistoricalEmptyIdentifier(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x36), nil,
	)
	deps.chain.tip = ochainsync.Tip{
		Point:       ocommon.NewPoint(70, testHash(0x37)),
		BlockNumber: 7,
	}
	called := false
	deps.ledger.utxos = func(
		[]lcommon.Address,
	) ([]models.Utxo, error) {
		called = true
		return nil, nil
	}
	deps.ledger.utxosAtSlot = func(
		lcommon.Address, uint64,
	) ([]models.Utxo, error) {
		t.Fatal("empty identifier must not take the historical path")
		return nil, nil
	}
	h := newTestHandler(t, deps)

	req := balanceRequest(addr)
	req.BlockIdentifier = &PartialBlockIdentifier{}
	rec := postJSON(t, h, "/account/balance", req)

	resp := decodeResponse[AccountBalanceResponse](t, rec)
	require.True(t, called)
	require.Equal(t, int64(7), resp.BlockIdentifier.Index)
}

// TestAccountBalanceHistoricalBlockNotFound covers a point the node
// cannot resolve, including the reorg case where the client holds the
// hash of a block that was rolled back: the balance must not silently
// fall back to another point.
func TestAccountBalanceHistoricalBlockNotFound(t *testing.T) {
	t.Parallel()

	rolledBack := testHash(0x38)
	tests := map[string]*PartialBlockIdentifier{
		"unknown index":    byIndex(999999),
		"rolled-back hash": byHash(hexString(rolledBack)),
	}
	for name, id := range tests {
		t.Run(name, func(t *testing.T) {
			deps := newTestDeps()
			addr := testAddress(
				t, lcommon.AddressTypeKeyNone,
				testKeyHash(0x39), nil,
			)
			deps.database.blockByIndex = func(
				uint64,
			) (models.Block, error) {
				return models.Block{},
					models.ErrBlockNotFound
			}
			deps.database.blockByHash = func(
				[]byte,
			) (models.Block, error) {
				return models.Block{},
					models.ErrBlockNotFound
			}
			deps.ledger.utxosAtSlot = func(
				lcommon.Address, uint64,
			) ([]models.Utxo, error) {
				t.Fatal("must not query an unresolved point")
				return nil, nil
			}
			h := newTestHandler(t, deps)

			req := balanceRequest(addr)
			req.BlockIdentifier = id
			rec := postJSON(t, h, "/account/balance", req)

			requireMeshError(
				t, rec, ErrBlockNotFound,
				http.StatusNotFound,
			)
		})
	}
}

// TestAccountBalanceHistoricalLedgerError asserts a failure reading the
// historical UTxO set is reported rather than degraded to an empty
// balance.
func TestAccountBalanceHistoricalLedgerError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x3a), nil,
	)
	deps.database.blockByIndex = func(
		uint64,
	) (models.Block, error) {
		return models.Block{
			Hash: testHash(0x3b), Number: 4, Slot: 40,
		}, nil
	}
	deps.ledger.utxosAtSlot = func(
		lcommon.Address, uint64,
	) ([]models.Utxo, error) {
		return nil, errors.New("historical read failed")
	}
	h := newTestHandler(t, deps)

	req := balanceRequest(addr)
	req.BlockIdentifier = byIndex(4)
	rec := postJSON(t, h, "/account/balance", req)

	got := requireMeshError(
		t, rec, ErrInternal, http.StatusInternalServerError,
	)
	require.Nil(t, got.Details)
}

func TestAccountBalanceInvalidAccount(t *testing.T) {
	t.Parallel()

	// An absent identifier and one carrying an empty address are
	// separate branches of parseAccountAddress, so the empty-address
	// case sets a non-nil identifier rather than relying on
	// balanceRequest("") leaving the field nil.
	emptyAddress := balanceRequest("")
	emptyAddress.AccountIdentifier = &AccountIdentifier{}

	tests := map[string]AccountBalanceRequest{
		"missing account identifier": balanceRequest(""),
		"empty address":              emptyAddress,
		"malformed address":          balanceRequest("not-an-address"),
	}
	for name, req := range tests {
		t.Run(name, func(t *testing.T) {
			h := newTestHandler(t, newTestDeps())

			rec := postJSON(t, h, "/account/balance", req)

			requireMeshError(
				t, rec, ErrInvalidRequest,
				http.StatusBadRequest,
			)
		})
	}
}

func TestAccountBalanceLedgerError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x07), nil,
	)
	deps.ledger.utxos = func(
		[]lcommon.Address,
	) ([]models.Utxo, error) {
		return nil, errors.New("ledger unavailable")
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/account/balance", balanceRequest(addr))

	got := requireMeshError(
		t, rec, ErrInternal, http.StatusInternalServerError,
	)
	require.Nil(t, got.Details)
}

func TestAccountCoins(t *testing.T) {
	t.Parallel()

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
		[]lcommon.Address,
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
	t.Parallel()

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
	t.Parallel()

	h := newTestHandler(t, newTestDeps())

	rec := postJSON(
		t, h, "/account/coins", coinsRequest("not-an-address"),
	)

	requireMeshError(
		t, rec, ErrInvalidRequest, http.StatusBadRequest,
	)
}

func TestAccountCoinsLedgerError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x0c), nil,
	)
	deps.ledger.utxos = func(
		[]lcommon.Address,
	) ([]models.Utxo, error) {
		return nil, errors.New("ledger unavailable")
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/account/coins", coinsRequest(addr))

	requireMeshError(
		t, rec, ErrInternal, http.StatusInternalServerError,
	)
}

// TestAccountBalanceHonorsAdvertisedCapability ties the capability
// advertised by /network/options to what /account/balance actually
// does. The two drifting apart is a client-visible contract break:
// a client that trusts the flag would either skip historical queries
// the node supports, or send ones it rejects.
func TestAccountBalanceHonorsAdvertisedCapability(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x3c), nil,
	)
	deps.database.blockByIndex = func(
		uint64,
	) (models.Block, error) {
		return models.Block{
			Hash: testHash(0x3d), Number: 2, Slot: 20,
		}, nil
	}
	h := newTestHandler(t, deps)

	optsRec := postJSON(t, h, "/network/options", NetworkRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
	})
	opts := decodeResponse[NetworkOptionsResponse](t, optsRec)

	req := balanceRequest(addr)
	req.BlockIdentifier = byIndex(2)
	rec := postJSON(t, h, "/account/balance", req)

	if opts.Allow.HistoricalBalanceLookup {
		resp := decodeResponse[AccountBalanceResponse](t, rec)
		require.Equal(t, int64(2), resp.BlockIdentifier.Index)
		return
	}
	requireMeshError(
		t, rec, ErrNotImplemented, http.StatusNotImplemented,
	)
}

// cursorPager serves utxos in order through UtxosByAddressBoundedPage,
// honoring the cursor, row limit and byte budget the way the database does:
// the first match always fits, and Next is nil only once the set is exhausted.
func cursorPager(
	t *testing.T,
	utxos []models.Utxo,
	calls *[]pageCall,
) func(
	context.Context,
	*models.UtxoWithOrderingQuery,
	int,
	int64,
) (models.UtxoAddressPage, error) {
	t.Helper()
	return func(
		_ context.Context,
		q *models.UtxoWithOrderingQuery,
		maxCandidates int,
		maxBytes int64,
	) (models.UtxoAddressPage, error) {
		*calls = append(*calls, pageCall{
			limit: q.Limit, maxCandidates: maxCandidates,
			maxBytes: maxBytes, after: q.After,
		})
		start := 0
		if q.After != nil {
			for i := range utxos {
				if int(utxos[i].OutputIdx) == int(q.After.OutputIdx) {
					start = i + 1
				}
			}
		}
		var page models.UtxoAddressPage
		var retained int64
		for i := start; i < len(utxos); i++ {
			size := int64(len(utxos[i].Cbor))
			if len(page.Utxos) == q.Limit ||
				(len(page.Utxos) > 0 && retained+size > maxBytes) {
				return page, nil
			}
			retained += size
			page.Utxos = append(
				page.Utxos, models.UtxoWithOrdering{Utxo: utxos[i]},
			)
			page.Next = &models.UtxoOrderingCursor{
				OutputIdx: utxos[i].OutputIdx, TxId: utxos[i].TxId,
			}
		}
		page.Next = nil
		return page, nil
	}
}

type pageCall struct {
	limit         int
	maxCandidates int
	maxBytes      int64
	after         *models.UtxoOrderingCursor
}

func TestAccountCoinsPassesRequestContextToLedger(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x31), nil,
	)
	ctxErr := make(chan error, 1)
	deps.ledger.boundedPage = func(
		ctx context.Context,
		_ *models.UtxoWithOrderingQuery,
		_ int,
		_ int64,
	) (models.UtxoAddressPage, error) {
		ctxErr <- ctx.Err()
		return models.UtxoAddressPage{}, ctx.Err()
	}
	h := newTestHandler(t, deps)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	raw, err := json.Marshal(coinsRequest(addr))
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(
		ctx, http.MethodPost, "/account/coins", bytes.NewReader(raw),
	)
	h.ServeHTTP(httptest.NewRecorder(), req)

	got := testutil.RequireReceive(
		t, ctxErr, 5*time.Second, "ledger read to start",
	)
	require.ErrorIs(
		t, got, context.Canceled,
		"the ledger read must run under the request context",
	)
}

func TestAccountCoinsPagesWithinBudgets(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	key := testKeyHash(0x32)
	addr := testAddress(t, lcommon.AddressTypeKeyNone, key, nil)
	out := testOutput(t, addr, 1_000_000)
	outCbor, err := cbor.Encode(&out)
	require.NoError(t, err)
	size := int64(len(outCbor))
	var utxos []models.Utxo
	for i := range 7 {
		u := testUtxo(testHash(0x40), uint32(i), 1_000_000, key, nil)
		u.Cbor = outCbor
		utxos = append(utxos, u)
	}
	var calls []pageCall
	deps.ledger.boundedPage = cursorPager(t, utxos, &calls)
	h := newTestHandler(t, deps, func(c *ServerConfig) {
		c.accountCoinsPageSize = 3
		c.accountCoinsMaxCandidates = 11
		// Two outputs fit, a third does not: the byte budget cuts pages
		// before the row limit does.
		c.accountCoinsMaxBytes = 2*size + 1
	})

	var seen []string
	var cursor any
	pages := 0
	for {
		req := coinsRequest(addr)
		req.Metadata = map[string]any{"paginate": true}
		if cursor != nil {
			req.Metadata["cursor"] = cursor
		}
		resp := decodeResponse[AccountCoinsResponse](
			t, postJSON(t, h, "/account/coins", req),
		)
		pages++
		require.LessOrEqual(t, len(resp.Coins), 2)
		for _, c := range resp.Coins {
			seen = append(seen, c.CoinIdentifier.Identifier)
		}
		next, more := resp.Metadata["next_cursor"]
		if !more {
			break
		}
		require.NotEmpty(t, next)
		cursor = next
		require.Less(t, pages, 20, "cursor must advance")
	}

	var want []string
	for i := range utxos {
		want = append(
			want, fmt.Sprintf("%s:%d", hexString(utxos[i].TxId), i),
		)
	}
	require.Equal(
		t, want, seen,
		"continuation must return every coin once, in order",
	)
	require.Equal(t, 4, pages)
	for _, c := range calls {
		require.Equal(t, 3, c.limit)
		require.Equal(t, 11, c.maxCandidates)
		require.Equal(t, 2*size+1, c.maxBytes)
	}
	require.Nil(t, calls[0].after)
	require.NotNil(t, calls[1].after)
}

// TestAccountCoinsStandardRequestIsCompleteOrRefused pins the response a
// client that does not opt into paging receives: every coin of the address
// when one bounded read exhausts it, otherwise a limit error naming the
// opt-in, and never a partial coin list.
func TestAccountCoinsStandardRequestIsCompleteOrRefused(t *testing.T) {
	t.Parallel()

	key := testKeyHash(0x34)
	addr := testAddress(t, lcommon.AddressTypeKeyNone, key, nil)
	utxosOf := func(n int) []models.Utxo {
		var utxos []models.Utxo
		for i := range n {
			utxos = append(
				utxos,
				testUtxo(testHash(0x41), uint32(i), 1_000_000, key, nil),
			)
		}
		return utxos
	}
	serve := func(t *testing.T, n int) *httptest.ResponseRecorder {
		t.Helper()
		deps := newTestDeps()
		var calls []pageCall
		deps.ledger.boundedPage = cursorPager(t, utxosOf(n), &calls)
		h := newTestHandler(t, deps, func(c *ServerConfig) {
			c.accountCoinsPageSize = 3
		})
		return postJSON(t, h, "/account/coins", coinsRequest(addr))
	}

	t.Run("fits one read", func(t *testing.T) {
		t.Parallel()
		resp := decodeResponse[AccountCoinsResponse](t, serve(t, 3))
		require.Len(t, resp.Coins, 3)
		require.NotContains(t, resp.Metadata, "next_cursor")
	})
	t.Run("exceeds one read", func(t *testing.T) {
		t.Parallel()
		got := requireMeshError(
			t, serve(t, 4), ErrInvalidRequest, http.StatusBadRequest,
		)
		require.Contains(t, got.Details["error"], "metadata.paginate")
	})
}

func TestAccountCoinsRejectsNonBooleanPaginate(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x35), nil,
	)
	called := false
	deps.ledger.boundedPage = func(
		context.Context,
		*models.UtxoWithOrderingQuery,
		int,
		int64,
	) (models.UtxoAddressPage, error) {
		called = true
		return models.UtxoAddressPage{}, nil
	}
	h := newTestHandler(t, deps)
	req := coinsRequest(addr)
	req.Metadata = map[string]any{"paginate": "yes"}

	rec := postJSON(t, h, "/account/coins", req)

	requireMeshError(t, rec, ErrInvalidRequest, http.StatusBadRequest)
	require.False(t, called)
}

func TestAccountCoinsRejectsMalformedCursor(t *testing.T) {
	t.Parallel()

	for name, cursor := range map[string]any{
		"not a cursor": "garbage",
		"bad tx id":    "1:0:0:zz",
		"not a string": 7,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			deps := newTestDeps()
			addr := testAddress(
				t, lcommon.AddressTypeKeyNone, testKeyHash(0x33), nil,
			)
			called := false
			deps.ledger.boundedPage = func(
				context.Context,
				*models.UtxoWithOrderingQuery,
				int,
				int64,
			) (models.UtxoAddressPage, error) {
				called = true
				return models.UtxoAddressPage{}, nil
			}
			h := newTestHandler(t, deps)
			req := coinsRequest(addr)
			req.Metadata = map[string]any{"cursor": cursor}

			rec := postJSON(t, h, "/account/coins", req)

			requireMeshError(
				t, rec, ErrInvalidRequest, http.StatusBadRequest,
			)
			require.False(t, called)
		})
	}
}

func TestAccountCoinsReadsAssetsFromOutputCbor(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	key := testKeyHash(0x34)
	addr := testAddress(t, lcommon.AddressTypeKeyNone, key, nil)
	policy := lcommon.NewBlake2b224(testKeyHash(0xdd))
	ma := lcommon.NewMultiAsset(
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeOutput{
			policy: {cbor.NewByteString([]byte("tok")): big.NewInt(9)},
		},
	)
	out := testOutput(t, addr, 2_000_000)
	out.OutputAmount = mary.MaryTransactionOutputValue{
		Amount: 2_000_000, Assets: &ma,
	}
	raw, err := cbor.Encode(&out)
	require.NoError(t, err)
	txID := testHash(0x50)
	u := testUtxo(txID, 0, 2_000_000, key, nil)
	u.Cbor = raw
	var calls []pageCall
	deps.ledger.boundedPage = cursorPager(t, []models.Utxo{u}, &calls)
	h := newTestHandler(t, deps)

	resp := decodeResponse[AccountCoinsResponse](
		t, postJSON(t, h, "/account/coins", coinsRequest(addr)),
	)

	require.Len(t, resp.Coins, 2)
	require.Equal(
		t,
		hexString(txID)+":0:"+hexString(policy.Bytes())+":"+
			hexString([]byte("tok")),
		resp.Coins[1].CoinIdentifier.Identifier,
	)
	require.Equal(t, "9", resp.Coins[1].Amount.Value)
	require.NotContains(t, resp.Metadata, "next_cursor")
}

func TestAccountCoinsAdmitsBoundedConcurrentReads(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x35), nil,
	)
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	deps.ledger.boundedPage = func(
		ctx context.Context,
		_ *models.UtxoWithOrderingQuery,
		_ int,
		_ int64,
	) (models.UtxoAddressPage, error) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return models.UtxoAddressPage{}, nil
	}
	h := newTestHandler(t, deps, func(c *ServerConfig) {
		c.maxConcurrentBulkReads = 1
	})
	// Release before the test ends even when an assertion fails, so the
	// blocked handler cannot outlive it.
	var releaseOnce = func() func() {
		done := false
		return func() {
			if !done {
				done = true
				close(release)
			}
		}
	}()
	defer releaseOnce()

	first := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		first <- postJSON(t, h, "/account/coins", coinsRequest(addr))
	}()
	testutil.RequireReceive(
		t, entered, 5*time.Second, "first read to hold its slot",
	)

	// The deadline only keeps a regression that admits this request from
	// hanging the run: its handler would otherwise block in the fake.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	raw, err := json.Marshal(coinsRequest(addr))
	require.NoError(t, err)
	refused := httptest.NewRecorder()
	h.ServeHTTP(refused, httptest.NewRequestWithContext(
		ctx, http.MethodPost, "/account/coins", bytes.NewReader(raw),
	))
	requireMeshError(
		t, refused, ErrUnavailable, http.StatusServiceUnavailable,
	)
	select {
	case <-entered:
		t.Fatal("a refused request must not reach the ledger")
	default:
	}

	releaseOnce()
	decodeResponse[AccountCoinsResponse](
		t,
		testutil.RequireReceive(
			t, first, 5*time.Second, "first read to finish",
		),
	)
	// The slot is free again once the first read completes.
	decodeResponse[AccountCoinsResponse](
		t, postJSON(t, h, "/account/coins", coinsRequest(addr)),
	)
}
