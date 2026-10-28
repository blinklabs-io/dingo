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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func indexedTxID(i int) []byte {
	id := make([]byte, 32)
	binary.BigEndian.PutUint64(id[24:], uint64(i)+1) // #nosec G115
	return id
}

// TestAccountCoinsReturnsAccountsPastAnyCardinality streams an account with
// more UTxOs and coins than a single page or any former response bound, with
// multi-asset outputs on both sides of page boundaries, and checks every coin
// arrives once, each UTxO followed by its assets, in scan order.
func TestAccountCoinsReturnsAccountsPastAnyCardinality(t *testing.T) {
	t.Parallel()

	const total = 10_001
	key := testKeyHash(0x40)
	addr := testAddress(t, lcommon.AddressTypeKeyNone, key, nil)
	policy := testKeyHash(0xcd)
	deps := newTestDeps()
	deps.ledger.utxos = func([]lcommon.Address) ([]models.Utxo, error) {
		t.Fatal("account coins must not use the bounded balance lookup")
		return nil, nil
	}
	var want []string
	utxos := make([]models.Utxo, 0, total)
	for i := range total {
		var assets []models.Asset
		// Page size 500: multi-asset outputs at the last and first slot of
		// a page and in the middle.
		if i%500 == 499 || i%500 == 0 || i%997 == 0 {
			assets = []models.Asset{
				testAsset(policy, []byte("a"), 1),
				testAsset(policy, []byte("b"), 2),
			}
		}
		u := testUtxo(indexedTxID(i), 0, 1_000_000, key, assets)
		utxos = append(utxos, u)
		base := hexString(u.TxId) + ":0"
		want = append(want, base)
		for _, a := range assets {
			want = append(want, base+":"+hexString(policy)+":"+hexString(a.Name))
		}
	}
	deps.ledger.utxosComplete = func(
		context.Context,
		[]lcommon.Address,
	) ([]models.Utxo, error) {
		return utxos, nil
	}

	resp := decodeResponse[AccountCoinsResponse](
		t,
		postJSON(t, newTestHandler(t, deps), "/account/coins", coinsRequest(addr)),
	)
	got := make([]string, 0, len(resp.Coins))
	for _, c := range resp.Coins {
		got = append(got, c.CoinIdentifier.Identifier)
	}
	require.Equal(t, want, got)
}

// reusingPageSource serves pages from one backing slice and one set of
// transaction-id buffers that it overwrites for every page, the way a
// storage layer that reuses its page buffers would. A handler that kept any
// page past its visit would emit overwritten coins.
type reusingPageSource struct {
	t        *testing.T
	pages    int
	pageSize int
	page     []models.UtxoWithOrdering
	ids      [][]byte
	limits   []int
	// written reports the coins already on the wire.
	written func() int
	emitted int
}

func (s *reusingPageSource) scan(
	ctx context.Context,
	q *models.UtxoWithOrderingQuery,
	maxCandidates int,
	maxBytes int64,
	visit func([]models.UtxoWithOrdering) error,
) (ochainsync.Tip, error) {
	s.limits = append(s.limits, q.Limit)
	require.Equal(s.t, defaultAccountCoinsPageCandidates, maxCandidates)
	require.EqualValues(s.t, defaultAccountCoinsPageBytes, maxBytes)
	for p := range s.pages {
		// Every coin of the previous page reached the writer before the
		// next page is read.
		require.Equal(s.t, s.emitted, s.written(), "page %d", p)
		s.page = s.page[:0]
		for i := range s.pageSize {
			n := p*s.pageSize + i
			binary.BigEndian.PutUint64(s.ids[i][24:], uint64(n)+1) // #nosec G115
			s.page = append(s.page, models.UtxoWithOrdering{
				Utxo: models.Utxo{TxId: s.ids[i], Amount: 1},
			})
		}
		if err := visit(s.page); err != nil {
			return ochainsync.Tip{}, err
		}
		s.emitted += s.pageSize
	}
	return ochainsync.Tip{
		Point: ocommon.NewPoint(5, testHash(0x05)), BlockNumber: 5,
	}, nil
}

// TestAccountCoinsHoldsOnePage proves the handler encodes and flushes each
// page before the next is read and retains nothing from it, so its memory is
// bounded by one page whatever the account size.
func TestAccountCoinsHoldsOnePage(t *testing.T) {
	t.Parallel()

	const pageSize, pages = 4, 6
	addr := testAddress(t, lcommon.AddressTypeKeyNone, testKeyHash(0x41), nil)
	rec := httptest.NewRecorder()
	src := &reusingPageSource{
		t: t, pages: pages, pageSize: pageSize,
		page: make([]models.UtxoWithOrdering, 0, pageSize),
		ids:  make([][]byte, pageSize),
		written: func() int {
			return strings.Count(rec.Body.String(), `"coin_identifier"`)
		},
	}
	for i := range src.ids {
		src.ids[i] = make([]byte, 32)
	}
	deps := newTestDeps()
	deps.ledger.utxos = func([]lcommon.Address) ([]models.Utxo, error) {
		t.Fatal("account coins must not use a full-set read")
		return nil, nil
	}
	deps.ledger.scan = src.scan
	h := newTestHandler(t, deps, func(c *ServerConfig) {
		c.accountCoinsPageSize = pageSize
	})
	body, err := json.Marshal(coinsRequest(addr))
	require.NoError(t, err)
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodPost, "/account/coins", bytes.NewReader(body),
	))

	resp := decodeResponse[AccountCoinsResponse](t, rec)
	require.Equal(t, []int{pageSize}, src.limits)
	require.Len(t, resp.Coins, pageSize*pages)
	for i, c := range resp.Coins {
		require.Equal(
			t, hexString(indexedTxID(i))+":0", c.CoinIdentifier.Identifier,
		)
	}
	require.Equal(t, int64(5), resp.BlockIdentifier.Index)
}

// TestAccountCoinsMidStreamFailureIsNotAWellFormedDocument fails the scan
// after a page is already on the wire. The status line can no longer carry a
// Mesh error, so the body must not parse as a complete response a client
// could take for the whole coin set; the bulk slot must still be returned.
func TestAccountCoinsMidStreamFailureIsNotAWellFormedDocument(t *testing.T) {
	t.Parallel()

	key := testKeyHash(0x42)
	addr := testAddress(t, lcommon.AddressTypeKeyNone, key, nil)
	deps := newTestDeps()
	var failNext atomic.Bool
	failNext.Store(true)
	deps.ledger.scan = func(
		_ context.Context,
		_ *models.UtxoWithOrderingQuery,
		_ int,
		_ int64,
		visit func([]models.UtxoWithOrdering) error,
	) (ochainsync.Tip, error) {
		if err := visit([]models.UtxoWithOrdering{
			{Utxo: testUtxo(testHash(1), 0, 1_000_000, key, nil)},
		}); err != nil {
			return ochainsync.Tip{}, err
		}
		if failNext.CompareAndSwap(true, false) {
			return ochainsync.Tip{}, errors.New("storage failed mid-scan")
		}
		return ochainsync.Tip{}, nil
	}
	srv := httptest.NewServer(newTestHandler(t, deps, func(c *ServerConfig) {
		c.maxConcurrentBulkReads = 1
	}))
	defer srv.Close()
	post := func() (*http.Response, []byte, error) {
		body, err := json.Marshal(coinsRequest(addr))
		require.NoError(t, err)
		resp, err := srv.Client().Post(
			srv.URL+"/account/coins", "application/json",
			bytes.NewReader(body),
		)
		require.NoError(t, err)
		defer resp.Body.Close()
		raw, readErr := io.ReadAll(resp.Body)
		return resp, raw, readErr
	}

	resp, raw, readErr := post()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Error(t, readErr, "body: %s", raw)
	require.False(t, json.Valid(raw), "body: %s", raw)

	resp, raw, readErr = post()
	require.NoError(t, readErr)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	var full AccountCoinsResponse
	require.NoError(t, json.Unmarshal(raw, &full), string(raw))
	require.Len(t, full.Coins, 1)
}

// TestAccountCoinsFailureBeforeFirstPageIsAMeshError keeps the error
// contract for a scan that fails before any coin is written.
func TestAccountCoinsFailureBeforeFirstPageIsAMeshError(t *testing.T) {
	t.Parallel()

	addr := testAddress(t, lcommon.AddressTypeKeyNone, testKeyHash(0x43), nil)
	deps := newTestDeps()
	deps.ledger.scan = func(
		context.Context,
		*models.UtxoWithOrderingQuery,
		int,
		int64,
		func([]models.UtxoWithOrdering) error,
	) (ochainsync.Tip, error) {
		return ochainsync.Tip{}, fmt.Errorf("snapshot unavailable")
	}
	requireMeshError(
		t,
		postJSON(t, newTestHandler(t, deps), "/account/coins", coinsRequest(addr)),
		ErrInternal,
		http.StatusInternalServerError,
	)
}
