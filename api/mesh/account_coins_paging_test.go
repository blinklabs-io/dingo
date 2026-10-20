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
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/stretchr/testify/require"
)

// TestAccountCoinsPagesRealDatabase follows next_cursor against the real
// ordered address scan. Outputs share a slot and an output index, so the
// cursor must carry slot, output index and tx id exactly as the database
// orders them; a base address sharing the payment key forces pages that
// examine candidates without returning them.
func TestAccountCoinsPagesRealDatabase(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)

	payment := testKeyHash(0x61)
	target := testAddress(t, lcommon.AddressTypeKeyNone, payment, nil)
	noise := testAddress(
		t, lcommon.AddressTypeKeyKey, payment, testKeyHash(0x62),
	)
	policyLow := lcommon.NewBlake2b224(testKeyHash(0xd1))
	policyHigh := lcommon.NewBlake2b224(testKeyHash(0xd2))
	ma := lcommon.NewMultiAsset(
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeOutput{
			policyHigh: {
				cbor.NewByteString([]byte("b")): big.NewInt(3),
				cbor.NewByteString([]byte("a")): big.NewInt(2),
			},
			policyLow: {cbor.NewByteString([]byte("x")): big.NewInt(1)},
		},
	)

	txA, txB, txC, txD := testHash(0xa1), testHash(0xa2), testHash(0xa3),
		testHash(0xa4)
	type row struct {
		txID   []byte
		idx    uint32
		slot   uint64
		addr   string
		assets bool
	}
	rows := []row{
		{txA, 0, 10, target, false},
		{txB, 0, 10, target, false},
		{txA, 1, 10, noise, false},
		{txA, 2, 10, target, true},
		{txC, 0, 20, noise, false},
		{txC, 1, 20, noise, false},
		{txC, 2, 20, noise, false},
		{txC, 3, 20, noise, false},
		{txC, 4, 20, noise, false},
		{txD, 0, 30, target, false},
	}
	require.NoError(t, db.Transaction(t.Context(), true).Do(
		func(txn *database.Txn) error {
			for _, r := range rows {
				out := testOutput(t, r.addr, 1_000_000)
				if r.assets {
					out.OutputAmount = mary.MaryTransactionOutputValue{
						Amount: 1_000_000, Assets: &ma,
					}
				}
				raw, err := cbor.Encode(&out)
				if err != nil {
					return err
				}
				u := testUtxo(r.txID, r.idx, 1_000_000, payment, nil)
				u.AddedSlot = r.slot
				if r.addr == noise {
					u.StakingKey = testKeyHash(0x62)
				}
				if err := db.CreateUtxo(t.Context(), txn, &u); err != nil {
					return err
				}
				if err := db.Blob().SetUtxo(
					txn.Blob(), r.txID, r.idx, raw,
				); err != nil {
					return err
				}
			}
			return nil
		},
	))

	deps := newTestDeps()
	deps.ledger.boundedPage = db.UtxosByAddressBoundedPage
	h := newTestHandler(t, deps, func(c *ServerConfig) {
		c.accountCoinsPageSize = 2
		c.accountCoinsMaxCandidates = 3
	})

	var seen []string
	var cursor any
	emptyContinuations := 0
	pages := 0
	for {
		req := coinsRequest(target)
		if cursor != nil {
			req.Metadata = map[string]any{"cursor": cursor}
		}
		resp := decodeResponse[AccountCoinsResponse](
			t, postJSON(t, h, "/account/coins", req),
		)
		pages++
		require.Less(t, pages, 20, "cursor must advance")
		for _, c := range resp.Coins {
			seen = append(seen, c.CoinIdentifier.Identifier)
		}
		next, more := resp.Metadata["next_cursor"]
		if !more {
			break
		}
		if len(resp.Coins) == 0 {
			emptyContinuations++
		}
		cursor = next
	}

	assetCoin := func(policy lcommon.Blake2b224, name string) string {
		return hexString(txA) + ":2:" + hexString(policy.Bytes()) + ":" +
			hexString([]byte(name))
	}
	require.Equal(t, []string{
		hexString(txA) + ":0",
		hexString(txB) + ":0",
		hexString(txA) + ":2",
		assetCoin(policyLow, "x"),
		assetCoin(policyHigh, "a"),
		assetCoin(policyHigh, "b"),
		hexString(txD) + ":0",
	}, seen, "every coin of the address once, in ledger order")
	require.Positive(
		t, emptyContinuations,
		"a page that only examined other addresses must still continue",
	)
}

// TestAccountCoinsCancelledReadFreesSlot holds the only bulk slot with a
// request whose client has gone away, then requires the next request to be
// admitted.
func TestAccountCoinsCancelledReadFreesSlot(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x63), nil,
	)
	deps.ledger.boundedPage = func(
		ctx context.Context,
		_ *models.UtxoWithOrderingQuery,
		_ int,
		_ int64,
	) (models.UtxoAddressPage, error) {
		return models.UtxoAddressPage{}, ctx.Err()
	}
	h := newTestHandler(t, deps, func(c *ServerConfig) {
		c.maxConcurrentBulkReads = 1
	})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	raw, err := json.Marshal(coinsRequest(addr))
	require.NoError(t, err)
	gone := httptest.NewRecorder()
	h.ServeHTTP(gone, httptest.NewRequestWithContext(
		ctx, http.MethodPost, "/account/coins", bytes.NewReader(raw),
	))
	require.Empty(t, gone.Body.String(), "no body for a departed client")

	decodeResponse[AccountCoinsResponse](
		t, postJSON(t, h, "/account/coins", coinsRequest(addr)),
	)
}
