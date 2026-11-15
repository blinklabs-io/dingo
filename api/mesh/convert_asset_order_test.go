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
	"math/big"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/stretchr/testify/require"
)

func assetOpIdentifiers(ops []*Operation) []string {
	var ret []string
	for _, op := range ops {
		if op.Amount == nil || op.Amount.Currency == nil ||
			op.Amount.Currency.Symbol == "ADA" {
			continue
		}
		ret = append(ret, op.CoinChange.CoinIdentifier.Identifier)
	}
	return ret
}

// TestConvertBodyToOpsNativeAssetsAreSorted: MultiAsset iteration is
// randomized, so operation order for a multi-policy output must be sorted
// by policy ID then asset name.
func TestConvertBodyToOpsNativeAssetsAreSorted(t *testing.T) {
	t.Parallel()

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		testKeyHash(0x01),
		nil,
	)
	require.NoError(t, err)
	data := map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{}
	for i := range 12 {
		var policy lcommon.Blake2b224
		policy[0] = byte((i * 37) % 251)
		data[policy] = map[cbor.ByteString]*big.Int{}
		for _, name := range []string{"b", "a", "ab", "aa"} {
			data[policy][cbor.NewByteString([]byte(name))] = big.NewInt(1)
		}
	}
	ma := lcommon.NewMultiAsset(data)
	output := babbage.BabbageTransactionOutput{
		OutputAddress: addr,
		OutputAmount: mary.MaryTransactionOutputValue{
			Amount: 1_000_000,
			Assets: &ma,
		},
	}

	for range 10 {
		ops := convertBodyToOps(
			nil,
			[]lcommon.TransactionOutput{output},
			nil, nil, "aa",
		)
		ids := assetOpIdentifiers(ops)
		require.Len(t, ids, 48)
		require.True(
			t,
			slices.IsSorted(ids),
			"native asset operations are not in canonical order",
		)
	}
}

// TestConvertTransactionAssetOperationsFollowCanonicalOrder: assets loaded in
// arbitrary (for example legacy insertion) order still produce operations in
// canonical order.
func TestConvertTransactionAssetOperationsFollowCanonicalOrder(t *testing.T) {
	t.Parallel()

	txHash := testHash(0x4a)
	paymentKey := testKeyHash(0x4c)
	p1, p2 := testKeyHash(0x01), testKeyHash(0x02)
	tx := convertTransaction(
		models.Transaction{
			Hash:  txHash,
			Valid: true,
			Outputs: []models.Utxo{
				testUtxo(txHash, 0, 1_000_000, paymentKey, []models.Asset{
					testAsset(p2, []byte("b"), 1),
					testAsset(p1, []byte("b"), 1),
					testAsset(p2, []byte("a"), 1),
					testAsset(p1, []byte("a"), 1),
				}),
			},
		},
		lcommon.AddressNetworkTestnet,
	)
	var got [][2][]byte
	for _, op := range tx.Operations[1:] {
		got = append(got, [2][]byte{
			[]byte(op.Amount.Currency.Metadata["policyId"].(string)),
			[]byte(op.Amount.Currency.Symbol),
		})
	}
	require.Len(t, got, 4)
	require.True(t, slices.IsSortedFunc(got, func(a, b [2][]byte) int {
		if c := bytes.Compare(a[0], b[0]); c != 0 {
			return c
		}
		return bytes.Compare(a[1], b[1])
	}))
}
