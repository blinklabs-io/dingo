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

package utxorpc

import (
	"bytes"
	"context"
	"math/big"
	"slices"
	"testing"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	sync "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
)

func orderTestMultiAsset[T int64 | uint64 | *big.Int](
	amount T,
) common.MultiAsset[T] {
	data := map[common.Blake2b224]map[cbor.ByteString]T{}
	for i := range 12 {
		var policy common.Blake2b224
		policy[0] = byte((i * 37) % 251)
		data[policy] = map[cbor.ByteString]T{}
		for _, name := range []string{"b", "a", "ab", "aa"} {
			data[policy][cbor.NewByteString([]byte(name))] = amount
		}
	}
	return common.NewMultiAsset(data)
}

func requireCanonicalMultiassets(
	t *testing.T,
	groups []*cardano.Multiasset,
) {
	t.Helper()
	require.Len(t, groups, 12)
	require.True(
		t,
		slices.IsSortedFunc(groups, func(a, b *cardano.Multiasset) int {
			return bytes.Compare(a.GetPolicyId(), b.GetPolicyId())
		}),
		"policies are not in canonical order",
	)
	for _, group := range groups {
		require.Len(t, group.GetAssets(), 4)
		require.True(
			t,
			slices.IsSortedFunc(
				group.GetAssets(),
				func(a, b *cardano.Asset) int {
					return bytes.Compare(a.GetName(), b.GetName())
				},
			),
			"asset names are not in canonical order",
		)
	}
}

// TestWatchTxTransactionAssetsAreCanonical: gouroboros builds the UTxO RPC
// multiasset lists from Go maps, so the server must put them in canonical
// order before sending.
func TestWatchTxTransactionAssetsAreCanonical(t *testing.T) {
	t.Parallel()
	outAssets := orderTestMultiAsset(big.NewInt(5))
	mint := orderTestMultiAsset(big.NewInt(5))
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxOutputs: []babbage.BabbageTransactionOutput{{
				OutputAmount: mary.MaryTransactionOutputValue{
					Amount: 1_000_000,
					Assets: &outAssets,
				},
			}},
			TxMint: &mint,
		},
		TxIsValid: true,
	}
	for range 10 {
		applied, _, err := watchTxBuildForwardMessagesForTransactions(
			[]common.Transaction{tx}, 1, 1, []byte{1},
			func(common.Transaction) bool { return true },
		)
		require.NoError(t, err)
		require.Len(t, applied, 1)
		got := applied[0].GetCardano()
		require.Len(t, got.GetOutputs(), 1)
		requireCanonicalMultiassets(t, got.GetOutputs()[0].GetAssets())
		requireCanonicalMultiassets(t, got.GetMint())
	}
}

func TestCanonicalizeBlockAssetsCoversNestedOutputs(t *testing.T) {
	t.Parallel()
	shuffled := func() []*cardano.Multiasset {
		return []*cardano.Multiasset{
			{
				PolicyId: []byte{2},
				Assets: []*cardano.Asset{
					{Name: []byte("b")},
					{Name: []byte("a")},
				},
			},
			{
				PolicyId: []byte{1},
				Assets: []*cardano.Asset{
					{Name: []byte("b")},
					{Name: []byte("a")},
				},
			},
		}
	}
	tx := &cardano.Tx{
		Outputs: []*cardano.TxOutput{{Assets: shuffled()}},
		Mint:    shuffled(),
		Inputs: []*cardano.TxInput{
			{AsOutput: &cardano.TxOutput{Assets: shuffled()}},
		},
		Collateral: &cardano.Collateral{
			CollateralReturn: &cardano.TxOutput{Assets: shuffled()},
		},
		ReferenceInputs: []*cardano.TxInput{
			{AsOutput: &cardano.TxOutput{Assets: shuffled()}},
		},
	}
	block := &cardano.Block{Body: &cardano.BlockBody{Tx: []*cardano.Tx{tx}}}
	canonicalizeBlockAssets(block)
	for _, groups := range [][]*cardano.Multiasset{
		tx.Outputs[0].Assets, tx.Mint, tx.Inputs[0].AsOutput.Assets,
		tx.Collateral.CollateralReturn.Assets,
		tx.ReferenceInputs[0].AsOutput.Assets,
	} {
		require.Equal(t, []byte{1}, groups[0].PolicyId)
		require.Equal(t, []byte("a"), groups[0].Assets[0].Name)
		require.Equal(t, []byte{2}, groups[1].PolicyId)
		require.Equal(t, []byte("a"), groups[1].Assets[0].Name)
	}
}

// orderTestBlock is a stored Conway block whose single transaction has an
// output and a mint field carrying orderTestMultiAsset.
func orderTestBlock(t *testing.T) models.Block {
	t.Helper()
	outAssets := orderTestMultiAsset[common.MultiAssetTypeOutput](big.NewInt(5))
	mint := orderTestMultiAsset[common.MultiAssetTypeMint](big.NewInt(5))
	addr, err := common.NewAddressFromParts(
		common.AddressTypeKeyNone,
		common.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x01}, 28),
		nil,
	)
	require.NoError(t, err)
	block := &conway.ConwayBlock{
		BlockHeader: &conway.ConwayBlockHeader{},
		TransactionBodies: []conway.ConwayTransactionBody{{
			TxOutputs: []babbage.BabbageTransactionOutput{{
				OutputAddress: addr,
				OutputAmount: mary.MaryTransactionOutputValue{
					Amount: 1_000_000,
					Assets: &outAssets,
				},
			}},
			TxMint: &mint,
		}},
		TransactionWitnessSets: []conway.ConwayTransactionWitnessSet{{}},
	}
	block.BlockHeader.Body.BlockNumber = 1
	block.BlockHeader.Body.Slot = 100
	// The decoder requires the header's byte-string fields to be present.
	block.BlockHeader.Body.VrfKey = make([]byte, 32)
	block.BlockHeader.Body.VrfResult.Output = make([]byte, 64)
	block.BlockHeader.Body.VrfResult.Proof = make([]byte, 80)
	block.BlockHeader.Body.OpCert.HotVkey = make([]byte, 32)
	block.BlockHeader.Body.OpCert.Signature = make([]byte, 64)
	block.BlockHeader.Body.ProtoVersion.Major = 10
	block.BlockHeader.Signature = make([]byte, 448)
	// The decoder checks the body hash: blake2b256 over the concatenated
	// hashes of the four body components.
	tmp, err := cbor.Encode(block)
	require.NoError(t, err)
	var comps []cbor.RawMessage
	_, err = cbor.Decode(tmp, &comps)
	require.NoError(t, err)
	require.Len(t, comps, 5)
	var concat []byte
	for i := 1; i < 5; i++ {
		h := common.Blake2b256Hash(comps[i])
		concat = append(concat, h.Bytes()...)
	}
	block.BlockHeader.Body.BlockBodyHash = common.Blake2b256Hash(concat)
	encoded, err := cbor.Encode(block)
	require.NoError(t, err)
	return models.Block{
		Slot:   100,
		Number: 1,
		Type:   conway.BlockTypeConway,
		Cbor:   encoded,
	}
}

func requireCanonicalBlockAssets(t *testing.T, acb *sync.AnyChainBlock) {
	t.Helper()
	txs := acb.GetCardano().GetBody().GetTx()
	require.Len(t, txs, 1)
	require.Len(t, txs[0].GetOutputs(), 1)
	requireCanonicalMultiassets(t, txs[0].GetOutputs()[0].GetAssets())
	requireCanonicalMultiassets(t, txs[0].GetMint())
}

func TestFetchBlockAssetsAreCanonical(t *testing.T) {
	t.Parallel()
	block := orderTestBlock(t)
	stub := &blockLookupStub{
		lookup: func(context.Context, ocommon.Point) (models.Block, error) {
			return block, nil
		},
	}
	srv := &syncServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
		Logger:      discardLogger(),
		LedgerState: stub,
	})}
	for range 10 {
		resp, err := srv.FetchBlock(
			context.Background(),
			connect.NewRequest(&sync.FetchBlockRequest{
				Ref: []*sync.BlockRef{
					{Slot: block.Slot, Hash: make([]byte, 32)},
				},
			}),
		)
		require.NoError(t, err)
		require.Len(t, resp.Msg.GetBlock(), 1)
		requireCanonicalBlockAssets(t, resp.Msg.GetBlock()[0])
	}
}

func TestFollowTipApplyAssetsAreCanonical(t *testing.T) {
	t.Parallel()
	block := orderTestBlock(t)
	srv := newFollowTipTimestampServer(&followTipTimestampLedger{
		tip: ochainsync.Tip{Point: ocommon.NewPoint(200, []byte{0xaa})},
	})
	for range 10 {
		resp, err := srv.followTipResponse(&chain.ChainIteratorResult{
			Point: ocommon.NewPoint(block.Slot, make([]byte, 32)),
			Block: block,
		})
		require.NoError(t, err)
		apply, ok := resp.GetAction().(*sync.FollowTipResponse_Apply)
		require.True(t, ok)
		requireCanonicalBlockAssets(t, apply.Apply)
	}
}
