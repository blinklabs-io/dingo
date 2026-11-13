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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// A certifying ranking block applies its closure before its own transactions,
// all under the ranking block's point. Position-ranked readers order by the
// stored block index, so the body's transactions must be stored after every
// closure transaction.
func TestLeiosCertifyingBlockStoresBodyAfterClosureTransactions(t *testing.T) {
	t.Parallel()
	deposit := pathTx{top: pathLevel{
		fields: pathDeposits(map[cbor.ByteString]uint64{
			pathKeyAccount(pathSignerKey): leiosCertApplyDeposit,
		}),
		funds: leiosCertApplyDeposit,
	}}
	f := newLeiosCertApplyFixtureWithTransactions(
		t,
		nil,
		pathSignerKey,
		[]pathTx{
			{top: pathLevel{fields: map[uint]any{
				4: []any{pathRegistration(pathSignerKey)},
			}}},
			deposit,
			deposit,
		},
	)
	closureTxs := f.txs[:2]
	bodyTx := f.txs[2]
	f.ebTxs = f.ebTxs[:2]

	parent := f.announce()
	block := &dijkstra.DijkstraBlock{
		BlockHeader: dijkstraTestBlockHeader(
			2,
			leiosCertApplyCertifyingSlot,
			lcommon.NewBlake2b256(parent.Hash().Bytes()),
			lcommon.Blake2b256{},
			0,
		),
	}
	block.BlockHeader.LeiosHeaderExtension = []cbor.RawMessage{
		leiosTestRaw(t, true),
		{0xf6},
	}
	block.BlockBody.LeiosCertificate = &dijkstra.DijkstraLeiosCertificate{
		Signers:             []byte{0x80},
		AggregatedSignature: make([]byte, 48),
	}
	block.BlockBody.Transactions = append(
		block.BlockBody.Transactions, *bodyTx,
	)
	block.BlockHeader.Body.BlockBodyHash = block.BlockBody.Hash()
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)
	offsets, err := database.NewBlockIndexer(
		block.SlotNumber(), block.Hash().Bytes(),
	).ComputeOffsets(blockCbor, block)
	require.NoError(t, err)

	require.NoError(t, f.ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{block},
	))
	require.NoError(t, f.process(block, offsets, envelopeParent{
		slot:        parent.SlotNumber(),
		blockNumber: parent.BlockNumber(),
	}))

	hashes := make([][]byte, 0, 3)
	for _, tx := range append(append([]*dijkstra.DijkstraTransaction{}, closureTxs...), bodyTx) {
		hashes = append(hashes, tx.Hash().Bytes())
	}
	stored, err := f.db.GetTransactionsByHashes(t.Context(), hashes, nil)
	require.NoError(t, err)
	require.Len(t, stored, 3, "closure and body transactions are all stored")
	indexByHash := make(map[string]uint32, len(stored))
	for _, tx := range stored {
		indexByHash[string(tx.Hash)] = tx.BlockIndex
	}
	bodyIndex := indexByHash[string(bodyTx.Hash().Bytes())]
	for _, tx := range closureTxs {
		require.Less(t, indexByHash[string(tx.Hash().Bytes())], bodyIndex,
			"the body applies after the closure, so it must be stored after it")
	}
}
