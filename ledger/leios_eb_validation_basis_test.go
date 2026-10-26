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
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// leiosBasisFirstTxHash returns the hash of the first transaction a path
// fixture builds. Fixture transactions are deterministic, so a later fixture
// can build a transaction that spends that one's output.
func leiosBasisFirstTxHash(t *testing.T) []byte {
	t.Helper()
	return newPathFixture(t, nil, nil, pathTx{}).txs[0].Hash().Bytes()
}

// leiosBasisSpend spends output 0 of txHash. The fixture's own input is
// replaced, so the value it carries is exactly what that output holds.
func leiosBasisSpend(txHash []byte) pathTx {
	return pathTx{top: pathLevel{fields: map[uint]any{
		0: []any{[]any{txHash, uint64(0)}},
	}}}
}

// leiosBasisEndorserTx encodes tx as an endorser-block transaction, which
// carries the trailing is_valid flag.
func leiosBasisEndorserTx(
	t *testing.T,
	tx *dijkstra.DijkstraTransaction,
) []byte {
	t.Helper()
	var parts []cbor.RawMessage
	_, err := cbor.Decode(tx.Cbor(), &parts)
	require.NoError(t, err)
	raw, err := cbor.Encode(append(parts, cbor.RawMessage{0xf5}))
	require.NoError(t, err)
	return raw
}

// leiosBasisCertifier builds the block that certifies parent's endorser block,
// as leiosCertApplyFixture.certifyingChild does, but with its header CBOR set
// before anything hashes it. The fixture's blocks otherwise all hash empty
// header CBOR, so a certifier would share its parent's hash and a check of
// which of the two is the ledger tip could not tell them apart.
func leiosBasisCertifier(
	f *leiosCertApplyFixture,
	parent *dijkstra.DijkstraBlock,
) (*dijkstra.DijkstraBlock, *database.BlockIngestionResult, envelopeParent) {
	t := f.t
	t.Helper()
	block := &dijkstra.DijkstraBlock{
		BlockHeader: dijkstraTestBlockHeader(
			2,
			leiosCertApplyCertifyingSlot,
			parent.Hash(),
			lcommon.Blake2b256{},
			0,
		),
	}
	block.BlockHeader.LeiosHeaderExtension = []cbor.RawMessage{
		leiosTestRaw(t, true),
		cbor.RawMessage{0xf6},
	}
	block.BlockBody.LeiosCertificate = &dijkstra.DijkstraLeiosCertificate{
		Signers:             []byte{0x80},
		AggregatedSignature: make([]byte, 48),
	}
	block.BlockHeader.Body.BlockBodyHash = block.BlockBody.Hash()
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	headerCbor, err := cbor.Encode(block.BlockHeader)
	require.NoError(t, err)
	block.BlockHeader.SetCbor(headerCbor)
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)
	require.NotEqual(t, parent.Hash(), block.Hash())
	offsets, err := database.NewBlockIndexer(
		leiosCertApplyCertifyingSlot,
		block.Hash().Bytes(),
	).ComputeOffsets(blockCbor, block)
	require.NoError(t, err)
	return block, offsets, envelopeParent{
		slot:        parent.SlotNumber(),
		blockNumber: parent.BlockNumber(),
	}
}

func leiosBasisSetTip(ls *LedgerState, block gledger.Block) {
	ls.SetTipForTesting(ochainsync.Tip{
		Point: ocommon.Point{
			Slot: block.SlotNumber(),
			Hash: block.Hash().Bytes(),
		},
		BlockNumber: block.BlockNumber(),
	})
}

// CIP-164 makes an endorser block's transactions an extension of the ledger
// state its announcing ranking block leaves, so they may spend that block's
// own outputs. The announcing block's parent state cannot see them.
func TestLeiosEndorserBlockSpendsAnnouncingBlockOutput(t *testing.T) {
	t.Parallel()
	f := newPathFixture(
		t,
		nil,
		[]pathTx{{}},
		leiosBasisSpend(leiosBasisFirstTxHash(t)),
	)
	header := f.block.Header()
	ebTxs := [][]byte{leiosBasisEndorserTx(t, f.txs[1])}

	err := f.ls.ValidateLeiosEndorserBlockTransactions(
		t.Context(),
		header,
		LeiosValidationParentHash(header),
		ebTxs,
	)
	require.ErrorIs(t, err, ErrInvalidLeiosEndorserBlockTransactions,
		"the parent state has no output from the announcing block's body")

	f.storeOriginAndBlock(true)
	require.NoError(t, f.applyBlock())
	leiosBasisSetTip(f.ls, f.block)
	require.NoError(t, f.ls.ValidateLeiosEndorserBlockTransactions(
		t.Context(),
		header,
		LeiosEndorserBlockBasisHash(header),
		ebTxs,
	))
}

// A ranking block that certifies its parent's endorser block applies that
// endorser block's transactions before its own, so an endorser block it
// announces may spend their outputs.
func TestLeiosEndorserBlockSpendsCertifiedEndorserBlockOutput(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixtureWithTransactions(
		t,
		nil,
		pathSignerKey,
		[]pathTx{{}, leiosBasisSpend(leiosBasisFirstTxHash(t))},
	)
	// The certified endorser block holds only the first transaction; the
	// second belongs to the endorser block the certifier announces.
	announcedTxs := [][]byte{f.ebTxs[1]}
	f.ebTxs = f.ebTxs[:1]

	parent := f.announce()
	leiosBasisSetTip(f.ls, parent)
	certifier, offsets, envParent := leiosBasisCertifier(f, parent)
	// The validator reads only the header's slot and hash, so the
	// certifier's header stands in for the one that announces the second
	// endorser block.
	header := certifier.Header()

	err := f.ls.ValidateLeiosEndorserBlockTransactions(
		t.Context(),
		header,
		LeiosValidationParentHash(header),
		announcedTxs,
	)
	require.ErrorIs(t, err, ErrInvalidLeiosEndorserBlockTransactions,
		"the parent state has no output from the certified endorser block")

	require.NoError(t, f.ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{certifier},
	))
	require.NoError(t, f.process(certifier, offsets, envParent))
	leiosBasisSetTip(f.ls, certifier)
	require.NoError(t, f.ls.ValidateLeiosEndorserBlockTransactions(
		t.Context(),
		header,
		LeiosEndorserBlockBasisHash(header),
		announcedTxs,
	))
}
