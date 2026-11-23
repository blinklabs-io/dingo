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

package cardano_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/sdk/cardano"
	"github.com/blinklabs-io/dingo/sdk/chain"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
)

// goldenDir holds the ouroboros-consensus Cardano goldens ouroboros-mock
// embeds; the Conway block there carries one transaction.
const goldenDir = "ouroboros-consensus/ouroboros-consensus-cardano/golden/" +
	"cardano/CardanoNodeToNodeVersion2/"

func goldenFixture(t *testing.T, name string) fixtures.Fixture {
	t.Helper()
	f, err := fixtures.NewHarness(fixtures.HarnessConfig{}).
		Fixture(goldenDir + name)
	if err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	return f
}

// testBlock returns the golden Conway block, decoded, with its type and the
// bytes it was decoded from.
func testBlock(t *testing.T) (lcommon.Block, uint, []byte) {
	t.Helper()
	f := goldenFixture(t, "Block_Conway")
	typ, err := f.LedgerBlockType()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := f.LedgerBlockBytes()
	if err != nil {
		t.Fatal(err)
	}
	b, err := gledger.NewBlockFromCbor(typ, raw)
	if err != nil {
		t.Fatalf("decode block: %v", err)
	}
	return b, typ, raw
}

// testHeader returns the golden Conway header, decoded, with its type and the
// bytes it was decoded from.
func testHeader(t *testing.T) (lcommon.BlockHeader, uint, []byte) {
	t.Helper()
	f := goldenFixture(t, "Header_Conway")
	typ, err := f.LedgerHeaderType()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := f.LedgerHeaderBytes()
	if err != nil {
		t.Fatal(err)
	}
	h, err := gledger.NewBlockHeaderFromCbor(typ, raw)
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	return h, typ, raw
}

func TestBlockPayloadFromBytesDecodes(t *testing.T) {
	t.Parallel()

	block, typ, raw := testBlock(t)
	view := cardano.NewBlock(block).View()
	if view.Type != typ || view.Slot != block.SlotNumber() ||
		view.Number != block.BlockNumber() ||
		view.TxCount != len(block.Transactions()) ||
		!bytes.Equal(view.Hash, block.Hash().Bytes()) ||
		!bytes.Equal(view.PrevHash, block.PrevHash().Bytes()) {
		t.Fatalf("view = %+v", view)
	}

	got, err := cardano.BlockOf(chain.NewPayload(view, raw))
	if err != nil {
		t.Fatalf("BlockOf: %v", err)
	}
	if got.Hash() != block.Hash() || !bytes.Equal(got.Cbor(), raw) {
		t.Fatal("block decoded from bytes differs from the original")
	}
}

func TestHeaderPayloadRoundTrips(t *testing.T) {
	t.Parallel()

	header, typ, raw := testHeader(t)
	sent := cardano.NewHeader(typ, header)
	view := sent.View()
	if view.Type != typ || view.Slot != header.SlotNumber() ||
		view.Number != header.BlockNumber() ||
		!bytes.Equal(view.Hash, header.Hash().Bytes()) {
		t.Fatalf("view = %+v", view)
	}
	native, err := cardano.HeaderOf(sent)
	if err != nil || native != header {
		t.Fatalf("HeaderOf native = %v, %v; want the sent header", native, err)
	}

	encoded, err := sent.Bytes()
	if err != nil || !bytes.Equal(encoded, raw) {
		t.Fatalf(
			"Bytes = %d bytes, %v; want the original bytes",
			len(encoded),
			err,
		)
	}
	got, err := cardano.HeaderOf(chain.NewPayload(view, encoded))
	if err != nil {
		t.Fatalf("HeaderOf bytes-only: %v", err)
	}
	if got.Hash() != header.Hash() {
		t.Fatalf("hash = %x; want %x", got.Hash(), header.Hash())
	}
}

func TestHeaderOfRejectsUndecodableBytes(t *testing.T) {
	t.Parallel()

	_, err := cardano.HeaderOf(chain.NewPayload(
		chain.HeaderView{Type: conway.BlockTypeConway}, []byte{0xff},
	))
	if !errors.Is(err, cardano.ErrNotHeader) {
		t.Fatalf("HeaderOf error = %v; want ErrNotHeader", err)
	}
}

func TestNewHeaderOfBlockCarriesHeaderOnly(t *testing.T) {
	t.Parallel()

	block, typ, _ := testBlock(t)
	sent := cardano.NewHeader(typ, block)
	if _, ok := chain.NativeOf[lcommon.Block](sent); ok {
		t.Fatal("header payload carries the whole block")
	}
	encoded, err := sent.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	if !bytes.Equal(encoded, block.Header().Cbor()) {
		t.Fatalf("header payload encoded %d bytes; want the %d header bytes",
			len(encoded), len(block.Header().Cbor()))
	}
	got, err := cardano.HeaderOf(chain.NewPayload(sent.View(), encoded))
	if err != nil {
		t.Fatalf("HeaderOf bytes-only: %v", err)
	}
	if got.Hash() != block.Hash() {
		t.Fatalf("hash = %x; want %x", got.Hash(), block.Hash())
	}
}

// assembledBlock returns a generated Conway block with its retained CBOR
// cleared, as a block the forger assembles carries none. The golden block
// cannot stand in: its upstream encoding is not the one gouroboros produces, so
// re-encoding it breaks its committed body hash.
func assembledBlock(t *testing.T) (lcommon.Block, *conway.ConwayBlock) {
	t.Helper()
	block, err := fixtures.GenerateBlock(conway.EraConway, 1, 10)
	if err != nil {
		t.Fatalf("generate block: %v", err)
	}
	conwayBlock, ok := block.(*conway.ConwayBlock)
	if !ok {
		t.Fatalf("generated block is %T; want *conway.ConwayBlock", block)
	}
	conwayBlock.SetCbor(nil)
	return block, conwayBlock
}

// TestPayloadsWithoutRetainedCborEncode covers values assembled rather than
// decoded, whose Cbor is empty: their payload must still produce bytes.
func TestPayloadsWithoutRetainedCborEncode(t *testing.T) {
	t.Parallel()

	block, conwayBlock := assembledBlock(t)
	typ := uint(conway.BlockTypeConway)
	encoded, err := cardano.NewBlock(conwayBlock).Bytes()
	if err != nil || len(encoded) == 0 {
		t.Fatalf(
			"block Bytes = %d bytes, %v; want an encoding",
			len(encoded),
			err,
		)
	}
	decoded, err := gledger.NewBlockFromCbor(typ, encoded)
	if err != nil || decoded.Hash() != block.Hash() {
		t.Fatalf("re-encoded block decodes to %v, %v", decoded, err)
	}

	tx, _ := testTx(t)
	conwayTx, ok := tx.(*conway.ConwayTransaction)
	if !ok {
		t.Fatalf("test tx is %T; want *conway.ConwayTransaction", tx)
	}
	conwayTx.SetCbor(nil)
	encoded, err = cardano.NewTx(conwayTx).Bytes()
	if err != nil || len(encoded) == 0 {
		t.Fatalf("tx Bytes = %d bytes, %v; want an encoding", len(encoded), err)
	}
	decodedTx, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway), encoded,
	)
	if err != nil || decodedTx.Hash() != tx.Hash() {
		t.Fatalf("re-encoded tx decodes to %v, %v", decodedTx, err)
	}
}
