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
	"context"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/sdk/cardano"
	"github.com/blinklabs-io/dingo/sdk/chain"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

const testTxHex = "84a700818258200c07395aed88bdddc6de0518d1462dd0ec7e52e1e3a53599f7cdb24dc80237f8010181a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a02dc6c00021a001e84800b5820192d0c0c2c2320e843e080b5f91a9ca35155bc50f3ef3bfdbc72c1711b86367e0d818258203af629a5cd75f76d0cc21172e1193b85f199ca78e837c3965d77d7d6bc90206b0010a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a006acfc0111a002dc6c0a4008182582025fcacade3fffc096b53bdaf4c7d012bded303c9edbee686d24b372dae60aa1b58409da928a064ff9f795110bdcb8ab05d2a7a023dd15ebc42044f102ce366c0c9077024c7951c2d63584b7d2eea7bf1da4a7453bde4c99dd083889c1e2e2e3db804048119077a0581840000187b820a0a06814746010000222601f4f6"

func testTx(t *testing.T) (lcommon.Transaction, []byte) {
	t.Helper()
	raw, err := hex.DecodeString(testTxHex)
	if err != nil {
		t.Fatalf("decode tx hex: %v", err)
	}
	tx, err := gledger.NewTransactionFromCbor(uint(conway.EraIdConway), raw)
	if err != nil {
		t.Fatalf("decode tx: %v", err)
	}
	return tx, raw
}

// fakeBlock implements only what the payload view and the roles read; any
// other method call panics on the nil embedded interface.
type fakeBlock struct {
	lcommon.Block
	cbor []byte
}

func (b fakeBlock) Type() int                           { return 7 }
func (b fakeBlock) Transactions() []lcommon.Transaction { return nil }
func (b fakeBlock) Hash() lcommon.Blake2b256            { return lcommon.Blake2b256{1} }
func (b fakeBlock) PrevHash() lcommon.Blake2b256        { return lcommon.Blake2b256{2} }
func (b fakeBlock) SlotNumber() uint64                  { return 42 }
func (b fakeBlock) BlockNumber() uint64                 { return 9 }
func (b fakeBlock) Cbor() []byte                        { return b.cbor }

type ledgerSource struct {
	gotTx lcommon.Transaction
	tip   ochainsync.Tip
}

func (s *ledgerSource) SecurityParam() int  { return 2160 }
func (s *ledgerSource) Tip() ochainsync.Tip { return s.tip }
func (s *ledgerSource) ValidateTx(tx lcommon.Transaction) error {
	s.gotTx = tx
	return nil
}

func TestTxPayloadViewDescribesTransaction(t *testing.T) {
	t.Parallel()

	tx, _ := testTx(t)
	view := cardano.NewTx(tx).View()
	if view.Type != uint(conway.EraIdConway) {
		t.Fatalf("view type = %d; want %d", view.Type, conway.EraIdConway)
	}
	if !bytes.Equal(view.Hash, tx.Hash().Bytes()) {
		t.Fatalf("view hash = %x; want %x", view.Hash, tx.Hash().Bytes())
	}
}

func TestLedgerRoleBuiltInPathKeepsDecodedTx(t *testing.T) {
	t.Parallel()

	tx, _ := testTx(t)
	src := &ledgerSource{}
	role, err := cardano.NewLedgerRole(src)
	if err != nil {
		t.Fatal(err)
	}
	encodes := 0
	payload := chain.NewNativePayload(
		cardano.NewTx(tx).View(),
		tx,
		func() ([]byte, error) {
			encodes++
			return tx.Cbor(), nil
		},
	)

	if err := role.ValidateTx(payload); err != nil {
		t.Fatalf("ValidateTx: %v", err)
	}
	if src.gotTx != tx {
		t.Fatal("source received a different value than the one sent")
	}
	if encodes != 0 {
		t.Fatalf("built-in path encoded %d times; want 0", encodes)
	}
}

func TestLedgerRoleExternalPathDecodesBytes(t *testing.T) {
	t.Parallel()

	tx, raw := testTx(t)
	src := &ledgerSource{}
	role, err := cardano.NewLedgerRole(src)
	if err != nil {
		t.Fatal(err)
	}
	payload := chain.NewPayload(
		chain.TxView{Type: uint(conway.EraIdConway)}, raw,
	)

	if err := role.ValidateTx(payload); err != nil {
		t.Fatalf("ValidateTx: %v", err)
	}
	if src.gotTx == nil {
		t.Fatal("source received no transaction")
	}
	if src.gotTx.Hash() != tx.Hash() ||
		!bytes.Equal(src.gotTx.Cbor(), raw) {
		t.Fatal("transaction decoded from bytes differs from the original")
	}
}

func TestTxRoundTripsThroughBytes(t *testing.T) {
	t.Parallel()

	tx, raw := testTx(t)
	sent := cardano.NewTx(tx)
	encoded, err := sent.Bytes()
	if err != nil || !bytes.Equal(encoded, raw) {
		t.Fatalf("Bytes = %x, %v; want the original bytes", encoded, err)
	}
	received := chain.NewPayload(sent.View(), encoded)
	got, err := cardano.TxOf(received)
	if err != nil {
		t.Fatalf("TxOf: %v", err)
	}
	if got.Hash() != tx.Hash() {
		t.Fatalf("hash = %x; want %x", got.Hash(), tx.Hash())
	}
}

func TestTxOfRejectsUndecodableBytes(t *testing.T) {
	t.Parallel()

	_, err := cardano.TxOf(chain.NewPayload(
		chain.TxView{Type: uint(conway.EraIdConway)}, []byte{0xff, 0x00},
	))
	if !errors.Is(err, cardano.ErrNotTx) {
		t.Fatalf("TxOf error = %v; want ErrNotTx", err)
	}
	_, err = cardano.TxOf(chain.NewNativePayload[chain.TxView](
		chain.TxView{}, nil, nil,
	))
	if !errors.Is(err, cardano.ErrNotTx) {
		t.Fatalf("TxOf without bytes error = %v; want ErrNotTx", err)
	}
}

func TestLedgerRoleReportsSourceState(t *testing.T) {
	t.Parallel()

	src := &ledgerSource{tip: ochainsync.Tip{BlockNumber: 5}}
	src.tip.Point.Slot = 77
	src.tip.Point.Hash = []byte{9}
	role, err := cardano.NewLedgerRole(src)
	if err != nil {
		t.Fatal(err)
	}
	if role.SecurityParam() != 2160 {
		t.Fatalf("SecurityParam = %d", role.SecurityParam())
	}
	tip := role.Tip()
	if tip.BlockNumber != 5 || tip.Point.Slot != 77 ||
		!bytes.Equal(tip.Point.Hash, []byte{9}) {
		t.Fatalf("Tip = %+v", tip)
	}
}

func TestBlockPayloadViewAndBuiltInPath(t *testing.T) {
	t.Parallel()

	block := fakeBlock{cbor: []byte{0x80}}
	payload := cardano.NewBlock(block)
	view := payload.View()
	if view.Type != 7 || view.Slot != 42 || view.Number != 9 ||
		view.TxCount != 0 ||
		!bytes.Equal(view.Hash, lcommon.Blake2b256{1}.Bytes()) ||
		!bytes.Equal(view.PrevHash, lcommon.Blake2b256{2}.Bytes()) {
		t.Fatalf("view = %+v", view)
	}
	got, err := cardano.BlockOf(payload)
	if err != nil {
		t.Fatal(err)
	}
	if got.Hash() != block.Hash() {
		t.Fatal("BlockOf returned a different block")
	}
	_, err = cardano.BlockOf(chain.NewPayload(
		chain.BlockView{HeaderView: chain.HeaderView{Type: 7}},
		[]byte{0xff},
	))
	if !errors.Is(err, cardano.ErrNotBlock) {
		t.Fatalf("BlockOf error = %v; want ErrNotBlock", err)
	}
}

type consensusSource struct {
	gotBlock lcommon.Block
	gotCbor  []byte
}

func (s *consensusSource) ShouldProduceBlock(slot uint64) bool {
	return slot%2 == 0
}

func (s *consensusSource) NextLeaderSlot(from uint64) (uint64, bool) {
	return from + 1, true
}

func (s *consensusSource) ValidateForgedBlock(
	_ context.Context, block lcommon.Block, blockCbor []byte,
) error {
	s.gotBlock, s.gotCbor = block, blockCbor
	return nil
}

func TestConsensusRole(t *testing.T) {
	t.Parallel()

	src := &consensusSource{}
	var gotA, gotB ochainsync.Tip
	role, err := cardano.NewConsensusRole(
		src,
		func(a, b ochainsync.Tip) chain.Comparison {
			gotA, gotB = a, b
			return chain.ComparisonBBetter
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !role.ShouldProduceBlock(4) || role.ShouldProduceBlock(3) {
		t.Fatal("ShouldProduceBlock did not follow the source")
	}
	if slot, ok := role.NextLeaderSlot(10); !ok || slot != 11 {
		t.Fatalf("NextLeaderSlot = %d, %v", slot, ok)
	}
	a := chain.Tip{Point: chain.Point{Slot: 1, Hash: []byte{1}}, BlockNumber: 1}
	b := chain.Tip{Point: chain.Point{Slot: 2, Hash: []byte{2}}, BlockNumber: 2}
	if got := role.Compare(a, b); got != chain.ComparisonBBetter {
		t.Fatalf("Compare = %d", got)
	}
	if gotA.Point.Slot != 1 || gotB.BlockNumber != 2 ||
		!bytes.Equal(gotB.Point.Hash, []byte{2}) {
		t.Fatalf("comparer saw %+v and %+v", gotA, gotB)
	}

	block := fakeBlock{cbor: []byte{0x80}}
	if err := role.ValidateForgedBlock(
		t.Context(), cardano.NewBlock(block),
	); err != nil {
		t.Fatal(err)
	}
	if src.gotBlock == nil || src.gotBlock.Hash() != block.Hash() ||
		!bytes.Equal(src.gotCbor, []byte{0x80}) {
		t.Fatal("source did not receive the forged block and its bytes")
	}
}

func TestConsensusRoleWithoutComparerIsUnknown(t *testing.T) {
	t.Parallel()

	role, err := cardano.NewConsensusRole(&consensusSource{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := role.Compare(chain.Tip{}, chain.Tip{}); got != chain.ComparisonUnknown {
		t.Fatalf("Compare = %d; want unknown", got)
	}
}

type networkSink struct {
	gotBlock lcommon.Block
	gotCbor  []byte
}

func (s *networkSink) AddBlock(
	_ context.Context, block lcommon.Block, cbor []byte,
) error {
	s.gotBlock, s.gotCbor = block, cbor
	return nil
}

func TestNetworkRoleBroadcastsDecodedBlock(t *testing.T) {
	t.Parallel()

	sink := &networkSink{}
	role, err := cardano.NewNetworkRole(sink)
	if err != nil {
		t.Fatal(err)
	}
	block := fakeBlock{cbor: []byte{0x80}}
	if err := role.Broadcast(t.Context(), cardano.NewBlock(block)); err != nil {
		t.Fatal(err)
	}
	if sink.gotBlock == nil || sink.gotBlock.Hash() != block.Hash() ||
		!bytes.Equal(sink.gotCbor, []byte{0x80}) {
		t.Fatal("sink did not receive the block and its bytes")
	}
}

type mempoolSource struct {
	gotType uint
	gotRaw  []byte
	removed string
}

func (s *mempoolSource) AddTransaction(
	_ context.Context, txType uint, txBytes []byte,
) error {
	s.gotType, s.gotRaw = txType, txBytes
	return nil
}

func (s *mempoolSource) RemoveTransaction(txHash string) { s.removed = txHash }

func TestMempoolRolePassesTypeAndBytes(t *testing.T) {
	t.Parallel()

	tx, raw := testTx(t)
	src := &mempoolSource{}
	role, err := cardano.NewMempoolRole(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := role.AddTx(t.Context(), cardano.NewTx(tx)); err != nil {
		t.Fatal(err)
	}
	if src.gotType != uint(conway.EraIdConway) ||
		!bytes.Equal(src.gotRaw, raw) {
		t.Fatalf("pool received type %d, %d bytes", src.gotType, len(src.gotRaw))
	}
	role.RemoveTx("abc")
	if src.removed != "abc" {
		t.Fatalf("removed = %q", src.removed)
	}
}

func TestRoleConstructorsRejectNilSources(t *testing.T) {
	t.Parallel()

	if _, err := cardano.NewLedgerRole(nil); !errors.Is(err, cardano.ErrNilSource) {
		t.Fatalf("ledger: %v", err)
	}
	if _, err := cardano.NewConsensusRole(nil, nil); !errors.Is(err, cardano.ErrNilSource) {
		t.Fatalf("consensus: %v", err)
	}
	if _, err := cardano.NewNetworkRole(nil); !errors.Is(err, cardano.ErrNilSource) {
		t.Fatalf("network: %v", err)
	}
	if _, err := cardano.NewMempoolRole(nil); !errors.Is(err, cardano.ErrNilSource) {
		t.Fatalf("mempool: %v", err)
	}
}
