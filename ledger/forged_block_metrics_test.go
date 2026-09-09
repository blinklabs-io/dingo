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
	"bytes"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	dingotestutil "github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc_cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type recordForgedBlockTestBlock struct {
	hash        lcommon.Blake2b256
	prevHash    lcommon.Blake2b256
	slot        uint64
	blockNumber uint64
	cbor        []byte
}

func newRecordForgedBlockTestBlock(
	slot uint64,
	blockNumber uint64,
) *recordForgedBlockTestBlock {
	return &recordForgedBlockTestBlock{
		hash:        lcommon.NewBlake2b256(bytes.Repeat([]byte{0x10}, 32)),
		prevHash:    lcommon.NewBlake2b256(bytes.Repeat([]byte{0x20}, 32)),
		slot:        slot,
		blockNumber: blockNumber,
		cbor:        []byte{0x83, 0x01, 0x02},
	}
}

func (b *recordForgedBlockTestBlock) Header() lcommon.BlockHeader { return b }
func (b *recordForgedBlockTestBlock) Type() int {
	return int(babbage.BlockTypeBabbage)
}
func (b *recordForgedBlockTestBlock) Transactions() []lcommon.Transaction {
	return nil
}
func (b *recordForgedBlockTestBlock) Utxorpc() (*utxorpc_cardano.Block, error) {
	return nil, nil
}
func (b *recordForgedBlockTestBlock) Hash() lcommon.Blake2b256 {
	return b.hash
}
func (b *recordForgedBlockTestBlock) PrevHash() lcommon.Blake2b256 {
	return b.prevHash
}
func (b *recordForgedBlockTestBlock) BlockNumber() uint64 {
	return b.blockNumber
}
func (b *recordForgedBlockTestBlock) SlotNumber() uint64 {
	return b.slot
}
func (b *recordForgedBlockTestBlock) IssuerVkey() lcommon.IssuerVkey {
	return lcommon.IssuerVkey{}
}
func (b *recordForgedBlockTestBlock) BlockBodySize() uint64 {
	return 0
}
func (b *recordForgedBlockTestBlock) Era() lcommon.Era {
	return babbage.EraBabbage
}
func (b *recordForgedBlockTestBlock) Cbor() []byte {
	return b.cbor
}
func (b *recordForgedBlockTestBlock) BlockBodyHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func TestRecordForgedBlockIncrementsCounterBeforeAdoption(t *testing.T) {
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()
	_, events := eb.Subscribe(event.BlockForgedEventType)

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus: eb,
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())

	block := newRecordForgedBlockTestBlock(42, 7)
	blockCbor := []byte{0x83, 0xaa, 0xbb}
	expectedHash := block.Hash().Bytes()
	expectedTxCount := uint(len(block.Transactions()))
	beforeRecord := time.Now()
	ls.RecordForgedBlock(block, blockCbor, 5*time.Millisecond)
	afterRecord := time.Now()

	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(ls.metrics.blocksForgedTotal),
	)
	evt := dingotestutil.RequireReceive(
		t,
		events,
		time.Second,
		"block forged event",
	)
	forged, ok := evt.Data.(event.BlockForgedEvent)
	require.True(t, ok)
	assert.Equal(t, uint64(42), forged.Slot)
	assert.Equal(t, uint64(7), forged.BlockNumber)
	assert.Equal(t, expectedHash, forged.BlockHash)
	assert.Equal(t, expectedTxCount, forged.TxCount)
	assert.Equal(t, uint(len(blockCbor)), forged.BlockSize)
	assert.False(t, forged.Timestamp.Before(beforeRecord))
	assert.False(t, forged.Timestamp.After(afterRecord))
}
