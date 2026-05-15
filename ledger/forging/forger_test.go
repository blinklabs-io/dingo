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

package forging

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc_cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type forgerTestLeader struct{}

func (forgerTestLeader) ShouldProduceBlock(uint64) bool { return true }

func (forgerTestLeader) NextLeaderSlot(
	fromSlot uint64,
) (uint64, bool) {
	return fromSlot, true
}

type forgerTestSlotClock struct {
	currentSlot       uint64
	chainTipSlot      uint64
	upstreamTipSlot   uint64
	slotsPerKESPeriod uint64
}

func (c forgerTestSlotClock) CurrentSlot() (uint64, error) {
	return c.currentSlot, nil
}

func (c forgerTestSlotClock) SlotsPerKESPeriod() uint64 {
	return c.slotsPerKESPeriod
}

func (c forgerTestSlotClock) ChainTipSlot() uint64 {
	return c.chainTipSlot
}

func (forgerTestSlotClock) NextSlotTime() (time.Time, error) {
	return time.Now(), nil
}

func (c forgerTestSlotClock) UpstreamTipSlot() uint64 {
	return c.upstreamTipSlot
}

type forgerTestBuilder struct {
	block ledger.Block
	cbor  []byte
	calls int
}

func (b *forgerTestBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	b.calls++
	return b.block, b.cbor, nil
}

type forgerTestBroadcaster struct {
	err   error
	calls int
}

func (b *forgerTestBroadcaster) AddBlock(
	ledger.Block,
	[]byte,
) error {
	b.calls++
	return b.err
}

type forgerTestBlock struct {
	hash        lcommon.Blake2b256
	prevHash    lcommon.Blake2b256
	slot        uint64
	blockNumber uint64
	cbor        []byte
}

func newForgerTestBlock(slot, blockNumber uint64) *forgerTestBlock {
	return &forgerTestBlock{
		hash:        lcommon.NewBlake2b256(bytes.Repeat([]byte{0x01}, 32)),
		prevHash:    lcommon.NewBlake2b256(bytes.Repeat([]byte{0x02}, 32)),
		slot:        slot,
		blockNumber: blockNumber,
		cbor:        []byte{0x83, 0x01, 0x02},
	}
}

func (b *forgerTestBlock) Header() lcommon.BlockHeader { return b }
func (b *forgerTestBlock) Type() int                   { return int(babbage.BlockTypeBabbage) }
func (b *forgerTestBlock) Transactions() []lcommon.Transaction {
	return nil
}
func (b *forgerTestBlock) Utxorpc() (*utxorpc_cardano.Block, error) {
	return nil, nil
}
func (b *forgerTestBlock) Hash() lcommon.Blake2b256          { return b.hash }
func (b *forgerTestBlock) PrevHash() lcommon.Blake2b256      { return b.prevHash }
func (b *forgerTestBlock) BlockNumber() uint64               { return b.blockNumber }
func (b *forgerTestBlock) SlotNumber() uint64                { return b.slot }
func (b *forgerTestBlock) IssuerVkey() lcommon.IssuerVkey    { return lcommon.IssuerVkey{} }
func (b *forgerTestBlock) BlockBodySize() uint64             { return 0 }
func (b *forgerTestBlock) Era() lcommon.Era                  { return babbage.EraBabbage }
func (b *forgerTestBlock) Cbor() []byte                      { return b.cbor }
func (b *forgerTestBlock) BlockBodyHash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }

func TestCheckAndForgeProductionObservesForgedBlockWhenNotAdopted(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	blockCbor := []byte{0x83, 0xaa, 0xbb}
	builder := &forgerTestBuilder{
		block: block,
		cbor:  blockCbor,
	}
	broadcaster := &forgerTestBroadcaster{
		err: errors.New("not adopted"),
	}
	var (
		observedBlock   ledger.Block
		observedCbor    []byte
		observedLatency time.Duration
	)

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		BlockForged: func(
			block ledger.Block,
			cbor []byte,
			latency time.Duration,
		) {
			observedBlock = block
			observedCbor = append([]byte(nil), cbor...)
			observedLatency = latency
		},
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	err = forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to add block")

	require.Same(t, block, observedBlock)
	assert.Equal(t, blockCbor, observedCbor)
	assert.GreaterOrEqual(t, observedLatency, time.Duration(0))
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeForged))
	assert.Equal(t, float64(0), testutil.ToFloat64(forger.metrics.forgeAdopted))
}

func TestCheckAndForgeProductionRecoversBlockForgedObserverPanic(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	blockCbor := []byte{0x83, 0xaa, 0xbb}
	builder := &forgerTestBuilder{
		block: block,
		cbor:  blockCbor,
	}
	broadcaster := &forgerTestBroadcaster{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		BlockForged: func(
			ledger.Block,
			[]byte,
			time.Duration,
		) {
			panic("observer panic")
		},
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeForged))
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
}
