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
	"strings"
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
	block      ledger.Block
	cbor       []byte
	calls      int
	leiosCalls int
	leiosData  LeiosBlockData
}

func (b *forgerTestBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	b.calls++
	return b.block, b.cbor, nil
}

func (b *forgerTestBuilder) BuildBlockWithLeios(
	_ uint64,
	_ uint64,
	leiosData LeiosBlockData,
) (ledger.Block, []byte, error) {
	b.leiosCalls++
	b.leiosData = leiosData
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

type forgerTestLeiosChecker struct {
	calls   int
	allowed bool
	reason  string
	err     error
}

func (c *forgerTestLeiosChecker) MayProduceEndorserBlock(
	uint64,
) (bool, string, error) {
	c.calls++
	return c.allowed, c.reason, c.err
}

type forgerTestLeiosCaster struct {
	slot     uint64
	hash     []byte
	cbor     []byte
	txBodies [][]byte
}

func (c *forgerTestLeiosCaster) BroadcastEndorserBlock(
	slot uint64,
	hash []byte,
	cbor []byte,
	txBodies [][]byte,
) error {
	c.slot = slot
	c.hash = append([]byte(nil), hash...)
	c.cbor = append([]byte(nil), cbor...)
	c.txBodies = append([][]byte(nil), txBodies...)
	return nil
}

type forgerTestMempoolProvider struct {
	txs []MempoolTransaction
}

func (p forgerTestMempoolProvider) Transactions() []MempoolTransaction {
	return p.txs
}

type forgerTestLeiosCerts struct {
	eligible []LeiosCertifiedEndorserBlock
	marked   []lcommon.Blake2b256
}

func (p *forgerTestLeiosCerts) EligibleCertifiedEndorserBlocks() []LeiosCertifiedEndorserBlock {
	return p.eligible
}

func (p *forgerTestLeiosCerts) MarkEndorserBlockEmbedded(
	ebHash lcommon.Blake2b256,
) {
	p.marked = append(p.marked, ebHash)
}

type forgerTestLeiosParentAnnouncement struct {
	hash  lcommon.Blake2b256
	ok    bool
	err   error
	calls int
}

func (p *forgerTestLeiosParentAnnouncement) ParentLeiosAnnouncement() (
	lcommon.Blake2b256,
	bool,
	error,
) {
	p.calls++
	return p.hash, p.ok, p.err
}

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

func TestCheckAndForgeProductionAnnouncesForgedLeiosEB(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	leiosChecker := &forgerTestLeiosChecker{allowed: true}
	leiosCaster := &forgerTestLeiosCaster{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		LeiosProduceChecker: leiosChecker,
		LeiosEBBroadcaster:  leiosCaster,
		LeiosMempool: forgerTestMempoolProvider{
			txs: []MempoolTransaction{
				{
					Hash: strings.Repeat("11", 32),
					Cbor: []byte{0x83, 0x01, 0x02, 0x03},
				},
			},
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, leiosChecker.calls)
	require.NotEmpty(t, leiosCaster.hash)
	require.Equal(t, uint64(10), leiosCaster.slot)
	require.Equal(t, 1, builder.leiosCalls)
	require.NotNil(t, builder.leiosData.Announcement)
	require.Nil(t, builder.leiosData.Certificate)
	assert.Equal(t, leiosCaster.hash, builder.leiosData.Announcement.Hash.Bytes())
	assert.Equal(t, uint64(len(leiosCaster.cbor)), builder.leiosData.Announcement.Size)
}

func TestCheckAndForgeProductionCertifiesLeiosEBAfterAdoption(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	ebHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x33}, 32))
	cert := &lcommon.LeiosEbCertificate{
		SlotNo:              9,
		EndorserBlockHash:   ebHash,
		Signers:             []byte{0x80},
		AggregatedSignature: make([]byte, lcommon.LeiosBlsSignatureSize),
	}
	leiosCerts := &forgerTestLeiosCerts{
		eligible: []LeiosCertifiedEndorserBlock{
			{
				SlotNo:            9,
				EndorserBlockHash: ebHash,
				Certificate:       cert,
			},
		},
	}
	parent := &forgerTestLeiosParentAnnouncement{hash: ebHash, ok: true}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		LeiosCertificateProvider:        leiosCerts,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.leiosCalls)
	require.Nil(t, builder.leiosData.Announcement)
	require.Same(t, cert, builder.leiosData.Certificate)
	require.Equal(t, []lcommon.Blake2b256{ebHash}, leiosCerts.marked)
	require.Equal(t, 1, parent.calls)
}

func TestCheckAndForgeProductionCertifiesOnlyParentAnnouncedLeiosEB(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	wrongHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x22}, 32))
	parentHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x33}, 32))
	wrongCert := &lcommon.LeiosEbCertificate{
		SlotNo:              8,
		EndorserBlockHash:   wrongHash,
		Signers:             []byte{0x80},
		AggregatedSignature: make([]byte, lcommon.LeiosBlsSignatureSize),
	}
	parentCert := &lcommon.LeiosEbCertificate{
		SlotNo:              9,
		EndorserBlockHash:   parentHash,
		Signers:             []byte{0x80},
		AggregatedSignature: make([]byte, lcommon.LeiosBlsSignatureSize),
	}
	leiosCerts := &forgerTestLeiosCerts{
		eligible: []LeiosCertifiedEndorserBlock{
			{
				SlotNo:            8,
				EndorserBlockHash: wrongHash,
				Certificate:       wrongCert,
			},
			{
				SlotNo:            9,
				EndorserBlockHash: parentHash,
				Certificate:       parentCert,
			},
		},
	}
	parent := &forgerTestLeiosParentAnnouncement{hash: parentHash, ok: true}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		LeiosCertificateProvider:        leiosCerts,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.leiosCalls)
	require.Nil(t, builder.leiosData.Announcement)
	require.Same(t, parentCert, builder.leiosData.Certificate)
	require.Equal(t, []lcommon.Blake2b256{parentHash}, leiosCerts.marked)
	require.Equal(t, 1, parent.calls)
}
