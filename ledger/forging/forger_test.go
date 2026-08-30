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
	"github.com/blinklabs-io/gouroboros/ledger/conway"
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
	upstreamActive    bool
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

func (c forgerTestSlotClock) UpstreamSyncStatus() (uint64, bool) {
	return c.upstreamTipSlot, c.upstreamActive || c.upstreamTipSlot > 0
}

func TestCheckAndForgeProductionWaitsForUnknownActiveUpstreamTarget(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
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
			upstreamActive:    true,
			slotsPerKESPeriod: 100,
		},
		ForgeSyncToleranceSlots: 99,
		PromRegistry:            prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Zero(t, builder.calls)
	assert.Zero(t, broadcaster.calls)
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
	panic bool
	calls int
}

func (b *forgerTestBroadcaster) AddBlock(
	ledger.Block,
	[]byte,
) error {
	b.calls++
	if b.panic {
		panic("broadcaster panic")
	}
	return b.err
}

// forgerTestPanicOnceLeader panics on its first ShouldProduceBlock
// call and reports leadership normally afterward, for exercising the
// forge cycle that follows a recovered panic.
type forgerTestPanicOnceLeader struct {
	calls int
}

func (l *forgerTestPanicOnceLeader) ShouldProduceBlock(uint64) bool {
	l.calls++
	if l.calls == 1 {
		panic("leader check panic")
	}
	return true
}

func (l *forgerTestPanicOnceLeader) NextLeaderSlot(
	fromSlot uint64,
) (uint64, bool) {
	return fromSlot, true
}

type forgerTestBlock struct {
	hash         lcommon.Blake2b256
	prevHash     lcommon.Blake2b256
	slot         uint64
	blockNumber  uint64
	cbor         []byte
	transactions []lcommon.Transaction
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

func (b *forgerTestBlock) Type() int { return int(babbage.BlockTypeBabbage) }
func (b *forgerTestBlock) Transactions() []lcommon.Transaction {
	return b.transactions
}
func (b *forgerTestBlock) Utxorpc() (*utxorpc_cardano.Block, error) {
	return nil, nil
}
func (b *forgerTestBlock) Hash() lcommon.Blake2b256 { return b.hash }

func (b *forgerTestBlock) PrevHash() lcommon.Blake2b256 { return b.prevHash }

func (b *forgerTestBlock) BlockNumber() uint64 { return b.blockNumber }
func (b *forgerTestBlock) SlotNumber() uint64  { return b.slot }

func (b *forgerTestBlock) IssuerVkey() lcommon.IssuerVkey { return lcommon.IssuerVkey{} }
func (b *forgerTestBlock) BlockBodySize() uint64          { return 0 }

func (b *forgerTestBlock) Era() lcommon.Era { return babbage.EraBabbage }
func (b *forgerTestBlock) Cbor() []byte     { return b.cbor }

func (b *forgerTestBlock) BlockBodyHash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }

type forgerTestLeiosChecker struct {
	calls   int
	allowed bool
	reason  string
	err     error
}

type forgerTestConfirmedTxRemover struct {
	hashes []string
}

func (r *forgerTestConfirmedTxRemover) RemoveTxsByHash(hashes []string) {
	r.hashes = append(r.hashes, hashes...)
}

func TestCheckAndForgeProductionRemovesConfirmedTransactions(t *testing.T) {
	creds := setupTestCredentials(t)
	tx, err := conway.NewConwayTransactionFromCbor(
		makeMinimalTxCbor(t, 0x42, 0),
	)
	require.NoError(t, err)
	block := newForgerTestBlock(10, 2)
	block.transactions = []lcommon.Transaction{tx}
	remover := &forgerTestConfirmedTxRemover{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block, cbor: block.cbor},
		BlockBroadcaster: &forgerTestBroadcaster{},
		ConfirmedTxs:     remover,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, []string{tx.Hash().String()}, remover.hashes)
}

func TestCheckAndForgeProductionUsesRetainedReconnectFrontier(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(114220801, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       114220801,
			chainTipSlot:      114220600,
			upstreamTipSlot:   114220800,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Zero(t, builder.calls)
	assert.Zero(t, broadcaster.calls)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeSyncSkip),
	)
}

func TestCheckAndForgeProductionWaitsForEventPairedCorroboratedTarget(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(101, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       101,
			chainTipSlot:      100,
			upstreamTipSlot:   200,
			upstreamActive:    true,
			slotsPerKESPeriod: 100,
		},
		ForgeSyncToleranceSlots: 99,
		PromRegistry:            prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Zero(t, builder.calls)
	assert.Zero(t, broadcaster.calls)
}

func TestCheckAndForgeProductionProceedsWithoutUpstreamFrontier(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
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
			// This is the value exposed after a close-before-switch event.
			upstreamTipSlot: 0,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
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
	eligible   []LeiosCertifiedEndorserBlock
	txHashes   []string
	txHashesOK bool
	marked     []lcommon.Blake2b256
}

func (p *forgerTestLeiosCerts) EligibleCertifiedEndorserBlocks() []LeiosCertifiedEndorserBlock {
	return p.eligible
}

func (p *forgerTestLeiosCerts) CertifiedEndorserBlockTxHashes(
	lcommon.Blake2b256,
) ([]string, bool) {
	return p.txHashes, p.txHashesOK
}

func (p *forgerTestLeiosCerts) MarkEndorserBlockEmbedded(
	ebHash lcommon.Blake2b256,
) {
	p.marked = append(p.marked, ebHash)
}

type forgerTestLeiosParentAnnouncement struct {
	rbHash lcommon.Blake2b256
	hash   lcommon.Blake2b256
	ok     bool
	err    error
	calls  int
}

func (p *forgerTestLeiosParentAnnouncement) ParentLeiosAnnouncement() (
	lcommon.Blake2b256,
	lcommon.Blake2b256,
	bool,
	error,
) {
	p.calls++
	return p.rbHash, p.hash, p.ok, p.err
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
	innerBroadcaster := &forgerTestBroadcaster{
		err: errors.New("not adopted"),
	}
	var callOrder []string
	broadcaster := &trackingBroadcaster{
		inner: innerBroadcaster,
		onAdd: func() { callOrder = append(callOrder, "adopt") },
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
			callOrder = append(callOrder, "observe")
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
	assert.Equal(t, []string{"adopt", "observe"}, callOrder)
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, innerBroadcaster.calls)
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

func TestCheckAndForgeProductionRecoversLeaderCheckPanic(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	leader := &forgerTestPanicOnceLeader{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	// A panic from the leader checker must not escape checkAndForgeProduction
	// (which would otherwise crash the producer-loop goroutine); it is
	// treated as "not leader" for the slot, same as a checker that simply
	// returns false.
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, leader.calls)
	assert.Equal(t, 0, builder.calls)
	assert.Equal(t, 0, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeNotLeader))
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgePanicRecovered.WithLabelValues("selection"),
		),
	)

	// The following forge cycle proceeds normally: worker accounting and
	// running state were not corrupted by the recovered panic.
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 2, leader.calls)
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeForged))
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
}

func TestCheckAndForgeProductionRecoversBlockValidatorPanic(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	broadcaster := &forgerTestBroadcaster{}
	validator := &forgerTestValidator{panic: true}

	forger := newForgerWithValidator(t, block, nil, broadcaster, validator)

	// A panic from the validator must not escape checkAndForgeProduction; it
	// is treated as a validation failure so the block is dropped rather than
	// adopted with unknown validity.
	err := forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "self-validation failed")
	assert.Equal(t, 1, validator.calls)
	assert.Equal(t, 0, broadcaster.calls)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeValidationFailed),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgePanicRecovered.WithLabelValues("validation"),
		),
	)

	// The following forge cycle proceeds normally.
	validator.panic = false
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 2, validator.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
}

func TestCheckAndForgeProductionRecoversBlockBroadcasterPanic(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{panic: true}

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
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	// A panic from the broadcaster must not escape checkAndForgeProduction;
	// it is treated as a publish failure, matching the existing error path
	// for a broadcaster that returns an error.
	err = forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to add block")
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeForged))
	assert.Equal(t, float64(0), testutil.ToFloat64(forger.metrics.forgeAdopted))
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgePanicRecovered.WithLabelValues("publication"),
		),
	)

	// The following forge cycle proceeds normally.
	broadcaster.panic = false
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 2, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
}

func TestNewBlockForgerRejectsProductionLeiosWithoutTxValidator(t *testing.T) {
	creds := setupTestCredentials(t)
	_, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		LeiosProduceChecker: &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:  &forgerTestLeiosCaster{},
		LeiosMempool:        forgerTestMempoolProvider{},
	})
	require.EqualError(
		t,
		err,
		"production Leios forging requires transaction validator",
	)
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
		LeiosTxValidator:    &mockTxValidator{},
		LeiosMempool: forgerTestMempoolProvider{
			txs: []MempoolTransaction{
				{
					Hash: strings.Repeat("11", 32),
					Cbor: makeMinimalTxCbor(t, 0x11, 0),
					Type: conway.TxTypeConway,
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
	assert.Equal(
		t,
		leiosCaster.hash,
		builder.leiosData.Announcement.Hash.Bytes(),
	)
	assert.Equal(
		t,
		uint64(len(leiosCaster.cbor)),
		builder.leiosData.Announcement.Size,
	)
}

func TestCheckAndForgeProductionCertifiesLeiosEBAfterAdoption(t *testing.T) {
	for _, test := range []struct {
		name        string
		txHashesOK  bool
		canAnnounce bool
	}{
		{name: "closure available", txHashesOK: true, canAnnounce: true},
		{name: "closure unavailable", txHashesOK: false, canAnnounce: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			creds := setupTestCredentials(t)
			block := newForgerTestBlock(10, 2)
			builder := &forgerTestBuilder{block: block, cbor: block.cbor}
			broadcaster := &forgerTestBroadcaster{}
			ebHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x33}, 32))
			rbHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x44}, 32))
			cert := &lcommon.LeiosEbCertificate{
				SlotNo:            9,
				EndorserBlockHash: ebHash,
				Signers:           []byte{0x80},
				AggregatedSignature: make(
					[]byte,
					lcommon.LeiosBlsSignatureSize,
				),
			}
			leiosCerts := &forgerTestLeiosCerts{
				txHashes:   []string{strings.Repeat("11", 32)},
				txHashesOK: test.txHashesOK,
				eligible: []LeiosCertifiedEndorserBlock{
					{
						SlotNo:            9,
						EndorserBlockHash: ebHash,
						Certificate:       cert,
						AnnouncingRbHash:  rbHash,
					},
				},
			}
			parent := &forgerTestLeiosParentAnnouncement{
				rbHash: rbHash, hash: ebHash, ok: true,
			}
			leiosChecker := &forgerTestLeiosChecker{allowed: true}
			leiosCaster := &forgerTestLeiosCaster{}

			forger, err := NewBlockForger(ForgerConfig{
				Mode: ModeProduction,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
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
				LeiosProduceChecker:             leiosChecker,
				LeiosEBBroadcaster:              leiosCaster,
				LeiosTxValidator:                &mockTxValidator{},
				LeiosMempool: forgerTestMempoolProvider{
					txs: []MempoolTransaction{
						{
							Hash: strings.Repeat("11", 32),
							Cbor: makeMinimalTxCbor(t, 0x11, 0),
							Type: conway.TxTypeConway,
						},
						{
							Hash: strings.Repeat("22", 32),
							Cbor: makeMinimalTxCbor(t, 0x22, 0),
							Type: conway.TxTypeConway,
						},
					},
				},
				PromRegistry: prometheus.NewRegistry(),
			})
			require.NoError(t, err)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			require.Equal(t, 1, builder.leiosCalls)
			require.Same(t, cert, builder.leiosData.Certificate)
			require.Equal(t, test.canAnnounce, leiosChecker.calls == 1)
			if test.canAnnounce {
				require.NotNil(t, builder.leiosData.Announcement)
				require.NotEmpty(t, leiosCaster.hash)
				require.Equal(
					t,
					[][]byte{makeMinimalTxCbor(t, 0x22, 0)},
					leiosCaster.txBodies,
				)
			} else {
				require.Nil(t, builder.leiosData.Announcement)
				require.Empty(t, leiosCaster.hash)
			}
			require.Equal(t, []lcommon.Blake2b256{ebHash}, leiosCerts.marked)
			require.Equal(t, 1, parent.calls)
		})
	}
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
	parentRbHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x44}, 32))
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
	wrongContextCert := &lcommon.LeiosEbCertificate{
		SlotNo:              8,
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
				AnnouncingRbHash:  parentRbHash,
			},
			{
				SlotNo:            8,
				EndorserBlockHash: parentHash,
				Certificate:       wrongContextCert,
				AnnouncingRbHash: lcommon.NewBlake2b256(
					bytes.Repeat([]byte{0x55}, 32),
				),
			},
			{
				SlotNo:            9,
				EndorserBlockHash: parentHash,
				Certificate:       parentCert,
				AnnouncingRbHash:  parentRbHash,
			},
		},
	}
	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: parentRbHash, hash: parentHash, ok: true,
	}

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
