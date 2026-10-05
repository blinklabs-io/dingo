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
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestForgeSyncToleranceLargerThanShortTestnetEpoch documents the
// proximate cause of DingoProducesInEachEra/Shelley=0 in the eras
// DevNet: dingo's default forge-sync gate is intentionally generous
// (100 slots) for production-network bootstrap, but in a short-epoch
// testnet (epochLength=75) it is wider than the entire first epoch,
// so the gate never withholds a forge regardless of how far behind
// dingo's local chain is from upstream.
//
// The forge gate in checkAndForgeProduction is:
//
//	if upstreamTip > 0 &&
//	    upstreamTip > tipSlot &&
//	    upstreamTip-tipSlot > forgeSyncToleranceSlots {
//	    skip
//	}
//
// "Cold start, upstream at end of epoch 0": tipSlot=0 (genesis),
// upstreamTip=74 (the last slot before the Allegra fork). The gap is
// 74. With the default tolerance of 100 the gate evaluates 74 > 100 →
// false → forge proceeds. So dingo, having just joined the network
// and barely begun chain-sync, forges its leader slots immediately
// against a stale tipSlot=0 view of the chain. Every such forge
// extends a one-block chain that has to compete with cardano-
// producer's longer chain (cardano started forging immediately at
// genesis). Praos chain selection ranks chains by block count first,
// then lower slot — and dingo's local chain has fewer blocks than
// cardano's at the point chainsync delivers cardano's chain — so
// dingo's bootstrap forges lose.
//
// The companion test below (TestChainSelectionPrefersLongerChainOverLowerSlot)
// pins the chainselection arm of the same mechanism.
func TestForgeSyncToleranceLargerThanShortTestnetEpoch(t *testing.T) {
	// Default tolerance, baked into the package as
	// forgeSyncToleranceSlots = 100.
	const epochLength uint64 = 75 // matches internal/test/erastest/testnet.yaml

	require.Greaterf(
		t,
		uint64(forgeSyncToleranceSlots),
		epochLength,
		"forge-sync tolerance (%d) is larger than the testnet's "+
			"epoch length (%d), so the gate is INACTIVE for the "+
			"entire first epoch even with the most adversarial "+
			"sync state (tipSlot=0, upstreamTip=epochLength-1). "+
			"For short-epoch tests, set DINGO_FORGE_SYNC_TOLERANCE_SLOTS "+
			"to something tighter than epochLength to make dingo "+
			"actually wait for chainsync to catch up before forging "+
			"competing chains during bootstrap.",
		forgeSyncToleranceSlots, epochLength,
	)

	// And document the gate evaluation explicitly: the worst-case
	// in-epoch lag (74) must be < tolerance (100), or the gate
	// would fire. That's the line the bootstrap fork race rides.
	const worstInEpochLag uint64 = 74
	require.LessOrEqualf(
		t,
		worstInEpochLag,
		uint64(forgeSyncToleranceSlots),
		"sanity: worst possible in-epoch lag must fit under the "+
			"tolerance, otherwise the gate would actually fire and "+
			"this whole class of failures would not exist",
	)
}

// TestChainSelectionPrefersLongerChainOverLowerSlot is the chainselection
// half of the Shelley=0 mechanism. Once the forge gate has waved
// dingo's leader slots through during bootstrap (above), the surviving
// blocks are picked by Praos chain selection. Longer chain wins, so a dingo
// local chain whose forge count lags cardano-producer's loses every fork
// resolution regardless of how low its tip slot is.
//
// Concretely: dingo joins late, forges one block at slot 5; chainsync
// then delivers cardano-producer's three-block chain ending at slot 7.
// Dingo's tip is at the lower slot (5 < 7), but cardano's chain is
// longer (3 blocks > 1). Block count wins: dingo rolls back its slot-5
// block and adopts cardano's chain. Repeat across every dingo forge
// that gets caught in this race during bootstrap and you get
// Shelley=0 on the canonical chain.
func TestChainSelectionPrefersLongerChainOverLowerSlot(t *testing.T) {
	dingoLocalTip := ochainsync.Tip{
		BlockNumber: 1,
		Point: ocommon.Point{
			Slot: 5,
			Hash: []byte("dingo-slot-5"),
		},
	}
	cardanoIncomingTip := ochainsync.Tip{
		BlockNumber: 3,
		Point: ocommon.Point{
			Slot: 7,
			Hash: []byte("cardano-slot-7"),
		},
	}

	require.Truef(
		t,
		praos.ComparePraosTips(
			cardanoIncomingTip,
			dingoLocalTip,
			praos.PraosTiebreakerView{},
			praos.PraosTiebreakerView{},
		) == praos.ChainABetter,
		"cardano's longer (block_number=3) chain must beat dingo's "+
			"local (block_number=1) chain even though dingo's tip "+
			"slot is lower (%d < %d).",
		dingoLocalTip.Point.Slot, cardanoIncomingTip.Point.Slot,
	)
	require.Falsef(
		t,
		praos.ComparePraosTips(
			dingoLocalTip,
			cardanoIncomingTip,
			praos.PraosTiebreakerView{},
			praos.PraosTiebreakerView{},
		) == praos.ChainABetter,
		"dingo's local chain at lower slot but fewer blocks must "+
			"NOT win against cardano's longer chain. If it did, "+
			"dingo would entrench a forked solo-extension chain "+
			"against a longer peer chain — even worse than the "+
			"current Shelley=0 outcome.",
	)
}

// corruptBlock mirrors the Conway block envelope but encodes
// transaction_bodies as a CBOR map instead of an array — the structural
// defect behind. Used only to exercise forgedBlockDiagnostics.
type corruptBlock struct {
	cbor.StructAsArray
	Header                 cbor.RawMessage
	TransactionBodies      map[uint]cbor.RawMessage
	TransactionWitnessSets []cbor.RawMessage
	TransactionMetadataSet cbor.RawMessage
	InvalidTransactions    []uint
}

// TestForgedBlockDiagnosticsPinpointsBadBodiesField verifies that the
// diagnostic dump labels the offending block field. When the bodies slot
// holds a map instead of an array, the notation must show
// "transaction_bodies" rendered as a map so the mismatch is obvious.
func TestForgedBlockDiagnosticsPinpointsBadBodiesField(t *testing.T) {
	// A minimal but structurally valid Conway header: [header_body, sig].
	header, err := cbor.Encode([]any{[]any{uint64(0)}, []byte{0x00}})
	require.NoError(t, err)

	bad, err := cbor.Encode(corruptBlock{
		Header:                 cbor.RawMessage(header),
		TransactionBodies:      map[uint]cbor.RawMessage{0: {0xa0}},
		TransactionWitnessSets: []cbor.RawMessage{{0xa0}},
		TransactionMetadataSet: cbor.RawMessage{0xa0},
		InvalidTransactions:    []uint{},
	})
	require.NoError(t, err)

	diag := forgedBlockDiagnostics(bad)
	t.Logf("diagnostics:\n%s", diag)

	assert.Contains(
		t,
		diag,
		"transaction_bodies",
		"diagnostics must label the bodies field",
	)
	// The bodies field should render as a map ("{ ... }"), which is the
	// shape mismatch the decoder rejects.
	idx := strings.Index(diag, "transaction_bodies")
	require.GreaterOrEqual(t, idx, 0)
	rest := diag[idx:]
	assert.True(
		t,
		strings.HasPrefix(
			strings.TrimSpace(rest[len("transaction_bodies:"):]),
			"{",
		),
		"transaction_bodies should render as a map in the diagnostics",
	)
}

// TestForgedBlockDiagnosticsNeverPanics ensures the helper degrades
// gracefully (returns a marker, never panics or errors) on garbage input.
func TestForgedBlockDiagnosticsNeverPanics(t *testing.T) {
	for _, in := range [][]byte{
		nil,
		{},
		{0xff, 0xff, 0xff}, // malformed
		{0x01},             // a bare uint, not a block
		[]byte("not cbor at all..."),
	} {
		out := forgedBlockDiagnostics(in)
		assert.NotEmpty(t, out, "should always return some diagnostic text")
	}
}

// TestForgedBlockDiagnosticsLabelsValidBlock confirms a well-formed forged
// block dumps with the bodies field as an array (the correct shape).
func TestForgedBlockDiagnosticsLabelsValidBlock(t *testing.T) {
	creds := setupTestCredentials(t)
	pp := &mockPParamsProvider{pparams: &conway.ConwayProtocolParameters{
		MaxTxSize:        1 << 20,
		MaxBlockBodySize: 1 << 22,
		MaxBlockExUnits:  lcommon.ExUnits{Memory: 1 << 40, Steps: 1 << 40},
	}}
	ct := &mockChainTip{tip: ochainsync.Tip{
		Point:       ocommon.Point{Slot: 1000, Hash: make([]byte, 32)},
		BlockNumber: 100,
	}}
	en := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         &mockMempool{},
		PParamsProvider: pp,
		ChainTip:        ct,
		EpochNonce:      en,
		Credentials:     creds,
	})
	require.NoError(t, err)
	_, blockCbor, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)

	diag := forgedBlockDiagnostics(blockCbor)
	assert.Contains(t, diag, "header")
	assert.Contains(t, diag, "transaction_bodies")
	idx := strings.Index(diag, "transaction_bodies")
	require.GreaterOrEqual(t, idx, 0)
	rest := strings.TrimSpace(diag[idx+len("transaction_bodies:"):])
	assert.True(
		t,
		strings.HasPrefix(rest, "["),
		"a valid block must render transaction_bodies as an array",
	)
}

// fenceTestStore is an in-memory ForgeFenceStore that survives forger
// instances, so a test can model a restart by building a second forger
// over the same store.
type fenceTestStore struct {
	slot     uint64
	present  bool
	loadErr  error
	storeErr error
	loads    int
	stored   []uint64
	onStore  func()
}

func (s *fenceTestStore) LoadLastForgedSlot() (uint64, bool, error) {
	s.loads++
	if s.loadErr != nil {
		return 0, false, s.loadErr
	}
	return s.slot, s.present, nil
}

func (s *fenceTestStore) StoreLastForgedSlot(slot uint64) error {
	if s.onStore != nil {
		s.onStore()
	}
	if s.storeErr != nil {
		return s.storeErr
	}
	s.slot = slot
	s.present = true
	s.stored = append(s.stored, slot)
	return nil
}

// fenceTestBuilder records the order of BuildBlock calls against a shared
// trace so a test can assert that the fence is written before signing.
type fenceTestBuilder struct {
	block  ledger.Block
	cbor   []byte
	calls  int
	onCall func()
}

func (b *fenceTestBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	b.calls++
	if b.onCall != nil {
		b.onCall()
	}
	return b.block, b.cbor, nil
}

func (b *fenceTestBuilder) BuildBlockWithLeios(
	uint64,
	uint64,
	LeiosBlockData,
) (ledger.Block, []byte, error) {
	return b.BuildBlock(0, 0)
}

// newFenceTestForger builds a production forger wired to store, with the
// slot clock reporting currentSlot over a chain tip one slot behind. The
// tip guard therefore never fires, so a refusal to forge can only come
// from the fence.
func newFenceTestForger(
	t *testing.T,
	store ForgeFenceStore,
	currentSlot uint64,
	builder BlockBuilder,
	broadcaster BlockBroadcaster,
	observed *[]string,
) (*BlockForger, error) {
	t.Helper()
	return NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		ForgeFence:       store,
		BlockForged: func(ledger.Block, []byte, time.Duration) {
			if observed != nil {
				*observed = append(*observed, "observe")
			}
		},
		SlotClock: forgerTestSlotClock{
			currentSlot:       currentSlot,
			chainTipSlot:      currentSlot - 1,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
}

// TestForgeFencePersistsBeforeSigning proves the fence is durable before
// the header is signed. A crash in the window between signing and
// adoption must not leave the slot unrecorded, or a restart could sign a
// second, different block for it.
func TestForgeFencePersistsBeforeSigning(t *testing.T) {
	var callOrder []string
	store := &fenceTestStore{
		onStore: func() { callOrder = append(callOrder, "fence") },
	}
	builder := &fenceTestBuilder{
		block:  newForgerTestBlock(10, 2),
		cbor:   []byte{0x83, 0xaa, 0xbb},
		onCall: func() { callOrder = append(callOrder, "build") },
	}
	forger, err := newFenceTestForger(
		t, store, 10, builder, &forgerTestBroadcaster{}, nil,
	)
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	assert.Equal(t, []string{"fence", "build"}, callOrder)
	assert.Equal(t, []uint64{10}, store.stored)
}

// TestForgeFenceRejectsDuplicateSlotAfterRestart covers the restart
// sequence: a fresh forger reloads the persisted fence and refuses a slot
// it has already committed to, even though the chain tip is behind that
// slot and the tip guard would allow forging.
func TestForgeFenceRejectsDuplicateSlotAfterRestart(t *testing.T) {
	store := &fenceTestStore{slot: 10, present: true}
	builder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xaa, 0xbb},
	}
	broadcaster := &forgerTestBroadcaster{}
	var observed []string

	forger, err := newFenceTestForger(
		t, store, 10, builder, broadcaster, &observed,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, store.loads)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	assert.Equal(t, 0, builder.calls, "must not sign a fenced slot")
	assert.Equal(t, 0, broadcaster.calls)
	assert.Empty(t, observed)
	assert.Empty(t, store.stored)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeFenceBlocked),
	)
}

// TestForgeFenceBlocksReforgeAfterFailedAddBlock is the replay sequence
// the fence exists for: adoption fails, the node restarts, and the slot
// clock hands the same slot back. The first block may already have
// reached peers, so signing a second one for that slot would equivocate.
func TestForgeFenceBlocksReforgeAfterFailedAddBlock(t *testing.T) {
	store := &fenceTestStore{}
	firstBuilder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xaa, 0xbb},
	}
	first, err := newFenceTestForger(
		t,
		store,
		10,
		firstBuilder,
		&forgerTestBroadcaster{err: errors.New("not adopted")},
		nil,
	)
	require.NoError(t, err)

	err = first.checkAndForgeProduction(context.Background())
	require.ErrorContains(t, err, "failed to add block")
	require.Equal(t, 1, firstBuilder.calls)
	require.Equal(t, []uint64{10}, store.stored)

	// Restart: a new forger over the same durable fence.
	secondBuilder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xcc, 0xdd},
	}
	secondBroadcaster := &forgerTestBroadcaster{}
	second, err := newFenceTestForger(
		t, store, 10, secondBuilder, secondBroadcaster, nil,
	)
	require.NoError(t, err)

	require.NoError(t, second.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 0, secondBuilder.calls)
	assert.Equal(t, 0, secondBroadcaster.calls)
	assert.Equal(t, []uint64{10}, store.stored)
}

// TestForgeFenceRejectsSlotBelowFence covers a backwards slot-clock jump:
// the fence is keyed on the highest slot used, not just the last one.
func TestForgeFenceRejectsSlotBelowFence(t *testing.T) {
	store := &fenceTestStore{slot: 20, present: true}
	builder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xaa, 0xbb},
	}
	forger, err := newFenceTestForger(
		t, store, 10, builder, &forgerTestBroadcaster{}, nil,
	)
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 0, builder.calls)
	assert.Empty(t, store.stored)
}

// TestForgeFenceAllowsSlotAboveFence keeps the fence from blocking normal
// forging: a slot above the recorded fence proceeds and advances it.
func TestForgeFenceAllowsSlotAboveFence(t *testing.T) {
	store := &fenceTestStore{slot: 9, present: true}
	builder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xaa, 0xbb},
	}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := newFenceTestForger(
		t, store, 10, builder, broadcaster, nil,
	)
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, []uint64{10}, store.stored)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeFenceBlocked),
	)
}

// TestForgeFenceWriteFailureAbortsForge fails closed: a fence that cannot
// be persisted offers no protection, so the block must not be signed.
func TestForgeFenceWriteFailureAbortsForge(t *testing.T) {
	store := &fenceTestStore{storeErr: errors.New("disk full")}
	builder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xaa, 0xbb},
	}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := newFenceTestForger(
		t, store, 10, builder, broadcaster, nil,
	)
	require.NoError(t, err)

	err = forger.checkAndForgeProduction(context.Background())
	require.ErrorContains(t, err, "disk full")
	assert.Equal(t, 0, builder.calls, "must not sign without a fence")
	assert.Equal(t, 0, broadcaster.calls)
}

// TestNewBlockForgerFailsWhenFenceUnreadable fails closed at wiring time
// rather than starting a producer with no duplicate-slot protection.
func TestNewBlockForgerFailsWhenFenceUnreadable(t *testing.T) {
	store := &fenceTestStore{loadErr: errors.New("metadata unavailable")}
	_, err := newFenceTestForger(
		t,
		store,
		10,
		&fenceTestBuilder{block: newForgerTestBlock(10, 2)},
		&forgerTestBroadcaster{},
		nil,
	)
	require.ErrorContains(t, err, "metadata unavailable")
}

// TestForgeFenceAbsentPreservesForging keeps a nil fence store working
// for embedders and dev-mode wiring that have no metadata store.
func TestForgeFenceAbsentPreservesForging(t *testing.T) {
	builder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xaa, 0xbb},
	}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := newFenceTestForger(
		t, nil, 10, builder, broadcaster, nil,
	)
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
}

// TestForgeFenceBlocksSameSlotRetryWithoutStore covers the in-process
// half of the fence. The slot-aligned loop can re-enter the same slot
// after a failed forge (a clock that has not advanced, or a NextSlotTime
// already in the past), and a second block built from a changed mempool
// would be a different block for a slot already signed for.
func TestForgeFenceBlocksSameSlotRetryWithoutStore(t *testing.T) {
	builder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xaa, 0xbb},
	}
	broadcaster := &forgerTestBroadcaster{err: errors.New("not adopted")}
	forger, err := newFenceTestForger(
		t, nil, 10, builder, broadcaster, nil,
	)
	require.NoError(t, err)

	err = forger.checkAndForgeProduction(context.Background())
	require.ErrorContains(t, err, "failed to add block")
	require.Equal(t, 1, builder.calls)

	// Same slot again: refused without building a second block.
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeFenceBlocked),
	)
}

const altTestRivalBlockNumber = uint64(42)

func altTestParentPoint() ocommon.Point {
	return ocommon.NewPoint(9, []byte("fork-point-hash"))
}

func altTestRivalTip(slot uint64) ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(slot, []byte("rival-block-hash")),
		BlockNumber: altTestRivalBlockNumber,
	}
}

// forgerTestChainContext implements AlternativeChainContextProvider over a
// fixed answer, standing in for chain.Chain.
type forgerTestChainContext struct {
	parent ocommon.Point
	tip    ochainsync.Tip
	ok     bool
	calls  int
}

func newAltTestChainContext(tipSlot uint64) *forgerTestChainContext {
	return &forgerTestChainContext{
		parent: altTestParentPoint(),
		tip:    altTestRivalTip(tipSlot),
		ok:     true,
	}
}

func (c *forgerTestChainContext) TipPredecessor() (
	ocommon.Point,
	ochainsync.Tip,
	bool,
) {
	c.calls++
	return c.parent, c.tip, c.ok
}

// forgerTestSiblingAdopter implements SiblingBlockAdopter with a fixed
// chain-selection outcome.
type forgerTestSiblingAdopter struct {
	adopted bool
	err     error
	panics  bool
	calls   int
	block   ledger.Block
}

func (a *forgerTestSiblingAdopter) AdoptLocalForgedSibling(
	block ledger.Block,
) (bool, error) {
	a.calls++
	a.block = block
	if a.panics {
		panic("sibling adopter panic")
	}
	return a.adopted, a.err
}

// newAlternativeTestForger is newGateTestForger plus the two optional
// providers that enable equal-slot alternative forging, and an observer count
// so a test can assert that a losing alternative is never published.
func newAlternativeTestForger(
	t *testing.T,
	clock forgerTestSlotClock,
	leader LeaderChecker,
	builder BlockBuilder,
	broadcaster BlockBroadcaster,
	fence ForgeFenceStore,
	chainContext AlternativeChainContextProvider,
	adopter SiblingBlockAdopter,
) *BlockForger {
	t.Helper()
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		ForgeFence:       fence,
		SlotClock:        clock,
		ChainContext:     chainContext,
		SiblingAdopter:   adopter,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

// TestEqualSlotAlternativeLosingChainSelectionIsNotPublished pins the losing
// half of a slot battle. Chain selection kept the block already at the tip, so
// our block was never adopted and must not be diffused, must not clear its
// transactions from the mempool, and must not count as adopted.
//
// The fence, by contrast, must stay burned: the block was signed, and a second
// signature for the same slot is equivocation whether or not the first one won.
func TestEqualSlotAlternativeLosingChainSelectionIsNotPublished(
	t *testing.T,
) {
	leader := &forgerCountingLeader{}
	builder := &forgerTestBuilder{
		block: newForgerTestBlock(10, altTestRivalBlockNumber),
		cbor:  []byte{0x01},
	}
	broadcaster := &forgerTestBroadcaster{}
	adopter := &forgerTestSiblingAdopter{adopted: false}
	fence := &fenceTestStore{}
	published := 0

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		ForgeFence:       fence,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      10,
			slotsPerKESPeriod: 100,
		},
		ChainContext:   newAltTestChainContext(10),
		SiblingAdopter: adopter,
		BlockForged: func(ledger.Block, []byte, time.Duration) {
			published++
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	assert.Equal(t, 1, builder.contextCalls, "the alternative is still built")
	assert.Equal(t, 1, adopter.calls)
	assert.Zero(t, published, "a losing alternative must not be published")
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeForged),
		"the block was forged, which is what forgeForged counts",
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeAdopted),
		"a losing alternative was not adopted",
	)
	assert.Equal(
		t,
		[]uint64{10},
		fence.stored,
		"the slot stays burned: the block was signed either way",
	)
}

// TestEqualSlotAlternativeReservesTheFenceBeforeSigning pins the ordering the
// fence depends on. The slot must be recorded durably before the builder
// is asked for a block, so a crash between signing and adoption still leaves
// the slot unusable.
func TestEqualSlotAlternativeReservesTheFenceBeforeSigning(t *testing.T) {
	fence := &fenceTestStore{}
	builder := &forgerTestBuilder{
		block: newForgerTestBlock(10, altTestRivalBlockNumber),
		cbor:  []byte{0x01},
	}
	// Observed at the moment the builder runs.
	var fenceAtBuild []uint64
	builder.onBuild = func() { fenceAtBuild = append([]uint64(nil), fence.stored...) }

	forger := newAlternativeTestForger(
		t,
		forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      10,
			slotsPerKESPeriod: 100,
		},
		&forgerCountingLeader{},
		builder,
		&forgerTestBroadcaster{},
		fence,
		newAltTestChainContext(10),
		&forgerTestSiblingAdopter{adopted: true},
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 1, builder.contextCalls)
	assert.Equal(
		t,
		[]uint64{10},
		fenceAtBuild,
		"the fence must be reserved before the block is signed",
	)
}

// TestEqualSlotAlternativeRefusesASlotWeAlreadyForged is the anti-equivocation
// companion with the alternative path fully wired. Our own block at the tip,
// or a fence already at the slot, must not produce a second signature for it.
func TestEqualSlotAlternativeRefusesASlotWeAlreadyForged(t *testing.T) {
	leader := &forgerCountingLeader{}
	builder := &forgerTestBuilder{
		block: newForgerTestBlock(10, altTestRivalBlockNumber),
		cbor:  []byte{0x01},
	}
	adopter := &forgerTestSiblingAdopter{adopted: true}
	fence := &fenceTestStore{slot: 10, present: true}
	forger := newAlternativeTestForger(
		t,
		forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      10,
			slotsPerKESPeriod: 100,
		},
		leader,
		builder,
		&forgerTestBroadcaster{},
		fence,
		newAltTestChainContext(10),
		adopter,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	assert.Zero(t, builder.contextCalls, "must not forge a second block")
	assert.Zero(t, builder.calls)
	assert.Zero(t, adopter.calls)
	assert.Empty(t, fence.stored, "must not advance the fence")
	assert.Zero(
		t,
		leader.callCount(),
		"our own block at our own slot needs no leader selection",
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"our own block is not a rival",
	)
}

// TestCheckAndForgeProductionEqualSlotDeclinesWithoutTheAlternativePath keeps
// the pre-alternative behaviour observable for any node that is not wired for
// it: the contested slot still reaches leader selection, is still counted as a
// leader slot, a slot battle and a could-not-forge, and is still logged rather
// than dropped in silence. Nothing is built and nothing is signed.
func TestCheckAndForgeProductionEqualSlotDeclinesWithoutTheAlternativePath(
	t *testing.T,
) {
	tests := []struct {
		name         string
		chainContext AlternativeChainContextProvider
		adopter      SiblingBlockAdopter
	}{
		{name: "nothing wired"},
		{
			name:         "no sibling adopter",
			chainContext: newAltTestChainContext(10),
		},
		{
			name:    "no chain context",
			adopter: &forgerTestSiblingAdopter{adopted: true},
		},
		{
			name: "tip has no resolvable predecessor",
			chainContext: &forgerTestChainContext{
				ok: false,
			},
			adopter: &forgerTestSiblingAdopter{adopted: true},
		},
		{
			name:         "tip is no longer at the contested slot",
			chainContext: newAltTestChainContext(9),
			adopter:      &forgerTestSiblingAdopter{adopted: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leader := &forgerCountingLeader{}
			builder := &forgerTestBuilder{}
			broadcaster := &forgerTestBroadcaster{}
			fence := &fenceTestStore{}
			forger := newAlternativeTestForger(
				t,
				forgerTestSlotClock{
					currentSlot:       10,
					chainTipSlot:      10,
					slotsPerKESPeriod: 100,
				},
				leader,
				builder,
				broadcaster,
				fence,
				tt.chainContext,
				tt.adopter,
			)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			assert.Equal(t, 1, leader.callCount())
			assert.Equal(
				t,
				float64(1),
				testutil.ToFloat64(forger.metrics.slotBattlesTotal),
			)
			assert.Equal(
				t,
				float64(1),
				testutil.ToFloat64(forger.metrics.forgeCouldNot),
			)
			assert.Zero(t, builder.calls)
			assert.Zero(t, builder.contextCalls)
			assert.Zero(t, broadcaster.calls)
			assert.Empty(
				t,
				fence.stored,
				"a declined battle must not burn the slot",
			)
		})
	}
}

// TestEqualSlotAlternativeRequiresADurableFence pins the anti-equivocation
// precondition for contesting a slot at all. This path cannot tell a rival's
// block from one of our own: it reads "the tip is at my slot" and forges an
// alternative to whatever is there.
//
// Without a durable fence store, lastForgedSlot is in-memory only and
// fenceLoaded is false after a restart, so the "slot already has our own
// block" gate cannot fire. A producer restarted inside a slot it has already
// forged for would therefore find its OWN block at the tip, treat it as a
// rival, sign a second different block for that slot -- equivocation, against
// a first block that may already have reached peers -- and roll its own good
// block off the tip to adopt it.
//
// Everything wired here is wired correctly: chain context, sibling adopter,
// context-capable builder, a tip genuinely at the contested slot. Only the
// fence is missing, and that alone must decline the contest. The node
// binary refuses to start a producer without a fence store at all
// (node_forging.go), so this guards embedders and dev-mode wiring, which the
// forging package still admits.
func TestEqualSlotAlternativeRequiresADurableFence(t *testing.T) {
	leader := &forgerCountingLeader{}
	builder := &forgerTestBuilder{
		block: newForgerTestBlock(10, altTestRivalBlockNumber),
		cbor:  []byte{0x01},
	}
	broadcaster := &forgerTestBroadcaster{}
	adopter := &forgerTestSiblingAdopter{adopted: true}
	published := 0

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		// No ForgeFence: the case this test exists for.
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      10,
			slotsPerKESPeriod: 100,
		},
		ChainContext:   newAltTestChainContext(10),
		SiblingAdopter: adopter,
		BlockForged: func(ledger.Block, []byte, time.Duration) {
			published++
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	// The slot is still observable as a contested one that was declined,
	// exactly as any other missing part of the capability is.
	assert.Equal(t, 1, leader.callCount())
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
	// Nothing signed, nothing adopted, nothing diffused.
	assert.Zero(t, builder.contextCalls, "no alternative may be built")
	assert.Zero(t, builder.calls)
	assert.Zero(t, builder.leiosCalls)
	assert.Zero(t, adopter.calls, "nothing may be offered to chain selection")
	assert.Zero(t, broadcaster.calls)
	assert.Zero(t, published)
}

// TestEqualSlotAlternativeRecoversAdopterPanic keeps a misbehaving adopter
// from taking down the forge loop goroutine, matching addBlockSafe.
func TestEqualSlotAlternativeRecoversAdopterPanic(t *testing.T) {
	builder := &forgerTestBuilder{
		block: newForgerTestBlock(10, altTestRivalBlockNumber),
		cbor:  []byte{0x01},
	}
	forger := newAlternativeTestForger(
		t,
		forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      10,
			slotsPerKESPeriod: 100,
		},
		&forgerCountingLeader{},
		builder,
		&forgerTestBroadcaster{},
		&fenceTestStore{},
		newAltTestChainContext(10),
		&forgerTestSiblingAdopter{panics: true},
	)

	err := forger.checkAndForgeProduction(context.Background())
	require.ErrorContains(t, err, "sibling block adopter panic")
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

// TestEqualSlotAlternativeCarriesNoLeiosData pins a deliberate limitation. An
// alternative is built on the contested block's predecessor, but
// ParentLeiosAnnouncement resolves "the parent" from the live tip -- the
// rival. A certificate chosen from that answer would certify an endorser
// block announced by a ranking block the alternative is not built on, so the
// alternative is forged as a plain ranking block: the parent-announcement
// provider is not consulted at all, and no new endorser block is announced
// either.
func TestEqualSlotAlternativeCarriesNoLeiosData(t *testing.T) {
	ebHash := lcommon.NewBlake2b256(
		[]byte(strings.Repeat("e", 32)),
	)
	rbHash := lcommon.NewBlake2b256(
		[]byte(strings.Repeat("r", 32)),
	)
	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: rbHash,
		hash:   ebHash,
		ok:     true,
	}
	leiosCerts := &forgerTestLeiosCerts{
		eligible: []LeiosCertifiedEndorserBlock{
			{
				SlotNo:            9,
				EndorserBlockHash: ebHash,
				Certificate: &lcommon.LeiosEbCertificate{
					EndorserBlockHash: ebHash,
				},
				AnnouncingRbHash: rbHash,
			},
		},
		txHashesOK: true,
	}
	builder := &forgerTestBuilder{
		block: newForgerTestBlock(10, altTestRivalBlockNumber),
		cbor:  []byte{0x01},
	}
	adopter := &forgerTestSiblingAdopter{adopted: true}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    &forgerCountingLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      10,
			slotsPerKESPeriod: 100,
		},
		ForgeFence:                      &fenceTestStore{},
		ChainContext:                    newAltTestChainContext(10),
		SiblingAdopter:                  adopter,
		LeiosCertificateProvider:        leiosCerts,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.contextCalls)
	assert.Zero(
		t,
		parent.calls,
		"the parent announcement provider must not be consulted for an "+
			"alternative: it answers for the tip, not for our parent",
	)
	assert.Nil(t, builder.leiosData.Certificate)
	assert.Nil(t, builder.leiosData.Announcement)
	assert.True(t, builder.leiosData.empty())
}

// fallbackTestBuilder implements the package-private constrained builder
// path the production forger uses, so a test can distinguish an attempt
// that was allowed to carry transactions from the empty-body fallback.
type fallbackTestBuilder struct {
	block      ledger.Block
	cbor       []byte
	calls      int
	emptyCalls int
	// selectErr is returned for every attempt allowed to carry
	// transactions, simulating a ledger publication landing during each
	// selection pass.
	selectErr error
	// emptyErr, when set, also fails the empty-body attempt.
	emptyErr error
}

func (b *fallbackTestBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	b.calls++
	return nil, nil, b.selectErr
}

func (b *fallbackTestBuilder) buildBlockWithCredentialGeneration(
	_ uint64,
	_ uint64,
	_ LeiosBlockData,
	_ *credentialGeneration,
	constraints blockSelectionConstraints,
	_ *BlockContext,
) (ledger.Block, []byte, error) {
	b.calls++
	if constraints.emptyBody {
		b.emptyCalls++
		if b.emptyErr != nil {
			return nil, nil, b.emptyErr
		}
		return b.block, b.cbor, nil
	}
	return nil, nil, b.selectErr
}

var _ credentialGenerationBlockBuilder = (*fallbackTestBuilder)(nil)

// TestForgeFallsBackToEmptyBlockWhenSelectionCannotComplete is the second
// half of the lost-slot fix: when no selection pass can complete against a
// stable snapshot before the slot runs out, forge a transaction-free block
// rather than nothing. A pool's reward for a slot does not depend on what
// the block carries, so an empty block is worth the whole slot.
func TestForgeFallsBackToEmptyBlockWhenSelectionCannotComplete(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &fallbackTestBuilder{
		block:     block,
		cbor:      block.cbor,
		selectErr: errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		// No slot time left, so the retry path is exhausted immediately
		// and only the fallback can save the slot.
		slotEnd: time.Now(),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 1, builder.emptyCalls)
	require.Equal(t, 1, broadcaster.calls, "the empty block must be adopted")
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
		"a slot kept by the empty fallback is not a could-not-forge",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("empty"),
		),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("lost"),
		),
	)
}

// TestForgeReportsLostSlotWhenEmptyFallbackAlsoFails keeps
// Forge_could_not_forge_int meaning what it always meant: the slot really
// produced nothing.
func TestForgeReportsLostSlotWhenEmptyFallbackAlsoFails(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &fallbackTestBuilder{
		block:     block,
		cbor:      block.cbor,
		selectErr: errTxValidationSnapshotChanged,
		emptyErr:  errors.New("VRF verification key not loaded"),
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now(),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	err := forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, errTxValidationSnapshotChanged)
	require.Equal(t, 1, builder.emptyCalls)
	require.Equal(t, 0, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("lost"),
		),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("empty"),
		),
	)
}

// TestForgeSkipsEmptyFallbackForNonSelectionFailures keeps the fallback
// scoped to the defect it exists for. A build that failed for an unrelated
// reason gets no second attempt.
func TestForgeSkipsEmptyFallbackForNonSelectionFailures(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &fallbackTestBuilder{
		block:     block,
		cbor:      block.cbor,
		selectErr: errors.New("epoch nonce not available"),
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(time.Hour),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.Error(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 0, builder.emptyCalls)
	require.Equal(t, 1, builder.calls)
}

// TestBuildBlockEmptyBodyConstraintDropsAllTransactions covers the builder
// half: the empty-body constraint must produce a block with no
// transactions and must not open a validation session at all, so a ledger
// publication cannot reject a candidate that contains nothing to validate.
func TestBuildBlockEmptyBodyConstraintDropsAllTransactions(t *testing.T) {
	validator := &sessionMockTxValidator{alwaysStale: true}
	builder := newSelectionTestBuilder(
		t,
		threeTxMempoolForSelection(t),
		selectionTestChainTip(),
		validator,
	)

	generation := builder.creds.acquireCredentialGeneration()
	defer generation.release()
	block, _, err := builder.buildBlockWithCredentialGeneration(
		1001,
		0,
		LeiosBlockData{},
		generation,
		blockSelectionConstraints{emptyBody: true},
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, block.Transactions())
	require.Zero(
		t,
		validator.sessions,
		"a transaction-free candidate has no snapshot to pin",
	)
	require.Zero(t, validator.validateCalls)
}

// TestBuildBlockWithNoCandidatesSkipsValidationSession pins the same
// property for a producer whose mempool is simply empty: before this, an
// unrelated ledger publication could reject a block that carried no
// transactions and cost the slot for nothing.
func TestBuildBlockWithNoCandidatesSkipsValidationSession(t *testing.T) {
	validator := &sessionMockTxValidator{alwaysStale: true}
	builder := newSelectionTestBuilder(
		t,
		&mockMempool{transactions: []MempoolTransaction{}},
		selectionTestChainTip(),
		validator,
	)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Empty(t, block.Transactions())
	require.Zero(t, validator.sessions)
}

// TestBuildBlockEmptyBodySkipsMempoolSnapshot keeps the fallback cheap.
// Taking a mempool snapshot is not free -- on a large or DAG-backed
// mempool that single call can consume what is left of the slot -- and the
// fallback exists precisely because the slot budget has already run out.
// A build that is going to carry no transactions must not ask for them.
func TestBuildBlockEmptyBodySkipsMempoolSnapshot(t *testing.T) {
	mempool := threeTxMempoolForSelection(t)
	builder := newSelectionTestBuilder(
		t,
		mempool,
		selectionTestChainTip(),
		&sessionMockTxValidator{},
	)

	generation := builder.creds.acquireCredentialGeneration()
	defer generation.release()
	block, _, err := builder.buildBlockWithCredentialGeneration(
		1001,
		0,
		LeiosBlockData{},
		generation,
		blockSelectionConstraints{emptyBody: true},
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, block.Transactions())
	require.Zero(
		t,
		mempool.calls,
		"the empty-body fallback must not snapshot the mempool",
	)
}

// TestBuildBlockSnapshotsMempoolForNormalBuild is the negative case: an
// ordinary build still reads the mempool exactly once.
func TestBuildBlockSnapshotsMempoolForNormalBuild(t *testing.T) {
	mempool := threeTxMempoolForSelection(t)
	builder := newSelectionTestBuilder(
		t,
		mempool,
		selectionTestChainTip(),
		&sessionMockTxValidator{},
	)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 3)
	require.Equal(t, 1, mempool.calls)
}

// TestForgeReportsLostSlotWhenTheBuilderCannotDropTransactions pins the
// compatibility promise the empty-body constraint makes to embedders. Only
// the package-private builder path carries per-attempt constraints, so an
// embedder-supplied BlockBuilder cannot be told to drop its transactions.
//
// Falling through to the plain BuildBlock entrypoint there would forge from
// a full mempool under a constraint that was silently discarded -- the
// fallback would stop being a fallback and become an ordinary build with a
// misleading name, at the one moment the slot has no time left for it. The
// slot is reported lost instead, and the reason is inspectable rather than
// hidden behind the selection abort that led to it.
func TestForgeReportsLostSlotWhenTheBuilderCannotDropTransactions(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{
		block:     block,
		cbor:      block.cbor,
		failCount: 1,
		err:       errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		// No slot time left, so the first abort goes straight to the
		// fallback rather than to a retry the builder would satisfy.
		slotEnd: time.Now(),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	err := forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorIs(
		t,
		err,
		errBlockConstraintsUnsupported,
		"the lost slot must name the constraint the builder could not honour",
	)
	require.ErrorIs(
		t,
		err,
		errTxValidationSnapshotChanged,
		"and must still carry the selection abort that reached the fallback",
	)
	require.Equal(
		t,
		1,
		builder.calls,
		"the fallback must not re-enter a builder that cannot drop "+
			"transactions: a second call would forge a full block",
	)
	require.Equal(t, 0, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("lost"),
		),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("empty"),
		),
	)
}

// newGateTestForger builds a production forger over an explicit slot
// clock so a test can place the chain tip ahead of, at, or behind the
// current slot and observe which pre-leader gate in
// checkAndForgeProduction fires.
func newGateTestForger(
	t *testing.T,
	clock forgerTestSlotClock,
	leader LeaderChecker,
	builder BlockBuilder,
	broadcaster BlockBroadcaster,
	fence ForgeFenceStore,
) *BlockForger {
	t.Helper()
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		ForgeFence:       fence,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

// TestCheckAndForgeProductionEqualSlotReachesLeaderCheck pins the EQ
// half of the tip gate. A rival pool forging the same slot and winning
// the propagation race puts the chain tip AT the current slot, not past
// it. ouroboros-consensus treats that as a contested slot rather than a
// reason to stop: mkCurrentBlockContext declines only for GT
// (TraceBlockFromFuture) and, for EQ, forges an alternative to the block
// at the tip -- same block number, the tip's predecessor as parent -- so
// the leader VRF and chain selection arbitrate.
//
// Dingo folded EQ into the GT skip with `currentSlot <= tipSlot`, which
// returns before checkLeaderSafe. The slot never reached leader
// selection, so no leadership counter moved and the only trace was a
// Debug line.
//
// With the alternative path wired, the contested slot must reach leader
// selection, be counted as a leader slot and a slot battle, and produce
// a block built on the tip's predecessor rather than on the tip.
// TestCheckAndForgeProductionEqualSlotDeclinesWithoutTheAlternativePath
// covers the unwired case, which still declines and still accounts for
// the battle.
func TestCheckAndForgeProductionEqualSlotReachesLeaderCheck(t *testing.T) {
	leader := &forgerCountingLeader{}
	builder := &forgerTestBuilder{
		block: newForgerTestBlock(10, altTestRivalBlockNumber),
		cbor:  []byte{0x01},
	}
	broadcaster := &forgerTestBroadcaster{}
	adopter := &forgerTestSiblingAdopter{adopted: true}
	forger := newAlternativeTestForger(
		t,
		forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      10,
			slotsPerKESPeriod: 100,
		},
		leader,
		builder,
		broadcaster,
		&fenceTestStore{},
		newAltTestChainContext(10),
		adopter,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	assert.Equal(
		t,
		1,
		leader.callCount(),
		"a tip at the current slot is a contested slot, not a reason "+
			"to skip leader selection",
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeNodeIsLeader),
		"the contested slot must be counted as a leader slot",
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"a rival block occupying our leader slot is a slot battle",
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
		"a contested slot we did forge for is not a could-not-forge",
	)

	// The block must be built on the tip's PREDECESSOR. Binding the live
	// tip -- the rival's block at this same slot -- would carry a parent
	// whose slot equals its own and be rejected by
	// ledger.validateBlockOrder and by every Praos peer.
	require.Equal(
		t,
		1,
		builder.contextCalls,
		"a contested slot must build on an explicit block context",
	)
	assert.Zero(
		t,
		builder.calls,
		"must not bind a same-slot parent",
	)
	assert.Equal(
		t,
		altTestParentPoint(),
		builder.blockCtx.Parent,
		"the alternative's parent is the tip's predecessor",
	)
	assert.Equal(
		t,
		altTestRivalBlockNumber,
		builder.blockCtx.BlockNumber,
		"the alternative carries the rival's block number, not one past it",
	)
	assert.Equal(
		t,
		altTestRivalTip(10),
		builder.blockCtx.Rival,
		"the alternative names the tip it competes with",
	)

	// It goes to chain selection, not to the extend-only add path.
	assert.Equal(t, 1, adopter.calls)
	assert.Zero(t, broadcaster.calls)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeForged),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeAdopted),
	)
}

// TestCheckAndForgeProductionEqualSlotDoesNotReForgeOurOwnSlot is the
// anti-equivocation companion to the test above. The slot-aligned loop
// can re-enter a slot it already committed to (a clock that has not
// advanced, or a NextSlotTime already in the past), and after a
// successful forge the chain tip is our OWN block at that slot, so
// tipSlot == currentSlot there too.
//
// That case must stay a quiet skip: it is not a slot battle, it must
// not build a second block for a slot this node already signed for, and
// it must not report a could-not-forge for a slot that was in fact
// forged.
func TestCheckAndForgeProductionEqualSlotDoesNotReForgeOurOwnSlot(
	t *testing.T,
) {
	leader := &forgerCountingLeader{}
	builder := &forgerTestBuilder{}
	broadcaster := &forgerTestBroadcaster{}
	// The fence records slot 10 as already committed to by this node,
	// which is what "the block at the tip is ours" means before the
	// block has been adopted.
	fence := &fenceTestStore{slot: 10, present: true}
	forger := newGateTestForger(
		t,
		forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      10,
			slotsPerKESPeriod: 100,
		},
		leader,
		builder,
		broadcaster,
		fence,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	assert.Zero(t, builder.calls, "must not forge a second block")
	assert.Zero(t, broadcaster.calls)
	assert.Empty(t, fence.stored, "must not advance the fence")
	assert.Zero(
		t,
		leader.callCount(),
		"our own block at our own slot needs no leader selection",
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"our own block is not a rival",
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
		"a slot we forged must not report could-not-forge",
	)
}

// TestForgeTakesLeaderSlotWhenUpstreamTargetUnknownAtTip pins the
// unknown-upstream-target branch of the sync gate for a node that is NOT
// behind.
//
// This test replaces TestForgeSkipsLeaderSlotWhenUpstreamTargetUnknownEvenAtTip
// and reverses its assertion. That test asserted the behaviour
// deliberately left in place -- a best-peer switch disabled forging outright,
// independent of local tip freshness and of forgeSyncToleranceSlots -- and
// said in as many words that keying the gate on local tip freshness instead
// would fail it and name the decision being revisited. This is that decision,
// taken because the old behaviour is self-sealing: LedgerState publishes the
// zero target for the window before the new peer's first admitted trusted
// header, and on a network where forging is the only source of headers no node
// forges, so none is admitted, so nothing lifts the target and the window never
// closes.
//
// The clock below is a healthy steady-state producer: the tip is the previous
// slot's block, so the node is at tip and has no evidence it is behind. It is
// scheduled to lead the current slot, and it must now take it -- the header it
// produces is what ends the unknown-target window.
//
// The tolerance cases pin that the branch is compared against the tolerance at
// all, which the `upstreamTip == 0` disjunct never was.
func TestForgeTakesLeaderSlotWhenUpstreamTargetUnknownAtTip(
	t *testing.T,
) {
	for _, tc := range []struct {
		name      string
		tolerance uint64
	}{
		{name: "default tolerance", tolerance: 0},
		{name: "tolerance far wider than the lag", tolerance: 100000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader := &forgerCountingLeader{}
			block := newForgerTestBlock(10, 2)
			builder := &forgerTestBuilder{
				block: block,
				cbor:  block.cbor,
			}
			broadcaster := &forgerTestBroadcaster{}
			forger, err := NewBlockForger(ForgerConfig{
				Mode: ModeProduction,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
				Credentials:      setupTestCredentials(t),
				LeaderChecker:    leader,
				BlockBuilder:     builder,
				BlockBroadcaster: broadcaster,
				SlotClock: forgerTestSlotClock{
					// At tip: the tip is the previous slot's block.
					currentSlot:  10,
					chainTipSlot: 9,
					// A peer switch has just happened, so the
					// corroborated upstream target is not yet known.
					upstreamTipSlot:   0,
					upstreamActive:    true,
					slotsPerKESPeriod: 100,
				},
				ForgeSyncToleranceSlots: tc.tolerance,
				PromRegistry:            prometheus.NewRegistry(),
			})
			require.NoError(t, err)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			assert.Equal(
				t,
				1,
				leader.callCount(),
				"an unknown upstream target is not evidence that this "+
					"node is behind, so the slot must reach leader "+
					"selection",
			)
			assert.Equal(t, 1, builder.calls)
			assert.Equal(t, 1, broadcaster.calls)
			assert.Equal(
				t,
				float64(1),
				testutil.ToFloat64(forger.metrics.forgeNodeIsLeader),
			)
			assert.Equal(
				t,
				float64(0),
				testutil.ToFloat64(forger.metrics.forgeSyncSkip),
				"the sync gate must not claim this slot",
			)
		})
	}
}

// TestForgeAllowsUnknownUpstreamTargetWhileWallClockIsStale verifies that a
// quiet network is not mistaken for an upstream peer being ahead. The target
// is unknown, so the forge gate has no peer-relative evidence that this node
// is behind.
func TestForgeAllowsUnknownUpstreamTargetWhileWallClockIsStale(
	t *testing.T,
) {
	leader := &forgerCountingLeader{}
	block := newForgerTestBlock(1000, 9)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			// The current slot is far beyond the previous block, but the
			// peer has no corroborated target and may also be at that tip.
			currentSlot:       1000,
			chainTipSlot:      9,
			upstreamTipSlot:   0,
			upstreamActive:    true,
			slotsPerKESPeriod: 100000,
		},
		ForgeSyncToleranceSlots: 100,
		PromRegistry:            prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	assert.Equal(t, 1, leader.callCount())
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeSyncSkip),
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipAppliedStale),
	)
	// This once asserted 991, the local tip's lag behind the wall clock,
	// because the sync-skip path was then the only writer that could make
	// dingo_forge_tip_gap_slots non-zero on this branch. The gauge now has
	// a single meaning -- the ledger-apply backlog, primary chain tip
	// minus applied tip -- and sets it once per leader check instead, so the
	// skip paths no longer overwrite it. The primary tip mirrors the applied tip
	// on this fixture, so the backlog is 0 and the gauge says so.
	//
	// The lag itself is not lost: ledger exports it continuously as
	// dingo_tip_gap_slots ("slots between wall-clock slot and chain tip",
	// ledger/state.go), on every slot tick rather than only on a leader-slot
	// skip, and the gate's own log line carries current_slot and tip_slot.
	// The wall-clock refusal is reported separately from the peer-sync counter;
	// TestForgeTakesLeaderSlotWhenUpstreamTargetUnknownAtTip covers the near-tip
	// case that remains eligible.
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.tipGapSlots),
		"dingo_forge_tip_gap_slots reports the ledger-apply backlog, "+
			"not the wall-clock lag",
	)
}

// forgerScheduleAwareLeader reports a fixed set of scheduled leader
// slots through NextLeaderSlot, the way Election answers from its
// precomputed VRF schedule, so a test can distinguish a skipped slot
// this node was due to lead from an ordinary one.
type forgerScheduleAwareLeader struct {
	scheduled map[uint64]struct{}
}

func (l *forgerScheduleAwareLeader) ShouldProduceBlock(slot uint64) bool {
	_, ok := l.scheduled[slot]
	return ok
}

func (l *forgerScheduleAwareLeader) NextLeaderSlot(
	fromSlot uint64,
) (uint64, bool) {
	if _, ok := l.scheduled[fromSlot]; ok {
		return fromSlot, true
	}
	return 0, false
}

// newGateSkipLogForger builds a production forger whose logs are
// captured, over a leader checker that answers NextLeaderSlot from a
// fixed schedule, so a test can read back the level a pre-leader gate
// skip was logged at.
func newGateSkipLogForger(
	t *testing.T,
	clock forgerTestSlotClock,
	scheduled map[uint64]struct{},
) (*BlockForger, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(
			logs,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		)),
		Credentials: setupTestCredentials(t),
		LeaderChecker: &forgerScheduleAwareLeader{
			scheduled: scheduled,
		},
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger, logs
}

// TestForgeGateSkipWarnsOnlyForScheduledLeaderSlots pins the log level
// of both gates that run before leader selection. Such a gate returns
// from checkAndForgeProduction before checkLeaderSafe, so the slot it
// drops moves about_to_lead and nothing else: no Forge_node_is_leader,
// no Forge_node_not_leader, no Forge_could_not_forge. A dropped
// scheduled leader slot is therefore indistinguishable at INFO from "we
// were simply not the leader", which is how a producer can decline its
// own leader slots while every standard SPO dashboard shows it healthy.
//
// A gate skip on an ordinary slot is routine and stays at Debug. A gate
// skip that swallows a slot this node was scheduled to lead is a lost
// block that nothing downstream will ever mention again, so it is
// raised to Warn and marked leader_slot=true.
func TestForgeGateSkipWarnsOnlyForScheduledLeaderSlots(t *testing.T) {
	// The leader slot under test is slot 10 in both gates.
	const leaderSlot = uint64(10)
	for _, gate := range []struct {
		name  string
		clock forgerTestSlotClock
		msg   string
	}{
		{
			// The chain tip has moved past the slot we would
			// produce for.
			name: "tip ahead",
			clock: forgerTestSlotClock{
				currentSlot:       leaderSlot,
				chainTipSlot:      leaderSlot + 1,
				slotsPerKESPeriod: 100,
			},
			msg: "forge skip: chain tip is ahead of the current slot",
		},
		{
			// The corroborated upstream target leads our tip by more
			// than the tolerance, so the node really is behind.
			// A target of zero no longer reaches this gate on a node
			// at tip; see
			// TestForgeTakesLeaderSlotWhenUpstreamTargetUnknownAtTip.
			name: "upstream syncing",
			clock: forgerTestSlotClock{
				currentSlot:       leaderSlot,
				chainTipSlot:      leaderSlot - 1,
				upstreamTipSlot:   leaderSlot + 500,
				upstreamActive:    true,
				slotsPerKESPeriod: 100,
			},
			msg: "chain syncing from peer, skipping forge",
		},
	} {
		t.Run(gate.name, func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				scheduled  map[uint64]struct{}
				wantLevel  string
				wantMarker bool
			}{
				{
					name:      "ordinary slot stays at debug",
					scheduled: map[uint64]struct{}{},
					wantLevel: "DEBUG",
				},
				{
					name: "scheduled leader slot warns",
					scheduled: map[uint64]struct{}{
						leaderSlot: {},
					},
					wantLevel:  "WARN",
					wantMarker: true,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					forger, logs := newGateSkipLogForger(
						t,
						gate.clock,
						tc.scheduled,
					)

					require.NoError(
						t,
						forger.checkAndForgeProduction(
							context.Background(),
						),
					)

					out := logs.String()
					require.Contains(t, out, gate.msg)
					assert.Contains(
						t,
						out,
						`"level":"`+tc.wantLevel+`"`,
					)
					if tc.wantMarker {
						assert.Contains(
							t,
							out,
							`"leader_slot":true`,
						)
					} else {
						assert.NotContains(
							t,
							out,
							`"leader_slot":true`,
						)
					}
				})
			}
		})
	}
}

// TestForgeStaleGapSkipMarksScheduledLeaderSlots covers the sub-branch
// of the tip-ahead gate that the marker would otherwise miss. When the
// tip runs further ahead than forgeStaleGapThresholdSlots the gate
// diagnoses a stale database at Error instead of routing the skip
// through logGateSkip, but it drops the slot just as silently and just
// as far before leader selection. The severity and wording are the
// operator's stale-genesis signal and stay as they are; the slot is
// additionally marked leader_slot=true when it was one this node was
// scheduled to lead, so a block lost this way is still attributable.
func TestForgeStaleGapSkipMarksScheduledLeaderSlots(t *testing.T) {
	const leaderSlot = uint64(10)
	// Well past the default forgeStaleGapThresholdSlots of 1000, so
	// the gate takes the stale-database branch.
	clock := forgerTestSlotClock{
		currentSlot:       leaderSlot,
		chainTipSlot:      leaderSlot + 2000,
		slotsPerKESPeriod: 100,
	}
	const msg = "chain tip is far ahead of slot clock; " +
		"database may contain data from a different genesis"

	for _, tc := range []struct {
		name       string
		scheduled  map[uint64]struct{}
		wantMarker bool
	}{
		{
			name:      "ordinary slot is not marked",
			scheduled: map[uint64]struct{}{},
		},
		{
			name: "scheduled leader slot is marked",
			scheduled: map[uint64]struct{}{
				leaderSlot: {},
			},
			wantMarker: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forger, logs := newGateSkipLogForger(
				t,
				clock,
				tc.scheduled,
			)

			require.NoError(
				t,
				forger.checkAndForgeProduction(
					context.Background(),
				),
			)

			out := logs.String()
			require.Contains(t, out, msg)
			// The stale-genesis diagnosis keeps its severity
			// whether or not the slot was a scheduled one.
			assert.Contains(t, out, `"level":"ERROR"`)
			if tc.wantMarker {
				assert.Contains(
					t,
					out,
					`"leader_slot":true`,
				)
			} else {
				assert.NotContains(
					t,
					out,
					`"leader_slot":true`,
				)
			}
		})
	}
}

// newOwnTipGateForger builds a production forger over an explicit slot
// clock and an optional fence store, and captures its logs at Debug so a
// test can assert on the level a skip was emitted at. Passing a nil
// fence leaves the duplicate-slot fence in-memory and unloaded, which is
// how a wiring with no metadata store behaves.
func newOwnTipGateForger(
	t *testing.T,
	clock SlotClockProvider,
	leader LeaderChecker,
	builder BlockBuilder,
	broadcaster BlockBroadcaster,
	fence ForgeFenceStore,
) (*BlockForger, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(
			logs,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		ForgeFence:       fence,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	require.Equal(
		t,
		fence != nil,
		forger.fenceLoaded,
		"the fence must be loaded exactly when a fence store is wired",
	)
	// Construction itself warns that no fence store is wired. Drop it so
	// the buffer holds only what the forge cycle under test emitted.
	logs.Reset()
	return forger, logs
}

// TestEqualSlotOwnBlockIsIdentifiedByHashNotByFence pins that "the block
// at the tip is ours" is decided by identity, not by the fence
// bookkeeping alone.
//
// The fence is a high-water mark over slots this node committed to, and
// it is only guaranteed to be present when a durable ForgeFenceStore is
// wired: without one it is in-memory (see BlockForger.fenceStore), so
// there are wirings in which the forger holds proof that the block at the
// tip is its own — SlotTracker recorded the hash it forged for that slot
// — while the fence signal says nothing. Keying the quiet skip solely on
// the fence sends that slot to the contested-slot branch, where the node
// records a slot battle at Warn against its own block and reports a
// could-not-forge for a slot it did in fact forge.
//
// Both subtests run with no fence store at all, leaving the hash as the
// only signal, and require the forger to tell its own block from a
// rival's using it.
func TestEqualSlotOwnBlockIsIdentifiedByHashNotByFence(t *testing.T) {
	const slot = uint64(10)
	ourHash := bytes.Repeat([]byte{0xa1}, 32)
	rivalHash := bytes.Repeat([]byte{0xb2}, 32)

	t.Run("our own block at the tip is a quiet skip", func(t *testing.T) {
		leader := &forgerCountingLeader{}
		builder := &forgerTestBuilder{}
		broadcaster := &forgerTestBroadcaster{}
		forger, logs := newOwnTipGateForger(
			t,
			forgerTestSlotClock{
				currentSlot:       slot,
				chainTipSlot:      slot,
				chainTipHash:      ourHash,
				slotsPerKESPeriod: 100,
			},
			leader,
			builder,
			broadcaster,
			nil,
		)
		// We forged this slot and the block was adopted: the tip is
		// byte-for-byte our block.
		forger.slotTracker.RecordForgedBlock(slot, ourHash)

		require.NoError(
			t,
			forger.checkAndForgeProduction(context.Background()),
		)

		assert.Zero(
			t,
			leader.callCount(),
			"our own block at our own slot needs no leader selection",
		)
		assert.Zero(t, builder.calls, "must not forge a second block")
		assert.Zero(t, broadcaster.calls)
		assert.Equal(
			t,
			float64(0),
			testutil.ToFloat64(forger.metrics.slotBattlesTotal),
			"our own block is not a rival, so this is not a slot battle",
		)
		assert.Equal(
			t,
			float64(0),
			testutil.ToFloat64(forger.metrics.forgeCouldNot),
			"a slot we forged must not report could-not-forge",
		)
		assert.NotContains(
			t,
			logs.String(),
			`"level":"WARN"`,
			"re-entering a slot whose tip block is ours is routine and "+
				"must not warn",
		)
		assert.Contains(
			t,
			logs.String(),
			"forge skip: slot already has our own block",
		)
		assert.Contains(
			t,
			logs.String(),
			`"matched_by":"forged_block_hash"`,
			"the skip must be attributed to the hash match, not to a "+
				"fence that was never loaded",
		)
	})

	t.Run(
		"a rival block at the tip is still a slot battle",
		func(t *testing.T) {
			leader := &forgerCountingLeader{}
			builder := &forgerTestBuilder{}
			broadcaster := &forgerTestBroadcaster{}
			forger, logs := newOwnTipGateForger(
				t,
				forgerTestSlotClock{
					currentSlot:       slot,
					chainTipSlot:      slot,
					chainTipHash:      rivalHash,
					slotsPerKESPeriod: 100,
				},
				leader,
				builder,
				broadcaster,
				nil,
			)
			// A tracker record for the slot must not be mistaken for
			// ownership of the block at the tip: we forged for this
			// slot, but a rival's block is what the chain adopted.
			forger.slotTracker.RecordForgedBlock(slot, ourHash)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			assert.Equal(
				t,
				1,
				leader.callCount(),
				"a rival's block at our slot is a contested slot",
			)
			assert.Equal(
				t,
				float64(1),
				testutil.ToFloat64(forger.metrics.slotBattlesTotal),
			)
			assert.Zero(t, builder.calls, "must not bind a same-slot parent")
			assert.Zero(t, broadcaster.calls)
			assert.Contains(
				t,
				logs.String(),
				`"level":"WARN"`,
				"a leader slot lost to a rival must be visible",
			)
		},
	)
}

// forgerMovingTipSlotClock is a slot clock whose chain tip moves between
// the read at the top of a forge cycle and the re-read tipBlockOwnership
// takes. The first ChainTip call answers a point at chainTipSlot, every
// later one a point at movedTipSlot, which is what a rival block landing
// mid-cycle looks like to the forger. Both points carry chainTipHash: the
// slot is what moves.
//
// hashReads counts calls to the optional ChainTipHashProvider, which
// tipBlockOwnership no longer consults -- it takes slot and hash from the
// one ChainTip snapshot. The counter is kept so the test can pin that.
type forgerMovingTipSlotClock struct {
	currentSlot       uint64
	chainTipSlot      uint64
	movedTipSlot      uint64
	chainTipHash      []byte
	slotsPerKESPeriod uint64

	mu        sync.Mutex
	tipReads  int
	hashReads int
}

func (c *forgerMovingTipSlotClock) CurrentSlot() (uint64, error) {
	return c.currentSlot, nil
}

func (c *forgerMovingTipSlotClock) SlotsPerKESPeriod() uint64 {
	return c.slotsPerKESPeriod
}

// ChainTip models a tip that moves between reads: the first read sees the
// original slot and every later one sees the moved slot. Each read returns a
// self-consistent point, as the SlotClockProvider contract requires, so the
// staleness this double injects is between successive reads -- exactly the
// case tipBlockOwnership's re-read exists to catch.
func (c *forgerMovingTipSlotClock) ChainTip() ocommon.Point {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tipReads++
	slot := c.movedTipSlot
	if c.tipReads == 1 {
		slot = c.chainTipSlot
	}
	return ocommon.Point{Slot: slot, Hash: c.chainTipHash}
}

func (c *forgerMovingTipSlotClock) ChainTipSnapshot() ochainsync.Tip {
	return ochainsync.Tip{Point: c.ChainTip()}
}

func (c *forgerMovingTipSlotClock) ForgeTipSnapshot() (ochainsync.Tip, int) {
	return c.ChainTipSnapshot(), 5
}

// PrimaryChainTip is pinned to the ORIGINAL tip and never moves. This double
// exists to model the applied tip shifting between the two reads
// tipBlockOwnership makes; letting the primary tip follow it would put the
// primary tip ahead of the current slot and trip the past-slot guard before
// the contested-slot branch this test is about.
func (c *forgerMovingTipSlotClock) PrimaryChainTip() ocommon.Point {
	c.mu.Lock()
	defer c.mu.Unlock()
	return ocommon.Point{Slot: c.chainTipSlot, Hash: c.chainTipHash}
}

func (c *forgerMovingTipSlotClock) PrimaryChainTipRelation(
	point ocommon.Point,
) (ochainsync.Tip, uint64, bool, error) {
	primary := c.PrimaryChainTip()
	return ochainsync.Tip{Point: primary}, 0,
		primary.Slot == point.Slot && bytes.Equal(primary.Hash, point.Hash), nil
}

func (c *forgerMovingTipSlotClock) ChainTipHash() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hashReads++
	return c.chainTipHash
}

func (*forgerMovingTipSlotClock) NextSlotTime() (time.Time, error) {
	return time.Now(), nil
}

func (*forgerMovingTipSlotClock) UpstreamTipSlot() uint64 {
	return 0
}

func (*forgerMovingTipSlotClock) UpstreamSyncStatus() (uint64, bool) {
	return 0, false
}

func (*forgerMovingTipSlotClock) UpstreamSyncTip() (ochainsync.Tip, bool) {
	return ochainsync.Tip{}, false
}

func (*forgerMovingTipSlotClock) SecurityParam() int { return 5 }

func (c *forgerMovingTipSlotClock) reads() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tipReads, c.hashReads
}

// TestEqualSlotLostBattleUnderTheFenceIsNotSilent covers the case where
// the two ownership signals disagree: the fence says this node already
// committed to the slot, and the recorded forge hash says the block that
// actually won it belongs to someone else.
//
// The fence must still win the forging decision. A slot at or below the
// fence may already have a signed, diffused block behind it, and signing
// a second different block for it is equivocation; losing a slot battle
// is not a licence to equivocate. So: no build, no broadcast, no
// advance of the fence.
//
// What must change is that the loss stops being silent. Reporting it as
// "slot already has our own block" at Debug is both false and exactly
// an invisible leader-slot loss, so the
// declined leader slot is counted as a could-not-forge and logged at
// Warn with both hashes.
//
// slotBattlesTotal stays where it is: LedgerState.checkSlotBattle
// already counted this battle when the rival block was accepted, over
// the same SlotTracker and the same recorder, so the forger counting it
// again would double it.
func TestEqualSlotLostBattleUnderTheFenceIsNotSilent(t *testing.T) {
	const slot = uint64(10)
	ourHash := bytes.Repeat([]byte{0xa1}, 32)
	rivalHash := bytes.Repeat([]byte{0xb2}, 32)

	leader := &forgerCountingLeader{}
	builder := &forgerTestBuilder{}
	broadcaster := &forgerTestBroadcaster{}
	fence := &fenceTestStore{slot: slot, present: true}
	forger, logs := newOwnTipGateForger(
		t,
		forgerTestSlotClock{
			currentSlot:       slot,
			chainTipSlot:      slot,
			chainTipHash:      rivalHash,
			slotsPerKESPeriod: 100,
		},
		leader,
		builder,
		broadcaster,
		fence,
	)
	// We forged for this slot, but the chain adopted a rival's block.
	forger.slotTracker.RecordForgedBlock(slot, ourHash)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	// The fence still refuses the slot.
	assert.Zero(t, builder.calls, "the fence must still forbid a second block")
	assert.Zero(t, broadcaster.calls)
	assert.Empty(t, fence.stored, "must not advance the fence")
	assert.Zero(
		t,
		leader.callCount(),
		"a slot the fence has already spent needs no leader selection",
	)

	// The battle itself is counted by LedgerState.checkSlotBattle, not
	// here. Reaching this case means a block other than ours was
	// accepted at a slot SlotTracker says we forged, and the only path
	// that accepts a peer's block already ran checkSlotBattle over the
	// same tracker and the same recorder — the node wiring points
	// ForgedBlockChecker at this forger's SlotTracker and
	// SlotBattleRecorder at the forger itself. Incrementing again here
	// would double the count, so the forger must leave it alone.
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"chainsync owns the slot-battle count for a rival block that "+
			"was accepted at a slot we forged; the forger must not "+
			"count it a second time",
	)

	// The declined leader slot is still visible, which is the point.
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
		"a leader slot we did not hold must be visible as a "+
			"could-not-forge, not as silence",
	)
	logged := logs.String()
	assert.Contains(t, logged, `"level":"WARN"`)
	assert.Contains(
		t,
		logged,
		"slot battle lost: rival block at tip for a slot this node "+
			"already forged",
	)
	assert.Contains(
		t,
		logged,
		hex.EncodeToString(ourHash),
		"the log must name the block we forged",
	)
	assert.Contains(
		t,
		logged,
		hex.EncodeToString(rivalHash),
		"the log must name the block that won the slot",
	)
	assert.NotContains(
		t,
		logged,
		"forge skip: slot already has our own block",
		"the block at the tip is demonstrably not ours",
	)
}

// TestEqualSlotOwnershipIsInconclusiveWhenTheTipMoves pins the tip-slot
// re-read inside the ownership check.
//
// tipSlot is sampled once at the top of checkAndForgeProduction, and the
// chain can move before ownership is decided. Reusing that stale slot would
// decide ownership from two different blocks: here it would judge a rival's
// hash against a slot the tip has already moved past and report a lost
// battle at slot 10.
//
// Re-reading the tip inside tipBlockOwnership makes that disagreement
// inconclusive instead, and the decision falls back to the fence, which is
// the conservative answer. Slot and hash now come from ONE ChainTip
// snapshot, so the pair can no longer straddle two tips; what this pins is
// that a tip which moved between the cycle's read and the ownership check
// cannot manufacture a slot-battle report.
func TestEqualSlotOwnershipIsInconclusiveWhenTheTipMoves(t *testing.T) {
	const slot = uint64(10)
	ourHash := bytes.Repeat([]byte{0xa1}, 32)
	rivalHash := bytes.Repeat([]byte{0xb2}, 32)

	leader := &forgerCountingLeader{}
	builder := &forgerTestBuilder{}
	broadcaster := &forgerTestBroadcaster{}
	fence := &fenceTestStore{slot: slot, present: true}
	clock := &forgerMovingTipSlotClock{
		currentSlot: slot,
		// The cycle opens with the tip at the current slot...
		chainTipSlot: slot,
		// ...and it has moved on by the time the hash is read.
		movedTipSlot:      slot + 1,
		chainTipHash:      rivalHash,
		slotsPerKESPeriod: 100,
	}
	forger, logs := newOwnTipGateForger(
		t,
		clock,
		leader,
		builder,
		broadcaster,
		fence,
	)
	forger.slotTracker.RecordForgedBlock(slot, ourHash)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	tipReads, hashReads := clock.reads()
	assert.Equal(
		t,
		2,
		tipReads,
		"the tip slot must be re-read next to the hash, not reused "+
			"from the top of the cycle",
	)
	assert.Equal(
		t,
		0,
		hashReads,
		"the hash must come from the same ChainTip snapshot as the slot, "+
			"not from a second read through ChainTipHashProvider",
	)

	assert.Zero(t, builder.calls)
	assert.Zero(t, broadcaster.calls)
	assert.Empty(t, fence.stored)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"a tip that moved proves nothing about who held slot 10",
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
	logged := logs.String()
	assert.NotContains(
		t,
		logged,
		`"level":"WARN"`,
		"an inconclusive comparison must not be reported as a lost battle",
	)
	assert.Contains(
		t,
		logged,
		`"matched_by":"forge_fence"`,
		"the decision must fall back to the fence",
	)
}

// errFallbackBuilderRefused stands in for the reasons an empty-body build can
// fail that have nothing to do with the mempool: an embedder's BlockBuilder
// that cannot honour the empty-body constraint, missing VRF or KES material.
var errFallbackBuilderRefused = errors.New(
	"block builder cannot honour the empty-body constraint",
)

// TestForgeLostSlotErrorCarriesTheFallbackFailure covers the error the caller
// is handed when both selection and the transaction-free fallback fail.
//
// The selection abort explains why the fallback was reached; it is the
// fallback's own failure that explains why the slot produced nothing. Only
// the selection error used to be returned, so a caller's errors.Is/As saw
// "transaction validation snapshot changed" and nothing else, and an operator
// went looking at the mempool for a fault in their key material or in a
// custom BlockBuilder.
func TestForgeLostSlotErrorCarriesTheFallbackFailure(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &fallbackTestBuilder{
		block:     block,
		cbor:      block.cbor,
		selectErr: errTxValidationSnapshotChanged,
		emptyErr:  errFallbackBuilderRefused,
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now(),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	err := forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, builder.emptyCalls)

	// Both halves stay inspectable: the abort that sent the forge to the
	// fallback, and the fallback failure that actually lost the slot.
	require.ErrorIs(
		t,
		err,
		errTxValidationSnapshotChanged,
		"the selection abort must remain inspectable",
	)
	require.ErrorIs(
		t,
		err,
		errFallbackBuilderRefused,
		"the fallback failure is the cause of the lost slot and must be inspectable",
	)
	require.Contains(t, err.Error(), errFallbackBuilderRefused.Error())
}

// TestForgeTimingReportsAdoptionForASuccessfulSlot pins the shape of the line
// for the ordinary case, so the adopted field the failure cases below rely on
// is known to be true when the block really reaches the chain.
func TestForgeTimingReportsAdoptionForASuccessfulSlot(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{block: block, cbor: block.cbor}
	var logs bytes.Buffer
	forger := newTimingForger(
		t,
		&logs,
		newTimingClock(),
		builder,
		&forgerTestBroadcaster{},
		nil,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	record := forgeTimingRecord(t, logs.String())
	require.Equal(t, "forged", record["outcome"])
	require.Equal(t, true, record["adopted"])
}

// TestForgeTimingDoesNotClaimSuccessWhenSelfValidationDropsTheBlock is the
// regression for the timing line describing an intermediate outcome.
//
// The line used to be emitted as soon as the block was built, so a block that
// self-validation then dropped left a record saying outcome=forged for a slot
// that put nothing on the chain. The line is the one an operator reads when a
// slot yielded no block, and it was pointing away from the failure.
func TestForgeTimingDoesNotClaimSuccessWhenSelfValidationDropsTheBlock(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	var logs bytes.Buffer
	forger := newTimingForger(
		t,
		&logs,
		newTimingClock(),
		builder,
		broadcaster,
		&forgerTestValidator{err: errors.New("bad VRF proof")},
	)

	require.Error(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 0, broadcaster.calls, "the block must not be adopted")

	record := forgeTimingRecord(t, logs.String())
	require.Equal(
		t,
		false,
		record["adopted"],
		"a dropped block must not be recorded as adopted",
	)
	// The block was built, and saying so is the point of keeping outcome
	// separate from adopted: this slot failed after selection, not during
	// it, which is a different fault to chase.
	require.Equal(t, "forged", record["outcome"])
}

// TestForgeTimingDoesNotClaimSuccessWhenAdoptionFails covers the other
// post-build loss: the block is built and self-validated, and AddBlock
// rejects it.
func TestForgeTimingDoesNotClaimSuccessWhenAdoptionFails(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{
		err: errors.New("block does not fit on the current chain tip"),
	}
	var logs bytes.Buffer
	forger := newTimingForger(
		t,
		&logs,
		newTimingClock(),
		builder,
		broadcaster,
		nil,
	)

	require.Error(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 1, broadcaster.calls)

	record := forgeTimingRecord(t, logs.String())
	require.Equal(t, false, record["adopted"])
	require.Equal(t, "forged", record["outcome"])
}

// TestForgeTimingIsEmittedWhenTheEmptyFallbackIsAdopted keeps the fallback's
// own success honest: an empty block that reaches the chain is a kept slot.
func TestForgeTimingIsEmittedWhenTheEmptyFallbackIsAdopted(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &fallbackTestBuilder{
		block:     block,
		cbor:      block.cbor,
		selectErr: errTxValidationSnapshotChanged,
	}
	var logs bytes.Buffer
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now(),
	}
	forger := newTimingForger(
		t,
		&logs,
		clock,
		builder,
		&forgerTestBroadcaster{},
		nil,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	record := forgeTimingRecord(t, logs.String())
	require.Equal(t, "empty", record["outcome"])
	require.Equal(t, true, record["adopted"])
}

// parentSwapBuilder fails its first attempt with errParentChangedDuringBuild
// and swaps the parent announcement underneath the forge while doing so, so
// the retry runs against a different parent than the one the Leios
// certificate was selected for. It records the Leios data handed to every
// attempt.
type parentSwapBuilder struct {
	block ledger.Block
	cbor  []byte
	calls int
	seen  []LeiosBlockData
	// seenEmpty records whether each attempt was the empty-body
	// fallback, in the same order as seen.
	seenEmpty []bool
	onFirst   func()
	failOnce  bool
	// failNonEmpty fails every attempt allowed to carry transactions, so
	// the forge is driven all the way to the empty-body fallback.
	failNonEmpty bool
}

func (b *parentSwapBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	return nil, nil, errParentChangedDuringBuild
}

func (b *parentSwapBuilder) buildBlockWithCredentialGeneration(
	_ uint64,
	_ uint64,
	leios LeiosBlockData,
	_ *credentialGeneration,
	constraints blockSelectionConstraints,
	_ *BlockContext,
) (ledger.Block, []byte, error) {
	b.calls++
	b.seen = append(b.seen, leios)
	b.seenEmpty = append(b.seenEmpty, constraints.emptyBody)
	if constraints.emptyBody {
		return b.block, b.cbor, nil
	}
	if b.calls == 1 {
		if b.onFirst != nil {
			b.onFirst()
		}
		if b.failOnce || b.failNonEmpty {
			return nil, nil, errParentChangedDuringBuild
		}
	}
	if b.failNonEmpty {
		return nil, nil, errParentChangedDuringBuild
	}
	return b.block, b.cbor, nil
}

var _ credentialGenerationBlockBuilder = (*parentSwapBuilder)(nil)

func leiosTestCertificate(
	ebHash lcommon.Blake2b256,
	slot uint64,
) *lcommon.LeiosEbCertificate {
	return &lcommon.LeiosEbCertificate{
		SlotNo:            slot,
		EndorserBlockHash: ebHash,
		Signers:           []byte{0x80},
		AggregatedSignature: make(
			[]byte,
			lcommon.LeiosBlsSignatureSize,
		),
	}
}

func leiosHash(b byte) lcommon.Blake2b256 {
	return lcommon.NewBlake2b256(bytes.Repeat([]byte{b}, 32))
}

// TestForgeReResolvesLeiosDataWhenParentChanges is the correctness half of
// the in-slot retry. The Leios certificate a ranking block carries is
// selected for one specific parent -- leiosBlockDataForSlot matches the
// certified endorser block against the parent's announcement -- so retrying
// against a new parent while reusing the old parent's certificate would
// commit a block to a certificate that does not belong to it.
func TestForgeReResolvesLeiosDataWhenParentChanges(t *testing.T) {
	oldParentRb := leiosHash(0xA1)
	oldEb := leiosHash(0xB1)
	newParentRb := leiosHash(0xA2)

	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: oldParentRb,
		hash:   oldEb,
		ok:     true,
	}
	certs := &forgerTestLeiosCerts{
		eligible: []LeiosCertifiedEndorserBlock{
			{
				EndorserBlockHash: oldEb,
				AnnouncingRbHash:  oldParentRb,
				SlotNo:            9,
				Certificate:       leiosTestCertificate(oldEb, 9),
			},
		},
	}
	block := newForgerTestBlock(10, 2)
	builder := &parentSwapBuilder{
		block:    block,
		cbor:     block.cbor,
		failOnce: true,
		onFirst: func() {
			// A peer block lands: the chain now has a different parent,
			// which announced no endorser block this node holds a
			// certificate for.
			parent.rbHash = newParentRb
			parent.hash = leiosHash(0xB2)
		},
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &retryTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		LeiosCertificateProvider:        certs,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(
		t,
		builder.seen,
		2,
		"the forge must retry after a parent change",
	)
	require.NotNil(
		t,
		builder.seen[0].Certificate,
		"the first attempt carries the certificate selected for the old parent",
	)
	require.Nil(
		t,
		builder.seen[1].Certificate,
		"the retry must not reuse the old parent's certificate",
	)
	require.Empty(
		t,
		certs.marked,
		"no endorser block was embedded, so none may be marked embedded",
	)
}

// TestForgeDropsStaleAnnouncementWhenParentChanges covers the other
// parent-dependent field. The announcement names an endorser block this
// node selected and broadcast against the previous parent's certified
// closure; carrying it onto a block with a different parent would commit
// to an exclusion set that no longer holds.
func TestForgeDropsStaleAnnouncementWhenParentChanges(t *testing.T) {
	oldParentRb := leiosHash(0xC1)
	oldEb := leiosHash(0xD1)
	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: oldParentRb,
		hash:   oldEb,
		ok:     true,
	}
	certs := &forgerTestLeiosCerts{}
	block := newForgerTestBlock(10, 2)
	builder := &parentSwapBuilder{
		block:    block,
		cbor:     block.cbor,
		failOnce: true,
		onFirst:  func() { parent.rbHash = leiosHash(0xC2) },
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &retryTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		LeiosProduceChecker: &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:  &forgerTestLeiosCaster{},
		LeiosTxValidator:    &sessionMockTxValidator{},
		LeiosMempool: forgerTestMempoolProvider{
			txs: leiosAnnouncementTxs(t),
		},
		LeiosCertificateProvider:        certs,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.seen, 2)
	require.NotNil(
		t,
		builder.seen[0].Announcement,
		"the first attempt announces the endorser block forged for the old parent",
	)
	require.Nil(
		t,
		builder.seen[1].Announcement,
		"the retry must not announce an endorser block selected for the old parent",
	)
}

// TestForgeKeepsLeiosDataWhenParentIsUnchanged is the negative case: a
// retry driven by a generation bump that did not move the parent must keep
// the certificate and the announcement it already resolved, rather than
// throwing away a valid endorser block.
func TestForgeKeepsLeiosDataWhenParentIsUnchanged(t *testing.T) {
	parentRb := leiosHash(0xE1)
	eb := leiosHash(0xF1)
	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: parentRb,
		hash:   eb,
		ok:     true,
	}
	certs := &forgerTestLeiosCerts{
		eligible: []LeiosCertifiedEndorserBlock{
			{
				EndorserBlockHash: eb,
				AnnouncingRbHash:  parentRb,
				SlotNo:            9,
				Certificate:       leiosTestCertificate(eb, 9),
			},
		},
		txHashes:   []string{},
		txHashesOK: true,
	}
	block := newForgerTestBlock(10, 2)
	builder := &parentSwapBuilder{
		block:    block,
		cbor:     block.cbor,
		failOnce: true,
	}

	logs := &strings.Builder{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(logs, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &retryTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		LeiosCertificateProvider:        certs,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.seen, 2)
	require.NotNil(t, builder.seen[1].Certificate)
	require.NotContains(
		t,
		logs.String(),
		"leios payload re-resolved",
		"an unchanged parent is not a re-resolution: reporting one would "+
			"make the warning that flags a real parent swap unreadable",
	)
	require.Equal(
		t,
		builder.seen[0].Certificate,
		builder.seen[1].Certificate,
		"an unchanged parent keeps the certificate already selected",
	)
	require.Equal(
		t,
		[]lcommon.Blake2b256{eb},
		certs.marked,
		"the embedded endorser block is still marked after the retry",
	)
}

func leiosAnnouncementTxs(t *testing.T) []MempoolTransaction {
	t.Helper()
	return []MempoolTransaction{
		{
			Hash: "1111111111111111111111111111111111111111111111111111111111111111",
			Cbor: makeMinimalTxCbor(t, 0x11, 0),
			Type: conway.TxTypeConway,
		},
	}
}

// TestForgeReResolvesLeiosDataForEmptyFallback closes the same hole on the
// other recovery path. The empty-body fallback is a fresh build against
// whatever the chain tip is now, so it needs its Leios payload resolved
// against that parent just as a retry does -- an empty block carrying a
// certificate that belongs to a different parent is no better than a full
// one.
func TestForgeReResolvesLeiosDataForEmptyFallback(t *testing.T) {
	oldParentRb := leiosHash(0x71)
	oldEb := leiosHash(0x81)
	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: oldParentRb,
		hash:   oldEb,
		ok:     true,
	}
	certs := &forgerTestLeiosCerts{
		eligible: []LeiosCertifiedEndorserBlock{
			{
				EndorserBlockHash: oldEb,
				AnnouncingRbHash:  oldParentRb,
				SlotNo:            9,
				Certificate:       leiosTestCertificate(oldEb, 9),
			},
		},
	}
	block := newForgerTestBlock(10, 2)
	builder := &parentSwapBuilder{
		block:        block,
		cbor:         block.cbor,
		failNonEmpty: true,
		onFirst: func() {
			parent.rbHash = leiosHash(0x72)
			parent.hash = leiosHash(0x82)
		},
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &retryTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			// No slot time left, so the first failure goes straight to
			// the empty-body fallback.
			slotEnd: time.Now(),
		},
		LeiosCertificateProvider:        certs,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.seen, 2)
	require.False(t, builder.seenEmpty[0])
	require.True(t, builder.seenEmpty[1], "the second attempt is the fallback")
	require.NotNil(t, builder.seen[0].Certificate)
	require.Nil(
		t,
		builder.seen[1].Certificate,
		"the empty fallback must not carry the old parent's certificate",
	)
	require.Empty(t, certs.marked)
}

// TestForgeKeepsAnnouncementWhenParentIsUnchanged is the announcement half
// of the negative case. A retry driven by a generation bump that left the
// parent alone must keep the endorser block this slot already forged and
// broadcast, rather than discarding a valid announcement.
func TestForgeKeepsAnnouncementWhenParentIsUnchanged(t *testing.T) {
	parentRb := leiosHash(0x91)
	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: parentRb,
		hash:   leiosHash(0x92),
		ok:     true,
	}
	block := newForgerTestBlock(10, 2)
	builder := &parentSwapBuilder{
		block:    block,
		cbor:     block.cbor,
		failOnce: true,
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &retryTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		LeiosProduceChecker: &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:  &forgerTestLeiosCaster{},
		LeiosTxValidator:    &sessionMockTxValidator{},
		LeiosMempool: forgerTestMempoolProvider{
			txs: leiosAnnouncementTxs(t),
		},
		LeiosCertificateProvider:        &forgerTestLeiosCerts{},
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.seen, 2)
	require.NotNil(t, builder.seen[0].Announcement)
	require.Equal(
		t,
		builder.seen[0].Announcement,
		builder.seen[1].Announcement,
		"an unchanged parent keeps the endorser block already announced",
	)
}

// TestForgeReResolvesLeiosDataWhenParentChangesBeforeTheFirstBuild covers the
// half of the parent-change problem that no retry can reach.
//
// The Leios payload is resolved before this slot's endorser-block production
// and the KES step, and the chain tip can move across that work. On the retry
// path a moved tip announces itself as errParentChangedDuringBuild, which is
// what triggers the re-resolve. Before the first build there is no such
// signal: the builder reads the tip when it starts, so a tip that moved
// beforehand simply becomes the parent, and the build succeeds on the first
// attempt while carrying the previous parent's certificate. The block is then
// committed to a certificate that does not belong to it, with no error and no
// retry anywhere in the trace.
func TestForgeReResolvesLeiosDataWhenParentChangesBeforeTheFirstBuild(
	t *testing.T,
) {
	oldParentRb := leiosHash(0xC1)
	oldEb := leiosHash(0xD1)
	newParentRb := leiosHash(0xC2)

	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: oldParentRb,
		hash:   oldEb,
		ok:     true,
	}
	certs := &forgerTestLeiosCerts{
		eligible: []LeiosCertifiedEndorserBlock{
			{
				EndorserBlockHash: oldEb,
				AnnouncingRbHash:  oldParentRb,
				SlotNo:            9,
				Certificate:       leiosTestCertificate(oldEb, 9),
			},
		},
		// The certified closure resolves, so endorser-block production is
		// reached rather than skipped -- that is the window this test needs.
		txHashesOK: true,
	}
	block := newForgerTestBlock(10, 2)
	// Succeeds on the first attempt: there is no retry in this scenario,
	// which is the whole point.
	builder := &parentSwapBuilder{block: block, cbor: block.cbor}

	// The parent moves during endorser-block production, which runs between
	// the Leios resolution and the first build.
	leiosChecker := &forgerTestLeiosChecker{
		reason: "not eligible",
		callback: func() error {
			parent.rbHash = newParentRb
			parent.hash = leiosHash(0xD2)
			return nil
		},
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &retryTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		LeiosCertificateProvider:        certs,
		LeiosParentAnnouncementProvider: parent,
		LeiosProduceChecker:             leiosChecker,
		LeiosEBBroadcaster:              &forgerTestLeiosCaster{},
		LeiosMempool:                    forgerTestMempoolProvider{},
		LeiosTxValidator:                &mockTxValidator{},
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(
		t,
		1,
		leiosChecker.calls,
		"the parent must have moved during endorser-block production",
	)
	require.Len(
		t,
		builder.seen,
		1,
		"the first build succeeds, so nothing retries and nothing else can re-resolve",
	)
	require.Nil(
		t,
		builder.seen[0].Certificate,
		"the first build must not carry the previous parent's certificate",
	)
	require.Empty(
		t,
		certs.marked,
		"no endorser block was embedded, so none may be marked embedded",
	)
}

// TestForgeMarksTheReResolvedEndorserBlockSlot pins the pairing of the two
// values that identify an embedded endorser block. Since the
// occurrence is (hash, slot), not hash alone: the same endorser-block hash
// can be a distinct occurrence at another slot, so marking a re-resolved
// hash against the slot of the endorser block it replaced would retire the
// wrong occurrence.
//
// A retry re-resolves the whole payload against the new parent, so both
// halves move together. Carrying only the hash back out of the retry leaves
// the slot at the value the first attempt resolved, which is exactly the
// mismatch exists to prevent.
func TestForgeMarksTheReResolvedEndorserBlockSlot(t *testing.T) {
	oldParentRb := leiosHash(0xC1)
	oldEb := leiosHash(0xD1)
	newParentRb := leiosHash(0xC2)
	newEb := leiosHash(0xD2)

	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: oldParentRb,
		hash:   oldEb,
		ok:     true,
	}
	certs := &forgerTestLeiosCerts{
		eligible: []LeiosCertifiedEndorserBlock{
			{
				EndorserBlockHash: oldEb,
				AnnouncingRbHash:  oldParentRb,
				SlotNo:            9,
				Certificate:       leiosTestCertificate(oldEb, 9),
			},
			{
				EndorserBlockHash: newEb,
				AnnouncingRbHash:  newParentRb,
				SlotNo:            7,
				Certificate:       leiosTestCertificate(newEb, 7),
			},
		},
	}
	block := newForgerTestBlock(10, 2)
	builder := &parentSwapBuilder{
		block:    block,
		cbor:     block.cbor,
		failOnce: true,
		onFirst: func() {
			// A peer block lands. The new parent announced a different
			// endorser block, certified at a different slot, and that is
			// the one the retry's block embeds.
			parent.rbHash = newParentRb
			parent.hash = newEb
		},
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &retryTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		LeiosCertificateProvider:        certs,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(
		t,
		builder.seen,
		2,
		"the forge must retry after a parent change",
	)
	require.NotNil(t, builder.seen[1].Certificate)
	require.Equal(
		t,
		newEb,
		builder.seen[1].Certificate.EndorserBlockHash,
		"the retry carries the certificate selected for the new parent",
	)
	require.Equal(
		t,
		[]lcommon.Blake2b256{newEb},
		certs.marked,
		"the endorser block the forged block embedded is the one marked",
	)
	require.Equal(
		t,
		[]uint64{7},
		certs.markedSlots,
		"the marked slot must be the re-resolved endorser block's own slot, "+
			"not the slot of the endorser block the first attempt resolved",
	)
}

// retryTestSlotClock is forgerTestSlotClock with a controllable slot-end
// instant, so tests can put the forge either comfortably inside its slot or
// past the end of it without sleeping.
type retryTestSlotClock struct {
	currentSlot         uint64
	chainTipSlot        uint64
	chainTipHash        []byte
	chainTipBlockNumber uint64
	// primaryTipExplicit selects whether primaryTipSlot/primaryTipHash are
	// used verbatim. When false the primary tip mirrors the applied tip,
	// which is the caught-up steady state and what every test that does not
	// care about the distinction wants. A test that needs an apply backlog
	// -- the primary chain tip ahead of, behind, or replaced at the applied
	// tip -- sets it and moves the primary tip on its own.
	primaryTipExplicit    bool
	primaryTipSlot        uint64
	primaryTipHash        []byte
	primaryTipRelationSet bool
	primaryTipBlockNumber uint64
	primaryTipDepth       uint64
	primaryTipAncestor    bool
	slotsPerKESPeriod     uint64
	slotEnd               time.Time
	// upstreamTarget and upstreamLive are what UpstreamSyncStatus reports.
	// The zero value is (0, false) -- no upstream peer -- which is what
	// every test that does not exercise the sync gate wants, and what this
	// clock reported before the fields existed.
	upstreamTarget      uint64
	upstreamLive        bool
	upstreamBlockNumber uint64
}

func (c *retryTestSlotClock) CurrentSlot() (uint64, error) {
	return c.currentSlot, nil
}

func (c *retryTestSlotClock) SlotsPerKESPeriod() uint64 {
	return c.slotsPerKESPeriod
}

// ChainTip is the ledger-applied tip. PrimaryChainTip mirrors it unless a
// test asks for the two to differ: most of these tests drive re-selection
// from the slot clock and the mempool, not from an apply backlog, so the
// default describes a caught-up node.
func (c *retryTestSlotClock) ChainTip() ocommon.Point {
	return ocommon.Point{Slot: c.chainTipSlot, Hash: c.chainTipHash}
}

func (c *retryTestSlotClock) PrimaryChainTip() ocommon.Point {
	if c.primaryTipExplicit {
		return ocommon.Point{Slot: c.primaryTipSlot, Hash: c.primaryTipHash}
	}
	return c.ChainTip()
}

func (c *retryTestSlotClock) NextSlotTime() (time.Time, error) {
	return c.slotEnd, nil
}

func (c *retryTestSlotClock) UpstreamTipSlot() uint64 { return 0 }

func (c *retryTestSlotClock) UpstreamSyncStatus() (uint64, bool) {
	return c.upstreamTarget, c.upstreamLive
}

func (c *retryTestSlotClock) ForgeTipSnapshot() (ochainsync.Tip, int) {
	blockNumber := c.chainTipBlockNumber
	if blockNumber == 0 {
		blockNumber = c.chainTipSlot
	}
	return ochainsync.Tip{
		Point:       c.ChainTip(),
		BlockNumber: blockNumber,
	}, 5
}

func (c *retryTestSlotClock) PrimaryChainTipRelation(
	point ocommon.Point,
) (ochainsync.Tip, uint64, bool, error) {
	primary := c.PrimaryChainTip()
	if c.primaryTipRelationSet {
		blockNumber := c.primaryTipBlockNumber
		if blockNumber == 0 {
			blockNumber = primary.Slot
		}
		return ochainsync.Tip{Point: primary, BlockNumber: blockNumber},
			c.primaryTipDepth, c.primaryTipAncestor, nil
	}
	depth := uint64(0)
	ancestor := true
	if primary.Slot < point.Slot {
		ancestor = false
	} else if primary.Slot == point.Slot && len(primary.Hash) > 0 &&
		len(point.Hash) > 0 && !bytes.Equal(primary.Hash, point.Hash) {
		ancestor = false
	} else if primary.Slot > point.Slot {
		depth = primary.Slot - point.Slot
	}
	return ochainsync.Tip{Point: primary, BlockNumber: primary.Slot},
		depth, ancestor, nil
}

func (c *retryTestSlotClock) UpstreamSyncTip() (ochainsync.Tip, bool) {
	return ochainsync.Tip{
		Point:       ocommon.Point{Slot: c.upstreamTarget},
		BlockNumber: c.upstreamBlockNumber,
	}, c.upstreamLive
}

// retryTestBuilder fails its first failCount build attempts with err and
// succeeds afterwards, so a test can observe whether the forger makes a
// second attempt for the same slot.
type retryTestBuilder struct {
	block     ledger.Block
	cbor      []byte
	calls     int
	failCount int
	err       error
}

func (b *retryTestBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	b.calls++
	if b.calls <= b.failCount {
		return nil, nil, b.err
	}
	return b.block, b.cbor, nil
}

func newRetryForger(
	t *testing.T,
	clock *retryTestSlotClock,
	builder BlockBuilder,
	broadcaster *forgerTestBroadcaster,
	opts ...func(*ForgerConfig),
) *BlockForger {
	t.Helper()
	cfg := ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	forger, err := NewBlockForger(cfg)
	require.NoError(t, err)
	return forger
}

// withSyncTolerance sets ForgeSyncToleranceSlots, the upstream-sync gate's
// bound, so a test can put a tip inside it at entry and outside it after a
// rollback without needing realistic slot numbers.
func withSyncTolerance(slots uint64) func(*ForgerConfig) {
	return func(cfg *ForgerConfig) {
		cfg.ForgeSyncToleranceSlots = slots
	}
}

// withSelectionDeadlineMargin turns the opt-in selection deadline on for a
// test. It is off by default, so a test that means to exercise truncation
// has to ask for it, exactly as an operator does.
func withSelectionDeadlineMargin(
	margin time.Duration,
) func(*ForgerConfig) {
	return func(cfg *ForgerConfig) {
		cfg.ForgeSelectionDeadlineMargin = margin
	}
}

// TestForgeRetriesSelectionWhenSnapshotChangesMidSlot is the regression for
// the lost leader slot: a ledger publication landing during transaction
// selection aborts the candidate, and before this the forger simply gave up
// and slept to the next slot. With time still left in the slot the forge
// must re-run selection against the new snapshot and still produce a block.
func TestForgeRetriesSelectionWhenSnapshotChangesMidSlot(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{
		block:     block,
		cbor:      block.cbor,
		failCount: 1,
		// Wrapped exactly as DefaultBlockBuilder wraps it, so the
		// forger has to match on the sentinel rather than on a string.
		err: fmt.Errorf(
			"failed to select block transactions: %w",
			errTxValidationSnapshotChanged,
		),
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(2 * time.Second),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 2, builder.calls, "forger must re-select within the slot")
	require.Equal(t, 1, broadcaster.calls)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
		"a slot recovered by retry is not a could-not-forge",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("retried"),
		),
	)
}

// TestForgeDoesNotRetrySelectionWithoutSlotTimeLeft pins the deadline half of
// the retry: when the slot is already over there is no point re-running
// selection, so the attempt is abandoned after one try.
func TestForgeDoesNotRetrySelectionWithoutSlotTimeLeft(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{
		block:     block,
		cbor:      block.cbor,
		failCount: 1,
		err:       errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		// Slot already ended: less than the retry margin remains.
		slotEnd: time.Now(),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	err := forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, errTxValidationSnapshotChanged)
	require.Equal(t, 1, builder.calls, "no slot time left means no retry")
	require.Equal(t, 0, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("lost"),
		),
	)
}

// TestForgeStopsRetryingSelectionAtAttemptCap keeps the retry bounded: a
// producer whose ledger publishes continuously must not spin on selection
// for the whole slot.
func TestForgeStopsRetryingSelectionAtAttemptCap(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{
		block:     block,
		cbor:      block.cbor,
		failCount: 1000,
		err:       errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(time.Hour),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	err := forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.Equal(
		t,
		1+defaultForgeSelectionMaxRetries,
		builder.calls,
		"retries must stop at the configured cap",
	)
	require.Equal(t, 0, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("lost"),
		),
	)
}

// TestForgeDoesNotRetryNonSelectionBuildFailures keeps the retry narrow: a
// build that failed for any reason other than the chain moving under
// selection is not made more likely to succeed by trying again.
func TestForgeDoesNotRetryNonSelectionBuildFailures(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{
		block:     block,
		cbor:      block.cbor,
		failCount: 1,
		err:       errors.New("VRF verification key not loaded"),
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(time.Hour),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	err := forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, builder.calls)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("lost"),
		),
		"a non-selection build failure is not a selection fallback",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

func forgeTimingRecord(t *testing.T, logs string) map[string]any {
	t.Helper()
	for line := range strings.SplitSeq(strings.TrimSpace(logs), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if record["msg"] == "forge timing" {
			return record
		}
	}
	t.Fatalf("no forge timing line in logs: %s", logs)
	return nil
}

// newTimingForger builds a production forger whose logs land in logs. validator
// may be nil for the tests that do not exercise self-validation.
func newTimingForger(
	t *testing.T,
	logs *bytes.Buffer,
	clock *retryTestSlotClock,
	builder BlockBuilder,
	broadcaster *forgerTestBroadcaster,
	validator BlockValidator,
) *BlockForger {
	t.Helper()
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(logs, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		BlockValidator:   validator,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

// newTimingClock is the slot clock the timing tests share: slot 10, a chain
// tip one slot behind, and a slot that has not run out.
func newTimingClock() *retryTestSlotClock {
	return &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(time.Second),
	}
}

// TestForgeLogsTimingForEveryForge records what the field trace behind this
// change had to be reconstructed from block timestamps: how long the leader
// gate took to clear and how long selection then ran. Without it a lost
// slot shows only an error line and a counter.
func TestForgeLogsTimingForEveryForge(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{block: block, cbor: block.cbor}
	var logs bytes.Buffer
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(time.Second),
	}
	forger := newTimingForger(
		t,
		&logs,
		clock,
		builder,
		&forgerTestBroadcaster{},
		nil,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	record := forgeTimingRecord(t, logs.String())
	require.Equal(t, float64(10), record["slot"])
	require.Equal(t, "forged", record["outcome"])
	require.Equal(t, float64(1), record["attempts"])
	for _, key := range []string{
		"leader_check",
		"pre_build",
		"build",
		"tx_count",
	} {
		require.Contains(t, record, key)
	}
}

// TestForgeTimingReportsTheEmptyFallbackOutcome makes the fallback legible
// in the log as well as in the metric.
func TestForgeTimingReportsTheEmptyFallbackOutcome(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &fallbackTestBuilder{
		block:     block,
		cbor:      block.cbor,
		selectErr: errTxValidationSnapshotChanged,
	}
	var logs bytes.Buffer
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now(),
	}
	forger := newTimingForger(
		t,
		&logs,
		clock,
		builder,
		&forgerTestBroadcaster{},
		nil,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	record := forgeTimingRecord(t, logs.String())
	require.Equal(t, "empty", record["outcome"])
	require.Equal(t, float64(2), record["attempts"])
}

// TestForgeTimingReportsALostSlot keeps the timing line on the failure path,
// which is the one an operator reads after a slot produced nothing.
func TestForgeTimingReportsALostSlot(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{
		block:     block,
		cbor:      block.cbor,
		failCount: 1000,
		err:       errTxValidationSnapshotChanged,
	}
	var logs bytes.Buffer
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(time.Hour),
	}
	forger := newTimingForger(
		t,
		&logs,
		clock,
		builder,
		&forgerTestBroadcaster{},
		nil,
	)

	require.Error(t, forger.checkAndForgeProduction(context.Background()))
	record := forgeTimingRecord(t, logs.String())
	require.Equal(t, "lost", record["outcome"])
	// One initial pass, the retry cap, and the empty-block fallback
	// attempt that also failed.
	require.Equal(
		t,
		float64(2+defaultForgeSelectionMaxRetries),
		record["attempts"],
	)
}

// tipMovingBuilder aborts its first selection pass the way a concurrent
// ledger publication does, and moves the chain tip while doing it. That is
// the real shape of the window this fix closes: the abort and the tip move
// are the same event, so the reading that cleared the forge's entry gates
// is stale by the time the retry or the fallback is decided.
type tipMovingBuilder struct {
	block ledger.Block
	cbor  []byte
	clock *retryTestSlotClock
	// tipDuringBuild is where the applied tip (and, with it, the primary
	// chain tip) lands during the first selection pass.
	tipDuringBuild uint64
	// moveTip, when set, replaces the tipDuringBuild move: it is how a test
	// moves the primary chain tip on its own, leaving the applied tip where
	// it was, to express an apply backlog opening mid-selection.
	moveTip    func(*retryTestSlotClock)
	calls      int
	emptyCalls int
	selectErr  error
}

func (b *tipMovingBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	b.calls++
	return nil, nil, b.selectErr
}

func (b *tipMovingBuilder) buildBlockWithCredentialGeneration(
	_ uint64,
	_ uint64,
	_ LeiosBlockData,
	_ *credentialGeneration,
	constraints blockSelectionConstraints,
	_ *BlockContext,
) (ledger.Block, []byte, error) {
	b.calls++
	if constraints.emptyBody {
		b.emptyCalls++
		return b.block, b.cbor, nil
	}
	if b.calls == 1 {
		if b.moveTip != nil {
			b.moveTip(b.clock)
		} else {
			b.clock.chainTipSlot = b.tipDuringBuild
		}
	}
	return nil, nil, b.selectErr
}

var _ credentialGenerationBlockBuilder = (*tipMovingBuilder)(nil)

// requireNoFallbackCounted asserts the slot was refused rather than saved or
// lost by the fallback: refusing before building is not a fallback outcome,
// and reporting one would credit or blame a path that never ran.
func requireNoFallbackCounted(t *testing.T, forger *BlockForger) {
	t.Helper()
	for _, result := range []string{
		forgeSelectionResultRetried,
		forgeSelectionResultEmpty,
		forgeSelectionResultLost,
	} {
		require.Equal(
			t,
			float64(0),
			testutil.ToFloat64(
				forger.metrics.forgeSelectionFallback.WithLabelValues(
					result,
				),
			),
			"no fallback outcome may be counted for result=%s",
			result,
		)
	}
}

// TestForgeRefusesTheFallbackWhenTheTipPassedTheForgedSlot is the blocker
// this test file exists for. The entry gate refuses a slot the chain has
// already covered; the fallback runs precisely because the chain moved, and
// used to build, VRF-prove and KES-sign against the tip read before that
// move. The resulting block names a parent whose slot is not below its own,
// and nothing local rejects that before diffusion: Chain.AddLocalBlock
// checks only prev-hash and block-number contiguity, so the block is admitted
// and broadcast, and ledger.validateBlockOrder runs only later, from
// ledgerProcessBlock, when the ledger applies it.
func TestForgeRefusesTheFallbackWhenTheTipPassedTheForgedSlot(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		// No slot time left, so the retry path is exhausted immediately
		// and the fallback is the only thing that can reach a build.
		slotEnd: time.Now(),
	}
	builder := &tipMovingBuilder{
		block:          block,
		cbor:           block.cbor,
		clock:          clock,
		tipDuringBuild: 11,
		selectErr:      errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(
		t,
		forger.checkAndForgeProduction(context.Background()),
		"a slot the chain took is declined, not an error",
	)
	require.Equal(
		t,
		0,
		builder.emptyCalls,
		"no block may be built for a slot the chain has passed",
	)
	require.Equal(t, 0, broadcaster.calls)
	requireNoFallbackCounted(t, forger)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"a tip beyond the slot is not a slot battle",
	)
}

// TestForgeCountsASlotBattleWhenTheTipReachesTheForgedSlot pins the equal
// case, which the entry gate counts and the fallback used to reach silently.
// Slot time remains here, so the refusal comes from the retry loop rather
// than from the fallback.
func TestForgeCountsASlotBattleWhenTheTipReachesTheForgedSlot(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(2 * time.Second),
	}
	builder := &tipMovingBuilder{
		block:          block,
		cbor:           block.cbor,
		clock:          clock,
		tipDuringBuild: 10,
		selectErr:      errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(
		t,
		1,
		builder.calls,
		"the retry must not re-enter the builder against the moved tip",
	)
	require.Equal(t, 0, builder.emptyCalls)
	require.Equal(t, 0, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"a rival block on our leader slot is the same battle whether it arrives before the forge or during it",
	)
	requireNoFallbackCounted(t, forger)
}

// TestForgeRefusesTheFallbackAsASlotBattle covers the equal case reaching
// the fallback itself, which is the path with no slot time left.
func TestForgeRefusesTheFallbackAsASlotBattle(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now(),
	}
	builder := &tipMovingBuilder{
		block:          block,
		cbor:           block.cbor,
		clock:          clock,
		tipDuringBuild: 10,
		selectErr:      errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 0, builder.emptyCalls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
	)
	requireNoFallbackCounted(t, forger)
}

// TestBuildBlockForSlotReportsBothTheAbortAndTheSupersededSlot keeps the
// refusal diagnosable: the selection abort explains why the fallback was
// reached, the tip error explains why it was refused, and an operator needs
// both to tell a superseded slot from a broken builder.
func TestBuildBlockForSlotReportsBothTheAbortAndTheSupersededSlot(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now(),
	}
	builder := &tipMovingBuilder{
		block:          block,
		cbor:           block.cbor,
		clock:          clock,
		tipDuringBuild: 11,
		selectErr:      errTxValidationSnapshotChanged,
	}
	forger := newRetryForger(t, clock, builder, &forgerTestBroadcaster{})

	leiosState := &forgeLeiosState{}
	_, _, _, err := forger.buildBlockForSlot(
		10,
		0,
		leiosState,
		nil,
		forgeTipGates{},
		new(*BlockContext),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errTxValidationSnapshotChanged)
	require.ErrorIs(t, err, errChainTipAheadOfSlot)
}

// TestForgeCountsTheEmptyFallbackOnlyAfterAdoption pins the counter move.
// dingo_forge_selection_fallback_total{result="empty"} is how the fix is
// read in the field, so it has to mean "the fallback saved this slot", not
// "the fallback built something".
func TestForgeCountsTheEmptyFallbackOnlyAfterAdoption(t *testing.T) {
	build := func(t *testing.T, adopt bool) *BlockForger {
		t.Helper()
		block := newForgerTestBlock(10, 2)
		builder := &fallbackTestBuilder{
			block:     block,
			cbor:      block.cbor,
			selectErr: errTxValidationSnapshotChanged,
		}
		broadcaster := &forgerTestBroadcaster{}
		if !adopt {
			broadcaster.err = errors.New("chain refused the block")
		}
		clock := &retryTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now(),
		}
		forger := newRetryForger(t, clock, builder, broadcaster)
		err := forger.checkAndForgeProduction(context.Background())
		if adopt {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
		require.Equal(t, 1, builder.emptyCalls)
		return forger
	}

	t.Run("adopted", func(t *testing.T) {
		forger := build(t, true)
		require.Equal(
			t,
			float64(1),
			testutil.ToFloat64(
				forger.metrics.forgeSelectionFallback.WithLabelValues(
					forgeSelectionResultEmpty,
				),
			),
		)
		require.Equal(
			t,
			float64(0),
			testutil.ToFloat64(
				forger.metrics.forgeSelectionFallback.WithLabelValues(
					forgeSelectionResultLost,
				),
			),
		)
	})

	t.Run("dropped before adoption", func(t *testing.T) {
		forger := build(t, false)
		require.Equal(
			t,
			float64(0),
			testutil.ToFloat64(
				forger.metrics.forgeSelectionFallback.WithLabelValues(
					forgeSelectionResultEmpty,
				),
			),
			"a block that never reached the chain did not save the slot",
		)
		require.Equal(
			t,
			float64(1),
			testutil.ToFloat64(
				forger.metrics.forgeSelectionFallback.WithLabelValues(
					forgeSelectionResultLost,
				),
			),
			"every aborted selection still lands in exactly one bucket",
		)
	})
}

// TestForgeCountsARetriedSlotOnlyAfterAdoption is the same rule for the
// other half of the counter.
func TestForgeCountsARetriedSlotOnlyAfterAdoption(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{
		block:     block,
		cbor:      block.cbor,
		failCount: 1,
		err:       errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{
		err: errors.New("chain refused the block"),
	}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(2 * time.Second),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.Error(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 2, builder.calls)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues(
				forgeSelectionResultRetried,
			),
		),
		"a re-selected block that never reached the chain did not save the slot",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues(
				forgeSelectionResultLost,
			),
		),
	)
}

// The tests below are the second blocker: after the entry gates decide
// a slot against BOTH tips, and a re-check that reads only the applied tip
// lets through exactly the builds the entry gates refuse. All three share
// one shape -- the applied tip stays below the forged slot for the whole
// attempt, so the applied-tip ordering alone admits every one of them, while
// the primary chain tip, the parent the builder actually uses, has moved to
// where the entry gates would have refused the slot. A retry or the fallback
// runs precisely because the primary tip moved, so this is the window in
// which the two tips are guaranteed to differ.

// applyBacklogClock is a retryTestSlotClock whose primary chain tip is
// driven separately from its applied tip.
func applyBacklogClock(
	currentSlot, appliedSlot uint64,
	appliedHash []byte,
	slotEnd time.Time,
) *retryTestSlotClock {
	return &retryTestSlotClock{
		currentSlot:        currentSlot,
		chainTipSlot:       appliedSlot,
		chainTipHash:       appliedHash,
		primaryTipExplicit: true,
		primaryTipSlot:     appliedSlot,
		primaryTipHash:     appliedHash,
		slotsPerKESPeriod:  100,
		slotEnd:            slotEnd,
	}
}

// requireStaleTipSkips asserts dingo_forge_stale_tip_skip_total holds
// exactly want, one entry per reason, and 0 for every other reason.
func requireStaleTipSkips(
	t *testing.T,
	forger *BlockForger,
	want map[string]float64,
) {
	t.Helper()
	for _, reason := range []string{
		forgeStaleTipReasonPrimaryNotAncestor,
		forgeStaleTipReasonBlockGap,
		forgeStaleTipReasonPeerHeightGap,
		forgeStaleTipReasonHashDiverged,
		forgeStaleTipReasonPrimaryTipBehind,
		forgeStaleTipReasonUnappliedRival,
		forgeStaleTipReasonAppliedStale,
		forgeStaleTipReasonEbManifestAhead,
	} {
		require.Equal(
			t,
			want[reason],
			testutil.ToFloat64(
				forger.metrics.forgeStaleTipSkip.WithLabelValues(reason),
			),
			"dingo_forge_stale_tip_skip_total{reason=%q}",
			reason,
		)
	}
}

// TestForgeRefusesTheRetryWhenThePrimaryTipReachesTheForgedSlot: a peer's
// block for the forged slot lands on the primary chain tip during selection
// while the ledger has not applied it. At entry this is the
// unapplied-block-at-slot refusal; with slot time left the retry used to
// re-enter the builder and parent a block for slot S on a tip already at S.
func TestForgeRefusesTheRetryWhenThePrimaryTipReachesTheForgedSlot(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	clock := applyBacklogClock(10, 9, nil, time.Now().Add(2*time.Second))
	builder := &tipMovingBuilder{
		block: block,
		cbor:  block.cbor,
		clock: clock,
		moveTip: func(c *retryTestSlotClock) {
			c.primaryTipSlot = 10
		},
		selectErr: errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(
		t,
		forger.checkAndForgeProduction(context.Background()),
		"a slot the primary chain already holds is declined, not an error",
	)
	require.Equal(
		t,
		1,
		builder.calls,
		"the retry must not re-enter the builder against a primary tip at the forged slot",
	)
	require.Equal(t, 0, builder.emptyCalls)
	require.Equal(t, 0, broadcaster.calls)
	requireNoFallbackCounted(t, forger)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"the rival block is unapplied, which the entry gate does not count as a slot battle",
	)
	requireStaleTipSkips(t, forger, map[string]float64{
		forgeStaleTipReasonUnappliedRival: 1,
	})
}

// TestForgeRefusesTheFallbackWhenThePrimaryTipPassesTheForgedSlot: the
// primary chain tip moves past the forged slot during selection with no slot
// time left, so the transaction-free fallback is the only thing that can
// reach a build. At entry a parent slot past the slot is refused outright;
// the fallback used to build on it.
func TestForgeRefusesTheFallbackWhenThePrimaryTipPassesTheForgedSlot(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	clock := applyBacklogClock(10, 9, nil, time.Now())
	builder := &tipMovingBuilder{
		block: block,
		cbor:  block.cbor,
		clock: clock,
		moveTip: func(c *retryTestSlotClock) {
			c.primaryTipSlot = 11
		},
		selectErr: errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(
		t,
		0,
		builder.emptyCalls,
		"no block may be built for a slot the primary chain has passed",
	)
	require.Equal(t, 0, broadcaster.calls)
	requireNoFallbackCounted(t, forger)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"a tip beyond the slot is not a slot battle",
	)
	requireStaleTipSkips(t, forger, map[string]float64{})
}

// TestForgeRefusesTheRetryWhenThePrimaryTipIsReplacedAtTheAppliedSlot: chain
// selection replaces the block at the applied tip's slot with a competing
// one at the same slot that the ledger has not applied. Both tips stay below
// the forged slot, so no slot ordering can see it; the entry gate refuses it
// by hash as primary_tip_hash_diverged, and the retry used to build on the
// replacement with transactions chosen against the block it replaced.
func TestForgeRefusesTheRetryWhenThePrimaryTipIsReplacedAtTheAppliedSlot(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	applied := bytes.Repeat([]byte{0xAA}, 32)
	rival := bytes.Repeat([]byte{0xBB}, 32)
	clock := applyBacklogClock(10, 9, applied, time.Now().Add(2*time.Second))
	builder := &tipMovingBuilder{
		block: block,
		cbor:  block.cbor,
		clock: clock,
		moveTip: func(c *retryTestSlotClock) {
			c.primaryTipHash = rival
		},
		selectErr: errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(
		t,
		1,
		builder.calls,
		"the retry must not re-enter the builder against a replaced parent",
	)
	require.Equal(t, 0, builder.emptyCalls)
	require.Equal(t, 0, broadcaster.calls)
	requireNoFallbackCounted(t, forger)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
	)
	requireStaleTipSkips(t, forger, map[string]float64{
		forgeStaleTipReasonHashDiverged: 1,
	})
}

// TestForgeRefusesTheRetryOnEveryStaleTipReason covers the remaining stale-tip
// refusals so the per-attempt decision is pinned to the whole entry decision
// rather than only the three positional cases above.
func TestForgeRefusesTheRetryOnEveryStaleTipReason(t *testing.T) {
	applied := bytes.Repeat([]byte{0xAA}, 32)
	cases := map[string]struct {
		appliedSlot uint64
		configure   func(*retryTestSlotClock)
		move        func(*retryTestSlotClock)
		reason      string
	}{
		"primary chain tip behind the applied tip": {
			appliedSlot: 9,
			move: func(c *retryTestSlotClock) {
				c.primaryTipSlot = 8
				c.primaryTipHash = bytes.Repeat([]byte{0xCC}, 32)
			},
			reason: forgeStaleTipReasonPrimaryTipBehind,
		},
		"apply backlog beyond the block limit": {
			// Applied at 3, primary moves to 9: six unapplied blocks,
			// with the parent slot still below the forged slot so no
			// ordering refusal fires first.
			appliedSlot: 3,
			move: func(c *retryTestSlotClock) {
				c.primaryTipSlot = 9
				c.primaryTipHash = bytes.Repeat([]byte{0xCC}, 32)
			},
			reason: forgeStaleTipReasonBlockGap,
		},
		"applied tip is no longer a primary-tip ancestor": {
			appliedSlot: 8,
			move: func(c *retryTestSlotClock) {
				c.primaryTipSlot = 9
				c.primaryTipHash = bytes.Repeat([]byte{0xCC}, 32)
				c.primaryTipRelationSet = true
				c.primaryTipDepth = 1
				c.primaryTipAncestor = false
			},
			reason: forgeStaleTipReasonPrimaryNotAncestor,
		},
		"corroborated peer height gap opens during selection": {
			appliedSlot: 9,
			configure: func(c *retryTestSlotClock) {
				c.chainTipBlockNumber = 100
				c.upstreamTarget = 9
				c.upstreamLive = true
				c.upstreamBlockNumber = 105
			},
			move: func(c *retryTestSlotClock) {
				c.chainTipSlot = 8
				c.chainTipBlockNumber = 99
			},
			reason: forgeStaleTipReasonPeerHeightGap,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			block := newForgerTestBlock(10, 2)
			clock := applyBacklogClock(
				10,
				tc.appliedSlot,
				applied,
				time.Now().Add(2*time.Second),
			)
			if tc.configure != nil {
				tc.configure(clock)
			}
			builder := &tipMovingBuilder{
				block:     block,
				cbor:      block.cbor,
				clock:     clock,
				moveTip:   tc.move,
				selectErr: errTxValidationSnapshotChanged,
			}
			broadcaster := &forgerTestBroadcaster{}
			forger := newRetryForger(t, clock, builder, broadcaster)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)
			require.Equal(t, 1, builder.calls)
			require.Equal(t, 0, builder.emptyCalls)
			require.Equal(t, 0, broadcaster.calls)
			requireNoFallbackCounted(t, forger)
			requireStaleTipSkips(t, forger, map[string]float64{
				tc.reason: 1,
			})
		})
	}
}

// TestTipGateRefusalsAreSkipsNotFailures pins the caller's contract: every
// error the per-attempt tip gates can return is the skip the entry gates
// would have taken, so none of them is reported up the forge loop as a
// build failure.
func TestTipGateRefusalsAreSkipsNotFailures(t *testing.T) {
	for _, err := range []error{
		errChainTipAheadOfSlot,
		errChainTipAtSlot,
		errPrimaryTipAtSlot,
		errTipsDisagree,
		errUpstreamSyncing,
	} {
		require.True(t, isTipGateRefusal(err), "%v", err)
		require.True(
			t,
			isTipGateRefusal(
				errors.Join(errTxValidationSnapshotChanged, err),
			),
			"joined with the selection abort: %v",
			err,
		)
	}
	require.False(t, isTipGateRefusal(errTxValidationSnapshotChanged))
	require.False(t, isTipGateRefusal(errBlockConstraintsUnsupported))
}

// TestForgeRefusesABuildAfterARollbackPutsTheTipBehindTheNetwork is the
// upstream-sync half of the per-attempt re-check, and the reason that gate
// had to move inside evaluateTipGates.
//
// The gate is decided from the LEDGER-APPLIED tip, and the applied tip is the
// one input to the gates that can move BACKWARDS during a forge: a rollback
// mid-selection leaves a node that was well inside ForgeSyncToleranceSlots at
// entry far outside it by the time the retry or the fallback builds. While
// the decision was taken once, at entry, and never re-run, the retry and the
// fallback built on the rolled-back view and broadcast the block -- exactly
// what the gate exists to stop -- with dingo_forge_sync_skip_total flat,
// because the only place that counter is reached had already been passed.
//
// Both ends of the ladder are covered: with slot time left the retry must not
// re-enter the builder, and with the slot over the transaction-free fallback
// must not be built either.
func TestForgeRefusesABuildAfterARollbackPutsTheTipBehindTheNetwork(
	t *testing.T,
) {
	cases := map[string]struct {
		slotEnd time.Time
	}{
		// Slot time left: the loop re-applies the gates before the second
		// attempt.
		"before the retry": {slotEnd: time.Now().Add(2 * time.Second)},
		// No slot time left: the retry path is exhausted immediately and
		// the fallback is the only thing that can reach a build.
		"before the transaction-free fallback": {slotEnd: time.Now()},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			block := newForgerTestBlock(10, 2)
			clock := &retryTestSlotClock{
				currentSlot:       10,
				chainTipSlot:      8,
				slotsPerKESPeriod: 100,
				slotEnd:           tc.slotEnd,
				// A live peer whose chain ends at slot 10. At entry the
				// applied tip trails it by 2, inside the tolerance of 3,
				// so the slot is admitted and the forge proceeds.
				upstreamTarget: 10,
				upstreamLive:   true,
			}
			builder := &tipMovingBuilder{
				block: block,
				cbor:  block.cbor,
				clock: clock,
				// The rollback: the applied tip drops to 5, so the same
				// upstream target now leads it by 5, beyond the tolerance.
				tipDuringBuild: 5,
				selectErr:      errTxValidationSnapshotChanged,
			}
			broadcaster := &forgerTestBroadcaster{}
			forger := newRetryForger(
				t,
				clock,
				builder,
				broadcaster,
				withSyncTolerance(3),
			)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
				"a slot refused because the node fell behind the network is declined, not an error",
			)
			require.Equal(
				t,
				1,
				builder.calls,
				"no attempt may be made against a tip the network has left behind",
			)
			require.Equal(
				t,
				0,
				builder.emptyCalls,
				"the transaction-free fallback is a build like any other and the gate refuses it too",
			)
			require.Equal(
				t,
				0,
				broadcaster.calls,
				"nothing may reach the wire from a rolled-back view",
			)
			require.Equal(
				t,
				float64(1),
				testutil.ToFloat64(forger.metrics.forgeSyncSkip),
				"the refusal is counted where the entry gate counts it",
			)
			requireNoFallbackCounted(t, forger)
			require.Equal(
				t,
				float64(0),
				testutil.ToFloat64(forger.metrics.slotBattlesTotal),
				"falling behind the network is not a slot battle",
			)
			requireStaleTipSkips(t, forger, map[string]float64{})
		})
	}
}

// TestForgeCountsAStaleTipAheadOfASlotBattleOnTheRetry pins the re-check's
// decision ORDER against the entry gates', for the readings that trip more
// than one gate at once.
//
// Every case here refuses the slot whichever gate is asked first, so nothing
// about the block produced is at stake: the only thing the order decides is
// WHICH counter moves, and the re-check has to move the one entry would have
// moved. Ordering is the whole subject of this test, and each case is one
// adjacent pair in the switch whose order no other test constrains -- reorder
// that pair and this test, alone, goes red.
//
// The pairs, in the switch's order:
//
//   - unappliedBlockAtSlot above syncSkip. A primary chain tip holding an
//     unapplied block AT the forged slot, with the applied tip far enough
//     behind it to be outside ForgeSyncToleranceSlots as well. Entry takes
//     the unapplied-block refusal before it reaches the sync gate, so this
//     counts unapplied_rival_at_leader_slot, not dingo_forge_sync_skip_total.
//
//   - syncSkip above staleReason. An applied tip outside the tolerance whose
//     apply backlog is also wider than the local block limit.
//     Entry takes the sync skip before the leader check and the stale-tip
//     refusal only after it, so this counts dingo_forge_sync_skip_total, not
//     a stale-tip reason.
//
//   - staleReason above appliedTipAtSlot. checkAndForgeProduction acts on
//     staleTipReason after the leader check and takes its slot-battle
//     decision only after that, so a reading with the applied tip AT the
//     forged slot and a primary chain tip that is diverged or behind is
//     counted on dingo_forge_stale_tip_skip_total at entry, not as a slot
//     battle. tipGatesRefuseSlot checked appliedTipAtSlot first, so the same
//     reading reached through a retry moved dingo_metrics_slotBattlesTotal_int
//     instead: the same event on the same node counted in two different series
//     purely on whether a peer's block landed before the forge started or
//     during it.
//
// Every case runs against ONE network view -- a live upstream whose chain ends
// at slot 10, tolerance 3 -- so the readings differ only in the two tips, which
// is the pair a retry or the fallback re-reads.
func TestForgeCountsAStaleTipAheadOfASlotBattleOnTheRetry(t *testing.T) {
	applied := bytes.Repeat([]byte{0xAA}, 32)
	rival := bytes.Repeat([]byte{0xBB}, 32)
	cases := map[string]struct {
		move func(*retryTestSlotClock)
		// reason is the dingo_forge_stale_tip_skip_total label expected to
		// hold 1, or "" when the refusal is counted somewhere else.
		reason string
		// syncSkip is the expected dingo_forge_sync_skip_total.
		syncSkip float64
	}{
		// The primary chain tip holds an unapplied block at the forged
		// slot while the applied tip has rolled back to 5, which the
		// upstream target at 10 leads by more than the tolerance of 3.
		// Both gates refuse; entry refuses on the unapplied block first.
		"unapplied rival at the slot outranks a tip outside the sync tolerance": {
			move: func(c *retryTestSlotClock) {
				c.chainTipSlot = 5
				c.primaryTipSlot = 10
			},
			reason: forgeStaleTipReasonUnappliedRival,
		},
		// The applied tip rolls back to 3, outside the tolerance of 3
		// against the upstream target at 10, and far enough behind the
		// primary chain tip at 9 for the apply backlog to exceed
		// the local block limit as well. Entry takes the sync skip before
		// the leader check, so the stale-tip counter must not move.
		"sync skip outranks the block-gap stale reason": {
			move: func(c *retryTestSlotClock) {
				c.chainTipSlot = 3
				c.primaryTipSlot = 9
			},
			syncSkip: 1,
		},
		// Chain selection replaced the block at the forged slot with a
		// competing one at the same slot that the ledger has applied
		// under the old hash.
		"primary chain tip diverged at the forged slot": {
			move: func(c *retryTestSlotClock) {
				c.chainTipSlot = 10
				c.primaryTipSlot = 10
				c.primaryTipHash = rival
			},
			reason: forgeStaleTipReasonHashDiverged,
		},
		// The ledger is at the forged slot while the primary chain tip
		// this node would parent on is a slot behind it.
		"primary chain tip behind the applied tip": {
			move: func(c *retryTestSlotClock) {
				c.chainTipSlot = 10
				c.primaryTipSlot = 9
			},
			reason: forgeStaleTipReasonPrimaryTipBehind,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			block := newForgerTestBlock(10, 2)
			clock := applyBacklogClock(
				10,
				9,
				applied,
				time.Now().Add(2*time.Second),
			)
			// One network view for every reading: a live peer whose
			// chain ends at slot 10. At entry the applied tip trails it
			// by 1, inside the tolerance of 3, so every case is admitted
			// and reaches the builder.
			clock.upstreamTarget = 10
			clock.upstreamLive = true
			builder := &tipMovingBuilder{
				block:     block,
				cbor:      block.cbor,
				clock:     clock,
				moveTip:   tc.move,
				selectErr: errTxValidationSnapshotChanged,
			}
			broadcaster := &forgerTestBroadcaster{}
			forger := newRetryForger(
				t,
				clock,
				builder,
				broadcaster,
				withSyncTolerance(3),
			)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)
			require.Equal(
				t,
				1,
				builder.calls,
				"the retry must not re-enter the builder while a gate refuses the slot",
			)
			require.Equal(t, 0, builder.emptyCalls)
			require.Equal(t, 0, broadcaster.calls)
			requireNoFallbackCounted(t, forger)
			require.Equal(
				t,
				float64(0),
				testutil.ToFloat64(forger.metrics.slotBattlesTotal),
				"entry refuses this reading on an earlier gate, so the re-check must too",
			)
			require.Equal(
				t,
				tc.syncSkip,
				testutil.ToFloat64(forger.metrics.forgeSyncSkip),
				"dingo_forge_sync_skip_total must move exactly when entry moves it",
			)
			want := map[string]float64{}
			if tc.reason != "" {
				want[tc.reason] = 1
			}
			requireStaleTipSkips(t, forger, want)
		})
	}
}

// TestBuildLeiosEBBodiesAlignWithRefs verifies that buildLeiosEB returns the
// transaction bodies in the same order as the manifest references, and that a
// transaction dropped from the manifest (invalid hash or size) is dropped from
// the bodies too, so body i stays aligned with reference i.
func TestBuildLeiosEBBodiesAlignWithRefs(t *testing.T) {
	txs := []MempoolTransaction{
		{Hash: strings.Repeat("11", 32), Cbor: []byte{0x01}},
		{Hash: "not-hex", Cbor: []byte{0x02}},            // dropped: bad hash
		{Hash: strings.Repeat("22", 32), Cbor: []byte{}}, // dropped: zero size
		{Hash: strings.Repeat("33", 32), Cbor: []byte{0x03, 0x04}},
	}

	ebCbor, ebHash, bodies, err := buildLeiosEB(txs, leiosEBCaps{})
	require.NoError(t, err)
	require.NotEmpty(t, ebHash)

	// Only the two valid transactions survive, in input order.
	require.Len(t, bodies, 2)
	require.Equal(t, []byte{0x01}, bodies[0])
	require.Equal(t, []byte{0x03, 0x04}, bodies[1])

	// The manifest references match the surviving bodies in order and size.
	eb, err := lcommon.NewLeiosEndorserBlockFromCbor(ebCbor)
	require.NoError(t, err)
	require.Len(t, eb.TransactionReferences, len(bodies))
	for i, ref := range eb.TransactionReferences {
		require.Equalf(
			t,
			len(bodies[i]),
			int(ref.TransactionSize),
			"reference %d size matches body length",
			i,
		)
	}
}

// TestBuildLeiosEBNoValidRefs verifies buildLeiosEB returns errNoValidTxRefs
// when no transaction yields a valid reference.
func TestBuildLeiosEBNoValidRefs(t *testing.T) {
	_, _, bodies, err := buildLeiosEB([]MempoolTransaction{
		{Hash: "not-hex", Cbor: []byte{0x01}},
	}, leiosEBCaps{})
	require.ErrorIs(t, err, errNoValidTxRefs)
	require.Nil(t, bodies)
}

type leiosOverlayValidator struct {
	base   map[utxoref.Key]struct{}
	reject map[string]struct{}
}

func (v *leiosOverlayValidator) ValidateTx(tx ledger.Transaction) error {
	return v.ValidateTxWithOverlay(tx, nil, nil)
}

func (v *leiosOverlayValidator) ValidateTxWithOverlay(
	tx ledger.Transaction,
	consumed map[utxoref.Key]struct{},
	created map[utxoref.Key]lcommon.Utxo,
) error {
	if _, reject := v.reject[tx.Hash().String()]; reject {
		return errors.New("rejected parent")
	}
	for _, input := range tx.Inputs() {
		key := utxoref.ForInput(input)
		if _, spent := consumed[key]; spent {
			return errors.New("already consumed")
		}
		if _, ok := created[key]; ok {
			continue
		}
		if _, ok := v.base[key]; !ok {
			return errors.New("missing input")
		}
	}
	return nil
}

func TestSelectValidLeiosTransactionsPreservesDependentChain(t *testing.T) {
	parentCbor := makeMinimalTxCbor(t, 0x41, 29)
	parent, err := conway.NewConwayTransactionFromCbor(parentCbor)
	require.NoError(t, err)
	childCbor := makeMinimalTxCborWithInput(t, parent.Hash().Bytes(), 0)
	child, err := conway.NewConwayTransactionFromCbor(childCbor)
	require.NoError(t, err)
	baseInput := parent.Inputs()[0]
	baseKey := utxoref.ForInput(baseInput)
	txs := []MempoolTransaction{
		{
			Hash: parent.Hash().String(),
			Cbor: parentCbor,
			Type: conway.TxTypeConway,
		},
		{
			Hash: child.Hash().String(),
			Cbor: childCbor,
			Type: conway.TxTypeConway,
		},
	}

	selected, _, err := selectValidLeiosTransactions(
		txs,
		&leiosOverlayValidator{
			base:   map[utxoref.Key]struct{}{baseKey: {}},
			reject: map[string]struct{}{},
		},
		leiosSelectionLimits{},
	)
	require.NoError(t, err)
	require.Equal(t, txs, selected)
}

func TestSelectValidLeiosTransactionsRejectsInvalidChain(t *testing.T) {
	parentCbor := makeMinimalTxCbor(t, 0x42, 29)
	parent, err := conway.NewConwayTransactionFromCbor(parentCbor)
	require.NoError(t, err)
	childCbor := makeMinimalTxCborWithInput(t, parent.Hash().Bytes(), 0)
	child, err := conway.NewConwayTransactionFromCbor(childCbor)
	require.NoError(t, err)
	baseInput := parent.Inputs()[0]
	baseKey := utxoref.ForInput(baseInput)
	txs := []MempoolTransaction{
		{
			Hash: parent.Hash().String(),
			Cbor: parentCbor,
			Type: conway.TxTypeConway,
		},
		{
			Hash: child.Hash().String(),
			Cbor: childCbor,
			Type: conway.TxTypeConway,
		},
	}

	selected, _, err := selectValidLeiosTransactions(
		txs,
		&leiosOverlayValidator{
			base: map[utxoref.Key]struct{}{baseKey: {}},
			reject: map[string]struct{}{
				parent.Hash().String(): {},
			},
		},
		leiosSelectionLimits{},
	)
	require.NoError(t, err)
	require.Empty(t, selected, "a rejected parent must not expose its output")
}

func TestSelectValidLeiosTransactionsRejectsUnrepresentableParent(
	t *testing.T,
) {
	parentCbor := makeMinimalTxCbor(t, 0x43, 29)
	parent, err := conway.NewConwayTransactionFromCbor(parentCbor)
	require.NoError(t, err)
	childCbor := makeMinimalTxCborWithInput(t, parent.Hash().Bytes(), 0)
	child, err := conway.NewConwayTransactionFromCbor(childCbor)
	require.NoError(t, err)
	baseInput := parent.Inputs()[0]
	baseKey := utxoref.ForInput(baseInput)

	selected, _, err := selectValidLeiosTransactions(
		[]MempoolTransaction{
			{Hash: "not-hex", Cbor: parentCbor, Type: conway.TxTypeConway},
			{
				Hash: child.Hash().String(),
				Cbor: childCbor,
				Type: conway.TxTypeConway,
			},
		},
		&leiosOverlayValidator{base: map[utxoref.Key]struct{}{baseKey: {}}},
		leiosSelectionLimits{},
	)
	require.NoError(t, err)
	require.Empty(t, selected)
}

// TestBuildLeiosEBReferencesUseFullTransactionHash verifies that buildLeiosEB
// content-addresses each manifest reference by the hash of the FULL transaction
// CBOR (not the Cardano tx-id / body hash). This is exactly the check the
// fetch-side validator (ouroboros.validateLeiosEndorserBlockTxs) performs —
// Blake2b256(txCbor) == ref.TransactionHash — so a peer fetching a locally
// forged EB validates every tx instead of rejecting it.
func TestBuildLeiosEBReferencesUseFullTransactionHash(t *testing.T) {
	txs := []MempoolTransaction{
		{Hash: strings.Repeat("11", 32), Cbor: []byte{0x01, 0x02, 0x03}},
		{Hash: strings.Repeat("aa", 32), Cbor: []byte{0xde, 0xad, 0xbe, 0xef}},
	}

	ebCbor, _, bodies, err := buildLeiosEB(txs, leiosEBCaps{})
	require.NoError(t, err)

	eb, err := lcommon.NewLeiosEndorserBlockFromCbor(ebCbor)
	require.NoError(t, err)
	require.Len(t, eb.TransactionReferences, len(bodies))
	for i, ref := range eb.TransactionReferences {
		// The same equality validateLeiosEndorserBlockTxs enforces on fetch.
		require.Equalf(
			t,
			lcommon.Blake2b256Hash(bodies[i]),
			ref.TransactionHash,
			"reference %d hash must be Blake2b256 of the full tx CBOR",
			i,
		)
	}

	// Regression guard: the old (buggy) contract hashed the decoded tx.Hash
	// (tx-id / body hash). That value must NOT equal the reference hash now,
	// or a fetching peer would reject every locally forged tx again.
	rawHash, err := hex.DecodeString(txs[0].Hash)
	require.NoError(t, err)
	require.NotEqual(
		t,
		lcommon.NewBlake2b256(rawHash),
		eb.TransactionReferences[0].TransactionHash,
		"reference hash must be the full-tx hash, not the tx-id/body hash",
	)
}

// TestBuildLeiosEBRespectsRefCap bounds the endorser block independently of
// the clock. The deadline is the operative bound in normal operation, but a
// stalled or unavailable slot clock must not let manifest construction
// scale without limit with mempool depth.
func TestBuildLeiosEBRespectsRefCap(t *testing.T) {
	txs := leiosCandidateTxs(t, 10)
	ebCbor, _, bodies, err := buildLeiosEB(txs, leiosEBCaps{maxRefs: 4})
	require.NoError(t, err)
	require.Len(t, bodies, 4)

	eb, err := lcommon.NewLeiosEndorserBlockFromCbor(ebCbor)
	require.NoError(t, err)
	require.Len(t, eb.TransactionReferences, 4)
}

// TestBuildLeiosEBRespectsByteCap bounds total referenced payload rather
// than reference count, which is what actually has to cross the wire.
func TestBuildLeiosEBRespectsByteCap(t *testing.T) {
	txs := leiosCandidateTxs(t, 10)
	// Room for exactly three transactions.
	capBytes := uint64(3 * len(txs[0].Cbor))
	_, _, bodies, err := buildLeiosEB(txs, leiosEBCaps{maxBytes: capBytes})
	require.NoError(t, err)
	require.Len(t, bodies, 3)
}

// TestBuildLeiosEBZeroCapsAreUnlimited keeps the zero value meaning what it
// meant before caps existed.
func TestBuildLeiosEBZeroCapsAreUnlimited(t *testing.T) {
	txs := leiosCandidateTxs(t, 10)
	_, _, bodies, err := buildLeiosEB(txs, leiosEBCaps{})
	require.NoError(t, err)
	require.Len(t, bodies, 10)
}

// TestCheckAndForgeProductionAppliesEBRefCap follows the runtime path from
// the forger config into manifest construction.
func TestCheckAndForgeProductionAppliesEBRefCap(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	leiosCaster := &forgerTestLeiosCaster{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &ebTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		LeiosProduceChecker: &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:  leiosCaster,
		LeiosTxValidator:    &sessionMockTxValidator{},
		LeiosMempool: forgerTestMempoolProvider{
			txs: leiosCandidateTxs(t, 12),
		},
		ForgeEBMaxTxRefs: capPtr(5),
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, leiosCaster.txBodies, 5)
}

// TestBuildLeiosEBBoundsPreallocation keeps the cap a bound on work and
// memory, not only on the emitted manifest. Preallocating for the whole
// mempool would let a deep mempool dictate allocation even when the cap
// admits a handful of references.
func TestBuildLeiosEBBoundsPreallocation(t *testing.T) {
	txs := leiosCandidateTxs(t, 500)
	_, _, bodies, err := buildLeiosEB(txs, leiosEBCaps{maxRefs: 3})
	require.NoError(t, err)
	require.Len(t, bodies, 3)
	require.LessOrEqual(
		t,
		cap(bodies),
		16,
		"capacity must follow the cap, not the mempool depth",
	)
}

// TestBuildLeiosEBBoundsPreallocationByTheByteCap is the same bound on the
// other cap. With references uncapped, a deep mempool would still dictate
// the allocation; every admitted transaction carries at least one byte, so
// the byte cap bounds the reference count too.
func TestBuildLeiosEBBoundsPreallocationByTheByteCap(t *testing.T) {
	txs := leiosCandidateTxs(t, 500)
	// One candidate's worth of bytes: enough to admit a reference, far
	// below the 500 the mempool would otherwise dictate.
	maxBytes := uint64(len(txs[0].Cbor))
	_, _, bodies, err := buildLeiosEB(txs, leiosEBCaps{maxBytes: maxBytes})
	require.NoError(t, err)
	require.NotEmpty(t, bodies)
	require.Less(
		t,
		cap(bodies),
		500,
		"capacity must follow the byte cap, not the mempool depth",
	)
	require.LessOrEqual(t, uint64(cap(bodies)), maxBytes)
}

// TestNewBlockForgerDefaultsTheEBSelectionReserve covers the same embedder
// path for the reserve: a ForgerConfig that never mentions it gets the
// built-in fallback rather than a zero budget, which would leave selection
// no time at all.
func TestNewBlockForgerDefaultsTheEBSelectionReserve(t *testing.T) {
	forger := newCapDefaultsForger(t, nil, nil)
	require.Equal(
		t,
		defaultForgeEBSelectionReserve,
		forger.forgeEBSelectionReserve,
	)
}

// TestNewBlockForgerHonoursAConfiguredEBSelectionReserve is the other
// half, and the last hop of the operator-facing path: yaml/env/CLI ->
// internal/config -> buildDingoConfig -> dingo.Config -> ForgerConfig ->
// this field.
func TestNewBlockForgerHonoursAConfiguredEBSelectionReserve(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block, cbor: block.cbor},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &ebTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		ForgeEBSelectionReserve: 750 * time.Millisecond,
		PromRegistry:            prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	require.Equal(t, 750*time.Millisecond, forger.forgeEBSelectionReserve)
}

// TestNewBlockForgerAppliesEBCapDefaults covers the embedder path: a
// ForgerConfig that never mentions the caps must still get the backstop,
// rather than silently running uncapped because the zero value means
// "disabled".
func TestNewBlockForgerAppliesEBCapDefaults(t *testing.T) {
	forger := newCapDefaultsForger(t, nil, nil)
	require.Equal(t, uint64(defaultForgeEBMaxTxRefs), forger.forgeEBMaxTxRefs)
	require.Equal(t, uint64(defaultForgeEBMaxBytes), forger.forgeEBMaxBytes)
}

// TestNewBlockForgerHonoursExplicitZeroEBCaps is the other half: an
// explicit zero disables the cap and must not be overwritten by the
// default.
func TestNewBlockForgerHonoursExplicitZeroEBCaps(t *testing.T) {
	zero := uint64(0)
	forger := newCapDefaultsForger(t, &zero, &zero)
	require.Zero(t, forger.forgeEBMaxTxRefs)
	require.Zero(t, forger.forgeEBMaxBytes)
}

//go:fix inline
func capPtr(v uint64) *uint64 { return new(v) }

func newCapDefaultsForger(
	t *testing.T,
	refs *uint64,
	bytes *uint64,
) *BlockForger {
	t.Helper()
	block := newForgerTestBlock(10, 2)
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block, cbor: block.cbor},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &ebTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		ForgeEBMaxTxRefs: refs,
		ForgeEBMaxBytes:  bytes,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

// TestForgeEBCapDefaultsArePinned is the ledger/forging half of the
// drift guard. internal/config declares the same two numbers for its
// yaml/env defaults and cannot import this package, so both sides pin the
// literals; a change to one without the other fails here.
func TestForgeEBCapDefaultsArePinned(t *testing.T) {
	require.Equal(t, uint64(20000), defaultForgeEBMaxTxRefs)
	require.Equal(t, uint64(25165824), defaultForgeEBMaxBytes)
	require.Equal(t, 300*time.Millisecond, defaultForgeEBSelectionReserve)
}

// TestEBSelectionBudgetSkipsALateSlot rejects both earlier behaviours: an
// expired budget neither drops the bound (an unbounded re-validation of
// the whole mempool, the defect this change exists to remove) nor buys a
// fresh one. The slot is over, so there is nothing to produce for.
func TestEBSelectionBudgetSkipsALateSlot(t *testing.T) {
	now := time.Now()
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
		slotClock: &ebTestSlotClock{
			currentSlot: 10,
			// The slot ended a second ago.
			slotEnd: now.Add(-time.Second),
		},
	}

	_, expired := forger.ebSelectionBudget(10)
	require.True(t, expired, "a slot that has closed gets no selection")
}

// TestEBSelectionBudgetBoundsWithoutASlotClock closes the other unbounded
// path. Without a clock there is no slot-derived deadline, but a full
// mempool re-validation is no more acceptable without a clock than with
// one, so the minimal fixed budget applies instead.
func TestEBSelectionBudgetBoundsWithoutASlotClock(t *testing.T) {
	now := time.Now()
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
	}

	deadline, expired := forger.ebSelectionBudget(10)
	require.False(t, expired, "an unreadable clock is not an expired slot")
	require.Equal(t, now.Add(300*time.Millisecond), deadline)
}

// TestEBSelectionBudgetBoundsWhenForgingAheadOfTheClock covers the other
// direction of clock disagreement. NextSlotTime would describe an earlier
// slot's boundary, which says nothing about this one, so the fallback
// budget applies rather than an expiry the clock has not reached.
func TestEBSelectionBudgetBoundsWhenForgingAheadOfTheClock(t *testing.T) {
	now := time.Now()
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
		slotClock: &ebTestSlotClock{
			currentSlot: 9,
			slotEnd:     now.Add(-time.Second),
		},
	}

	deadline, expired := forger.ebSelectionBudget(10)
	require.False(t, expired)
	require.Equal(t, now.Add(300*time.Millisecond), deadline)
}

// TestEBSelectionBudgetRejectsASlotBoundaryStraddle is the regression test
// for reading a moving clock twice. NextSlotTime derives the boundary from
// the clock's own current slot, so if the clock crosses into slot 11 while
// the boundary is being read, its answer is slot 11's end -- nearly a full
// extra slot of budget for a forge whose slot is already over. The reading
// is taken on both sides of NextSlotTime and the straddle is rejected.
func TestEBSelectionBudgetRejectsASlotBoundaryStraddle(t *testing.T) {
	now := time.Now()
	clock := &advancingEBTestSlotClock{
		slots: []uint64{10, 11},
		// The boundary the clock hands back belongs to slot 11.
		slotEnd: now.Add(time.Second),
	}
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
		slotClock:               clock,
	}

	_, expired := forger.ebSelectionBudget(10)
	require.True(
		t,
		expired,
		"a boundary read across a slot change must not grant the next slot's budget",
	)
	require.Equal(t, 2, clock.slotReads, "the slot is read on both sides")
}

// advancingEBTestSlotClock returns a different current slot on each read,
// which is what a real clock does when the forge straddles a boundary.
type advancingEBTestSlotClock struct {
	slots     []uint64
	slotReads int
	slotEnd   time.Time
}

func (c *advancingEBTestSlotClock) CurrentSlot() (uint64, error) {
	slot := c.slots[min(c.slotReads, len(c.slots)-1)]
	c.slotReads++
	return slot, nil
}

func (c *advancingEBTestSlotClock) SlotsPerKESPeriod() uint64 { return 100 }

func (c *advancingEBTestSlotClock) ChainTip() ocommon.Point {
	return ocommon.Point{Slot: 9}
}

func (c *advancingEBTestSlotClock) PrimaryChainTip() ocommon.Point {
	return c.ChainTip()
}

func (c *advancingEBTestSlotClock) NextSlotTime() (time.Time, error) {
	return c.slotEnd, nil
}

func (c *advancingEBTestSlotClock) UpstreamTipSlot() uint64 { return 0 }

func (c *advancingEBTestSlotClock) UpstreamSyncStatus() (uint64, bool) {
	return 0, false
}

// TestEBSelectionBudgetShrinksReserveOnShortSlots covers fast-slot
// networks. With a 100ms slot a fixed 300ms reserve puts the deadline
// before the slot even began, which would leave selection with no budget
// at all -- or, before this, unbounded. The reserve never takes more than
// half of what is left.
func TestEBSelectionBudgetShrinksReserveOnShortSlots(t *testing.T) {
	now := time.Now()
	slotEnd := now.Add(100 * time.Millisecond)
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
		slotClock: &ebTestSlotClock{
			currentSlot: 10,
			slotEnd:     slotEnd,
		},
	}

	deadline, expired := forger.ebSelectionBudget(10)
	require.False(t, expired)
	require.Equal(
		t,
		slotEnd.Add(-50*time.Millisecond),
		deadline,
		"the reserve is capped at half the remaining slot",
	)
	require.True(t, deadline.After(now), "selection must still get budget")
}

// TestEBSelectionBudgetUsesFullReserveOnNormalSlots is the negative case:
// on a one-second slot the configured reserve applies unchanged.
func TestEBSelectionBudgetUsesFullReserveOnNormalSlots(t *testing.T) {
	now := time.Now()
	slotEnd := now.Add(time.Second)
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
		slotClock: &ebTestSlotClock{
			currentSlot: 10,
			slotEnd:     slotEnd,
		},
	}

	deadline, expired := forger.ebSelectionBudget(10)
	require.False(t, expired)
	require.Equal(t, slotEnd.Add(-300*time.Millisecond), deadline)
}

// TestCheckAndForgeProductionBoundsEBSelectionWithoutASlotClock closes the
// last unbounded path at the call site. When the slot clock cannot answer
// there is no slot-derived deadline, but a full mempool re-validation is
// no more acceptable without a clock than with one, so the minimal budget
// applies there too.
func TestCheckAndForgeProductionBoundsEBSelectionWithoutASlotClock(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	leiosCaster := &forgerTestLeiosCaster{}
	validator := &sessionMockTxValidator{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block, cbor: block.cbor},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &ebTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			// A slot clock that cannot report the next slot boundary.
			slotEnd: time.Time{},
		},
		LeiosProduceChecker:     &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:      leiosCaster,
		LeiosTxValidator:        validator,
		ForgeEBSelectionReserve: 300 * time.Millisecond,
		LeiosMempool: forgerTestMempoolProvider{
			txs: leiosCandidateTxs(t, 20),
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	fakeNow := time.Now()
	forger.now = func() time.Time { return fakeNow }
	validator.onValidate = func(int) {
		fakeNow = fakeNow.Add(200 * time.Millisecond)
	}

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.NotEmpty(t, leiosCaster.txBodies)
	require.Less(
		t,
		len(leiosCaster.txBodies),
		20,
		"a missing slot clock must not license an unbounded pass",
	)
}

// leiosCandidateTxs builds count mempool transactions that all pass
// validLeiosTransactionReference, so selection cost -- not reference
// filtering -- is what the test observes.
func leiosCandidateTxs(t *testing.T, count int) []MempoolTransaction {
	t.Helper()
	txs := make([]MempoolTransaction, 0, count)
	for i := range count {
		txs = append(txs, MempoolTransaction{
			Hash: fmt.Sprintf("%064x", i+1),
			Cbor: makeMinimalTxCbor(t, byte(i+1), 0),
			Type: conway.TxTypeConway,
		})
	}
	return txs
}

// TestLeiosEBSelectionStopsAtTheDeadline is the endorser-block half of the
// lost-slot defect fixed in ranking-block selection. Endorser-block
// selection re-validated every mempool candidate serially with no clock, so
// on a chain holding ~1000 transactions it spent seconds of a 1-second slot
// before the ranking block was even started. An endorser block with fewer
// references beats one that arrives after its slot.
func TestLeiosEBSelectionStopsAtTheDeadline(t *testing.T) {
	const (
		candidates    = 10
		perTxCost     = 10 * time.Millisecond
		selectionTime = 55 * time.Millisecond
	)
	start := time.Now()
	fakeNow := start
	validator := &sessionMockTxValidator{}
	validator.onValidate = func(int) { fakeNow = fakeNow.Add(perTxCost) }

	selected, truncated, err := selectValidLeiosTransactions(
		leiosCandidateTxs(t, candidates),
		validator,
		leiosSelectionLimits{
			now:      func() time.Time { return fakeNow },
			deadline: start.Add(selectionTime),
		},
	)
	require.NoError(t, err)
	require.True(t, truncated)
	// Checks land at 0, 10, 20, 30, 40 and 50ms; the check at 60ms stops
	// the pass.
	require.Len(t, selected, 6)
	require.Equal(t, 6, validator.validateCalls)
}

// TestLeiosEBSelectionCompletesWithinBudget is the negative case: a pass
// that finishes before the deadline must not report truncation or drop
// candidates.
func TestLeiosEBSelectionCompletesWithinBudget(t *testing.T) {
	validator := &sessionMockTxValidator{}
	selected, truncated, err := selectValidLeiosTransactions(
		leiosCandidateTxs(t, 5),
		validator,
		leiosSelectionLimits{
			now:      time.Now,
			deadline: time.Now().Add(time.Hour),
		},
	)
	require.NoError(t, err)
	require.False(t, truncated)
	require.Len(t, selected, 5)
}

// TestLeiosEBSelectionAbortsWhenSnapshotChanges mirrors the ranking-block
// fix: stillCurrent() was consulted once, after every candidate had already
// been re-validated, so the whole pass was paid for and then discarded.
func TestLeiosEBSelectionAbortsWhenSnapshotChanges(t *testing.T) {
	validator := &sessionMockTxValidator{staleAfterCalls: 1}
	_, _, err := selectValidLeiosTransactions(
		leiosCandidateTxs(t, 10),
		validator,
		leiosSelectionLimits{now: time.Now},
	)
	require.ErrorIs(t, err, errTxValidationSnapshotChanged)
	require.Equal(
		t,
		1,
		validator.validateCalls,
		"selection must stop at the first check after the snapshot moved",
	)
}

// TestLeiosEBSelectionWithoutDeadlineIsUnbounded keeps the zero value
// meaning what it did before: no clock, no bound.
func TestLeiosEBSelectionWithoutDeadlineIsUnbounded(t *testing.T) {
	validator := &sessionMockTxValidator{}
	selected, truncated, err := selectValidLeiosTransactions(
		leiosCandidateTxs(t, 8),
		validator,
		leiosSelectionLimits{},
	)
	require.NoError(t, err)
	require.False(t, truncated)
	require.Len(t, selected, 8)
}

// ebTestSlotClock is forgerTestSlotClock with a controllable slot-end
// instant so a test can place the forge inside or past its slot without
// sleeping.
type ebTestSlotClock struct {
	currentSlot       uint64
	chainTipSlot      uint64
	slotsPerKESPeriod uint64
	slotEnd           time.Time
}

func (c *ebTestSlotClock) CurrentSlot() (uint64, error) {
	return c.currentSlot, nil
}

func (c *ebTestSlotClock) SlotsPerKESPeriod() uint64 {
	return c.slotsPerKESPeriod
}

func (c *ebTestSlotClock) ChainTip() ocommon.Point {
	return ocommon.Point{Slot: c.chainTipSlot}
}

// PrimaryChainTip mirrors the applied tip: the caught-up steady state, so
// these tests observe no ledger backlog and no staleness refusal.
func (c *ebTestSlotClock) PrimaryChainTip() ocommon.Point {
	return c.ChainTip()
}

func (c *ebTestSlotClock) NextSlotTime() (time.Time, error) {
	return c.slotEnd, nil
}

func (c *ebTestSlotClock) UpstreamTipSlot() uint64 { return 0 }

func (c *ebTestSlotClock) UpstreamSyncStatus() (uint64, bool) {
	return 0, false
}

func (c *ebTestSlotClock) ForgeTipSnapshot() (ochainsync.Tip, int) {
	return ochainsync.Tip{
		Point:       c.ChainTip(),
		BlockNumber: c.chainTipSlot,
	}, 5
}

func (c *ebTestSlotClock) PrimaryChainTipRelation(
	point ocommon.Point,
) (ochainsync.Tip, uint64, bool, error) {
	primary := c.PrimaryChainTip()
	depth := uint64(0)
	ancestor := primary.Slot >= point.Slot
	if ancestor && primary.Slot > point.Slot {
		depth = primary.Slot - point.Slot
	}
	return ochainsync.Tip{
		Point:       primary,
		BlockNumber: primary.Slot,
	}, depth, ancestor, nil
}

func (*ebTestSlotClock) UpstreamSyncTip() (ochainsync.Tip, bool) {
	return ochainsync.Tip{}, false
}

// TestCheckAndForgeProductionSkipsEBWhenSlotIsOver pins what a late slot
// does: nothing. The endorser block's hash is committed into the
// ranking-block header, so once the slot has closed, selecting would only
// delay the block that actually extends the chain -- and the endorser
// block is announced by that same ranking block, so a ranking block
// orphaned for being late cannot carry it either. The ranking block is
// still forged and broadcast.
func TestCheckAndForgeProductionSkipsEBWhenSlotIsOver(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	leiosCaster := &forgerTestLeiosCaster{}
	validator := &sessionMockTxValidator{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: &ebTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			// The slot has already ended.
			slotEnd: time.Now(),
		},
		LeiosProduceChecker:     &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:      leiosCaster,
		LeiosTxValidator:        validator,
		ForgeEBSelectionReserve: 300 * time.Millisecond,
		LeiosMempool: forgerTestMempoolProvider{
			txs: leiosCandidateTxs(t, 20),
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Empty(
		t,
		leiosCaster.txBodies,
		"a slot that has closed produces no endorser block",
	)
	require.Zero(
		t,
		validator.validateCalls,
		"a late slot must not re-validate the mempool at all",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.leiosEbSkipped.WithLabelValues("slot_expired"),
		),
	)
	require.Equal(
		t,
		1,
		broadcaster.calls,
		"the ranking block is still forged and broadcast",
	)
}

func ebLogRecord(t *testing.T, logs, msg string) map[string]any {
	t.Helper()
	for line := range strings.SplitSeq(strings.TrimSpace(logs), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if record["msg"] == msg {
			return record
		}
	}
	t.Fatalf("no %q line in logs: %s", msg, logs)
	return nil
}

func histogramSampleCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, h.Write(m))
	return m.GetHistogram().GetSampleCount()
}

func newEBTimingForger(
	t *testing.T,
	logs *bytes.Buffer,
	clock *ebTestSlotClock,
	validator TxValidator,
	caster *forgerTestLeiosCaster,
	broadcaster *forgerTestBroadcaster,
	txs []MempoolTransaction,
) *BlockForger {
	t.Helper()
	block := newForgerTestBlock(10, 2)
	forger, err := NewBlockForger(ForgerConfig{
		Mode:                ModeProduction,
		Logger:              slog.New(slog.NewJSONHandler(logs, nil)),
		Credentials:         setupTestCredentials(t),
		LeaderChecker:       forgerTestLeader{},
		BlockBuilder:        &forgerTestBuilder{block: block, cbor: block.cbor},
		BlockBroadcaster:    broadcaster,
		SlotClock:           clock,
		LeiosProduceChecker: &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:  caster,
		LeiosTxValidator:    validator,
		LeiosMempool:        forgerTestMempoolProvider{txs: txs},
		PromRegistry:        prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

// TestLeiosEBProducedLineCarriesTimingBreakdown makes endorser-block
// construction legible from the node's own logs. The field trace behind
// this change had to be reconstructed from a 3.5-second gap between log
// lines, because nothing recorded how long selection took.
func TestLeiosEBProducedLineCarriesTimingBreakdown(t *testing.T) {
	var logs bytes.Buffer
	forger := newEBTimingForger(
		t,
		&logs,
		&ebTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		&sessionMockTxValidator{},
		&forgerTestLeiosCaster{},
		&forgerTestBroadcaster{},
		leiosCandidateTxs(t, 4),
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	record := ebLogRecord(t, logs.String(), "leios endorser block produced")
	for _, key := range []string{
		"eb_select",
		"eb_build",
		"eb_broadcast",
		"candidates",
	} {
		require.Contains(t, record, key)
	}
	require.Equal(t, float64(4), record["tx_refs"])
	require.Equal(
		t,
		uint64(1),
		histogramSampleCount(t, forger.metrics.leiosEbSelectionSeconds),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.leiosEbSelectionTruncated),
	)
}

// TestLeiosEBSelectionTruncationIsCounted follows the runtime composition
// path -- checkAndForgeProduction -> checkAndForgeLeiosEB ->
// selectValidLeiosTransactions -- and proves that the slot deadline
// reaches the pass that spends the slot, that operators get the signal
// that the slot budget (not the mempool) decided endorser-block size, and
// that the ranking block is still forged. A deadline that exists in the
// forger but never arrives at selection bounds nothing.
func TestLeiosEBSelectionTruncationIsCounted(t *testing.T) {
	var logs bytes.Buffer
	validator := &sessionMockTxValidator{}
	caster := &forgerTestLeiosCaster{}
	broadcaster := &forgerTestBroadcaster{}
	forger := newEBTimingForger(
		t,
		&logs,
		&ebTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Second),
		},
		validator,
		caster,
		broadcaster,
		leiosCandidateTxs(t, 10),
	)
	// Every candidate consumes a tenth of a second of the budget, so the
	// pass cannot get through all ten inside the slot.
	fakeNow := time.Now()
	forger.now = func() time.Time { return fakeNow }
	validator.onValidate = func(int) {
		fakeNow = fakeNow.Add(100 * time.Millisecond)
	}

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.NotEmpty(t, caster.txBodies)
	require.Less(
		t,
		len(caster.txBodies),
		10,
		"endorser-block selection must stop when the slot budget is gone",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.leiosEbSelectionTruncated),
	)
	require.Equal(
		t,
		uint64(1),
		histogramSampleCount(t, forger.metrics.leiosEbSelectionSeconds),
	)
	require.Equal(
		t,
		1,
		broadcaster.calls,
		"the ranking block must still be forged and broadcast",
	)
}

// opCertSequenceGateForger builds a production BlockForger whose credentials'
// OpCert issue number is the fixed value baked into testOpCertJSON (0, see
// TestValidateOpCertUnsafe_ValidCertificate), wired with the given ledger
// view / era params so tests can drive the pre-flight counter gate. The
// caller retains leader so it can assert whether leader selection ran.
func opCertSequenceGateForger(
	t *testing.T,
	view LedgerView,
	eraParams ProtocolParamsProvider,
	leader LeaderChecker,
	builder *forgerTestBuilder,
	broadcaster *forgerTestBroadcaster,
	logs *bytes.Buffer,
) *BlockForger {
	t.Helper()
	creds := setupTestCredentials(t)
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(logs, nil)),
		Credentials:      creds,
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       1,
			chainTipSlot:      0,
			slotsPerKESPeriod: 100,
		},
		OpCertLedgerView: view,
		EraParams:        eraParams,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

func newOpCertSequenceGateTestBuilder() (*forgerTestBuilder, *forgerTestBroadcaster) {
	block := newForgerTestBlock(0, 1)
	return &forgerTestBuilder{block: block, cbor: block.cbor},
		&forgerTestBroadcaster{}
}

// TestNewBlockForgerRequiresEraParamsWithOpCertLedgerView pins the
// construction-time contract: the pre-flight counter check cannot resolve
// the era-scoped rule (no-gap vs. stale-only) without EraParams, so wiring
// one provider without the other must fail closed at startup rather than at
// the first forge attempt.
func TestNewBlockForgerRequiresEraParamsWithOpCertLedgerView(t *testing.T) {
	creds := setupTestCredentials(t)
	_, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock:        forgerTestSlotClock{slotsPerKESPeriod: 1},
		OpCertLedgerView: &fakeLedgerView{},
	})
	require.ErrorContains(t, err, "EraParams")
}

// TestCheckAndForgeProductionSkipsOnStaleOpCertCounter covers a stale
// counter -- the ledger has observed a higher issue number for this pool
// than the loaded credentials carry -- which block application would
// reject regardless of era. It must be caught after leader selection (so
// the ledger read it performs is skipped for slots this pool does not
// lead) but before block build or the forge-slot fence.
func TestCheckAndForgeProductionSkipsOnStaleOpCertCounter(t *testing.T) {
	builder, broadcaster := newOpCertSequenceGateTestBuilder()
	leader := &forgerCountingLeader{}
	var logs bytes.Buffer
	view := &fakeLedgerView{seqFound: true, latestSeq: 1}
	eraParams := &mockPParamsProvider{
		pparams: &babbage.BabbageProtocolParameters{},
	}
	forger := opCertSequenceGateForger(
		t, view, eraParams, leader, builder, broadcaster, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		leader.callCount(),
		"leader selection must run before the counter check",
	)
	require.Zero(t, builder.calls, "stale counter must not build a block")
	require.Zero(
		t,
		broadcaster.calls,
		"stale counter must not adopt a block",
	)
	require.Contains(t, logs.String(), "operational certificate counter")
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

// TestCheckAndForgeProductionAllowsUnobservedOpCertCounter covers the
// baseline case: the ledger has never observed a counter for this pool
// (fresh registration, or a pool absent from a Mithril-restored certified
// counter map), so the counter is judged against zero and the fixture's
// counter 0 is accepted.
func TestCheckAndForgeProductionAllowsUnobservedOpCertCounter(t *testing.T) {
	builder, broadcaster := newOpCertSequenceGateTestBuilder()
	var logs bytes.Buffer
	view := &fakeLedgerView{seqFound: false}
	eraParams := &mockPParamsProvider{
		pparams: &babbage.BabbageProtocolParameters{},
	}
	forger := opCertSequenceGateForger(
		t,
		view,
		eraParams,
		&forgerCountingLeader{},
		builder,
		broadcaster,
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	require.Equal(t, 1, broadcaster.calls)
}

// TestCheckAndForgeProductionSkipsWhenEraUnresolvable covers a provider
// that cannot yet supply protocol parameters for the slot (e.g. very early
// startup). The gate fails closed rather than guessing an era-scoped rule.
func TestCheckAndForgeProductionSkipsWhenEraUnresolvable(t *testing.T) {
	builder, broadcaster := newOpCertSequenceGateTestBuilder()
	leader := &forgerCountingLeader{}
	var logs bytes.Buffer
	view := &fakeLedgerView{seqFound: true, latestSeq: 0}
	eraParams := &mockPParamsProvider{pparams: nil}
	forger := opCertSequenceGateForger(
		t, view, eraParams, leader, builder, broadcaster, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		leader.callCount(),
		"leader selection must run before the counter check",
	)
	require.Zero(
		t,
		builder.calls,
		"an unresolvable era must not build a block",
	)
	require.Zero(t, broadcaster.calls)
}

// TestCheckAndForgeProductionSkipsOnTransientLedgerReadError covers a
// transient database error from LatestOpCertSequence itself (as opposed to
// a definite stale/gapped counter). A DB hiccup on a leader slot costs a
// could-not-forge disposition for that slot rather than forging with an
// unverified key state; the slot is not adopted either way, so failing
// closed here does not risk a block the chain would reject.
func TestCheckAndForgeProductionSkipsOnTransientLedgerReadError(t *testing.T) {
	builder, broadcaster := newOpCertSequenceGateTestBuilder()
	leader := &forgerCountingLeader{}
	var logs bytes.Buffer
	view := &fakeLedgerView{seqErr: errors.New("transient database error")}
	eraParams := &mockPParamsProvider{
		pparams: &babbage.BabbageProtocolParameters{},
	}
	forger := opCertSequenceGateForger(
		t, view, eraParams, leader, builder, broadcaster, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		leader.callCount(),
		"leader selection must run before the counter check",
	)
	require.Zero(t, builder.calls, "a lookup error must not build a block")
	require.Zero(t, broadcaster.calls)
	require.Contains(t, logs.String(), "opcert sequence lookup")
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

// TestCheckAndForgeProductionSkipsOnTypedNilProtocolParameters covers a
// ProtocolParamsForSlot implementation that returns a typed-nil pointer of
// a known era's type (e.g. a lookup miss represented as
// (*babbage.BabbageProtocolParameters)(nil)) rather than a true nil
// interface. This is the case extractPParamsLimits' reflect-based guard
// exists for; TestCheckAndForgeProductionSkipsWhenEraUnresolvable above
// only exercises the plain-nil-interface half of that guard.
func TestCheckAndForgeProductionSkipsOnTypedNilProtocolParameters(
	t *testing.T,
) {
	builder, broadcaster := newOpCertSequenceGateTestBuilder()
	leader := &forgerCountingLeader{}
	var logs bytes.Buffer
	view := &fakeLedgerView{seqFound: true, latestSeq: 0}
	var nilPParams *babbage.BabbageProtocolParameters
	eraParams := &mockPParamsProvider{pparams: nilPParams}
	forger := opCertSequenceGateForger(
		t, view, eraParams, leader, builder, broadcaster, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		leader.callCount(),
		"leader selection must run before the counter check",
	)
	require.Zero(
		t,
		builder.calls,
		"a typed-nil protocol parameters value must not build a block",
	)
	require.Zero(t, broadcaster.calls)
}

// TestCheckAndForgeProductionEraScopedOpCertCounterRule covers the era-
// scoped no-gap rule end to end through the running forger: TPraos
// (Shelley-Alonzo) accepts a counter that jumps ahead of the last observed
// value, Praos (Babbage onward) does not. Boundary values (exactly one
// ahead, exactly equal) are covered for both eras.
func TestCheckAndForgeProductionEraScopedOpCertCounterRule(t *testing.T) {
	tests := []struct {
		name       string
		pparams    lcommon.ProtocolParameters
		candidate  uint64
		stored     uint64
		wantForged bool
	}{
		{
			name:       "tpraos era equal to last seen",
			pparams:    &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
			candidate:  5,
			stored:     5,
			wantForged: true,
		},
		{
			name:       "tpraos era exactly one ahead (boundary)",
			pparams:    &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
			candidate:  6,
			stored:     5,
			wantForged: true,
		},
		{
			name:       "tpraos era gap of two accepted (era change from praos)",
			pparams:    &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
			candidate:  7,
			stored:     5,
			wantForged: true,
		},
		{
			name:       "praos era exactly one ahead (boundary)",
			pparams:    &babbage.BabbageProtocolParameters{},
			candidate:  6,
			stored:     5,
			wantForged: true,
		},
		{
			name:       "praos era gap of two rejected (era change from tpraos)",
			pparams:    &babbage.BabbageProtocolParameters{},
			candidate:  7,
			stored:     5,
			wantForged: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder, broadcaster := newOpCertSequenceGateTestBuilder()
			leader := &forgerCountingLeader{}
			var logs bytes.Buffer
			view := &fakeLedgerView{seqFound: true, latestSeq: tt.stored}
			eraParams := &mockPParamsProvider{pparams: tt.pparams}
			forger := opCertSequenceGateForger(
				t, view, eraParams, leader, builder, broadcaster, &logs,
			)
			forger.creds.mu.Lock()
			forger.creds.opCert.IssueNumber = tt.candidate
			forger.creds.mu.Unlock()

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			require.Equal(
				t,
				1,
				leader.callCount(),
				"leader selection must run before the counter check",
			)
			if tt.wantForged {
				require.Equal(t, 1, builder.calls)
				require.Equal(t, 1, broadcaster.calls)
			} else {
				require.Zero(t, builder.calls)
				require.Zero(t, broadcaster.calls)
				require.Contains(
					t,
					logs.String(),
					"operational certificate counter",
				)
			}
		})
	}
}

// TestCheckAndForgeProductionSkipsOpCertSequenceCheckWhenLedgerViewNil
// confirms the pre-flight counter gate is opt-in: embedders and dev-mode
// wiring that leave OpCertLedgerView nil (every other forger test in this
// package) are unaffected by this change.
func TestCheckAndForgeProductionSkipsOpCertSequenceCheckWhenLedgerViewNil(
	t *testing.T,
) {
	builder, broadcaster := newOpCertSequenceGateTestBuilder()
	var logs bytes.Buffer
	forger := opCertSequenceGateForger(
		t, nil, nil, &forgerCountingLeader{}, builder, broadcaster, &logs,
	)
	// A counter far ahead of any plausible on-chain value would be rejected
	// under the gate if it were active; with no LedgerView wired it is not
	// evaluated at all.
	forger.creds.mu.Lock()
	forger.creds.opCert.IssueNumber = 1000
	forger.creds.mu.Unlock()

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	require.Equal(t, 1, broadcaster.calls)
}

// TestCheckAndForgeProductionUnobservedOpCertCounterUsesZeroBaseline covers a
// pool with no counter observed on chain through the running forger. The
// reference's baseline for such a pool is zero (Praos currentIssueNo), so a
// Praos-era slot is forged for counters 0 and 1 and declined for 2, which
// block application would reject as over-incremented. TPraos checks
// monotonicity only and forges any counter.
func TestCheckAndForgeProductionUnobservedOpCertCounterUsesZeroBaseline(
	t *testing.T,
) {
	tests := []struct {
		name       string
		pparams    lcommon.ProtocolParameters
		candidate  uint64
		wantForged bool
	}{
		{
			name:       "praos era counter zero",
			pparams:    &babbage.BabbageProtocolParameters{},
			candidate:  0,
			wantForged: true,
		},
		{
			name:       "praos era counter one",
			pparams:    &babbage.BabbageProtocolParameters{},
			candidate:  1,
			wantForged: true,
		},
		{
			name:       "praos era counter two declined",
			pparams:    &babbage.BabbageProtocolParameters{},
			candidate:  2,
			wantForged: false,
		},
		{
			name:       "tpraos era large counter",
			pparams:    &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
			candidate:  490,
			wantForged: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder, broadcaster := newOpCertSequenceGateTestBuilder()
			var logs bytes.Buffer
			view := &fakeLedgerView{seqFound: false}
			eraParams := &mockPParamsProvider{pparams: tt.pparams}
			forger := opCertSequenceGateForger(
				t,
				view,
				eraParams,
				&forgerCountingLeader{},
				builder,
				broadcaster,
				&logs,
			)
			forger.creds.mu.Lock()
			forger.creds.opCert.IssueNumber = tt.candidate
			forger.creds.mu.Unlock()

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			if tt.wantForged {
				require.Equal(t, 1, builder.calls)
				require.Equal(t, 1, broadcaster.calls)
				return
			}
			require.Zero(t, builder.calls)
			require.Zero(t, broadcaster.calls)
			require.Contains(t, logs.String(), "skips ahead of last seen 0")
		})
	}
}

// identityColdVKey and identitySignature are the edwards25519 identity point
// and the signature (R = identity, S = 0) that crypto/ed25519.Verify accepts
// against it for any message: the equation [S]B == R + [h]A reduces to
// identity == identity. libsodium's crypto_sign_ed25519_verify_detached, which
// cardano-node reaches through Ed25519DSIGN, rejects both points as
// small-order.
var (
	identityColdVKey  = append([]byte{0x01}, make([]byte, 31)...)
	identitySignature = append(
		append([]byte{0x01}, make([]byte, 31)...),
		make([]byte, 32)...,
	)
)

func strictTestKESVKey() []byte {
	kesVKey := make([]byte, 32)
	for i := range kesVKey {
		kesVKey[i] = byte(i)
	}
	return kesVKey
}

// TestValidateOpCertAcceptsGenuineColdSignature keeps the rejection below
// conditional: a certificate whose cold key really did sign its own hot vkey,
// counter and period still validates.
func TestValidateOpCertAcceptsGenuineColdSignature(t *testing.T) {
	t.Parallel()

	kesVKey := strictTestKESVKey()
	coldVKey, coldSKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	pc := NewPoolCredentials()
	pc.kesVKey = kesVKey
	pc.opCert = &OpCert{
		KESVKey:     kesVKey,
		IssueNumber: 7,
		KESPeriod:   3,
		ColdVKey:    coldVKey,
		Signature: ed25519.Sign(
			coldSKey,
			lcommon.OpCertSignableBytes(kesVKey, 7, 3),
		),
	}

	require.NoError(t, pc.ValidateOpCert())
}

// TestValidateOpCertRejectsSmallOrderColdKey pins the criteria, not merely the
// presence, of the cold-signature check. The identity cold vkey with an
// all-zero S is accepted by crypto/ed25519 for any hot vkey, counter and
// period, so a node validating with it passes its own startup check and then
// forges blocks whose opcert every conformant peer rejects.
func TestValidateOpCertRejectsSmallOrderColdKey(t *testing.T) {
	t.Parallel()

	kesVKey := strictTestKESVKey()
	signable := lcommon.OpCertSignableBytes(kesVKey, 7, 3)
	require.True(
		t,
		ed25519.Verify(identityColdVKey, signable, identitySignature),
		"crypto/ed25519 no longer accepts the identity pair; this test no longer discriminates",
	)

	pc := NewPoolCredentials()
	pc.kesVKey = kesVKey
	pc.opCert = &OpCert{
		KESVKey:     kesVKey,
		IssueNumber: 7,
		KESPeriod:   3,
		ColdVKey:    identityColdVKey,
		Signature:   identitySignature,
	}

	require.ErrorContains(
		t,
		pc.ValidateOpCert(),
		"signature verification failed",
	)
}

// decodeCborArrayElementUint decodes one element of a CBOR array as an
// unsigned integer, without going through either header body struct. The
// point of these tests is what dingo puts on the wire, so reading it back
// through the same struct that wrote it would assert nothing.
func decodeCborArrayElementUint(
	t *testing.T,
	encoded []byte,
	index int,
) uint64 {
	t.Helper()
	var elements []cbor.RawMessage
	_, err := cbor.Decode(encoded, &elements)
	require.NoError(t, err)
	require.Greater(t, len(elements), index)
	var value uint64
	// require.Greater above fails the test unless len(elements) > index,
	// which nilaway does not model.
	//nolint:nilaway // bounded by the require.Greater above
	_, err = cbor.Decode(elements[index], &value)
	require.NoError(t, err)
	return value
}

// TestTPraosHeaderBodyEncodesOpCertBeyondUint32 pins the width of the
// operational certificate fields dingo writes into a TPraos-era header body.
//
// cardano-ledger decodes the counter as Word64 and the KES period as
// KESPeriod{Word} with no bound, and the CDDL declares both uint .size 8, so
// a header body whose fields are uint32 truncates a certificate the chain
// accepts. The counter and period here are one past math.MaxUint32, which is
// the first value a uint32 field cannot hold.
func TestTPraosHeaderBodyEncodesOpCertBeyondUint32(t *testing.T) {
	const (
		sequenceNumber = uint64(math.MaxUint32) + 1
		kesPeriod      = uint64(math.MaxUint32) + 7
	)
	body := tpraosHeaderBody{
		BlockNumber:          101,
		Slot:                 1001,
		IssuerVkey:           lcommon.IssuerVkey{},
		VrfKey:               make([]byte, 32),
		BlockBodySize:        4,
		BlockBodyHash:        lcommon.Blake2b256{},
		OpCertHotVkey:        make([]byte, 32),
		OpCertSequenceNumber: sequenceNumber,
		OpCertKesPeriod:      kesPeriod,
		OpCertSignature:      make([]byte, 64),
		ProtoMajorVersion:    2,
		ProtoMinorVersion:    0,
	}
	encoded, err := cbor.Encode(body)
	require.NoError(t, err)

	// The TPraos header body is a flat 15-element array; the operational
	// certificate occupies elements 9 through 12.
	assert.Equal(
		t,
		sequenceNumber,
		decodeCborArrayElementUint(t, encoded, 10),
	)
	assert.Equal(
		t,
		kesPeriod,
		decodeCborArrayElementUint(t, encoded, 11),
	)
}

// TestPraosOpCertEncodesCounterBeyondUint32 is the same contract for the
// nested operational_cert array a Praos-era header body carries.
func TestPraosOpCertEncodesCounterBeyondUint32(t *testing.T) {
	const (
		sequenceNumber = uint64(math.MaxUint32) + 1
		kesPeriod      = uint64(math.MaxUint32) + 7
	)
	encoded, err := cbor.Encode(praosOpCert{
		HotVkey:        make([]byte, 32),
		SequenceNumber: sequenceNumber,
		KesPeriod:      kesPeriod,
		Signature:      make([]byte, 64),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		sequenceNumber,
		decodeCborArrayElementUint(t, encoded, 1),
	)
	assert.Equal(
		t,
		kesPeriod,
		decodeCborArrayElementUint(t, encoded, 2),
	)
}

// TestBuildBlockDoesNotNarrowOpCertCounterAtUint32 exercises the assignment
// rather than the struct: a counter one past math.MaxUint32 reaches the
// encoder intact, so no bound of dingo's stands between the certificate and
// the forged header.
//
// buildBlock re-decodes the block it encoded, so the assertion depends on
// gouroboros decoding the field at full width: the module is pinned past
// gouroboros, which widened
// shelley.ShelleyBlockHeaderBody.OpCertSequenceNumber from uint32 to
// uint64, so the re-decode now returns a block whose header carries the
// full counter rather than reporting an upstream overflow.
func TestBuildBlockDoesNotNarrowOpCertCounterAtUint32(t *testing.T) {
	const counter = uint64(math.MaxUint32) + 1
	creds := setupTestCredentials(t)
	creds.opCert.IssueNumber = counter

	builder := newTPraosTestBuilder(t, creds)
	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	header, ok := block.Header().(*shelley.ShelleyBlockHeader)
	require.True(t, ok, "TPraos forge must return a Shelley header")
	assert.Equal(
		t,
		counter,
		uint64(header.Body.OpCertSequenceNumber),
	)
}

// TestBuildBlockRejectsOpCertCounterAbovePersistableBound covers the other
// end: a counter the reference accepts but this node cannot record is
// refused before the leader slot is spent, naming the bound, rather than
// being forged and then failing its own block apply.
func TestBuildBlockRejectsOpCertCounterAbovePersistableBound(t *testing.T) {
	creds := setupTestCredentials(t)
	creds.opCert.IssueNumber = eras.MaxPersistableOpCertCounter + 1

	builder := newTPraosTestBuilder(t, creds)
	_, _, err := builder.BuildBlock(1001, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pool_opcert_sequence")
	assert.NotContains(t, err.Error(), "exceeds uint32 max")
}

// TestValidateOpCertSequenceRejectsCounterAbovePersistableBound pins the
// forge loop's pre-flight against the same bound block application applies,
// so the two cannot disagree about which counters are forgeable.
func TestValidateOpCertSequenceRejectsCounterAbovePersistableBound(
	t *testing.T,
) {
	require.NoError(
		t,
		validateOpCertSequence(5, true, uint64(math.MaxInt64), false),
	)
	err := validateOpCertSequence(
		5,
		true,
		eras.MaxPersistableOpCertCounter+1,
		false,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pool_opcert_sequence")
}

func newTPraosTestBuilder(
	t *testing.T,
	creds *PoolCredentials,
) *DefaultBlockBuilder {
	t.Helper()
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{transactions: []MempoolTransaction{}},
		PParamsProvider: &mockPParamsProvider{
			pparams: &shelley.ShelleyProtocolParameters{
				MaxTxSize:        16384,
				MaxBlockBodySize: 90112,
				ProtocolMajor:    2,
			},
		},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)
	return builder
}
