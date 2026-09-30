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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stageInFlightBlockfetchBatch puts the fixture in the state that exists while
// a blockfetch batch requested for the rival's continuation is in flight: the
// batch carries the rollback generation current at request time, and one
// fetched block has been delivered and is waiting to be flushed onto the
// chain.
//
// The block is delivered through the real arrival handler rather than pushed
// onto the pending slice, so the test covers what arrival itself is allowed to
// conclude. A range-failure record is seeded for that exact range first, as
// noteBlockfetchRangeUnavailable would leave it after a miss: arrival must not
// clear it, because a delivered block that is never applied leaves the queued
// header just as stuck as one that was never delivered.
func stageInFlightBlockfetchBatch(
	t *testing.T,
	ls *LedgerState,
	block gledger.Block,
) {
	t.Helper()
	point := ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
	ls.blockfetchBatchRollbackGeneration = ls.blockfetchRollbackGeneration.Load()
	ls.blockfetchRangeFailure = blockfetchRangeFailureState{
		slot:  point.Slot,
		hash:  string(point.Hash),
		count: 1,
	}
	connId := testRecycleConnId()
	ls.activeBlockfetchConnId = connId
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	previousMithrilLedgerSlot := ls.mithrilLedgerSlot
	ls.mithrilLedgerSlot = point.Slot
	defer func() {
		ls.mithrilLedgerSlot = previousMithrilLedgerSlot
	}()
	require.NoError(t, handleEventBlockfetchBlockDeferred(ls,
		BlockfetchEvent{
			ConnectionId: connId,
			Block:        block,
			Point:        point,
			Type:         uint(block.Type()),
		},
		nil,
	))
	require.Len(t, ls.pendingBlockfetchEvents, 1)
	require.Equal(
		t,
		1,
		ls.blockfetchRangeFailure.count,
		"arrival is not range progress: the block has not been applied yet",
	)
}

// TestStaleBlockfetchBatchIsDiscardedAfterLocalSiblingAdoption covers the race
// between an equal-slot alternative winning chain selection and a blockfetch
// batch that was already in flight for the chain the alternative displaces.
//
// The adoption rolls the rival off the tip while that batch is still being
// delivered, and delivery runs under chainsyncBlockfetchMutex rather than the
// chainsyncMutex the adoption holds, so the flush can land at any point during
// or after the truncation. Its blocks were fetched for headers descending from
// the rival -- the segment the rollback abandoned -- and must not be applied.
//
// The chainsync rollback paths supersede such a batch by re-queueing the
// winning fork's headers and restarting blockfetch. This path does neither, so
// the batch is invalidated by generation: the rollback publishes a newer one
// before truncating, and the flush drops anything still carrying the old one.
func TestStaleBlockfetchBatchIsDiscardedAfterLocalSiblingAdoption(
	t *testing.T,
) {
	// The block the in-flight batch is delivering: the rival's continuation.
	newContinuation := func(t *testing.T, f *siblingFixture) gledger.Block {
		t.Helper()
		return newSiblingTestBlock(
			t,
			f.rival.BlockNumber()+1,
			f.rival.SlotNumber()+10,
			f.rival.Hash(),
			0x44,
			0x44,
			1,
		)
	}

	t.Run("control: the batch lands with no rollback", func(t *testing.T) {
		f := newSiblingFixture(t)
		continuation := newContinuation(t, f)
		stageInFlightBlockfetchBatch(t, f.ls, continuation)

		require.NoError(t, f.ls.flushPendingBlockfetchBlocksDeferred(nil))
		assert.Equal(
			t,
			continuation.Hash().Bytes(),
			f.ls.chain.Tip().Point.Hash,
			"without a rollback the fetched block extends the chain",
		)
		assert.Zero(
			t,
			f.ls.blockfetchRangeFailure.count,
			"a block that extends the chain clears the range failure record",
		)
	})

	t.Run("stale batch after the alternative wins", func(t *testing.T) {
		f := newSiblingFixture(t)
		continuation := newContinuation(t, f)
		stageInFlightBlockfetchBatch(t, f.ls, continuation)
		before := f.ls.blockfetchRollbackGeneration.Load()

		// Lower VRF output wins, so the alternative replaces the rival.
		ours := f.newSibling(t, siblingRivalVrfSeed-1)
		adopted, err := f.ls.AdoptLocalForgedSibling(ours)
		require.NoError(t, err)
		require.True(t, adopted)
		require.Greater(
			t,
			f.ls.blockfetchRollbackGeneration.Load(),
			before,
			"the adoption must publish a new rollback generation",
		)

		// The batch flushes only now, after the rollback and the adoption.
		require.NoError(t, f.ls.flushPendingBlockfetchBlocksDeferred(nil))

		assert.Equal(
			t,
			ours.Hash().Bytes(),
			f.ls.chain.Tip().Point.Hash,
			"a superseded batch must not move the tip off the adopted alternative",
		)
		assert.Empty(
			t,
			f.ls.pendingBlockfetchEvents,
			"the discarded batch must not be left queued for a later flush",
		)
		assert.Equal(
			t,
			1,
			f.ls.blockfetchRangeFailure.count,
			"a discarded block is not range progress: clearing the record "+
				"would reset the count that unsticks the queued header "+
				"blocking local forging",
		)
	})

	t.Run("stale batch inside the rollback window", func(t *testing.T) {
		// The window the generation exists for. The adoption publishes
		// the generation before it truncates, and the flush holds a
		// different mutex, so it can land while the chain is still the
		// one the batch was fetched for. Here the truncation is refused
		// after the generation is published (the fork point is below the
		// Mithril boundary), which leaves the chain at the rival with
		// the batch's block still fitting its tip perfectly.
		//
		// Without the generation the block lands and re-extends a chain
		// the node has already decided to abandon; the tip-fit check
		// cannot see anything wrong with it.
		f := newSiblingFixture(t)
		continuation := newContinuation(t, f)
		stageInFlightBlockfetchBatch(t, f.ls, continuation)
		f.ls.mithrilLedgerSlot = siblingParentSlot + 1
		before := f.ls.blockfetchRollbackGeneration.Load()

		ours := f.newSibling(t, siblingRivalVrfSeed-1)
		adopted, err := f.ls.AdoptLocalForgedSibling(ours)
		require.ErrorIs(t, err, ErrRollbackExceedsMithrilBoundary)
		require.False(t, adopted)
		require.Greater(
			t,
			f.ls.blockfetchRollbackGeneration.Load(),
			before,
			"the generation must be published before the truncation",
		)
		require.Equal(
			t,
			f.rival.Hash().Bytes(),
			f.ls.chain.Tip().Point.Hash,
			"precondition: the chain still holds the rival, so the "+
				"batch's block fits its tip",
		)

		require.NoError(t, f.ls.flushPendingBlockfetchBlocksDeferred(nil))
		assert.Equal(
			t,
			f.rival.Hash().Bytes(),
			f.ls.chain.Tip().Point.Hash,
			"a batch superseded by a published rollback generation must "+
				"not extend the chain even when its block still fits",
		)
		assert.Equal(
			t,
			1,
			f.ls.blockfetchRangeFailure.count,
			"a discarded block is not range progress",
		)
	})

	t.Run("rollback lands after the fast path passes", func(t *testing.T) {
		// The window the early generation test cannot cover. That test
		// releases nothing, but it holds nothing either: the adoption
		// that publishes a generation and truncates holds chainsyncMutex
		// while this drain holds chainsyncBlockfetchMutex, so a rollback
		// can land between the drain testing its batch and the chain add
		// taking the chain mutex.
		//
		// Expressed sequentially -- fast path would pass, generation then
		// moves, add is attempted -- because the guarantee under test is
		// that the same predicate is re-evaluated at add time. That it is
		// evaluated under the chain mutex, and so cannot itself be raced,
		// is pinned by
		// TestAddBlockWithPointDeferredIfEvaluatesAdmitUnderTheChainMutex.
		f := newSiblingFixture(t)
		continuation := newContinuation(t, f)
		point := ocommon.NewPoint(
			continuation.SlotNumber(),
			continuation.Hash().Bytes(),
		)
		f.ls.blockfetchBatchRollbackGeneration = f.ls.blockfetchRollbackGeneration.Load()
		require.True(
			t,
			f.ls.blockfetchBatchStillCurrent(),
			"precondition: the drain's own fast path would admit this batch",
		)

		// The rollback publishes its generation, as AdoptLocalForgedSibling
		// does before it truncates.
		f.ls.blockfetchRollbackGeneration.Add(1)

		// The block still fits the tip perfectly, so nothing the chain
		// checks for itself can reject it.
		require.Equal(
			t,
			f.rival.Hash().Bytes(),
			f.ls.chain.Tip().Point.Hash,
		)
		_, err := f.ls.chain.AddBlockWithPointDeferredIf(
			continuation,
			point,
			nil,
			f.ls.blockfetchBatchStillCurrent,
		)
		require.ErrorIs(t, err, chain.ErrBlockAddNotAdmitted)
		assert.Equal(
			t,
			f.rival.Hash().Bytes(),
			f.ls.chain.Tip().Point.Hash,
			"a batch superseded after the fast path must still be refused "+
				"at the moment of the add",
		)
	})

	t.Run("stale batch after the alternative loses", func(t *testing.T) {
		// A losing alternative performs no rollback, so the batch it
		// raced is still valid and must still land. Invalidating on the
		// attempt rather than on the truncation would stall the chain
		// every time a slot battle is lost.
		f := newSiblingFixture(t)
		continuation := newContinuation(t, f)
		stageInFlightBlockfetchBatch(t, f.ls, continuation)
		before := f.ls.blockfetchRollbackGeneration.Load()

		// Higher VRF output loses.
		ours := f.newSibling(t, siblingRivalVrfSeed+1)
		adopted, err := f.ls.AdoptLocalForgedSibling(ours)
		require.NoError(t, err)
		require.False(t, adopted)
		require.Equal(
			t,
			before,
			f.ls.blockfetchRollbackGeneration.Load(),
			"a lost slot battle truncates nothing and must not invalidate a batch",
		)

		require.NoError(t, f.ls.flushPendingBlockfetchBlocksDeferred(nil))
		assert.Equal(
			t,
			continuation.Hash().Bytes(),
			f.ls.chain.Tip().Point.Hash,
			"the batch the losing alternative raced must still land",
		)
	})
}

func siblingTestBytes(size int, seed byte) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = seed
	}
	return b
}

// newSiblingTestBlock builds a wire-decodable Conway block with a chosen
// issuer, opcert sequence number and VRF output, so the Praos select view the
// chain-selection comparison reads is real rather than synthesized.
func newSiblingTestBlock(
	t *testing.T,
	blockNumber, slot uint64,
	prevHash lcommon.Blake2b256,
	issuerSeed, vrfSeed byte,
	opCertSeqNo uint64,
) gledger.Block {
	t.Helper()
	emptyTxsCbor, err := cbor.Encode([]conway.ConwayTransactionBody{})
	require.NoError(t, err)
	emptyWitsCbor, err := cbor.Encode([]conway.ConwayTransactionWitnessSet{})
	require.NoError(t, err)
	emptyAuxCbor, err := cbor.Encode(lcommon.TransactionMetadataSet{})
	require.NoError(t, err)
	emptyInvalidCbor, err := cbor.Encode([]uint{})
	require.NoError(t, err)

	var issuer lcommon.IssuerVkey
	copy(issuer[:], siblingTestBytes(32, issuerSeed))

	block := &conway.ConwayBlock{
		BlockHeader: &conway.ConwayBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: blockNumber,
					Slot:        slot,
					PrevHash:    prevHash,
					IssuerVkey:  issuer,
					VrfKey:      siblingTestBytes(32, issuerSeed),
					VrfResult: lcommon.VrfResult{
						Output: siblingTestBytes(64, vrfSeed),
						Proof:  siblingTestBytes(80, vrfSeed),
					},
					BlockBodySize: uint64(
						len(emptyTxsCbor) + len(emptyWitsCbor) +
							len(emptyAuxCbor) + len(emptyInvalidCbor),
					),
					BlockBodyHash: fixtures.ComputeBlockBodyHash(
						emptyTxsCbor,
						emptyWitsCbor,
						emptyAuxCbor,
						emptyInvalidCbor,
					),
					OpCert: babbage.BabbageOpCert{
						HotVkey:        siblingTestBytes(32, issuerSeed),
						SequenceNumber: opCertSeqNo,
						Signature:      siblingTestBytes(64, issuerSeed),
					},
					ProtoVersion: babbage.BabbageProtoVersion{Major: 9},
				},
				Signature: siblingTestBytes(64, issuerSeed),
			},
		},
	}
	blockCbor, err := cbor.Encode(block)
	require.NoError(t, err)
	decoded, err := conway.NewConwayBlockFromCbor(blockCbor)
	require.NoError(t, err)
	return decoded
}

type siblingFixture struct {
	ls     *LedgerState
	parent gledger.Block
	rival  gledger.Block
}

const (
	siblingParentSlot = uint64(10)
	siblingSlot       = uint64(20)
	// The rival's VRF output. A candidate seeded below this wins the
	// tiebreak (lower VRF wins); one seeded above loses.
	siblingRivalVrfSeed = byte(0x80)
)

func newSiblingFixture(t *testing.T) *siblingFixture {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)

	parent := newSiblingTestBlock(
		t, 1, siblingParentSlot, lcommon.Blake2b256{}, 0x11, 0x11, 1,
	)
	rival := newSiblingTestBlock(
		t, 2, siblingSlot, parent.Hash(), 0x22, siblingRivalVrfSeed, 1,
	)
	require.NoError(t, cm.PrimaryChain().AddBlock(parent, nil))
	require.NoError(t, cm.PrimaryChain().AddBlock(rival, nil))

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	parentTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			parent.SlotNumber(),
			parent.Hash().Bytes(),
		),
		BlockNumber: parent.BlockNumber(),
	}
	rivalTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(rival.SlotNumber(), rival.Hash().Bytes()),
		BlockNumber: rival.BlockNumber(),
	}
	require.NoError(t, db.SetBlockNonce(
		parentTip.Point.Hash, parentTip.Point.Slot,
		siblingTestBytes(32, 0xa1), true, nil,
	))
	require.NoError(t, db.SetBlockNonce(
		rivalTip.Point.Hash, rivalTip.Point.Slot,
		siblingTestBytes(32, 0xb2), false, nil,
	))
	require.NoError(t, db.SetTip(rivalTip, nil))
	ls.currentTip = rivalTip
	ls.currentTipBlockNonce = siblingTestBytes(32, 0xb2)
	ls.chainsyncState = SyncingChainsyncState
	ls.publishSnapshotsLocked()

	return &siblingFixture{ls: ls, parent: parent, rival: rival}
}

// newSibling builds our own alternative to the rival: same slot and block
// number, the rival's parent as parent, a different issuer, and a VRF output
// seeded to win or lose the tiebreak.
func (f *siblingFixture) newSibling(
	t *testing.T,
	vrfSeed byte,
) gledger.Block {
	t.Helper()
	return newSiblingTestBlock(
		t,
		f.rival.BlockNumber(),
		f.rival.SlotNumber(),
		f.parent.Hash(),
		0x33,
		vrfSeed,
		1,
	)
}

// TestAdoptLocalForgedSiblingAdoptsTheWinnerOfChainSelection covers the two
// outcomes of a slot battle a locally forged alternative can have. Nothing in
// either case is decided by the block being ours: the same Praos comparison a
// peer's competing block goes through picks the winner, and the loser is
// discarded.
func TestAdoptLocalForgedSiblingAdoptsTheWinnerOfChainSelection(
	t *testing.T,
) {
	t.Run("ours wins the VRF tiebreak", func(t *testing.T) {
		f := newSiblingFixture(t)
		// Lower VRF output wins.
		ours := f.newSibling(t, siblingRivalVrfSeed-1)

		adopted, err := f.ls.AdoptLocalForgedSibling(ours)
		require.NoError(t, err)
		require.True(t, adopted, "lower VRF output must win the tiebreak")

		tip := f.ls.chain.Tip()
		assert.Equal(t, ours.Hash().Bytes(), tip.Point.Hash)
		assert.Equal(t, f.rival.BlockNumber(), tip.BlockNumber)
		assert.Equal(t, f.rival.SlotNumber(), tip.Point.Slot)
		// The rollback was exactly one block deep: the fork point is still
		// on the chain and is our block's parent.
		parent, _, ok := f.ls.chain.TipPredecessor()
		require.True(t, ok)
		assert.Equal(t, f.parent.Hash().Bytes(), parent.Hash)
	})

	t.Run("the rival wins the VRF tiebreak", func(t *testing.T) {
		f := newSiblingFixture(t)
		// Higher VRF output loses.
		ours := f.newSibling(t, siblingRivalVrfSeed+1)

		adopted, err := f.ls.AdoptLocalForgedSibling(ours)
		require.NoError(t, err)
		require.False(t, adopted, "higher VRF output must lose the tiebreak")

		tip := f.ls.chain.Tip()
		assert.Equal(
			t,
			f.rival.Hash().Bytes(),
			tip.Point.Hash,
			"a losing local block must leave the incumbent tip alone",
		)
	})

	t.Run("an identical view does not prefer ours", func(t *testing.T) {
		f := newSiblingFixture(t)
		// Same VRF output as the rival: ComparePraosTips answers
		// ChainEqual, which keeps the incumbent. A "prefer ours" rule
		// would adopt here.
		ours := f.newSibling(t, siblingRivalVrfSeed)

		adopted, err := f.ls.AdoptLocalForgedSibling(ours)
		require.NoError(t, err)
		require.False(t, adopted)
		assert.Equal(t, f.rival.Hash().Bytes(), f.ls.chain.Tip().Point.Hash)
	})
}

// TestAdoptLocalForgedSiblingRejectsNonSiblings pins the structural
// precondition. The adoption path rolls the chain back one block, so a block
// that is not a genuine competitor for the tip must never reach it.
func TestAdoptLocalForgedSiblingRejectsNonSiblings(t *testing.T) {
	t.Run("extends the tip rather than competing with it", func(t *testing.T) {
		f := newSiblingFixture(t)
		extension := newSiblingTestBlock(
			t, f.rival.BlockNumber()+1, f.rival.SlotNumber()+1,
			f.rival.Hash(), 0x33, 0x33, 1,
		)
		adopted, err := f.ls.AdoptLocalForgedSibling(extension)
		require.ErrorIs(t, err, ErrNotChainTipSibling)
		assert.False(t, adopted)
	})

	t.Run("wrong block number", func(t *testing.T) {
		f := newSiblingFixture(t)
		wrong := newSiblingTestBlock(
			t, f.rival.BlockNumber()+1, f.rival.SlotNumber(),
			f.parent.Hash(), 0x33, 0x33, 1,
		)
		adopted, err := f.ls.AdoptLocalForgedSibling(wrong)
		require.ErrorIs(t, err, ErrNotChainTipSibling)
		assert.False(t, adopted)
	})

	t.Run("slot at or below the fork point", func(t *testing.T) {
		f := newSiblingFixture(t)
		wrong := newSiblingTestBlock(
			t, f.rival.BlockNumber(), siblingParentSlot,
			f.parent.Hash(), 0x33, 0x33, 1,
		)
		adopted, err := f.ls.AdoptLocalForgedSibling(wrong)
		require.ErrorIs(t, err, ErrNotChainTipSibling)
		assert.False(t, adopted)
	})

	t.Run("same block number at a different slot", func(t *testing.T) {
		// A same-block-number competitor one slot above the tip is an
		// ordinary fork, not the equal-slot (EQ) case this path serves.
		// It would win or lose the Praos tiebreak on its merits, but
		// arbitrating it here would adopt a fork through a one-block
		// rollback driven from the forge loop instead of through
		// chainsync's fork resolution.
		f := newSiblingFixture(t)
		offSlot := newSiblingTestBlock(
			t, f.rival.BlockNumber(), f.rival.SlotNumber()+1,
			f.parent.Hash(), 0x33, 0x33, 1,
		)
		require.Greater(t, offSlot.SlotNumber(), siblingParentSlot)
		adopted, err := f.ls.AdoptLocalForgedSibling(offSlot)
		require.ErrorIs(t, err, ErrNotChainTipSibling)
		require.ErrorContains(t, err, "is not the chain tip's slot")
		assert.False(t, adopted)
	})

	t.Run("the block already at the tip", func(t *testing.T) {
		f := newSiblingFixture(t)
		adopted, err := f.ls.AdoptLocalForgedSibling(f.rival)
		require.ErrorIs(t, err, ErrNotChainTipSibling)
		assert.False(t, adopted)
	})
}

// TestPraosPrefersCandidateSiblingUsesTheStandardRule exercises the decision
// itself, including the cases the fixture cannot reach: it must be
// length-first, must never invent a preference, and must keep the incumbent
// whenever no reference implementation rule applies.
func TestPraosPrefersCandidateSiblingUsesTheStandardRule(t *testing.T) {
	const slot = uint64(20)
	incumbentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(slot, siblingTestBytes(32, 0x01)),
		BlockNumber: 2,
	}
	candidateTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(slot, siblingTestBytes(32, 0x02)),
		BlockNumber: 2,
	}
	viewWith := func(
		tip ochainsync.Tip,
		issuerSeed, vrfSeed byte,
		issueNo uint64,
		cfg praos.PraosTiebreakerConfig,
	) praos.PraosTiebreakerView {
		return praos.NewPraosTiebreakerViewFull(
			tip,
			siblingTestBytes(32, issuerSeed),
			issueNo,
			siblingTestBytes(64, vrfSeed),
			cfg,
		)
	}
	conwayCfg := praos.PraosTiebreakerConfigConway()

	tests := []struct {
		name                      string
		candidate, incumbent      ochainsync.Tip
		candidateView, incumbView praos.PraosTiebreakerView
		want                      bool
	}{
		{
			name:      "lower VRF wins",
			candidate: candidateTip, incumbent: incumbentTip,
			candidateView: viewWith(candidateTip, 0x33, 0x40, 1, conwayCfg),
			incumbView:    viewWith(incumbentTip, 0x22, 0x80, 1, conwayCfg),
			want:          true,
		},
		{
			name:      "higher VRF loses",
			candidate: candidateTip, incumbent: incumbentTip,
			candidateView: viewWith(candidateTip, 0x33, 0xC0, 1, conwayCfg),
			incumbView:    viewWith(incumbentTip, 0x22, 0x80, 1, conwayCfg),
			want:          false,
		},
		{
			name:      "identical VRF keeps the incumbent",
			candidate: candidateTip, incumbent: incumbentTip,
			candidateView: viewWith(candidateTip, 0x33, 0x80, 1, conwayCfg),
			incumbView:    viewWith(incumbentTip, 0x22, 0x80, 1, conwayCfg),
			want:          false,
		},
		{
			name:      "unarmed tiebreaker keeps the incumbent",
			candidate: candidateTip, incumbent: incumbentTip,
			candidateView: viewWith(
				candidateTip, 0x33, 0x40, 1,
				praos.PraosTiebreakerConfigUnknown(),
			),
			incumbView: viewWith(
				incumbentTip, 0x22, 0x80, 1,
				praos.PraosTiebreakerConfigUnknown(),
			),
			want: false,
		},
		{
			name: "shorter candidate loses regardless of VRF",
			candidate: ochainsync.Tip{
				Point:       candidateTip.Point,
				BlockNumber: 1,
			},
			incumbent:     incumbentTip,
			candidateView: viewWith(candidateTip, 0x33, 0x00, 1, conwayCfg),
			incumbView:    viewWith(incumbentTip, 0x22, 0xFF, 1, conwayCfg),
			want:          false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := praosPrefersCandidateSibling(
				tt.candidate,
				tt.incumbent,
				tt.candidateView,
				tt.incumbView,
			)
			assert.Equal(t, tt.want, got)
		})
	}
}
