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
	opCertSeqNo uint32,
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
		[]byte("nonce-parent"), true, nil,
	))
	require.NoError(t, db.SetBlockNonce(
		rivalTip.Point.Hash, rivalTip.Point.Slot,
		[]byte("nonce-rival"), false, nil,
	))
	require.NoError(t, db.SetTip(rivalTip, nil))
	ls.currentTip = rivalTip
	ls.currentTipBlockNonce = []byte("nonce-rival")
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
