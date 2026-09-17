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
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testHash32(b byte) []byte {
	h := make([]byte, 32)
	for i := range h {
		h[i] = b
	}
	return h
}

// newBlockContextTestBuilder returns a builder whose live chain tip is the
// contested block: slot contestedSlot, block number contestedBlockNumber.
func newBlockContextTestBuilder(
	t *testing.T,
	tip ochainsync.Tip,
) *DefaultBlockBuilder {
	t.Helper()
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{transactions: []MempoolTransaction{}},
		PParamsProvider: &mockPParamsProvider{
			pparams: &conway.ConwayProtocolParameters{
				MaxTxSize:        16384,
				MaxBlockBodySize: 90112,
				MaxBlockExUnits: lcommon.ExUnits{
					Memory: 62000000,
					Steps:  20000000000,
				},
			},
		},
		ChainTip:    &mockChainTip{tip: tip},
		EpochNonce:  &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)},
		Credentials: setupTestCredentials(t),
	})
	require.NoError(t, err)
	return builder
}

// TestBuildBlockOnContextForgesAnAlternativeToTheTip is the builder half of the
// equal-slot alternative. The live tip is a rival block at the slot being
// forged; the explicit context names the rival's predecessor as parent and the
// rival's own block number, so the two blocks are siblings that chain selection
// arbitrates between. This is ouroboros-consensus mkCurrentBlockContext's EQ
// branch: "forge an alternative to @hdr@: same block no and same predecessor".
func TestBuildBlockOnContextForgesAnAlternativeToTheTip(t *testing.T) {
	const (
		contestedSlot   = uint64(1000)
		parentSlot      = uint64(999)
		rivalBlockNumbr = uint64(100)
	)
	rivalHash := testHash32(0xAA)
	parentHash := testHash32(0xBB)
	rival := ochainsync.Tip{
		Point:       ocommon.Point{Slot: contestedSlot, Hash: rivalHash},
		BlockNumber: rivalBlockNumbr,
	}
	builder := newBlockContextTestBuilder(t, rival)

	block, blockCbor, err := builder.BuildBlockOnContext(
		contestedSlot,
		0,
		LeiosBlockData{},
		BlockContext{
			Parent: ocommon.Point{
				Slot: parentSlot,
				Hash: parentHash,
			},
			BlockNumber: rivalBlockNumbr,
			Rival:       rival,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, block)
	assert.NotEmpty(t, blockCbor)

	assert.Equal(t, contestedSlot, block.SlotNumber())
	// Same block number as the rival, not the rival's plus one: the
	// alternative competes with it, it does not extend it.
	assert.Equal(t, rivalBlockNumbr, block.BlockNumber())
	assert.Equal(
		t,
		parentHash,
		block.PrevHash().Bytes(),
		"alternative must name the rival's predecessor as parent",
	)
	assert.NotEqual(
		t,
		rivalHash,
		block.PrevHash().Bytes(),
		"alternative must not name the rival itself as parent",
	)
}

// TestBuildBlockRefusesAParentAtItsOwnSlot is the negative half: the default
// live-tip path must never produce the block the equal-slot case would
// otherwise ask it for. Binding the tip as parent when the tip is already at
// the forged slot yields a block whose parent slot equals its own, which
// ledger.validateBlockOrder rejects ("block slot %d does not follow parent slot
// %d") and every Praos peer rejects for the same reason. The builder refuses
// before signing rather than emitting it.
func TestBuildBlockRefusesAParentAtItsOwnSlot(t *testing.T) {
	const contestedSlot = uint64(1000)
	rival := ochainsync.Tip{
		Point:       ocommon.Point{Slot: contestedSlot, Hash: testHash32(0xAA)},
		BlockNumber: 100,
	}
	builder := newBlockContextTestBuilder(t, rival)

	t.Run("live tip path", func(t *testing.T) {
		block, blockCbor, err := builder.BuildBlock(contestedSlot, 0)
		require.ErrorIs(t, err, errParentSlotNotBelowBlock)
		assert.Nil(t, block)
		assert.Nil(t, blockCbor)
	})

	t.Run("explicit context naming the rival", func(t *testing.T) {
		block, blockCbor, err := builder.BuildBlockOnContext(
			contestedSlot,
			0,
			LeiosBlockData{},
			BlockContext{
				Parent:      rival.Point,
				BlockNumber: rival.BlockNumber,
				Rival:       rival,
			},
		)
		require.ErrorIs(t, err, errParentSlotNotBelowBlock)
		assert.Nil(t, block)
		assert.Nil(t, blockCbor)
	})

	t.Run("explicit context with a parent above the forged slot", func(t *testing.T) {
		block, _, err := builder.BuildBlockOnContext(
			contestedSlot,
			0,
			LeiosBlockData{},
			BlockContext{
				Parent: ocommon.Point{
					Slot: contestedSlot + 1,
					Hash: testHash32(0xBB),
				},
				BlockNumber: rival.BlockNumber,
				Rival:       rival,
			},
		)
		require.ErrorIs(t, err, errParentSlotNotBelowBlock)
		assert.Nil(t, block)
	})
}

// TestBuildBlockOnContextAbandonsAStaleContest pins that a candidate bound to a
// rival which is no longer the chain tip is dropped before any signing work.
// The contest is over: either the rival was rolled back, or the chain moved on
// past it, and in both cases the alternative is meaningless.
func TestBuildBlockOnContextAbandonsAStaleContest(t *testing.T) {
	const contestedSlot = uint64(1000)
	liveTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: contestedSlot,
			Hash: testHash32(0xCC),
		},
		BlockNumber: 100,
	}
	builder := newBlockContextTestBuilder(t, liveTip)

	staleRival := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: contestedSlot,
			Hash: testHash32(0xAA),
		},
		BlockNumber: 100,
	}
	block, _, err := builder.BuildBlockOnContext(
		contestedSlot,
		0,
		LeiosBlockData{},
		BlockContext{
			Parent: ocommon.Point{
				Slot: contestedSlot - 1,
				Hash: testHash32(0xBB),
			},
			BlockNumber: staleRival.BlockNumber,
			Rival:       staleRival,
		},
	)
	require.ErrorIs(t, err, errParentChangedDuringBuild)
	assert.Nil(t, block)
}

// TestBuildBlockOnContextRequiresAResolvedParent pins the fail-closed contract
// shared with chain.TipPredecessor: a context with no parent must not silently
// fall back to a genesis-shaped (null prevHash) block.
func TestBuildBlockOnContextRequiresAResolvedParent(t *testing.T) {
	const contestedSlot = uint64(1000)
	rival := ochainsync.Tip{
		Point:       ocommon.Point{Slot: contestedSlot, Hash: testHash32(0xAA)},
		BlockNumber: 100,
	}
	builder := newBlockContextTestBuilder(t, rival)

	block, _, err := builder.BuildBlockOnContext(
		contestedSlot,
		0,
		LeiosBlockData{},
		BlockContext{BlockNumber: rival.BlockNumber, Rival: rival},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolved parent")
	assert.Nil(t, block)
}

// TestBuildBlockStillExtendsTheLiveTipByDefault guards the unchanged path: an
// uncontested slot builds on the live tip with the tip's block number plus one.
func TestBuildBlockStillExtendsTheLiveTipByDefault(t *testing.T) {
	tipHash := testHash32(0xAA)
	builder := newBlockContextTestBuilder(t, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 1000, Hash: tipHash},
		BlockNumber: 100,
	})

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.NotNil(t, block)
	assert.Equal(t, uint64(1001), block.SlotNumber())
	assert.Equal(t, uint64(101), block.BlockNumber())
	assert.Equal(t, tipHash, block.PrevHash().Bytes())
}

// TestBuildBlockOnContextCarriesNoMempoolTransactions is the state-mismatch
// half of the equal-slot alternative. Every transaction validator reachable
// from the builder answers against the ledger's live state, which has the
// rival applied; the alternative is built on the rival's predecessor and
// adoption rolls the rival back before applying it. A mempool transaction
// spending a UTxO the rival created therefore passes validation, gets
// selected, and then fails to apply after adoption -- wedging the node at the
// fork point with neither candidate on the chain.
//
// The validator here accepts everything, exactly as a live-state validator
// would for such a transaction. The live-tip build shows it is admitted there;
// the context build must not consult the validator at all.
func TestBuildBlockOnContextCarriesNoMempoolTransactions(t *testing.T) {
	const (
		contestedSlot   = uint64(1000)
		parentSlot      = uint64(999)
		rivalBlockNumbr = uint64(100)
	)
	rivalHash := testHash32(0xAA)
	parentHash := testHash32(0xBB)
	rival := ochainsync.Tip{
		Point:       ocommon.Point{Slot: contestedSlot, Hash: rivalHash},
		BlockNumber: rivalBlockNumbr,
	}
	// A UTxO that only exists because the rival block created it.
	rivalCreatedTxHash := testHash32(0xCC)
	mempool := &mockMempool{
		transactions: []MempoolTransaction{
			{
				Hash: "spends_rival_output",
				Cbor: makeMinimalTxCborWithInput(t, rivalCreatedTxHash, 0),
				Type: conway.TxTypeConway,
			},
		},
	}
	validator := &sessionMockTxValidator{}
	builder := newSelectionTestBuilder(
		t,
		mempool,
		&mockChainTip{tip: rival},
		validator,
	)

	// Live-tip control: the validator admits the transaction, so a normal
	// build selects it. This is the state the alternative must not inherit.
	control, _, err := builder.BuildBlock(contestedSlot+1, 0)
	require.NoError(t, err)
	require.Len(
		t,
		control.Transactions(),
		1,
		"control: the live-state validator admits this transaction",
	)
	require.Equal(t, 1, validator.validateCalls)

	block, _, err := builder.BuildBlockOnContext(
		contestedSlot,
		0,
		LeiosBlockData{},
		BlockContext{
			Parent:      ocommon.Point{Slot: parentSlot, Hash: parentHash},
			BlockNumber: rivalBlockNumbr,
			Rival:       rival,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, block)
	assert.Empty(
		t,
		block.Transactions(),
		"an alternative must carry no transaction selected against the rival's state",
	)
	assert.Equal(
		t,
		1,
		validator.validateCalls,
		"no mempool transaction may be offered to a live-state validator for an alternative",
	)
}

// TestBuildBlockOnContextRejectsLeiosData pins the fail-closed guard on the
// exported entrypoint. Leios certificate and announcement data is resolved
// against the live tip -- the rival -- so a block that does not build on the
// rival must not carry it. The forger omits it already; the builder refuses it
// rather than trusting every caller to.
func TestBuildBlockOnContextRejectsLeiosData(t *testing.T) {
	const (
		contestedSlot   = uint64(1000)
		parentSlot      = uint64(999)
		rivalBlockNumbr = uint64(100)
	)
	rival := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: contestedSlot,
			Hash: testHash32(0xAA),
		},
		BlockNumber: rivalBlockNumbr,
	}
	builder := newBlockContextTestBuilder(t, rival)
	blockCtx := BlockContext{
		Parent: ocommon.Point{
			Slot: parentSlot,
			Hash: testHash32(0xBB),
		},
		BlockNumber: rivalBlockNumbr,
		Rival:       rival,
	}

	for name, leios := range map[string]LeiosBlockData{
		"announcement": {
			Announcement: &LeiosEndorserBlockAnnouncement{},
		},
		"certificate": {
			Certificate: &lcommon.LeiosEbCertificate{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			block, blockCbor, err := builder.BuildBlockOnContext(
				contestedSlot,
				0,
				leios,
				blockCtx,
			)
			require.ErrorIs(t, err, errLeiosDataOnAlternative)
			assert.Nil(t, block)
			assert.Nil(t, blockCbor)
		})
	}

	// Empty Leios data on the same context still builds, so the guard is
	// rejecting the data and not the context.
	block, _, err := builder.BuildBlockOnContext(
		contestedSlot,
		0,
		LeiosBlockData{},
		blockCtx,
	)
	require.NoError(t, err)
	require.NotNil(t, block)
}
