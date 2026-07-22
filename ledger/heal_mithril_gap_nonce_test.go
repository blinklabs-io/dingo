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

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

// newGapHealTestLedgerState builds a minimal LedgerState backed by an
// in-memory database for exercising healMithrilGapBlockNonces guard logic.
func newGapHealTestLedgerState(
	t *testing.T,
	db *database.Database,
	boundary uint64,
	tipSlot uint64,
	tipHash []byte,
) *LedgerState {
	t.Helper()
	return &LedgerState{
		db:                db,
		mithrilLedgerSlot: boundary,
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{Slot: tipSlot, Hash: tipHash},
		},
		epochNonceHexCache: map[uint64]string{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
}

func countBlockNonces(t *testing.T, db *database.Database) int {
	t.Helper()
	// A slot range wide enough to cover every row written in these tests.
	rows, err := db.GetBlockNoncesInSlotRange(0, 1_000_000, nil)
	require.NoError(t, err)
	return len(rows)
}

func TestHealMithrilGapBlockNonces_ReconstructsGapAndRefreshesTip(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	const (
		anchorSlot uint64 = 100
		byronSlot  uint64 = 110
		firstSlot  uint64 = 120
		boundary   uint64 = 130
	)

	var origin lcommon.Blake2b256
	blocks, err := fixtures.GenerateConwayChain(1, origin, firstSlot, 10, 3)
	require.NoError(t, err)
	require.Len(t, blocks, 3)

	anchorHash := bytes.Repeat([]byte{0x0a}, 32)
	anchorNonce := bytes.Repeat([]byte{0xaa}, 32)
	byronHash := bytes.Repeat([]byte{0xb0}, 32)
	staleTipNonce := bytes.Repeat([]byte{0xee}, 32)
	tipBlock := blocks[len(blocks)-1]
	tipSlot := tipBlock.SlotNumber()
	tipHash := tipBlock.Hash().Bytes()

	require.NoError(t, db.SetBlockNonce(
		anchorHash,
		anchorSlot,
		anchorNonce,
		true,
		nil,
	))
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:     byronSlot,
		Hash:     byronHash,
		PrevHash: anchorHash,
		Cbor:     []byte{0x80},
		Number:   1,
		Type:     byron.BlockTypeByronMain,
	}, nil))
	for _, block := range blocks {
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     block.SlotNumber(),
			Hash:     block.Hash().Bytes(),
			PrevHash: block.PrevHash().Bytes(),
			Cbor:     block.Cbor(),
			Number:   block.BlockNumber(),
			Type:     conway.BlockTypeConway,
		}, nil))
	}
	require.NoError(t, db.SetBlockNonce(
		tipHash,
		tipSlot,
		staleTipNonce,
		true,
		nil,
	))

	ls := newGapHealTestLedgerState(t, db, boundary, tipSlot, tipHash)
	ls.config.CardanoNodeConfig = newConwayBootstrapStabilityCfg(t)
	ls.currentTipBlockNonce = bytes.Clone(staleTipNonce)
	ls.epochNonceHexCache[42] = "stale"

	expected := bytes.Clone(anchorNonce)
	expectedBySlot := map[uint64][]byte{}
	for _, block := range blocks {
		expected, err = eras.CalculateEtaVConway(
			ls.config.CardanoNodeConfig,
			expected,
			block,
		)
		require.NoError(t, err)
		expectedBySlot[block.SlotNumber()] = bytes.Clone(expected)
	}

	require.NoError(t, ls.healMithrilGapBlockNonces(t.Context()))

	require.Equal(t, 4, countBlockNonces(t, db))
	byronNonce, err := db.GetBlockNonce(
		ocommon.Point{Slot: byronSlot, Hash: byronHash},
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, byronNonce, "Byron blocks must not get Praos block_nonce rows")

	for _, block := range blocks {
		got, err := db.GetBlockNonce(
			ocommon.Point{Slot: block.SlotNumber(), Hash: block.Hash().Bytes()},
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, expectedBySlot[block.SlotNumber()], got)
	}
	tipNonce, err := db.GetBlockNonce(
		ocommon.Point{Slot: tipSlot, Hash: tipHash},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, expected, tipNonce)
	require.NotEqual(t, staleTipNonce, tipNonce)

	rows, err := db.GetBlockNoncesInSlotRange(anchorSlot, tipSlot+1, nil)
	require.NoError(t, err)
	rowsBySlot := map[uint64]models.BlockNonce{}
	for _, row := range rows {
		rowsBySlot[row.Slot] = row
	}
	require.True(t, rowsBySlot[anchorSlot].IsCheckpoint)
	require.False(t, rowsBySlot[firstSlot].IsCheckpoint)
	require.True(t, rowsBySlot[boundary].IsCheckpoint)
	require.True(t, rowsBySlot[tipSlot].IsCheckpoint)

	require.Equal(t, expected, ls.currentTipBlockNonce)
	require.Empty(t, ls.epochNonceHexCache)
}

// TestHealMithrilGapBlockNonces_CanonicalChainExcludesForkBlob verifies that
// when a chain index is attached, the heal folds only canonical-chain blocks:
// a retained fork blob inside the anchor→tip slot range must not contribute to
// the reconstructed evolving nonce (a raw block-blob slot scan would fold it
// and silently corrupt every subsequent nonce).
func TestHealMithrilGapBlockNonces_CanonicalChainExcludesForkBlob(
	t *testing.T,
) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	canonicalChain := cm.PrimaryChain()

	var origin lcommon.Blake2b256
	blocks, err := fixtures.GenerateConwayChain(1, origin, 100, 10, 3)
	require.NoError(t, err)
	require.Len(t, blocks, 3)
	for _, block := range blocks {
		require.NoError(t, canonicalChain.AddBlock(block, nil))
	}
	anchorBlock := blocks[0] // slot 100
	boundary := blocks[1].SlotNumber()
	tipBlock := blocks[2]

	// Retained fork blob between the anchor and the boundary: present in the
	// blob store but not on the canonical chain.
	forkHash := bytes.Repeat([]byte{0xf0}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       99,
		Slot:     105,
		Hash:     forkHash,
		PrevHash: anchorBlock.Hash().Bytes(),
		Cbor:     blocks[1].Cbor(),
		Number:   99,
		Type:     conway.BlockTypeConway,
	}, nil))

	anchorNonce := bytes.Repeat([]byte{0xaa}, 32)
	require.NoError(t, db.SetBlockNonce(
		anchorBlock.Hash().Bytes(),
		anchorBlock.SlotNumber(),
		anchorNonce,
		true,
		nil,
	))

	ls := newGapHealTestLedgerState(
		t, db, boundary, tipBlock.SlotNumber(), tipBlock.Hash().Bytes())
	ls.config.CardanoNodeConfig = newConwayBootstrapStabilityCfg(t)
	ls.chain = canonicalChain

	// Expected fold: anchor nonce through the two canonical blocks only.
	expected := bytes.Clone(anchorNonce)
	expectedBySlot := map[uint64][]byte{}
	for _, block := range blocks[1:] {
		expected, err = eras.CalculateEtaVConway(
			ls.config.CardanoNodeConfig,
			expected,
			block,
		)
		require.NoError(t, err)
		expectedBySlot[block.SlotNumber()] = bytes.Clone(expected)
	}

	require.NoError(t, ls.healMithrilGapBlockNonces(t.Context()))

	// Anchor + the two canonical blocks; the fork blob gets no row.
	require.Equal(t, 3, countBlockNonces(t, db))
	forkNonce, err := db.GetBlockNonce(
		ocommon.Point{Slot: 105, Hash: forkHash},
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, forkNonce, "fork blob must not be folded or given a row")
	for _, block := range blocks[1:] {
		got, err := db.GetBlockNonce(
			ocommon.Point{
				Slot: block.SlotNumber(),
				Hash: block.Hash().Bytes(),
			},
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, expectedBySlot[block.SlotNumber()], got,
			"canonical block nonce must be the fold of canonical blocks only")
	}
}

// TestHealMithrilGapBlockNonces_BoundaryCompletionRequiresTrustHash verifies
// that a nonce row at the trust-boundary slot only marks the heal complete
// when it belongs to the recorded Mithril boundary hash. A retained fork row
// at the same slot must not mask missing canonical nonce history.
func TestHealMithrilGapBlockNonces_BoundaryCompletionRequiresTrustHash(
	t *testing.T,
) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	canonicalChain := cm.PrimaryChain()

	var origin lcommon.Blake2b256
	blocks, err := fixtures.GenerateConwayChain(1, origin, 100, 10, 3)
	require.NoError(t, err)
	require.Len(t, blocks, 3)
	for _, block := range blocks {
		require.NoError(t, canonicalChain.AddBlock(block, nil))
	}
	anchorBlock := blocks[0]
	boundaryBlock := blocks[1]
	tipBlock := blocks[2]

	anchorNonce := bytes.Repeat([]byte{0xaa}, 32)
	forkBoundaryHash := bytes.Repeat([]byte{0xfb}, 32)
	forkBoundaryNonce := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.SetBlockNonce(
		anchorBlock.Hash().Bytes(),
		anchorBlock.SlotNumber(),
		anchorNonce,
		true,
		nil,
	))
	require.NoError(t, db.SetBlockNonce(
		forkBoundaryHash,
		boundaryBlock.SlotNumber(),
		forkBoundaryNonce,
		true,
		nil,
	))

	ls := newGapHealTestLedgerState(
		t,
		db,
		boundaryBlock.SlotNumber(),
		tipBlock.SlotNumber(),
		tipBlock.Hash().Bytes(),
	)
	ls.config.CardanoNodeConfig = newConwayBootstrapStabilityCfg(t)
	ls.chain = canonicalChain
	ls.mithrilLedgerHash = boundaryBlock.Hash().Bytes()

	expected := bytes.Clone(anchorNonce)
	expectedBySlot := map[uint64][]byte{}
	for _, block := range blocks[1:] {
		expected, err = eras.CalculateEtaVConway(
			ls.config.CardanoNodeConfig,
			expected,
			block,
		)
		require.NoError(t, err)
		expectedBySlot[block.SlotNumber()] = bytes.Clone(expected)
	}

	require.NoError(t, ls.healMithrilGapBlockNonces(t.Context()))

	gotBoundary, err := db.GetBlockNonce(
		ocommon.Point{
			Slot: boundaryBlock.SlotNumber(),
			Hash: boundaryBlock.Hash().Bytes(),
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, expectedBySlot[boundaryBlock.SlotNumber()], gotBoundary)
	gotTip, err := db.GetBlockNonce(
		ocommon.Point{
			Slot: tipBlock.SlotNumber(),
			Hash: tipBlock.Hash().Bytes(),
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, expected, gotTip)
	gotFork, err := db.GetBlockNonce(
		ocommon.Point{
			Slot: boundaryBlock.SlotNumber(),
			Hash: forkBoundaryHash,
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, forkBoundaryNonce, gotFork)
}

// TestHealMithrilGapBlockNonces_FallsBackFromNonCanonicalAnchor verifies that
// a valid nonce on a retained fork below the trust boundary is not allowed to
// make startup healing skip when a lower canonical anchor can seed the fold.
func TestHealMithrilGapBlockNonces_FallsBackFromNonCanonicalAnchor(
	t *testing.T,
) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	canonicalChain := cm.PrimaryChain()

	var origin lcommon.Blake2b256
	blocks, err := fixtures.GenerateConwayChain(1, origin, 100, 10, 3)
	require.NoError(t, err)
	require.Len(t, blocks, 3)
	for _, block := range blocks {
		require.NoError(t, canonicalChain.AddBlock(block, nil))
	}
	anchorBlock := blocks[0]
	boundaryBlock := blocks[1]
	tipBlock := blocks[2]

	anchorNonce := bytes.Repeat([]byte{0xaa}, 32)
	require.NoError(t, db.SetBlockNonce(
		anchorBlock.Hash().Bytes(),
		anchorBlock.SlotNumber(),
		anchorNonce,
		true,
		nil,
	))

	forkHash := bytes.Repeat([]byte{0xfc}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       99,
		Slot:     anchorBlock.SlotNumber() + 5,
		Hash:     forkHash,
		PrevHash: anchorBlock.Hash().Bytes(),
		Cbor:     boundaryBlock.Cbor(),
		Number:   99,
		Type:     conway.BlockTypeConway,
	}, nil))
	require.NoError(t, db.SetBlockNonce(
		forkHash,
		anchorBlock.SlotNumber()+5,
		bytes.Repeat([]byte{0xff}, 32),
		false,
		nil,
	))

	ls := newGapHealTestLedgerState(
		t,
		db,
		boundaryBlock.SlotNumber(),
		tipBlock.SlotNumber(),
		tipBlock.Hash().Bytes(),
	)
	ls.config.CardanoNodeConfig = newConwayBootstrapStabilityCfg(t)
	ls.chain = canonicalChain

	expected := bytes.Clone(anchorNonce)
	expectedBySlot := map[uint64][]byte{}
	for _, block := range blocks[1:] {
		expected, err = eras.CalculateEtaVConway(
			ls.config.CardanoNodeConfig,
			expected,
			block,
		)
		require.NoError(t, err)
		expectedBySlot[block.SlotNumber()] = bytes.Clone(expected)
	}

	require.NoError(t, ls.healMithrilGapBlockNonces(t.Context()))

	gotBoundary, err := db.GetBlockNonce(
		ocommon.Point{
			Slot: boundaryBlock.SlotNumber(),
			Hash: boundaryBlock.Hash().Bytes(),
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, expectedBySlot[boundaryBlock.SlotNumber()], gotBoundary)
	gotTip, err := db.GetBlockNonce(
		ocommon.Point{
			Slot: tipBlock.SlotNumber(),
			Hash: tipBlock.Hash().Bytes(),
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, expected, gotTip)
}

// TestHealMithrilGapBlockNonces_MalformedRowDoesNotMaskAnchor verifies anchor
// selection ignores rows with an invalid nonce length: a malformed higher-slot
// row below the trust boundary must not displace a valid lower-slot anchor and
// make the heal decline.
func TestHealMithrilGapBlockNonces_MalformedRowDoesNotMaskAnchor(
	t *testing.T,
) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	var origin lcommon.Blake2b256
	blocks, err := fixtures.GenerateConwayChain(1, origin, 120, 10, 1)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	tipBlock := blocks[0]
	boundary := tipBlock.SlotNumber()
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:     tipBlock.SlotNumber(),
		Hash:     tipBlock.Hash().Bytes(),
		PrevHash: tipBlock.PrevHash().Bytes(),
		Cbor:     tipBlock.Cbor(),
		Number:   tipBlock.BlockNumber(),
		Type:     conway.BlockTypeConway,
	}, nil))

	anchorHash := bytes.Repeat([]byte{0x0a}, 32)
	anchorNonce := bytes.Repeat([]byte{0xaa}, 32)
	require.NoError(t, db.SetBlockNonce(anchorHash, 100, anchorNonce, true, nil))
	// Malformed row at a higher slot below the boundary.
	require.NoError(t, db.SetBlockNonce(
		bytes.Repeat([]byte{0x0b}, 32), 110, []byte{0x01, 0x02}, false, nil))

	ls := newGapHealTestLedgerState(
		t, db, boundary, tipBlock.SlotNumber(), tipBlock.Hash().Bytes())
	ls.config.CardanoNodeConfig = newConwayBootstrapStabilityCfg(t)

	require.NoError(t, ls.healMithrilGapBlockNonces(t.Context()))

	want, err := eras.CalculateEtaVConway(
		ls.config.CardanoNodeConfig,
		anchorNonce,
		tipBlock,
	)
	require.NoError(t, err)
	got, err := db.GetBlockNonce(
		ocommon.Point{
			Slot: tipBlock.SlotNumber(),
			Hash: tipBlock.Hash().Bytes(),
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, want, got,
		"heal must seed from the valid lower-slot anchor, not decline")
}

// TestHealMithrilGapBlockNonces_MalformedBoundaryRowDoesNotSkip verifies that a
// boundary row whose nonce is malformed (non-32-byte) does not short-circuit
// the heal. Only a valid 32-byte nonce is a completion marker, so a boundary
// block imported without its VRF folded must still be reconstructed and
// overwritten with the correct nonce rather than leaving the epoch nonce wrong.
func TestHealMithrilGapBlockNonces_MalformedBoundaryRowDoesNotSkip(
	t *testing.T,
) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	var origin lcommon.Blake2b256
	blocks, err := fixtures.GenerateConwayChain(1, origin, 120, 10, 1)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	tipBlock := blocks[0]
	boundary := tipBlock.SlotNumber()
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:     tipBlock.SlotNumber(),
		Hash:     tipBlock.Hash().Bytes(),
		PrevHash: tipBlock.PrevHash().Bytes(),
		Cbor:     tipBlock.Cbor(),
		Number:   tipBlock.BlockNumber(),
		Type:     conway.BlockTypeConway,
	}, nil))

	anchorHash := bytes.Repeat([]byte{0x0a}, 32)
	anchorNonce := bytes.Repeat([]byte{0xaa}, 32)
	require.NoError(t, db.SetBlockNonce(anchorHash, 100, anchorNonce, true, nil))
	// Malformed nonce already stored AT the boundary slot (a block imported
	// without folding its VRF). It must not be mistaken for a completion marker.
	require.NoError(t, db.SetBlockNonce(
		tipBlock.Hash().Bytes(), boundary, []byte{0x01, 0x02}, false, nil))

	ls := newGapHealTestLedgerState(
		t, db, boundary, tipBlock.SlotNumber(), tipBlock.Hash().Bytes())
	ls.config.CardanoNodeConfig = newConwayBootstrapStabilityCfg(t)

	require.NoError(t, ls.healMithrilGapBlockNonces(t.Context()))

	want, err := eras.CalculateEtaVConway(
		ls.config.CardanoNodeConfig,
		anchorNonce,
		tipBlock,
	)
	require.NoError(t, err)
	got, err := db.GetBlockNonce(
		ocommon.Point{Slot: boundary, Hash: tipBlock.Hash().Bytes()},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, want, got,
		"malformed boundary nonce must be reconstructed, not treated as complete")
	require.NotEqual(t, []byte{0x01, 0x02}, got,
		"the malformed boundary nonce must have been overwritten")
}

// TestHealMithrilGapBlockNonces_NoBoundaryNoOp verifies the heal is a no-op on
// a non-Mithril (genesis-synced) DB, where mithrilLedgerSlot is zero.
func TestHealMithrilGapBlockNonces_NoBoundaryNoOp(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	ls := newGapHealTestLedgerState(t, db, 0, 300, bytes.Repeat([]byte{0x01}, 32))
	require.NoError(t, ls.healMithrilGapBlockNonces(t.Context()))
	require.Equal(t, 0, countBlockNonces(t, db),
		"heal must not write any block_nonce rows when not Mithril-bootstrapped")
}

// TestHealMithrilGapBlockNonces_AlreadyHealedNoOp verifies idempotency: when a
// block_nonce already exists at the trust-boundary slot (either no gap existed,
// or the heal ran previously), the heal detects completion and does nothing.
// This is the critical invariant for a startup heal — it must not re-fold or
// mutate an already-correct chain.
func TestHealMithrilGapBlockNonces_AlreadyHealedNoOp(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	const boundary = 200
	anchorNonce := bytes.Repeat([]byte{0xaa}, 32)
	boundaryNonce := bytes.Repeat([]byte{0xbb}, 32)
	// Anchor below the boundary and a nonce present AT the boundary slot: the
	// completion signal.
	boundaryHash := bytes.Repeat([]byte{0x0b}, 32)
	require.NoError(t, db.SetBlockNonce(
		bytes.Repeat([]byte{0x0a}, 32), 150, anchorNonce, true, nil))
	require.NoError(t, db.SetBlockNonce(
		boundaryHash, boundary, boundaryNonce, true, nil))

	ls := newGapHealTestLedgerState(
		t, db, boundary, 300, bytes.Repeat([]byte{0x0c}, 32))
	require.NoError(t, ls.healMithrilGapBlockNonces(t.Context()))

	// Both rows must be untouched and no new rows created.
	require.Equal(t, 2, countBlockNonces(t, db))
	got, err := db.GetBlockNonce(
		ocommon.Point{Slot: boundary, Hash: boundaryHash}, nil)
	require.NoError(t, err)
	require.Equal(t, boundaryNonce, got,
		"boundary nonce must be left unchanged when already healed")
}

// TestHealMithrilGapBlockNonces_NoAnchorSkips verifies the heal declines to act
// when no anchor evolving nonce exists below the trust boundary. Reconstructing
// from an unknown seed would be worse than leaving state as-is, so the heal must
// not write anything or error.
func TestHealMithrilGapBlockNonces_NoAnchorSkips(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	ls := newGapHealTestLedgerState(
		t, db, 200, 300, bytes.Repeat([]byte{0x0c}, 32))
	require.NoError(t, ls.healMithrilGapBlockNonces(t.Context()))
	require.Equal(t, 0, countBlockNonces(t, db),
		"heal must not write block_nonce rows without a usable anchor nonce")
}

// TestHealMithrilGapBlockNonces_TipBelowBoundaryNoOp verifies the heal does not
// act when the tip has not reached the trust boundary (an abnormal state that
// implies no gap has been crossed yet).
func TestHealMithrilGapBlockNonces_TipBelowBoundaryNoOp(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	require.NoError(t, db.SetBlockNonce(
		bytes.Repeat([]byte{0x0a}, 32), 150,
		bytes.Repeat([]byte{0xaa}, 32), true, nil))

	ls := newGapHealTestLedgerState(
		t, db, 200, 180, bytes.Repeat([]byte{0x0c}, 32))
	require.NoError(t, ls.healMithrilGapBlockNonces(t.Context()))
	require.Equal(t, 1, countBlockNonces(t, db),
		"heal must not act when the tip is below the trust boundary")
}
