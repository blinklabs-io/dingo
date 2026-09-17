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

// newTruncateGapHealTestLedgerState builds a minimal LedgerState backed by
// an in-memory database for exercising healTruncateGapBlockNonces.
func newTruncateGapHealTestLedgerState(
	t *testing.T,
	db *database.Database,
	tipSlot uint64,
	tipHash []byte,
	tipNonce []byte,
) *LedgerState {
	t.Helper()
	return &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{Slot: tipSlot, Hash: tipHash},
		},
		currentTipBlockNonce: tipNonce,
		epochNonceHexCache:   map[uint64]epochNonceHexCacheEntry{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
}

// TestHealTruncateGapBlockNonces_ReconstructsFromCheckpoint proves the
// reconstruction produces the CORRECT nonce, not merely "a" nonce: it
// independently computes the expected evolving nonce by folding the same
// blocks with eras.CalculateEtaVConway directly (ordinary forward replay,
// bypassing the heal entirely) and asserts the heal's persisted tip nonce
// equals that ground truth exactly.
//
// Setup mirrors what database.TruncateAfterSlot now allows through: a
// checkpoint row survives (retained forever), but every non-checkpoint
// block_nonce row between the checkpoint and the tip has been pruned by
// routine 3-epoch retention -- exactly what remains after a deep
// disaster-recovery truncate whose target's own nonce row was already
// pruned. No chain index is attached, so this also exercises the raw
// blob-range-scan fallback path (database.ForEachBlockInRangeDB) used by
// offline tooling.
func TestHealTruncateGapBlockNonces_ReconstructsFromCheckpoint(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	const checkpointSlot uint64 = 100

	var origin lcommon.Blake2b256
	blocks, err := fixtures.GenerateConwayChain(1, origin, 110, 10, 5)
	require.NoError(t, err)
	require.Len(t, blocks, 5)

	checkpointHash := bytes.Repeat([]byte{0x0c}, 32)
	checkpointNonce := bytes.Repeat([]byte{0xaa}, 32)
	require.NoError(t, db.SetBlockNonce(
		checkpointHash,
		checkpointSlot,
		checkpointNonce,
		true, // isCheckpoint
		nil,
	))
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

	tipBlock := blocks[len(blocks)-1]
	tipSlot := tipBlock.SlotNumber()
	tipHash := tipBlock.Hash().Bytes()

	// No block_nonce row at all between the checkpoint and the tip: routine
	// 3-epoch retention pruned every non-checkpoint row in that range,
	// including the tip's own, exactly as it would after a deep truncate.

	ls := newTruncateGapHealTestLedgerState(t, db, tipSlot, tipHash, nil)
	ls.config.CardanoNodeConfig = newConwayBootstrapStabilityCfg(t)
	ls.epochNonceHexCache[42] = epochNonceHexCacheEntry{
		nonce: []byte("stale"),
		hex:   "stale",
	}

	// Ground truth: fold the same blocks independently via the era's own
	// VRF-fold function, not via the code under test.
	expected := bytes.Clone(checkpointNonce)
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

	require.NoError(t, ls.healTruncateGapBlockNonces(t.Context()))

	for _, block := range blocks {
		got, err := db.GetBlockNonce(
			ocommon.Point{Slot: block.SlotNumber(), Hash: block.Hash().Bytes()},
			nil,
		)
		require.NoError(t, err)
		require.Equal(
			t,
			expectedBySlot[block.SlotNumber()],
			got,
			"reconstructed nonce at slot %d must equal the value ordinary "+
				"forward replay would have computed",
			block.SlotNumber(),
		)
	}

	tipNonce, err := db.GetBlockNonce(
		ocommon.Point{Slot: tipSlot, Hash: tipHash},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, expected, tipNonce)
	require.Equal(t, expected, ls.currentTipBlockNonce)
	require.Empty(
		t,
		ls.epochNonceHexCache,
		"stale cached epoch nonce hex must be dropped after reconstruction",
	)
}

// TestHealTruncateGapBlockNonces_CanonicalChainExcludesForkBlob verifies
// that when a chain index is attached (the live-node path), the heal folds
// only canonical-chain blocks: a retained fork blob inside the
// checkpoint-to-tip slot range must not contribute to the reconstructed
// evolving nonce.
func TestHealTruncateGapBlockNonces_CanonicalChainExcludesForkBlob(
	t *testing.T,
) {
	t.Parallel()

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
	checkpointBlock := blocks[0]
	tipBlock := blocks[2]

	// Retained fork blob between the checkpoint and the tip: present in the
	// blob store but not on the canonical chain.
	forkHash := bytes.Repeat([]byte{0xf0}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       99,
		Slot:     105,
		Hash:     forkHash,
		PrevHash: checkpointBlock.Hash().Bytes(),
		Cbor:     blocks[1].Cbor(),
		Number:   99,
		Type:     conway.BlockTypeConway,
	}, nil))

	checkpointNonce := bytes.Repeat([]byte{0xaa}, 32)
	require.NoError(t, db.SetBlockNonce(
		checkpointBlock.Hash().Bytes(),
		checkpointBlock.SlotNumber(),
		checkpointNonce,
		true, // isCheckpoint
		nil,
	))

	ls := newTruncateGapHealTestLedgerState(
		t, db, tipBlock.SlotNumber(), tipBlock.Hash().Bytes(), nil)
	ls.config.CardanoNodeConfig = newConwayBootstrapStabilityCfg(t)
	ls.chain = canonicalChain

	// Ground truth: fold only the two canonical blocks after the checkpoint.
	expected := bytes.Clone(checkpointNonce)
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

	require.NoError(t, ls.healTruncateGapBlockNonces(t.Context()))

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

	tipNonce, err := db.GetBlockNonce(
		ocommon.Point{Slot: tipBlock.SlotNumber(), Hash: tipBlock.Hash().Bytes()},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, expected, tipNonce)
	require.Equal(t, expected, ls.currentTipBlockNonce)
}

// TestHealTruncateGapBlockNonces_NoOpWhenTipNonceValid verifies the fast,
// cheap path every ordinary startup takes: a valid (32-byte) tip nonce means
// there is no gap, so the heal must not touch the database at all.
func TestHealTruncateGapBlockNonces_NoOpWhenTipNonceValid(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	tipHash := bytes.Repeat([]byte{0x01}, 32)
	tipNonce := bytes.Repeat([]byte{0x02}, 32)
	ls := newTruncateGapHealTestLedgerState(t, db, 500, tipHash, tipNonce)
	ls.config.CardanoNodeConfig = newConwayBootstrapStabilityCfg(t)

	require.NoError(t, ls.healTruncateGapBlockNonces(t.Context()))
	require.Equal(t, tipNonce, ls.currentTipBlockNonce)
	require.Equal(t, 0, countBlockNonces(t, db))
}

// TestHealTruncateGapBlockNonces_NoOpForByronTip verifies Byron blocks are
// exempt: they carry no Praos VRF nonce, so an empty tip nonce at a Byron
// tip is correct, not a gap, mirroring TruncateAfterSlot's own exemption.
func TestHealTruncateGapBlockNonces_NoOpForByronTip(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	byronHash := bytes.Repeat([]byte{0x0b}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:   200,
		Hash:   byronHash,
		Cbor:   []byte{0x80},
		Number: 1,
		Type:   byron.BlockTypeByronMain,
	}, nil))

	ls := newTruncateGapHealTestLedgerState(t, db, 200, byronHash, nil)
	ls.config.CardanoNodeConfig = newConwayBootstrapStabilityCfg(t)

	require.NoError(t, ls.healTruncateGapBlockNonces(t.Context()))
	require.Empty(t, ls.currentTipBlockNonce)
	require.Equal(t, 0, countBlockNonces(t, db))
}

// TestHealTruncateGapBlockNonces_NoOpWithoutCheckpoint verifies the
// graceful-decline path for the case reconstruction genuinely cannot
// handle: no checkpoint row survives at or before the tip, so there is
// nothing to fold forward from. This is unreachable for a genuine
// truncate-created gap (database.TruncateAfterSlot's own checkpoint check
// already refuses such a truncate before it ever produces this tip) --
// what actually reaches this path is a chain that never went through
// ordinary ledger block application (e.g. this test, which seeds a block
// directly). Mirroring healMithrilGapBlockNonces's own no-anchor case, the
// heal warns and leaves state as-is rather than blocking LedgerState
// startup outright: the real safety enforcement for a genuine
// truncate-created gap lives in TruncateAfterSlot's checkpoint check, not
// here.
func TestHealTruncateGapBlockNonces_FailsWithoutCheckpoint(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	tipHash := bytes.Repeat([]byte{0x01}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:   500,
		Hash:   tipHash,
		Cbor:   []byte{0x80},
		Number: 1,
		Type:   conway.BlockTypeConway,
	}, nil))

	ls := newTruncateGapHealTestLedgerState(t, db, 500, tipHash, nil)
	ls.config.CardanoNodeConfig = newConwayBootstrapStabilityCfg(t)

	err = ls.healTruncateGapBlockNonces(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, "no block_nonce checkpoint")
	require.Empty(t, ls.currentTipBlockNonce)
	require.Equal(t, 0, countBlockNonces(t, db))
}

// TestHealTruncateGapBlockNonces_FailsWhenOnlyCheckpointIsForkOnly proves the
// heal refuses to proceed -- rather than silently leaving the tip nonce
// empty -- when the only block_nonce checkpoint at or before the tip belongs
// to a since-abandoned fork. database.TruncateAfterSlot's own guard checks
// only that a checkpoint row exists, not that it sits on the primary chain
// (it has no chain-topology knowledge to do so), so it can let a truncate
// through on the strength of a checkpoint this heal then correctly refuses
// to use.
func TestHealTruncateGapBlockNonces_FailsWhenOnlyCheckpointIsForkOnly(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	canonicalChain := cm.PrimaryChain()

	var origin lcommon.Blake2b256
	blocks, err := fixtures.GenerateConwayChain(1, origin, 100, 10, 2)
	require.NoError(t, err)
	require.Len(t, blocks, 2)
	for _, block := range blocks {
		require.NoError(t, canonicalChain.AddBlock(block, nil))
	}
	tipBlock := blocks[1]

	// The only checkpoint at or before the tip belongs to a fork that
	// diverges from genesis -- never added to the canonical chain at all --
	// modeling a checkpoint whose fork was abandoned without its
	// block_nonce row being cleaned up by whatever rollback should have
	// pruned it.
	forkCheckpointHash := bytes.Repeat([]byte{0xf0}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:     99,
		Slot:   50,
		Hash:   forkCheckpointHash,
		Cbor:   blocks[0].Cbor(),
		Number: 99,
		Type:   conway.BlockTypeConway,
	}, nil))
	require.NoError(t, db.SetBlockNonce(
		forkCheckpointHash,
		50,
		bytes.Repeat([]byte{0xaa}, 32),
		true, // isCheckpoint
		nil,
	))

	ls := newTruncateGapHealTestLedgerState(
		t, db, tipBlock.SlotNumber(), tipBlock.Hash().Bytes(), nil)
	ls.config.CardanoNodeConfig = newConwayBootstrapStabilityCfg(t)
	ls.chain = canonicalChain

	err = ls.healTruncateGapBlockNonces(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, "primary chain")
	require.Empty(t, ls.currentTipBlockNonce)
}
