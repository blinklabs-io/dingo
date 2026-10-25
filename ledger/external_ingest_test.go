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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type externalIngestHarness struct {
	ls       *LedgerState
	db       *database.Database
	cm       *chain.ChainManager
	shutOnce sync.Once
}

// newExternalIngestHarness builds a Conway ledger at origin over a fresh
// file-backed store. manual selects the dingo-load configuration
// (ManualBlockProcessing with TrustedReplay), which is the reference the
// external-ingest path must match; otherwise the ledger is configured as a
// live node, with neither flag set.
func newExternalIngestHarness(
	t testing.TB,
	manual bool,
) *externalIngestHarness {
	t.Helper()
	return newExternalIngestHarnessAt(t, manual, t.TempDir())
}

// newExternalIngestHarnessAt is newExternalIngestHarness over dataDir, which
// may already hold a store an earlier harness shut down. The ledger tip is
// then the store's committed tip.
func newExternalIngestHarnessAt(
	t testing.TB,
	manual bool,
	dataDir string,
) *externalIngestHarness {
	t.Helper()
	return newExternalIngestHarnessWith(
		t, manual, dataDir, types.StorageModeCore, 0,
	)
}

// newExternalIngestHarnessWith is newExternalIngestHarnessAt in storageMode,
// with the Shelley security parameter set to securityParam when it is not
// zero. A small one shrinks the stability window to a few dozen slots.
func newExternalIngestHarnessWith(
	t testing.TB,
	manual bool,
	dataDir string,
	storageMode string,
	securityParam int,
) *externalIngestHarness {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		StorageMode: storageMode,
	})
	require.NoError(t, err)
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	cfg := newTestShelleyGenesisCfg(t)
	cfg.ShelleyGenesis().NetworkId = "Testnet"
	if securityParam != 0 {
		cfg.ShelleyGenesis().SecurityParam = securityParam
	}
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:              db,
		ChainManager:          cm,
		CardanoNodeConfig:     cfg,
		Logger:                testLogger(),
		PromRegistry:          prometheus.NewRegistry(),
		ManualBlockProcessing: manual,
		TrustedReplay:         manual,
	})
	require.NoError(t, err)
	h := &externalIngestHarness{ls: ls, db: db, cm: cm}
	t.Cleanup(func() { h.shutdown(t) })
	nonce := bytes.Repeat([]byte{0x42}, 32)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = &conway.ConwayProtocolParameters{
		KeyDeposit:       2_000_000,
		PoolDeposit:      500_000_000,
		DRepDeposit:      500_000_000,
		GovActionDeposit: 100_000_000,
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionPlomin,
		},
	}
	ls.currentEpoch = models.Epoch{
		SlotLength:    1_000,
		LengthInSlots: 1_000,
		EraId:         eras.ConwayEraDesc.Id,
		Nonce:         nonce,
		EvolvingNonce: nonce,
	}
	ls.epochCache = []models.Epoch{ls.currentEpoch}
	ls.currentTip = ochainsync.Tip{}
	ls.currentTipBlockNonce = nonce
	ls.publishSnapshotsLocked()
	require.NoError(t, ls.loadTip(t.Context()))
	require.NoError(t, cm.SetLedger(ls))
	return h
}

// shutdown closes the ledger and its store, as a process exit would.
func (h *externalIngestHarness) shutdown(t testing.TB) {
	t.Helper()
	h.shutOnce.Do(func() {
		require.NoError(t, h.ls.Close())
		require.NoError(t, dbtest.CloseDatabase(h.db))
	})
}

// startLiveLoop runs the ledger's chain-iterator loop the way Start does,
// without Start's reload of epoch and tip state from the empty store.
func (h *externalIngestHarness) startLiveLoop(t *testing.T) {
	t.Helper()
	// A node that has not latched reachedTip holds each under-full batch for
	// up to two seconds waiting for more blocks (gatherCoalesceMaxGapAttempts),
	// which would leave these tests nothing under their five second waits on
	// a slow runner. These tests drive a live node, so latch it.
	h.ls.reachedTip.Store(true)
	ctx, cancel := context.WithCancel(t.Context())
	h.ls.processBlocksCancel = cancel
	h.ls.processBlocksWG.Go(func() {
		h.ls.ledgerProcessBlocks(ctx)
	})
}

// externalIngestBlocks returns the row-batching scenario's three linked
// blocks, then a block holding a phase-2-invalid transaction and a block
// spending that transaction's collateral return, followed by extra empty
// blocks two slots apart.
//
// The invalid transaction names drep:0 as its input, which stays unspent,
// takes register:1 as collateral, and produces only its collateral return
// (output index 1, after its one regular output); retspend spends it.
func externalIngestBlocks(
	t *testing.T,
	extra int,
) ([]gledger.Block, map[string]rowBatchingTx) {
	t.Helper()
	blocks, names := rowBatchingScenario(t)
	payOther := rowBatchingBaseAddress(t, 0x44, 0x55)
	payStake := rowBatchingBaseAddress(t, 0x44, 0x22)
	names["invalid"] = newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{names["drep"].hash, uint64(0)}},
		1: []any{map[uint]any{0: payOther, 1: uint64(900_000)}},
		2: uint64(100_000),
		13: []any{
			[]any{names["register"].hash, uint64(1)},
		},
		16: map[uint]any{0: payStake, 1: uint64(1_500_000)},
		17: uint64(500_000),
	})
	names["retspend"] = newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{names["invalid"].hash, uint64(1)}},
		1: []any{map[uint]any{0: payOther, 1: uint64(1_400_000)}},
		2: uint64(100_000),
	})
	b3 := blocks[len(blocks)-1]
	b4 := rowBatchingBlockWithInvalid(
		t, 35, 4, b3.Hash(), []uint{0}, names["invalid"],
	)
	b5 := rowBatchingBlock(t, 40, 5, b4.Hash(), names["retspend"])
	blocks = append(blocks, b4, b5)
	return extendExternalIngestBlocks(t, blocks, extra, 2), names
}

// extendExternalIngestBlocks appends count empty blocks, step slots apart.
func extendExternalIngestBlocks(
	t testing.TB,
	blocks []gledger.Block,
	count int,
	step uint64,
) []gledger.Block {
	t.Helper()
	for range count {
		prev := blocks[len(blocks)-1]
		blocks = append(blocks, rowBatchingBlock(
			t,
			prev.SlotNumber()+step,
			prev.BlockNumber()+1,
			prev.Hash(),
		))
	}
	return blocks
}

func blockPoint(block gledger.Block) ocommon.Point {
	return ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
}

func (h *externalIngestHarness) applyPaused(
	t testing.TB,
	expected ocommon.Point,
	blocks []gledger.Block,
) (ocommon.Point, error) {
	t.Helper()
	resume := h.ls.PauseChainIngest()
	defer resume()
	return h.ls.ApplyTrustedBatch(t.Context(), expected, blocks)
}

func utxoRowCount(t *testing.T, db *database.Database) int {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	return rowBatchingQueryOne[int](t, raw, `SELECT COUNT(*) FROM utxo`)
}

func utxoRowExists(
	t *testing.T,
	db *database.Database,
	txHash []byte,
	idx uint32,
) bool {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	return rowBatchingQueryOne[int](t, raw,
		`SELECT COUNT(*) FROM utxo WHERE tx_id = ? AND output_idx = ?`,
		txHash, idx) > 0
}

func TestApplyTrustedBatchRequiresPause(t *testing.T) {
	t.Parallel()

	h := newExternalIngestHarness(t, false)
	blocks, _ := externalIngestBlocks(t, 0)
	_, err := h.ls.ApplyTrustedBatch(t.Context(), ocommon.Point{}, blocks)
	require.Error(t, err)
	require.Zero(t, h.ls.Tip().Point.Slot)
	require.Zero(t, h.cm.PrimaryChain().Tip().Point.Slot,
		"an unpaused call must not store blocks")
}

func TestApplyTrustedBatchStaleTip(t *testing.T) {
	t.Parallel()

	h := newExternalIngestHarness(t, false)
	blocks, _ := externalIngestBlocks(t, 0)
	before := utxoRowCount(t, h.db)

	stale := ocommon.NewPoint(5, bytes.Repeat([]byte{0x99}, 32))
	tip, err := h.applyPaused(t, stale, blocks)
	require.ErrorIs(t, err, ErrStaleBatch)
	require.Equal(t, ocommon.Point{}.Slot, tip.Slot)
	require.Equal(t, ochainsync.Tip{}, h.ls.Tip(), "the ledger tip moved")
	require.Equal(t, before, utxoRowCount(t, h.db), "the UTxO set changed")
	require.Zero(t, h.cm.PrimaryChain().Tip().Point.Slot,
		"a stale batch must not store blocks")
}

func TestApplyTrustedBatchIntraBatchSpend(t *testing.T) {
	t.Parallel()

	h := newExternalIngestHarness(t, false)
	blocks, names := externalIngestBlocks(t, 0)
	tip, err := h.applyPaused(t, ocommon.Point{}, blocks)
	require.NoError(t, err)
	require.Equal(t, blockPoint(blocks[len(blocks)-1]), tip)

	// register:0 is created in block 1 and spent by spend in block 2, and
	// spend:1 is created in block 2 and spent by chain in block 3.
	require.False(t, utxoRowExists(t, h.db, names["register"].hash, 0),
		"an output created and spent within the batch was written")
	require.False(t, utxoRowExists(t, h.db, names["spend"].hash, 1),
		"an output created and spent within the batch was written")
	require.True(t, utxoRowExists(t, h.db, names["spend"].hash, 0))
	require.True(t, utxoRowExists(t, h.db, names["chain"].hash, 0))
	// The phase-2-invalid transaction consumed only its collateral
	// (register:1) and produced only its collateral return (invalid:1),
	// which retspend then spent.
	require.True(t, utxoRowExists(t, h.db, names["drep"].hash, 0),
		"an invalid transaction's regular input was consumed")
	require.False(t, utxoRowExists(t, h.db, names["register"].hash, 1),
		"collateral created and spent within the batch was written")
	require.False(t, utxoRowExists(t, h.db, names["invalid"].hash, 0),
		"an invalid transaction's regular output was produced")
	require.False(t, utxoRowExists(t, h.db, names["invalid"].hash, 1),
		"a collateral return created and spent within the batch was written")
	require.True(t, utxoRowExists(t, h.db, names["retspend"].hash, 0))
	_, err = h.db.UtxoByRef(t.Context(), names["chain"].hash, 0, nil)
	require.NoError(t, err)
}

func TestApplyTrustedBatchSingleCommit(t *testing.T) {
	t.Parallel()

	h := newExternalIngestHarness(t, false)
	blocks, _ := externalIngestBlocks(t, 0)
	blocks = extendExternalIngestBlocks(
		t, blocks, MaxTrustedBatchBlocks-len(blocks), 2,
	)
	require.Len(t, blocks, MaxTrustedBatchBlocks)
	require.Greater(t, len(blocks), batchSize,
		"the batch must exceed one live-loop transaction")
	// Two feed chunks, delivered as one batch.
	chunks := [][]gledger.Block{blocks[:len(blocks)/2], blocks[len(blocks)/2:]}
	batch := append(append([]gledger.Block(nil), chunks[0]...), chunks[1]...)

	var commits atomic.Int64
	h.ls.afterLedgerWriteCommit = func() { commits.Add(1) }
	tip, err := h.applyPaused(t, ocommon.Point{}, batch)
	require.NoError(t, err)
	require.Equal(t, blockPoint(batch[len(batch)-1]), tip)
	require.Equal(t, int64(1), commits.Load(),
		"a trusted batch must commit its metadata in one transaction")
	stored, err := h.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, tip.Hash, stored.Point.Hash)
}

// withoutPruneFloor drops the consumed-UTxO prune floor from a digest. The
// periodic cleanup writes it from whatever tip it last saw, so it records
// when cleanup ran, not what was applied.
func withoutPruneFloor(digest string) string {
	lines := strings.Split(digest, "\n")
	kept := lines[:0]
	for _, line := range lines {
		if !strings.HasPrefix(line, "sync_key=consumed_utxo_prune_floor|") {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}

// A batch Badger refuses as too big for one transaction is applied in halves,
// each its own commit, down to what Badger accepts: the same blocks are
// applied once each and the result matches one clean batch, and a single
// block still refused fails the call rather than being skipped.
func TestApplyTrustedBatchSplitsTxnTooBig(t *testing.T) {
	t.Parallel()
	blocks, _ := externalIngestBlocks(t, 0)
	blocks = extendExternalIngestBlocks(t, blocks, 40-len(blocks), 2)
	clean := newExternalIngestHarness(t, false)
	_, err := clean.applyPaused(t, ocommon.Point{}, blocks)
	require.NoError(t, err)

	t.Run("splits", func(t *testing.T) {
		t.Parallel()
		h := newExternalIngestHarness(t, false)
		// Badger refuses any transaction over 7 blocks, as it would a dense
		// range; the error arrives wrapped, as through the storage layers.
		const limit = 7
		var committed []int
		var splits atomic.Int64
		h.ls.trustedBatchBeforeCommit = func(applied []gledger.Block) error {
			if len(applied) > limit {
				return fmt.Errorf("blob commit failed: %w", badger.ErrTxnTooBig)
			}
			committed = append(committed, len(applied))
			return nil
		}
		h.ls.trustedBatchSplit = func(int) { splits.Add(1) }
		var commits atomic.Int64
		h.ls.afterLedgerWriteCommit = func() { commits.Add(1) }
		overlays := h.countOverlays()
		tip, err := h.applyPaused(t, ocommon.Point{}, blocks)
		require.NoError(t, err)
		require.Equal(t, blockPoint(blocks[len(blocks)-1]), tip)
		assert.Positive(t, splits.Load())
		assert.Positive(t, overlays.Load(),
			"the split halves must apply through the overlay")
		total := 0
		for _, n := range committed {
			assert.LessOrEqual(t, n, limit)
			total += n
		}
		assert.Equal(t, len(blocks), total, "a block applied twice or never")
		assert.Equal(t, int64(len(committed)), commits.Load(),
			"each sub-batch commits once")
		assert.Equal(t,
			withoutPruneFloor(rowBatchingDigest(t, clean.db)),
			withoutPruneFloor(rowBatchingDigest(t, h.db)),
			"a split batch diverged from one clean batch",
		)
	})

	t.Run("single block refused", func(t *testing.T) {
		t.Parallel()
		h := newExternalIngestHarness(t, false)
		refused := blockPoint(blocks[5])
		h.ls.trustedBatchBeforeCommit = func(applied []gledger.Block) error {
			for _, b := range applied {
				if pointMatches(blockPoint(b), refused) {
					return fmt.Errorf("set: %v", badger.ErrTxnTooBig)
				}
			}
			return nil
		}
		tip, err := h.applyPaused(t, ocommon.Point{}, blocks)
		require.Error(t, err)
		assert.True(t, isTxnTooBig(err))
		// Everything before the refused block committed; nothing after it.
		assert.Equal(t, blockPoint(blocks[4]), tip)
		assert.Equal(t, blockPoint(blocks[4]), h.ls.Tip().Point)
	})
}

func TestApplyTrustedBatchCrashBetweenStores(t *testing.T) {
	t.Parallel()

	blocks, _ := externalIngestBlocks(t, 4)
	clean := newExternalIngestHarness(t, false)
	_, err := clean.applyPaused(t, ocommon.Point{}, blocks)
	require.NoError(t, err)

	h := newExternalIngestHarness(t, false)
	injected := errors.New("injected crash before metadata commit")
	var fail atomic.Bool
	fail.Store(true)
	h.ls.trustedBatchBeforeCommit = func([]gledger.Block) error {
		if fail.Load() {
			return injected
		}
		return nil
	}
	_, err = h.applyPaused(t, ocommon.Point{}, blocks)
	require.ErrorIs(t, err, injected)
	require.Equal(t, ochainsync.Tip{}, h.ls.Tip(), "the ledger tip moved")
	stored, err := h.db.GetTip(nil)
	require.NoError(t, err)
	require.Zero(t, stored.Point.Slot, "the metadata tip moved")
	require.Zero(t, utxoRowCount(t, h.db), "metadata rows were committed")
	require.Equal(t,
		blocks[len(blocks)-1].Hash().Bytes(),
		h.cm.PrimaryChain().Tip().Point.Hash,
		"the blob write precedes the metadata commit",
	)

	// Replay from the unchanged committed tip over the stored blobs.
	fail.Store(false)
	overlays := h.countOverlays()
	tip, err := h.applyPaused(t, ocommon.Point{}, blocks)
	require.NoError(t, err)
	require.Positive(t, overlays.Load(),
		"the replay must apply through the overlay")
	require.Equal(t, blockPoint(blocks[len(blocks)-1]), tip)
	require.Equal(t,
		rowBatchingDigest(t, clean.db),
		rowBatchingDigest(t, h.db),
		"re-replay after the crash diverged from a clean run",
	)
}

// countOverlays counts the block-apply transactions h's ledger applies
// through the batch UTxO overlay.
func (h *externalIngestHarness) countOverlays() *atomic.Int64 {
	var n atomic.Int64
	h.ls.afterTrustedBatchOverlay = func() { n.Add(1) }
	return &n
}

// countBatchedWrites counts the transactions h's ledger writes through the
// row-batching accumulator.
func (h *externalIngestHarness) countBatchedWrites() *atomic.Int64 {
	var n atomic.Int64
	h.ls.afterBatchedTransactionWrite = func() { n.Add(1) }
	return &n
}

// TestApplyTrustedBatchMatchesReplay is also the row-batching equivalence
// check for trusted batches: ApplyTrustedBatch always writes through the
// accumulator, and the reference replay, without ApplyRowBatchingEnabled,
// never does.
// dropCatchUpIndexes puts the store in the state a held node-managed
// Mithril catch-up applies under: the lazy and catch-up droppable indexes
// dropped.
func dropCatchUpIndexes(t *testing.T, db *database.Database) {
	t.Helper()
	manager, ok := db.Metadata().(metadata.CatchUpIndexManager)
	require.True(t, ok)
	require.NoError(t, manager.DropCatchUpIndexesContext(t.Context()))
	missing, err := db.Metadata().(metadata.MissingCriticalDeferredIndexLister).
		MissingCriticalDeferredIndexes()
	require.NoError(t, err)
	require.NotEmpty(t, missing)
}

func TestApplyTrustedBatchMatchesReplay(t *testing.T) {
	t.Parallel()
	for _, dropped := range []bool{false, true} {
		t.Run(fmt.Sprintf("catchup_indexes_dropped=%t", dropped),
			func(t *testing.T) {
				t.Parallel()
				testApplyTrustedBatchMatchesReplay(t, dropped)
			})
	}
}

func testApplyTrustedBatchMatchesReplay(t *testing.T, dropped bool) {
	blocks, _ := externalIngestBlocks(t, 4)

	reference := newExternalIngestHarness(t, true)
	referenceBatched := reference.countBatchedWrites()
	for _, block := range blocks {
		require.NoError(t, reference.cm.PrimaryChain().AddBlock(
			t.Context(), block, nil,
		))
	}
	batches := make(chan []gledger.Block, 1)
	batches <- blocks
	close(batches)
	require.NoError(t, reference.ls.ProcessTrustedBlockBatches(
		t.Context(), batches,
	))
	// The trusted path keeps spent rows until the routine consumed-UTxO
	// sweep removes them; the external path never commits the rows its
	// own batch created and spent. Sweep the reference to the same tip.
	lastSlot := blocks[len(blocks)-1].SlotNumber()
	pruned, err := reference.db.UtxosDeleteConsumed(
		t.Context(), lastSlot, 1_000, nil,
	)
	require.NoError(t, err)
	require.Positive(t, pruned, "the scenario must spend within the batch")

	h := newExternalIngestHarness(t, false)
	if dropped {
		dropCatchUpIndexes(t, h.db)
	}
	batched := h.countBatchedWrites()
	overlays := h.countOverlays()
	tip, err := h.applyPaused(t, ocommon.Point{}, blocks)
	require.NoError(t, err)
	require.Equal(t, blockPoint(blocks[len(blocks)-1]), tip)
	require.Zero(t, referenceBatched.Load(),
		"the reference must take the unbatched path")
	require.Positive(t, batched.Load(),
		"a trusted batch must write through the row-batching accumulator")
	require.Positive(t, overlays.Load(),
		"a trusted batch must apply through the batch UTxO overlay")

	if dropped {
		// The run ends its cycle with the full rebuild; the digest
		// compares rows, and the marker is one.
		require.NoError(t, h.db.Metadata().(metadata.DeferredIndexManager).
			BuildDeferredIndexes())
	}
	require.Equal(t,
		rowBatchingDigest(t, reference.db),
		rowBatchingDigest(t, h.db),
		"external ingest diverged from the trusted replay path",
	)
}

func TestPauseChainIngest(t *testing.T) {
	t.Parallel()

	h := newExternalIngestHarness(t, false)
	waiting := make(chan struct{}, 1)
	h.ls.chainIngestGateWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}
	h.startLiveLoop(t)
	blocks, _ := externalIngestBlocks(t, 4)

	resume := h.ls.PauseChainIngest()
	// Pausing again while paused does not wait on itself.
	resumeAgain := h.ls.PauseChainIngest()
	require.NoError(t, h.cm.PrimaryChain().AddBlocks(t.Context(), blocks))
	testutil.RequireReceive(t, waiting, 5*time.Second,
		"the live loop never reached the paused gate")
	require.Equal(t, ochainsync.Tip{}, h.ls.Tip(),
		"a block was applied while chain ingest was paused")

	resumeAgain()
	resume()
	resume() // idempotent
	last := blockPoint(blocks[len(blocks)-1])
	require.Eventually(t, func() bool {
		return pointMatches(h.ls.Tip().Point, last)
	}, 5*time.Second, 10*time.Millisecond)
}

// TestPauseChainIngestResumesFromExternalTip applies a trusted batch while the
// live loop holds a read of the same blocks, then resumes it. The loop must
// continue from the externally applied tip rather than replay its stale read.
func TestPauseChainIngestResumesFromExternalTip(t *testing.T) {
	t.Parallel()

	h := newExternalIngestHarness(t, false)
	h.startLiveLoop(t)
	blocks, _ := externalIngestBlocks(t, 6)
	split := 4

	resume := h.ls.PauseChainIngest()
	tip, err := h.ls.ApplyTrustedBatch(
		t.Context(), ocommon.Point{}, blocks[:split],
	)
	require.NoError(t, err)
	require.Equal(t, blockPoint(blocks[split-1]), tip)
	resume()

	require.NoError(t, h.cm.PrimaryChain().AddBlocks(
		t.Context(), blocks[split:],
	))
	last := blockPoint(blocks[len(blocks)-1])
	require.Eventually(t, func() bool {
		return pointMatches(h.ls.Tip().Point, last)
	}, 5*time.Second, 10*time.Millisecond)

	reference := newExternalIngestHarness(t, false)
	_, err = reference.applyPaused(t, ocommon.Point{}, blocks[:split])
	require.NoError(t, err)
	_, err = reference.applyPaused(
		t,
		blockPoint(blocks[split-1]),
		blocks[split:],
	)
	require.NoError(t, err)
	// The external path prunes spent rows up to each batch tip; the live loop
	// leaves them to the routine sweep. Sweep the loop's store to the same tip.
	_, err = h.db.UtxosDeleteConsumed(t.Context(), last.Slot, 1_000, nil)
	require.NoError(t, err)
	require.Equal(t,
		rowBatchingDigest(t, reference.db),
		rowBatchingDigest(t, h.db),
		"the resumed loop diverged from applying the same blocks externally",
	)
}

// requireNothingCommitted checks that a refused batch left the metadata store
// at origin.
func requireNothingCommitted(t *testing.T, h *externalIngestHarness) {
	t.Helper()
	stored, err := h.db.GetTip(nil)
	require.NoError(t, err)
	require.Zero(t, stored.Point.Slot, "the metadata tip moved")
	require.Zero(t, utxoRowCount(t, h.db), "metadata rows were committed")
}

// TestApplyTrustedBatchTipMovesAfterGuard moves the ledger tip after the tip
// guard has passed, as a rollback racing the batch would. The batch must be
// reported stale, not as a pipeline restart, and commit nothing.
func TestApplyTrustedBatchTipMovesAfterGuard(t *testing.T) {
	t.Parallel()

	h := newExternalIngestHarness(t, false)
	blocks, _ := externalIngestBlocks(t, 0)
	moved := ochainsync.Tip{
		Point:       ocommon.NewPoint(5, bytes.Repeat([]byte{0x77}, 32)),
		BlockNumber: 1,
	}
	h.ls.afterTrustedBatchGuard = func() { h.ls.SetTipForTesting(moved) }
	_, err := h.applyPaused(t, ocommon.Point{}, blocks)
	require.ErrorIs(t, err, ErrStaleBatch)
	require.NotErrorIs(t, err, errRestartLedgerPipeline)
	require.Equal(t, moved, h.ls.Tip(), "the batch moved the ledger tip")
	requireNothingCommitted(t, h)
}

// TestApplyTrustedBatchChainAppendWhilePaused appends a block that is not
// the batch's to the primary chain while ingest is paused. The batch no
// longer joins the chain, which is a tip race the caller replays from.
func TestApplyTrustedBatchChainAppendWhilePaused(t *testing.T) {
	t.Parallel()

	h := newExternalIngestHarness(t, false)
	blocks, _ := externalIngestBlocks(t, 0)
	other := rowBatchingBlock(t, 7, 1, lcommon.Blake2b256{})
	require.NotEqual(t, other.Hash(), blocks[0].Hash())

	resume := h.ls.PauseChainIngest()
	defer resume()
	require.NoError(t, h.cm.PrimaryChain().AddBlocks(
		t.Context(), []gledger.Block{other},
	))
	_, err := h.ls.ApplyTrustedBatch(t.Context(), ocommon.Point{}, blocks)
	require.ErrorIs(t, err, ErrStaleBatch)
	require.Equal(t, ochainsync.Tip{}, h.ls.Tip())
	require.Equal(t, other.Hash().Bytes(),
		h.cm.PrimaryChain().Tip().Point.Hash,
		"the refused batch changed the primary chain")
	requireNothingCommitted(t, h)
}

func TestApplyTrustedBatchTooLarge(t *testing.T) {
	t.Parallel()

	h := newExternalIngestHarness(t, false)
	blocks, _ := externalIngestBlocks(t, 0)
	batch := make([]gledger.Block, MaxTrustedBatchBlocks+1)
	for i := range batch {
		batch[i] = blocks[0]
	}
	_, err := h.applyPaused(t, ocommon.Point{}, batch)
	require.ErrorIs(t, err, ErrTrustedBatchTooLarge)
	require.Zero(t, h.cm.PrimaryChain().Tip().Point.Slot,
		"an oversized batch must not store blocks")
	requireNothingCommitted(t, h)
}

func TestApplyTrustedBatchBelowMithrilBoundary(t *testing.T) {
	t.Parallel()

	h := newExternalIngestHarness(t, false)
	blocks, _ := externalIngestBlocks(t, 0)
	h.ls.Lock()
	h.ls.mithrilLedgerSlot = blocks[1].SlotNumber()
	h.ls.Unlock()
	_, err := h.applyPaused(t, ocommon.Point{}, blocks)
	require.ErrorIs(t, err, ErrTrustedBatchBelowMithrilBoundary)
	require.Zero(t, h.cm.PrimaryChain().Tip().Point.Slot,
		"a batch below the boundary must not store blocks")
	requireNothingCommitted(t, h)
}

// externalIngestLedgerState is the in-memory ledger state a batch leaves
// behind beside its stored state.
type externalIngestLedgerState struct {
	tip        ochainsync.Tip
	tipNonce   []byte
	epoch      models.Epoch
	epochCache []models.Epoch
	era        uint
	pparams    lcommon.ProtocolParameters
}

func captureExternalIngestLedgerState(
	ls *LedgerState,
) externalIngestLedgerState {
	ls.RLock()
	defer ls.RUnlock()
	return externalIngestLedgerState{
		tip:        cloneTip(ls.currentTip),
		tipNonce:   bytes.Clone(ls.currentTipBlockNonce),
		epoch:      ls.currentEpoch,
		epochCache: append([]models.Epoch(nil), ls.epochCache...),
		era:        ls.currentEra.Id,
		pparams:    ls.currentPParams,
	}
}

// prepareRollover gives the harness what an epoch rollover reads: reward
// parameters, a stored epoch 0 and an empty reward basis for it.
func (h *externalIngestHarness) prepareRollover(t *testing.T) {
	t.Helper()
	pparams := epochBoundaryBenchPParams()
	pparams.GovActionDeposit = 100_000_000
	h.ls.config.CardanoNodeConfig.ShelleyGenesisHash = strings.Repeat("42", 32)
	h.ls.Lock()
	h.ls.currentPParams = pparams
	epoch := h.ls.currentEpoch
	h.ls.publishSnapshotsLocked()
	h.ls.Unlock()
	require.NoError(t, h.db.SetEpoch(
		epoch.StartSlot, epoch.EpochId,
		epoch.Nonce, epoch.EvolvingNonce, nil, nil,
		epoch.EraId, epoch.SlotLength, epoch.LengthInSlots,
		nil,
	))
	seedEmptyRewardBasisForRollover(t, h.db, epoch, pparams)
}

// TestApplyTrustedBatchCrossesEpochMatchesReplay applies a batch spanning an
// epoch boundary, so it commits in the pre-boundary, rollover and
// post-boundary transactions. A transaction after the boundary spends an
// output produced before it, so that output is committed live by the first
// transaction and removed by the last. Stored and in-memory state must match
// the trusted replay path.
func TestApplyTrustedBatchCrossesEpochMatchesReplay(t *testing.T) {
	t.Parallel()
	for _, dropped := range []bool{false, true} {
		t.Run(fmt.Sprintf("catchup_indexes_dropped=%t", dropped),
			func(t *testing.T) {
				t.Parallel()
				testApplyTrustedBatchCrossesEpochMatchesReplay(t, dropped)
			})
	}
}

func testApplyTrustedBatchCrossesEpochMatchesReplay(
	t *testing.T,
	dropped bool,
) {
	blocks, names := externalIngestBlocks(t, 0)
	// Cross the slot-1000 boundary with empty blocks.
	blocks = extendExternalIngestBlocks(t, blocks, 20, 50)
	last := blocks[len(blocks)-1]
	require.GreaterOrEqual(t, last.SlotNumber(), uint64(1_000))
	names["late"] = newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{names["chain"].hash, uint64(0)}},
		1: []any{map[uint]any{
			0: rowBatchingBaseAddress(t, 0x44, 0x55), 1: uint64(200_000),
		}},
		2: uint64(100_000),
	})
	blocks = append(blocks, rowBatchingBlock(
		t, last.SlotNumber()+10, last.BlockNumber()+1, last.Hash(),
		names["late"],
	))
	blocks = extendExternalIngestBlocks(t, blocks, 2, 10)
	require.Less(t, blocks[0].SlotNumber(), uint64(1_000))

	reference := newExternalIngestHarness(t, true)
	reference.prepareRollover(t)
	referenceBatched := reference.countBatchedWrites()
	for _, block := range blocks {
		require.NoError(t, reference.cm.PrimaryChain().AddBlock(
			t.Context(), block, nil,
		))
	}
	batches := make(chan []gledger.Block, 1)
	batches <- blocks
	close(batches)
	require.NoError(t, reference.ls.ProcessTrustedBlockBatches(
		t.Context(), batches,
	))
	_, err := reference.db.UtxosDeleteConsumed(
		t.Context(), blocks[len(blocks)-1].SlotNumber(), 1_000, nil,
	)
	require.NoError(t, err)

	h := newExternalIngestHarness(t, false)
	if dropped {
		dropCatchUpIndexes(t, h.db)
	}
	h.prepareRollover(t)
	var commits atomic.Int64
	h.ls.afterLedgerWriteCommit = func() { commits.Add(1) }
	batched := h.countBatchedWrites()
	overlays := h.countOverlays()
	tip, err := h.applyPaused(t, ocommon.Point{}, blocks)
	require.NoError(t, err)
	require.Equal(t, blockPoint(blocks[len(blocks)-1]), tip)
	require.GreaterOrEqual(t, overlays.Load(), int64(2),
		"each side of the boundary must apply through the overlay")
	require.Zero(t, referenceBatched.Load())
	require.Positive(t, batched.Load(),
		"a trusted batch must write through the row-batching accumulator")
	require.GreaterOrEqual(t, commits.Load(), int64(3),
		"the batch must split at the epoch boundary")
	require.False(t, utxoRowExists(t, h.db, names["chain"].hash, 0),
		"an output spent after the boundary was kept")

	want := captureExternalIngestLedgerState(reference.ls)
	got := captureExternalIngestLedgerState(h.ls)
	require.Positive(t, got.epoch.EpochId, "the batch did not roll over")
	require.Equal(t, want, got,
		"external ingest diverged from trusted replay in memory")
	if dropped {
		// The run ends its cycle with the full rebuild; the digest
		// compares rows, and the marker is one.
		require.NoError(t, h.db.Metadata().(metadata.DeferredIndexManager).
			BuildDeferredIndexes())
	}
	require.Equal(t,
		rowBatchingDigest(t, reference.db),
		rowBatchingDigest(t, h.db),
		"external ingest diverged from trusted replay across an epoch",
	)
}

// restartDigest is rowBatchingDigest with block_nonce checkpoint marks
// cleared. Start resets the per-epoch checkpoint flag, so the first block a
// restarted ledger applies is marked as a checkpoint; that is restart
// behaviour, not something a batch decides.
func restartDigest(t *testing.T, db *database.Database) string {
	t.Helper()
	return strings.ReplaceAll(
		rowBatchingDigest(t, db), "|is_checkpoint=1", "|is_checkpoint=0",
	)
}

// liveUtxoKeys lists the live UTxO set as tx_id:index strings.
func liveUtxoKeys(t *testing.T, db *database.Database) []string {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	rows, err := raw.Query(
		`SELECT hex(tx_id) || ':' || output_idx FROM utxo
		 WHERE deleted_slot = 0 ORDER BY 1`,
	)
	require.NoError(t, err)
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		require.NoError(t, rows.Scan(&key))
		keys = append(keys, key)
	}
	require.NoError(t, rows.Err())
	return keys
}

// TestApplyTrustedBatchCrashRestart fails a batch's metadata commit after its
// blocks are stored, shuts the ledger and store down as a crash would, and
// restarts over the same data directory. Both recovery routes -- replaying
// the batch from the committed tip, and letting the live loop apply the
// stored blocks -- must reach the state of an uninterrupted run.
func TestApplyTrustedBatchCrashRestart(t *testing.T) {
	t.Parallel()

	blocks, _ := externalIngestBlocks(t, 4)
	first, second := blocks[:3], blocks[3:]
	firstTip := blockPoint(first[len(first)-1])
	lastTip := blockPoint(blocks[len(blocks)-1])

	crashed := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		h := newExternalIngestHarnessAt(t, false, dir)
		overlays := h.countOverlays()
		_, err := h.applyPaused(t, ocommon.Point{}, first)
		require.NoError(t, err)
		require.Positive(t, overlays.Load(),
			"the first batch must apply through the overlay")
		injected := errors.New("injected crash before metadata commit")
		h.ls.trustedBatchBeforeCommit = func([]gledger.Block) error {
			return injected
		}
		before := overlays.Load()
		_, err = h.applyPaused(t, firstTip, second)
		require.ErrorIs(t, err, injected)
		require.Greater(t, overlays.Load(), before,
			"the crashed batch must apply through the overlay")
		require.Equal(t, lastTip.Hash, h.cm.PrimaryChain().Tip().Point.Hash,
			"the blob write precedes the metadata commit")
		h.shutdown(t)
		return dir
	}

	t.Run("replay from committed tip", func(t *testing.T) {
		t.Parallel()
		clean := newExternalIngestHarness(t, false)
		_, err := clean.applyPaused(t, ocommon.Point{}, first)
		require.NoError(t, err)
		_, err = clean.applyPaused(t, firstTip, second)
		require.NoError(t, err)

		h := newExternalIngestHarnessAt(t, false, crashed(t))
		require.Equal(t, firstTip, h.ls.Tip().Point,
			"the restart did not resume from the committed tip")
		overlays := h.countOverlays()
		tip, err := h.applyPaused(t, h.ls.Tip().Point, second)
		require.NoError(t, err)
		require.Positive(t, overlays.Load(),
			"the replay after a restart must apply through the overlay")
		require.Equal(t, lastTip, tip)
		require.Equal(t, liveUtxoKeys(t, clean.db), liveUtxoKeys(t, h.db),
			"duplicate or missing UTxOs after restart")
		require.Equal(t,
			restartDigest(t, clean.db),
			restartDigest(t, h.db),
			"replay after a restart diverged from an uninterrupted run",
		)
	})

	t.Run("live loop applies stored blocks", func(t *testing.T) {
		t.Parallel()
		clean := newExternalIngestHarness(t, false)
		_, err := clean.applyPaused(t, ocommon.Point{}, first)
		require.NoError(t, err)
		clean.startLiveLoop(t)
		require.NoError(t, clean.cm.PrimaryChain().AddBlocks(
			t.Context(), second,
		))
		require.Eventually(t, func() bool {
			return pointMatches(clean.ls.Tip().Point, lastTip)
		}, 5*time.Second, 10*time.Millisecond)

		h := newExternalIngestHarnessAt(t, false, crashed(t))
		require.Equal(t, firstTip, h.ls.Tip().Point)
		h.startLiveLoop(t)
		require.Eventually(t, func() bool {
			return pointMatches(h.ls.Tip().Point, lastTip)
		}, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, liveUtxoKeys(t, clean.db), liveUtxoKeys(t, h.db),
			"duplicate or missing UTxOs after restart")
		require.Equal(t,
			restartDigest(t, clean.db),
			restartDigest(t, h.db),
			"the live loop after a restart diverged from an uninterrupted run",
		)
	})
}

func spentUtxoRowCount(t *testing.T, db *database.Database) int {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	return rowBatchingQueryOne[int](t, raw,
		`SELECT COUNT(*) FROM utxo WHERE deleted_slot != 0`)
}

// TestApplyTrustedBatchPrunesSpentRows spends, in later batches, outputs an
// earlier batch committed, so the spent rows are committed rather than
// dropped within a batch. The trusted cleanup after a batch must hard-delete
// them, persist the prune floor at the batch tip and leave the live UTxO set
// as a run without the cleanup has it; API storage mode must keep them.
func TestApplyTrustedBatchPrunesSpentRows(t *testing.T) {
	t.Parallel()

	blocks, _ := externalIngestBlocks(t, 2)
	tipOf := func(b []gledger.Block) ocommon.Point {
		return blockPoint(b[len(b)-1])
	}
	// run applies the scenario as four batches. Holding
	// cleanupConsumedUtxosRunning, as an in-flight periodic sweep does, keeps
	// the trusted cleanup out of a batch.
	run := func(
		t *testing.T, mode string, cleanup func(batch int) bool,
	) (*externalIngestHarness, []int) {
		t.Helper()
		h := newExternalIngestHarnessWith(t, false, t.TempDir(), mode, 0)
		batches := [][]gledger.Block{
			blocks[:3], blocks[3:4], blocks[4:5], blocks[5:],
		}
		spent := make([]int, 0, len(batches))
		tip := ocommon.Point{}
		for i, batch := range batches {
			h.ls.cleanupConsumedUtxosRunning.Store(!cleanup(i))
			var err error
			tip, err = h.applyPaused(t, tip, batch)
			require.NoError(t, err)
			require.Equal(t, tipOf(batch), tip)
			spent = append(spent, spentUtxoRowCount(t, h.db))
		}
		h.ls.cleanupConsumedUtxosRunning.Store(false)
		return h, spent
	}

	for _, tc := range []struct {
		name  string
		mode  string
		prune bool
	}{
		{name: "core", mode: types.StorageModeCore, prune: true},
		{name: "api", mode: types.StorageModeAPI, prune: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reference, kept := run(t, tc.mode, func(int) bool { return false })
			require.Positive(t, kept[1],
				"the second batch must spend a committed output")
			require.Greater(t, kept[2], kept[1],
				"the third batch must spend another committed output")

			// The second batch is held back from the cleanup, so its spent
			// row is still there when the third batch prunes.
			h, spent := run(t, tc.mode, func(batch int) bool {
				return batch != 1
			})
			floor, err := h.ls.readConsumedUtxoPruneFloor(nil)
			require.NoError(t, err)
			require.Equal(t,
				liveUtxoKeys(t, reference.db), liveUtxoKeys(t, h.db),
				"the cleanup changed the live UTxO set")
			if !tc.prune {
				require.Equal(t, kept, spent,
					"API storage mode must keep spent rows")
				require.Zero(t, floor)
				return
			}
			require.Equal(t, kept[1], spent[1])
			require.Zero(t, spent[2],
				"spent rows at or below the trusted tip were kept")
			require.Zero(t, spent[3])
			require.Equal(t, tipOf(blocks).Slot, floor,
				"the prune floor must be persisted at the batch tip")
		})
	}
}

// TestApplyTrustedBatchPruneKeepsUpWithDenseBatch spends, in one batch, more
// committed outputs than sixteen cleanup deletes remove. The cleanup after
// that batch must drain every prunable row rather than stop at a fixed number
// of deletes and leave the backlog to grow.
func TestApplyTrustedBatchPruneKeepsUpWithDenseBatch(t *testing.T) {
	t.Parallel()

	const outputs = 40
	seed := bytes.Repeat([]byte{0x7a}, 32)
	addr := rowBatchingBaseAddress(t, 0x44, 0x22)
	produced := make([]any, outputs)
	for i := range produced {
		produced[i] = map[uint]any{0: addr, 1: uint64(1_000_000)}
	}
	fanOut := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(0)}},
		1: produced,
		2: uint64(100_000),
	})
	spent := make([]any, outputs)
	for i := range spent {
		spent[i] = []any{fanOut.hash, uint64(i)}
	}
	fanIn := newRowBatchingTx(t, map[uint]any{
		0: spent,
		1: []any{map[uint]any{0: addr, 1: uint64(1_000_000)}},
		2: uint64(100_000),
	})
	b1 := rowBatchingBlock(t, 10, 1, lcommon.Blake2b256{}, fanOut)
	b2 := rowBatchingBlock(t, 20, 2, b1.Hash(), fanIn)

	h := newExternalIngestHarness(t, false)
	// One row per delete makes sixteen deletes fall short of this batch.
	h.ls.trustedPruneRows = 1
	tip, err := h.applyPaused(t, ocommon.Point{}, []gledger.Block{b1})
	require.NoError(t, err)
	tip, err = h.applyPaused(t, tip, []gledger.Block{b2})
	require.NoError(t, err)
	require.Equal(t, blockPoint(b2), tip)
	require.Zero(t, spentUtxoRowCount(t, h.db),
		"spent rows at or below the trusted tip were left behind")
	backlog, err := h.db.CountUtxosConsumedUpTo(t.Context(), tip.Slot, 100)
	require.NoError(t, err)
	require.Zero(t, backlog)
	require.Len(t, liveUtxoKeys(t, h.db), 1)
}
