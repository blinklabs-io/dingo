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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
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
	closeSet func() error
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
	opts ...func(*LedgerStateConfig),
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
	set, closeSet := OpenLiveSetAtForTest(t, db, dataDir)
	lsCfg := LedgerStateConfig{
		Database:              db,
		ChainManager:          cm,
		UtxoSet:               set,
		CardanoNodeConfig:     cfg,
		Logger:                testLogger(),
		PromRegistry:          prometheus.NewRegistry(),
		ManualBlockProcessing: manual,
		TrustedReplay:         manual,
	}
	for _, opt := range opts {
		opt(&lsCfg)
	}
	ls, err := NewLedgerState(lsCfg)
	require.NoError(t, err)
	h := &externalIngestHarness{ls: ls, db: db, cm: cm, closeSet: closeSet}
	t.Cleanup(func() { h.shutdown(t) })
	nonce := bytes.Repeat([]byte{0x42}, 32)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = &conway.ConwayProtocolParameters{
		KeyDeposit:         2_000_000,
		PoolDeposit:        500_000_000,
		DRepDeposit:        500_000_000,
		GovActionDeposit:   100_000_000,
		MaxBlockBodySize:   90_112,
		MaxBlockHeaderSize: 1_100,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62_000_000,
			Steps:  20_000_000_000,
		},
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
		require.NoError(t, h.closeSet())
		require.NoError(t, dbtest.CloseDatabase(h.db))
	})
}

// startLiveLoop runs the ledger's chain-iterator loop the way Start does,
// without Start's reload of epoch and tip state from the empty store.
func (h *externalIngestHarness) startLiveLoop(t *testing.T) {
	t.Helper()
	// These fixtures model a live node at tip, so under-full batches should
	// not wait for catch-up coalescing (gatherCoalesceMaxGapAttempts).
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

// liveOutputs is the live UTxO set of h's ledger as tx_id:index strings,
// sorted.
func liveOutputs(t *testing.T, h *externalIngestHarness) []string {
	t.Helper()
	it, ok := h.ls.config.UtxoSet.(UtxoSetIterator)
	require.True(t, ok)
	var keys []string
	require.NoError(t, it.EachUtxo(func(txID []byte, idx uint32, _ []byte) error {
		keys = append(keys, fmt.Sprintf("%X:%d", txID, idx))
		return nil
	}))
	slices.Sort(keys)
	return keys
}

func outputLive(
	t *testing.T,
	h *externalIngestHarness,
	txHash []byte,
	idx uint32,
) bool {
	t.Helper()
	r, ok := h.ls.config.UtxoSet.(UtxoSetReader)
	require.True(t, ok)
	_, live, err := r.Get(txHash, idx)
	require.NoError(t, err)
	return live
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
	before := len(liveOutputs(t, h))

	stale := ocommon.NewPoint(5, bytes.Repeat([]byte{0x99}, 32))
	tip, err := h.applyPaused(t, stale, blocks)
	require.ErrorIs(t, err, ErrStaleBatch)
	require.Equal(t, ocommon.Point{}.Slot, tip.Slot)
	require.Equal(t, ochainsync.Tip{}, h.ls.Tip(), "the ledger tip moved")
	require.Equal(t, before, len(liveOutputs(t, h)), "the UTxO set changed")
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
	require.False(t, outputLive(t, h, names["register"].hash, 0),
		"an output created and spent within the batch is live")
	require.False(t, outputLive(t, h, names["spend"].hash, 1),
		"an output created and spent within the batch is live")
	require.True(t, outputLive(t, h, names["spend"].hash, 0))
	require.True(t, outputLive(t, h, names["chain"].hash, 0))
	// The phase-2-invalid transaction consumed only its collateral
	// (register:1) and produced only its collateral return (invalid:1),
	// which retspend then spent.
	require.True(t, outputLive(t, h, names["drep"].hash, 0),
		"an invalid transaction's regular input was consumed")
	require.False(t, outputLive(t, h, names["register"].hash, 1),
		"collateral created and spent within the batch is live")
	require.False(t, outputLive(t, h, names["invalid"].hash, 0),
		"an invalid transaction's regular output was produced")
	require.False(t, outputLive(t, h, names["invalid"].hash, 1),
		"a collateral return created and spent within the batch is live")
	require.True(t, outputLive(t, h, names["retspend"].hash, 0))
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
		tip, err := h.applyPaused(t, ocommon.Point{}, blocks)
		require.NoError(t, err)
		require.Equal(t, blockPoint(blocks[len(blocks)-1]), tip)
		assert.Positive(t, splits.Load())
		total := 0
		for _, n := range committed {
			assert.LessOrEqual(t, n, limit)
			total += n
		}
		assert.Equal(t, len(blocks), total, "a block applied twice or never")
		assert.Equal(t, int64(len(committed)), commits.Load(),
			"each sub-batch commits once")
		assert.Equal(t,
			rowBatchingDigest(t, clean.db),
			rowBatchingDigest(t, h.db),
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
	require.Empty(t, liveOutputs(t, h), "outputs were committed")
	require.Equal(t,
		blocks[len(blocks)-1].Hash().Bytes(),
		h.cm.PrimaryChain().Tip().Point.Hash,
		"the blob write precedes the metadata commit",
	)

	// Replay from the unchanged committed tip over the stored blobs.
	fail.Store(false)
	tip, err := h.applyPaused(t, ocommon.Point{}, blocks)
	require.NoError(t, err)
	require.Equal(t, blockPoint(blocks[len(blocks)-1]), tip)
	require.Equal(t,
		rowBatchingDigest(t, clean.db),
		rowBatchingDigest(t, h.db),
		"re-replay after the crash diverged from a clean run",
	)
}

// TestApplyTrustedBatchMatchesReplay requires ApplyTrustedBatch to leave the
// ledger as the trusted replay of the same blocks leaves it.
func TestApplyTrustedBatchMatchesReplay(t *testing.T) {
	t.Parallel()
	blocks, _ := externalIngestBlocks(t, 4)

	reference := newExternalIngestHarness(t, true)
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

	h := newExternalIngestHarness(t, false)
	tip, err := h.applyPaused(t, ocommon.Point{}, blocks)
	require.NoError(t, err)
	require.Equal(t, blockPoint(blocks[len(blocks)-1]), tip)

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
	testutil.RequireReceive(t, waiting, testutil.AsyncWait,
		"the live loop never reached the paused gate")
	require.Equal(t, ochainsync.Tip{}, h.ls.Tip(),
		"a block was applied while chain ingest was paused")

	resumeAgain()
	resume()
	resume() // idempotent
	last := blockPoint(blocks[len(blocks)-1])
	testutil.WaitForCondition(t, func() bool {
		return pointMatches(h.ls.Tip().Point, last)
	}, testutil.AsyncWait, "live fixture reaches its committed tip")
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
	testutil.WaitForCondition(t, func() bool {
		return pointMatches(h.ls.Tip().Point, last)
	}, testutil.AsyncWait, "live fixture reaches its committed tip")

	reference := newExternalIngestHarness(t, false)
	_, err = reference.applyPaused(t, ocommon.Point{}, blocks[:split])
	require.NoError(t, err)
	_, err = reference.applyPaused(
		t,
		blockPoint(blocks[split-1]),
		blocks[split:],
	)
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
	require.Empty(t, liveOutputs(t, h), "outputs were committed")
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
	require.NoError(t, seedDB(t, h.db).SetEpoch(
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

	h := newExternalIngestHarness(t, false)
	h.prepareRollover(t)
	var commits atomic.Int64
	h.ls.afterLedgerWriteCommit = func() { commits.Add(1) }
	tip, err := h.applyPaused(t, ocommon.Point{}, blocks)
	require.NoError(t, err)
	require.Equal(t, blockPoint(blocks[len(blocks)-1]), tip)
	require.GreaterOrEqual(t, commits.Load(), int64(3),
		"the batch must split at the epoch boundary")
	require.False(t, outputLive(t, h, names["chain"].hash, 0),
		"an output spent after the boundary was kept")

	want := captureExternalIngestLedgerState(reference.ls)
	got := captureExternalIngestLedgerState(h.ls)
	require.Positive(t, got.epoch.EpochId, "the batch did not roll over")
	require.Equal(t, want, got,
		"external ingest diverged from trusted replay in memory")
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
		_, err := h.applyPaused(t, ocommon.Point{}, first)
		require.NoError(t, err)
		injected := errors.New("injected crash before metadata commit")
		h.ls.trustedBatchBeforeCommit = func([]gledger.Block) error {
			return injected
		}
		_, err = h.applyPaused(t, firstTip, second)
		require.ErrorIs(t, err, injected)
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
		tip, err := h.applyPaused(t, h.ls.Tip().Point, second)
		require.NoError(t, err)
		require.Equal(t, lastTip, tip)
		require.Equal(t, liveOutputs(t, clean), liveOutputs(t, h),
			"duplicate or missing UTxOs after restart")
		require.Equal(t,
			restartDigest(t, clean.db),
			restartDigest(t, h.db),
			"replay after a restart diverged from an uninterrupted run",
		)
		require.Equal(t,
			ledgerDBDigest(t, clean.ls),
			ledgerDBDigest(t, h.ls),
			"the LedgerDB state after a restart diverged from an uninterrupted run",
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
		testutil.WaitForCondition(t, func() bool {
			return pointMatches(clean.ls.Tip().Point, lastTip)
		}, testutil.AsyncWait, "live fixture reaches its committed tip")

		h := newExternalIngestHarnessAt(t, false, crashed(t))
		require.Equal(t, firstTip, h.ls.Tip().Point)
		h.startLiveLoop(t)
		testutil.WaitForCondition(t, func() bool {
			return pointMatches(h.ls.Tip().Point, lastTip)
		}, testutil.AsyncWait, "live fixture reaches its committed tip")
		require.Equal(t, liveOutputs(t, clean), liveOutputs(t, h),
			"duplicate or missing UTxOs after restart")
		require.Equal(t,
			restartDigest(t, clean.db),
			restartDigest(t, h.db),
			"the live loop after a restart diverged from an uninterrupted run",
		)
		require.Equal(t,
			ledgerDBDigest(t, clean.ls),
			ledgerDBDigest(t, h.ls),
			"the LedgerDB state after a restart diverged from an uninterrupted run",
		)
	})
}

// A cancelled context makes the chain report every block absent. Storing
// must surface the cancellation instead of treating the batch as new blocks.
func TestStoreTrustedBatchBlocksCancelled(t *testing.T) {
	t.Parallel()

	h := newExternalIngestHarness(t, false)
	blocks, _ := externalIngestBlocks(t, 0)
	require.NoError(t, h.cm.PrimaryChain().AddBlocks(t.Context(), blocks))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := h.ls.storeTrustedBatchBlocks(ctx, ocommon.Point{}, blocks)
	require.ErrorIs(t, err, context.Canceled)
}
