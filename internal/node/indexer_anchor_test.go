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

package node

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"maps"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/indexer"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

const (
	anchorChunks = 20
	chainChunks  = 30
)

// cappedSource presents the canonical chain as ending at limit while keeping
// the database source's anchor and origin capabilities.
type cappedSource struct {
	*indexer.DatabaseSource
	limit models.Block
}

func (s *cappedSource) LedgerTip(context.Context) (ochainsync.Tip, error) {
	return ochainsync.Tip{
		Point:       ocommon.Point{Slot: s.limit.Slot, Hash: s.limit.Hash},
		BlockNumber: s.limit.Number,
	}, nil
}

func (s *cappedSource) IsCanonical(
	ctx context.Context,
	hash []byte,
) (bool, error) {
	blk, err := s.DatabaseSource.BlockByHash(ctx, hash)
	if err != nil {
		return false, err
	}
	if blk.Slot > s.limit.Slot {
		return false, nil
	}
	return s.DatabaseSource.IsCanonical(ctx, hash)
}

func (s *cappedSource) BlocksAfter(
	ctx context.Context,
	after indexerdb.Tip,
	throughSlot uint64,
	limit int,
) ([]models.Block, error) {
	return s.DatabaseSource.BlocksAfter(
		ctx, after, min(throughSlot, s.limit.Slot), limit,
	)
}

// cancelingSource cancels its context once BlocksAfter has been called
// afterCalls times, which stops a replay between two batches.
type cancelingSource struct {
	indexer.ChainSource
	cancel     context.CancelFunc
	afterCalls int
	calls      int
}

func (s *cancelingSource) BlocksAfter(
	ctx context.Context,
	after indexerdb.Tip,
	throughSlot uint64,
	limit int,
) ([]models.Block, error) {
	s.calls++
	if s.calls > s.afterCalls {
		s.cancel()
	}
	return s.ChainSource.BlocksAfter(ctx, after, throughSlot, limit)
}

func (s *cancelingSource) Anchor(
	ctx context.Context,
) (indexerdb.Tip, bool, error) {
	return s.ChainSource.(indexer.AnchorChain).Anchor(ctx)
}

// anchorFixture is a node's storage as `mithril sync` leaves it: the ledger
// database holds the chain and, in its live UTxO set, the ledger state at a
// later point than the anchor it records. refAtAnchor and refAtTip give the
// tables of an IndexerDB indexed from genesis at the anchor and at the chain's
// end, which an IndexerDB bootstrapped at the anchor must reproduce.
type anchorFixture struct {
	consensus  *database.Database
	indexerDB  *database.Database
	indexerRaw *sql.DB
	deps       dingo.IndexerWriterDeps
	anchor     models.Block
	tip        ochainsync.Tip
}

// refAtAnchor and refAtTip are asked for when a test compares, not when it
// sets up, so the genesis index is built while the other tests run.
func (f *anchorFixture) refAtAnchor(t *testing.T) map[string][]string {
	t.Helper()
	return anchorReference(t).atAnchor
}

func (f *anchorFixture) refAtTip(t *testing.T) map[string][]string {
	t.Helper()
	return anchorReference(t).atTip
}

// anchorSkipTables differ by construction: the IndexerDB takes the import
// baseline from the ledger, one indexed from genesis holds none.
var anchorSkipTables = []string{"account_import_baseline"}

func newAnchorFixture(t *testing.T) *anchorFixture {
	t.Helper()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	f := &anchorFixture{}

	dataDir := anchorStoreTemplate.copyDir(t)
	dbConfig, selections, deps := storeConfig(dataDir)
	runtime, err := internalplugins.OpenDatabase(
		ctx,
		dbConfig,
		selections,
		deps,
	)
	require.NoError(t, err)
	f.consensus = runtime.Database
	anchorPt := anchorPoint(t)
	f.tip, err = f.consensus.GetTip(nil)
	require.NoError(t, err)
	require.Greater(t, f.tip.Point.Slot, anchorPt.Slot)

	f.anchor, err = database.BlockByHash(ctx, f.consensus, anchorPt.Hash)
	require.NoError(t, err)
	require.NoError(t, f.consensus.SetChainState(
		"mithril_ledger_slot", strconv.FormatUint(f.anchor.Slot, 10), nil,
	))
	require.NoError(t, f.consensus.SetChainState(
		"mithril_ledger_hash", hex.EncodeToString(f.anchor.Hash), nil,
	))
	f.indexerDB, err = internalplugins.OpenIndexerDatabase(
		ctx, runtime.Host, f.consensus, dbConfig, selections, deps,
	)
	require.NoError(t, err)
	f.indexerRaw = openRaw(
		t,
		filepath.Join(
			dataDir,
			internalplugins.IndexerDirName,
			"metadata.sqlite",
		),
	)
	t.Cleanup(func() {
		require.NoError(t, f.indexerDB.Close())
		require.NoError(t, runtime.Close(context.WithoutCancel(ctx)))
	})

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath(
			"preview",
		),
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	// The ledger serves the fact rows and the imported state at the anchor
	// from the live UTxO set, as it does in a node.
	factLedger, _ := openFactLedger(t, f.consensus)
	f.deps = dingo.IndexerWriterDeps{
		Consensus: f.consensus, FactRows: factLedger, Indexer: f.indexerDB,
		NodeConfig: nodeCfg, Logger: logger, MaxReadyLagBlocks: 10,
	}
	return f
}

// writer builds the node's writer over src, or over the ledger database when
// src is nil.
func (f *anchorFixture) writer(
	t *testing.T,
	src indexer.ChainSource,
	batchBlocks int,
) (*indexer.Writer, *indexerdb.Store) {
	t.Helper()
	if src == nil {
		src = indexer.NewDatabaseSource(f.consensus)
	}
	w, store, err := newIndexerWriter(f.deps, src, batchBlocks)
	require.NoError(t, err)
	return w, store
}

func (f *anchorFixture) cappedAtAnchor() *cappedSource {
	return &cappedSource{
		DatabaseSource: indexer.NewDatabaseSource(f.consensus),
		limit:          f.anchor,
	}
}

func (f *anchorFixture) requireTablesAt(
	t *testing.T,
	want map[string][]string,
) {
	t.Helper()
	got := dumpTables(t, f.indexerRaw)
	for _, name := range anchorSkipTables {
		delete(got, name)
	}
	requireSameTables(t, want, got)
}

func TestIndexerMithrilBootstrapMatchesAGenesisIndexAtAnchor(t *testing.T) {
	t.Parallel()
	f := newAnchorFixture(t)
	ctx := context.Background()

	// The ledger is past the anchor, so the replay must stop there: the
	// ledger's account rows have moved on and only the baseline holds the
	// anchor's.
	w, store := f.writer(t, f.cappedAtAnchor(), indexer.DefaultBatchBlocks)
	require.NoError(t, w.Prepare(ctx))
	ready := func() bool {
		ok, err := store.ReadyServing(indexerdb.KindHistory, f.anchor.Number)
		require.NoError(t, err)
		return ok
	}
	require.False(
		t,
		ready(),
		"the anchor is recorded before anything is indexed",
	)
	require.NoError(t, w.CatchUp(ctx))

	tip, err := store.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, f.anchor.Hash, tip.Hash)
	anchor, err := store.Anchor(nil)
	require.NoError(t, err)
	require.Equal(t, f.anchor.Hash, anchor.Hash)
	require.Equal(t, f.anchor.Number, anchor.BlockNumber)
	phase, err := store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, indexerdb.PhaseDone, phase)
	require.True(t, ready())
	// A reward pot dated before the anchor is copied with the rewards the
	// ledger filled in afterwards, so the pot of an epoch whose round
	// completed past the anchor is ahead of the reference's at that
	// point. It is compared at the end of the chain, where both are final.
	atAnchor := maps.Clone(f.refAtAnchor(t))
	delete(atAnchor, "reward_ada_pots")
	got := dumpTables(t, f.indexerRaw)
	for _, name := range anchorSkipTables {
		delete(got, name)
	}
	delete(got, "reward_ada_pots")
	requireSameTables(t, atAnchor, got)

	// Following the ledger from the anchor reaches the reference's
	// state at the end of the chain.
	follow, _ := f.writer(t, nil, indexer.DefaultBatchBlocks)
	require.NoError(t, follow.CatchUp(ctx))
	tip, err = store.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, f.tip.Point.Hash, tip.Hash)
	f.requireTablesAt(t, f.refAtTip(t))
}

var errStopped = errors.New("stopped")

func (f *anchorFixture) lazyIndexExists(t *testing.T) bool {
	t.Helper()
	return dbtest.MetadataIndexExists(
		t,
		f.indexerRaw,
		dbtest.LazyManifestIndex(t),
	)
}

func TestIndexerMithrilBootstrapResumesAfterAStopMidReplayAndMidIndexBuild(
	t *testing.T,
) {
	t.Parallel()
	f := newAnchorFixture(t)
	ctx := context.Background()
	lazy := func() bool { return f.lazyIndexExists(t) }
	require.True(
		t,
		lazy(),
		"precondition: the IndexerDB starts with its indexes",
	)

	// Building the service records the anchor, so a reader asking before the
	// writer's first pass is told the IndexerDB is not ready.
	built, err := NewIndexerService(f.deps)
	require.NoError(t, err)
	early, err := built.(*IndexerService).Store().ReadyServing(
		indexerdb.KindHistory, f.anchor.Number,
	)
	require.NoError(t, err)
	require.False(t, early, "not ready before the writer has run")

	// Stop between two replay batches.
	stopCtx, stop := context.WithCancel(ctx)
	defer stop()
	src := &cancelingSource{
		ChainSource: f.cappedAtAnchor(), cancel: stop, afterCalls: 3,
	}
	w, store := f.writer(t, src, 7)
	require.Error(t, w.CatchUp(stopCtx))
	tip, err := store.Tip(nil)
	require.NoError(t, err)
	require.Positive(t, tip.BlockNumber, "a batch must have committed")
	require.Less(
		t,
		tip.BlockNumber,
		f.anchor.Number,
		"the replay must not have finished",
	)
	phase, err := store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, indexerdb.PhaseReplay, phase)
	require.False(t, lazy(), "the indexes are dropped while the replay runs")
	ready, err := store.ReadyServing(indexerdb.KindHistory, f.anchor.Number)
	require.NoError(t, err)
	require.False(t, ready, "not ready before the anchor")

	// A restart replays to the anchor, then is stopped part-way through the
	// index build, after the critical subset and before the rest.
	w, store = f.writer(t, f.cappedAtAnchor(), 7)
	w.WithDeferredIndexes(
		func(ctx context.Context) error {
			NewDeferredIndexRebuilder(ctx, f.indexerDB, f.deps.Logger).Drop()
			return nil
		},
		func(ctx context.Context) error {
			builder, ok := f.indexerDB.Metadata().(metadata.ContextCriticalDeferredIndexBuilder)
			require.True(t, ok)
			if err := builder.BuildCriticalDeferredIndexesContext(ctx); err != nil {
				return err
			}
			return errStopped
		},
	)
	require.ErrorIs(t, w.CatchUp(ctx), errStopped)
	tip, err = store.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, f.anchor.Hash, tip.Hash)
	phase, err = store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, indexerdb.PhaseIndex, phase)
	require.False(t, lazy(), "the lazy indexes are still missing")
	ready, err = store.ReadyServing(indexerdb.KindHistory, f.anchor.Number)
	require.NoError(t, err)
	require.False(t, ready, "not ready while the indexes are being built")

	// The next start builds what is missing and finishes.
	w, store = f.writer(t, f.cappedAtAnchor(), 7)
	require.NoError(t, w.CatchUp(ctx))
	require.True(t, lazy())
	phase, err = store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, indexerdb.PhaseDone, phase)
	ready, err = store.ReadyServing(indexerdb.KindHistory, f.anchor.Number)
	require.NoError(t, err)
	require.True(t, ready)

	// The result is the one an uninterrupted bootstrap reaches.
	atAnchor := maps.Clone(f.refAtAnchor(t))
	delete(atAnchor, "reward_ada_pots")
	got := dumpTables(t, f.indexerRaw)
	for _, name := range anchorSkipTables {
		delete(got, name)
	}
	delete(got, "reward_ada_pots")
	requireSameTables(t, atAnchor, got)
}

func TestIndexerMithrilBootstrapRefusesRowsThatDisagreeWithTheImportedState(
	t *testing.T,
) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		corrupt string
		message string
	}{
		{
			"utxo set",
			`DELETE FROM utxo WHERE id = (SELECT MIN(id) FROM utxo WHERE deleted_slot = 0)`,
			"UTxO set",
		},
		{
			"transaction count",
			`DELETE FROM applied_transaction WHERE rowid = (SELECT MIN(rowid) FROM applied_transaction)`,
			"applied transactions",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newAnchorFixture(t)
			ctx := context.Background()
			w, store := f.writer(
				t,
				f.cappedAtAnchor(),
				indexer.DefaultBatchBlocks,
			)
			w.WithDeferredIndexes(
				func(ctx context.Context) error {
					NewDeferredIndexRebuilder(
						ctx,
						f.indexerDB,
						f.deps.Logger,
					).Drop()
					return nil
				},
				func(ctx context.Context) error {
					if err := NewDeferredIndexRebuilder(
						ctx, f.indexerDB, f.deps.Logger,
					).BuildAll(); err != nil {
						return err
					}
					_, err := f.indexerRaw.Exec(tc.corrupt)
					return err
				},
			)
			err := w.CatchUp(ctx)
			require.ErrorIs(t, err, indexer.ErrAnchorMismatch)
			require.ErrorContains(t, err, tc.message)
			phase, perr := store.Bootstrap(nil)
			require.NoError(t, perr)
			require.Equal(
				t,
				indexerdb.PhaseIndex,
				phase,
				"the bootstrap must not finish",
			)
			ready, rerr := store.ReadyServing(
				indexerdb.KindHistory,
				f.anchor.Number,
			)
			require.NoError(t, rerr)
			require.False(t, ready)
			// The failure is not recomputed on every pass.
			require.ErrorIs(t, w.CatchUp(ctx), indexer.ErrAnchorMismatch)
		})
	}
}

// The ledger-state families live in the live UTxO set, so a writer that is not
// given the ledger to read them through finds neither the imported balances
// nor the journal the anchor state is rebuilt from, and an empty IndexerDB
// refuses the bootstrap rather than take over an empty state.
func TestIndexerMithrilBootstrapRefusesAWriterWithoutTheLedgerState(
	t *testing.T,
) {
	t.Parallel()
	f := newAnchorFixture(t)
	ctx := context.Background()
	f.deps.FactRows = nil

	w, store := f.writer(t, f.cappedAtAnchor(), indexer.DefaultBatchBlocks)
	require.ErrorIs(t, w.Prepare(ctx), indexer.ErrAnchorUnserved)
	phase, err := store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, indexerdb.PhaseNone, phase, "no anchor was recorded")
}
