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
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"maps"
	"math"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/indexer"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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

// anchorFixture is a split node bootstrapped the way `mithril sync` leaves
// one: the ledger database holds the chain and the ledger state at a later
// point than the anchor, and records the anchor with the import baseline of
// the accounts the snapshot held there. liveAtAnchor and liveAtTip are a
// shared-layout store's tables at the anchor and at the chain's end, which a
// split IndexerDB must reproduce.
type anchorFixture struct {
	consensus    *database.Database
	consensusRaw *sql.DB
	indexerDB    *database.Database
	indexerRaw   *sql.DB
	deps         dingo.IndexerWriterDeps
	anchor       models.Block
	tip          ochainsync.Tip
	liveAtAnchor map[string][]string
	liveAtTip    map[string][]string
}

// anchorSkipTables differ by construction: the IndexerDB takes the import
// baseline from the ledger database, a store that applied the blocks live
// holds none.
var anchorSkipTables = []string{"account_import_baseline"}

func newAnchorFixture(t *testing.T) *anchorFixture {
	t.Helper()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	f := &anchorFixture{}

	dirAnchor := copyImmutableChunkPrefix(t, anchorChunks)
	dirChain := copyImmutableChunkPrefix(t, chainChunks)

	// The oracle: blocks applied live to a shared-layout store, read at the
	// anchor and again at the end of the chain.
	live, liveRaw := newEquivalenceDB(t)
	require.NoError(t, LoadWithDB(
		ctx, &config.Config{Network: "preview"}, logger, dirAnchor, live,
	))
	anchorTip, err := live.GetTip(nil)
	require.NoError(t, err)
	f.liveAtAnchor = dumpTables(t, liveRaw)
	type baseline struct {
		tag             uint8
		key, pool, drep []byte
		drepType        int64
		active          bool
	}
	var baselines []baseline
	rs, err := liveRaw.Query(
		`SELECT credential_tag, staking_key, pool, drep, drep_type, active FROM account`,
	)
	require.NoError(t, err)
	for rs.Next() {
		var b baseline
		require.NoError(
			t,
			rs.Scan(&b.tag, &b.key, &b.pool, &b.drep, &b.drepType, &b.active),
		)
		baselines = append(baselines, b)
	}
	require.NoError(t, rs.Err())
	require.NoError(t, rs.Close())
	require.NoError(t, LoadWithDB(
		ctx, &config.Config{Network: "preview"}, logger, dirChain, live,
	))
	f.liveAtTip = dumpTables(t, liveRaw)
	for _, name := range anchorSkipTables {
		delete(f.liveAtAnchor, name)
		delete(f.liveAtTip, name)
	}

	dataDir := t.TempDir()
	selections := internalplugins.StorageSelections{
		Blob: plugin.Selection{
			Provider: "badger", Config: testutil.BadgerBlobConfig(),
		},
		Metadata: plugin.Selection{
			Provider: "sqlite",
			Config:   map[string]any{},
		},
	}
	deps := internalplugins.StorageDependencies{
		DataDir: dataDir, StorageMode: "core", Logger: logger,
	}
	dbConfig := &database.Config{
		DataDir: dataDir, Logger: logger, StorageMode: "core",
		IndexerLayout: "split", Network: "preview",
	}
	runtime, err := internalplugins.OpenDatabase(
		ctx,
		dbConfig,
		selections,
		deps,
	)
	require.NoError(t, err)
	f.consensus = runtime.Database
	require.NoError(t, LoadWithDB(
		ctx, &config.Config{Network: "preview"}, logger, dirChain, f.consensus,
	))
	f.tip, err = f.consensus.GetTip(nil)
	require.NoError(t, err)
	require.Greater(t, f.tip.Point.Slot, anchorTip.Point.Slot)

	f.anchor, err = database.BlockByHash(ctx, f.consensus, anchorTip.Point.Hash)
	require.NoError(t, err)
	require.NoError(t, f.consensus.SetSyncState(
		"mithril_ledger_slot", strconv.FormatUint(f.anchor.Slot, 10), nil,
	))
	require.NoError(t, f.consensus.SetSyncState(
		"mithril_ledger_hash", hex.EncodeToString(f.anchor.Hash), nil,
	))
	f.consensusRaw = openRaw(t, filepath.Join(dataDir, "metadata.sqlite"))
	for _, b := range baselines {
		_, err := f.consensusRaw.Exec(
			`INSERT INTO account_import_baseline
			(credential_tag, staking_key, pool, drep, drep_type, active, added_slot)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			b.tag, b.key, b.pool, b.drep, b.drepType, b.active, f.anchor.Slot,
		)
		require.NoError(t, err)
	}

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
	f.deps = dingo.IndexerWriterDeps{
		Consensus: f.consensus, Indexer: f.indexerDB, NodeConfig: nodeCfg,
		Logger: logger, MaxReadyLagBlocks: 10,
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

func TestSplitMithrilBootstrapMatchesSharedLayoutAtAnchor(t *testing.T) {
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
	// completed past the anchor is ahead of the shared layout's at that
	// point. It is compared at the end of the chain, where both are final.
	atAnchor := maps.Clone(f.liveAtAnchor)
	delete(atAnchor, "reward_ada_pots")
	got := dumpTables(t, f.indexerRaw)
	for _, name := range anchorSkipTables {
		delete(got, name)
	}
	delete(got, "reward_ada_pots")
	requireSameTables(t, atAnchor, got)

	// Following the ledger from the anchor reaches the shared layout's
	// state at the end of the chain.
	follow, _ := f.writer(t, nil, indexer.DefaultBatchBlocks)
	require.NoError(t, follow.CatchUp(ctx))
	tip, err = store.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, f.tip.Point.Hash, tip.Hash)
	f.requireTablesAt(t, f.liveAtTip)
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

func TestSplitMithrilBootstrapResumesAfterAStopMidReplayAndMidIndexBuild(
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
			if err := NewDeferredIndexRebuilder(
				ctx, f.indexerDB, f.deps.Logger,
			).BuildCritical(); err != nil {
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
	atAnchor := maps.Clone(f.liveAtAnchor)
	delete(atAnchor, "reward_ada_pots")
	got := dumpTables(t, f.indexerRaw)
	for _, name := range anchorSkipTables {
		delete(got, name)
	}
	delete(got, "reward_ada_pots")
	requireSameTables(t, atAnchor, got)
}

func TestSplitMithrilBootstrapRefusesRowsThatDisagreeWithTheImportedState(
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

// The ledger database's core-mode sweep hard-deletes spent UTxOs, so by the
// time the replay reaches the anchor it has lost the outputs that were live at
// the anchor and spent since. The digest taken when the anchor was recorded
// stands in for the ledger's set, so the sweep is not read as a mismatch.
func TestSplitMithrilBootstrapToleratesSweptSpentOutputs(t *testing.T) {
	t.Parallel()
	f := newAnchorFixture(t)
	ctx := context.Background()
	w, store := f.writer(t, f.cappedAtAnchor(), indexer.DefaultBatchBlocks)
	w.WithDeferredIndexes(
		func(ctx context.Context) error {
			NewDeferredIndexRebuilder(ctx, f.indexerDB, f.deps.Logger).Drop()
			return nil
		},
		func(ctx context.Context) error {
			return NewDeferredIndexRebuilder(
				ctx, f.indexerDB, f.deps.Logger,
			).BuildAll()
		},
	)
	require.NoError(t, w.Prepare(ctx))
	f.sweepSpentOutputs(t)

	require.NoError(t, w.CatchUp(ctx))
	phase, err := store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, indexerdb.PhaseDone, phase)
}

// A row the IndexerDB lost is still caught once the ledger has swept past the
// anchor: the comparison no longer depends on the ledger's current rows.
func TestSplitMithrilBootstrapDetectsCorruptionAfterTheSweep(t *testing.T) {
	t.Parallel()
	f := newAnchorFixture(t)
	ctx := context.Background()
	w, store := f.writer(t, f.cappedAtAnchor(), indexer.DefaultBatchBlocks)
	w.WithDeferredIndexes(
		func(ctx context.Context) error {
			NewDeferredIndexRebuilder(ctx, f.indexerDB, f.deps.Logger).Drop()
			return nil
		},
		func(ctx context.Context) error {
			if err := NewDeferredIndexRebuilder(
				ctx, f.indexerDB, f.deps.Logger,
			).BuildAll(); err != nil {
				return err
			}
			_, err := f.indexerRaw.Exec(
				`UPDATE utxo SET amount = '999999999' WHERE id = (SELECT MIN(id) FROM utxo WHERE deleted_slot = 0)`,
			)
			return err
		},
	)
	require.NoError(t, w.Prepare(ctx))
	f.sweepSpentOutputs(t)

	require.ErrorIs(t, w.CatchUp(ctx), indexer.ErrAnchorMismatch)
	phase, err := store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, indexerdb.PhaseIndex, phase)
}

// sweepSpentOutputs removes the outputs spent after the anchor from the
// ledger database and moves its prune floor past the anchor, as the core-mode
// sweep does.
func (f *anchorFixture) sweepSpentOutputs(t *testing.T) {
	t.Helper()
	res, err := f.consensusRaw.Exec(
		`DELETE FROM utxo WHERE deleted_slot > ?`, f.anchor.Slot,
	)
	require.NoError(t, err)
	swept, err := res.RowsAffected()
	require.NoError(t, err)
	require.Positive(t, swept, "the chain past the anchor must spend outputs")
	require.NoError(t, f.consensus.SetSyncState(
		database.ConsumedUtxoPruneFloorSyncKey,
		strconv.FormatUint(f.tip.Point.Slot, 10), nil,
	))
}

func TestSplitMithrilBootstrapAppliesTheImportedAccountState(t *testing.T) {
	t.Parallel()
	f := newAnchorFixture(t)
	ctx := context.Background()

	// An account the snapshot held on a pool other than the one its
	// certificates delegate to (a reaped pool's delegations are cleared by a
	// rule no certificate carries), with a balance; a later certificate then
	// moved it to a third pool, and credits and a withdrawal were journaled
	// since.
	var key, replayedPool, anchorPool, laterPool []byte
	require.NoError(t, f.consensusRaw.QueryRow(
		`SELECT staking_key, pool FROM account ORDER BY id LIMIT 1`,
	).Scan(&key, &replayedPool))
	require.NoError(t, f.consensusRaw.QueryRow(
		`SELECT pool FROM account WHERE pool != ? ORDER BY id LIMIT 1`,
		replayedPool,
	).Scan(&anchorPool))
	require.NoError(t, f.consensusRaw.QueryRow(
		`SELECT pool FROM account WHERE pool != ? AND pool != ? ORDER BY id LIMIT 1`,
		replayedPool,
		anchorPool,
	).Scan(&laterPool))
	_, err := f.consensusRaw.Exec(
		`UPDATE account_import_baseline SET pool = ? WHERE staking_key = ?`,
		anchorPool, key,
	)
	require.NoError(t, err)
	_, err = f.consensusRaw.Exec(
		`UPDATE account SET pool = ?, reward = '100', added_slot = ? WHERE staking_key = ?`,
		laterPool,
		f.tip.Point.Slot,
		key,
	)
	require.NoError(t, err)
	_, err = f.consensusRaw.Exec(
		`INSERT INTO account_reward_delta
		(staking_key, credential_tag, tx_hash, amount, added_slot, withdrawal, post_snapshot)
		VALUES (?, 0, ?, '30', ?, 0, 0), (?, 0, ?, '10', ?, 1, 0)`,
		key, []byte("credit"), f.anchor.Slot+1,
		key, []byte("withdrawal"), f.anchor.Slot+2,
	)
	require.NoError(t, err)

	w, _ := f.writer(t, f.cappedAtAnchor(), indexer.DefaultBatchBlocks)
	require.NoError(t, w.CatchUp(ctx))
	var pool []byte
	var reward string
	require.NoError(t, f.indexerRaw.QueryRow(
		`SELECT pool, reward FROM account WHERE staking_key = ?`, key,
	).Scan(&pool, &reward))
	require.Equal(
		t,
		anchorPool,
		pool,
		"the anchor holds the snapshot's delegation, neither the replayed nor the later one",
	)
	require.Equal(
		t,
		"80",
		reward,
		"the anchor balance is today's 100 less the later credit of 30 plus the later withdrawal of 10",
	)
	var baselines int
	require.NoError(t, f.indexerRaw.QueryRow(
		`SELECT COUNT(*) FROM account_import_baseline WHERE staking_key = ?`,
		key,
	).Scan(&baselines))
	require.Equal(t, 1, baselines)

	// Past the anchor the writer reaches the ledger's later delegation.
	follow, _ := f.writer(t, nil, indexer.DefaultBatchBlocks)
	require.NoError(t, follow.CatchUp(ctx))
	require.NoError(t, f.indexerRaw.QueryRow(
		`SELECT pool FROM account WHERE staking_key = ?`, key,
	).Scan(&pool))
	require.Equal(t, laterPool, pool)
}

func TestSplitMithrilBootstrapRecordsCertifiedOpCertCounters(t *testing.T) {
	t.Parallel()
	f := newAnchorFixture(t)
	ctx := context.Background()

	// A snapshot import writes one counter per certified pool at the anchor
	// slot. The pool below never issued a block in the fixture's chain.
	var certified lcommon.PoolKeyHash
	copy(certified[:], bytes.Repeat([]byte{0xC7}, len(certified)))
	require.NoError(t, f.consensus.UpdatePoolOpCertSequence(
		ctx, certified, 7, f.anchor.Slot, nil,
	))

	w, _ := f.writer(t, f.cappedAtAnchor(), indexer.DefaultBatchBlocks)
	require.NoError(t, w.CatchUp(ctx))

	var rows int
	require.NoError(t, f.indexerRaw.QueryRow(
		`SELECT COUNT(*) FROM pool_opcert_sequence WHERE pool_key_hash = ? AND slot = ?`,
		certified[:], f.anchor.Slot,
	).Scan(&rows))
	require.Equal(t, 1, rows, "the certified counter is written at the anchor slot")

	counts, total, err := f.indexerDB.Metadata().CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash{certified}, 0, math.MaxInt64, nil,
	)
	require.NoError(t, err)
	require.Zero(t, total)
	require.Zero(t, counts[string(certified[:])], "a certified counter is not a block")

	var blocks int
	require.NoError(t, f.indexerRaw.QueryRow(
		`SELECT COUNT(*) FROM pool_opcert_sequence WHERE pool_key_hash != ?`,
		certified[:],
	).Scan(&blocks))
	require.NotZero(t, blocks, "replay writes one row per block")
}

// A ledger that drained its accounts holds neither the imported balances nor
// the journal the anchor state is rebuilt from, so an empty IndexerDB refuses
// the bootstrap rather than take over an empty state.
func TestSplitMithrilBootstrapRefusesALedgerThatDrainedItsAccounts(
	t *testing.T,
) {
	t.Parallel()
	f := newAnchorFixture(t)
	ctx := context.Background()
	require.NoError(t, f.consensus.RecordDrainedFamily(
		ctx, nodesettings.FamilyDReps,
	))
	require.NoError(t, f.consensus.RecordDrainedFamily(
		ctx, nodesettings.FamilyAccounts,
	))

	w, store := f.writer(t, f.cappedAtAnchor(), indexer.DefaultBatchBlocks)
	require.ErrorIs(t, w.Prepare(ctx), indexer.ErrAnchorUnserved)
	phase, err := store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, indexerdb.PhaseNone, phase, "no anchor was recorded")
}
