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
	"fmt"
	"io"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/indexer"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/plugin"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// equivalenceChunks is the number of immutable chunks loaded: enough real
// preview blocks to cross an epoch boundary.
const equivalenceChunks = 110

// equivalenceSkipTables are bookkeeping tables that legitimately differ
// between a ledger store and an indexer store.
var equivalenceSkipTables = map[string]bool{
	"schema_migrations": true, "sqlite_sequence": true,
	"commit_timestamp": true, "node_settings": true,
	"node_settings_gate": true, "sync_state": true, "tip": true,
	"import_checkpoint": true, "backfill_checkpoint": true,
}

func newEquivalenceDB(t *testing.T) (*database.Database, *sql.DB) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		StorageMode: types.StorageModeAPI,
		DataDir:     t.TempDir(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Network:     "preview",
	})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	return db, raw
}

// newEquivalenceIndexer opens an IndexerDB the way the node does: its own
// metadata store over a read-only view of live's blob store, so the writer
// finds the stored parent of the block it resumes from.
func newEquivalenceIndexer(
	t *testing.T,
	live *database.Database,
) (*database.Database, *sql.DB) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := t.TempDir()
	host, err := internalplugins.NewHost()
	require.NoError(t, err)
	db, err := internalplugins.OpenIndexerDatabase(
		t.Context(),
		host,
		live,
		&database.Config{
			DataDir: dataDir, Logger: logger, Network: "preview",
		},
		internalplugins.StorageSelections{
			Metadata: plugin.Selection{
				Provider: "sqlite", Config: map[string]any{},
			},
		},
		internalplugins.StorageDependencies{
			DataDir: dataDir, StorageMode: "api", Logger: logger,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, host.Stop(context.WithoutCancel(t.Context())))
	})
	return db, openRaw(
		t,
		filepath.Join(
			dataDir,
			internalplugins.IndexerDirName,
			"metadata.sqlite",
		),
	)
}

// dumpTables renders every row of every compared table without its surrogate
// id, in a canonical order.
func dumpTables(t *testing.T, raw *sql.DB) map[string][]string {
	t.Helper()
	rs, err := raw.Query("SELECT name FROM sqlite_master WHERE type = 'table'")
	require.NoError(t, err)
	var names []string
	for rs.Next() {
		var n string
		require.NoError(t, rs.Scan(&n))
		if !equivalenceSkipTables[n] {
			names = append(names, n)
		}
	}
	require.NoError(t, rs.Err())
	require.NoError(t, rs.Close())
	out := make(map[string][]string, len(names))
	for _, n := range names {
		rows, err := raw.Query("SELECT * FROM `" + n + "`")
		require.NoError(t, err)
		cols, err := rows.Columns()
		require.NoError(t, err)
		var lines []string
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			var sb strings.Builder
			for i, c := range cols {
				if c == "id" {
					continue
				}
				fmt.Fprintf(&sb, "%s=%v|", c, vals[i])
			}
			lines = append(lines, sb.String())
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		sort.Strings(lines)
		out[n] = lines
	}
	return out
}

func requireSameTables(t *testing.T, want, got map[string][]string) {
	t.Helper()
	var names []string
	for n := range want {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if slices.Equal(want[n], got[n]) {
			continue
		}
		missing, extra := diffLines(want[n], got[n])
		t.Errorf(
			"table %s: live %d rows, writer %d rows; only in live: %v; only in writer: %v",
			n,
			len(want[n]),
			len(got[n]),
			firstN(missing, 2),
			firstN(extra, 2),
		)
	}
}

func diffLines(a, b []string) (onlyA, onlyB []string) {
	seen := map[string]int{}
	for _, l := range b {
		seen[l]++
	}
	for _, l := range a {
		if seen[l] > 0 {
			seen[l]--
			continue
		}
		onlyA = append(onlyA, l)
	}
	seen = map[string]int{}
	for _, l := range a {
		seen[l]++
	}
	for _, l := range b {
		if seen[l] > 0 {
			seen[l]--
			continue
		}
		onlyB = append(onlyB, l)
	}
	return
}

func firstN(s []string, n int) []string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func TestIndexerWriterMatchesLiveApplyOnRealBlocks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	live, liveRaw := newEquivalenceDB(t)
	dir := copyImmutableChunkPrefix(t, equivalenceChunks)
	require.NoError(t, LoadWithDB(
		ctx, &config.Config{Network: "preview"}, logger, dir, live,
	))

	idxDB, idxRaw := newEquivalenceIndexer(t, live)
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	store, err := indexerdb.NewStore(idxDB, idxDB, 10)
	require.NoError(t, err)
	facts, err := indexer.NewMetadataFacts(live, idxDB)
	require.NoError(t, err)
	bf := NewBackfill(idxDB, nodeCfg, logger)
	bf.SetDelegatorInactivityEnabled(false)
	w := indexer.NewWriter(
		store, indexer.NewDatabaseSource(live), bf, logger, 700,
	).WithFacts(facts).WithGenesisSeed(bf.SeedGenesis)
	require.NoError(t, w.CatchUp(ctx))

	tip, err := store.Tip(nil)
	require.NoError(t, err)
	ledgerTip, err := live.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, ledgerTip.Point.Hash, tip.Hash)

	want := dumpTables(t, liveRaw)
	for _, n := range slices.Sorted(maps.Keys(want)) {
		if len(want[n]) > 0 {
			t.Logf("compared %s: %d rows", n, len(want[n]))
		}
	}
	requireSameTables(t, want, dumpTables(t, idxRaw))

	// A rollback of the canonical chain must leave the same rows as the
	// ledger's own rollback to that block. The point is mid-epoch, past several
	// boundaries, so epoch facts, stake snapshots and reward state roll back.
	src := indexer.NewDatabaseSource(live)
	last := ledgerTip.Point.Slot
	var point models.Block
	for blocks, after := ([]models.Block)(nil), (indexerdb.Tip{}); ; {
		blocks, err = src.BlocksAfter(ctx, after, last*6/10, 500)
		require.NoError(t, err)
		if len(blocks) == 0 {
			break
		}
		point = blocks[len(blocks)-1]
		after = indexerdb.Tip{
			Slot:        point.Slot,
			BlockNumber: point.Number,
			Hash:        point.Hash,
		}
	}
	require.NotEmpty(t, point.Hash)
	cut := &truncatedSource{ChainSource: src, limit: point}

	rolled := indexer.NewWriter(store, cut, bf, logger, 700).
		WithFacts(facts).WithGenesisSeed(bf.SeedGenesis)
	rollBack := func() {
		require.NoError(t, rolled.CatchUp(ctx))
		tipNow, err := store.Tip(nil)
		require.NoError(t, err)
		require.Equal(t, point.Hash, tipNow.Hash)
	}
	rollBack()
	// Indexing forward again from the rollback point must reproduce the
	// original result: parameter resolution and epoch tracking restart
	// from the rolled-back state.
	require.NoError(t, w.CatchUp(ctx))
	again := dumpTables(t, idxRaw)
	// The ledger's rollback clears the reward pot of every epoch whose round
	// has no pool outputs (rewardPotWithoutOutputsResetQuery) and a forward
	// pass copies only rows dated after the rollback point, so the cleared
	// pots of earlier epochs stay cleared. That is the ledger's own result
	// of the same rollback, which the oracle below compares.
	delete(want, "reward_ada_pots")
	delete(again, "reward_ada_pots")
	requireSameTables(t, want, again)
	rollBack()

	// The ledger's own metadata rollback to the same point is the oracle.
	txn := live.Transaction(ctx, true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return live.RollbackMetadataAfterSlot(
			ctx, ocommon.Point{Slot: point.Slot, Hash: point.Hash}, 0, txn,
		)
	}))
	after := dumpTables(t, liveRaw)
	require.Less(t, len(after["epoch"]), len(want["epoch"]),
		"the rollback must remove epochs, or it checked nothing")
	requireSameTables(t, after, dumpTables(t, idxRaw))
}

// truncatedSource presents the canonical chain as ending at limit, as it
// does after a rollback to that block.
type truncatedSource struct {
	indexer.ChainSource
	limit models.Block
}

func (s *truncatedSource) LedgerTip(context.Context) (ochainsync.Tip, error) {
	return ochainsync.Tip{
		Point:       ocommon.Point{Slot: s.limit.Slot, Hash: s.limit.Hash},
		BlockNumber: s.limit.Number,
	}, nil
}

func (s *truncatedSource) IsCanonical(
	ctx context.Context,
	hash []byte,
) (bool, error) {
	blk, err := s.ChainSource.BlockByHash(ctx, hash)
	if err != nil {
		return false, err
	}
	if blk.Slot > s.limit.Slot {
		return false, nil
	}
	return s.ChainSource.IsCanonical(ctx, hash)
}

func (s *truncatedSource) BlocksAfter(
	ctx context.Context,
	after indexerdb.Tip,
	throughSlot uint64,
	limit int,
) ([]models.Block, error) {
	return s.ChainSource.BlocksAfter(
		ctx, after, min(throughSlot, s.limit.Slot), limit,
	)
}

func (s *truncatedSource) FromGenesis(ctx context.Context) (bool, error) {
	return s.ChainSource.(indexer.GenesisChain).FromGenesis(ctx)
}
