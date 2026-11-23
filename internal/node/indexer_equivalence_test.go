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

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/blinklabs-io/dingo/internal/nodestorage"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/plugin"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
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

// newEquivalenceDB opens a consensus store as a node's is: core mode, with
// the ledger state in the live UTxO set a load opens beside it.
func newEquivalenceDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		StorageMode:   types.StorageModeCore,
		IndexerLayout: config.IndexerLayoutSplit,
		DataDir:       t.TempDir(),
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Network:       "preview",
	})
	require.NoError(t, err)
	return db
}

// equivalenceLoadConfig is the configuration of the API node whose database
// the tests load, so the live UTxO set keeps the reward records the IndexerDB
// copies.
func equivalenceLoadConfig() *config.Config {
	return &config.Config{Network: "preview", StorageMode: "api"}
}

// equivalenceParts are the store, the fact source and the block indexer a
// node's writer over consensus and idxDB is built from, with the fact rows
// read through ls.
func equivalenceParts(
	t *testing.T,
	consensus, idxDB *database.Database,
	ls *ledger.LedgerState,
) (*indexerdb.Store, indexer.FactsSource, *Backfill) {
	t.Helper()
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	store, err := indexerdb.NewStore(idxDB, idxDB, 10)
	require.NoError(t, err)
	facts, err := indexer.NewMetadataFacts(ls, idxDB)
	require.NoError(t, err)
	bf := NewBackfill(
		idxDB, nodeCfg, slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	bf.SetDelegatorInactivityEnabled(false)
	return store, facts, bf
}

// newEquivalenceWriter is the writer a node runs over the parts, reading the
// chain through src.
func newEquivalenceWriter(
	store *indexerdb.Store,
	src indexer.ChainSource,
	facts indexer.FactsSource,
	bf *Backfill,
) *indexer.Writer {
	return indexer.NewWriter(
		store, src, bf, slog.New(slog.NewTextHandler(io.Discard, nil)), 700,
	).WithFacts(facts).WithGenesisSeed(bf.SeedGenesis)
}

// indexFromGenesis fills a new IndexerDB over consensus by one uninterrupted
// pass of the writer from genesis through src, which is how a node started
// over a loaded database fills its own. It is the reference the other writer
// paths (a retry, a rollback, a Mithril bootstrap) are compared with.
func indexFromGenesis(
	t *testing.T,
	consensus *database.Database,
	ls *ledger.LedgerState,
	src indexer.ChainSource,
) (*database.Database, *sql.DB) {
	t.Helper()
	idxDB, idxRaw := newEquivalenceIndexer(t, consensus)
	store, facts, bf := equivalenceParts(t, consensus, idxDB, ls)
	if src == nil {
		src = indexer.NewDatabaseSource(consensus)
	}
	require.NoError(
		t,
		newEquivalenceWriter(store, src, facts, bf).
			CatchUp(context.Background()),
	)
	return idxDB, idxRaw
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
	return dumpNamedTables(t, raw, names)
}

// dumpProjections replace a table's own columns where one holds a dictionary
// id, which is a node-local surrogate that differs with write order, with the
// value it names.
var dumpProjections = map[string]string{
	"utxo_address": "SELECT ua.utxo_id, a.address, ua.tx_id, ua.output_idx, " +
		"ua.slot, ua.block_index FROM utxo_address ua " +
		"JOIN address a ON a.id = ua.address_id",
	"utxo": "SELECT u.tx_id, u.output_idx, u.collateral_return_for_tx_id, " +
		"u.spent_at_tx_id, pc.hash AS payment_key, sc.hash AS staking_key, " +
		"u.credential_tag, d.hash AS datum_hash, u.amount, u.payment_script, " +
		"u.added_slot, u.deleted_slot FROM utxo u " +
		"LEFT JOIN credential pc ON pc.id = u.payment_key_id " +
		"LEFT JOIN credential sc ON sc.id = u.staking_key_id " +
		"LEFT JOIN datum d ON d.id = u.datum_id",
	"address_transaction": "SELECT pc.hash AS payment_key, a.payment_script, " +
		"sc.hash AS staking_key, a.credential_tag, a.transaction_id, a.slot, " +
		"a.tx_index FROM address_transaction a " +
		"LEFT JOIN credential pc ON pc.id = a.payment_key_id " +
		"LEFT JOIN credential sc ON sc.id = a.staking_key_id",
	"pool_opcert_sequence": "SELECT c.hash AS pool_key_hash, o.slot, " +
		"o.sequence FROM pool_opcert_sequence o " +
		"JOIN credential c ON c.id = o.pool_key_id",
}

// dumpNamedTables is dumpTables over the named tables.
func dumpNamedTables(
	t *testing.T,
	raw *sql.DB,
	names []string,
) map[string][]string {
	t.Helper()
	out := make(map[string][]string, len(names))
	for _, n := range names {
		query := "SELECT * FROM `" + n + "`"
		if projected, ok := dumpProjections[n]; ok {
			query = projected
		}
		rows, err := raw.Query(query)
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

// A rollback of the canonical chain leaves the rows an IndexerDB indexed only
// up to the rollback point holds, and indexing forward again reproduces the
// rows of an uninterrupted pass. That the writer's rows are those a ledger
// applying the blocks itself wrote is proven against a released version's
// database by TestMigratedNodeServesTheSameAPIsAsAFreshOne.
func TestIndexerWriterRollbackMatchesAFreshIndex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	consensus := chainStoreTemplate.openEquivalence(t)
	ls, _ := openFactLedger(t, consensus)

	idxDB, idxRaw := newEquivalenceIndexer(t, consensus)
	// One block indexer serves both writers, as a node's does across a
	// rollback: its running epoch state follows the rows it wrote.
	store, facts, bf := equivalenceParts(t, consensus, idxDB, ls)
	src := indexer.NewDatabaseSource(consensus)
	w := newEquivalenceWriter(store, src, facts, bf)
	require.NoError(t, w.CatchUp(ctx))

	tip, err := store.Tip(nil)
	require.NoError(t, err)
	ledgerTip, err := consensus.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, ledgerTip.Point.Hash, tip.Hash)

	want := dumpTables(t, idxRaw)
	for _, n := range slices.Sorted(maps.Keys(want)) {
		if len(want[n]) > 0 {
			t.Logf("compared %s: %d rows", n, len(want[n]))
		}
	}

	// The point is mid-epoch, past several boundaries, so epoch facts, stake
	// snapshots and reward state roll back.
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

	rolled := newEquivalenceWriter(store, cut, facts, bf)
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
	// The rollback clears the reward pot of every epoch whose round has no
	// pool outputs (rewardPotWithoutOutputsResetQuery), and a forward pass
	// copies only rows dated after the rollback point, so the cleared pots of
	// earlier epochs stay cleared, while an index that was never rolled back
	// copied them complete.
	delete(want, "reward_ada_pots")
	delete(again, "reward_ada_pots")
	requireSameTables(t, want, again)
	rollBack()

	// An IndexerDB that never held the rolled-back blocks is the oracle.
	_, shortRaw := indexFromGenesis(t, consensus, ls, cut)
	after := dumpTables(t, shortRaw)
	require.Less(t, len(after["epoch"]), len(want["epoch"]),
		"the rollback must remove epochs, or it checked nothing")
	rolledBack := dumpTables(t, idxRaw)
	delete(after, "reward_ada_pots")
	delete(rolledBack, "reward_ada_pots")
	requireSameTables(t, after, rolledBack)
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

// openFactLedger opens the ledger over a loaded consensus database and its
// live UTxO set, as a node does at startup. The ledger-state families live in
// the set, so the ledger is what a node's writer reads its fact rows from.
// The returned function closes the ledger and the set; it must run before
// the database is loaded again or closed.
func openFactLedger(
	t *testing.T,
	consensus *database.Database,
) (*ledger.LedgerState, func()) {
	t.Helper()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	set, _, err := nodestorage.OpenUtxoSet(nodestorage.UtxoSetConfig{
		DataDir:             consensus.DataDir(),
		Logger:              logger,
		CardanoNodeConfig:   nodeCfg,
		RetainRewardRecords: true,
	})
	require.NoError(t, err)
	cm, err := chain.NewManager(ctx, consensus, nil)
	require.NoError(t, err)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:              consensus,
		UtxoSet:               set,
		ChainManager:          cm,
		Logger:                logger,
		CardanoNodeConfig:     nodeCfg,
		Network:               "preview",
		PromRegistry:          prometheus.NewRegistry(),
		ManualBlockProcessing: true,
	})
	require.NoError(t, err)
	require.NoError(t, (nodestorage.Reconcile{
		DB:      consensus,
		Set:     set,
		DataDir: consensus.DataDir(),
		Logger:  logger,
		Publish: func(mask nodesettings.FamilyMask) {
			ls.SetDrainedFamilies(mask)
			set.SetDrainedFamilies(mask)
		},
		Resume: ls.ResumeUtxoSetFrom,
	}).Run(ctx))
	require.NoError(t, ls.Start(ctx))
	closed := false
	closeLedger := func() {
		if closed {
			return
		}
		closed = true
		require.NoError(t, ls.Close())
		require.NoError(t, set.Close())
	}
	t.Cleanup(closeLedger)
	return ls, closeLedger
}
