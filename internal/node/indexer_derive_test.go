package node

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/stretchr/testify/require"
)

// factTableNames are the tables of ledger-derived facts the IndexerDB holds.
func factTableNames() []string {
	var names []string
	for _, ft := range metadata.FactTables {
		names = append(names, ft.Name)
	}
	names = append(names, "account_reward_delta")
	sort.Strings(names)
	return names
}

var (
	boundaryCol = regexp.MustCompile(`\|boundary_slot=(\d+)\|`)
	potsRewards = regexp.MustCompile(`\|rewards=\d+\|`)
)

// factDump returns the fact tables' rows. A reward round whose boundary the
// chain has not reached is left out, and so is the reward pot of the epoch the
// chain ends in: the ledger computes both ahead of the boundary, in a
// background job, and the IndexerDB derives them when it crosses it.
func factDump(t *testing.T, raw *sql.DB) map[string][]string {
	t.Helper()
	dump := dumpNamedTables(t, raw, factTableNames())
	var last uint64
	for _, l := range dump["epoch_summary"] {
		if m := boundaryCol.FindStringSubmatch(l); m != nil {
			n, err := strconv.ParseUint(m[1], 10, 64)
			require.NoError(t, err)
			last = max(last, n)
		}
	}
	for _, table := range []string{
		"reward_pool_output", "reward_account_output",
	} {
		var kept []string
		for _, l := range dump[table] {
			m := boundaryCol.FindStringSubmatch(l)
			n, _ := strconv.ParseUint(m[1], 10, 64)
			if n <= last {
				kept = append(kept, l)
			}
		}
		dump[table] = kept
	}
	for i, l := range dump["reward_ada_pots"] {
		if strings.Contains(l, "captured_slot="+strconv.FormatUint(last, 10)+"|") {
			dump["reward_ada_pots"][i] = potsRewards.ReplaceAllString(l, "|rewards=-|")
		}
	}
	sort.Strings(dump["reward_ada_pots"])
	return dump
}

func renderFacts(t *testing.T, dump map[string][]string) string {
	t.Helper()
	var sb strings.Builder
	for _, n := range factTableNames() {
		for _, l := range dump[n] {
			sb.WriteString(n + "\t" + l + "\n")
		}
	}
	return sb.String()
}

// deriveParts are the store, block indexer and deriver of a node's writer over
// idx, with no ledger beside it.
func deriveParts(
	t *testing.T,
	idx *database.Database,
) (*indexerdb.Store, *Backfill, *indexer.Deriver) {
	t.Helper()
	return deriveInactivityParts(t, idx, 0)
}

// deriveInactivityParts is deriveParts with CIP-0163 delegator inactivity on
// and the given window in epochs, or off for a zero window.
func deriveInactivityParts(
	t *testing.T,
	idx *database.Database,
	window uint64,
) (*indexerdb.Store, *Backfill, *indexer.Deriver) {
	t.Helper()
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	store, err := indexerdb.NewStore(idx, idx, 10)
	require.NoError(t, err)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	bf := NewBackfill(idx, nodeCfg, logger)
	bf.SetDelegatorInactivityEnabled(window > 0)
	bf.SetDelegatorInactivityWindow(window)
	d, err := indexer.NewDeriver(idx, indexer.DeriveConfig{
		Logger: logger, NodeConfig: nodeCfg,
		DelegatorInactivityEnabled: window > 0,
		DelegatorInactivity:        window,
	})
	require.NoError(t, err)
	return store, bf, d
}

// deriveWriterOver is a writer reading src: block rows come from replaying
// the blocks, and every ledger fact from crossing the epoch boundaries through
// the epoch engine.
func deriveWriterOver(
	store *indexerdb.Store,
	src indexer.ChainSource,
	bi indexer.BlockIndexer,
	bf *Backfill,
	d *indexer.Deriver,
	batch int,
) *indexer.Writer {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return indexer.NewWriter(store, src, bi, logger, batch).
		WithDeriver(d).WithGenesisSeed(bf.SeedGenesis)
}

func deriveWriter(
	t *testing.T,
	consensus, idx *database.Database,
) (*indexer.Writer, *indexerdb.Store) {
	t.Helper()
	store, bf, d := deriveParts(t, idx)
	return deriveWriterOver(
		store, indexer.NewDatabaseSource(consensus), bf, bf, d, 700,
	), store
}

// readGolden returns the rows the ledger computed for the fixture chain.
func readGolden(t *testing.T) map[string][]string {
	t.Helper()
	golden, err := os.ReadFile("testdata/preview_facts.golden")
	require.NoError(t, err)
	want := map[string][]string{}
	for l := range strings.SplitSeq(strings.TrimSuffix(string(golden), "\n"), "\n") {
		table, row, _ := strings.Cut(l, "\t")
		want[table] = append(want[table], row)
	}
	for _, rows := range want {
		sort.Strings(rows)
	}
	return want
}

func requireGoldenFacts(t *testing.T, raw *sql.DB) {
	t.Helper()
	want, got := readGolden(t), factDump(t, raw)
	for _, n := range factTableNames() {
		if len(want[n]) == 0 && len(got[n]) == 0 {
			continue
		}
		require.Equal(t, want[n], got[n], "table %s", n)
	}
}

// The facts the IndexerDB derives by replaying the preview chain equal the
// rows the ledger computed for it, recorded in the golden file: epochs and
// summaries, pool stake snapshots, ADA pots, reward inputs and outputs, credit
// rounds, network state and protocol parameters.
func TestDerivedFactsEqualTheLedgers(t *testing.T) {
	t.Parallel()
	consensus := chainStoreTemplate.openEquivalence(t)
	idx, raw := newEquivalenceIndexer(t, consensus)
	w, _ := deriveWriter(t, consensus, idx)
	require.NoError(t, w.CatchUp(context.Background()))

	requireGoldenFacts(t, raw)
}

// pointAt is the last canonical block at or below the given fraction of the
// chain's last slot.
func pointAt(
	t *testing.T,
	src indexer.ChainSource,
	last uint64,
	num, den uint64,
) models.Block {
	t.Helper()
	var point models.Block
	for blocks, after := ([]models.Block)(nil), (indexerdb.Tip{}); ; {
		var err error
		blocks, err = src.BlocksAfter(
			context.Background(), after, last*num/den, 500,
		)
		require.NoError(t, err)
		if len(blocks) == 0 {
			break
		}
		point = blocks[len(blocks)-1]
		after = indexerdb.Tip{
			Slot: point.Slot, BlockNumber: point.Number, Hash: point.Hash,
		}
	}
	require.NotEmpty(t, point.Hash)
	return point
}

// A writer restarted at several points of the chain, with small batches,
// derives every fact: nothing depends on the position another writer held.
func TestDerivedFactsSurviveRestarts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	consensus := chainStoreTemplate.openEquivalence(t)
	idx, raw := newEquivalenceIndexer(t, consensus)
	src := indexer.NewDatabaseSource(consensus)
	tip, err := consensus.GetTip(nil)
	require.NoError(t, err)
	for _, frac := range [][2]uint64{{1, 5}, {1, 2}, {9, 10}, {1, 1}} {
		point := pointAt(t, src, tip.Point.Slot, frac[0], frac[1])
		// A writer built from nothing but the IndexerDB, as after a restart.
		store, bf, d := deriveParts(t, idx)
		w := deriveWriterOver(
			store, &truncatedSource{ChainSource: src, limit: point}, bf, bf, d, 37,
		)
		require.NoError(t, w.CatchUp(ctx))
		got, err := store.Tip(nil)
		require.NoError(t, err)
		require.Equal(t, point.Hash, got.Hash)
	}
	requireGoldenFacts(t, raw)
}

// failAfterIndex fails the failOn-th IndexBlocks call after it has written its
// rows, so the writer rolls the whole batch back.
type failAfterIndex struct {
	*Backfill
	calls  int
	failOn int
}

func (f *failAfterIndex) IndexBlocks(
	ctx context.Context,
	txn *database.Txn,
	blocks []models.Block,
) error {
	if err := f.Backfill.IndexBlocks(ctx, txn, blocks); err != nil {
		return err
	}
	f.calls++
	if f.calls == f.failOn {
		return errors.New("database or disk is full")
	}
	return nil
}

// A batch that fails after the boundary facts were derived in it leaves no
// trace, and the retry derives them again: the facts equal those of a pass
// that never failed.
func TestDerivedFactsResumeAfterAFailedBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	consensus := chainStoreTemplate.openEquivalence(t)
	idx, raw := newEquivalenceIndexer(t, consensus)
	store, bf, d := deriveParts(t, idx)
	failing := &failAfterIndex{Backfill: bf, failOn: 4}
	w := deriveWriterOver(
		store, indexer.NewDatabaseSource(consensus), failing, bf, d, 700,
	)
	require.Error(t, w.CatchUp(ctx))
	require.NoError(t, w.CatchUp(ctx))
	require.GreaterOrEqual(t, failing.calls, 5)
	requireGoldenFacts(t, raw)
}

// A rollback below the writer's tip removes the facts dated after it, and
// indexing forward again derives them again: the rows equal those of a writer
// that never held the abandoned blocks, and then those of an uninterrupted
// pass.
func TestDerivedFactsFollowARollback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	consensus := chainStoreTemplate.openEquivalence(t)
	idx, raw := newEquivalenceIndexer(t, consensus)
	src := indexer.NewDatabaseSource(consensus)
	store, bf, d := deriveParts(t, idx)
	w := deriveWriterOver(store, src, bf, bf, d, 700)
	require.NoError(t, w.CatchUp(ctx))
	full := factDump(t, raw)

	tip, err := consensus.GetTip(nil)
	require.NoError(t, err)
	point := pointAt(t, src, tip.Point.Slot, 6, 10)
	cut := &truncatedSource{ChainSource: src, limit: point}
	rolled := deriveWriterOver(store, cut, bf, bf, d, 700)
	require.NoError(t, rolled.CatchUp(ctx))
	got, err := store.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, point.Hash, got.Hash)
	short := factDump(t, raw)
	require.Less(t, len(short["epoch"]), len(full["epoch"]),
		"the rollback must remove epochs, or it checked nothing")

	// A writer that only ever saw the shorter chain is the oracle.
	oracleIdx, oracleRaw := newEquivalenceIndexer(t, consensus)
	oStore, oBf, oD := deriveParts(t, oracleIdx)
	require.NoError(t, deriveWriterOver(oStore, cut, oBf, oBf, oD, 700).CatchUp(ctx))
	require.Equal(t, factDump(t, oracleRaw), short)

	require.NoError(t, w.CatchUp(ctx))
	require.Equal(t, full, factDump(t, raw))
	requireGoldenFacts(t, raw)
}
