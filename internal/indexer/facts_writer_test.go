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

package indexer

import (
	"context"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// recordingFacts logs the order in which the writer drives a FactsSource and
// the blocks it indexes between the calls.
type recordingFacts struct {
	boundaries []uint64
	log        *[]string
}

func (f *recordingFacts) Boundaries(
	_ context.Context, from, through uint64,
) ([]uint64, error) {
	var out []uint64
	for _, b := range f.boundaries {
		if b >= from && b <= through {
			out = append(out, b)
		}
	}
	return out, nil
}

func (f *recordingFacts) CopyFacts(
	_ context.Context, _ *database.Txn, from, through uint64,
) error {
	*f.log = append(*f.log, fmt.Sprintf("copy[%d,%d]", from, through))
	return nil
}

func (f *recordingFacts) PatchFacts(
	_ context.Context, _ *database.Txn, from, through uint64,
) error {
	*f.log = append(*f.log, fmt.Sprintf("patch[%d,%d]", from, through))
	return nil
}

func (f *recordingFacts) RollbackFacts(
	_ context.Context, _ *database.Txn, point ocommon.Point,
) error {
	*f.log = append(*f.log, fmt.Sprintf("rollback>%d", point.Slot))
	return nil
}

// loggingIndexer records each IndexBlocks call into the same log.
type loggingIndexer struct {
	*syncRowIndexer
	log *[]string
}

func (l *loggingIndexer) IndexBlocks(
	ctx context.Context, txn *database.Txn, blocks []models.Block,
) error {
	*l.log = append(*l.log, fmt.Sprintf(
		"index[%d..%d]", blocks[0].Number, blocks[len(blocks)-1].Number,
	))
	return l.syncRowIndexer.IndexBlocks(ctx, txn, blocks)
}

func TestWriterSplitsABatchAtEpochBoundaries(t *testing.T) {
	t.Parallel()
	chain := newFakeChain(12, "c") // block n is at slot n*10
	store := newTestStore(t)
	var log []string
	facts := &recordingFacts{boundaries: []uint64{0, 50, 90}, log: &log}
	idx := &loggingIndexer{syncRowIndexer: &syncRowIndexer{}, log: &log}
	w := NewWriter(store, chain, idx, nil, 100).WithFacts(facts)
	require.NoError(t, w.CatchUp(context.Background()))

	// The boundary at slot 50 falls between blocks 4 and 5 and the one at
	// slot 90 between blocks 8 and 9, so facts dated at a boundary are
	// copied after the blocks before it and before the blocks after it.
	require.Equal(t, []string{
		"copy[0,49]", "index[0..4]", "patch[0,49]",
		"copy[50,89]", "index[5..8]", "patch[50,89]",
		"copy[90,110]", "index[9..11]", "patch[90,110]",
	}, log)
}

func TestWriterCopiesFactsAfterTheTipOnLaterBatches(t *testing.T) {
	t.Parallel()
	chain := newFakeChain(6, "c")
	store := newTestStore(t)
	var log []string
	facts := &recordingFacts{log: &log}
	idx := &loggingIndexer{syncRowIndexer: &syncRowIndexer{}, log: &log}
	w := NewWriter(store, chain, idx, nil, 3).WithFacts(facts)
	require.NoError(t, w.CatchUp(context.Background()))
	// A later batch starts the fact range after the committed tip's slot,
	// so no fact is copied twice and none falls between batches.
	require.Equal(t, []string{
		"copy[0,20]", "index[0..2]", "patch[0,20]",
		"copy[21,50]", "index[3..5]", "patch[21,50]",
	}, log)
}

func TestWriterRollsBackFactsWithRows(t *testing.T) {
	t.Parallel()
	chain := newFakeChain(6, "a")
	store := newTestStore(t)
	var log []string
	facts := &recordingFacts{log: &log}
	idx := &loggingIndexer{syncRowIndexer: &syncRowIndexer{}, log: &log}
	w := NewWriter(store, chain, idx, nil, 100).WithFacts(facts)
	require.NoError(t, w.CatchUp(context.Background()))

	chain.extend(3, 3, "b") // blocks 3..5 replaced; block 2 stays canonical
	log = log[:0]
	require.NoError(t, w.CatchUp(context.Background()))
	// Facts dated after the surviving ancestor go with the rolled-back rows.
	require.Equal(t, []string{
		"rollback>20", "copy[21,50]", "index[3..5]", "patch[21,50]",
	}, log)
}

type genesisChain struct {
	*fakeChain
	fromGenesis bool
}

func (g genesisChain) FromGenesis(context.Context) (bool, error) {
	return g.fromGenesis, nil
}

func TestWriterSeedsGenesisOnlyFromGenesisAndOnlyOnce(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T, fromGenesis bool) (int, string) {
		t.Helper()
		store := newTestStore(t)
		chain := genesisChain{
			fakeChain:   newFakeChain(6, "g"),
			fromGenesis: fromGenesis,
		}
		var log []string
		seeded := 0
		idx := &loggingIndexer{syncRowIndexer: &syncRowIndexer{}, log: &log}
		w := NewWriter(store, chain, idx, nil, 2).
			WithGenesisSeed(func(context.Context, *database.Txn) error {
				seeded++
				log = append(log, "seed")
				return nil
			})
		require.NoError(t, w.CatchUp(context.Background()))
		return seeded, log[0]
	}
	seeded, first := run(t, true)
	require.Equal(t, 1, seeded, "three batches must seed once")
	require.Equal(t, "seed", first, "the seed precedes the first block")
	seeded, _ = run(t, false)
	require.Zero(t, seeded, "a chain imported from a snapshot is not seeded")
}

func TestWriterRollsBackSeedWithFailedFirstBatch(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := genesisChain{fakeChain: newFakeChain(4, "g"), fromGenesis: true}
	idx := &syncRowIndexer{failOnNumber: map[uint64]bool{1: true}}
	seeds := 0
	w := NewWriter(store, chain, idx, nil, 10).
		WithGenesisSeed(func(_ context.Context, txn *database.Txn) error {
			seeds++
			return txn.DB().SetSyncState("test_seed", "x", txn)
		})
	require.Error(t, w.CatchUp(context.Background()))
	v, err := store.Database().GetSyncState("test_seed", nil)
	require.NoError(t, err)
	require.Empty(t, v, "the seed commits with the first batch or not at all")
	require.Equal(t, 1, seeds)
}

func TestWriterReportsProgressAfterEachBatchAndWhenCaughtUp(t *testing.T) {
	t.Parallel()
	chain := newFakeChain(5, "p")
	store := newTestStore(t)
	var seen [][2]uint64
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 2).
		WithProgress(func(tip indexerdb.Tip, ledgerTip ochainsync.Tip) {
			seen = append(
				seen,
				[2]uint64{tip.BlockNumber, ledgerTip.BlockNumber},
			)
		})
	require.NoError(t, w.CatchUp(context.Background()))
	require.Equal(
		t,
		[][2]uint64{{1, 4}, {3, 4}, {4, 4}, {4, 4}},
		seen,
		"each committed batch reports, and the pass that finds nothing left reports the caught-up tip",
	)
}
