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
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// anchoredChain is a fake chain bootstrapped from a snapshot at block
// anchorNumber.
type anchoredChain struct {
	*fakeChain
	anchorNumber uint64
}

func (c *anchoredChain) Anchor(context.Context) (indexerdb.Tip, bool, error) {
	b := c.canonical[c.anchorNumber]
	return indexerdb.Tip{
		Slot:        b.Slot,
		BlockNumber: b.Number,
		Hash:        b.Hash,
	}, true, nil
}

// countingIndexer reports one transaction per indexed block.
type countingIndexer struct {
	*loggingIndexer
	last uint64
}

func (c *countingIndexer) IndexBlocks(
	ctx context.Context, txn *database.Txn, blocks []models.Block,
) error {
	c.last = uint64(len(blocks))
	return c.loggingIndexer.IndexBlocks(ctx, txn, blocks)
}

func (c *countingIndexer) LastIndexedTransactions() uint64 { return c.last }

// anchorFacts is a FactsSource that also takes over and verifies anchor state.
type anchorFacts struct {
	*recordingFacts
	verifyErr error
	// digestErr, when set, is what the anchor digest capture fails with.
	digestErr error
	// verifiedAgainst is the digest VerifyAnchor was handed, nil when the
	// capture produced none.
	verifiedAgainst *metadata.UtxoSetDigest
}

func (f *anchorFacts) CopyAnchorState(
	_ context.Context, _ *database.Txn, anchor uint64,
) error {
	*f.log = append(*f.log, fmt.Sprintf("copy-anchor@%d", anchor))
	return nil
}

func (f *anchorFacts) RequireAnchorState(context.Context) error { return nil }

func (f *anchorFacts) CopyAnchorOpCerts(
	_ context.Context,
	_ *database.Txn,
	anchor uint64,
) error {
	*f.log = append(*f.log, fmt.Sprintf("copy-opcerts@%d", anchor))
	return nil
}

func (f *anchorFacts) CaptureAnchorDigest(
	_ context.Context, anchor uint64,
) (metadata.UtxoSetDigest, error) {
	*f.log = append(*f.log, fmt.Sprintf("capture@%d", anchor))
	if f.digestErr != nil {
		return metadata.UtxoSetDigest{}, f.digestErr
	}
	return metadata.UtxoSetDigest{Count: 7}, nil
}

func (f *anchorFacts) VerifyAnchor(
	_ context.Context, anchor, replayed uint64,
	captured *metadata.UtxoSetDigest,
) error {
	f.verifiedAgainst = captured
	*f.log = append(
		*f.log,
		fmt.Sprintf("verify@%d replayed=%d", anchor, replayed),
	)
	return f.verifyErr
}

type anchorRig struct {
	chain *anchoredChain
	store *indexerdb.Store
	log   *[]string
	w     *Writer
	facts *anchorFacts
	// indexedAtBuild is the block numbers indexed when the indexes were built.
	indexedAtBuild []uint64
}

func newAnchorRig(t *testing.T, blocks int, anchorNumber uint64) *anchorRig {
	t.Helper()
	return newAnchorRigWithStore(t, blocks, anchorNumber, newTestStore(t))
}

func newAnchorRigWithStore(
	t *testing.T,
	blocks int,
	anchorNumber uint64,
	store *indexerdb.Store,
) *anchorRig {
	t.Helper()
	log := &[]string{}
	chain := &anchoredChain{
		fakeChain: newFakeChain(blocks, "c"), anchorNumber: anchorNumber,
	}
	facts := &anchorFacts{recordingFacts: &recordingFacts{log: log}}
	idx := &countingIndexer{
		loggingIndexer: &loggingIndexer{
			syncRowIndexer: &syncRowIndexer{},
			log:            log,
		},
	}
	r := &anchorRig{chain: chain, store: store, log: log, facts: facts}
	r.w = NewWriter(store, chain, idx, nil, 5).
		WithFacts(facts).
		WithDeferredIndexes(
			func(context.Context) error { *log = append(*log, "drop"); return nil },
			func(context.Context) error {
				*log = append(*log, "build")
				r.indexedAtBuild = slices.Clone(idx.indexed)
				return nil
			},
		)
	return r
}

func TestWriterReplaysToTheAnchorBeforeFollowingTheLedger(t *testing.T) {
	t.Parallel()
	r := newAnchorRig(t, 30, 12)
	require.NoError(t, r.w.CatchUp(context.Background()))

	var anchorLog []string
	for _, entry := range *r.log {
		switch {
		case entry == "drop", entry == "build", entry == "copy-anchor@120",
			entry == "copy-opcerts@120",
			entry[:min(len(entry), 6)] == "verify":
			anchorLog = append(anchorLog, entry)
		}
	}
	// The certified counters are read when the anchor is recorded, before the
	// replay. Blocks 0..12 are 13 transactions of replay; the indexes come
	// back and the rows are checked at the anchor slot before the imported
	// state is applied.
	require.Equal(t, []string{
		"copy-opcerts@120", "drop", "build", "verify@120 replayed=13",
		"copy-anchor@120",
	}, anchorLog)

	// Nothing past the anchor is indexed before the indexes are built.
	require.Equal(
		t,
		[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		r.indexedAtBuild,
	)
	tip, err := r.store.Tip(nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(29),
		tip.BlockNumber,
		"the ledger is then followed to its tip",
	)
	phase, err := r.store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, indexerdb.PhaseDone, phase)
	ok, err := r.store.ReadyServing(indexerdb.KindHistory, 29)
	require.NoError(t, err)
	require.True(t, ok)
}

// A ledger that can no longer rebuild its UTxO set at the anchor leaves the
// replay uncompared with it, and the bootstrap still finishes; any other
// capture failure stops it.
func TestWriterBootstrapsWithoutAnAnchorDigestTheLedgerCannotRebuild(t *testing.T) {
	t.Parallel()
	r := newAnchorRig(t, 30, 12)
	r.facts.digestErr = fmt.Errorf(
		"%w: journal starts later", metadata.ErrAnchorDigestUnavailable,
	)
	require.NoError(t, r.w.CatchUp(context.Background()))
	require.Nil(t, r.facts.verifiedAgainst)
	phase, err := r.store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, indexerdb.PhaseDone, phase)

	other := newAnchorRig(t, 30, 12)
	other.facts.digestErr = errors.New("read failed")
	require.ErrorContains(
		t, other.w.CatchUp(context.Background()), "read failed",
	)
	phase, err = other.store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, indexerdb.PhaseNone, phase, "no anchor was recorded")
}

func TestWriterAdoptsAStoreWithHistoryWithoutReplaying(t *testing.T) {
	t.Parallel()
	r := newAnchorRig(t, 30, 12)
	// A store adopted from a v0.94.2 API database already holds its history.
	txn := r.store.Database().Transaction(context.Background(), true)
	b := r.chain.canonical[20]
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return r.store.SetTip(
			indexerdb.Tip{Slot: b.Slot, BlockNumber: b.Number, Hash: b.Hash},
			txn,
		)
	}))
	require.NoError(t, r.w.CatchUp(context.Background()))

	for _, entry := range *r.log {
		require.NotEqual(
			t,
			"drop",
			entry,
			"no replay for a store that has history",
		)
		require.NotEqual(t, "build", entry)
	}
	phase, err := r.store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, indexerdb.PhaseDone, phase)
	anchor, err := r.store.Anchor(nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(12),
		anchor.BlockNumber,
		"readiness still waits on the anchor",
	)
}

func TestWriterRefusesToReplayAChainThatStartsLater(t *testing.T) {
	t.Parallel()
	r := newAnchorRig(t, 30, 12)
	// The first block's parent is stored: the chain does not begin at the
	// origin, so seeding genesis state under it would be wrong.
	r.chain.canonical[0].PrevHash = []byte("parent")
	r.chain.stored["parent"] = models.Block{Hash: []byte("parent")}
	err := r.w.CatchUp(context.Background())
	require.ErrorContains(t, err, "does not start at the chain origin")
	tip, terr := r.store.Tip(nil)
	require.NoError(t, terr)
	require.True(t, tip.IsZero())
}

func TestWriterKeepsAVerificationFailure(t *testing.T) {
	t.Parallel()
	r := newAnchorRig(t, 30, 12)
	r.facts.verifyErr = fmt.Errorf("%w: injected", ErrAnchorMismatch)
	require.ErrorIs(t, r.w.CatchUp(context.Background()), ErrAnchorMismatch)
	phase, err := r.store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(
		t,
		indexerdb.PhaseIndex,
		phase,
		"the bootstrap must not finish",
	)
	ok, err := r.store.ReadyServing(indexerdb.KindHistory, 29)
	require.NoError(t, err)
	require.False(t, ok)

	verifies := func() int {
		n := 0
		for _, e := range *r.log {
			if len(e) >= 6 && e[:6] == "verify" {
				n++
			}
		}
		return n
	}
	before := verifies()
	require.ErrorIs(t, r.w.CatchUp(context.Background()), ErrAnchorMismatch)
	require.Equal(
		t,
		before,
		verifies(),
		"a later pass does not redo the checks",
	)
	tip, err := r.store.Tip(nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(12),
		tip.BlockNumber,
		"nothing is indexed past a failed anchor",
	)
}

// debitingIndexer records the withdrawal-debit mode each IndexBlocks call ran
// under.
type debitingIndexer struct {
	*countingIndexer
	debit bool
	modes []bool
}

func (d *debitingIndexer) DebitWithdrawals(on bool) { d.debit = on }

func (d *debitingIndexer) IndexBlocks(
	ctx context.Context, txn *database.Txn, blocks []models.Block,
) error {
	for range blocks {
		d.modes = append(d.modes, d.debit)
	}
	return d.countingIndexer.IndexBlocks(ctx, txn, blocks)
}

// The replay up to a Mithril anchor has no reward credits before it, so its
// withdrawals must not debit balances the anchor then sets; every block after
// the anchor replays the credits and must debit.
func TestWriterDebitsWithdrawalsOnlyPastTheAnchor(t *testing.T) {
	t.Parallel()
	log := &[]string{}
	chain := &anchoredChain{fakeChain: newFakeChain(30, "c"), anchorNumber: 12}
	store := newTestStore(t)
	facts := &anchorFacts{recordingFacts: &recordingFacts{log: log}}
	idx := &debitingIndexer{countingIndexer: &countingIndexer{
		loggingIndexer: &loggingIndexer{
			syncRowIndexer: &syncRowIndexer{},
			log:            log,
		},
	}}
	w := NewWriter(store, chain, idx, nil, 5).WithFacts(facts)
	require.NoError(t, w.CatchUp(context.Background()))

	require.Len(t, idx.modes, 30)
	for number, debited := range idx.modes {
		require.Equal(t, number > 12, debited, "block %d", number)
	}
}

// plannerStatsProbe records the incremental planner-statistics runs the
// metadata store is asked for. Embedding only the interface keeps the
// store's other optional statistics interfaces out of the wrapper's method
// set, so the Writer can reach the incremental form alone.
type plannerStatsProbe struct {
	metadata.MetadataStore
	mu       sync.Mutex
	triggers []string
}

func (p *plannerStatsProbe) OptimizePlannerStatsContext(
	ctx context.Context, trigger string,
) (types.PlannerStatsResult, error) {
	p.mu.Lock()
	p.triggers = append(p.triggers, trigger)
	p.mu.Unlock()
	return p.MetadataStore.(metadata.IncrementalPlannerStatsUpdater).
		OptimizePlannerStatsContext(ctx, trigger)
}

func (p *plannerStatsProbe) runs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.triggers)
}

// The statistics the API plans with after the backfill must not be the last
// periodic ones: reaching the anchor takes exactly one more, bounded pass.
func TestWriterTakesOneFinalPlannerStatsPassAtTheAnchor(t *testing.T) {
	t.Parallel()
	probe := &plannerStatsProbe{}
	db, err := dbtest.NewDatabaseWithMetadataWrapper(
		t,
		dbtest.Options{Config: &database.Config{}},
		func(m metadata.MetadataStore) metadata.MetadataStore {
			probe.MetadataStore = m
			return probe
		},
	)
	require.NoError(t, err)
	store, err := indexerdb.NewStore(db, db, 10)
	require.NoError(t, err)
	r := newAnchorRigWithStore(t, 30, 12, store)
	require.NoError(t, r.w.CatchUp(context.Background()))
	require.Equal(t, []string{"anchor"}, probe.runs())

	// Following the ledger past the anchor takes no further pass.
	require.NoError(t, r.w.CatchUp(context.Background()))
	require.Equal(t, []string{"anchor"}, probe.runs())
}

// fullPassProbe stands in for a backend with no incremental statistics form
// (PostgreSQL, MySQL): OptimizePlannerStatsContext reports Supported=false and
// only the full refresh does anything.
type fullPassProbe struct {
	metadata.MetadataStore
	mu   sync.Mutex
	full int
}

func (p *fullPassProbe) OptimizePlannerStatsContext(
	context.Context, string,
) (types.PlannerStatsResult, error) {
	return types.PlannerStatsResult{}, nil
}

func (p *fullPassProbe) UpdatePlannerStatsContext(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.full++
	return nil
}

func (p *fullPassProbe) fullRuns() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.full
}

// A backend without the incremental form still gets its final pass at the
// anchor, through the full refresh.
func TestWriterFallsBackToAFullPlannerStatsPassAtTheAnchor(t *testing.T) {
	t.Parallel()
	probe := &fullPassProbe{}
	db, err := dbtest.NewDatabaseWithMetadataWrapper(
		t,
		dbtest.Options{Config: &database.Config{}},
		func(m metadata.MetadataStore) metadata.MetadataStore {
			probe.MetadataStore = m
			return probe
		},
	)
	require.NoError(t, err)
	store, err := indexerdb.NewStore(db, db, 10)
	require.NoError(t, err)
	r := newAnchorRigWithStore(t, 30, 12, store)
	require.NoError(t, r.w.CatchUp(context.Background()))
	require.Equal(t, 1, probe.fullRuns())
}

// failingStatsProbe fails the anchor's planner-statistics pass: the
// incremental form with incrementalErr, or, when that is nil, reports the form
// unsupported and fails the full refresh with fullErr.
type failingStatsProbe struct {
	metadata.MetadataStore
	incrementalErr error
	fullErr        error
}

func (p *failingStatsProbe) OptimizePlannerStatsContext(
	context.Context, string,
) (types.PlannerStatsResult, error) {
	return types.PlannerStatsResult{Supported: p.incrementalErr != nil},
		p.incrementalErr
}

func (p *failingStatsProbe) UpdatePlannerStatsContext(context.Context) error {
	return p.fullErr
}

// warnRecorder keeps the warning records a logger emits.
type warnRecorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *warnRecorder) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn
}

func (h *warnRecorder) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *warnRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *warnRecorder) WithGroup(string) slog.Handler { return h }

// errorFor returns the error attribute of the first record with message msg.
func (h *warnRecorder) errorFor(msg string) (error, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Message != msg {
			continue
		}
		var found error
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "error" {
				found, _ = a.Value.Any().(error)
				return false
			}
			return true
		})
		return found, true
	}
	return nil, false
}

// The anchor is durable once PhaseDone commits, so a failed statistics pass
// at the anchor is logged and catch-up still reaches the ledger tip, whichever
// form of the pass failed.
func TestWriterLogsAFailedAnchorPlannerStatsPass(t *testing.T) {
	t.Parallel()
	injected := errors.New("injected planner statistics failure")
	for name, probe := range map[string]*failingStatsProbe{
		"incremental": {incrementalErr: injected},
		"full":        {fullErr: injected},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db, err := dbtest.NewDatabaseWithMetadataWrapper(
				t,
				dbtest.Options{Config: &database.Config{}},
				func(m metadata.MetadataStore) metadata.MetadataStore {
					probe.MetadataStore = m
					return probe
				},
			)
			require.NoError(t, err)
			store, err := indexerdb.NewStore(db, db, 10)
			require.NoError(t, err)
			r := newAnchorRigWithStore(t, 30, 12, store)
			warnings := &warnRecorder{}
			r.w.logger = slog.New(warnings)

			require.NoError(t, r.w.CatchUp(context.Background()))
			logged, ok := warnings.errorFor(
				"indexer final planner statistics failed",
			)
			require.True(t, ok, "the failed pass is logged")
			require.ErrorIs(t, logged, injected)
			phase, err := r.store.Bootstrap(nil)
			require.NoError(t, err)
			require.Equal(t, indexerdb.PhaseDone, phase)
			tip, err := r.store.Tip(nil)
			require.NoError(t, err)
			require.Equal(t, uint64(29), tip.BlockNumber)
		})
	}
}
