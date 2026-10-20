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

package mithril

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBatchApplier stands in for the ledger. It accepts a batch only at its
// current tip, as ApplyTrustedBatch does, and records every block it applies
// so a test can see a block applied twice.
type fakeBatchApplier struct {
	mu        sync.Mutex
	tip       ocommon.Point
	maxBlocks int
	applied   []ocommon.Point
	batches   [][]ocommon.Point
	// beforeApply runs under the lock on every call that reaches the apply
	// step. Returning a non-nil error fails the call with it.
	beforeApply func(a *fakeBatchApplier, blocks []ledger.Block) error
}

func (a *fakeBatchApplier) Tip() ocommon.Point {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.tip
}

func (a *fakeBatchApplier) ApplyTrustedBatch(
	_ context.Context,
	expected ocommon.Point,
	blocks []ledger.Block,
) (ocommon.Point, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !samePoint(expected, a.tip) {
		return a.tip, fmt.Errorf("fake: tip moved: %w", ErrStaleBatch)
	}
	if a.maxBlocks > 0 && len(blocks) > a.maxBlocks {
		return a.tip, fmt.Errorf(
			"fake: batch of %d blocks over limit %d", len(blocks), a.maxBlocks,
		)
	}
	if a.beforeApply != nil {
		if err := a.beforeApply(a, blocks); err != nil {
			return a.tip, err
		}
	}
	a.applyLocked(blocks)
	return a.tip, nil
}

// applyLocked records blocks as applied and moves the tip to the last one.
func (a *fakeBatchApplier) applyLocked(blocks []ledger.Block) {
	batch := make([]ocommon.Point, 0, len(blocks))
	for _, b := range blocks {
		p := blockPoint(b)
		a.applied = append(a.applied, p)
		batch = append(batch, p)
		a.tip = p
	}
	a.batches = append(a.batches, batch)
}

func (a *fakeBatchApplier) snapshot() ([]ocommon.Point, [][]ocommon.Point) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]ocommon.Point(nil), a.applied...),
		append([][]ocommon.Point(nil), a.batches...)
}

func samePoint(a, b ocommon.Point) bool {
	return a.Slot == b.Slot && bytes.Equal(a.Hash, b.Hash)
}

// streamChain is the certified chain a fixture serves, read through a feed on
// a second, identical fixture so the stream run's request log stays clean.
type streamChain struct {
	points []ocommon.Point
	chunk  map[string]uint64 // point hash -> immutable file number
	last   map[uint64]ocommon.Point
}

func (c streamChain) chunkOf(p ocommon.Point) uint64 {
	return c.chunk[string(p.Hash)]
}

func readStreamChain(t *testing.T, opts v2FixtureOptions) streamChain {
	t.Helper()
	ref := newV2Fixture(t, opts)
	feed := openFeed(t, t.Context(), feedConfig(ref, t.TempDir(), 0, 0, 0))
	chain := streamChain{
		chunk: map[string]uint64{},
		last:  map[uint64]ocommon.Point{},
	}
	for {
		c, err := feed.Next(nextCtx(t))
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		for _, b := range c.Blocks {
			p := blockPoint(b)
			chain.points = append(chain.points, p)
			chain.chunk[string(p.Hash)] = c.Num
			chain.last[c.Num] = p
		}
		require.NoError(t, feed.Release(c.Num))
	}
	return chain
}

func streamFixtureOptions(
	last uint64,
	blocksPerChunk func(uint64) int,
) v2FixtureOptions {
	return v2FixtureOptions{
		immutableFileNumber: last,
		validAllImmutables:  true,
		blocksPerChunk:      blocksPerChunk,
	}
}

func streamConfig(
	fixture *v2Fixture,
	dir string,
	db *database.Database,
	applier BatchApplier,
	batchChunks, window int,
) StreamConfig {
	return StreamConfig{
		Feed:        feedConfig(fixture, dir, 0, window, 0),
		BatchChunks: batchChunks,
		DB:          db,
		Applier:     applier,
		Logger:      slog.New(slog.DiscardHandler),
	}
}

func requireMarker(t *testing.T, db *database.Database, want uint64) {
	t.Helper()
	got, ok, err := getImmutableImportMarker(db)
	require.NoError(t, err)
	require.True(t, ok, "import marker is not set")
	require.Equal(t, want, got)
}

func requireDirEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	require.Empty(t, names, "download dir still holds files")
}

func TestStreamCatchUpFromEmpty(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		batchChunks   int
		window        int
		wantBatchLens []int
	}{
		{"two chunks per batch", 2, 4, []int{4, 4, 2}},
		{"one chunk per batch", 1, 0, []int{2, 2, 2, 2, 2}},
		// A batch wider than the window would wait forever on a chunk the
		// window holds back, so the window closes the batch first.
		{"batch wider than window", 4, 2, []int{4, 4, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opts := streamFixtureOptions(4, nil)
			chain := readStreamChain(t, opts)
			fixture := newV2Fixture(t, opts)
			db := newSyncModeTestDB(t)
			dir := t.TempDir()
			applier := &fakeBatchApplier{}

			res, err := RunStreamCatchUp(t.Context(), streamConfig(
				fixture, dir, db, applier, tc.batchChunks, tc.window,
			))
			require.NoError(t, err)

			applied, batches := applier.snapshot()
			assert.Equal(t, chain.points, applied)
			lens := make([]int, 0, len(batches))
			for _, b := range batches {
				lens = append(lens, len(b))
			}
			assert.Equal(t, tc.wantBatchLens, lens)
			certifiedTip := chain.points[len(chain.points)-1]
			assert.True(t, samePoint(certifiedTip, res.Tip), "result tip")
			assert.True(t, samePoint(certifiedTip, applier.Tip()), "ledger tip")
			assert.Equal(t, uint64(4), res.Immutable)
			assert.Equal(t, 0, res.Restarts)
			requireMarker(t, db, 4)
			requireDirEmpty(t, dir)
		})
	}
}

func TestStreamCatchUpResumeFromMarker(t *testing.T) {
	t.Parallel()
	opts := streamFixtureOptions(6, nil)
	chain := readStreamChain(t, opts)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	require.NoError(t, setImmutableImportMarker(db, 3))
	applier := &fakeBatchApplier{tip: chain.last[3]}

	res, err := RunStreamCatchUp(t.Context(), streamConfig(
		fixture, t.TempDir(), db, applier, 2, 2,
	))
	require.NoError(t, err)

	applied, _ := applier.snapshot()
	require.NotEmpty(t, applied)
	assert.Equal(t, uint64(4), chain.chunkOf(applied[0]))
	var want []ocommon.Point
	for _, p := range chain.points {
		if chain.chunkOf(p) > 3 {
			want = append(want, p)
		}
	}
	assert.Equal(t, want, applied)
	for _, num := range fixture.requestedImmutables() {
		assert.Greater(t, num, uint64(3), "requested chunk %d", num)
	}
	assert.Equal(t, uint64(6), res.Immutable)
	requireMarker(t, db, 6)
}

func TestStreamCatchUpStaleRestart(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// blocks is the number of blocks per chunk.
		blocks int
		// advance is how many blocks of the refused batch the simulated
		// competing writer applies before the batch is refused.
		advance func(batch int) int
	}{
		{"tip moves one chunk", 2, func(batch int) int { return batch }},
		{"tip moves into a chunk", 3, func(int) int { return 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opts := streamFixtureOptions(
				5, func(uint64) int { return tc.blocks },
			)
			chain := readStreamChain(t, opts)
			fixture := newV2Fixture(t, opts)
			db := newSyncModeTestDB(t)
			fired := false
			applier := &fakeBatchApplier{}
			applier.beforeApply = func(
				a *fakeBatchApplier, blocks []ledger.Block,
			) error {
				if fired || chain.chunkOf(blockPoint(blocks[0])) != 2 {
					return nil
				}
				fired = true
				a.applyLocked(blocks[:tc.advance(len(blocks))])
				return fmt.Errorf("fake: competing writer: %w", ErrStaleBatch)
			}

			res, err := RunStreamCatchUp(t.Context(), streamConfig(
				fixture, t.TempDir(), db, applier, 1, 2,
			))
			require.NoError(t, err)
			require.True(t, fired)

			applied, _ := applier.snapshot()
			assert.Equal(t, chain.points, applied, "each block exactly once")
			assert.Equal(t, 1, res.Restarts)
			certifiedTip := chain.points[len(chain.points)-1]
			assert.True(t, samePoint(certifiedTip, res.Tip))
			requireMarker(t, db, 5)
		})
	}
}

func TestStreamCatchUpRestartLimit(t *testing.T) {
	t.Parallel()
	fixture := newV2Fixture(t, streamFixtureOptions(1, nil))
	db := newSyncModeTestDB(t)
	applier := &fakeBatchApplier{}
	applier.beforeApply = func(*fakeBatchApplier, []ledger.Block) error {
		return ErrStaleBatch
	}
	res, err := RunStreamCatchUp(t.Context(), streamConfig(
		fixture, t.TempDir(), db, applier, 1, 1,
	))
	require.ErrorIs(t, err, ErrStaleBatch)
	assert.Equal(t, maxStreamRestarts, res.Restarts)
}

// storeDescendant stores the certified tip block and a child of it in db, and
// returns the child's point: a local chain strictly ahead of the target.
func storeDescendant(
	t *testing.T,
	db *database.Database,
	certifiedTip ocommon.Point,
) ocommon.Point {
	t.Helper()
	aheadHash := bytes.Repeat([]byte{0xcc}, 32)
	require.NoError(t, db.BlockCreate(catchupTestBlock(
		certifiedTip.Slot, certifiedTip.Hash,
		bytes.Repeat([]byte{0x01}, 32), 98,
	), nil))
	require.NoError(t, db.BlockCreate(catchupTestBlock(
		certifiedTip.Slot+5000, aheadHash, certifiedTip.Hash, 99,
	), nil))
	return ocommon.NewPoint(certifiedTip.Slot+5000, aheadHash)
}

// TestStreamCatchUpLocalAhead pins that a local tip beyond the target costs
// one probe of the target file and applies nothing.
func TestStreamCatchUpLocalAhead(t *testing.T) {
	t.Parallel()

	t.Run("marker at target probes only the target file", func(t *testing.T) {
		t.Parallel()
		opts := streamFixtureOptions(3, nil)
		chain := readStreamChain(t, opts)
		fixture := newV2Fixture(t, opts)
		db := newSyncModeTestDB(t)
		require.NoError(t, setImmutableImportMarker(db, 3))
		ahead := storeDescendant(t, db, chain.points[len(chain.points)-1])
		applier := &fakeBatchApplier{tip: ahead}

		res, err := RunStreamCatchUp(t.Context(), streamConfig(
			fixture, t.TempDir(), db, applier, 2, 2,
		))
		require.NoError(t, err)
		_, batches := applier.snapshot()
		assert.Empty(t, batches)
		assert.Equal(t, []uint64{3}, fixture.requestedImmutables())
		assert.True(t, samePoint(ahead, res.Tip))
		assert.Equal(t, uint64(3), res.Immutable)
	})

	t.Run(
		"markerless descendant probes only the target file",
		func(t *testing.T) {
			t.Parallel()
			opts := streamFixtureOptions(3, nil)
			opts.chunkSlots = 100
			chain := readStreamChain(t, opts)
			fixture := newV2Fixture(t, opts)
			db := newSyncModeTestDB(t)
			ahead := storeDescendant(t, db, chain.points[len(chain.points)-1])
			applier := &fakeBatchApplier{tip: ahead}
			cfg := streamConfig(fixture, t.TempDir(), db, applier, 2, 2)
			cfg.ChunkSlots = 100

			res, err := RunStreamCatchUp(t.Context(), cfg)
			require.NoError(t, err)
			_, batches := applier.snapshot()
			assert.Empty(t, batches)
			assert.True(t, samePoint(ahead, res.Tip))
			assert.Equal(t, []uint64{3}, fixture.requestedImmutables())
			requireMarker(t, db, 3)

			_, err = RunStreamCatchUp(t.Context(), cfg)
			require.NoError(t, err)
			assert.Equal(t, []uint64{3, 3}, fixture.requestedImmutables())
		},
	)
}

// requestsAtFirstApply records the fixture's request log the first time the
// applier is asked to apply anything.
func requestsAtFirstApply(
	fixture *v2Fixture,
	applier *fakeBatchApplier,
) func() []uint64 {
	var got []uint64
	seen := false
	applier.beforeApply = func(*fakeBatchApplier, []ledger.Block) error {
		if !seen {
			seen = true
			got = fixture.requestedImmutables()
		}
		return nil
	}
	return func() []uint64 {
		applier.mu.Lock()
		defer applier.mu.Unlock()
		return got
	}
}

// TestStreamCatchUpEstimateLocatesTip pins that a non-empty database without
// a marker finds its tip's file from the slot estimate and one probe, and
// downloads nothing below it.
func TestStreamCatchUpEstimateLocatesTip(t *testing.T) {
	t.Parallel()
	opts := streamFixtureOptions(6, func(uint64) int { return 3 })
	opts.chunkSlots = 100
	chain := readStreamChain(t, opts)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	tipIdx := 4*3 + 1 // the second block of file 4
	applier := &fakeBatchApplier{tip: chain.points[tipIdx]}
	firstApply := requestsAtFirstApply(fixture, applier)
	cfg := streamConfig(fixture, t.TempDir(), db, applier, 1, 1)
	cfg.ChunkSlots = 100

	_, err := RunStreamCatchUp(t.Context(), cfg)
	require.NoError(t, err)
	applied, _ := applier.snapshot()
	assert.Equal(t, chain.points[tipIdx+1:], applied)
	// One probe of file 4, then the stream's own copy of file 4.
	assert.Equal(t, []uint64{4, 4}, firstApply())
	for _, num := range fixture.requestedImmutables() {
		assert.GreaterOrEqual(t, num, uint64(4))
	}
	requireMarker(t, db, 6)
}

// TestStreamCatchUpWrongEstimateBinarySearch pins the fallback: with an
// estimate that misses, the tip's file is found by bisecting the certified
// file numbers, in at most ceil(log2(N))+2 probes.
func TestStreamCatchUpWrongEstimateBinarySearch(t *testing.T) {
	t.Parallel()
	const last = 32 // N = 33 files, ceil(log2(33)) = 6
	opts := streamFixtureOptions(last, nil)
	opts.chunkSlots = 100
	chain := readStreamChain(t, opts)
	for _, tc := range []struct {
		name       string
		chunkSlots uint64
	}{
		{"estimate too high", 7},
		{"estimate too low", 1000},
		{"no estimate", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newV2Fixture(t, opts)
			db := newSyncModeTestDB(t)
			applier := &fakeBatchApplier{tip: chain.last[20]}
			firstApply := requestsAtFirstApply(fixture, applier)
			cfg := streamConfig(fixture, t.TempDir(), db, applier, 4, 4)
			cfg.ChunkSlots = tc.chunkSlots

			_, err := RunStreamCatchUp(t.Context(), cfg)
			require.NoError(t, err)
			var want []ocommon.Point
			for _, p := range chain.points {
				if chain.chunkOf(p) > 20 {
					want = append(want, p)
				}
			}
			applied, _ := applier.snapshot()
			assert.Equal(t, want, applied)
			requests := firstApply()
			// The stream window is 4, so up to four of these are the
			// stream's own downloads from file 21.
			probes := 0
			for _, num := range requests {
				if num <= 20 {
					probes++
				}
			}
			assert.LessOrEqual(t, len(requests), 6+2+4, "requests %v", requests)
			assert.LessOrEqual(t, probes, 6+2, "requests %v", requests)
			requireMarker(t, db, last)
		})
	}
}

// TestStreamCatchUpStaleMarker pins that a marker far below the ledger tip is
// only a hint: nothing below the tip's file is downloaded.
func TestStreamCatchUpStaleMarker(t *testing.T) {
	t.Parallel()
	opts := streamFixtureOptions(6, nil)
	opts.chunkSlots = 100
	chain := readStreamChain(t, opts)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	require.NoError(t, setImmutableImportMarker(db, 1))
	applier := &fakeBatchApplier{tip: chain.points[5*2]} // file 5's first
	cfg := streamConfig(fixture, t.TempDir(), db, applier, 1, 1)
	cfg.ChunkSlots = 100

	_, err := RunStreamCatchUp(t.Context(), cfg)
	require.NoError(t, err)
	applied, _ := applier.snapshot()
	assert.Equal(t, chain.points[5*2+1:], applied)
	for _, num := range fixture.requestedImmutables() {
		assert.GreaterOrEqual(t, num, uint64(5))
	}
	requireMarker(t, db, 6)
}

// TestStreamCatchUpMarkerAboveTip pins that a marker above the ledger tip's
// file (left by a truncate, say) is reset to what the ledger holds, instead
// of failing every run.
func TestStreamCatchUpMarkerAboveTip(t *testing.T) {
	t.Parallel()
	opts := streamFixtureOptions(5, nil)
	chain := readStreamChain(t, opts)

	t.Run("tip inside the certified range", func(t *testing.T) {
		t.Parallel()
		fixture := newV2Fixture(t, opts)
		db := newSyncModeTestDB(t)
		require.NoError(t, setImmutableImportMarker(db, 4))
		applier := &fakeBatchApplier{tip: chain.last[1]}
		var markerAtFirstApply []uint64
		applier.beforeApply = func(*fakeBatchApplier, []ledger.Block) error {
			if markerAtFirstApply == nil {
				num, _, _ := getImmutableImportMarker(db)
				markerAtFirstApply = []uint64{num}
			}
			return nil
		}

		_, err := RunStreamCatchUp(t.Context(), streamConfig(
			fixture, t.TempDir(), db, applier, 1, 1,
		))
		require.NoError(t, err)
		applied, _ := applier.snapshot()
		var want []ocommon.Point
		for _, p := range chain.points {
			if chain.chunkOf(p) > 1 {
				want = append(want, p)
			}
		}
		assert.Equal(t, want, applied)
		assert.Equal(t, []uint64{1}, markerAtFirstApply)
		requireMarker(t, db, 5)
	})

	t.Run("empty ledger", func(t *testing.T) {
		t.Parallel()
		fixture := newV2Fixture(t, opts)
		db := newSyncModeTestDB(t)
		require.NoError(t, setImmutableImportMarker(db, 3))
		applier := &fakeBatchApplier{}
		markerSetAtFirstApply := true
		first := true
		applier.beforeApply = func(*fakeBatchApplier, []ledger.Block) error {
			if first {
				first = false
				_, markerSetAtFirstApply, _ = getImmutableImportMarker(db)
			}
			return nil
		}

		_, err := RunStreamCatchUp(t.Context(), streamConfig(
			fixture, t.TempDir(), db, applier, 1, 1,
		))
		require.NoError(t, err)
		applied, _ := applier.snapshot()
		assert.Equal(t, chain.points, applied)
		assert.False(t, markerSetAtFirstApply, "marker kept over empty ledger")
		requireMarker(t, db, 5)
	})
}

// TestStreamCatchUpSweepsStaleFeedDirs pins that feed directories a crashed
// run left behind are removed before a new feed opens, and nothing else is.
func TestStreamCatchUpSweepsStaleFeedDirs(t *testing.T) {
	t.Parallel()
	fixture := newV2Fixture(t, streamFixtureOptions(3, nil))
	db := newSyncModeTestDB(t)
	dir := t.TempDir()
	stale := filepath.Join(
		dir, "chunkfeed-0123456789abcdef-12345", "extract", "immutable",
	)
	require.NoError(t, os.MkdirAll(stale, 0o750))
	for _, ext := range []string{"chunk", "primary", "secondary"} {
		require.NoError(t, os.WriteFile(
			filepath.Join(stale, "00000."+ext), []byte("stale"), 0o600,
		))
	}
	keep := filepath.Join(dir, "chunkfeed-not-ours")
	require.NoError(t, os.MkdirAll(keep, 0o750))
	keepFile := filepath.Join(dir, "chunkfeed-0123456789abcdef-1")
	require.NoError(t, os.WriteFile(keepFile, []byte("x"), 0o600))

	applier := &fakeBatchApplier{}
	var feedDirs []int
	applier.beforeApply = func(*fakeBatchApplier, []ledger.Block) error {
		trios, err := filepath.Glob(filepath.Join(
			dir, "chunkfeed-*", "extract", "immutable", "*.chunk",
		))
		if err != nil {
			return err
		}
		owners := map[string]bool{}
		for _, trio := range trios {
			rel, err := filepath.Rel(dir, trio)
			if err != nil {
				return err
			}
			owners[strings.SplitN(filepath.ToSlash(rel), "/", 2)[0]] = true
		}
		feedDirs = append(feedDirs, len(owners))
		return nil
	}
	_, err := RunStreamCatchUp(t.Context(), streamConfig(
		fixture, dir, db, applier, 1, 1,
	))
	require.NoError(t, err)
	require.NotEmpty(t, feedDirs)
	for _, n := range feedDirs {
		assert.Equal(t, 1, n, "feed dirs holding trios")
	}
	assert.NoDirExists(t, filepath.Dir(filepath.Dir(stale)))
	assert.DirExists(t, keep)
	assert.FileExists(t, keepFile)
}

// TestStreamCatchUpDownloadDirUnderDataDir pins the default download
// location: under the node's data directory, never the OS temp directory.
func TestStreamCatchUpDownloadDirUnderDataDir(t *testing.T) {
	t.Parallel()
	fixture := newV2Fixture(t, streamFixtureOptions(1, nil))
	db := newSyncModeTestDB(t)
	dataDir := t.TempDir()
	applier := &fakeBatchApplier{}
	var feedDirs []string
	applier.beforeApply = func(*fakeBatchApplier, []ledger.Block) error {
		matches, err := filepath.Glob(filepath.Join(
			dataDir, streamDownloadSubdir, "chunkfeed-*",
		))
		feedDirs = append(feedDirs, matches...)
		return err
	}
	cfg := streamConfig(fixture, "", db, applier, 1, 1)
	cfg.DataDir = dataDir
	_, err := RunStreamCatchUp(t.Context(), cfg)
	require.NoError(t, err)
	assert.NotEmpty(t, feedDirs)
	// The sweep recognises exactly the names real feeds get.
	for _, dir := range feedDirs {
		assert.Regexp(t, chunkFeedDirPattern, filepath.Base(dir))
	}

	cfg.DataDir = ""
	_, err = RunStreamCatchUp(t.Context(), cfg)
	require.Error(t, err)
}

func TestStreamCatchUpNotOnCertifiedChain(t *testing.T) {
	t.Parallel()
	opts := streamFixtureOptions(3, nil)
	chain := readStreamChain(t, opts)
	certifiedTip := chain.points[len(chain.points)-1]

	for _, tc := range []struct {
		name  string
		tip   func() ocommon.Point
		setup func(t *testing.T, db *database.Database)
	}{
		{
			name: "hash absent inside the certified range",
			tip: func() ocommon.Point {
				return ocommon.NewPoint(
					chain.last[1].Slot, bytes.Repeat([]byte{0xab}, 32),
				)
			},
		},
		{
			name: "tip beyond the target and missing locally",
			tip: func() ocommon.Point {
				return ocommon.NewPoint(
					certifiedTip.Slot+5000, bytes.Repeat([]byte{0xab}, 32),
				)
			},
		},
		{
			name: "tip beyond the target on another fork",
			tip: func() ocommon.Point {
				return ocommon.NewPoint(
					certifiedTip.Slot+5000, bytes.Repeat([]byte{0xab}, 32),
				)
			},
			setup: func(t *testing.T, db *database.Database) {
				fork := bytes.Clone(certifiedTip.Hash)
				fork[0] ^= 0xff
				require.NoError(t, db.BlockCreate(catchupTestBlock(
					certifiedTip.Slot, fork,
					bytes.Repeat([]byte{0x01}, 32), 98,
				), nil))
				require.NoError(t, db.BlockCreate(catchupTestBlock(
					certifiedTip.Slot+5000, bytes.Repeat([]byte{0xab}, 32),
					fork, 99,
				), nil))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newV2Fixture(t, opts)
			db := newSyncModeTestDB(t)
			if tc.setup != nil {
				tc.setup(t, db)
			}
			applier := &fakeBatchApplier{tip: tc.tip()}
			_, err := RunStreamCatchUp(t.Context(), streamConfig(
				fixture, t.TempDir(), db, applier, 2, 2,
			))
			require.ErrorIs(t, err, ErrNotOnCertifiedChain)
			_, batches := applier.snapshot()
			assert.Empty(t, batches)
			_, ok, err := getImmutableImportMarker(db)
			require.NoError(t, err)
			assert.False(t, ok, "marker written for an unverified chain")
		})
	}
}

func TestStreamCatchUpNoBootstrapMarker(t *testing.T) {
	t.Parallel()
	fixture := newV2Fixture(t, streamFixtureOptions(2, nil))
	db := newSyncModeTestDB(t)
	_, err := RunStreamCatchUp(t.Context(), streamConfig(
		fixture, t.TempDir(), db, &fakeBatchApplier{}, 1, 1,
	))
	require.NoError(t, err)
	requireMarker(t, db, 2)

	bootstrapped, err := WasBootstrapped(db)
	require.NoError(t, err)
	assert.False(t, bootstrapped)
	for _, key := range []string{
		mithrilLedgerSlotSyncKey, mithrilLedgerHashSyncKey,
	} {
		val, err := db.GetSyncState(key, nil)
		require.NoError(t, err)
		assert.Empty(t, val, key)
	}
}

// TestStreamCatchUpKeepsBootstrapMarker pins the other direction: a stream
// run over a database a snapshot import populated must not clear the fact
// that it was bootstrapped.
func TestStreamCatchUpKeepsBootstrapMarker(t *testing.T) {
	t.Parallel()
	opts := streamFixtureOptions(3, nil)
	chain := readStreamChain(t, opts)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	require.NoError(t, setImmutableImportMarker(db, 1))
	bootstrapped, err := WasBootstrapped(db)
	require.NoError(t, err)
	require.True(t, bootstrapped)

	_, err = RunStreamCatchUp(t.Context(), streamConfig(
		fixture, t.TempDir(), db, &fakeBatchApplier{tip: chain.last[1]}, 1, 1,
	))
	require.NoError(t, err)
	requireMarker(t, db, 3)
	bootstrapped, err = WasBootstrapped(db)
	require.NoError(t, err)
	assert.True(t, bootstrapped)
}

// TestStreamCatchUpSplitsChunks pins the batching rules: a batch holds at most
// MaxBatchBlocks blocks, so a chunk spans several batches, and the marker
// names or releases a chunk only once its last block has committed.
func TestStreamCatchUpSplitsChunks(t *testing.T) {
	t.Parallel()
	opts := streamFixtureOptions(3, func(uint64) int { return 7 })
	chain := readStreamChain(t, opts)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	applier := &fakeBatchApplier{maxBlocks: 2}
	dir := t.TempDir()
	var markerErrs []string
	applier.beforeApply = func(_ *fakeBatchApplier, blocks []ledger.Block) error {
		chunk := chain.chunkOf(blockPoint(blocks[0]))
		// The chunk of an uncommitted block must still be on disk.
		for _, b := range blocks {
			num := chain.chunkOf(blockPoint(b))
			trio, err := filepath.Glob(filepath.Join(
				dir, "chunkfeed-*", "extract", "immutable",
				fmt.Sprintf("%05d.chunk", num),
			))
			if err != nil || len(trio) != 1 {
				markerErrs = append(markerErrs, fmt.Sprintf(
					"chunk %d released before its blocks committed", num,
				))
			}
		}
		got, ok, err := getImmutableImportMarker(db)
		switch {
		case err != nil:
			markerErrs = append(markerErrs, err.Error())
		case chunk == 0 && ok:
			markerErrs = append(markerErrs, fmt.Sprintf(
				"marker %d before chunk 0 committed", got,
			))
		case chunk > 0 && (!ok || got != chunk-1):
			markerErrs = append(markerErrs, fmt.Sprintf(
				"batch in chunk %d saw marker %d (set %v)", chunk, got, ok,
			))
		}
		return nil
	}
	cfg := streamConfig(fixture, dir, db, applier, 2, 1)
	cfg.MaxBatchBlocks = 2

	_, err := RunStreamCatchUp(t.Context(), cfg)
	require.NoError(t, err)
	assert.Empty(t, markerErrs)

	applied, batches := applier.snapshot()
	assert.Equal(t, chain.points, applied)
	perChunk := map[uint64]int{}
	for _, b := range batches {
		assert.LessOrEqual(t, len(b), 2)
		seen := map[uint64]bool{}
		for _, p := range b {
			seen[chain.chunkOf(p)] = true
		}
		for num := range seen {
			perChunk[num]++
		}
	}
	for num := range uint64(4) {
		assert.GreaterOrEqual(t, perChunk[num], 3, "chunk %d", num)
	}
	requireMarker(t, db, 3)
}

// TestStreamCatchUpEmptyChunks pins that a chunk with no blocks is counted
// and marked like any other, first or last, without an empty batch.
func TestStreamCatchUpEmptyChunks(t *testing.T) {
	t.Parallel()
	opts := streamFixtureOptions(4, func(num uint64) int {
		if num == 0 || num == 2 || num == 4 {
			return 0
		}
		return 2
	})
	chain := readStreamChain(t, opts)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	applier := &fakeBatchApplier{}

	res, err := RunStreamCatchUp(t.Context(), streamConfig(
		fixture, t.TempDir(), db, applier, 1, 1,
	))
	require.NoError(t, err)
	applied, batches := applier.snapshot()
	assert.Equal(t, chain.points, applied)
	for _, b := range batches {
		assert.NotEmpty(t, b)
	}
	assert.True(t, samePoint(chain.last[3], res.Tip))
	assert.Equal(t, uint64(4), res.Immutable)
	requireMarker(t, db, 4)
}

// TestStreamCatchUpLocatesMarkerlessTip pins the markerless non-empty case:
// the run skips the certified blocks the local chain already holds and
// applies from the block after its tip.
func TestStreamCatchUpLocatesMarkerlessTip(t *testing.T) {
	t.Parallel()
	opts := streamFixtureOptions(3, func(uint64) int { return 3 })
	chain := readStreamChain(t, opts)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	start := 4 // the second block of chunk 1
	applier := &fakeBatchApplier{tip: chain.points[start]}

	_, err := RunStreamCatchUp(t.Context(), streamConfig(
		fixture, t.TempDir(), db, applier, 1, 1,
	))
	require.NoError(t, err)
	applied, _ := applier.snapshot()
	assert.Equal(t, chain.points[start+1:], applied)
	requireMarker(t, db, 3)
}

// A digest mismatch at chunk 2 fails the run after chunks 0 and 1 have
// committed. They stay committed and recorded, so the caller's fallback to
// Ouroboros continues from chunk 1's last block.
func TestStreamCatchUpDigestMismatchKeepsCommitted(t *testing.T) {
	t.Parallel()
	opts := streamFixtureOptions(4, nil)
	chain := readStreamChain(t, opts)
	tamper := uint64(2)
	opts.tamperImmutable = &tamper
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	applier := &fakeBatchApplier{}

	// A one-chunk window commits each chunk before chunk 2 is fetched.
	res, err := RunStreamCatchUp(t.Context(), streamConfig(
		fixture, t.TempDir(), db, applier, 1, 1,
	))
	require.Error(t, err)

	applied, _ := applier.snapshot()
	var want []ocommon.Point
	for _, p := range chain.points {
		if chain.chunkOf(p) <= 1 {
			want = append(want, p)
		}
	}
	assert.Equal(t, want, applied)
	assert.True(t, samePoint(chain.last[1], applier.Tip()), "ledger tip")
	assert.True(t, samePoint(chain.last[1], res.Tip), "result tip")
	assert.Equal(t, uint64(1), res.Immutable)
	requireMarker(t, db, 1)
}

// TestStreamCatchUpVerifiesArtifactOnce pins that one run verifies its
// artifact's certificate chain and digest list once, however many tip probes
// and stale-tip replays open feeds on it.
func TestStreamCatchUpVerifiesArtifactOnce(t *testing.T) {
	t.Parallel()
	const last = 16
	opts := streamFixtureOptions(last, nil)
	opts.chunkSlots = 100
	chain := readStreamChain(t, opts)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	applier := &fakeBatchApplier{tip: chain.last[9]}
	// One stale replay, so a second feed and a second locate run too.
	moved := false
	applier.beforeApply = func(a *fakeBatchApplier, _ []ledger.Block) error {
		if moved {
			return nil
		}
		moved = true
		a.tip = chain.last[10]
		return fmt.Errorf("fake: moved: %w", ErrStaleBatch)
	}
	cfg := streamConfig(fixture, t.TempDir(), db, applier, 2, 2)
	// A wrong estimate makes the locate bisect over several probes.
	cfg.ChunkSlots = 7
	fixture.requestedPaths()

	res, err := RunStreamCatchUp(t.Context(), cfg)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Restarts)
	paths := fixture.requestedPaths()
	digests := 0
	for _, p := range paths {
		if p == "/files/digests.tar.zst" ||
			p == "/artifact/cardano-database/digests" {
			digests++
		}
	}
	assert.Equal(t, 1, digests, "digest list fetches")
	assert.Len(t, certificateRequests(paths), 2, "certificate requests")
	requireMarker(t, db, last)
}

// TestStreamCatchUpHoldsOnlyToApply pins that verifying the artifact, its
// certificate chain and digest list, and locating the tip all happen before
// the hold, and that every batch applies under it.
func TestStreamCatchUpHoldsOnlyToApply(t *testing.T) {
	t.Parallel()
	const last = 8
	opts := streamFixtureOptions(last, nil)
	opts.chunkSlots = 100
	chain := readStreamChain(t, opts)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	var (
		mu                  sync.Mutex
		held                bool
		holds, releases     int
		heldRequests        []string
		unheldApplies       int
		probedBeforeHolding int
	)
	fixture.serveHook = func(_ http.ResponseWriter, r *http.Request) bool {
		mu.Lock()
		defer mu.Unlock()
		p := r.URL.Path
		if strings.HasPrefix(p, "/files/imm/") {
			if !held {
				probedBeforeHolding++
			}
			return false
		}
		if held {
			heldRequests = append(heldRequests, p)
		}
		return false
	}
	applier := &fakeBatchApplier{tip: chain.last[3]}
	applier.beforeApply = func(*fakeBatchApplier, []ledger.Block) error {
		mu.Lock()
		defer mu.Unlock()
		if !held {
			unheldApplies++
		}
		return nil
	}
	cfg := streamConfig(fixture, t.TempDir(), db, applier, 2, 2)
	cfg.ChunkSlots = 7 // a wrong estimate: the locate bisects
	cfg.Hold = func() func() {
		mu.Lock()
		defer mu.Unlock()
		held = true
		holds++
		return func() {
			mu.Lock()
			defer mu.Unlock()
			held = false
			releases++
		}
	}

	_, err := RunStreamCatchUp(t.Context(), cfg)
	require.NoError(t, err)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, holds)
	assert.Equal(t, 1, releases)
	assert.Empty(t, heldRequests,
		"aggregator metadata requested while holding Ouroboros")
	assert.Positive(t, probedBeforeHolding, "the tip was not located unheld")
	assert.Zero(t, unheldApplies)
	requireMarker(t, db, last)
}

// runCtxApplier records the context the run applies batches under.
type runCtxApplier struct {
	*fakeBatchApplier
	mu  sync.Mutex
	ctx context.Context
}

func (a *runCtxApplier) ApplyTrustedBatch(
	ctx context.Context,
	expected ocommon.Point,
	blocks []ledger.Block,
) (ocommon.Point, error) {
	a.mu.Lock()
	a.ctx = ctx
	a.mu.Unlock()
	return a.fakeBatchApplier.ApplyTrustedBatch(ctx, expected, blocks)
}

// TestStreamCatchUpReleaseOutlivesWatchdog pins that the hold's release runs
// after the no-progress watchdog has stopped. The node rebuilds the indexes a
// catch-up dropped inside the release, which can take far longer than the
// timeout, and the watchdog must not fire on that.
func TestStreamCatchUpReleaseOutlivesWatchdog(t *testing.T) {
	t.Parallel()
	// Long enough that a batch commit under -race and a loaded package run
	// lands inside it; the release below outlasts it several times over.
	const timeout = time.Second
	const last = 4
	opts := streamFixtureOptions(last, nil)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	applier := &runCtxApplier{fakeBatchApplier: &fakeBatchApplier{}}
	cfg := streamConfig(fixture, t.TempDir(), db, applier, 2, 2)
	cfg.NoProgressTimeout = timeout
	var cause error
	cfg.Hold = func() func() {
		return func() {
			// Stands in for an index rebuild several timeouts long.
			rebuild := time.NewTimer(5 * timeout)
			defer rebuild.Stop()
			<-rebuild.C
			applier.mu.Lock()
			defer applier.mu.Unlock()
			cause = context.Cause(applier.ctx)
		}
	}

	_, err := RunStreamCatchUp(t.Context(), cfg)
	require.NoError(t, err)
	require.ErrorIs(t, cause, context.Canceled,
		"the run context was not ended before the release")
	require.NotErrorIs(t, cause, ErrStreamStalled,
		"the watchdog fired during the release")
	requireMarker(t, db, last)
}

// TestStreamCatchUpNoProgressWatchdog pins that a run whose downloads stall
// gives up once the no-progress timeout passes without a committed batch,
// and releases the hold when it does.
func TestStreamCatchUpNoProgressWatchdog(t *testing.T) {
	t.Parallel()
	const timeout = 300 * time.Millisecond
	opts := streamFixtureOptions(6, nil)
	fixture := newV2Fixture(t, opts)
	// File 0 arrives; every later file hangs until the run gives up on it.
	fixture.serveHook = func(_ http.ResponseWriter, r *http.Request) bool {
		p := r.URL.Path
		if strings.HasPrefix(p, "/files/imm/") &&
			p != "/files/imm/00000.tar.zst" {
			<-r.Context().Done()
			return true
		}
		return false
	}
	db := newSyncModeTestDB(t)
	applier := &fakeBatchApplier{}
	cfg := streamConfig(fixture, t.TempDir(), db, applier, 1, 2)
	cfg.NoProgressTimeout = timeout
	var (
		mu                sync.Mutex
		heldAt, releaseAt time.Time
		lastCommit        time.Time
	)
	applier.beforeApply = func(*fakeBatchApplier, []ledger.Block) error {
		mu.Lock()
		defer mu.Unlock()
		lastCommit = time.Now()
		return nil
	}
	cfg.Hold = func() func() {
		mu.Lock()
		defer mu.Unlock()
		heldAt = time.Now()
		return func() {
			mu.Lock()
			defer mu.Unlock()
			releaseAt = time.Now()
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), testutil.AsyncWait)
	defer cancel()
	res, err := RunStreamCatchUp(ctx, cfg)
	require.ErrorIs(t, err, ErrStreamStalled)
	assert.NoError(t, ctx.Err(), "the run did not end on its own")
	mu.Lock()
	defer mu.Unlock()
	require.False(t, heldAt.IsZero())
	require.False(t, releaseAt.IsZero(), "the hold was never released")
	require.False(t, lastCommit.IsZero(), "file 0 was never committed")
	// Released no sooner than the timeout after the last commit, and not
	// long after it either: the stall cost Ouroboros one timeout.
	assert.GreaterOrEqual(t, releaseAt.Sub(lastCommit), timeout)
	assert.Less(t, releaseAt.Sub(lastCommit), timeout+10*time.Second)
	requireMarker(t, db, 0)
	assert.Equal(t, uint64(0), res.Immutable)
}
