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
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newFeedFixture(t *testing.T, last uint64, tamper *uint64) *v2Fixture {
	t.Helper()
	return newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber: last,
		validAllImmutables:  true,
		tamperImmutable:     tamper,
	})
}

func feedConfig(
	f *v2Fixture,
	dir string,
	start uint64,
	window, workers int,
) ChunkFeedConfig {
	bcfg := f.bootstrapConfig(dir)
	bcfg.Logger = slog.New(slog.DiscardHandler)
	return ChunkFeedConfig{
		Bootstrap:      bcfg,
		StartImmutable: start,
		WindowChunks:   window,
		Workers:        workers,
	}
}

func openFeed(
	t *testing.T,
	ctx context.Context,
	cfg ChunkFeedConfig,
) *ChunkFeed {
	t.Helper()
	feed, err := NewChunkFeed(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = feed.Close() })
	return feed
}

// feedChunkDir is where the feed keeps its trios.
func feedChunkDir(f *ChunkFeed) string {
	return filepath.Join(f.extractDir, "immutable")
}

func trioCount(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)
	n := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".chunk" {
			n++
		}
	}
	return n
}

func nextCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testutil.AsyncWait)
	t.Cleanup(cancel)
	return ctx
}

func waitWorkersExit(t *testing.T, f *ChunkFeed) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		f.wg.Wait()
		close(done)
	}()
	testutil.RequireReceive(t, done, testutil.AsyncWait, "workers exit")
}

func TestChunkFeedOrderedDespiteOutOfOrderDownload(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 4, nil)
	release0 := make(chan struct{})
	later := make(chan uint64, 8)
	fixture.immutableServe = func(num uint64) {
		if num == 0 {
			<-release0
			return
		}
		later <- num
	}
	feed := openFeed(t, t.Context(), feedConfig(fixture, t.TempDir(), 0, 0, 4))

	// Chunks 1 and 2 are requested while 0 is still held back.
	got := map[uint64]bool{}
	for len(got) < 2 {
		got[testutil.RequireReceive(t, later, testutil.AsyncWait, "later chunk")] = true
	}
	close(release0)

	for want := range uint64(5) {
		chunk, err := feed.Next(nextCtx(t))
		require.NoError(t, err)
		assert.Equal(t, want, chunk.Num)
		require.Len(t, chunk.Blocks, 2)
		assert.Equal(t, 1000+10*want, chunk.Blocks[1].SlotNumber())
	}
	_, err := feed.Next(nextCtx(t))
	assert.ErrorIs(t, err, io.EOF)
	_, err = feed.Next(nextCtx(t))
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, uint64(4), feed.Target().Immutable)
	assert.Equal(t, uint64(1040), feed.Target().Tip.Slot)
	assert.NotEmpty(t, feed.Target().Tip.Hash)
}

func TestChunkFeedWindowBound(t *testing.T) {
	t.Parallel()
	const window = 3
	fixture := newFeedFixture(t, 7, nil)
	var (
		mu       sync.Mutex
		maxSeen  int
		feedDir  string
		requests = make(chan uint64, 16)
	)
	fixture.immutableServe = func(num uint64) {
		mu.Lock()
		dir := feedDir
		mu.Unlock()
		// Trios already on disk when a new download starts: with the window
		// full this must still leave room for the one being requested.
		entries, _ := os.ReadDir(dir)
		n := 0
		for _, e := range entries {
			if filepath.Ext(e.Name()) == ".chunk" {
				n++
			}
		}
		mu.Lock()
		maxSeen = max(maxSeen, n+1)
		mu.Unlock()
		requests <- num
	}
	feed := openFeed(
		t,
		t.Context(),
		feedConfig(fixture, t.TempDir(), 0, window, 8),
	)
	mu.Lock()
	feedDir = feedChunkDir(feed)
	mu.Unlock()

	for want := range uint64(window) {
		chunk, err := feed.Next(nextCtx(t))
		require.NoError(t, err)
		require.Equal(t, want, chunk.Num)
	}
	// Nothing is released, so the window is full and no fourth download may
	// have started. nextStart is claimed before the request goes out.
	feed.mu.Lock()
	claimed := feed.nextStart
	feed.mu.Unlock()
	assert.Equal(t, uint64(window), claimed)
	assert.Equal(t, window, trioCount(t, feedChunkDir(feed)))

	// A fourth chunk cannot be delivered either.
	shortCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err := feed.Next(shortCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ElementsMatch(t, []uint64{0, 1, 2}, fixture.requestedImmutables())

	require.NoError(t, feed.Release(0))
	chunk, err := feed.Next(nextCtx(t))
	require.NoError(t, err)
	assert.Equal(t, uint64(3), chunk.Num)

	// Drain the rest, releasing as a consumer would.
	for num := uint64(1); num < 4; num++ {
		require.NoError(t, feed.Release(num))
	}
	for want := uint64(4); want <= 7; want++ {
		chunk, err := feed.Next(nextCtx(t))
		require.NoError(t, err)
		require.Equal(t, want, chunk.Num)
		require.NoError(t, feed.Release(want))
	}
	mu.Lock()
	defer mu.Unlock()
	assert.LessOrEqual(t, maxSeen, window)
}

func TestChunkFeedWindowAdvancesOnOutOfOrderRelease(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 5, nil)
	feed := openFeed(t, t.Context(), feedConfig(fixture, t.TempDir(), 0, 3, 3))
	for range 3 {
		_, err := feed.Next(nextCtx(t))
		require.NoError(t, err)
	}
	// Releasing 1 and 2 deletes their files but cannot move the window while 0
	// is held, so chunk 3 must still wait.
	require.NoError(t, feed.Release(2))
	require.NoError(t, feed.Release(1))
	feed.mu.Lock()
	assert.Equal(t, uint64(3), feed.nextStart)
	assert.Equal(t, uint64(0), feed.lowest)
	feed.mu.Unlock()
	assert.Equal(t, 1, trioCount(t, feedChunkDir(feed)))
	require.NoError(t, feed.Release(0))
	feed.mu.Lock()
	assert.Equal(t, uint64(3), feed.lowest)
	feed.mu.Unlock()
	for want := uint64(3); want <= 5; want++ {
		chunk, err := feed.Next(nextCtx(t))
		require.NoError(t, err)
		require.Equal(t, want, chunk.Num)
	}
}

func TestChunkFeedUnboundedWindow(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 5, nil)
	feed := openFeed(t, t.Context(), feedConfig(fixture, t.TempDir(), 0, 0, 2))
	for want := range uint64(6) {
		chunk, err := feed.Next(nextCtx(t))
		require.NoError(t, err)
		require.Equal(t, want, chunk.Num)
	}
	assert.Equal(t, 6, trioCount(t, feedChunkDir(feed)))
}

func TestChunkFeedReleaseDeletes(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 2, nil)
	feed := openFeed(t, t.Context(), feedConfig(fixture, t.TempDir(), 0, 0, 2))
	chunk, err := feed.Next(nextCtx(t))
	require.NoError(t, err)
	dir := feedChunkDir(feed)
	for _, ext := range immutableFileExtensions {
		require.FileExists(
			t,
			filepath.Join(dir, fmt.Sprintf("%05d.%s", chunk.Num, ext)),
		)
	}
	require.NoError(t, feed.Release(chunk.Num))
	for _, ext := range immutableFileExtensions {
		require.NoFileExists(
			t,
			filepath.Join(dir, fmt.Sprintf("%05d.%s", chunk.Num, ext)),
		)
	}
}

func TestChunkFeedReleaseContract(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 4, nil)
	feed := openFeed(t, t.Context(), feedConfig(fixture, t.TempDir(), 2, 0, 2))

	// Out of range, and not yet delivered, are errors.
	require.ErrorIs(t, feed.Release(0), ErrChunkNotDelivered)
	require.ErrorIs(t, feed.Release(99), ErrChunkNotDelivered)
	require.ErrorIs(t, feed.Release(2), ErrChunkNotDelivered)

	chunk, err := feed.Next(nextCtx(t))
	require.NoError(t, err)
	require.Equal(t, uint64(2), chunk.Num)
	require.NoError(t, feed.Release(2))
	// A duplicate is a no-op, and does not touch a later chunk's files.
	require.NoError(t, feed.Release(2))
	require.ErrorIs(t, feed.Release(3), ErrChunkNotDelivered)

	require.NoError(t, feed.Close())
	assert.ErrorIs(t, feed.Release(2), ErrChunkFeedClosed)
}

func TestChunkFeedDigestMismatch(t *testing.T) {
	t.Parallel()
	tampered := uint64(2)
	fixture := newFeedFixture(t, 4, &tampered)
	feed := openFeed(t, t.Context(), feedConfig(fixture, t.TempDir(), 0, 0, 4))

	for want := range uint64(2) {
		chunk, err := feed.Next(nextCtx(t))
		require.NoError(t, err)
		require.Equal(t, want, chunk.Num)
	}
	_, err := feed.Next(nextCtx(t))
	require.Error(t, err)
	var mismatch *DigestMismatchError
	require.ErrorAs(t, err, &mismatch)
	assert.Equal(t, "00002.chunk", mismatch.FileName)

	// The error is sticky, even though chunks 3 and 4 may have downloaded.
	for range 3 {
		_, again := feed.Next(nextCtx(t))
		require.ErrorAs(t, again, &mismatch)
	}
	assert.NoFileExists(t, filepath.Join(feedChunkDir(feed), "00002.chunk"))
}

func TestChunkFeedRefusesBeforeAnyDownload(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		opts   v2FixtureOptions
		mutate func(*ChunkFeedConfig)
	}{
		{
			name: "wrong network",
			opts: v2FixtureOptions{
				immutableFileNumber: 2,
				validAllImmutables:  true,
			},
			mutate: func(c *ChunkFeedConfig) {
				c.Bootstrap.Network = "mainnet"
			},
		},
		{
			name: "certified root differs from artifact",
			opts: v2FixtureOptions{
				immutableFileNumber:  2,
				validAllImmutables:   true,
				tamperCertMerkleRoot: true,
			},
		},
		{
			name: "digest list fails the merkle root",
			opts: v2FixtureOptions{
				immutableFileNumber: 2,
				validAllImmutables:  true,
				tamperDigestList:    true,
			},
		},
		{
			name: "wrong genesis key",
			opts: v2FixtureOptions{
				immutableFileNumber: 2,
				validAllImmutables:  true,
			},
			mutate: func(c *ChunkFeedConfig) {
				c.Bootstrap.GenesisVerificationKey = ""
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fixture := newV2Fixture(t, tt.opts)
			cfg := feedConfig(fixture, t.TempDir(), 0, 0, 2)
			if tt.mutate != nil {
				tt.mutate(&cfg)
			}
			feed, err := NewChunkFeed(t.Context(), cfg)
			require.Error(t, err)
			assert.Nil(t, feed)
			assert.Empty(t, fixture.requestedImmutables())
		})
	}
}

func TestChunkFeedWrongNetwork(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 2, nil)
	cfg := feedConfig(fixture, t.TempDir(), 0, 0, 2)
	cfg.Bootstrap.Network = "mainnet"
	_, err := NewChunkFeed(t.Context(), cfg)
	require.ErrorContains(t, err, "network mismatch")
	assert.Empty(t, fixture.requestedImmutables())
	_, err = LatestCertifiedTarget(t.Context(), cfg.Bootstrap)
	require.ErrorContains(t, err, "network mismatch")
}

func TestChunkFeedStartImmutable(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 7, nil)
	feed := openFeed(t, t.Context(), feedConfig(fixture, t.TempDir(), 5, 0, 3))
	for want := uint64(5); want <= 7; want++ {
		chunk, err := feed.Next(nextCtx(t))
		require.NoError(t, err)
		require.Equal(t, want, chunk.Num)
	}
	_, err := feed.Next(nextCtx(t))
	require.ErrorIs(t, err, io.EOF)
	requested := fixture.requestedImmutables()
	slices.Sort(requested)
	assert.Equal(t, []uint64{5, 6, 7}, requested)
}

func TestChunkFeedStartBeyondTargetIsEmpty(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 3, nil)
	feed := openFeed(t, t.Context(), feedConfig(fixture, t.TempDir(), 9, 2, 2))
	_, err := feed.Next(nextCtx(t))
	require.ErrorIs(t, err, io.EOF)
	assert.Empty(t, fixture.requestedImmutables())
	assert.Equal(t, uint64(3), feed.Target().Immutable)
	require.NoError(t, feed.Close())
}

func TestChunkFeedCancel(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 6, nil)
	started := make(chan struct{}, 8)
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	fixture.immutableServe = func(uint64) {
		started <- struct{}{}
		<-hold
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	feed := openFeed(t, ctx, feedConfig(fixture, t.TempDir(), 0, 3, 3))
	testutil.RequireReceive(t, started, testutil.AsyncWait, "download started")

	result := make(chan error, 1)
	go func() {
		_, err := feed.Next(t.Context())
		result <- err
	}()
	cancel()
	err := testutil.RequireReceive(
		t,
		result,
		testutil.AsyncWait,
		"Next returns",
	)
	require.ErrorIs(t, err, context.Canceled)
	waitWorkersExit(t, feed)
	// Sticky.
	_, err = feed.Next(t.Context())
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, feed.Close())
}

func TestChunkFeedNextContextIsPerCall(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 2, nil)
	gate := make(chan struct{})
	fixture.immutableServe = func(uint64) { <-gate }
	feed := openFeed(t, t.Context(), feedConfig(fixture, t.TempDir(), 0, 0, 1))
	shortCtx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err := feed.Next(shortCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(gate)
	chunk, err := feed.Next(nextCtx(t))
	require.NoError(t, err)
	assert.Equal(t, uint64(0), chunk.Num)
}

func TestChunkFeedCloseUnblocksNextAndCleans(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 3, nil)
	hold := make(chan struct{})
	started := make(chan struct{}, 8)
	t.Cleanup(func() { close(hold) })
	fixture.immutableServe = func(uint64) {
		started <- struct{}{}
		<-hold
	}
	dir := t.TempDir()
	feed := openFeed(t, t.Context(), feedConfig(fixture, dir, 0, 0, 2))
	testutil.RequireReceive(t, started, testutil.AsyncWait, "download started")
	result := make(chan error, 1)
	go func() {
		_, err := feed.Next(t.Context())
		result <- err
	}()
	require.NoError(t, feed.Close())
	err := testutil.RequireReceive(
		t,
		result,
		testutil.AsyncWait,
		"Next returns",
	)
	require.ErrorIs(t, err, ErrChunkFeedClosed)
	waitWorkersExit(t, feed)
	require.NoError(t, feed.Close(), "Close is idempotent")
	_, err = feed.Next(t.Context())
	require.ErrorIs(t, err, ErrChunkFeedClosed)
	assert.NoDirExists(t, feed.extractDir)
	assert.NoDirExists(t, feed.archiveDir)
}

func TestChunkFeedOwnsTempDirWhenNoneGiven(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 1, nil)
	cfg := feedConfig(fixture, "", 0, 0, 1)
	feed, err := NewChunkFeed(t.Context(), cfg)
	require.NoError(t, err)
	require.NotEmpty(t, feed.tempDir)
	require.DirExists(t, feed.tempDir)
	require.NoError(t, feed.Close())
	assert.NoDirExists(t, feed.tempDir)
}

func TestLatestCertifiedTarget(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 4, nil)
	bcfg := fixture.bootstrapConfig(t.TempDir())
	bcfg.Logger = slog.New(slog.DiscardHandler)
	target, err := LatestCertifiedTarget(t.Context(), bcfg)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), target.Immutable)
	assert.Empty(t, fixture.requestedImmutables())

	bcfg.GenesisVerificationKey = ""
	_, err = LatestCertifiedTarget(t.Context(), bcfg)
	require.Error(t, err)
}

func TestChunkFeedFailedDeleteDoesNotAdvanceWindow(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs a directory the process cannot unlink from")
	}
	fixture := newFeedFixture(t, 4, nil)
	feed := openFeed(t, t.Context(), feedConfig(fixture, t.TempDir(), 0, 2, 2))
	for range 2 {
		_, err := feed.Next(nextCtx(t))
		require.NoError(t, err)
	}
	dir := feedChunkDir(feed)
	require.NoError(t, os.Chmod(dir, 0o500))
	restored := false
	restore := func() {
		if !restored {
			restored = true
			_ = os.Chmod(dir, 0o700)
		}
	}
	t.Cleanup(restore)

	require.Error(t, feed.Release(0))
	feed.mu.Lock()
	assert.Equal(t, uint64(0), feed.lowest)
	assert.Equal(t, uint64(2), feed.nextStart, "window must not advance")
	feed.mu.Unlock()
	assert.ElementsMatch(t, []uint64{0, 1}, fixture.requestedImmutables())
	assert.Equal(t, 2, trioCount(t, dir))

	restore()
	require.NoError(t, feed.Release(0))
	chunk, err := feed.Next(nextCtx(t))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), chunk.Num)
}

func TestChunkFeedsSharingDownloadDirDoNotCollide(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 2, nil)
	dir := t.TempDir()
	first := openFeed(t, t.Context(), feedConfig(fixture, dir, 0, 0, 1))
	second := openFeed(t, t.Context(), feedConfig(fixture, dir, 0, 0, 1))
	require.NotEqual(t, first.feedDir, second.feedDir)
	chunk, err := first.Next(nextCtx(t))
	require.NoError(t, err)
	require.Equal(t, uint64(0), chunk.Num)
	require.NoError(t, second.Close())
	require.FileExists(t, filepath.Join(feedChunkDir(first), "00000.chunk"))
	_, err = first.Next(nextCtx(t))
	require.NoError(t, err)
}

func TestChunkFeedFailedDigestFetchLeavesNoCache(t *testing.T) {
	t.Parallel()
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber: 2,
		validAllImmutables:  true,
		digestsCloudBadRoot: true,
		tamperDigestList:    true,
	})
	dir := t.TempDir()
	_, err := NewChunkFeed(t.Context(), feedConfig(fixture, dir, 0, 0, 1))
	require.Error(t, err)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// TestChunkFeedDecodesOnNext checks that verified trios wait in the window
// undecoded: a trio changed on disk after its verified download, but before
// Next takes it, is refused by Next's digest-checked read instead of being
// handed out from blocks a worker decoded earlier.
func TestChunkFeedDecodesOnNext(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 3, nil)
	feed := openFeed(t, t.Context(), feedConfig(fixture, t.TempDir(), 0, 2, 2))
	testutil.WaitForCondition(t, func() bool {
		feed.mu.Lock()
		defer feed.mu.Unlock()
		_, ok0 := feed.ready[0]
		_, ok1 := feed.ready[1]
		return ok0 && ok1
	}, 5*time.Second, "window filled")

	chunk, err := feed.Next(nextCtx(t))
	require.NoError(t, err)
	require.Equal(t, uint64(0), chunk.Num)
	require.NotEmpty(t, chunk.Blocks)

	path := filepath.Join(feedChunkDir(feed), "00001.chunk")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[len(data)-1] ^= 0xff
	require.NoError(t, os.WriteFile(path, data, 0o600))

	_, err = feed.Next(nextCtx(t))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decoding immutable file 00001")
	// The failure is the feed's from then on.
	_, again := feed.Next(nextCtx(t))
	assert.Equal(t, err, again)
}
