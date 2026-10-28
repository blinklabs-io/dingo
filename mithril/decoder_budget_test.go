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
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decoderGateWait is how long a blocked decoder must stay blocked. It only
// bounds how long a regression is observed for: a load-induced delay cannot
// make a correctly gated decoder finish early.
const decoderGateWait = 300 * time.Millisecond

func testTarZst(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, writeTarZst(&buf, []tarEntry{
		bytesEntry("immutable/00000.chunk", []byte("chunk")),
	}))
	return buf.Bytes()
}

// holdDecoderBudget takes the whole budget so any decoder that reserves must
// wait, and returns the release.
func holdDecoderBudget(t *testing.T, b *decoderMemoryBudget) func() {
	t.Helper()
	require.True(t, b.sem.TryAcquire(b.capacity))
	var once sync.Once
	return func() { once.Do(func() { b.sem.Release(b.capacity) }) }
}

func TestDecoderWeightFollowsSmallerLimit(t *testing.T) {
	t.Parallel()
	require.Equal(
		t,
		int64(maxZstdDecoderMemory)+decoderOverheadBytes,
		decoderWeight(maxZstdWindowSize, maxZstdDecoderMemory),
		"the memory limit caps the streaming window",
	)
	require.Equal(
		t,
		int64(1<<20)+decoderOverheadBytes,
		decoderWeight(1<<20, maxZstdDecoderMemory),
	)
	require.Equal(
		t,
		decoderMemoryBudgetBytes+decoderOverheadBytes,
		decoderWeight(1<<62, 1<<62),
	)
}

func TestDecoderMemoryBudgetBoundsConcurrentDecoders(t *testing.T) {
	t.Parallel()
	b := newDecoderMemoryBudget()
	weight := min(
		decoderWeight(maxZstdWindowSize, maxZstdDecoderMemory), b.capacity,
	)
	fit := b.capacity / weight
	require.Less(t, fit, int64(immutableDownloadWorkers))

	var releases []func()
	for range fit {
		release, err := b.reserve(
			t.Context(), maxZstdWindowSize, maxZstdDecoderMemory,
		)
		require.NoError(t, err)
		releases = append(releases, release)
	}
	blocked := make(chan struct{})
	go func() {
		release, err := b.reserve(
			context.Background(), maxZstdWindowSize, maxZstdDecoderMemory,
		)
		if err == nil {
			release()
		}
		close(blocked)
	}()
	testutil.RequireNoReceive(
		t, blocked, decoderGateWait, "decoder past the memory budget",
	)
	releases[0]()
	testutil.RequireReceive(
		t, blocked, 30*time.Second, "decoder after a reservation was freed",
	)
	for _, release := range releases[1:] {
		release()
	}

	cancelled, cancel := context.WithCancel(t.Context())
	hold := holdDecoderBudget(t, b)
	cancel()
	_, err := b.reserve(cancelled, maxZstdWindowSize, maxZstdDecoderMemory)
	require.ErrorIs(t, err, context.Canceled)
	hold()
}

func TestNilDecoderMemoryBudgetReservesNothing(t *testing.T) {
	t.Parallel()
	var b *decoderMemoryBudget
	release, err := b.reserve(t.Context(), maxZstdWindowSize, maxZstdDecoderMemory)
	require.NoError(t, err)
	release()
}

func TestExtractArchiveWaitsForDecoderBudget(t *testing.T) {
	t.Parallel()
	archive := filepath.Join(t.TempDir(), "a.tar.zst")
	require.NoError(t, os.WriteFile(archive, testTarZst(t), 0o600))
	b := newDecoderMemoryBudget()
	release := holdDecoderBudget(t, b)
	defer release()

	done := make(chan error, 1)
	go func() {
		_, err := ExtractArchive(
			t.Context(), archive, filepath.Join(t.TempDir(), "out"),
			slog.New(slog.DiscardHandler), withDecoderBudget(b),
		)
		done <- err
	}()
	testutil.RequireNoReceive(
		t, done, decoderGateWait, "extraction ran without a decoder reservation",
	)
	release()
	require.NoError(t, testutil.RequireReceive(
		t, done, 30*time.Second, "extraction after the budget was freed",
	))
	require.True(
		t, b.sem.TryAcquire(b.capacity),
		"extraction must return its reservation",
	)
}

func TestAncillaryPreflightWaitsForDecoderBudget(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "ancillary.tar.zst")
	require.NoError(t, os.WriteFile(path, testTarZst(t), 0o600))
	file, err := os.Open(path) //nolint:gosec // test temp dir
	require.NoError(t, err)
	defer file.Close()
	b := newDecoderMemoryBudget()
	release := holdDecoderBudget(t, b)
	defer release()

	done := make(chan error, 1)
	go func() {
		_, _, err := readVerifiedAncillaryManifest(t.Context(), file, "", b)
		done <- err
	}()
	testutil.RequireNoReceive(
		t, done, decoderGateWait, "preflight ran without a decoder reservation",
	)
	release()
	// The archive carries no manifest, so the preflight refuses it once it is
	// allowed to decode.
	require.ErrorContains(t, testutil.RequireReceive(
		t, done, 30*time.Second, "preflight after the budget was freed",
	), "no signed manifest")
}

func TestFetchImmutableArchiveWaitsForDecoderBudget(t *testing.T) {
	t.Parallel()
	body := testTarZst(t)
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(body)
		},
	))
	defer server.Close()
	b := newDecoderMemoryBudget()
	release := holdDecoderBudget(t, b)
	defer release()
	cfg := BootstrapConfig{
		Logger:            slog.New(slog.DiscardHandler),
		AllowInsecureHTTP: true,
		httpClient:        server.Client(),
		decoderBudget:     b,
	}
	location := &CardanoDatabaseLocation{
		URITemplate: server.URL + "/{immutable_file_number}.tar.zst",
	}

	done := make(chan error, 1)
	go func() {
		done <- fetchImmutableArchive(
			t.Context(), cfg, slog.New(slog.DiscardHandler), location, 0,
			t.TempDir(), t.TempDir(),
		)
	}()
	testutil.RequireNoReceive(
		t, done, decoderGateWait, "immutable extraction ran unreserved",
	)
	release()
	require.NoError(t, testutil.RequireReceive(
		t, done, 30*time.Second, "extraction after the budget was freed",
	))
}

func TestDownloadDigestsArchiveWaitsForDecoderBudget(t *testing.T) {
	t.Parallel()
	fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 1})
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(fixture.digestArchive)
		},
	))
	defer server.Close()
	b := newDecoderMemoryBudget()
	release := holdDecoderBudget(t, b)
	defer release()
	cfg := BootstrapConfig{
		Logger:            slog.New(slog.DiscardHandler),
		AllowInsecureHTTP: true,
		decoderBudget:     b,
	}

	done := make(chan error, 1)
	go func() {
		_, err := downloadDigestsArchive(
			t.Context(), cfg, server.URL, fixture.artifact, t.TempDir(),
		)
		done <- err
	}()
	testutil.RequireNoReceive(
		t, done, decoderGateWait, "digest list extraction ran unreserved",
	)
	release()
	require.NoError(t, testutil.RequireReceive(
		t, done, 30*time.Second, "digest list after the budget was freed",
	))
}

// The verified path reserves for the manifest preflight first; the unverified
// path reaches the extraction's own reservation.
func TestDownloadAncillaryV2WaitsForDecoderBudget(t *testing.T) {
	t.Parallel()
	for _, verify := range []bool{true, false} {
		t.Run(map[bool]string{true: "verified", false: "unverified"}[verify],
			func(t *testing.T) {
				t.Parallel()
				fixture := newV2Fixture(
					t, v2FixtureOptions{immutableFileNumber: 0},
				)
				cfg := fixture.bootstrapConfig(t.TempDir())
				cfg.Logger = slog.New(slog.DiscardHandler)
				cfg.VerifyCertificateChain = verify
				b := newDecoderMemoryBudget()
				release := holdDecoderBudget(t, b)
				defer release()
				cfg.decoderBudget = b

				done := make(chan error, 1)
				go func() {
					vetted, _, _, err := downloadAncillaryV2(
						t.Context(), cfg, fixture.artifact, t.TempDir(),
					)
					if vetted != nil {
						vetted.Close()
					}
					done <- err
				}()
				testutil.RequireNoReceive(
					t, done, decoderGateWait, "ancillary decoded unreserved",
				)
				release()
				require.NoError(t, testutil.RequireReceive(
					t, done, 30*time.Second, "ancillary after the budget was freed",
				))
			})
	}
}

func TestOneShotDownloadClientCarriesDecoderBudget(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 1, nil)
	dir := t.TempDir()
	bcfg, url, err := prepareFeedConfig(
		feedConfig(fixture, dir, 0, 0, 1).Bootstrap,
	)
	require.NoError(t, err)
	dcfg, closeClient, err := withOneShotDownloadClient(bcfg, url, dir, true)
	require.NoError(t, err)
	t.Cleanup(closeClient)
	assert.NotNil(t, dcfg.decoderBudget)
}

// A feed decodes against the budget it is given, and one given none creates
// its own rather than decoding unbounded.
func TestChunkFeedDecoderBudget(t *testing.T) {
	t.Parallel()
	t.Run("own", func(t *testing.T) {
		t.Parallel()
		fixture := newFeedFixture(t, 1, nil)
		feed := openFeed(t, t.Context(), feedConfig(fixture, t.TempDir(), 0, 2, 1))
		assert.NotNil(t, feed.cfg.decoderBudget)
	})
	t.Run("shared", func(t *testing.T) {
		t.Parallel()
		fixture := newFeedFixture(t, 1, nil)
		cfg := feedConfig(fixture, t.TempDir(), 0, 2, 1)
		b := newDecoderMemoryBudget()
		release := holdDecoderBudget(t, b)
		defer release()
		cfg.Bootstrap.decoderBudget = b

		// The feed's workers keep writing beneath the t.TempDir above until
		// Close returns, so the result is sent only after Close, and this
		// cleanup, registered after t.TempDir and so run before it, holds
		// the directory until the goroutine has finished on a failing run.
		stopped := make(chan struct{})
		t.Cleanup(func() { <-stopped })
		done := make(chan error, 1)
		go func() {
			defer close(stopped)
			done <- func() (err error) {
				feed, err := NewChunkFeed(t.Context(), cfg)
				if err != nil {
					return err
				}
				defer func() { err = errors.Join(err, feed.Close()) }()
				if feed.cfg.decoderBudget != b {
					return errors.New("feed replaced the budget it was given")
				}
				_, err = feed.Next(t.Context())
				return err
			}()
		}()
		testutil.RequireNoReceive(
			t, done, decoderGateWait, "feed decoded outside its budget",
		)
		release()
		require.NoError(t, testutil.RequireReceive(
			t, done, 30*time.Second, "feed after the budget was freed",
		))
	})
}
