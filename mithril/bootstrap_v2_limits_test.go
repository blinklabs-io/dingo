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
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestObjectMaxBytes(t *testing.T) {
	t.Parallel()

	require.Equal(t, int64(7), BootstrapConfig{}.objectMaxBytes(7))
	require.Equal(
		t, int64(5),
		BootstrapConfig{DownloadMaxBytes: 5}.objectMaxBytes(7),
		"an operator limit replaces the built-in one, up or down",
	)
	require.Equal(
		t, int64(9),
		BootstrapConfig{DownloadMaxBytes: 9}.objectMaxBytes(7),
	)
}

// TestDownloadImmutablesBoundsInFlightBytes holds every immutable request open
// and checks how many the pool lets through at once. With the limit raised to
// half the in-flight budget only two fit, however many workers are free.
func TestDownloadImmutablesBoundsInFlightBytes(t *testing.T) {
	t.Parallel()

	fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 7})
	arrivals := make(chan struct{}, 64)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	fixture.immutableGate = func() {
		arrivals <- struct{}{}
		<-release
	}

	cfg := fixture.bootstrapConfig(t.TempDir())
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg.DownloadMaxBytes = immutableInflightBytes / 2

	type outcome struct {
		result *BootstrapResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := Bootstrap(context.Background(), cfg)
		done <- outcome{result, err}
	}()

	testutil.RequireReceive(t, arrivals, 30*time.Second, "first request")
	testutil.RequireReceive(t, arrivals, 30*time.Second, "second request")
	select {
	case <-arrivals:
		t.Error("a third immutable download started while two already " +
			"claimed the whole in-flight byte budget")
	case <-time.After(500 * time.Millisecond):
	}
	releaseAll()

	got := testutil.RequireReceive(t, done, 60*time.Second, "bootstrap")
	require.NoError(t, got.err)
	t.Cleanup(got.result.CloseHandles)
	require.EqualValues(t, 8, fixture.immutableHits.Load())
}

func TestFetchImmutableArchiveAdmitsOnlyOwnTrio(t *testing.T) {
	t.Parallel()

	fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 1})
	location := fixture.artifact.Immutables.Locations[0]
	digests := map[string]string{}
	for _, e := range fixture.digestEntries {
		digests[e.ImmutableFileName] = e.Digest
	}

	trio0 := func(extra map[string][]byte) []byte {
		files := map[string][]byte{}
		for name, content := range fixture.immutableContent {
			if strings.HasPrefix(name, "00000.") {
				files["immutable/"+name] = content
			}
		}
		maps.Copy(files, extra)
		return buildTarZst(t, files)
	}

	for _, tc := range []struct {
		name    string
		archive []byte
		wantErr error
		absent  string
	}{
		{name: "own trio", archive: trio0(nil)},
		{
			name: "another archive's file",
			archive: trio0(map[string][]byte{
				"immutable/00001.chunk": []byte("overwrite attempt"),
			}),
			wantErr: ErrExtractUnexpectedMember,
			absent:  "immutable/00001.chunk",
		},
		{
			name: "unlisted name",
			archive: trio0(map[string][]byte{
				"immutable/junk": []byte("x"),
			}),
			wantErr: ErrExtractUnexpectedMember,
			absent:  "immutable/junk",
		},
		{
			name: "outside the immutable directory",
			archive: trio0(map[string][]byte{
				"00000.extra": []byte("x"),
			}),
			wantErr: ErrExtractUnexpectedMember,
			absent:  "00000.extra",
		},
		{
			name: "member failing its certified digest",
			archive: buildTarZst(t, map[string][]byte{
				"immutable/00000.chunk": []byte("tampered"),
			}),
			wantErr: &DigestMismatchError{},
			absent:  "immutable/00000.chunk",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, _ *http.Request) {
					_, _ = w.Write(tc.archive)
				},
			))
			t.Cleanup(server.Close)
			dir := t.TempDir()
			cfg := fixture.bootstrapConfig(dir)
			cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
			cfg.immutableDigests = digests
			loc := location
			loc.URITemplate = server.URL + "/{immutable_file_number}.tar.zst"
			extractDir := filepath.Join(dir, "extracted")
			require.NoError(t, os.MkdirAll(extractDir, 0o750))

			err := fetchImmutableArchive(
				context.Background(), cfg, cfg.Logger, &loc, 0,
				filepath.Join(dir, "archives"), extractDir,
			)
			switch want := tc.wantErr.(type) {
			case nil:
				require.NoError(t, err)
			case *DigestMismatchError:
				require.ErrorAs(t, err, &want)
			default:
				require.ErrorIs(t, err, tc.wantErr)
			}
			if tc.absent != "" {
				_, statErr := os.Stat(filepath.Join(extractDir, tc.absent))
				require.ErrorIs(t, statErr, os.ErrNotExist)
			}
		})
	}
}

func TestDownloadDigestsArchiveAdmitsOnlyTheDigestList(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		files   map[string][]byte
		wantErr error
	}{
		{
			name:  "single top-level json",
			files: map[string][]byte{"digests.json": []byte("[]")},
		},
		{
			name: "extra member",
			files: map[string][]byte{
				"digests.json": []byte("[]"),
				"extra":        []byte("x"),
			},
			wantErr: ErrExtractUnexpectedMember,
		},
		{
			name:    "nested json",
			files:   map[string][]byte{"sub/digests.json": []byte("[]")},
			wantErr: ErrExtractUnexpectedMember,
		},
		{
			// Zeros compress to almost nothing, so this is the shape of a
			// digest list sized to fill the disk.
			name: "oversized digest list",
			files: map[string][]byte{
				"digests.json": make([]byte, digestListMaxMemberBytes+1),
			},
			wantErr: ErrExtractLimitExceeded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 0})
			fixture.digestArchive = buildTarZst(t, tc.files)
			dir := t.TempDir()
			cfg := fixture.bootstrapConfig(dir)
			cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
			_, err := downloadDigestsArchive(
				context.Background(), cfg,
				fixture.server.URL+"/files/digests.tar.zst",
				fixture.artifact, dir,
			)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDownloadAncillaryV2AdmitsOnlyConsumedMembers(t *testing.T) {
	t.Parallel()

	manifest := []byte(`{"data":{},"signature":""}`)
	for _, tc := range []struct {
		name    string
		files   map[string][]byte
		wantErr error
	}{
		{
			name: "manifest, ledger state and next trio",
			files: map[string][]byte{
				ancillaryManifestFilename:   manifest,
				"ledger/100/state":          []byte("state"),
				"ledger/100/tables/values":  []byte("values"),
				"immutable/00003.chunk":     []byte("c"),
				"immutable/00003.primary":   []byte("p"),
				"immutable/00003.secondary": []byte("s"),
			},
		},
		{
			name: "unrelated top-level file",
			files: map[string][]byte{
				ancillaryManifestFilename: manifest,
				"ledger/100/state":        []byte("state"),
				"unrelated.bin":           []byte("x"),
			},
			wantErr: ErrExtractUnexpectedMember,
		},
		{
			name: "unknown name in the immutable directory",
			files: map[string][]byte{
				ancillaryManifestFilename: manifest,
				"ledger/100/state":        []byte("state"),
				"immutable/notes.txt":     []byte("x"),
			},
			wantErr: ErrExtractUnexpectedMember,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 0})
			fixture.ancillaryArchive = buildTarZst(t, tc.files)
			cfg := fixture.bootstrapConfig(t.TempDir())
			cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
			cfg.VerifyCertificateChain = false
			dir := t.TempDir()
			vetted, _, _, err := downloadAncillaryV2(
				context.Background(), cfg, fixture.artifact, dir,
			)
			if vetted != nil {
				vetted.Close()
			}
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestBootstrapV2RefusesImmutableArchiveWithForeignMember drives the whole
// immutable pool: an archive that carries another archive's file is refused
// rather than letting it replace that archive's verified trio.
func TestBootstrapV2RefusesImmutableArchiveWithForeignMember(t *testing.T) {
	t.Parallel()

	fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 1})
	files := map[string][]byte{
		"immutable/00001.chunk": []byte("foreign"),
	}
	for name, content := range fixture.immutableContent {
		if strings.HasPrefix(name, "00000.") {
			files["immutable/"+name] = content
		}
	}
	fixture.immutableArchives[0] = buildTarZst(t, files)

	cfg := fixture.bootstrapConfig(t.TempDir())
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	result, err := Bootstrap(context.Background(), cfg)
	if err == nil {
		result.CloseHandles()
	}
	require.ErrorContains(t, err, ErrExtractUnexpectedMember.Error())
}

// TestDownloadAncillaryV1AdmitsOnlyConsumedMembers covers the v1 ancillary
// archive, which the aggregator builds with the same layout as the v2 one.
func TestDownloadAncillaryV1AdmitsOnlyConsumedMembers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		files   map[string][]byte
		wantErr error
	}{
		{
			name: "ledger state and next trio",
			files: map[string][]byte{
				"ledger/100/state":          []byte("state"),
				"ledger/100/tables/values":  []byte("values"),
				"immutable/00003.chunk":     []byte("c"),
				"immutable/00003.primary":   []byte("p"),
				"immutable/00003.secondary": []byte("s"),
			},
		},
		{
			name: "unrelated top-level file",
			files: map[string][]byte{
				"ledger/100/state": []byte("state"),
				"unrelated.bin":    []byte("x"),
			},
			wantErr: ErrExtractUnexpectedMember,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			archive := buildTarZst(t, tc.files)
			srv := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write(archive)
				},
			))
			t.Cleanup(srv.Close)
			tree, _, err := downloadAncillary(
				t.Context(),
				BootstrapConfig{
					AllowInsecureHTTP: true,
					Logger: slog.New(
						slog.NewTextHandler(io.Discard, nil),
					),
				},
				&SnapshotListItem{
					SnapshotBase: SnapshotBase{
						Digest:             "abc123",
						Network:            "preprod",
						AncillaryLocations: []string{srv.URL},
					},
				},
				t.TempDir(),
			)
			if tree != nil {
				tree.Close()
			}
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
