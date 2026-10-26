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
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

type limitsTestEntry struct {
	name     string
	typeflag byte
	content  []byte
}

func writeLimitsArchive(t *testing.T, entries []limitsTestEntry) string {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)
	tw := tar.NewWriter(zw)
	for _, e := range entries {
		flag := e.typeflag
		if flag == 0 {
			flag = tar.TypeReg
		}
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name:     e.name,
			Typeflag: flag,
			Mode:     0o750,
			Size:     int64(len(e.content)),
		}))
		_, err := tw.Write(e.content)
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, zw.Close())
	path := filepath.Join(t.TempDir(), "archive.tar.zst")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))
	return path
}

func extractWithLimits(
	t *testing.T,
	entries []limitsTestEntry,
	limits archiveLimits,
) (string, error) {
	t.Helper()
	dest := filepath.Join(t.TempDir(), "out")
	_, err := ExtractArchive(
		context.Background(), writeLimitsArchive(t, entries), dest, nil,
		withArchiveLimits(limits),
	)
	return dest, err
}

func TestExtractArchiveLimits(t *testing.T) {
	t.Parallel()

	manyDirs := func(n int) []limitsTestEntry {
		var out []limitsTestEntry
		for i := range n {
			out = append(out, limitsTestEntry{
				name:     strings.Repeat("d", i+1),
				typeflag: tar.TypeDir,
			})
		}
		return out
	}

	for _, tc := range []struct {
		name    string
		entries []limitsTestEntry
		limits  archiveLimits
		wantErr error
	}{
		{
			name:    "entry count at limit",
			entries: manyDirs(3),
			limits:  archiveLimits{maxEntries: 3},
		},
		{
			// Directories carry no bytes, so only the entry count sees them.
			name:    "excessive entries",
			entries: manyDirs(4),
			limits:  archiveLimits{maxEntries: 3},
			wantErr: ErrExtractLimitExceeded,
		},
		{
			name: "materialized path at entry limit",
			entries: []limitsTestEntry{
				{name: "ledger/100/state"},
			},
			limits: archiveLimits{maxEntries: 3},
		},
		{
			name: "materialized path exceeds entry limit",
			entries: []limitsTestEntry{
				{name: "ledger/100/tables/state"},
			},
			limits:  archiveLimits{maxEntries: 3},
			wantErr: ErrExtractLimitExceeded,
		},
		{
			name:    "member at limit",
			entries: []limitsTestEntry{{name: "a", content: make([]byte, 100)}},
			limits:  archiveLimits{maxMemberBytes: 100},
		},
		{
			name:    "oversized member",
			entries: []limitsTestEntry{{name: "a", content: make([]byte, 101)}},
			limits:  archiveLimits{maxMemberBytes: 100},
			wantErr: ErrExtractLimitExceeded,
		},
		{
			name: "aggregate over limit across members",
			entries: []limitsTestEntry{
				{name: "a", content: make([]byte, 60)},
				{name: "b", content: make([]byte, 60)},
			},
			limits:  archiveLimits{maxMemberBytes: 100, maxTotalBytes: 100},
			wantErr: ErrExtractLimitExceeded,
		},
		{
			// A zero-filled member is tiny compressed and huge expanded.
			name: "compression ratio",
			entries: []limitsTestEntry{
				{name: "zeros", content: make([]byte, 8<<20)},
			},
			limits: archiveLimits{
				maxExpansion: 50, expansionFloor: 1 << 20,
			},
			wantErr: ErrExtractLimitExceeded,
		},
		{
			name: "ratio floor admits a small repetitive member",
			entries: []limitsTestEntry{
				{name: "zeros", content: make([]byte, 1<<20)},
			},
			limits: archiveLimits{
				maxExpansion: 1, expansionFloor: 2 << 20,
			},
		},
		{
			name: "member outside the allowlist",
			entries: []limitsTestEntry{
				{name: "wanted", content: []byte("x")},
				{name: "extra", content: []byte("x")},
			},
			limits: archiveLimits{
				allow: func(n string) bool { return n == "wanted" },
			},
			wantErr: ErrExtractUnexpectedMember,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dest, err := extractWithLimits(t, tc.entries, tc.limits)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				_, statErr := os.Stat(dest)
				require.ErrorIs(t, statErr, os.ErrNotExist,
					"a refused archive must publish nothing")
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestUnverifiedAncillaryPathComponentsConsumeBootstrapBudget(t *testing.T) {
	t.Parallel()

	limits := ancillaryLimits()
	budget := &bootstrapExtractionBudget{
		maxEntries:     3,
		maxStreamBytes: math.MaxInt64,
	}
	limits.budget = budget
	dest, err := extractWithLimits(
		t,
		[]limitsTestEntry{{name: "ledger/100/state"}},
		limits,
	)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dest, "ledger", "100", "state"))
	require.NoError(t, err)
	require.Equal(t, 3, budget.entries)

	_, err = extractWithLimits(
		t,
		[]limitsTestEntry{{name: "ledger/101/state"}},
		limits,
	)
	require.ErrorIs(t, err, ErrExtractLimitExceeded)
}

func TestExtractArchiveRefusesUnexpectedMemberBeforeWriting(t *testing.T) {
	t.Parallel()

	dest := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.MkdirAll(dest, 0o750))
	_, err := ExtractArchive(
		context.Background(),
		writeLimitsArchive(t, []limitsTestEntry{
			{name: "extra", content: []byte("payload")},
		}),
		dest, nil,
		WithMergeIntoDestination(),
		withArchiveLimits(archiveLimits{
			allow: func(string) bool { return false },
		}),
	)
	require.ErrorIs(t, err, ErrExtractUnexpectedMember)
	_, statErr := os.Stat(filepath.Join(dest, "extra"))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestExtractArchiveRejectsRootFiles(t *testing.T) {
	t.Parallel()

	for _, entry := range []limitsTestEntry{
		{name: ".", content: []byte("payload")},
		{name: "."},
	} {
		_, err := extractWithLimits(t, []limitsTestEntry{entry}, archiveLimits{})
		require.ErrorIs(t, err, ErrExtractUnexpectedMember)
	}
}

func TestExtractArchiveVerifiesMemberDigest(t *testing.T) {
	t.Parallel()

	good := []byte("authentic")
	sum := sha256.Sum256(good)
	want := hex.EncodeToString(sum[:])

	dest := filepath.Join(t.TempDir(), "ok")
	_, err := ExtractArchive(
		context.Background(),
		writeLimitsArchive(t, []limitsTestEntry{{name: "m", content: good}}),
		dest, nil,
		withArchiveLimits(archiveLimits{digests: map[string]string{"m": want}}),
	)
	require.NoError(t, err)

	dest = filepath.Join(t.TempDir(), "bad")
	require.NoError(t, os.MkdirAll(dest, 0o750))
	_, err = ExtractArchive(
		context.Background(),
		writeLimitsArchive(t, []limitsTestEntry{
			{name: "m", content: []byte("tampered")},
		}),
		dest, nil,
		WithMergeIntoDestination(),
		withArchiveLimits(archiveLimits{digests: map[string]string{"m": want}}),
	)
	var mismatch *DigestMismatchError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, want, mismatch.Expected)
	_, statErr := os.Stat(filepath.Join(dest, "m"))
	require.ErrorIs(t, statErr, os.ErrNotExist,
		"a member that fails its digest must not stay on disk")
}

func TestVerifiedAncillaryLimitsAdmitRootEntry(t *testing.T) {
	t.Parallel()
	state := []byte("state")
	digest := sha256.Sum256(state)
	manifest := &ancillaryManifest{
		Data: map[string]string{
			"ledger/100/state": hex.EncodeToString(digest[:]),
		},
	}
	manifestData := []byte("{}")
	entries := []limitsTestEntry{
		{name: "./", typeflag: tar.TypeDir},
		{name: "ledger/", typeflag: tar.TypeDir},
		{name: "ledger/100/", typeflag: tar.TypeDir},
		{name: "ledger/100/state", content: state},
		{name: ancillaryManifestFilename, content: manifestData},
	}

	dest, err := extractWithLimits(
		t, entries, verifiedAncillaryLimits(manifest, manifestData, nil),
	)

	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dest, "ledger", "100", "state"))
	require.NoError(t, err)
}

func TestAggregatorHTTPClientKeepsRequestTimeout(t *testing.T) {
	t.Parallel()
	base := &http.Client{Transport: http.DefaultTransport}

	client := aggregatorHTTPClient(base)

	require.Equal(t, aggregatorRequestTimeout, client.Timeout)
	require.Same(t, base.Transport, client.Transport)
	require.Zero(t, base.Timeout, "the download client stays unbounded")
}
