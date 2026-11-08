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
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
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

type sparseTarFormat string

const (
	sparsePAX00  sparseTarFormat = "pax-0.0"
	sparsePAX01  sparseTarFormat = "pax-0.1"
	sparsePAX10  sparseTarFormat = "pax-1.0"
	sparseOldGNU sparseTarFormat = "old-gnu"
)

func writeSparseArchive(
	t *testing.T,
	name string,
	format sparseTarFormat,
) string {
	t.Helper()
	const logicalSize = int64(1 << 20)
	const dataOffset = logicalSize - 1

	var tarData bytes.Buffer
	tw := tar.NewWriter(&tarData)
	switch format {
	case sparsePAX00:
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name: name, Mode: 0o640, Size: 1, Typeflag: tar.TypeReg,
			Format: tar.FormatPAX,
			PAXRecords: map[string]string{
				"XNU.sparse.size":      strconv.FormatInt(logicalSize, 10),
				"XNU.sparse.numblocks": "1",
				"XNA.sparse.offset":    strconv.FormatInt(dataOffset, 10),
				"XNZ.sparse.numbytes":  "1",
			},
		}))
		_, err := tw.Write([]byte("x"))
		require.NoError(t, err)
	case sparsePAX01:
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name: name, Mode: 0o640, Size: 1, Typeflag: tar.TypeReg,
			Format: tar.FormatPAX,
			PAXRecords: map[string]string{
				"XNU.sparse.major":     "0",
				"XNU.sparse.minor":     "1",
				"XNU.sparse.numblocks": "1",
				"XNU.sparse.map":       fmt.Sprintf("%d,1", dataOffset),
				"XNU.sparse.size":      strconv.FormatInt(logicalSize, 10),
			},
		}))
		_, err := tw.Write([]byte("x"))
		require.NoError(t, err)
	case sparsePAX10:
		mapData := []byte(fmt.Sprintf("1\n%d\n1\n", dataOffset))
		mapData = append(mapData, make([]byte, 512-len(mapData))...)
		mapData = append(mapData, 'x')
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name: "GNUSparseFile.0/state", Mode: 0o640,
			Size: int64(len(mapData)), Typeflag: tar.TypeReg,
			Format: tar.FormatPAX,
			PAXRecords: map[string]string{
				"XNU.sparse.major":    "1",
				"XNU.sparse.minor":    "0",
				"XNU.sparse.name":     name,
				"XNU.sparse.realsize": strconv.FormatInt(logicalSize, 10),
			},
		}))
		_, err := tw.Write(mapData)
		require.NoError(t, err)
	case sparseOldGNU:
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name: name, Mode: 0o640, Size: 1, Typeflag: tar.TypeGNUSparse,
			Format: tar.FormatGNU,
		}))
		_, err := tw.Write([]byte("x"))
		require.NoError(t, err)
	default:
		t.Fatalf("unknown sparse format %q", format)
	}
	require.NoError(t, tw.Close())
	if format == sparseOldGNU {
		patchOldGNUSparseHeader(t, tarData.Bytes(), dataOffset, logicalSize)
	} else {
		patchSparsePAXKeys(t, tarData.Bytes())
	}

	var compressed bytes.Buffer
	zw, err := zstd.NewWriter(&compressed)
	require.NoError(t, err)
	_, err = zw.Write(tarData.Bytes())
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	archivePath := filepath.Join(t.TempDir(), "sparse.tar.zst")
	require.NoError(t, os.WriteFile(archivePath, compressed.Bytes(), 0o600))
	return archivePath
}

func patchSparsePAXKeys(t *testing.T, data []byte) {
	t.Helper()
	var replaced int
	for _, replacement := range []struct {
		old string
		new string
	}{
		{old: "XNU.sparse.", new: "GNU.sparse."},
		{old: "XNA.sparse.offset", new: "GNU.sparse.offset"},
		{old: "XNZ.sparse.numbytes", new: "GNU.sparse.numbytes"},
	} {
		old := []byte(replacement.old)
		new := []byte(replacement.new)
		require.Len(t, new, len(old))
		for offset := 0; ; {
			index := bytes.Index(data[offset:], old)
			if index < 0 {
				break
			}
			index += offset
			copy(data[index:index+len(new)], new)
			offset = index + len(new)
			replaced++
		}
	}
	require.Positive(t, replaced)
}

func patchOldGNUSparseHeader(
	t *testing.T,
	data []byte,
	offset int64,
	realSize int64,
) {
	t.Helper()
	require.GreaterOrEqual(t, len(data), 512)
	header := data[:512]
	writeTarOctal(header[386:398], offset)
	writeTarOctal(header[398:410], 1)
	writeTarOctal(header[483:495], realSize)
	for i := 148; i < 156; i++ {
		header[i] = ' '
	}
	var checksum int
	for _, b := range header {
		checksum += int(b)
	}
	copy(header[148:156], fmt.Sprintf("%06o\x00 ", checksum))
}

func writeTarOctal(field []byte, value int64) {
	encoded := strconv.FormatInt(value, 8)
	clear(field)
	for i := 0; i < len(field)-len(encoded)-1; i++ {
		field[i] = '0'
	}
	copy(field[len(field)-len(encoded)-1:], encoded)
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

func TestExtractArchiveRejectsSparseMembers(t *testing.T) {
	t.Parallel()

	for _, format := range []sparseTarFormat{
		sparsePAX00,
		sparsePAX01,
		sparsePAX10,
		sparseOldGNU,
	} {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()
			dest := filepath.Join(t.TempDir(), "out")
			_, err := ExtractArchive(
				t.Context(),
				writeSparseArchive(t, "ledger/100/state", format),
				dest,
				nil,
			)
			if !errors.Is(err, ErrExtractSparseMember) {
				t.Fatalf(
					"sparse %s member was not rejected at extraction: %v",
					format,
					err,
				)
			}
			_, statErr := os.Stat(dest)
			require.ErrorIs(
				t,
				statErr,
				os.ErrNotExist,
				"a refused sparse archive must publish nothing",
			)
		})
	}
}

func TestAncillaryPreflightRejectsSparseMembersBeforeManifest(t *testing.T) {
	t.Parallel()

	for _, format := range []sparseTarFormat{
		sparsePAX00,
		sparsePAX01,
		sparsePAX10,
		sparseOldGNU,
	} {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()
			file, err := os.Open(writeSparseArchive(
				t,
				"ledger/100/state",
				format,
			))
			require.NoError(t, err)
			defer file.Close()

			_, _, err = readVerifiedAncillaryManifest(
				t.Context(),
				file,
				"",
				nil,
			)
			if !errors.Is(err, ErrExtractSparseMember) {
				t.Fatalf(
					"sparse %s member before the manifest was not rejected: %v",
					format,
					err,
				)
			}
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
