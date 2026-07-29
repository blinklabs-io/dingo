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
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTestArchive writes a zstd tar archive containing files to a temp path
// and returns that path.
func writeTestArchive(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	archivePath := filepath.Join(dir, "archive.tar.zst")
	require.NoError(
		t, os.WriteFile(archivePath, createTestArchive(t, files), 0o640),
	)
	return archivePath
}

// requireSymlinkSupport skips on platforms where an unprivileged process
// cannot create symlinks (notably Windows without developer mode), since the
// attack these tests model is not reproducible there.
func requireSymlinkSupport(t *testing.T, oldname, newname string) {
	t.Helper()
	if err := os.Symlink(oldname, newname); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink creation unsupported: %v", err)
		}
		require.NoError(t, err)
	}
}

// TestExtractArchiveRefusesNonEmptyDestination covers the default exclusive
// mode: a destination that already holds content is not extracted into, so
// archive contents can never be merged with files placed there by someone
// else.
func TestExtractArchiveRefusesNonEmptyDestination(t *testing.T) {
	archivePath := writeTestArchive(t, map[string]string{
		"immutable/00000.chunk": "chunk0",
	})

	destDir := filepath.Join(t.TempDir(), "extracted")
	require.NoError(t, os.MkdirAll(destDir, 0o750))
	preexisting := filepath.Join(destDir, "preexisting.txt")
	require.NoError(t, os.WriteFile(preexisting, []byte("keep"), 0o640))

	_, err := ExtractArchive(
		t.Context(), archivePath, destDir, nil,
	)
	require.ErrorIs(t, err, ErrExtractDestinationNotEmpty)

	// The refusal must not have disturbed what was already there.
	data, readErr := os.ReadFile(preexisting)
	require.NoError(t, readErr)
	assert.Equal(t, "keep", string(data))
}

// TestExtractArchiveReplaceSwapsDestination covers the explicit recovery
// path: re-extracting over a stale or partial destination replaces it
// wholesale rather than merging into it.
func TestExtractArchiveReplaceSwapsDestination(t *testing.T) {
	archivePath := writeTestArchive(t, map[string]string{
		"immutable/00000.chunk": "chunk0",
	})

	destDir := filepath.Join(t.TempDir(), "extracted")
	require.NoError(t, os.MkdirAll(destDir, 0o750))
	stale := filepath.Join(destDir, "stale.txt")
	require.NoError(t, os.WriteFile(stale, []byte("stale"), 0o640))

	result, err := ExtractArchive(
		t.Context(), archivePath, destDir, nil,
		WithReplaceDestination(),
	)
	require.NoError(t, err)
	require.Equal(t, destDir, result)

	data, err := os.ReadFile(
		filepath.Join(destDir, "immutable", "00000.chunk"),
	)
	require.NoError(t, err)
	assert.Equal(t, "chunk0", string(data))

	_, statErr := os.Stat(stale)
	assert.True(t, os.IsNotExist(statErr),
		"replacing a destination must not leave stale content behind")
}

// TestExtractArchiveDoesNotWriteThroughPreExistingSymlink is the regression
// test for the finding: a symlink planted in the destination, named to match
// a directory in the archive, must not redirect extracted writes outside the
// destination.
func TestExtractArchiveDoesNotWriteThroughPreExistingSymlink(t *testing.T) {
	archivePath := writeTestArchive(t, map[string]string{
		"immutable/00000.chunk": "chunk0",
	})

	root := t.TempDir()
	outside := filepath.Join(root, "outside")
	require.NoError(t, os.MkdirAll(outside, 0o750))
	canary := filepath.Join(outside, "00000.chunk")
	require.NoError(t, os.WriteFile(canary, []byte("original"), 0o640))

	destDir := filepath.Join(root, "extracted")
	require.NoError(t, os.MkdirAll(destDir, 0o750))
	requireSymlinkSupport(t, outside, filepath.Join(destDir, "immutable"))

	// Replace mode is the permissive path — even here the symlink must be
	// discarded rather than followed.
	_, err := ExtractArchive(
		t.Context(), archivePath, destDir, nil,
		WithReplaceDestination(),
	)
	require.NoError(t, err)

	data, err := os.ReadFile(canary)
	require.NoError(t, err)
	assert.Equal(t, "original", string(data),
		"extraction must not write through a pre-existing symlink")

	// The extracted file belongs inside the destination, not the symlink
	// target.
	extracted, err := os.ReadFile(
		filepath.Join(destDir, "immutable", "00000.chunk"),
	)
	require.NoError(t, err)
	assert.Equal(t, "chunk0", string(extracted))
}

// TestExtractArchiveMergeDoesNotWriteThroughPreExistingSymlink covers the
// same attack in merge mode, which cannot use a private temp directory and
// therefore relies entirely on the per-path symlink checks.
func TestExtractArchiveMergeDoesNotWriteThroughPreExistingSymlink(
	t *testing.T,
) {
	archivePath := writeTestArchive(t, map[string]string{
		"immutable/00000.chunk": "chunk0",
	})

	root := t.TempDir()
	outside := filepath.Join(root, "outside")
	require.NoError(t, os.MkdirAll(outside, 0o750))
	canary := filepath.Join(outside, "00000.chunk")
	require.NoError(t, os.WriteFile(canary, []byte("original"), 0o640))

	destDir := filepath.Join(root, "extracted")
	require.NoError(t, os.MkdirAll(destDir, 0o750))
	requireSymlinkSupport(t, outside, filepath.Join(destDir, "immutable"))

	_, err := ExtractArchive(
		t.Context(), archivePath, destDir, nil,
		WithMergeIntoDestination(),
	)
	require.ErrorIs(t, err, ErrExtractUnsafePath)

	data, readErr := os.ReadFile(canary)
	require.NoError(t, readErr)
	assert.Equal(t, "original", string(data),
		"merge extraction must not write through a pre-existing symlink")
}

// TestExtractArchiveAllowsSymlinkedAncestor pins the boundary of the symlink
// checks: directories above the destination belong to the operator and are
// not part of the threat, so a symlink among them must not block extraction.
//
// Rejecting them breaks ordinary systems rather than attackers — on macOS
// every temporary path resolves through /var, which is a symlink to
// /private/var.
func TestExtractArchiveAllowsSymlinkedAncestor(t *testing.T) {
	archivePath := writeTestArchive(t, map[string]string{
		"immutable/00000.chunk": "chunk0",
	})

	root := t.TempDir()
	realParent := filepath.Join(root, "real")
	require.NoError(t, os.MkdirAll(realParent, 0o750))
	linkedParent := filepath.Join(root, "linked")
	requireSymlinkSupport(t, realParent, linkedParent)

	// The destination is reached through a symlinked ancestor.
	destDir := filepath.Join(linkedParent, "extracted")
	_, err := ExtractArchive(t.Context(), archivePath, destDir, nil)
	require.NoError(t, err)

	data, err := os.ReadFile(
		filepath.Join(destDir, "immutable", "00000.chunk"),
	)
	require.NoError(t, err)
	assert.Equal(t, "chunk0", string(data))
}

// TestExtractArchiveRefusesSymlinkedDestination covers the destination path
// itself being a symlink, which would otherwise relocate the whole
// extraction.
func TestExtractArchiveRefusesSymlinkedDestination(t *testing.T) {
	archivePath := writeTestArchive(t, map[string]string{
		"immutable/00000.chunk": "chunk0",
	})

	root := t.TempDir()
	outside := filepath.Join(root, "outside")
	require.NoError(t, os.MkdirAll(outside, 0o750))

	destDir := filepath.Join(root, "extracted")
	requireSymlinkSupport(t, outside, destDir)

	_, err := ExtractArchive(
		t.Context(), archivePath, destDir, nil,
		WithMergeIntoDestination(),
	)
	require.ErrorIs(t, err, ErrExtractUnsafePath)

	entries, readErr := os.ReadDir(outside)
	require.NoError(t, readErr)
	assert.Empty(t, entries,
		"nothing may be written through a symlinked destination")
}

// TestExtractArchiveLeavesNoDestinationOnFailure is the observable
// consequence of extracting into a private temp directory: a failed
// extraction publishes nothing, rather than leaving a half-populated
// destination for a later run to mistake for a complete one.
func TestExtractArchiveLeavesNoDestinationOnFailure(t *testing.T) {
	// A traversal entry fails partway through extraction.
	archivePath := writeTestArchive(t, map[string]string{
		"immutable/00000.chunk": "chunk0",
		"../escape.txt":         "evil",
	})

	destDir := filepath.Join(t.TempDir(), "extracted")
	_, err := ExtractArchive(t.Context(), archivePath, destDir, nil)
	require.Error(t, err)

	_, statErr := os.Stat(destDir)
	assert.True(t, os.IsNotExist(statErr),
		"a failed extraction must not publish a partial destination")
}

// TestExtractPublishRejectsSwappedParent covers the window between staging and
// publishing. Extraction of a mainnet snapshot takes minutes, and the parent
// is the shared download directory, so a directory checked once at the start
// is not still known-good at the end. Publishing resolves the destination
// through that parent, and unlike the final component, an intermediate symlink
// is followed by RemoveAll and Rename.
func TestExtractPublishRejectsSwappedParent(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "downloads")
	require.NoError(t, os.MkdirAll(parent, 0o750))
	destDir := filepath.Join(parent, "extracted")

	workDir, publish, cleanup, err := prepareExtractDestination(
		destDir, extractConfig{},
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t,
		os.WriteFile(filepath.Join(workDir, "chunk"), []byte("data"), 0o640),
	)

	// Swap the parent for a symlink pointing at somewhere else, as an
	// attacker with write access to the download directory could.
	elsewhere := filepath.Join(root, "elsewhere")
	require.NoError(t, os.MkdirAll(elsewhere, 0o750))
	require.NoError(t, os.Rename(parent, filepath.Join(root, "downloads.real")))
	requireSymlinkSupport(t, elsewhere, parent)

	require.ErrorIs(t, publish(), ErrExtractUnsafePath)

	entries, err := os.ReadDir(elsewhere)
	require.NoError(t, err)
	assert.Empty(t, entries,
		"publishing must not write through a parent swapped mid-extraction")
}

// TestExtractPublishAllowsStableSymlinkedParent is the counterpart that keeps
// the check honest. Operators routinely place a data directory behind a
// symlink, pointing it at a larger volume. Rejecting a parent merely for being
// a symlink would break those installs, so the check compares directory
// identity instead: a symlink that still resolves to the same directory is
// fine, and only a genuine substitution is refused.
func TestExtractPublishAllowsStableSymlinkedParent(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "volume", "downloads")
	require.NoError(t, os.MkdirAll(real, 0o750))
	parent := filepath.Join(root, "downloads")
	requireSymlinkSupport(t, real, parent)

	destDir := filepath.Join(parent, "extracted")
	workDir, publish, cleanup, err := prepareExtractDestination(
		destDir, extractConfig{},
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t,
		os.WriteFile(filepath.Join(workDir, "chunk"), []byte("data"), 0o640),
	)

	require.NoError(t, publish())

	data, err := os.ReadFile(filepath.Join(destDir, "chunk"))
	require.NoError(t, err)
	assert.Equal(t, "data", string(data))
}

// TestExtractArchiveMergeAccumulates pins the behaviour the parallel
// immutable-archive download depends on: successive archives extracted into
// one shared destination add to it rather than replacing it.
func TestExtractArchiveMergeAccumulates(t *testing.T) {
	first := writeTestArchive(t, map[string]string{
		"immutable/00000.chunk": "chunk0",
	})
	second := writeTestArchive(t, map[string]string{
		"immutable/00001.chunk": "chunk1",
	})

	destDir := filepath.Join(t.TempDir(), "extracted")
	for _, archivePath := range []string{first, second} {
		_, err := ExtractArchive(
			t.Context(), archivePath, destDir, nil,
			WithMergeIntoDestination(),
		)
		require.NoError(t, err)
	}

	for name, want := range map[string]string{
		"immutable/00000.chunk": "chunk0",
		"immutable/00001.chunk": "chunk1",
	} {
		data, err := os.ReadFile(filepath.Join(destDir, name))
		require.NoError(t, err, "%s should have survived both extractions", name)
		assert.Equal(t, want, string(data))
	}
}
