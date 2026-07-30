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

// requireDirectorySwap renames oldpath to newpath, modelling a writer
// replacing a directory while extraction is under way.
//
// Windows refuses to rename a directory that has open handles beneath it, so
// the scenario cannot be staged there at all. That is a constraint on the
// attack rather than a gap in the guarantee, but it does mean these tests can
// only run where the swap is possible.
func requireDirectorySwap(t *testing.T, oldpath, newpath string) {
	t.Helper()
	if err := os.Rename(oldpath, newpath); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf(
				"cannot swap a directory with open handles beneath it: %v",
				err,
			)
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

// TestExtractRootWritesSurviveParentSwap is the damage path the publish-time
// check cannot cover on its own: a parent replaced partway through a
// multi-minute extraction, with writes still to come.
//
// The staging tree is held by a directory handle, so a later write goes to the
// directory extraction started in regardless of what the pathname now resolves
// to. Publication is refused separately, but by then nothing has leaked.
func TestExtractRootWritesSurviveParentSwap(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "downloads")
	require.NoError(t, os.MkdirAll(parent, 0o750))
	destDir := filepath.Join(parent, "extracted")

	extractRoot, publish, cleanup, err := prepareExtractDestination(
		destDir, extractConfig{},
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	stagingPath := extractRoot.Name()

	// An early write, before any tampering.
	require.NoError(t, extractRoot.MkdirAll("immutable", 0o750))

	// Swap the parent out from under the staging pathname, as an attacker
	// with write access to the download directory could mid-extraction.
	elsewhere := filepath.Join(root, "elsewhere")
	require.NoError(t, os.MkdirAll(elsewhere, 0o750))
	requireDirectorySwap(t, parent, filepath.Join(root, "downloads.real"))
	requireSymlinkSupport(t, elsewhere, parent)

	// A later write must still land in the original staging directory.
	f, err := extractRoot.OpenFile(
		filepath.Join("immutable", "00000.chunk"),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640,
	)
	require.NoError(t, err)
	_, err = f.Write([]byte("chunk0"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// The handle tracks the directory itself, so the bytes are in the real
	// staging directory at its new location — never at the pathname the
	// attacker now controls.
	movedStaging := filepath.Join(
		root, "downloads.real", filepath.Base(stagingPath),
	)
	data, err := os.ReadFile(
		filepath.Join(movedStaging, "immutable", "00000.chunk"),
	)
	require.NoError(t, err,
		"write must land in the directory the handle was opened on")
	assert.Equal(t, "chunk0", string(data))

	_, statErr := os.Stat(
		filepath.Join(parent, filepath.Base(stagingPath), "immutable"),
	)
	assert.True(t, os.IsNotExist(statErr),
		"nothing may appear under the substituted parent pathname")

	var leaked []string
	_ = filepath.Walk(elsewhere,
		func(p string, info os.FileInfo, _ error) error {
			if info != nil && !info.IsDir() {
				leaked = append(leaked, p)
			}
			return nil
		})
	assert.Empty(t, leaked,
		"no extracted write may reach the substituted directory")

	// Publication follows the same handle, so it lands in the real staging
	// parent rather than anywhere the swapped pathname now points.
	require.NoError(t, publish())
}

// TestExtractRootRefusesEscapingEntry covers an archive entry that resolves
// outside the extraction root through a pre-existing symlink. The root handle
// refuses it rather than relying on a prior path inspection that a writer
// could invalidate between check and open.
func TestExtractRootRefusesEscapingEntry(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(root, "outside")
	require.NoError(t, os.MkdirAll(outside, 0o750))
	destDir := filepath.Join(root, "extracted")
	require.NoError(t, os.MkdirAll(destDir, 0o750))
	requireSymlinkSupport(t, outside, filepath.Join(destDir, "immutable"))

	extractRoot, _, cleanup, err := prepareExtractDestination(
		destDir, extractConfig{merge: true},
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	_, err = extractRoot.OpenFile(
		filepath.Join("immutable", "00000.chunk"),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640,
	)
	require.Error(t, err, "a path escaping the root must not open")

	entries, readErr := os.ReadDir(outside)
	require.NoError(t, readErr)
	assert.Empty(t, entries)
}

// TestExtractPublishFollowsParentHandle covers a parent replaced between the
// last write and publication.
//
// Removing and renaming are pathname operations, so a check before them can
// always be overtaken. Holding the parent open instead means both resolve to
// the directory extraction started in: the tree lands where it belongs and the
// substituted directory receives nothing, with no window to lose.
func TestExtractPublishFollowsParentHandle(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "downloads")
	require.NoError(t, os.MkdirAll(parent, 0o750))
	destDir := filepath.Join(parent, "extracted")

	workDir, publish, cleanup, err := prepareExtractDestination(
		destDir, extractConfig{},
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, workDir.WriteFile("chunk", []byte("data"), 0o640))

	elsewhere := filepath.Join(root, "elsewhere")
	require.NoError(t, os.MkdirAll(elsewhere, 0o750))
	movedParent := filepath.Join(root, "downloads.real")
	requireDirectorySwap(t, parent, movedParent)
	requireSymlinkSupport(t, elsewhere, parent)

	require.NoError(t, publish())

	data, err := os.ReadFile(filepath.Join(movedParent, "extracted", "chunk"))
	require.NoError(t, err,
		"publication must land in the directory the handle was opened on")
	assert.Equal(t, "data", string(data))

	entries, err := os.ReadDir(elsewhere)
	require.NoError(t, err)
	assert.Empty(t, entries,
		"the substituted parent must receive nothing")
}

// TestExtractPublishRefusesConcurrentDestinationContent covers a destination
// populated by someone else while extraction was running.
//
// The emptiness check before extraction says nothing about the state minutes
// later, and publication removes whatever occupies the destination. Without a
// re-check that removal would silently delete another writer's files even
// though this caller never asked to replace anything.
func TestExtractPublishRefusesConcurrentDestinationContent(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "downloads")
	require.NoError(t, os.MkdirAll(parent, 0o750))
	destDir := filepath.Join(parent, "extracted")

	workDir, publish, cleanup, err := prepareExtractDestination(
		destDir, extractConfig{},
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, workDir.WriteFile("chunk", []byte("data"), 0o640))

	// Another process populates the destination mid-extraction.
	require.NoError(t, os.MkdirAll(destDir, 0o750))
	theirs := filepath.Join(destDir, "theirs.txt")
	require.NoError(t, os.WriteFile(theirs, []byte("keep"), 0o640))

	require.ErrorIs(t, publish(), ErrExtractDestinationNotEmpty)

	data, err := os.ReadFile(theirs)
	require.NoError(t, err,
		"a refused publication must not delete another writer's content")
	assert.Equal(t, "keep", string(data))
}

// TestExtractPublishAcceptsEmptyDestination pins that refusing to delete does
// not cost the legitimate case: an empty directory already sitting at the
// destination is still published into, because renaming onto it destroys
// nothing.
func TestExtractPublishAcceptsEmptyDestination(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "downloads")
	require.NoError(t, os.MkdirAll(parent, 0o750))
	destDir := filepath.Join(parent, "extracted")
	require.NoError(t, os.MkdirAll(destDir, 0o750))

	workDir, publish, cleanup, err := prepareExtractDestination(
		destDir, extractConfig{},
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, workDir.WriteFile("chunk", []byte("data"), 0o640))

	require.NoError(t, publish())

	data, err := os.ReadFile(filepath.Join(destDir, "chunk"))
	require.NoError(t, err)
	assert.Equal(t, "data", string(data))
}

// TestExtractPublishReplacesConcurrentDestinationContent is the counterpart:
// a caller that explicitly asked to replace the destination still does so,
// since that is the documented recovery path.
func TestExtractPublishReplacesConcurrentDestinationContent(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "downloads")
	require.NoError(t, os.MkdirAll(parent, 0o750))
	destDir := filepath.Join(parent, "extracted")

	workDir, publish, cleanup, err := prepareExtractDestination(
		destDir, extractConfig{replace: true},
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, workDir.WriteFile("chunk", []byte("data"), 0o640))

	require.NoError(t, os.MkdirAll(destDir, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(destDir, "stale.txt"), []byte("stale"), 0o640,
	))

	require.NoError(t, publish())

	data, err := os.ReadFile(filepath.Join(destDir, "chunk"))
	require.NoError(t, err)
	assert.Equal(t, "data", string(data))
	_, statErr := os.Stat(filepath.Join(destDir, "stale.txt"))
	assert.True(t, os.IsNotExist(statErr))
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
	require.NoError(t, workDir.WriteFile("chunk", []byte("data"), 0o640))

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
