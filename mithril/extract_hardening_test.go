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

// requireDirectorySwapSupport reports the same constraint as
// requireDirectorySwap without performing the caller's swap.
//
// The archive swap tests stage their swap from a response-body Read callback.
// io.Copy calls that Read synchronously on the calling test goroutine. Probing
// the capability here, before the scenario is armed, lets a platform limitation
// skip cleanly; once armed, the callback records any failure as scenario
// evidence instead of using testing APIs. Where the platform refuses the
// rename, the manoeuvre cannot be staged at all, which bounds the attack rather
// than the guarantee.
func requireDirectorySwapSupport(t *testing.T) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "swap-probe")
	require.NoError(t, os.Mkdir(dir, 0o750))
	// A handle *beneath* the directory, which is the shape the extraction path
	// holds open while the swap is attempted.
	held, err := os.Create(filepath.Join(dir, "held"))
	require.NoError(t, err)
	defer held.Close()
	if err := os.Rename(dir, dir+".moved"); err != nil {
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

// TestExtractPublishRefusesSubstitutedStaging covers a writer replacing the
// staging entry between extraction and publication.
//
// Renaming names its source, so it moves whatever occupies that name at the
// instant it runs rather than the directory extraction wrote into. Go has no
// rename keyed on a descriptor, so the substitution cannot be prevented
// outright — but publishing an attacker's tree under the destination can be,
// by confirming afterwards that what landed is the directory that was filled.
func TestExtractPublishRefusesSubstitutedStaging(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "downloads")
	require.NoError(t, os.MkdirAll(parent, 0o750))
	destDir := filepath.Join(parent, "extracted")

	workDir, publish, cleanup, err := prepareExtractDestination(
		destDir, extractConfig{},
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, workDir.WriteFile("ours", []byte("genuine"), 0o640))

	stagingName := filepath.Base(workDir.Name())
	stagingPath := filepath.Join(parent, stagingName)

	// Move the real staging directory aside and leave a tree of our own under
	// its name, as a writer with access to the parent could.
	requireDirectorySwap(t, stagingPath, filepath.Join(root, "moved-aside"))
	require.NoError(t, os.MkdirAll(stagingPath, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(stagingPath, "theirs"), []byte("substituted"), 0o640,
	))

	require.ErrorIs(t, publish(), ErrExtractUnsafePath)

	// The substituted tree must not remain published at the destination.
	_, statErr := os.Stat(filepath.Join(destDir, "theirs"))
	assert.True(t, os.IsNotExist(statErr),
		"a substituted staging tree must not survive at the destination")
}

// TestExtractPublishRefusesSymlinkedStaging covers the same substitution done
// with a symlink rather than a directory, which a rename would otherwise
// relocate to the destination intact.
func TestExtractPublishRefusesSymlinkedStaging(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "downloads")
	require.NoError(t, os.MkdirAll(parent, 0o750))
	destDir := filepath.Join(parent, "extracted")
	elsewhere := filepath.Join(root, "elsewhere")
	require.NoError(t, os.MkdirAll(elsewhere, 0o750))

	workDir, publish, cleanup, err := prepareExtractDestination(
		destDir, extractConfig{},
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, workDir.WriteFile("ours", []byte("genuine"), 0o640))

	stagingName := filepath.Base(workDir.Name())
	stagingPath := filepath.Join(parent, stagingName)
	requireDirectorySwap(t, stagingPath, filepath.Join(root, "moved-aside"))
	requireSymlinkSupport(t, elsewhere, stagingPath)

	require.ErrorIs(t, publish(), ErrExtractUnsafePath)

	entries, err := os.ReadDir(elsewhere)
	require.NoError(t, err)
	assert.Empty(t, entries,
		"publication must not reach through a substituted symlink")
}

// TestExtractPublishRefusesNonDirectoryDestination covers a file occupying the
// destination name when publication runs.
//
// Clearing an empty destination directory is safe because rmdir refuses a
// populated one, so the removal cannot cost anyone content. Unlinking a file
// carries no such protection: it would destroy something this caller never
// asked to replace. Only directories are removed; anything else is refused
// where it stands.
func TestExtractPublishRefusesNonDirectoryDestination(t *testing.T) {
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

	// Someone else takes the destination name for a file mid-extraction.
	require.NoError(t, os.WriteFile(destDir, []byte("theirs"), 0o640))

	require.ErrorIs(t, publish(), ErrExtractDestinationNotEmpty)

	// The file is left exactly as it was found.
	data, err := os.ReadFile(destDir)
	require.NoError(t, err,
		"a refused publication must not unlink another writer's file")
	assert.Equal(t, "theirs", string(data))
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

// TestExtractArchiveAcceptsEmptyDestination covers the other half of the
// default exclusive mode: the contract is that the destination must be empty,
// not that it must be absent.
//
// An empty destination directory is routine — an operator creating the
// directory ahead of time, or a previous run cleaning up after itself — and
// refusing it would turn a documented, supported arrangement into a failure.
func TestExtractArchiveAcceptsEmptyDestination(t *testing.T) {
	archivePath := writeTestArchive(t, map[string]string{
		"immutable/00000.chunk": "chunk0",
	})

	destDir := filepath.Join(t.TempDir(), "extracted")
	require.NoError(t, os.MkdirAll(destDir, 0o750))

	_, err := ExtractArchive(
		t.Context(), archivePath, destDir, nil,
	)
	require.NoError(t, err,
		"an empty destination satisfies the exclusive-mode contract")

	data, err := os.ReadFile(
		filepath.Join(destDir, "immutable", "00000.chunk"),
	)
	require.NoError(t, err)
	assert.Equal(t, "chunk0", string(data))
}

// TestExtractPublishAcceptsDestinationEmptiedConcurrently covers the same
// contract at publication, where the destination appears after the check that
// preceded extraction.
//
// Removing an empty directory cannot cost anyone content: the removal is the
// emptiness check, so a writer who populated it first makes the removal fail
// rather than lose their files. That is what separates this case from a
// populated destination, which is refused.
func TestExtractPublishAcceptsDestinationEmptiedConcurrently(t *testing.T) {
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

	// The destination appears while extraction is running, but stays empty.
	require.NoError(t, os.MkdirAll(destDir, 0o750))

	require.NoError(t, publish(),
		"an empty destination must not block publication")

	data, err := os.ReadFile(filepath.Join(destDir, "chunk"))
	require.NoError(t, err)
	assert.Equal(t, "data", string(data))
}

// TestExtractPublishesDestinationWithGroupTraversal pins the mode of the
// published destination.
//
// Extraction stages into a temporary directory and renames it into place, and
// rename preserves the source mode. MkdirTemp creates 0700, so without an
// explicit widening the destination would silently arrive 0700 rather than the
// 0750 the extracted tree carried before staging was introduced, dropping
// group traversal for deployments that separate the downloader from the node.
func TestExtractPublishesDestinationWithGroupTraversal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix permission bits are not meaningful on windows")
	}
	archivePath := writeTestArchive(t, map[string]string{
		"immutable/00000.chunk": "chunk0",
	})
	destDir := filepath.Join(t.TempDir(), "extracted")

	_, err := ExtractArchive(t.Context(), archivePath, destDir, nil)
	require.NoError(t, err)

	info, err := os.Stat(destDir)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o750), info.Mode().Perm(),
		"the published destination keeps group traversal")
}

// TestExtractRefusesSymlinkInsideDestination covers the symlinks the root
// handle does not reject on its own.
//
// os.Root refuses an absolute symlink outright and refuses a relative one
// whose target leaves the root, so the only symlink that can still redirect a
// write is a relative one pointing back inside the destination. It cannot
// carry bytes out, but it does mean the tree on disk is not the tree the
// archive described — a directory the archive never created ends up holding
// its contents.
//
// Both positions matter. Inspecting an entry's complete path reports on its
// last component and resolves everything before it, so a symlink at
// `immutable` goes unnoticed while a write to `immutable/sub/00000.chunk`
// follows it.
func TestExtractRefusesSymlinkInsideDestination(t *testing.T) {
	for _, tt := range []struct {
		name  string
		entry string
	}{
		{name: "final component", entry: "immutable/00000.chunk"},
		{name: "intermediate component", entry: "immutable/sub/00000.chunk"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			archivePath := writeTestArchive(t, map[string]string{
				tt.entry: "chunk0",
			})

			destDir := filepath.Join(t.TempDir(), "extracted")
			elsewhere := filepath.Join(destDir, "elsewhere")
			require.NoError(t, os.MkdirAll(elsewhere, 0o750))
			// Relative, so os.Root will follow it rather than refuse it for
			// being absolute.
			requireSymlinkSupport(
				t, "elsewhere", filepath.Join(destDir, "immutable"),
			)

			_, err := ExtractArchive(
				t.Context(), archivePath, destDir, nil,
				WithMergeIntoDestination(),
			)
			require.ErrorIs(t, err, ErrExtractUnsafePath)

			entries, readErr := os.ReadDir(elsewhere)
			require.NoError(t, readErr)
			assert.Empty(t, entries,
				"no write may be redirected through a symlink in the tree")
		})
	}
}

// TestExtractRootRefusesEntrySubstitutedDuringExtraction covers an escaping
// symlink staged as a race rather than as pre-existing content: it appears
// after the extraction root has been opened and every destination check has
// run.
//
// Containment does not come from those checks, so the timing does not matter.
// The root refuses any name resolving outside it whenever the write happens.
// The symlink is relative so that it is refused for escaping rather than for
// being absolute, which os.Root rejects on sight.
func TestExtractRootRefusesEntrySubstitutedDuringExtraction(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(root, "outside")
	require.NoError(t, os.MkdirAll(outside, 0o750))
	destDir := filepath.Join(root, "extracted")
	require.NoError(t, os.MkdirAll(destDir, 0o750))

	extractRoot, _, cleanup, err := prepareExtractDestination(
		destDir, extractConfig{merge: true},
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	// Planted after every destination check has already run.
	requireSymlinkSupport(
		t, filepath.Join("..", "outside"),
		filepath.Join(destDir, "immutable"),
	)

	_, err = createExtractedFile(
		extractRoot, filepath.Join("immutable", "00000.chunk"),
	)
	require.Error(t, err, "a path escaping the root must not open")

	entries, readErr := os.ReadDir(outside)
	require.NoError(t, readErr)
	assert.Empty(t, entries,
		"a symlink planted mid-extraction must not redirect a write")
}

// TestOpenExtractRootRefusesSubstitutedDestination covers the destination
// being replaced between the check that vetted it and the open that acts on
// it, which is the window merge mode cannot stage into a private directory.
//
// The substitution is staged statically because the window itself cannot be
// driven from a test, but the code exercised is what closes it: openExtractRoot
// is handed a destination nothing has inspected, and must refuse it. The
// symlink points at a sibling of the destination and is relative, so neither
// the parent handle's containment nor os.Root's refusal of absolute links can
// be what rejects it.
func TestOpenExtractRootRefusesSubstitutedDestination(t *testing.T) {
	parent := t.TempDir()
	sibling := filepath.Join(parent, "sibling")
	require.NoError(t, os.MkdirAll(sibling, 0o750))
	requireSymlinkSupport(t, "sibling", filepath.Join(parent, "extracted"))

	parentRoot, err := os.OpenRoot(parent)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parentRoot.Close() })

	_, err = openExtractRoot(parentRoot, "extracted")
	require.ErrorIs(t, err, ErrExtractUnsafePath)

	entries, readErr := os.ReadDir(sibling)
	require.NoError(t, readErr)
	assert.Empty(t, entries,
		"nothing may be created through a substituted destination")
}

// TestOpenExtractRootCreatesMissingDestination pins the other half of
// openExtractRoot: an absent destination is created, since merge mode has no
// staging directory to fall back on.
func TestOpenExtractRootCreatesMissingDestination(t *testing.T) {
	parent := t.TempDir()

	parentRoot, err := os.OpenRoot(parent)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parentRoot.Close() })

	extractRoot, err := openExtractRoot(parentRoot, "extracted")
	require.NoError(t, err)
	t.Cleanup(func() { _ = extractRoot.Close() })

	opened, err := extractRoot.Stat(".")
	require.NoError(t, err)
	created, err := os.Lstat(filepath.Join(parent, "extracted"))
	require.NoError(t, err)
	assert.True(t, os.SameFile(opened, created),
		"the handle must refer to the directory at the destination name")
}

// TestRemoveEmptyExtractDirRefusesFile is what closes the last race in
// publication.
//
// Publication clears an empty destination directory out of the way, and a
// writer can swap that directory for a file after it has been identified as a
// directory and before it is removed. There is no way to prevent the swap, so
// the removal itself must be unable to act on a file: a directory-only removal
// fails where a general one would unlink whatever it found.
func TestRemoveEmptyExtractDirRefusesFile(t *testing.T) {
	parent := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(parent, "theirs"), []byte("keep"), 0o640,
	))

	parentRoot, err := os.OpenRoot(parent)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parentRoot.Close() })

	require.Error(t, removeEmptyExtractDir(parentRoot, "theirs", filepath.Join(parent, "theirs")),
		"a directory-only removal must refuse a file")

	data, err := os.ReadFile(filepath.Join(parent, "theirs"))
	require.NoError(t, err, "the file must not have been unlinked")
	assert.Equal(t, "keep", string(data))
}

// TestRemoveEmptyExtractDirRefusesPopulatedDir pins the other half: the
// removal is the emptiness test, so a writer who populated the destination
// first makes it fail rather than lose their content.
func TestRemoveEmptyExtractDirRefusesPopulatedDir(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "theirs")
	require.NoError(t, os.MkdirAll(dir, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "keep.txt"), []byte("keep"), 0o640,
	))

	parentRoot, err := os.OpenRoot(parent)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parentRoot.Close() })

	require.Error(t, removeEmptyExtractDir(parentRoot, "theirs", filepath.Join(parent, "theirs")))

	data, err := os.ReadFile(filepath.Join(dir, "keep.txt"))
	require.NoError(t, err)
	assert.Equal(t, "keep", string(data))
}

// TestRemoveEmptyExtractDirRemovesEmptyDir pins that it still does its job.
func TestRemoveEmptyExtractDirRemovesEmptyDir(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "stale")
	require.NoError(t, os.MkdirAll(dir, 0o750))

	parentRoot, err := os.OpenRoot(parent)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parentRoot.Close() })

	require.NoError(t, removeEmptyExtractDir(parentRoot, "stale", filepath.Join(parent, "stale")))
	_, statErr := os.Stat(dir)
	assert.True(t, os.IsNotExist(statErr))
}

// TestExtractArchiveRefusesConflictingDestinationOptions covers a caller
// passing both destination policies.
//
// They describe incompatible things — merge accumulates into the destination,
// replace swaps it wholesale — and merge silently won, so a caller that meant
// to replace would have quietly kept the old files instead.
func TestExtractArchiveRefusesConflictingDestinationOptions(t *testing.T) {
	archivePath := writeTestArchive(t, map[string]string{
		"immutable/00000.chunk": "chunk0",
	})
	destDir := filepath.Join(t.TempDir(), "extracted")

	_, err := ExtractArchive(
		t.Context(), archivePath, destDir, nil,
		WithMergeIntoDestination(), WithReplaceDestination(),
	)
	require.ErrorIs(t, err, ErrExtractConflictingOptions)

	_, statErr := os.Stat(destDir)
	assert.True(t, os.IsNotExist(statErr),
		"a refused extraction must not create the destination")
}

// TestExtractPublishSurvivesDestinationToFileSubstitution is the regression
// test for a destination that turns from a directory into a file between the
// moment publication identifies it and the moment publication acts on it.
//
// The interleaving is staged rather than raced, because the window is interior
// to publication and cannot be driven from outside it. The steps are the ones
// publication takes, in order, with the substitution placed exactly where a
// concurrent writer would land it: the destination is observed as a directory,
// replaced with a file, and only then handed to the removal.
//
// The removal is what has to hold here. It cannot be told the destination
// changed, so it must be incapable of acting on the thing it changed into.
func TestExtractPublishSurvivesDestinationToFileSubstitution(t *testing.T) {
	parent := t.TempDir()
	destName := "extracted"
	destPath := filepath.Join(parent, destName)

	parentRoot, err := os.OpenRoot(parent)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parentRoot.Close() })

	// As publication finds it: an empty directory, cleared to make way.
	require.NoError(t, os.MkdirAll(destPath, 0o750))
	info, err := parentRoot.Lstat(destName)
	require.NoError(t, err)
	require.True(t, info.IsDir(), "the check must see a directory")

	// A concurrent writer takes the name for a file of their own.
	require.NoError(t, os.Remove(destPath))
	require.NoError(t, os.WriteFile(destPath, []byte("theirs"), 0o640))

	// Publication proceeds on what it observed, which is no longer true.
	require.Error(t, removeEmptyExtractDir(parentRoot, destName, destPath),
		"removal must refuse what the destination became")

	data, err := os.ReadFile(destPath)
	require.NoError(t, err, "the substituted file must not be unlinked")
	assert.Equal(t, "theirs", string(data))
}

// TestOpenVerifiedDirRefusesSymlinkedDir covers the cache-reuse fast paths,
// which decide whether a previous run already produced a usable tree.
//
// Those directories are derived inside the download directory rather than
// chosen by the operator, so a symlink at one of them is planted content, not
// a layout decision. Following it hands back a directory somebody else chose
// and skips the extraction that would have replaced it.
func TestOpenVerifiedDirRefusesSymlinkedDir(t *testing.T) {
	parent := t.TempDir()
	outside := filepath.Join(parent, "outside")
	require.NoError(t, os.MkdirAll(outside, 0o750))
	candidate := filepath.Join(parent, "extracted")
	requireSymlinkSupport(t, "outside", candidate)

	_, err := openVerifiedDir(candidate)
	require.ErrorIs(t, err, ErrExtractUnsafePath)
}

// TestOpenVerifiedDirOpensRealDir pins that an ordinary directory still opens,
// and that the handle refers to it.
func TestOpenVerifiedDirOpensRealDir(t *testing.T) {
	parent := t.TempDir()
	candidate := filepath.Join(parent, "extracted")
	require.NoError(t, os.MkdirAll(candidate, 0o750))

	root, err := openVerifiedDir(candidate)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })

	opened, err := root.Stat(".")
	require.NoError(t, err)
	want, err := os.Lstat(candidate)
	require.NoError(t, err)
	assert.True(t, os.SameFile(opened, want))
}

// TestVettedNamesInspectedDirectory pins the ordinary case: the inspected
// directory is returned, under the name that denotes it, with the handle still
// open so the consumer reads that directory rather than that name.
func TestVettedNamesInspectedDirectory(t *testing.T) {
	dir := t.TempDir()
	candidate := filepath.Join(dir, "immutable")
	require.NoError(t, os.MkdirAll(candidate, 0o750))

	root, err := openVerifiedDir(dir)
	require.NoError(t, err)
	self := vetted(root, dir, ".")
	require.NotNil(t, self)
	t.Cleanup(self.Close)
	assert.Equal(t, dir, self.Path())

	candidateRoot, err := openVerifiedDir(candidate)
	require.NoError(t, err)
	child := vetted(candidateRoot, dir, "immutable")
	require.NotNil(t, child)
	t.Cleanup(child.Close)
	assert.Equal(t, candidate, child.Path())

	// The handle survives the handoff, which is the whole point of it: a
	// consumer given only Path would resolve the name again.
	opened, err := child.Root().Stat(".")
	require.NoError(t, err)
	want, err := os.Lstat(candidate)
	require.NoError(t, err)
	assert.True(t, os.SameFile(opened, want))
}

// TestVettedRefusesSwappedDirectory covers a name that stopped denoting the
// directory it was inspected under.
//
// A handle refers to a directory; the name it was opened under refers to
// whatever currently occupies that name. The two disagreeing is evidence of
// interference, and the lookup refuses rather than reporting a cached snapshot
// under a name somebody else has taken.
func TestVettedRefusesSwappedDirectory(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "extracted")
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "immutable"), 0o750))

	root, err := openVerifiedDir(dir)
	require.NoError(t, err)

	theirs := filepath.Join(parent, "theirs")
	require.NoError(t, os.MkdirAll(filepath.Join(theirs, "immutable"), 0o750))
	requireDirectorySwap(t, dir, filepath.Join(parent, "moved-aside"))
	requireDirectorySwap(t, theirs, dir)

	assert.Nil(t, vetted(root, dir, "."),
		"the name no longer denotes the inspected directory")
}

// TestVettedRefusesCandidateSwappedAfterInspection covers the substitution
// of the candidate itself rather than the directory above it.
//
// Binding through the parent handle instead would compare two fresh
// resolutions of one name against each other: both would see the replacement,
// both would agree, and a tree that was never inspected would be returned. The
// comparison has to be against the directory that was read.
//
// The interleaving is staged rather than raced, because the window is interior
// to the lookup and cannot be driven from outside it. The steps below are the
// ones `chunkDirUnder` takes, in order, with the substitution placed exactly
// where a concurrent writer would land it: the candidate is opened, its chunk
// files are read, and only then is the name bound.
func TestVettedRefusesCandidateSwappedAfterInspection(t *testing.T) {
	base := t.TempDir()
	ours := filepath.Join(base, "immutable")
	require.NoError(t, os.MkdirAll(ours, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(ours, "00000.chunk"), []byte("ours"), 0o640,
	))

	baseRoot, err := openVerifiedDir(base)
	require.NoError(t, err)
	t.Cleanup(func() { _ = baseRoot.Close() })
	candidate, err := openVerifiedRoot(baseRoot, "immutable")
	require.NoError(t, err)
	require.True(t, hasChunkFilesIn(candidate, "."),
		"the read must see our tree, as the lookup would")

	// A writer takes the name for a tree of their own.
	theirs := filepath.Join(base, "theirs")
	require.NoError(t, os.MkdirAll(theirs, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(theirs, "00000.chunk"), []byte("theirs"), 0o640,
	))
	requireDirectorySwap(t, ours, filepath.Join(base, "moved-aside"))
	requireDirectorySwap(t, theirs, ours)

	assert.Nil(t, vetted(candidate, base, "immutable"),
		"a tree that was never inspected must not be returned")
}

// TestLedgerDir holds the ancillary fast path to the same rule as the immutable
// one: it returns the directory it inspected, as the handle it inspected it
// through, rather than a name the caller pairs with its own reading of the
// tree. The manifest check and the ledger-state import both go through that
// handle, so neither can end up describing a different tree.
func TestLedgerDir(t *testing.T) {
	t.Run("returns the inspected directory", func(t *testing.T) {
		dir := t.TempDir()
		state := filepath.Join(dir, "ledger", "42")
		require.NoError(t, os.MkdirAll(state, 0o750))
		require.NoError(t, os.WriteFile(
			filepath.Join(state, "state"), []byte("ours"), 0o640,
		))

		found := ledgerDir(dir)
		require.NotNil(t, found)
		t.Cleanup(found.Close)
		assert.Equal(t, dir, found.Path())

		opened, err := found.Root().Stat(".")
		require.NoError(t, err)
		want, err := os.Lstat(dir)
		require.NoError(t, err)
		assert.True(t, os.SameFile(opened, want),
			"the handle must refer to the directory that was inspected")
	})

	t.Run("refuses a tree without ledger state", func(t *testing.T) {
		assert.Nil(t, ledgerDir(t.TempDir()))
	})

	t.Run("refuses a symlinked directory", func(t *testing.T) {
		parent := t.TempDir()
		outside := filepath.Join(parent, "outside", "ledger", "42")
		require.NoError(t, os.MkdirAll(outside, 0o750))
		require.NoError(t, os.WriteFile(
			filepath.Join(outside, "state"), []byte("theirs"), 0o640,
		))
		candidate := filepath.Join(parent, "ancillary-abc123")
		requireSymlinkSupport(t, "outside", candidate)

		assert.Nil(t, ledgerDir(candidate))
	})
}

// TestOpenVerifiedDirAllowsSymlinkedAncestor keeps the boundary where the rest
// of the package puts it: the directories above a candidate belong to the
// operator, and rejecting them would break ordinary layouts rather than
// attackers.
func TestOpenVerifiedDirAllowsSymlinkedAncestor(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "real")
	require.NoError(t, os.MkdirAll(filepath.Join(real, "extracted"), 0o750))
	linked := filepath.Join(root, "linked")
	requireSymlinkSupport(t, real, linked)

	opened, err := openVerifiedDir(filepath.Join(linked, "extracted"))
	require.NoError(t, err)
	_ = opened.Close()
}

// TestExtractDoesNotAdoptAPreExistingFile is the difference between an inode
// this process owns and one it merely writes to.
//
// Merge extraction writes straight into a shared destination, so a name it is
// about to write can already be occupied by a file somebody else created.
// Opening that with O_CREATE|O_TRUNC keeps their inode, their owner and their
// mode, and puts certified bytes inside a file they can still write to — which
// no amount of verifying at the descriptor helps, because a same-inode write
// is visible through a descriptor already open on it.
//
// So the entry is unlinked and created exclusively: what gets written is
// always an inode created here, owned by this process, at extraction's own
// mode.
//
// The assertion is a write through the descriptor they kept, not a comparison
// of the two stat results. os.SameFile compares device and inode number, and
// an inode number is reused as soon as it is freed — an earlier version of
// this test unlinked the planted file, saw the number handed straight back on
// Linux, and reported adoption where there was none. That is the same reason
// the production code does not identify files by os.SameFile either. Holding
// the descriptor open also pins the old inode, so the reuse cannot happen and
// the test is deterministic rather than dependent on the allocator.
func TestExtractDoesNotAdoptAPreExistingFile(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })

	// Group- and world-writable, as somebody planting a file to write to
	// later would leave it — and they keep a descriptor on it, which is the
	// whole point of planting one.
	planted := filepath.Join(dir, "00000.chunk")
	require.NoError(t, os.WriteFile(planted, []byte("theirs"), 0o666))
	theirs, err := os.OpenFile(planted, os.O_WRONLY, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = theirs.Close() })

	file, err := createExtractedFile(root, "00000.chunk")
	if runtime.GOOS == "windows" {
		require.Error(t, err,
			"Windows refuses to unlink a file held open by another process")
		assert.ErrorIs(t, err, ErrExtractUnsafePath)
		contents, readErr := os.ReadFile(planted)
		require.NoError(t, readErr)
		assert.Equal(t, "theirs", string(contents),
			"a locked pre-existing file must remain untouched")
		return
	}
	require.NoError(t, err)
	_, writeErr := file.WriteString("ours")
	require.NoError(t, writeErr)
	require.NoError(t, file.Close())

	// They write through the descriptor they held all along. If extraction
	// had adopted their inode this reaches the extracted file, and no check
	// downstream could tell.
	_, err = theirs.WriteAt([]byte("XXXX"), 0)
	require.NoError(t, err)

	contents, err := os.ReadFile(planted)
	require.NoError(t, err)
	assert.Equal(t, "ours", string(contents),
		"a writer holding the pre-existing inode must not reach the "+
			"extracted file")
	if runtime.GOOS != "windows" {
		info, statErr := os.Stat(planted)
		require.NoError(t, statErr)
		assert.Equal(t, os.FileMode(0o640), info.Mode().Perm(),
			"the extracted file must carry extraction's own mode, not the "+
				"mode it found")
	}
}

// TestExtractRefusesADirectoryAtAFileName keeps the clearing step from being a
// removal of whatever it finds.
//
// Making the extracted file's inode our own means getting rid of what occupies
// the name, and the easy way to do that removes an empty directory as readily
// as a file. That is a different act: a file at the name was going to have its
// contents replaced anyway, but a directory is something extraction was never
// asked to touch — before this it failed on one, and it must keep failing
// rather than start deleting.
//
// It also has to be the removal that refuses, not a check before it. A check
// establishes what the name holds and the removal acts on what it holds now,
// and a writer between the two turns "refuse the directory" into "unlink their
// file".
func TestExtractRefusesADirectoryAtAFileName(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })

	occupied := filepath.Join(dir, "00000.chunk")
	require.NoError(t, os.Mkdir(occupied, 0o750))

	_, err = createExtractedFile(root, "00000.chunk")
	require.Error(t, err, "a directory at the name must fail the extraction")

	info, statErr := os.Lstat(occupied)
	require.NoError(t, statErr, "the directory must survive the refusal")
	assert.True(t, info.IsDir())
}

// TestRemoveExtractedFileRefusesASymlinkedParent keeps the clearing step's
// traversal under the same rule as the checks in front of it.
//
// os.Root confines resolution to the root but still follows a symlink whose
// target stays inside it, so opening the parent with Root.OpenRoot would let a
// component substituted mid-extraction redirect the unlink — and the retry
// after it — at another directory in the tree. Every component is therefore
// opened through its parent and confirmed to be the entry the name denotes,
// which rejects a symlink and a substitution with one check.
func TestRemoveExtractedFileRefusesASymlinkedParent(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "real"), 0o750))
	victim := filepath.Join(dir, "real", "00000.chunk")
	require.NoError(t, os.WriteFile(victim, []byte("ours"), 0o640))
	// Relative and inside the root: os.Root refuses an absolute link or one
	// leaving the root outright, so only this shape reaches the check.
	requireSymlinkSupport(t, "real", filepath.Join(dir, "link"))

	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })

	// Control: through the real name it is removed, so the refusal below is
	// about the symlink and not about the removal never working.
	require.NoError(t, removeExtractedFile(
		root, "real/00000.chunk", filepath.Join(dir, "real", "00000.chunk"),
	))
	require.NoError(t, os.WriteFile(victim, []byte("ours"), 0o640))

	err = removeExtractedFile(
		root, "link/00000.chunk", filepath.Join(dir, "link", "00000.chunk"),
	)
	require.Error(t, err, "a symlinked parent must not be traversed")
	_, statErr := os.Lstat(victim)
	assert.NoError(t, statErr,
		"the file behind the symlink must survive the refusal")
}
