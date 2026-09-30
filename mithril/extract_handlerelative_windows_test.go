//go:build windows
//go:build windows
//go:build windows

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
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

// TestBuildRenameInformationLayout decodes the buffer by hand, at the byte
// level, rather than casting it back through fileRenameInformationHeader —
// the whole risk here is that Go's struct layout and NtSetInformationFile's
// expected wire layout could silently disagree, and reinterpreting the same
// bytes through the same struct would not catch that; only an independent,
// offset-by-offset reading would.
//
// The layout is: ReplaceIfExists (1 byte), 7 bytes of alignment padding on a
// 64-bit target, RootDirectory (pointer-sized), FileNameLength (4 bytes,
// little-endian), then FileName immediately after with no further padding.
func TestBuildRenameInformationLayout(t *testing.T) {
	t.Parallel()

	const destDir = windows.Handle(0x1234)
	buf, err := buildRenameInformation(destDir, "dest.tmp", true)
	require.NoError(t, err)

	handleSize := int(unsafe.Sizeof(windows.Handle(0)))
	// A 1-byte bool pads out to the handle's own alignment, which for a
	// power-of-two size no larger than the handle equals the handle size
	// itself.
	rootDirOffset := handleSize
	nameOffset := rootDirOffset + handleSize + 4

	require.GreaterOrEqual(t, len(buf), nameOffset)
	assert.Equal(t, byte(1), buf[0], "ReplaceIfExists must be TRUE")

	var gotHandle uint64
	for i := range handleSize {
		gotHandle |= uint64(buf[rootDirOffset+i]) << (8 * i)
	}
	assert.Equal(t, uint64(destDir), gotHandle,
		"RootDirectory must round-trip through the buffer")

	nameLen := binary.LittleEndian.Uint32(buf[rootDirOffset+handleSize:])
	assert.Equal(t, uint32(len("dest.tmp")*2), nameLen,
		"FileNameLength must count UTF-16 bytes, excluding any terminator")
	assert.Len(
		t,
		buf,
		nameOffset+int(nameLen),
		"the buffer must hold exactly the header plus the encoded name, no extra padding",
	)

	gotName := windows.UTF16ToString(
		unsafe.Slice((*uint16)(unsafe.Pointer(&buf[nameOffset])), nameLen/2),
	)
	assert.Equal(t, "dest.tmp", gotName)
}

func TestBuildRenameInformationReplaceIfExistsFalse(t *testing.T) {
	t.Parallel()

	buf, err := buildRenameInformation(windows.Handle(1), "x", false)
	require.NoError(t, err)
	assert.Equal(t, byte(0), buf[0], "ReplaceIfExists must be FALSE")
}

func TestBuildRenameInformationEmptyName(t *testing.T) {
	t.Parallel()

	handleSize := int(unsafe.Sizeof(windows.Handle(0)))
	rootDirOffset := handleSize
	nameOffset := rootDirOffset + handleSize + 4
	buf, err := buildRenameInformation(windows.Handle(1), "", true)
	require.NoError(t, err)
	assert.Len(t, buf, nameOffset)
	assert.Equal(
		t,
		uint32(0),
		binary.LittleEndian.Uint32(buf[rootDirOffset+handleSize:]),
	)
}

// These tests cover the case issue #3228 says the directory-component walk
// (openVerifiedParent/openVerifiedRoot) cannot catch by itself: a component
// the walk already verified is substituted afterward, while the walk's own
// handle on it is still held. Under the old MoveFile/DeleteFile/RemoveDirectory
// implementation that substitution was followed, because those APIs address
// their target by resolving a path a second time. Each test therefore walks
// and verifies a component first, substitutes it on disk exactly as a writer
// with access to the parent could, and only then performs the operation —
// reproducing the gap between verification and the act that a single,
// uninterrupted call cannot exercise from outside.
//
// If any of these regress to a path-based resolution, they fail by acting on
// the substituted tree instead of the one the walk verified.

func TestHandleRelativeDeletionSurvivesParentSubstitution(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "real"), 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "real", "00000.chunk"), []byte("ours"), 0o640,
	))

	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })

	// This is exactly removeExtractedFile's own first step: walk and verify
	// "real", holding the resulting handle open across what follows.
	parent, base, release, err := openVerifiedParent(root, "real/00000.chunk")
	require.NoError(t, err)
	defer release()
	require.Equal(t, "00000.chunk", base)
	dirFile, dirHandle, err := rootDirHandle(parent)
	require.NoError(t, err)
	defer dirFile.Close()

	// A writer with access to dir substitutes "real" after the walk above
	// already verified and held it.
	elsewhere := filepath.Join(dir, "elsewhere")
	require.NoError(t, os.MkdirAll(elsewhere, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(elsewhere, "00000.chunk"), []byte("theirs"), 0o640,
	))
	requireDirectorySwap(
		t, filepath.Join(dir, "real"), filepath.Join(dir, "real.moved-aside"),
	)
	requireSymlinkSupport(t, elsewhere, filepath.Join(dir, "real"))

	// Addressed through dirHandle, obtained before the substitution — not
	// through the name "real", which now refers to the symlink above.
	handle, err := openRelativeForDeletion(
		dirHandle, base, windows.FILE_NON_DIRECTORY_FILE,
	)
	require.NoError(t, err)
	require.NoError(t, setDeleteDisposition(handle))
	require.NoError(t, windows.CloseHandle(handle))

	_, err = os.Lstat(filepath.Join(dir, "real.moved-aside", "00000.chunk"))
	assert.True(t, os.IsNotExist(err),
		"the file the walk verified must be removed through the held handle")
	_, err = os.Lstat(filepath.Join(elsewhere, "00000.chunk"))
	assert.NoError(t, err,
		"the substituted tree must not be reachable through the held handle")
}

func TestHandleRelativeRmdirSurvivesParentSubstitution(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "real"), 0o750))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "real", "empty"), 0o750))

	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })

	parent, base, release, err := openVerifiedParent(root, "real/empty")
	require.NoError(t, err)
	defer release()
	require.Equal(t, "empty", base)
	dirFile, dirHandle, err := rootDirHandle(parent)
	require.NoError(t, err)
	defer dirFile.Close()

	elsewhere := filepath.Join(dir, "elsewhere")
	require.NoError(t, os.MkdirAll(filepath.Join(elsewhere, "empty"), 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(elsewhere, "empty", "sentinel"), []byte("theirs"), 0o640,
	))
	requireDirectorySwap(
		t, filepath.Join(dir, "real"), filepath.Join(dir, "real.moved-aside"),
	)
	requireSymlinkSupport(t, elsewhere, filepath.Join(dir, "real"))

	handle, err := openRelativeForDeletion(
		dirHandle, base, windows.FILE_DIRECTORY_FILE,
	)
	require.NoError(t, err)
	require.NoError(t, setDeleteDisposition(handle))
	require.NoError(t, windows.CloseHandle(handle))

	_, err = os.Lstat(filepath.Join(dir, "real.moved-aside", "empty"))
	assert.True(t, os.IsNotExist(err),
		"the empty directory the walk verified must be removed")
	_, err = os.Lstat(filepath.Join(elsewhere, "empty", "sentinel"))
	assert.NoError(t, err,
		"the substituted, non-empty directory must survive untouched")
}

func TestHandleRelativeRenameSurvivesParentSubstitution(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(
		t,
		os.MkdirAll(filepath.Join(dir, "real", "staging"), 0o750),
	)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "destreal"), 0o750))

	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })

	oldParent, oldBase, releaseOld, err := openVerifiedParent(
		root,
		"real/staging",
	)
	require.NoError(t, err)
	defer releaseOld()
	newParent, newBase, releaseNew, err := openVerifiedParent(
		root,
		"destreal/moved",
	)
	require.NoError(t, err)
	defer releaseNew()
	oldDirFile, oldDir, err := rootDirHandle(oldParent)
	require.NoError(t, err)
	defer oldDirFile.Close()
	newDirFile, newDir, err := rootDirHandle(newParent)
	require.NoError(t, err)
	defer newDirFile.Close()

	// A writer with access to dir substitutes both endpoints' parents after
	// the walks above already verified and held them.
	elsewhereOld := filepath.Join(dir, "elsewhere-old")
	require.NoError(t, os.MkdirAll(elsewhereOld, 0o750))
	requireDirectorySwap(
		t, filepath.Join(dir, "real"), filepath.Join(dir, "real.moved-aside"),
	)
	requireSymlinkSupport(t, elsewhereOld, filepath.Join(dir, "real"))

	elsewhereNew := filepath.Join(dir, "elsewhere-new")
	require.NoError(t, os.MkdirAll(elsewhereNew, 0o750))
	requireDirectorySwap(
		t,
		filepath.Join(dir, "destreal"),
		filepath.Join(dir, "destreal.moved-aside"),
	)
	requireSymlinkSupport(t, elsewhereNew, filepath.Join(dir, "destreal"))

	source, err := openRelativeForRename(oldDir, oldBase)
	require.NoError(t, err)
	defer func() { _ = windows.CloseHandle(source) }()
	require.NoError(t, renameRelativeHandle(source, newDir, newBase, false))

	_, err = os.Lstat(filepath.Join(dir, "destreal.moved-aside", "moved"))
	assert.NoError(t, err,
		"the rename must land beside the destination the walk verified")
	entries, err := os.ReadDir(elsewhereNew)
	require.NoError(t, err)
	assert.Empty(t, entries, "the substituted destination must receive nothing")

	_, err = os.Lstat(filepath.Join(dir, "real.moved-aside", "staging"))
	assert.True(t, os.IsNotExist(err),
		"the directory the walk verified as the source must have moved")
	entries, err = os.ReadDir(elsewhereOld)
	require.NoError(t, err)
	assert.Empty(t, entries, "the substituted source parent must be untouched")
}
