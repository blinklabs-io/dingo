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

package immutable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListFilesIgnoresForeignNames(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, name := range []string{
		"00001.chunk", "00001.primary", "00000.secondary",
		"00002.chunk.unshare", "notes.txt", "1.chunk",
	} {
		require.NoError(
			t,
			os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600),
		)
	}
	files, err := ListFiles(dir)
	require.NoError(t, err)
	var names []string
	for _, f := range files {
		names = append(names, f.Name)
	}
	require.Equal(
		t,
		[]string{"00000.secondary", "00001.chunk", "00001.primary"},
		names,
	)
	none, err := ListFiles(filepath.Join(dir, "absent"))
	require.NoError(t, err)
	require.Empty(t, none)
}

// A snapshot hard-links completed chunks, so cutting or extending a linked
// chunk in place must leave the linked copy's bytes alone.
func TestStoreEditsNeverChangeHardLinkedChunk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := OpenStore(dir, previewGeometry)
	require.NoError(t, err)
	blocks := fixtureBlocks(t, 2)
	for _, b := range blocks {
		_, err := s.Append(b)
		require.NoError(t, err)
	}
	require.NoError(t, s.Sync())
	require.NoError(t, s.Close())

	linked := t.TempDir()
	want := map[string][]byte{}
	for _, f := range []string{"00000.chunk", "00000.secondary", "00000.primary"} {
		require.NoError(
			t,
			os.Link(filepath.Join(dir, f), filepath.Join(linked, f)),
		)
		b, err := os.ReadFile(filepath.Join(dir, f))
		require.NoError(t, err)
		want[f] = b
	}

	s, err = OpenStore(dir, previewGeometry)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	var inFirst []WriteBlock
	for _, b := range blocks {
		if b.Slot < previewGeometry.ChunkSlots {
			inFirst = append(inFirst, b)
		}
	}
	require.Greater(t, len(inFirst), 2)
	cut := inFirst[len(inFirst)/2]
	require.NoError(t, s.TruncateAfter(cut.Slot, cut.Hash))

	for f, b := range want {
		got, err := os.ReadFile(filepath.Join(linked, f))
		require.NoError(t, err)
		require.Equal(t, b, got, "linked %s changed", f)
	}
	live, err := os.Stat(filepath.Join(dir, "00000.chunk"))
	require.NoError(t, err)
	require.Less(t, live.Size(), int64(len(want["00000.chunk"])))
}

func writeChunkZero(t *testing.T) (string, []WriteBlock, []Location) {
	t.Helper()
	dir := t.TempDir()
	s, err := OpenStore(dir, previewGeometry)
	require.NoError(t, err)
	var blocks []WriteBlock
	for _, b := range fixtureBlocks(t, 2) {
		if b.Slot < previewGeometry.ChunkSlots {
			blocks = append(blocks, b)
		}
	}
	require.Greater(t, len(blocks), 10)
	locs := make([]Location, len(blocks))
	for i, b := range blocks {
		loc, err := s.Append(b)
		require.NoError(t, err)
		locs[i] = loc
	}
	require.NoError(t, s.Sync())
	require.NoError(t, s.Close())
	return dir, blocks, locs
}

func flipByteAt(t *testing.T, path string, off uint64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var b [1]byte
	_, err = f.ReadAt(b[:], int64(off))
	require.NoError(t, err)
	b[0] ^= 0xFF
	_, err = f.WriteAt(b[:], int64(off))
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

// A bad entry followed by good ones is corruption inside promoted data, not a
// torn tail: trimming there would silently discard good blocks and keeping the
// entry would leave a block that cannot be read. Recovery refuses both.
func TestStoreRecoveryRefusesCorruptionBeforeTheTail(t *testing.T) {
	t.Parallel()
	dir, blocks, locs := writeChunkZero(t)
	mid := len(blocks) / 2
	flipByteAt(t, filepath.Join(dir, "00000.chunk"),
		locs[mid].Offset+uint64(locs[mid].Size)/2)
	_, err := OpenStore(dir, previewGeometry)
	require.ErrorIs(t, err, ErrStoreCorrupt)
}

// Damage confined to the last entries is a torn tail and is trimmed.
func TestStoreRecoveryTrimsEveryBadEntryAtTheTail(t *testing.T) {
	t.Parallel()
	dir, blocks, locs := writeChunkZero(t)
	n := len(blocks)
	for _, i := range []int{n - 2, n - 1} {
		flipByteAt(t, filepath.Join(dir, "00000.chunk"),
			locs[i].Offset+uint64(locs[i].Size)/2)
	}
	s, err := OpenStore(dir, previewGeometry)
	require.NoError(t, err)
	defer s.Close()
	tip, ok := s.Tip()
	require.True(t, ok)
	require.Equal(t, blocks[n-3].Slot, tip.Slot)
}
