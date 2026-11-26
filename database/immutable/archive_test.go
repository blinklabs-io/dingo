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

func archivedStoreBlocks(t *testing.T) []WriteBlock {
	t.Helper()
	// Chunks of 10 slots: one block in each of chunks 0, 1 and 2.
	return []WriteBlock{
		syntheticBlock(t, 1, 1, 1),
		syntheticBlock(t, 1, 12, 2),
		syntheticBlock(t, 1, 25, 3),
	}
}

func openFilledStore(
	t *testing.T,
	dir string,
	compress bool,
	blocks []WriteBlock,
) (*Store, []Location) {
	t.Helper()
	s, err := OpenStoreWithOptions(
		dir,
		Geometry{ChunkSlots: 10},
		StoreOptions{Compress: compress},
	)
	require.NoError(t, err)
	locs := make([]Location, 0, len(blocks))
	for _, b := range blocks {
		loc, err := s.Append(b)
		require.NoError(t, err)
		locs = append(locs, loc)
	}
	require.NoError(t, s.Sync())
	return s, locs
}

func requireExists(t *testing.T, path string, want bool) {
	t.Helper()
	_, err := os.Stat(path)
	if want {
		require.NoError(t, err, path)
	} else {
		require.ErrorIs(t, err, os.ErrNotExist, path)
	}
}

// TestCompressedStoreArchivesCompletedChunks appends across three chunks to a
// compressing store. The two completed chunks are stored only as archives in
// the Mithril layout, holding the bytes an uncompressed store writes, and the
// open chunk stays plain; every block reads back the same.
func TestCompressedStoreArchivesCompletedChunks(t *testing.T) {
	t.Parallel()
	blocks := archivedStoreBlocks(t)
	dir := t.TempDir()
	s, locs := openFilledStore(t, dir, true, blocks)
	t.Cleanup(func() { _ = s.Close() })
	plainDir := t.TempDir()
	plain, _ := openFilledStore(t, plainDir, false, blocks)
	require.NoError(t, plain.Close())

	for _, n := range []uint64{0, 1} {
		requireExists(t, filepath.Join(dir, ArchiveName(n)), true)
		requireExists(t, filepath.Join(dir, ChunkName(n)+".chunk"), false)
		files, err := readChunkArchive(filepath.Join(dir, ArchiveName(n)), n)
		require.NoError(t, err)
		for _, ext := range archiveFileExtensions {
			want, err := os.ReadFile(filepath.Join(plainDir, ChunkName(n)+ext))
			require.NoError(t, err)
			got, err := files.file(ext)
			require.NoError(t, err)
			require.Equal(t, want, got, "chunk %d %s", n, ext)
		}
	}
	requireExists(t, filepath.Join(dir, ChunkName(2)+".chunk"), true)
	requireExists(t, filepath.Join(dir, ArchiveName(2)), false)

	for i, b := range blocks {
		got, loc, err := s.GetBlock(b.Slot, b.Hash)
		require.NoError(t, err)
		require.NotNil(t, got, "slot %d", b.Slot)
		require.Equal(t, b.Cbor, got.Cbor)
		require.Equal(t, locs[i], *loc)
		typ, inner, err := ReadBlockAt(dir, locs[i])
		require.NoError(t, err)
		require.Equal(t, b.Type, typ)
		require.Equal(t, b.Cbor, inner)
	}
}

// TestCompressedStoreReopensAndKeepsArchiving reopens a compressing store and
// appends into a new chunk: the chunk it leaves is archived in turn.
func TestCompressedStoreReopensAndKeepsArchiving(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, _ := openFilledStore(t, dir, true, archivedStoreBlocks(t))
	require.NoError(t, s.Close())

	s, err := OpenStoreWithOptions(
		dir, Geometry{ChunkSlots: 10}, StoreOptions{Compress: true},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	tip, ok := s.Tip()
	require.True(t, ok)
	require.Equal(t, uint64(25), tip.Slot)
	_, err = s.Append(syntheticBlock(t, 1, 31, 4))
	require.NoError(t, err)
	requireExists(t, filepath.Join(dir, ArchiveName(2)), true)
	requireExists(t, filepath.Join(dir, ChunkName(2)+".chunk"), false)
	got, _, err := s.GetBlock(25, archivedStoreBlocks(t)[2].Hash)
	require.NoError(t, err)
	require.NotNil(t, got)
}

// TestTruncateIntoArchivedChunk truncates to a block in an archived chunk:
// the chunk returns to plain files, cut after the point, as the new tail.
func TestTruncateIntoArchivedChunk(t *testing.T) {
	t.Parallel()
	blocks := archivedStoreBlocks(t)
	dir := t.TempDir()
	s, _ := openFilledStore(t, dir, true, blocks)
	t.Cleanup(func() { _ = s.Close() })

	require.NoError(t, s.TruncateAfter(blocks[1].Slot, blocks[1].Hash))
	requireExists(t, filepath.Join(dir, ArchiveName(1)), false)
	requireExists(t, filepath.Join(dir, ChunkName(1)+".chunk"), true)
	requireExists(t, filepath.Join(dir, ChunkName(2)+".chunk"), false)
	tip, ok := s.Tip()
	require.True(t, ok)
	require.Equal(t, blocks[1].Slot, tip.Slot)
	got, _, err := s.GetBlock(blocks[0].Slot, blocks[0].Hash)
	require.NoError(t, err)
	require.NotNil(t, got)
	_, err = s.Append(syntheticBlock(t, 1, 14, 5))
	require.NoError(t, err)
}

// TestRecoverySettlesAnInterruptedPack leaves what a crash during a pack
// can: the plain files beside an archive already renamed into place, and a
// temporary file. Reopening keeps the archive as the chunk's only form.
func TestRecoverySettlesAnInterruptedPack(t *testing.T) {
	t.Parallel()
	blocks := archivedStoreBlocks(t)
	dir := t.TempDir()
	s, _ := openFilledStore(t, dir, true, blocks)
	require.NoError(t, s.Close())
	plainDir := t.TempDir()
	plain, _ := openFilledStore(t, plainDir, false, blocks)
	require.NoError(t, plain.Close())
	for _, ext := range archiveFileExtensions {
		data, err := os.ReadFile(filepath.Join(plainDir, ChunkName(0)+ext))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, ChunkName(0)+ext), data, 0o644,
		))
	}
	tmp := filepath.Join(dir, ArchiveName(1)+".tmp")
	require.NoError(t, os.WriteFile(tmp, []byte("partial"), 0o644))

	s, err := OpenStoreWithOptions(
		dir, Geometry{ChunkSlots: 10}, StoreOptions{Compress: true},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	requireExists(t, filepath.Join(dir, ChunkName(0)+".chunk"), false)
	requireExists(t, filepath.Join(dir, ArchiveName(0)), true)
	requireExists(t, tmp, false)
	got, _, err := s.GetBlock(blocks[0].Slot, blocks[0].Hash)
	require.NoError(t, err)
	require.NotNil(t, got)
}

// TestUncompressedStoreReadsArchivedChunks opens a store with archives as an
// uncompressed store: it reads them and leaves them archived.
func TestUncompressedStoreReadsArchivedChunks(t *testing.T) {
	t.Parallel()
	blocks := archivedStoreBlocks(t)
	dir := t.TempDir()
	s, _ := openFilledStore(t, dir, true, blocks)
	require.NoError(t, s.Close())

	s, err := OpenStore(dir, Geometry{ChunkSlots: 10})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	for _, b := range blocks {
		got, _, err := s.GetBlock(b.Slot, b.Hash)
		require.NoError(t, err)
		require.NotNil(t, got)
	}
	_, err = s.Append(syntheticBlock(t, 1, 31, 4))
	require.NoError(t, err)
	requireExists(t, filepath.Join(dir, ChunkName(2)+".chunk"), true)
	requireExists(t, filepath.Join(dir, ArchiveName(0)), true)
}

// TestArchiveCompletedChunk archives a chunk a store wrote plain, as Mithril
// adoption does once a chunk is committed. The highest chunk, which may
// still be appended to, is left plain.
func TestArchiveCompletedChunk(t *testing.T) {
	t.Parallel()
	blocks := archivedStoreBlocks(t)
	dir := t.TempDir()
	s, _ := openFilledStore(t, dir, false, blocks)
	require.NoError(t, s.Close())

	require.NoError(t, ArchiveCompletedChunk(dir, 2))
	requireExists(t, filepath.Join(dir, ArchiveName(2)), false)
	require.NoError(t, ArchiveCompletedChunk(dir, 0))
	requireExists(t, filepath.Join(dir, ArchiveName(0)), true)
	requireExists(t, filepath.Join(dir, ChunkName(0)+".chunk"), false)
	require.NoError(t, ArchiveCompletedChunk(dir, 0))

	s, err := OpenStore(dir, Geometry{ChunkSlots: 10})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	got, _, err := s.GetBlock(blocks[0].Slot, blocks[0].Hash)
	require.NoError(t, err)
	require.NotNil(t, got)
}

// TestArchiveCacheEvictsToItsBound fills a cache sized for one of two
// archived chunks: reading the second evicts the first, the bytes held never
// exceed the bound, and an archive larger than the bound is not kept.
func TestArchiveCacheEvictsToItsBound(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, _ := openFilledStore(t, dir, true, archivedStoreBlocks(t))
	require.NoError(t, s.Close())
	paths := []string{
		filepath.Join(dir, ArchiveName(0)),
		filepath.Join(dir, ArchiveName(1)),
	}
	sizes := make([]int64, len(paths))
	for i, p := range paths {
		files, err := readChunkArchive(p, uint64(i)) // #nosec G115
		require.NoError(t, err)
		sizes[i] = files.size()
	}

	c := newArchiveCache(max(sizes[0], sizes[1]))
	for i, p := range paths {
		_, err := c.get(p, uint64(i)) // #nosec G115
		require.NoError(t, err)
		require.LessOrEqual(t, c.used, c.maxBytes)
	}
	require.Len(t, c.entries, 1)
	require.Equal(t, sizes[1], c.used)
	for key := range c.entries {
		require.Equal(t, paths[1], key.path)
	}

	small := newArchiveCache(min(sizes[0], sizes[1]) - 1)
	files, err := small.get(paths[0], 0)
	require.NoError(t, err)
	require.NotNil(t, files)
	require.Empty(t, small.entries)
	require.Zero(t, small.used)
}

// TestSetArchiveCacheBytesBoundsReads sets the shared cache's bound below
// one archived chunk: reads still succeed and the cache keeps nothing.
//
// Not t.Parallel: it changes the process-wide archive cache bound.
func TestSetArchiveCacheBytesBoundsReads(t *testing.T) {
	SetArchiveCacheBytes(1)
	t.Cleanup(func() { SetArchiveCacheBytes(DefaultArchiveCacheBytes) })
	blocks := archivedStoreBlocks(t)
	dir := t.TempDir()
	s, locs := openFilledStore(t, dir, true, blocks)
	t.Cleanup(func() { _ = s.Close() })

	_, inner, err := ReadBlockAt(dir, locs[0])
	require.NoError(t, err)
	require.Equal(t, blocks[0].Cbor, inner)
	c := sharedArchiveCache
	c.mu.Lock()
	defer c.mu.Unlock()
	require.LessOrEqual(t, c.used, int64(1))
}
