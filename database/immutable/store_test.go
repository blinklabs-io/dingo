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
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// previewGeometry is the geometry of the preview fixture chunks.
var previewGeometry = Geometry{ChunkSlots: 4320, CanContainEBB: true}

func fixtureBlocks(t *testing.T, chunks int) []WriteBlock {
	t.Helper()
	db, err := New("testdata")
	require.NoError(t, err)
	var out []WriteBlock
	for n := range chunks {
		it, err := db.BlocksInChunk(uint64(n))
		require.NoError(t, err)
		for {
			b, err := it.Next()
			require.NoError(t, err)
			if b == nil {
				break
			}
			out = append(out, WriteBlock{
				Hash: b.Hash, Cbor: b.Cbor, Type: b.Type, Slot: b.Slot,
			})
		}
		require.NoError(t, it.Close())
	}
	return out
}

func requireSameFiles(t *testing.T, dir string, chunks int) {
	t.Helper()
	for n := range chunks {
		for _, ext := range []string{".chunk", ".secondary", ".primary"} {
			name := ChunkName(uint64(n)) + ext
			want, err := os.ReadFile(filepath.Join("testdata", name))
			require.NoError(t, err)
			got, err := os.ReadFile(filepath.Join(dir, name))
			require.NoError(t, err)
			require.Equal(t, want, got, name)
		}
	}
}

func TestStoreRoundTripIsByteIdentical(t *testing.T) {
	t.Parallel()
	const chunks = 3
	dir := t.TempDir()
	s, err := OpenStore(dir, previewGeometry)
	require.NoError(t, err)
	blocks := fixtureBlocks(t, chunks)
	for _, b := range blocks {
		_, err := s.Append(b)
		require.NoError(t, err)
	}
	require.NoError(t, s.Sync())
	// The tail chunk's primary is only complete once a later chunk starts, so
	// compare the two finished chunks.
	requireSameFiles(t, dir, chunks-1)
	last := blocks[len(blocks)-1]
	tip, ok := s.Tip()
	require.True(t, ok)
	require.Equal(t, last.Slot, tip.Slot)
	require.NoError(t, s.Close())
}

func TestStorePointReadIsO1AndMatchesScan(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := OpenStore(dir, previewGeometry)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	blocks := fixtureBlocks(t, 2)
	for _, b := range blocks {
		_, err := s.Append(b)
		require.NoError(t, err)
	}
	for _, i := range []int{0, 1, len(blocks) / 2, len(blocks) - 1} {
		got, loc, err := s.GetBlock(blocks[i].Slot, blocks[i].Hash)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.NotNil(t, loc)
		require.Equal(t, blocks[i].Cbor, got.Cbor)
		require.Equal(t, blocks[i].Type, got.Type)
	}
	// Wrong hash and empty slot both miss.
	got, _, err := s.GetBlock(blocks[0].Slot, make([]byte, 32))
	require.NoError(t, err)
	require.Nil(t, got)
	got, _, err = s.GetBlock(blocks[0].Slot+1, blocks[0].Hash)
	require.NoError(t, err)
	require.Nil(t, got)
}

// syntheticBlock builds a block whose payload is a CBOR array the store can
// wrap; the store never interprets it beyond the header position.
func syntheticBlock(t *testing.T, typ uint, slot uint64, tag byte) WriteBlock {
	t.Helper()
	hash := make([]byte, 32)
	hash[0], hash[1] = tag, byte(slot)
	// [header(bytes), body(bytes)]
	payload := append(
		[]byte{0x82, 0x43, tag, 0x01, 0x02, 0x44},
		4,
		5,
		6,
		byte(slot),
	)
	return WriteBlock{Hash: hash, Cbor: payload, Type: typ, Slot: slot}
}

func TestStoreEbbSkippedChunksAndReopen(t *testing.T) {
	t.Parallel()
	geom := Geometry{ChunkSlots: 10, CanContainEBB: true}
	dir := t.TempDir()
	s, err := OpenStore(dir, geom)
	require.NoError(t, err)
	// EBB of epoch 2 (slot 20) then a regular block at slot 20 and one in
	// chunk 4, leaving chunks 0, 1 and 3 empty.
	blocks := []WriteBlock{
		syntheticBlock(t, 0, 20, 1),
		syntheticBlock(t, 1, 20, 2),
		syntheticBlock(t, 1, 45, 3),
	}
	for _, b := range blocks {
		_, err := s.Append(b)
		require.NoError(t, err)
	}
	require.NoError(t, s.Sync())
	for n := range uint64(5) {
		_, err := os.Stat(filepath.Join(dir, ChunkName(n)+".primary"))
		require.NoError(t, err, "chunk %d", n)
	}
	st, err := os.Stat(filepath.Join(dir, "00000.chunk"))
	require.NoError(t, err)
	require.Zero(t, st.Size())
	// Empty chunks are complete: version byte plus relSlots+1 zero offsets.
	pri, err := os.ReadFile(filepath.Join(dir, "00000.primary"))
	require.NoError(t, err)
	require.Len(t, pri, 1+4*(int(geom.relSlots())+1))

	for _, b := range blocks {
		got, _, err := s.GetBlock(b.Slot, b.Hash)
		require.NoError(t, err)
		require.NotNil(t, got, "slot %d type %d", b.Slot, b.Type)
		require.Equal(t, b.Cbor, got.Cbor)
		require.Equal(t, b.Type == 0, got.IsEbb)
	}
	// Reads through the generic reader see the EBB with its epoch.
	require.NoError(t, s.Close())
	db, err := New(dir)
	require.NoError(t, err)
	it, err := db.BlocksFromPoint(ocommonOrigin())
	require.NoError(t, err)
	b0, err := it.Next()
	require.NoError(t, err)
	require.NotNil(t, b0)
	require.True(t, b0.IsEbb)
	require.Equal(t, uint64(2), b0.Slot)
	require.NoError(t, it.Close())

	s, err = OpenStore(dir, geom)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	tip, ok := s.Tip()
	require.True(t, ok)
	require.Equal(t, uint64(45), tip.Slot)
	_, err = s.Append(syntheticBlock(t, 1, 44, 9))
	require.ErrorIs(t, err, ErrStoreOutOfOrder)
	_, err = s.Append(syntheticBlock(t, 1, 46, 9))
	require.NoError(t, err)
}

func TestStoreRecoversTornAppendAtEverySeam(t *testing.T) {
	t.Parallel()
	geom := Geometry{ChunkSlots: 10, CanContainEBB: false}
	// Each case cuts the third append short after the named file was written.
	cases := []struct {
		name string
		cut  func(t *testing.T, dir string)
	}{
		{"chunk written only", func(t *testing.T, dir string) {
			truncateTo(t, filepath.Join(dir, "00000.secondary"), 2*56)
			truncateTo(t, filepath.Join(dir, "00000.primary"), 1+4*4)
		}},
		{"chunk and secondary written", func(t *testing.T, dir string) {
			truncateTo(t, filepath.Join(dir, "00000.primary"), 1+4*4)
		}},
		{"torn chunk bytes", func(t *testing.T, dir string) {
			p := filepath.Join(dir, "00000.chunk")
			st, err := os.Stat(p)
			require.NoError(t, err)
			truncateTo(t, p, st.Size()-3)
		}},
		{"torn primary word", func(t *testing.T, dir string) {
			p := filepath.Join(dir, "00000.primary")
			st, err := os.Stat(p)
			require.NoError(t, err)
			truncateTo(t, p, st.Size()-2)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			s, err := OpenStore(dir, geom)
			require.NoError(t, err)
			blocks := []WriteBlock{
				syntheticBlock(t, 1, 0, 1),
				syntheticBlock(t, 1, 2, 2),
				syntheticBlock(t, 1, 5, 3),
			}
			for _, b := range blocks {
				_, err := s.Append(b)
				require.NoError(t, err)
			}
			require.NoError(t, s.Sync())
			require.NoError(t, s.Close())
			tc.cut(t, dir)

			s, err = OpenStore(dir, geom)
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			tip, ok := s.Tip()
			require.True(t, ok)
			require.Equal(t, uint64(2), tip.Slot)
			got, _, err := s.GetBlock(2, blocks[1].Hash)
			require.NoError(t, err)
			require.NotNil(t, got)
			gone, _, err := s.GetBlock(5, blocks[2].Hash)
			require.NoError(t, err)
			require.Nil(t, gone)
			// The store is appendable again and the files stay consistent.
			_, err = s.Append(blocks[2])
			require.NoError(t, err)
			require.NoError(t, s.Sync())
			got, _, err = s.GetBlock(5, blocks[2].Hash)
			require.NoError(t, err)
			require.NotNil(t, got)
		})
	}
}

func TestStoreTruncateAfterMidChunk(t *testing.T) {
	t.Parallel()
	geom := Geometry{ChunkSlots: 10}
	dir := t.TempDir()
	s, err := OpenStore(dir, geom)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	blocks := []WriteBlock{
		syntheticBlock(t, 1, 1, 1),
		syntheticBlock(t, 1, 4, 2),
		syntheticBlock(t, 1, 7, 3),
		syntheticBlock(t, 1, 12, 4),
		syntheticBlock(t, 1, 25, 5),
	}
	for _, b := range blocks {
		_, err := s.Append(b)
		require.NoError(t, err)
	}
	require.NoError(t, s.TruncateAfter(4, blocks[1].Hash))
	_, err = os.Stat(filepath.Join(dir, "00001.chunk"))
	require.ErrorIs(t, err, os.ErrNotExist)
	tip, ok := s.Tip()
	require.True(t, ok)
	require.Equal(t, uint64(4), tip.Slot)
	gone, _, err := s.GetBlock(7, blocks[2].Hash)
	require.NoError(t, err)
	require.Nil(t, gone)
	_, err = s.Append(blocks[2])
	require.NoError(t, err)
	got, _, err := s.GetBlock(7, blocks[2].Hash)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Error(t, s.TruncateAfter(4, make([]byte, 32)))
	require.NoError(t, s.TruncateAll())
	_, ok = s.Tip()
	require.False(t, ok)
}

func truncateTo(t *testing.T, path string, size int64) {
	t.Helper()
	require.NoError(t, os.Truncate(path, size))
}

func ocommonOrigin() ocommon.Point { return ocommon.NewPointOrigin() }

// syncCounter records which files the store fsyncs.
type syncCounter struct {
	mu    sync.Mutex
	names map[string]int
}

func (c *syncCounter) sync(f *os.File) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.names[filepath.Base(f.Name())]++
	return f.Sync()
}

func (c *syncCounter) count(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.names[name]
}

func TestStoreAdvancingChunkFsyncsFinishedChunk(t *testing.T) {
	t.Parallel()
	geom := Geometry{ChunkSlots: 10, CanContainEBB: false}
	s, err := OpenStore(t.TempDir(), geom)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	c := &syncCounter{names: map[string]int{}}
	s.syncFile = c.sync
	// First block lands in chunk 2, so chunks 0 and 1 are skipped trios.
	_, err = s.Append(syntheticBlock(t, 1, 21, 1))
	require.NoError(t, err)
	for n := range 2 {
		for _, ext := range []string{".chunk", ".secondary", ".primary"} {
			require.Positive(t, c.count(ChunkName(uint64(n))+ext),
				"skipped chunk %d%s was closed without fsync", n, ext)
		}
	}
	// Crossing from chunk 2 to chunk 3 without calling Sync.
	_, err = s.Append(syntheticBlock(t, 1, 25, 2))
	require.NoError(t, err)
	for _, ext := range []string{".chunk", ".secondary", ".primary"} {
		require.Zero(t, c.count("00002"+ext))
	}
	_, err = s.Append(syntheticBlock(t, 1, 31, 3))
	require.NoError(t, err)
	for _, ext := range []string{".chunk", ".secondary", ".primary"} {
		require.Positive(t, c.count("00002"+ext),
			"finished chunk 2%s was closed without fsync", ext)
	}
}

func TestStoreRecoveryRejectsNonContiguousEntries(t *testing.T) {
	t.Parallel()
	geom := Geometry{ChunkSlots: 10, CanContainEBB: false}
	dir := t.TempDir()
	s, err := OpenStore(dir, geom)
	require.NoError(t, err)
	var blocks []WriteBlock
	var locs []Location
	for i := range 3 {
		b := syntheticBlock(t, 1, uint64(i+1), byte(i+1))
		loc, err := s.Append(b)
		require.NoError(t, err)
		blocks, locs = append(blocks, b), append(locs, loc)
	}
	require.NoError(t, s.Sync())
	require.NoError(t, s.Close())
	// Open a gap before entry 1 and shift entries 1 and 2 over it. Their own
	// bytes and checksums still agree.
	const gap = 7
	chunkPath := filepath.Join(dir, "00000.chunk")
	data, err := os.ReadFile(chunkPath)
	require.NoError(t, err)
	cut := int(locs[1].Offset)
	shifted := append(append(append([]byte(nil), data[:cut]...),
		make([]byte, gap)...), data[cut:]...)
	require.NoError(t, os.WriteFile(chunkPath, shifted, 0o644))
	secPath := filepath.Join(dir, "00000.secondary")
	sec, err := os.ReadFile(secPath)
	require.NoError(t, err)
	for i := 1; i < 3; i++ {
		off := i * secondaryIndexEntrySize
		binary.BigEndian.PutUint64(sec[off:],
			binary.BigEndian.Uint64(sec[off:])+gap)
	}
	require.NoError(t, os.WriteFile(secPath, sec, 0o644))

	_, err = OpenStore(dir, geom)
	require.ErrorIs(t, err, ErrStoreCorrupt)
}
