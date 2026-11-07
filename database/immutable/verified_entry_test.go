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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// largeEntrySize is above the old 16 MiB single-pass bound and inside the
// range of real mainnet chunk files.
const largeEntrySize = 48 << 20

// countingSource counts the bytes readVerifiedEntry pulls from the file, and
// can misreport the file's size the way a file being written, or a planted
// one, does. The file is a field rather than embedded so its WriteTo is not
// promoted: io.Copy would take it and read past the counter.
type countingSource struct {
	file *os.File
	read int64
	// statSize, when non-negative, replaces the size Stat reports.
	statSize int64
}

func (c *countingSource) Read(p []byte) (int, error) {
	n, err := c.file.Read(p)
	c.read += int64(n)
	return n, err
}

func (c *countingSource) Seek(offset int64, whence int) (int64, error) {
	return c.file.Seek(offset, whence)
}

type sizedInfo struct {
	fs.FileInfo
	size int64
}

func (s sizedInfo) Size() int64 { return s.size }

func (c *countingSource) Stat() (fs.FileInfo, error) {
	info, err := c.file.Stat()
	if err != nil || c.statSize < 0 {
		return info, err
	}
	return sizedInfo{FileInfo: info, size: c.statSize}, nil
}

// writeVerifiedEntry writes size bytes of non-repeating content to a file in
// a fresh directory and returns its path, its bytes, and a database whose
// digest map certifies exactly those bytes.
func writeVerifiedEntry(
	tb testing.TB,
	size int,
) (string, []byte, *ImmutableDb) {
	tb.Helper()
	data := make([]byte, size)
	var counter [8]byte
	for off := 0; off < size; off += sha256.Size {
		for j := range counter {
			counter[j] = byte(off >> (8 * j))
		}
		block := sha256.Sum256(counter[:])
		copy(data[off:], block[:])
	}
	path := filepath.Join(tb.TempDir(), "00000.chunk")
	require.NoError(tb, os.WriteFile(path, data, 0o640))
	sum := sha256.Sum256(data)
	db := &ImmutableDb{
		dataDir: filepath.Dir(path),
		digests: map[string]string{
			"00000.chunk": hex.EncodeToString(sum[:]),
		},
	}
	return path, data, db
}

func openCounting(tb testing.TB, path string, statSize int64) *countingSource {
	tb.Helper()
	f, err := os.Open(path)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = f.Close() })
	return &countingSource{file: f, statSize: statSize}
}

// TestReadVerifiedEntryReadsALargeEntryOnce is the cost the single-pass bound
// exists to remove: a certified chunk the size of a real mainnet one is read
// from the file once, not streamed for its digest and then read again.
func TestReadVerifiedEntryReadsALargeEntryOnce(t *testing.T) {
	t.Parallel()
	path, want, db := writeVerifiedEntry(t, largeEntrySize)
	src := openCounting(t, path, -1)

	entry, err := db.readVerifiedEntry(src, "00000.chunk")
	require.NoError(t, err)
	got, ok := entry.(*bytesEntry)
	require.True(t, ok, "a verified read must serve the buffer it hashed")
	require.True(t, bytes.Equal(want, got.data))
	require.Equal(
		t, int64(largeEntrySize), src.read,
		"a %d byte certified entry must be read from the file exactly once",
		largeEntrySize,
	)
}

// TestReadVerifiedEntryRefusesALargeEntryThatDiffers covers the single-pass
// path's refusals at a size that used to take the streaming path: changed
// content and a shortened file are both mismatches.
func TestReadVerifiedEntryRefusesALargeEntryThatDiffers(t *testing.T) {
	t.Parallel()
	t.Run("changed", func(t *testing.T) {
		t.Parallel()
		path, data, db := writeVerifiedEntry(t, largeEntrySize)
		data[len(data)/2] ^= 0xFF
		require.NoError(t, os.WriteFile(path, data, 0o640))
		_, err := db.readVerifiedEntry(openCounting(t, path, -1), "00000.chunk")
		require.ErrorIs(t, err, ErrDigestMismatch)
	})
	t.Run("truncated", func(t *testing.T) {
		t.Parallel()
		path, _, db := writeVerifiedEntry(t, largeEntrySize)
		require.NoError(t, os.Truncate(path, largeEntrySize-1))
		_, err := db.readVerifiedEntry(openCounting(t, path, -1), "00000.chunk")
		require.ErrorIs(t, err, ErrDigestMismatch)
	})
	t.Run("uncertified", func(t *testing.T) {
		t.Parallel()
		path, _, db := writeVerifiedEntry(t, largeEntrySize)
		_, err := db.readVerifiedEntry(openCounting(t, path, -1), "00001.chunk")
		require.ErrorIs(t, err, ErrDigestMismatch)
	})
}

// TestReadVerifiedEntryFallsBackWhenStatIsWrong pins what happens when the
// size the file reports is not the size it holds. A smaller report must not
// cut the entry to a prefix, and a report above the bound must not size an
// allocation; both go to the streaming pass, which still returns the certified
// bytes.
func TestReadVerifiedEntryFallsBackWhenStatIsWrong(t *testing.T) {
	t.Parallel()
	for name, statSize := range map[string]int64{
		"under-reported": largeEntrySize / 2,
		"above bound":    maxSinglePassVerifiedEntrySize + 1,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path, want, db := writeVerifiedEntry(t, largeEntrySize)
			entry, err := db.readVerifiedEntry(
				openCounting(t, path, statSize), "00000.chunk",
			)
			require.NoError(t, err)
			got, ok := entry.(*bytesEntry)
			require.True(t, ok)
			require.True(t, bytes.Equal(want, got.data))
		})
	}
}

// TestReadVerifiedEntryAllocationIsBoundedForAPlantedFile states the price of
// reading before hashing: a planted file that reports a size inside the bound
// costs one buffer of that size and a refusal, and nothing it reports can make
// the buffer larger than the bound.
//
// Not t.Parallel: it measures process-wide allocation.
func TestReadVerifiedEntryAllocationIsBoundedForAPlantedFile(t *testing.T) {
	_, _, db := writeVerifiedEntry(t, sha256.Size)
	path := filepath.Join(t.TempDir(), "00000.chunk")
	planted, err := os.Create(path)
	require.NoError(t, err)
	if err := planted.Truncate(maxSinglePassVerifiedEntrySize); err != nil {
		_ = planted.Close()
		t.Skipf("filesystem will not hold a sparse fixture: %s", err)
	}
	require.NoError(t, planted.Close())

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = db.readVerifiedEntry(openCounting(t, path, -1), "00000.chunk")
	runtime.ReadMemStats(&after)

	require.ErrorIs(t, err, ErrDigestMismatch)
	const slack = 4 << 20
	grew := after.TotalAlloc - before.TotalAlloc
	require.LessOrEqual(
		t, grew, uint64(maxSinglePassVerifiedEntrySize+slack),
		"refusing a planted entry allocated %d bytes", grew,
	)
}

// BenchmarkReadVerifiedEntry reports file bytes read per entry byte: 1 for a
// single pass, 2 for a streaming pass followed by a buffered one.
func BenchmarkReadVerifiedEntry(b *testing.B) {
	for _, size := range []int{8 << 20, 48 << 20, 90 << 20} {
		b.Run(fmt.Sprintf("%dMiB", size>>20), func(b *testing.B) {
			path, _, db := writeVerifiedEntry(b, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			var read int64
			b.ResetTimer()
			for b.Loop() {
				src := openCounting(b, path, -1)
				if _, err := db.readVerifiedEntry(
					src, "00000.chunk",
				); err != nil {
					b.Fatal(err)
				}
				read += src.read
				_ = src.file.Close()
			}
			b.ReportMetric(
				float64(read)/float64(int64(b.N)*int64(size)), "reads/byte",
			)
		})
	}
}
