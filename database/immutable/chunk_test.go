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
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

type benchmarkBlockSpan struct {
	offset int64
	size   int
}

var benchmarkReadSink byte

func benchmarkChunkSpans(b *testing.B) ([]benchmarkBlockSpan, string) {
	b.Helper()
	chunkPath := filepath.Join("testdata", "00000.chunk")
	secondaryPath := filepath.Join("testdata", "00000.secondary")
	chunkInfo, err := os.Stat(chunkPath)
	if err != nil {
		b.Fatal(err)
	}
	secondary, err := os.ReadFile(secondaryPath)
	if err != nil {
		b.Fatal(err)
	}
	if len(secondary)%secondaryIndexEntrySize != 0 {
		b.Fatalf(
			"secondary index size %d is not aligned to %d",
			len(secondary),
			secondaryIndexEntrySize,
		)
	}
	spans := make([]benchmarkBlockSpan, 0, len(secondary)/secondaryIndexEntrySize)
	for offset := 0; offset < len(secondary); offset += secondaryIndexEntrySize {
		start := binary.BigEndian.Uint64(secondary[offset:])
		end := uint64(chunkInfo.Size())
		if offset+secondaryIndexEntrySize < len(secondary) {
			end = binary.BigEndian.Uint64(
				secondary[offset+secondaryIndexEntrySize:],
			)
		}
		if end <= start || end > uint64(chunkInfo.Size()) {
			b.Fatalf("invalid block span [%d:%d]", start, end)
		}
		spans = append(spans, benchmarkBlockSpan{
			offset: int64(start),
			size:   int(end - start),
		})
	}
	return spans, chunkPath
}

// BenchmarkChunkBlockRead compares the old seek/read pair with the positional
// read used by chunk.Next. Both cases read the same immutable block spans and
// allocate the same destination buffers; the benchmark isolates the syscall
// change from CBOR decoding and Badger writes.
func BenchmarkChunkBlockRead(b *testing.B) {
	spans, chunkPath := benchmarkChunkSpans(b)
	for _, method := range []string{"seek-read", "read-at"} {
		b.Run(method, func(b *testing.B) {
			f, err := os.Open(chunkPath)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = f.Close() })
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for _, span := range spans {
					buf := make([]byte, span.size)
					var n int
					if method == "seek-read" {
						if _, err := f.Seek(span.offset, io.SeekStart); err != nil {
							b.Fatal(err)
						}
						n, err = f.Read(buf)
					} else {
						n, err = f.ReadAt(buf, span.offset)
					}
					if err != nil {
						b.Fatal(err)
					}
					if n != len(buf) {
						b.Fatalf("read %d bytes, want %d", n, len(buf))
					}
					benchmarkReadSink ^= buf[0]
				}
			}
		})
	}
}

// TestChunkNamesSortNumericallyPastFiveDigits covers the ordering every lookup
// here rests on.
//
// Chunk names are padded to five digits, so they stop being a fixed width at
// 100000. The tip is the last entry of this listing and the point search
// bisects it, so a lexical sort past that width — where "100000" sorts before
// "99999" — reports the wrong tip and bisects a list that is not ordered.
//
// Internal because the listing is what is under test, and asserting it through
// a tip read would need a hundred thousand real chunk files to reach the width
// where the two orderings differ.
func TestChunkNamesSortNumericallyPastFiveDigits(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"99999", "100000", "100001"} {
		if err := os.WriteFile(
			filepath.Join(dir, name+chunkFileExtension), []byte("x"), 0o640,
		); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}
	imm, err := New(dir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	got, err := imm.getChunkNames()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	want := []string{"99999", "100000", "100001"}
	if !slices.Equal(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

// TestChunkNamesDropsNonCanonicalNames keeps the numeric ordering's premise
// true rather than assumed.
//
// Ordering by width and then by text is only numeric ordering while every name
// is ChunkName's own output. A differently padded name breaks it — "0000001"
// is seven characters, so it sorts above every six-digit chunk and becomes the
// tip — and the tip is what bounds the copy and what the catch-up compares
// against.
//
// Dropped rather than refused, unlike the slot entries in a ledger tree. There
// the choice is between candidates, so ignoring one selects another; here a
// name that is not a chunk name names no chunk, and the verified reader
// refuses anything absent from the digest map in any case. Including it is the
// only option that lets a planted file decide the tip.
func TestChunkNamesDropsNonCanonicalNames(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"00000", "00001",
		"0000001", // canonical for no number: wider than ChunkName pads to
		"1e5",     // not a number at all
		"00002x",  // trailing junk
		"-00003",  // negative
	} {
		if err := os.WriteFile(
			filepath.Join(dir, name+chunkFileExtension), []byte("x"), 0o640,
		); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}
	imm, err := New(dir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	got, err := imm.getChunkNames()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	want := []string{"00000", "00001"}
	if !slices.Equal(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestChunkNextReadsDistinctCBORAtNonzeroOffset(t *testing.T) {
	t.Parallel()

	payloads := [][]byte{{0x80}, {0x81, 0x01}}
	var chunkData []byte
	blockOffsets := make([]uint64, len(payloads))
	for i, payload := range payloads {
		blockOffsets[i] = uint64(len(chunkData))
		wrapped, err := cbor.Encode([]any{uint64(1), cbor.RawMessage(payload)})
		require.NoError(t, err)
		chunkData = append(chunkData, wrapped...)
	}
	primaryData := make([]byte, 1+4*len(payloads))
	primaryData[0] = primaryIndexVersion
	for i := range payloads {
		binary.BigEndian.PutUint32(
			primaryData[1+i*4:],
			uint32(i*secondaryIndexEntrySize),
		)
	}
	secondaryData := make([]byte, secondaryIndexEntrySize*len(payloads))
	for i, offset := range blockOffsets {
		base := i * secondaryIndexEntrySize
		binary.BigEndian.PutUint64(secondaryData[base:], offset)
		copy(secondaryData[base+16:base+48], bytes.Repeat([]byte{byte(i + 1)}, 32))
		binary.BigEndian.PutUint64(secondaryData[base+48:], uint64(100+i))
	}

	tests := []struct {
		name   string
		reader func(t *testing.T) entryReader
	}{
		{name: "fileEntry", reader: func(t *testing.T) entryReader {
			path := filepath.Join(t.TempDir(), "chunk")
			require.NoError(t, os.WriteFile(path, chunkData, 0o600))
			file, err := os.Open(path)
			require.NoError(t, err)
			return fileEntry{file: file}
		}},
		{name: "bytesEntry", reader: func(t *testing.T) entryReader {
			return newBytesEntry(chunkData)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			primary := newPrimaryIndex()
			require.NoError(t, primary.Open(newBytesEntry(primaryData)))
			secondary := newSecondaryIndex()
			require.NoError(t, secondary.Open(newBytesEntry(secondaryData), primary))
			chunk := newChunk()
			require.NoError(t, chunk.Open(test.reader(t), secondary))
			t.Cleanup(func() { require.NoError(t, chunk.Close()) })

			for i, want := range payloads {
				block, err := chunk.Next()
				require.NoError(t, err)
				require.Equal(t, uint64(100+i), block.Slot)
				require.Equal(t, bytes.Repeat([]byte{byte(i + 1)}, 32), block.Hash)
				require.Equal(t, want, block.Cbor)
			}
			block, err := chunk.Next()
			require.NoError(t, err)
			require.Nil(t, block)
		})
	}
}
