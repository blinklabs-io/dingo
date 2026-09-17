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
	"io"
	"os"
	"path/filepath"
	"testing"
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
