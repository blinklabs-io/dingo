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

package database

import (
	"encoding/binary"
	"testing"
)

// benchBlockKeyHash builds a 32-byte block hash whose leading bytes encode the
// given index. The leading bytes drive shard selection, so encoding the index
// here spreads the working set uniformly across shards once the cache is
// sharded.
func benchBlockKeyHash(idx int) [32]byte {
	var h [32]byte
	binary.LittleEndian.PutUint64(h[:8], uint64(idx))
	return h
}

// benchmarkBlockLRUParallel exercises the cache from many goroutines over a
// working set that fits within capacity (so reads hit). It mirrors the
// methodology of https://strebkov.dev/posts/shard-your-locks/ : a fixed key
// space hammered concurrently, with a tunable read/write mix. Note that the
// hot path Get also mutates the LRU ordering (MoveToFront), so even reads
// contend on the lock — the case where a single mutex scales backwards.
//
// Run with -cpu=1,4,8 to see the scaling curve.
func benchmarkBlockLRUParallel(b *testing.B, workingSet int, writeEvery int) {
	cache := NewBlockLRUCache(workingSet)

	// Pre-populate the full working set so reads are hits.
	for i := range workingSet {
		cache.Put(uint64(i), benchBlockKeyHash(i), &CachedBlock{
			RawBytes: make([]byte, 256),
		})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Per-goroutine counter avoids shared RNG state; the stride keeps
		// successive ops landing on different keys/shards.
		i := 0
		for pb.Next() {
			idx := (i * 2654435761) % workingSet
			if idx < 0 {
				idx += workingSet
			}
			slot := uint64(idx)
			hash := benchBlockKeyHash(idx)
			if writeEvery > 0 && i%writeEvery == 0 {
				cache.Put(slot, hash, &CachedBlock{RawBytes: make([]byte, 256)})
			} else {
				cache.Get(slot, hash)
			}
			i++
		}
	})
}

// BenchmarkBlockLRUParallelReadHeavy is ~90% Get / ~10% Put.
func BenchmarkBlockLRUParallelReadHeavy(b *testing.B) {
	benchmarkBlockLRUParallel(b, 500, 10)
}

// BenchmarkBlockLRUParallelBalanced is ~50% Get / ~50% Put.
func BenchmarkBlockLRUParallelBalanced(b *testing.B) {
	benchmarkBlockLRUParallel(b, 500, 2)
}

// BenchmarkBlockLRUParallelReadOnly is pure Get (still mutates LRU order).
func BenchmarkBlockLRUParallelReadOnly(b *testing.B) {
	benchmarkBlockLRUParallel(b, 500, 0)
}
