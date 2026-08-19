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

package koiosparity

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestChunkAddressesByCountAndSizeEmptyInput proves an empty or nil address
// list produces no chunks at all, rather than one spurious empty chunk.
func TestChunkAddressesByCountAndSizeEmptyInput(t *testing.T) {
	require.Nil(t, chunkAddressesByCountAndSize(nil, 100, 1000))
	require.Nil(t, chunkAddressesByCountAndSize([]string{}, 100, 1000))
}

// TestChunkAddressesByCountAndSizeCountBoundary proves the address-count
// bound splits evenly-sized groups with a smaller leftover final chunk,
// with no byte bound in play.
func TestChunkAddressesByCountAndSizeCountBoundary(t *testing.T) {
	addrs := make([]string, 25)
	for i := range addrs {
		addrs[i] = strings.Repeat("a", 5)
	}
	chunks := chunkAddressesByCountAndSize(addrs, 10, 0)
	require.Len(t, chunks, 3)
	require.Len(t, chunks[0], 10)
	require.Len(t, chunks[1], 10)
	require.Len(t, chunks[2], 5, "leftover final chunk")

	var total int
	for _, c := range chunks {
		total += len(c)
	}
	require.Equal(t, len(addrs), total)
}

// TestChunkAddressesByCountAndSizeByteBoundary proves the encoded-body-size
// bound caps addresses per chunk even when the count bound is effectively
// unlimited, matching dingo #3099's "shape requests by encoded size too"
// requirement.
func TestChunkAddressesByCountAndSizeByteBoundary(t *testing.T) {
	// Each address encodes as `"aaaaaaaaaa"` — 10 chars + 3 overhead bytes =
	// 13 bytes. maxBytes=40 fits 3 addresses per chunk (39 bytes), not 4
	// (52 bytes) — count (1_000_000) is never the limiting factor here.
	addr := strings.Repeat("a", 10)
	addrs := []string{addr, addr, addr, addr, addr, addr, addr}
	chunks := chunkAddressesByCountAndSize(addrs, 1_000_000, 40)
	require.NotEmpty(t, chunks)
	for _, c := range chunks[:len(chunks)-1] {
		require.LessOrEqual(t, len(c), 3)
	}
	var total int
	for _, c := range chunks {
		total += len(c)
	}
	require.Equal(t, len(addrs), total)
}

// TestChunkAddressesByCountAndSizeSingleOversizedAddressNotDropped proves a
// single address whose own encoded size already exceeds maxBytes still gets
// its own one-address chunk instead of being silently discarded.
func TestChunkAddressesByCountAndSizeSingleOversizedAddressNotDropped(
	t *testing.T,
) {
	huge := strings.Repeat("x", 500)
	addrs := []string{huge, "short1", "short2"}
	chunks := chunkAddressesByCountAndSize(addrs, 100, 40)
	require.NotEmpty(t, chunks)
	require.Equal(
		t,
		[]string{huge},
		chunks[0],
		"an address alone exceeding maxBytes still gets its own chunk, never dropped",
	)
	var total int
	for _, c := range chunks {
		total += len(c)
	}
	require.Equal(t, len(addrs), total)
}

// TestChunkAddressesByCountAndSizeZeroMaxCountUsesDefault proves maxCount<=0
// falls back to koiosAccountChunkSize rather than producing one giant chunk
// or erroring.
func TestChunkAddressesByCountAndSizeZeroMaxCountUsesDefault(t *testing.T) {
	addrs := make([]string, koiosAccountChunkSize+1)
	for i := range addrs {
		addrs[i] = "addr"
	}
	chunks := chunkAddressesByCountAndSize(addrs, 0, 0)
	require.Len(t, chunks, 2)
	require.Len(t, chunks[0], koiosAccountChunkSize)
	require.Len(t, chunks[1], 1)
}

// TestChunkAddressesByCountAndSizeDeterministicAcrossRepeatedCalls proves the
// same input plus the same bounds always produce byte-for-byte identical
// chunk boundaries — the property dingo #3099's content-addressed
// chunk-hash resume mechanism depends on.
func TestChunkAddressesByCountAndSizeDeterministicAcrossRepeatedCalls(
	t *testing.T,
) {
	addrs := make([]string, 237)
	for i := range addrs {
		addrs[i] = strings.Repeat("z", (i%7)+1)
	}
	first := chunkAddressesByCountAndSize(addrs, 20, 200)
	second := chunkAddressesByCountAndSize(addrs, 20, 200)
	require.Equal(
		t,
		first,
		second,
		"same input + same bounds must always produce identical chunk boundaries (content-addressed resume depends on this)",
	)
}

// TestChunkAddressesByCountAndSizeNoByteBoundIsCountOnly proves maxBytes<=0
// disables the byte bound entirely (count alone governs), even for
// addresses large enough that a byte bound would otherwise split them
// further.
func TestChunkAddressesByCountAndSizeNoByteBoundIsCountOnly(t *testing.T) {
	addrs := make([]string, 15)
	for i := range addrs {
		addrs[i] = strings.Repeat("a", 1000)
	}
	chunks := chunkAddressesByCountAndSize(addrs, 5, 0)
	require.Len(t, chunks, 3)
	for _, c := range chunks {
		require.LessOrEqual(t, len(c), 5)
	}
}
