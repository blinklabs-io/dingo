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

package utxocond

import (
	"strings"
	"testing"
)

func mkRefs(n int) []Ref {
	refs := make([]Ref, n)
	for i := range refs {
		refs[i] = Ref{TxID: []byte{byte(i), byte(i >> 8)}, Idx: uint32(i)}
	}
	return refs
}

// TestChunksBoundsDistinctShapes is the core property for issue #2943: across
// every input count in a wide range, only a small, bounded set of distinct SQL
// shapes (Condition strings) is produced, so the prepared-statement cache can
// reuse them. Without padding this set would grow with the input count.
func TestChunksBoundsDistinctShapes(t *testing.T) {
	shapes := make(map[string]struct{})
	for n := 1; n <= 1000; n++ {
		for _, c := range Chunks(mkRefs(n), DefaultMaxTerms) {
			shapes[c.Condition] = struct{}{}
		}
	}
	// Padded lengths are powers of two in [1, 256]: 1,2,4,8,16,32,64,128,256.
	if len(shapes) > 9 {
		t.Fatalf("expected at most 9 distinct shapes, got %d", len(shapes))
	}
	// Every shape's term count must be a power of two.
	for s := range shapes {
		terms := strings.Count(s, "output_idx")
		if terms&(terms-1) != 0 {
			t.Fatalf("shape has non-power-of-two term count %d: %q", terms, s)
		}
	}
}

// TestChunksPreservesRealRefsInOrder verifies that padding never drops or
// reorders real refs: the first Real*2 args of each chunk are exactly the input
// refs, and padding repeats the chunk's last ref.
func TestChunksPreservesRealRefsInOrder(t *testing.T) {
	refs := mkRefs(5) // pads to 8
	chunks := Chunks(refs, DefaultMaxTerms)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	c := chunks[0]
	if c.Real != 5 {
		t.Fatalf("expected Real=5, got %d", c.Real)
	}
	wantTerms := 8 // next power of two >= 5
	if got := strings.Count(c.Condition, "output_idx"); got != wantTerms {
		t.Fatalf("expected %d terms, got %d", wantTerms, got)
	}
	if len(c.Args) != wantTerms*2 {
		t.Fatalf("expected %d args, got %d", wantTerms*2, len(c.Args))
	}
	// First 5 arg-pairs are the real refs in order.
	for i := range refs {
		idx, ok := c.Args[i*2+1].(uint32)
		if !ok || idx != refs[i].Idx {
			t.Fatalf("arg %d: expected idx %d, got %v", i, refs[i].Idx, c.Args[i*2+1])
		}
	}
	// Padding (indices 5..7) repeats the last real ref (idx 4).
	for i := 5; i < wantTerms; i++ {
		if idx, ok := c.Args[i*2+1].(uint32); !ok || idx != refs[4].Idx {
			t.Fatalf("padding arg %d: expected repeated idx %d, got %v", i, refs[4].Idx, c.Args[i*2+1])
		}
	}
}

// TestChunksSplitsOverMaxTerms verifies large inputs are chunked at maxTerms and
// each chunk is independently padded to a power of two.
func TestChunksSplitsOverMaxTerms(t *testing.T) {
	chunks := Chunks(mkRefs(300), 256)
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks for 300 refs at max 256, got %d", len(chunks))
	}
	if chunks[0].Real != 256 || strings.Count(chunks[0].Condition, "output_idx") != 256 {
		t.Fatalf("chunk 0: expected 256 real/terms, got Real=%d terms=%d",
			chunks[0].Real, strings.Count(chunks[0].Condition, "output_idx"))
	}
	// Second chunk: 44 real refs, padded to 64.
	if chunks[1].Real != 44 || strings.Count(chunks[1].Condition, "output_idx") != 64 {
		t.Fatalf("chunk 1: expected Real=44 terms=64, got Real=%d terms=%d",
			chunks[1].Real, strings.Count(chunks[1].Condition, "output_idx"))
	}
	// Sum of Real across chunks equals input count.
	total := 0
	for _, c := range chunks {
		total += c.Real
	}
	if total != 300 {
		t.Fatalf("expected sum(Real)=300, got %d", total)
	}
}

// TestChunksNonPowerOfTwoMaxTerms pins the invariant documented on
// Chunk.Condition when maxTerms is not a power of two: the effective bound is
// maxTerms rounded down to a power of two, so every chunk's term count is a
// power of two AND stays within the caller's maxTerms (which bounds the driver
// bind-parameter count at two per term). Real, arg count and padding-by-repeat
// semantics are unchanged.
func TestChunksNonPowerOfTwoMaxTerms(t *testing.T) {
	for _, maxTerms := range []int{3, 5, 100, 200, 255, 257, 999} {
		for _, n := range []int{1, 2, 7, 63, 150, 300, 1001} {
			refs := mkRefs(n)
			chunks := Chunks(refs, maxTerms)
			if len(chunks) == 0 {
				t.Fatalf("maxTerms=%d n=%d: expected chunks", maxTerms, n)
			}
			total := 0
			for i, c := range chunks {
				terms := strings.Count(c.Condition, "output_idx")
				if terms&(terms-1) != 0 {
					t.Fatalf(
						"maxTerms=%d n=%d chunk %d: term count %d is not a power of two",
						maxTerms, n, i, terms,
					)
				}
				if terms > maxTerms {
					t.Fatalf(
						"maxTerms=%d n=%d chunk %d: term count %d exceeds maxTerms",
						maxTerms, n, i, terms,
					)
				}
				if c.Real < 1 || c.Real > terms {
					t.Fatalf(
						"maxTerms=%d n=%d chunk %d: Real=%d out of range for %d terms",
						maxTerms, n, i, c.Real, terms,
					)
				}
				if len(c.Args) != terms*2 {
					t.Fatalf(
						"maxTerms=%d n=%d chunk %d: expected %d args, got %d",
						maxTerms, n, i, terms*2, len(c.Args),
					)
				}
				// Real args are the input refs in order; padding repeats the
				// chunk's last real ref, which is what makes it idempotent.
				lastReal := refs[total+c.Real-1]
				for j := range terms {
					want := lastReal
					if j < c.Real {
						want = refs[total+j]
					}
					idx, ok := c.Args[j*2+1].(uint32)
					if !ok || idx != want.Idx {
						t.Fatalf(
							"maxTerms=%d n=%d chunk %d arg %d: expected idx %d, got %v",
							maxTerms, n, i, j, want.Idx, c.Args[j*2+1],
						)
					}
				}
				total += c.Real
			}
			if total != n {
				t.Fatalf(
					"maxTerms=%d n=%d: expected sum(Real)=%d, got %d",
					maxTerms, n, n, total,
				)
			}
		}
	}
}

// TestChunksNonPowerOfTwoMaxTermsBoundsShapes is the #2943 property for a
// non-power-of-two maxTerms: the distinct-shape set must still be bounded by
// the powers of two under the effective bound, not gain an extra ragged shape.
func TestChunksNonPowerOfTwoMaxTermsBoundsShapes(t *testing.T) {
	shapes := make(map[int]struct{})
	for n := 1; n <= 500; n++ {
		for _, c := range Chunks(mkRefs(n), 200) {
			shapes[strings.Count(c.Condition, "output_idx")] = struct{}{}
		}
	}
	// Effective bound is 128 (200 rounded down), so shapes are 1..128: 8 total.
	if len(shapes) > 8 {
		t.Fatalf("expected at most 8 distinct shapes, got %d: %v", len(shapes), shapes)
	}
	for terms := range shapes {
		if terms&(terms-1) != 0 || terms > 200 {
			t.Fatalf("invalid shape term count %d", terms)
		}
	}
}

// TestChunksMaxTermsBelowOne pins the documented fallback to DefaultMaxTerms.
func TestChunksMaxTermsBelowOne(t *testing.T) {
	for _, maxTerms := range []int{0, -1, -256} {
		got := Chunks(mkRefs(300), maxTerms)
		want := Chunks(mkRefs(300), DefaultMaxTerms)
		if len(got) != len(want) {
			t.Fatalf("maxTerms=%d: expected %d chunks, got %d",
				maxTerms, len(want), len(got))
		}
		for i := range got {
			if got[i].Condition != want[i].Condition ||
				got[i].Real != want[i].Real {
				t.Fatalf("maxTerms=%d chunk %d: expected Real=%d/%d terms, got Real=%d/%d terms",
					maxTerms, i, want[i].Real,
					strings.Count(want[i].Condition, "output_idx"),
					got[i].Real,
					strings.Count(got[i].Condition, "output_idx"))
			}
		}
	}
}

func TestChunksEmpty(t *testing.T) {
	if Chunks(nil, DefaultMaxTerms) != nil {
		t.Fatal("expected nil for empty input")
	}
	if Chunks([]Ref{}, DefaultMaxTerms) != nil {
		t.Fatal("expected nil for empty slice")
	}
}

func TestChunksSingle(t *testing.T) {
	chunks := Chunks(mkRefs(1), DefaultMaxTerms)
	if len(chunks) != 1 || chunks[0].Condition != term {
		t.Fatalf("expected single-term condition %q, got %+v", term, chunks)
	}
	if len(chunks[0].Args) != 2 || chunks[0].Real != 1 {
		t.Fatalf("expected 2 args and Real=1, got args=%d Real=%d",
			len(chunks[0].Args), chunks[0].Real)
	}
}
