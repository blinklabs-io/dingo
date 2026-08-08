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
	"slices"
	"testing"
)

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
