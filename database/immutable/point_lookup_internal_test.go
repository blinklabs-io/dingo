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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func pointLookupPoint(slot uint64, value byte) ocommon.Point {
	return ocommon.NewPoint(slot, bytes.Repeat([]byte{value}, 32))
}

func writePointLookupChunk(
	t *testing.T,
	dir string,
	name string,
	points []ocommon.Point,
) {
	t.Helper()
	primaryOffsets := make([]uint32, len(points)+1)
	for idx := range primaryOffsets {
		primaryOffsets[idx] = uint32(idx * secondaryIndexEntrySize)
	}
	writeImmutableChunkTrio(
		t,
		dir,
		name,
		primaryIndexVersion,
		primaryOffsets,
		nil,
		points,
	)
}

func writePointLookupFixture(t *testing.T) (string, []ocommon.Point) {
	t.Helper()
	dir := t.TempDir()
	points := []ocommon.Point{
		pointLookupPoint(100, 0x10),
		pointLookupPoint(110, 0x11),
		pointLookupPoint(200, 0x20),
		pointLookupPoint(210, 0x21),
		pointLookupPoint(300, 0x30),
		pointLookupPoint(310, 0x31),
		pointLookupPoint(400, 0x40),
		pointLookupPoint(410, 0x41),
		pointLookupPoint(500, 0x50),
		pointLookupPoint(510, 0x51),
	}
	for idx := range 5 {
		writePointLookupChunk(
			t,
			dir,
			ChunkName(uint64(idx)),
			points[idx*2:idx*2+2],
		)
	}
	return dir, points
}

func TestGetChunkNamesFromPointReturnsContainingChunk(t *testing.T) {
	dir, points := writePointLookupFixture(t)
	imm, err := New(dir)
	if err != nil {
		t.Fatalf("open immutable DB: %s", err)
	}
	tests := []struct {
		name      string
		point     ocommon.Point
		wantChunk string
	}{
		{name: "first", point: points[0], wantChunk: "00000"},
		{name: "middle", point: points[5], wantChunk: "00002"},
		{name: "last", point: points[9], wantChunk: "00004"},
		{
			name:      "missing before first",
			point:     ocommon.NewPoint(99, nil),
			wantChunk: "00000",
		},
		{
			name:      "adjacent after chunk",
			point:     ocommon.NewPoint(211, nil),
			wantChunk: "00002",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := imm.getChunkNamesFromPoint(test.point)
			if err != nil {
				t.Fatalf("locate chunk: %s", err)
			}
			if len(got) == 0 || got[0] != test.wantChunk {
				t.Fatalf("first candidate = %v, want %s", got, test.wantChunk)
			}
		})
	}
	_, err = imm.getChunkNamesFromPoint(ocommon.NewPoint(511, nil))
	if !errors.Is(err, ErrPointBeyondLastChunk) {
		t.Fatalf("beyond-tip lookup error = %v, want ErrPointBeyondLastChunk", err)
	}
}

func TestGetBlockChecksHashAfterLocatingContainingChunk(t *testing.T) {
	dir, points := writePointLookupFixture(t)
	imm, err := New(dir)
	if err != nil {
		t.Fatalf("open immutable DB: %s", err)
	}
	want := points[4]
	got, err := imm.GetBlock(want)
	if err != nil {
		t.Fatalf("get exact point: %s", err)
	}
	if got == nil || got.Slot != want.Slot || !bytes.Equal(got.Hash, want.Hash) {
		t.Fatalf("exact point lookup = %#v, want slot %d hash %x", got, want.Slot, want.Hash)
	}
	wrongHash := pointLookupPoint(want.Slot, 0xFF)
	got, err = imm.GetBlock(wrongHash)
	if err != nil {
		t.Fatalf("get wrong-hash point: %s", err)
	}
	if got != nil {
		t.Fatalf("wrong-hash lookup returned %#v, want nil", got)
	}
}

func TestGetBlockFindsExactPointInSingleEntryChunk(t *testing.T) {
	dir := t.TempDir()
	want := pointLookupPoint(777, 0x77)
	writePointLookupChunk(t, dir, "00000", []ocommon.Point{want})
	imm, err := New(dir)
	if err != nil {
		t.Fatalf("open immutable DB: %s", err)
	}
	got, err := imm.GetBlock(want)
	if err != nil {
		t.Fatalf("get exact point: %s", err)
	}
	if got == nil || got.Slot != want.Slot || !bytes.Equal(got.Hash, want.Hash) {
		t.Fatalf("exact point lookup = %#v, want slot %d hash %x", got, want.Slot, want.Hash)
	}
}

func TestGetChunkNamesFromPointSkipsEmptyChunks(t *testing.T) {
	dir := t.TempDir()
	writePointLookupChunk(t, dir, "00000", nil)
	first := pointLookupPoint(200, 0x20)
	writePointLookupChunk(t, dir, "00001", []ocommon.Point{first})
	writePointLookupChunk(t, dir, "00002", nil)
	second := pointLookupPoint(400, 0x40)
	writePointLookupChunk(t, dir, "00003", []ocommon.Point{second})
	writePointLookupChunk(t, dir, "00004", nil)
	imm, err := New(dir)
	if err != nil {
		t.Fatalf("open immutable DB: %s", err)
	}
	tests := []struct {
		point     ocommon.Point
		wantChunk string
	}{
		{point: first, wantChunk: "00001"},
		{point: ocommon.NewPoint(300, nil), wantChunk: "00003"},
		{point: second, wantChunk: "00003"},
	}
	for _, test := range tests {
		got, err := imm.getChunkNamesFromPoint(test.point)
		if err != nil {
			t.Fatalf("locate slot %d: %s", test.point.Slot, err)
		}
		if len(got) == 0 || got[0] != test.wantChunk {
			t.Fatalf(
				"slot %d first candidate = %v, want %s",
				test.point.Slot,
				got,
				test.wantChunk,
			)
		}
	}
	tip, err := imm.GetTip()
	if err != nil {
		t.Fatalf("get tip through trailing empty chunk: %s", err)
	}
	if tip == nil || tip.Slot != second.Slot || !bytes.Equal(tip.Hash, second.Hash) {
		t.Fatalf("tip = %#v, want slot %d hash %x", tip, second.Slot, second.Hash)
	}
	_, err = imm.getChunkNamesFromPoint(ocommon.NewPoint(401, nil))
	if !errors.Is(err, ErrPointBeyondLastChunk) {
		t.Fatalf("trailing-empty lookup error = %v, want beyond-last error", err)
	}

	emptyDir := t.TempDir()
	writePointLookupChunk(t, emptyDir, "00000", nil)
	empty, err := New(emptyDir)
	if err != nil {
		t.Fatalf("open empty immutable DB: %s", err)
	}
	_, err = empty.getChunkNamesFromPoint(ocommon.NewPoint(1, nil))
	if !errors.Is(err, ErrPointBeyondLastChunk) {
		t.Fatalf("empty-chunk lookup error = %v, want beyond-last error", err)
	}
	tip, err = empty.GetTip()
	if err != nil {
		t.Fatalf("get tip from all-empty DB: %s", err)
	}
	if tip != nil {
		t.Fatalf("all-empty DB tip = %#v, want nil", tip)
	}
}

func requireChunkTrioState(
	t *testing.T,
	dir string,
	name string,
	wantPresent bool,
) {
	t.Helper()
	for _, suffix := range []string{
		chunkFileExtension,
		primaryFileExtension,
		secondaryFileExtension,
	} {
		_, err := os.Stat(filepath.Join(dir, name+suffix))
		if wantPresent && err != nil {
			t.Fatalf("expected %s%s to remain: %s", name, suffix, err)
		}
		if !wantPresent && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected %s%s to be removed, got %v", name, suffix, err)
		}
	}
}

func TestTruncateChunksFromPointKeepsChunksBeforeExactBoundary(t *testing.T) {
	dir, points := writePointLookupFixture(t)
	imm, err := New(dir)
	if err != nil {
		t.Fatalf("open immutable DB: %s", err)
	}
	if err := imm.TruncateChunksFromPoint(points[4]); err != nil {
		t.Fatalf("truncate from exact point: %s", err)
	}
	for idx := range 5 {
		requireChunkTrioState(t, dir, ChunkName(uint64(idx)), idx < 2)
	}
}

func TestTruncateChunksFromPointRefusesMissingExactPoint(t *testing.T) {
	dir, points := writePointLookupFixture(t)
	imm, err := New(dir)
	if err != nil {
		t.Fatalf("open immutable DB: %s", err)
	}
	missing := pointLookupPoint(points[4].Slot, 0xFF)
	err = imm.TruncateChunksFromPoint(missing)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("wrong-hash truncate error = %v, want point-not-found error", err)
	}
	for idx := range 5 {
		requireChunkTrioState(t, dir, ChunkName(uint64(idx)), true)
	}
}

func TestTruncateChunksFromPointReportsPartialDeletion(t *testing.T) {
	dir, points := writePointLookupFixture(t)
	failingPath := filepath.Join(dir, "00004"+secondaryFileExtension)
	if err := os.Remove(failingPath); err != nil {
		t.Fatalf("remove fixture secondary: %s", err)
	}
	if err := os.Mkdir(failingPath, 0o750); err != nil {
		t.Fatalf("replace secondary with directory: %s", err)
	}
	if err := os.WriteFile(
		filepath.Join(failingPath, "keep"),
		[]byte("x"),
		0o640,
	); err != nil {
		t.Fatalf("make secondary directory non-empty: %s", err)
	}

	imm, err := New(dir)
	if err != nil {
		t.Fatalf("open immutable DB: %s", err)
	}
	err = imm.TruncateChunksFromPoint(points[4])
	if err == nil || !strings.Contains(err.Error(), "truncation is partial") {
		t.Fatalf("truncate error = %v, want explicit partial-deletion report", err)
	}
	if !strings.Contains(err.Error(), "after removing 7 entries") {
		t.Fatalf("truncate error = %v, want removed-entry count", err)
	}
	for idx := range 2 {
		requireChunkTrioState(t, dir, ChunkName(uint64(idx)), true)
	}
	for idx := 2; idx < 4; idx++ {
		requireChunkTrioState(t, dir, ChunkName(uint64(idx)), false)
	}
	if _, err := os.Stat(filepath.Join(dir, "00004"+chunkFileExtension)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected 00004 chunk to be removed, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "00004"+primaryFileExtension)); err != nil {
		t.Fatalf("expected 00004 primary to remain: %s", err)
	}
}
