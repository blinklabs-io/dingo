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
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type immutableIndexFixture struct {
	dir    string
	points []ocommon.Point
}

func writeImmutableIndexFixture(
	t *testing.T,
	version byte,
	primaryOffsets []uint32,
	blockOffsets []uint64,
) immutableIndexFixture {
	t.Helper()
	if len(blockOffsets) == 0 {
		t.Fatal("fixture requires at least one block")
	}
	dir := t.TempDir()
	points := make([]ocommon.Point, len(blockOffsets))
	for idx := range blockOffsets {
		hash := make([]byte, 32)
		for hashIdx := range hash {
			hash[hashIdx] = byte(idx + 1)
		}
		points[idx] = ocommon.NewPoint(uint64(100+idx), hash)
	}
	writeImmutableChunkTrio(
		t,
		dir,
		"00000",
		version,
		primaryOffsets,
		blockOffsets,
		points,
	)
	return immutableIndexFixture{dir: dir, points: points}
}

func writeImmutableChunkTrio(
	t *testing.T,
	dir string,
	name string,
	version byte,
	primaryOffsets []uint32,
	blockOffsets []uint64,
	points []ocommon.Point,
) {
	t.Helper()
	if blockOffsets != nil && len(blockOffsets) != len(points) {
		t.Fatalf(
			"fixture has %d block offsets for %d points",
			len(blockOffsets),
			len(points),
		)
	}
	primary := make([]byte, 1+4*len(primaryOffsets))
	primary[0] = version
	for i, offset := range primaryOffsets {
		binary.BigEndian.PutUint32(primary[1+i*4:], offset)
	}
	secondary := make([]byte, secondaryIndexEntrySize*len(points))
	var chunk []byte
	for idx, point := range points {
		if len(point.Hash) != 32 {
			t.Fatalf(
				"fixture point hash has length %d, want 32",
				len(point.Hash),
			)
		}
		block, err := cbor.Encode([]any{
			uint64(1),
			cbor.RawMessage{0x80},
		})
		if err != nil {
			t.Fatalf("encode fixture block: %s", err)
		}
		blockOffset := uint64(len(chunk))
		if blockOffsets != nil {
			blockOffset = blockOffsets[idx]
		}

		base := idx * secondaryIndexEntrySize
		binary.BigEndian.PutUint64(secondary[base:], blockOffset)
		copy(secondary[base+16:base+48], point.Hash)
		binary.BigEndian.PutUint64(secondary[base+48:], point.Slot)
		chunk = append(chunk, block...)
	}
	for suffix, data := range map[string][]byte{
		chunkFileExtension:     chunk,
		primaryFileExtension:   primary,
		secondaryFileExtension: secondary,
	} {
		if err := os.WriteFile(
			filepath.Join(dir, name+suffix),
			data,
			0o640,
		); err != nil {
			t.Fatalf("write %s%s: %s", name, suffix, err)
		}
	}
}

func getBlockRecoveringPanic(
	imm *ImmutableDb,
	point ocommon.Point,
) (block *Block, err error, panicValue any) {
	defer func() {
		panicValue = recover()
	}()
	block, err = imm.GetBlock(point)
	return
}

func requireGetBlockError(
	t *testing.T,
	fixture immutableIndexFixture,
	want string,
) {
	t.Helper()
	imm, err := New(fixture.dir)
	if err != nil {
		t.Fatalf("open immutable DB: %s", err)
	}
	_, err, panicValue := getBlockRecoveringPanic(imm, ocommon.Point{})
	if panicValue != nil {
		t.Fatalf(
			"GetBlock panicked instead of returning an error: %v",
			panicValue,
		)
	}
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("GetBlock error = %v, want an error containing %q", err, want)
	}
}

// requireGetBlockErrorIs is requireGetBlockError plus an errors.Is check
// against target. Use it for failures chunk.Next wraps in a sentinel, so a
// dropped %w is caught even though the message would still match wantSubstr.
func requireGetBlockErrorIs(
	t *testing.T,
	fixture immutableIndexFixture,
	target error,
	wantSubstr string,
) {
	t.Helper()
	imm, err := New(fixture.dir)
	if err != nil {
		t.Fatalf("open immutable DB: %s", err)
	}
	_, err, panicValue := getBlockRecoveringPanic(imm, ocommon.Point{})
	if panicValue != nil {
		t.Fatalf(
			"GetBlock panicked instead of returning an error: %v",
			panicValue,
		)
	}
	if !errors.Is(err, target) {
		t.Fatalf(
			"GetBlock error = %v, want errors.Is match for %v",
			err,
			target,
		)
	}
	if err == nil || !strings.Contains(err.Error(), wantSubstr) {
		t.Fatalf(
			"GetBlock error = %v, want an error containing %q",
			err,
			wantSubstr,
		)
	}
}

func TestImmutableIndexAcceptsValidSingleAndMultipleBlockChunks(t *testing.T) {
	tests := []struct {
		name           string
		primaryOffsets []uint32
		blockOffsets   []uint64
	}{
		{
			name:           "single block",
			primaryOffsets: []uint32{0, secondaryIndexEntrySize},
			blockOffsets:   []uint64{0},
		},
		{
			name: "multiple blocks",
			primaryOffsets: []uint32{
				0,
				secondaryIndexEntrySize,
				2 * secondaryIndexEntrySize,
			},
			blockOffsets: []uint64{0, 3},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := writeImmutableIndexFixture(
				t, 1, test.primaryOffsets, test.blockOffsets,
			)
			imm, err := New(fixture.dir)
			if err != nil {
				t.Fatalf("open immutable DB: %s", err)
			}
			iter, err := imm.BlocksFromPoint(ocommon.Point{})
			if err != nil {
				t.Fatalf("BlocksFromPoint returned an error: %s", err)
			}
			defer func() { _ = iter.Close() }()
			block, err := iter.Next()
			if err != nil {
				t.Fatalf("iterator returned an error: %s", err)
			}
			if block == nil {
				t.Fatal("iterator returned no block")
			}
			if block.Slot != fixture.points[0].Slot {
				t.Fatalf(
					"block slot = %d, want %d",
					block.Slot,
					fixture.points[0].Slot,
				)
			}
		})
	}
}

func TestImmutableIndexRejectsUnsupportedPrimaryVersion(t *testing.T) {
	fixture := writeImmutableIndexFixture(
		t, 2, []uint32{0, secondaryIndexEntrySize}, []uint64{0},
	)
	requireGetBlockError(t, fixture, "unsupported primary index version")
}

func TestImmutableIndexRejectsMisalignedSecondaryOffset(t *testing.T) {
	fixture := writeImmutableIndexFixture(
		t, 1, []uint32{1, secondaryIndexEntrySize}, []uint64{0},
	)
	requireGetBlockError(t, fixture, "not aligned")
}

func TestImmutableIndexRejectsNonMonotonicSecondaryOffsets(t *testing.T) {
	fixture := writeImmutableIndexFixture(
		t,
		1,
		[]uint32{0, secondaryIndexEntrySize, 0, 2 * secondaryIndexEntrySize},
		[]uint64{0, 3},
	)
	requireGetBlockError(t, fixture, "non-monotonic")
}

func TestImmutableIndexRejectsLastBlockOffsetBeyondChunk(t *testing.T) {
	fixture := writeImmutableIndexFixture(
		t, 1, []uint32{0, secondaryIndexEntrySize}, []uint64{1000},
	)
	requireGetBlockErrorIs(
		t, fixture, ErrInvalidChunkOffset, "beyond chunk size",
	)
}

func TestImmutableIndexRejectsDescendingBlockOffsets(t *testing.T) {
	fixture := writeImmutableIndexFixture(
		t,
		1,
		[]uint32{0, secondaryIndexEntrySize, 2 * secondaryIndexEntrySize},
		[]uint64{3, 0},
	)
	requireGetBlockErrorIs(
		t,
		fixture,
		ErrInvalidChunkOffset,
		"does not follow current block offset",
	)
}

func TestImmutableIndexRejectsBlockOffsetOverflow(t *testing.T) {
	tests := []struct {
		name         string
		blockOffsets []uint64
	}{
		{
			// Exercises chunk.Next's last-entry branch, which sizes the
			// block from the current offset and the file size.
			name:         "last entry",
			blockOffsets: []uint64{math.MaxUint64},
		},
		{
			// Exercises chunk.Next's two-entry branch, which sizes the
			// block from the current and next offsets.
			name:         "next entry",
			blockOffsets: []uint64{0, math.MaxUint64},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			primaryOffsets := make([]uint32, len(test.blockOffsets)+1)
			for i := range primaryOffsets {
				primaryOffsets[i] = uint32(i * secondaryIndexEntrySize)
			}
			fixture := writeImmutableIndexFixture(
				t, 1, primaryOffsets, test.blockOffsets,
			)
			requireGetBlockErrorIs(
				t, fixture, ErrInvalidChunkOffset, "overflows int64",
			)
		})
	}
}

func TestImmutableIndexRejectsMisalignedSecondaryFile(t *testing.T) {
	fixture := writeImmutableIndexFixture(
		t, 1, []uint32{0, secondaryIndexEntrySize}, []uint64{0},
	)
	secondaryPath := filepath.Join(fixture.dir, "00000.secondary")
	f, err := os.OpenFile(secondaryPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open secondary index: %s", err)
	}
	if _, err := f.Write([]byte{0}); err != nil {
		_ = f.Close()
		t.Fatalf("extend secondary index: %s", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close secondary index: %s", err)
	}
	requireGetBlockError(
		t,
		fixture,
		fmt.Sprintf("not aligned to %d-byte records", secondaryIndexEntrySize),
	)
}

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
		t.Fatalf(
			"beyond-tip lookup error = %v, want ErrPointBeyondLastChunk",
			err,
		)
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
	if got == nil || got.Slot != want.Slot ||
		!bytes.Equal(got.Hash, want.Hash) {
		t.Fatalf(
			"exact point lookup = %#v, want slot %d hash %x",
			got,
			want.Slot,
			want.Hash,
		)
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
	if got == nil || got.Slot != want.Slot ||
		!bytes.Equal(got.Hash, want.Hash) {
		t.Fatalf(
			"exact point lookup = %#v, want slot %d hash %x",
			got,
			want.Slot,
			want.Hash,
		)
	}
}

func TestSingleEntryChunkLookupsAtZeroAndNonzeroSlot(t *testing.T) {
	t.Parallel()
	for _, slot := range []uint64{0, 777} {
		t.Run(fmt.Sprintf("slot %d", slot), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			want := pointLookupPoint(slot, 0x77)
			writePointLookupChunk(t, dir, "00000", []ocommon.Point{want})
			imm, err := New(dir)
			if err != nil {
				t.Fatalf("open immutable DB: %s", err)
			}
			start, end, found, err := imm.chunkSlotRange("00000")
			if err != nil || !found || start != slot || end != slot {
				t.Fatalf(
					"chunkSlotRange = (%d, %d, %v, %v), want (%d, %d, true, nil)",
					start, end, found, err, slot, slot,
				)
			}
			got, err := imm.GetBlock(want)
			if err != nil {
				t.Fatalf("exact lookup: %s", err)
			}
			if got == nil || got.Slot != slot ||
				!bytes.Equal(got.Hash, want.Hash) {
				t.Fatalf("exact lookup = %#v, want slot %d", got, slot)
			}
			// Same slot, different hash: located but not a match.
			got, err = imm.GetBlock(pointLookupPoint(slot, 0xFF))
			if err != nil || got != nil {
				t.Fatalf("wrong-hash lookup = (%#v, %v), want (nil, nil)", got, err)
			}
			// The slot just after the only block is past the last chunk.
			_, err = imm.getChunkNamesFromPoint(
				ocommon.NewPoint(slot+1, nil),
			)
			if !errors.Is(err, ErrPointBeyondLastChunk) {
				t.Fatalf("adjacent-after error = %v, want ErrPointBeyondLastChunk", err)
			}
			if slot > 0 {
				// The slot just before is not in the chunk, but the chunk is
				// still the first candidate.
				got, err = imm.GetBlock(pointLookupPoint(slot-1, 0x77))
				if err != nil || got != nil {
					t.Fatalf("adjacent-before lookup = (%#v, %v), want (nil, nil)", got, err)
				}
			}
		})
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
	if tip == nil || tip.Slot != second.Slot ||
		!bytes.Equal(tip.Hash, second.Hash) {
		t.Fatalf(
			"tip = %#v, want slot %d hash %x",
			tip,
			second.Slot,
			second.Hash,
		)
	}
	_, err = imm.getChunkNamesFromPoint(ocommon.NewPoint(401, nil))
	if !errors.Is(err, ErrPointBeyondLastChunk) {
		t.Fatalf(
			"trailing-empty lookup error = %v, want beyond-last error",
			err,
		)
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
		t.Fatalf(
			"wrong-hash truncate error = %v, want point-not-found error",
			err,
		)
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
		t.Fatalf(
			"truncate error = %v, want explicit partial-deletion report",
			err,
		)
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
	if _, err := os.Stat(filepath.Join(dir, "00004"+chunkFileExtension)); !errors.Is(
		err,
		os.ErrNotExist,
	) {
		t.Fatalf("expected 00004 chunk to be removed, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "00004"+primaryFileExtension)); err != nil {
		t.Fatalf("expected 00004 primary to remain: %s", err)
	}
}
