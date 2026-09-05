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

package immutable_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"github.com/blinklabs-io/dingo/database/immutable"
)

const secondaryIndexEntrySize = 56

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
	primary := make([]byte, 1+4*len(primaryOffsets))
	primary[0] = version
	for i, offset := range primaryOffsets {
		binary.BigEndian.PutUint32(primary[1+i*4:], offset)
	}
	secondary := make([]byte, secondaryIndexEntrySize*len(blockOffsets))
	var chunk []byte
	points := make([]ocommon.Point, 0, len(blockOffsets))
	for i, blockOffset := range blockOffsets {
		base := i * secondaryIndexEntrySize
		binary.BigEndian.PutUint64(secondary[base:], blockOffset)
		hash := make([]byte, 32)
		for j := range hash {
			hash[j] = byte(i + 1)
		}
		copy(secondary[base+16:base+48], hash)
		slot := uint64(100 + i)
		binary.BigEndian.PutUint64(secondary[base+48:], slot)
		points = append(points, ocommon.NewPoint(slot, hash))

		block, err := cbor.Encode([]any{
			uint64(1),
			cbor.RawMessage{0x80},
		})
		if err != nil {
			t.Fatalf("encode fixture block: %s", err)
		}
		chunk = append(chunk, block...)
	}
	for name, data := range map[string][]byte{
		"00000.chunk":     chunk,
		"00000.primary":   primary,
		"00000.secondary": secondary,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o640); err != nil {
			t.Fatalf("write %s: %s", name, err)
		}
	}
	return immutableIndexFixture{dir: dir, points: points}
}

func getBlockRecoveringPanic(
	imm *immutable.ImmutableDb,
	point ocommon.Point,
) (block *immutable.Block, err error, panicValue any) {
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
	imm, err := immutable.New(fixture.dir)
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
			imm, err := immutable.New(fixture.dir)
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
	requireGetBlockError(t, fixture, "beyond chunk size")
}

func TestImmutableIndexRejectsDescendingBlockOffsets(t *testing.T) {
	fixture := writeImmutableIndexFixture(
		t,
		1,
		[]uint32{0, secondaryIndexEntrySize, 2 * secondaryIndexEntrySize},
		[]uint64{3, 0},
	)
	requireGetBlockError(t, fixture, "does not follow current block offset")
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
