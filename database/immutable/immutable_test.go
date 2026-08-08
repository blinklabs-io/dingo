// Copyright 2024 Blink Labs Software
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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"github.com/blinklabs-io/dingo/database/immutable"
)

const (
	testDataDir = "./testdata"
)

func TestGetTip(t *testing.T) {
	// These expected values correspond to the last block in our test data
	var expectedSlot uint64 = 1295990
	expectedHash := "acff9c292f679ca2bd321fadd20f89af16d8f36d1b74794f0e97a6fa29fed359"
	imm, err := immutable.New(testDataDir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	tip, err := imm.GetTip()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if tip == nil {
		t.Fatalf("did not get expected tip value, got nil instead")
	}
	if tip.Slot != expectedSlot {
		t.Fatalf(
			"did not get expected slot value: expected %d, got %d",
			expectedSlot,
			tip.Slot,
		)
	}
	if hex.EncodeToString(tip.Hash) != expectedHash {
		t.Fatalf(
			"did not get expected hash value: expected %s, got %x",
			expectedHash,
			tip.Hash,
		)
	}
}

func TestLastSlotInChunk(t *testing.T) {
	imm, err := immutable.New(testDataDir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// testdata has 300 chunks (0..299); chunk 299's last block is the DB tip.
	last, ok, err := imm.LastSlotInChunk(299)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if !ok {
		t.Fatalf("expected chunk 299 to be present")
	}
	if last != 1295990 {
		t.Fatalf("chunk 299 last slot: expected 1295990, got %d", last)
	}
	// Per-chunk last slots must increase with chunk number.
	s0, ok0, err0 := imm.LastSlotInChunk(0)
	s1, ok1, err1 := imm.LastSlotInChunk(1)
	if err0 != nil || err1 != nil {
		t.Fatalf("unexpected error: %v %v", err0, err1)
	}
	if !ok0 || !ok1 {
		t.Fatalf("expected chunks 0 and 1 present")
	}
	if !(s0 < s1) {
		t.Fatalf("expected chunk 0 last slot < chunk 1, got %d >= %d", s0, s1)
	}
	// An out-of-range chunk number returns ok=false, no error.
	_, ok, err = imm.LastSlotInChunk(300)
	if err != nil {
		t.Fatalf("unexpected error for out-of-range chunk: %s", err)
	}
	if ok {
		t.Fatalf("expected ok=false for out-of-range chunk 300")
	}
}

func TestBlocksFromPointBeyondLastChunkError(t *testing.T) {
	imm, err := immutable.New(testDataDir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	_, err = imm.BlocksFromPoint(
		ocommon.Point{Slot: 999999999, Hash: []byte{}},
	)
	if !errors.Is(err, immutable.ErrPointBeyondLastChunk) {
		t.Fatalf(
			"expected ErrPointBeyondLastChunk, got %v",
			err,
		)
	}
}

func TestGetSpecificBlock(t *testing.T) {
	imm, err := immutable.New(testDataDir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Get an iterator starting from the beginning
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: []byte{}})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer iter.Close()

	// Skip to the 100th block to test a block that's not the first one
	blocks := make([]*immutable.Block, 0, 100)
	for i := range 100 {
		b, err := iter.Next()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if b == nil {
			t.Fatalf("expected to get at least 100 blocks, got only %d", i)
		}
		blocks = append(blocks, b)
	}
	block := blocks[len(blocks)-1]

	// Now test GetBlock with the point of this block
	retrievedBlock, err := imm.GetBlock(
		ocommon.Point{Slot: block.Slot, Hash: block.Hash},
	)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if retrievedBlock == nil {
		t.Fatalf("expected to get the block, got nil")
	}
	if retrievedBlock.Slot != block.Slot {
		t.Fatalf(
			"slot mismatch: expected %d, got %d",
			block.Slot,
			retrievedBlock.Slot,
		)
	}
	if string(retrievedBlock.Hash) != string(block.Hash) {
		t.Fatalf("hash mismatch")
	}
}

func TestBlocksRangeMultipleChunks(t *testing.T) {
	imm, err := immutable.New(testDataDir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Get an iterator starting from a point in the middle of the chain (slot 1000000)
	// This ensures we're testing range queries that may traverse multiple chunks
	iter, err := imm.BlocksFromPoint(
		ocommon.Point{Slot: 1000000, Hash: []byte{}},
	)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer iter.Close()

	var prevSlot uint64 = 999999 // Less than 1000000
	blockCount := 0
	for blockCount < 50 { // Iterate enough blocks to potentially traverse multiple chunks
		block, err := iter.Next()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if block == nil {
			break // No more blocks
		}
		if block.Slot < prevSlot {
			t.Fatalf(
				"slots not increasing: prev %d, current %d",
				prevSlot,
				block.Slot,
			)
		}
		prevSlot = block.Slot
		blockCount++
	}
	if blockCount == 0 {
		t.Fatalf("expected to get at least one block")
	}
	// Since we started from the middle and there are multiple chunks,
	// and we iterated blocks, we tested range queries across chunks
}

// TestNewFromRootReadsThroughTheHandle pins what NewFromRoot exists for: the
// reads follow the directory the handle refers to, not the name it was opened
// under.
//
// A caller that vetted a directory and then handed on its name would have the
// name resolved again at open time, and whatever occupies it by then is what
// gets read. Mithril bootstrap extracts into a download area where that is a
// real possibility, so the handle is what it passes.
func TestNewFromRootReadsThroughTheHandle(t *testing.T) {
	base := t.TempDir()
	ours := filepath.Join(base, "immutable")
	copyChunkTrio(t, "00000", ours)

	root, err := os.OpenRoot(ours)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer func() { _ = root.Close() }()
	imm, err := immutable.NewFromRoot(root)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	want, err := imm.GetTip()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if want == nil {
		t.Fatal("test fixture produced no tip")
	}

	// Somebody takes the name for a tree built from a different immutable
	// file, so which tree was read is visible in the tip.
	theirs := filepath.Join(base, "theirs")
	copyChunkTrio(t, "00001", theirs)
	if err := os.Rename(ours, filepath.Join(base, "moved-aside")); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("cannot swap a directory with an open handle: %s", err)
		}
		t.Fatalf("unexpected error: %s", err)
	}
	if err := os.Rename(theirs, ours); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	byName, err := immutable.New(ours)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	swapped, err := byName.GetTip()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if swapped == nil || swapped.Slot == want.Slot {
		t.Fatal(
			"the substitution must be observable through the name, or this " +
				"test proves nothing",
		)
	}

	got, err := imm.GetTip()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if got == nil || got.Slot != want.Slot {
		t.Fatalf(
			"read through the handle must stay on the original tree: "+
				"expected slot %d, got %v",
			want.Slot,
			got,
		)
	}
}

// TestNewFromRootRejectsNilHandle keeps the nil case an error rather than a
// silent fall back to a pathname the caller did not supply.
func TestNewFromRootRejectsNilHandle(t *testing.T) {
	if _, err := immutable.NewFromRoot(nil); err == nil {
		t.Fatal("expected an error for a nil directory handle")
	}
}

// copyChunkTrio copies one immutable file's chunk/primary/secondary trio out of
// the shared testdata into dir, producing a real single-chunk ImmutableDB.
func copyChunkTrio(t *testing.T, name, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	for _, ext := range []string{".chunk", ".primary", ".secondary"} {
		data, err := os.ReadFile(filepath.Join(testDataDir, name+ext))
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := os.WriteFile(
			filepath.Join(dir, name+ext), data, 0o640,
		); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}
}

// trioDigests is the digest map a Mithril v2 bootstrap holds for the files in
// dir: the certified SHA-256 of every chunk/primary/secondary it downloaded,
// keyed by the name the reader will ask for.
func trioDigests(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	digests := make(map[string]string, len(entries))
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		sum := sha256.Sum256(data)
		digests[entry.Name()] = hex.EncodeToString(sum[:])
	}
	return digests
}

// TestNewFromRootVerifiedReadsTheVerifiedTree is the baseline the refusals
// below are only meaningful against: a tree whose files match their certified
// digests reads exactly as an unverified open of the same tree does.
func TestNewFromRootVerifiedReadsTheVerifiedTree(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "immutable")
	copyChunkTrio(t, "00000", dir)

	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer func() { _ = root.Close() }()

	plain, err := immutable.NewFromRoot(root)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	want, err := plain.GetTip()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if want == nil {
		t.Fatal("test fixture produced no tip")
	}

	imm, err := immutable.NewFromRootVerified(root, trioDigests(t, dir), "")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	got, err := imm.GetTip()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if got == nil || got.Slot != want.Slot {
		t.Fatalf("expected tip slot %d, got %v", want.Slot, got)
	}
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer func() { _ = iter.Close() }()
	block, err := iter.Next()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if block == nil {
		t.Fatal("expected to read a block from a tree that verifies")
	}
}

// TestNewFromRootVerifiedRefusesAFileSubstitutedAfterTheDigestCheck is the
// finding. The digest map stands for the check a Mithril bootstrap performs
// when the archive lands; the substitution happens afterwards, which is all a
// writer sharing the download directory has to do to have uncertified bytes
// read. A handle on the directory does not stop it — the file is reached by
// name inside a directory that is still the right one.
func TestNewFromRootVerifiedRefusesAFileSubstitutedAfterTheDigestCheck(
	t *testing.T,
) {
	base := t.TempDir()
	dir := filepath.Join(base, "immutable")
	copyChunkTrio(t, "00000", dir)
	digests := trioDigests(t, dir)

	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer func() { _ = root.Close() }()
	imm, err := immutable.NewFromRootVerified(root, digests, "")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Renamed over rather than written through: the extracted file belongs to
	// this process, so replacing the name is what a writer with the directory
	// but not the file actually does.
	theirs := filepath.Join(base, "theirs.chunk")
	data, err := os.ReadFile(filepath.Join(testDataDir, "00001.chunk"))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := os.WriteFile(theirs, data, 0o640); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := os.Rename(
		theirs, filepath.Join(dir, "00000.chunk"),
	); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer func() { _ = iter.Close() }()
	_, err = iter.Next()
	if !errors.Is(err, immutable.ErrDigestMismatch) {
		t.Fatalf(
			"reading a substituted chunk must fail as a digest mismatch, got %v",
			err,
		)
	}
}

// TestNewFromRootVerifiedRefusesAFileTheDigestsDoNotCover keeps "no digest" an
// error rather than an open. A file nothing certified is exactly what an
// attacker adds, and reading it because the map is silent about it would make
// the map's coverage the attacker's choice.
func TestNewFromRootVerifiedRefusesAFileTheDigestsDoNotCover(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "immutable")
	copyChunkTrio(t, "00000", dir)
	digests := trioDigests(t, dir)
	delete(digests, "00000.chunk")

	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer func() { _ = root.Close() }()
	imm, err := immutable.NewFromRootVerified(root, digests, "")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer func() { _ = iter.Close() }()
	if _, err := iter.Next(); err == nil {
		t.Fatal("expected an uncovered file to be refused")
	}
}

// TestNewFromRootVerifiedStopsAtTheChunkLimit covers the pipelined copy, which
// reads a tree the download pool is still writing into. Chunks arrive out of
// order, so a chunk above the contiguous prefix may be present and half
// written; the bound keeps the reader inside the prefix whose digests have
// been checked instead of failing on one that is merely unfinished.
func TestNewFromRootVerifiedStopsAtTheChunkLimit(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "immutable")
	copyChunkTrio(t, "00000", dir)
	copyChunkTrio(t, "00001", dir)
	digests := trioDigests(t, dir)

	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer func() { _ = root.Close() }()

	full, err := immutable.NewFromRootVerified(root, digests, "")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if _, ok, err := full.LastSlotInChunk(1); err != nil || !ok {
		t.Fatalf("both chunks must be visible unbounded: ok=%v err=%v", ok, err)
	}

	bounded, err := immutable.NewFromRootVerified(
		root, digests, immutable.ChunkName(0),
	)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if _, ok, err := bounded.LastSlotInChunk(1); err != nil || ok {
		t.Fatalf(
			"chunk 1 is above the bound and must be invisible: ok=%v err=%v",
			ok, err,
		)
	}
	boundedTip, err := bounded.GetTip()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	fullTip, err := full.GetTip()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if boundedTip == nil || fullTip == nil {
		t.Fatal("test fixture produced no tip")
	}
	if boundedTip.Slot >= fullTip.Slot {
		t.Fatalf(
			"the bound must hold the tip inside the prefix: bounded %d, full %d",
			boundedTip.Slot, fullTip.Slot,
		)
	}
}

// TestLastSlotInChunkIsByNumberNotPosition covers a range that does not start
// at zero, which a Mithril catch-up produces: it downloads only the archives
// above the import marker, so the lowest chunk on disk is not chunk 0.
//
// Answering by position then names a different chunk than the caller asked
// for — silently, and further off the higher the marker — so the copy would be
// bounded by the wrong slot.
func TestLastSlotInChunkIsByNumberNotPosition(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "immutable")
	copyChunkTrio(t, "00001", dir)
	copyChunkTrio(t, "00002", dir)

	imm, err := immutable.New(dir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// The two chunks are at positions 0 and 1 and are numbered 1 and 2, so
	// every assertion below distinguishes the two readings.
	got, ok, err := imm.LastSlotInChunk(1)
	if err != nil || !ok {
		t.Fatalf("chunk 1 must be present: ok=%v err=%v", ok, err)
	}
	want, ok, err := imm.LastSlotInChunk(2)
	if err != nil || !ok {
		t.Fatalf("chunk 2 must be present: ok=%v err=%v", ok, err)
	}
	if got >= want {
		t.Fatalf(
			"chunk 1 must end before chunk 2: got %d and %d — position "+
				"lookup would return chunk 2's slot for chunk 1",
			got, want,
		)
	}
	// Chunk 0 was never downloaded. Occupying position 0 is not the same as
	// being present, and reporting it as present is how the copy comes to read
	// a chunk that is not there.
	if _, ok, err := imm.LastSlotInChunk(0); err != nil || ok {
		t.Fatalf("chunk 0 is absent and must report so: ok=%v err=%v", ok, err)
	}
}

// TestNewFromRootVerifiedBoundHoldsPastFiveDigits covers the width change.
//
// ChunkName pads to five digits, which stops being a fixed width at 100000 —
// and comparing "99999" with "100000" as strings puts them the wrong way
// round, because '9' sorts after '1'. The bound would then hide the chunk
// immediately below it and admit the one above, which is both halves of what
// it exists to prevent.
//
// No fixture is needed on disk for this: the names alone decide it, and a
// real tree at those numbers is a hundred thousand files.
func TestNewFromRootVerifiedBoundHoldsPastFiveDigits(t *testing.T) {
	for _, tc := range []struct {
		name  string
		bound string
		above bool
	}{
		{name: "00000", bound: "00000", above: false},
		{name: "00001", bound: "00000", above: true},
		// The pair a string comparison gets backwards, both ways round.
		{name: "99999", bound: "100000", above: false},
		{name: "100000", bound: "99999", above: true},
		{name: "100000", bound: "100000", above: false},
		{name: "100001", bound: "100000", above: true},
		{name: "999999", bound: "1000000", above: false},
	} {
		if got := immutable.ChunkNameAbove(
			tc.name, tc.bound,
		); got != tc.above {
			t.Errorf(
				"chunk %s above bound %s: got %v, want %v",
				tc.name, tc.bound, got, tc.above,
			)
		}
	}

	// And the numbers those names come from stay ordered through the change.
	if immutable.ChunkNameAbove(
		immutable.ChunkName(99999), immutable.ChunkName(100000),
	) {
		t.Error("chunk 99999 must be inside a bound of 100000")
	}
	if !immutable.ChunkNameAbove(
		immutable.ChunkName(100000), immutable.ChunkName(99999),
	) {
		t.Error("chunk 100000 must be outside a bound of 99999")
	}
}
