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
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/blinklabs-io/dingo/internal/fsyncdir"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
)

// Geometry describes how slots map onto chunk files. It must match the
// geometry the node that produced (or will read) the files uses: every chunk
// spans ChunkSlots regular slots, and when CanContainEBB is set each chunk
// also has a leading relative slot for a Byron epoch boundary block.
type Geometry struct {
	ChunkSlots    uint64
	CanContainEBB bool
}

// Valid reports whether the geometry can address chunk files.
func (g Geometry) Valid() bool {
	// Primary-index offsets are 32-bit counts of 56-byte records, and a
	// chunk's relative slot is an int.
	return g.ChunkSlots > 0 && g.ChunkSlots < 1<<24
}

// relSlots is the number of relative slots a chunk's primary index covers.
func (g Geometry) relSlots() uint64 {
	if g.CanContainEBB {
		return g.ChunkSlots + 1
	}
	return g.ChunkSlots
}

// position returns the chunk and relative slot a block occupies.
func (g Geometry) position(slot uint64, ebb bool) (uint64, uint64, error) {
	chunkNo := slot / g.ChunkSlots
	if ebb {
		if !g.CanContainEBB {
			return 0, 0, errors.New(
				"immutable store: epoch boundary block in a geometry " +
					"that cannot contain one",
			)
		}
		if slot%g.ChunkSlots != 0 {
			return 0, 0, fmt.Errorf(
				"immutable store: epoch boundary block slot %d is not a "+
					"multiple of chunk size %d",
				slot, g.ChunkSlots,
			)
		}
		return chunkNo, 0, nil
	}
	rel := slot % g.ChunkSlots
	if g.CanContainEBB {
		rel++
	}
	return chunkNo, rel, nil
}

// ErrStoreCorrupt reports a chunk whose entries are damaged before its last
// good entry. Only a torn tail is repaired; damage inside written data needs
// the chunk files restored.
var ErrStoreCorrupt = errors.New("immutable store: corrupt chunk")

// ErrStoreOutOfOrder reports an append at or before the store's tip position.
var ErrStoreOutOfOrder = errors.New(
	"immutable store: block does not follow the tip",
)

// WriteBlock is one block to append. Cbor is the era-specific block CBOR; the
// store wraps it as [Type, Cbor], which is how cardano-node lays blocks out in
// a chunk file.
type WriteBlock struct {
	Hash []byte
	Cbor []byte
	Type uint
	Slot uint64
}

// Location addresses a block's wrapped bytes in a chunk file.
type Location struct {
	Chunk  uint64
	Offset uint64
	Size   uint32
	// CRC is the CRC-32 (IEEE) of the wrapped bytes, as the secondary index
	// stores it.
	CRC uint32
}

// StoreTip is the last block in the store.
type StoreTip struct {
	Hash []byte
	Slot uint64
}

// Store appends cardano-node compatible chunk, primary and secondary files to a
// directory and reads blocks back from them. One Store owns its directory;
// there must be no second writer.
type Store struct {
	dir  string
	geom Geometry

	mu sync.RWMutex
	// The open tip chunk's append handles. Nil until the first append.
	chunkFile, secFile, priFile *os.File
	chunkNo                     uint64
	chunkSize                   uint64
	secSize                     uint64
	// nextRel is the first relative slot with no primary offset yet.
	nextRel uint64
	hasTip  bool
	tip     StoreTip
	closed  bool
	// syncFile fsyncs one file. Tests replace it to count durability points;
	// nil means (*os.File).Sync.
	syncFile func(*os.File) error
}

// OpenStore opens dir for appending, creating it if needed, and recovers a
// torn tail left by an interrupted append.
func OpenStore(dir string, geom Geometry) (*Store, error) {
	if !geom.Valid() {
		return nil, fmt.Errorf("immutable store: invalid geometry %+v", geom)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("immutable store: create directory: %w", err)
	}
	s := &Store{dir: dir, geom: geom}
	if err := s.recover(); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) path(num uint64, ext string) string {
	return filepath.Join(s.dir, ChunkName(num)+ext)
}

// listChunks returns the chunk numbers present, ascending, that have a
// .chunk file.
func (s *Store) listChunks() ([]uint64, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	nums := make([]uint64, 0, len(entries))
	for _, e := range entries {
		base, ext, ok := strings.Cut(e.Name(), ".")
		if !ok || ext != "chunk" || !isCanonicalChunkName(base) {
			continue
		}
		n, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			continue
		}
		nums = append(nums, n)
	}
	sortUint64(nums)
	return nums, nil
}

func sortUint64(v []uint64) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j-1] > v[j]; j-- {
			v[j-1], v[j] = v[j], v[j-1]
		}
	}
}

type secEntry struct {
	secondaryIndexEntryInner
}

func decodeSecEntry(b []byte) secEntry {
	return secEntry{secondaryIndexEntryInner{
		BlockOffset:  binary.BigEndian.Uint64(b[0:]),
		HeaderOffset: binary.BigEndian.Uint16(b[8:]),
		HeaderSize:   binary.BigEndian.Uint16(b[10:]),
		Checksum:     binary.BigEndian.Uint32(b[12:]),
		HeaderHash:   [32]byte(b[16:48]),
		BlockOrEbb:   binary.BigEndian.Uint64(b[48:]),
	}}
}

func (e secEntry) encode() []byte {
	b := make([]byte, secondaryIndexEntrySize)
	binary.BigEndian.PutUint64(b[0:], e.BlockOffset)
	binary.BigEndian.PutUint16(b[8:], e.HeaderOffset)
	binary.BigEndian.PutUint16(b[10:], e.HeaderSize)
	binary.BigEndian.PutUint32(b[12:], e.Checksum)
	copy(b[16:48], e.HeaderHash[:])
	binary.BigEndian.PutUint64(b[48:], e.BlockOrEbb)
	return b
}

// wrappedLen returns the length of the CBOR item at the start of data.
func wrappedLen(data []byte) (int, error) {
	var tmp struct {
		cbor.StructAsArray
		BlockType uint
		BlockCbor cbor.RawMessage
	}
	return cbor.Decode(data, &tmp)
}

// primaryOffsets reads a primary index's offsets, dropping a torn trailing
// partial offset. A missing or empty file has none.
func readPrimaryOffsets(path string) ([]uint32, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return []uint32{}, nil
		}
		return nil, err
	}
	if len(data) < 1 {
		return []uint32{}, nil
	}
	if data[0] != primaryIndexVersion {
		return nil, fmt.Errorf(
			"immutable store: unsupported primary index version %d in %s",
			data[0], path,
		)
	}
	n := (len(data) - 1) / 4
	offs := make([]uint32, n)
	for i := range offs {
		offs[i] = binary.BigEndian.Uint32(data[1+4*i:])
	}
	return offs, nil
}

// occupiedRels maps each secondary entry index to its relative slot, from a
// primary offset list. Slot k is occupied when the offset after it is larger.
func occupiedRels(offs []uint32) []uint64 {
	rels := make([]uint64, 0, max(len(offs)-1, 0))
	for k := range max(len(offs)-1, 0) {
		if offs[k+1] > offs[k] {
			rels = append(rels, uint64(k))
		}
	}
	return rels
}

// validPrimaryPrefix returns offs up to the first offset that does not follow
// its predecessor by zero or one secondary entry, the only steps an append
// writes. What follows that point is a torn tail when no offset in it claims
// more entries than the prefix, as a zero-filled tail does. One that claims
// more is damage inside the written index, and trimming there would drop the
// valid blocks it hides.
func validPrimaryPrefix(offs []uint32) ([]uint32, bool) {
	for i, off := range offs {
		var prev uint32
		if i > 0 {
			prev = offs[i-1]
			if off == prev || off == prev+secondaryIndexEntrySize {
				continue
			}
		} else if off == 0 {
			continue
		}
		for _, later := range offs[i:] {
			if later > prev {
				return nil, false
			}
		}
		return offs[:i], true
	}
	return offs, true
}

// recover reads the highest chunk and trims it to its last entry that the
// primary index, the secondary index and the chunk bytes all agree on.
func (s *Store) recover() error {
	nums, err := s.listChunks()
	if err != nil {
		return fmt.Errorf("immutable store: list chunks: %w", err)
	}
	// A chunk is only ever created after the previous one is complete, so
	// every chunk below the highest is already good.
	if len(nums) == 0 {
		return nil
	}
	for i, num := range slices.Backward(nums) {
		found, err := s.recoverChunk(num)
		if err != nil {
			return err
		}
		if found {
			// Chunks above an empty recovered one were torn creations.
			for _, n := range nums[i+1:] {
				if err := s.removeChunk(n); err != nil {
					return err
				}
			}
			return s.openTail(num)
		}
	}
	// No chunk holds a block: start over from the empty directory.
	for _, n := range nums {
		if err := s.removeChunk(n); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) removeChunk(num uint64) error {
	for _, ext := range []string{".chunk", ".secondary", ".primary"} {
		if err := os.Remove(s.path(num, ext)); err != nil &&
			!errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("immutable store: remove %s: %w",
				s.path(num, ext), err)
		}
	}
	return fsyncdir.Sync(s.dir)
}

// recoverChunk trims chunk num to its last good entry and reports whether any
// entry survived. The files are left consistent either way.
func (s *Store) recoverChunk(num uint64) (bool, error) {
	chunkBytes, err := os.ReadFile(s.path(num, ".chunk"))
	if err != nil {
		return false, fmt.Errorf("immutable store: read chunk: %w", err)
	}
	secBytes, err := os.ReadFile(s.path(num, ".secondary"))
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return false, fmt.Errorf("immutable store: read secondary: %w", err)
		}
		secBytes = []byte{}
	}
	offs, err := readPrimaryOffsets(s.path(num, ".primary"))
	if err != nil {
		return false, err
	}
	offs, ok := validPrimaryPrefix(offs)
	if !ok {
		return false, fmt.Errorf(
			"%w: chunk %d primary index has an offset that skips entries; "+
				"restore the chunk files or truncate the database",
			ErrStoreCorrupt, num,
		)
	}
	rels := occupiedRels(offs)
	good := min(len(rels), len(secBytes)/secondaryIndexEntrySize)
	// Check every entry, not just the tail: an entry that fails while a later
	// one passes is damage inside written data, which trimming would turn into
	// silent loss of the good blocks after it.
	var (
		lastEnd  uint64
		lastGood int
		firstBad = -1
	)
	for i := 0; i < good; i++ {
		entry := decodeSecEntry(secBytes[uint64(i)*secondaryIndexEntrySize:])
		end, ok := verifiedEntryEnd(chunkBytes, entry)
		// Entries are written back to back: one that does not start where
		// its predecessor ended is damaged even when its own bytes check out.
		if ok && i == lastGood && entry.BlockOffset != lastEnd {
			ok = false
		}
		if !ok {
			if firstBad < 0 {
				firstBad = i
			}
			continue
		}
		lastGood, lastEnd = i+1, end
	}
	if firstBad >= 0 && firstBad < lastGood {
		return false, fmt.Errorf(
			"%w: chunk %d entry %d fails its checksum but entry %d passes; "+
				"restore the chunk files or truncate the database",
			ErrStoreCorrupt, num, firstBad, lastGood-1,
		)
	}
	good = lastGood
	if err := truncateFile(s.path(num, ".chunk"), lastEnd); err != nil {
		return false, err
	}
	if err := truncateFile(
		s.path(num, ".secondary"),
		uint64(good)*secondaryIndexEntrySize, // #nosec G115 -- good is non-negative
	); err != nil {
		return false, err
	}
	// Primary: keep offsets through the last good entry's slot, then the
	// final offset.
	priPath := s.path(num, ".primary")
	keep := uint64(0)
	if good > 0 {
		keep = rels[good-1] + 1
	}
	finalOff := uint32(
		good,
	) * secondaryIndexEntrySize // #nosec G115 -- bounded by geometry
	if primaryIsPadded(offs, keep, finalOff) {
		// A completed chunk's primary is padded to the full slot count and
		// already agrees with the entries; rewriting it would change files a
		// reader may have verified against a certified digest.
		return good > 0, nil
	}
	if err := truncateFile(priPath, 1+4*keep); err != nil {
		return false, err
	}
	if err := ensureFileHeader(priPath); err != nil {
		return false, err
	}
	f, err := os.OpenFile(priPath, os.O_WRONLY, 0o644)
	if err != nil {
		return false, err
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], finalOff)
	_, werr := f.WriteAt(buf[:], int64(1+4*keep))
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		return false, err
	}
	return good > 0, nil
}

// primaryIsPadded reports whether offs is a primary index whose entries end at
// slot keep with final offset finalOff and whose remaining slots all repeat
// that offset.
func primaryIsPadded(offs []uint32, keep uint64, finalOff uint32) bool {
	if uint64(len(offs)) <= keep+1 {
		return false
	}
	for _, o := range offs[keep:] {
		if o != finalOff {
			return false
		}
	}
	return true
}

// verifiedEntryEnd returns the offset just past the entry's block when its
// bytes are present and match the checksum the secondary entry records.
func verifiedEntryEnd(chunkBytes []byte, entry secEntry) (uint64, bool) {
	if entry.BlockOffset >= uint64(len(chunkBytes)) {
		return 0, false
	}
	n, err := wrappedLen(chunkBytes[entry.BlockOffset:])
	if err != nil {
		return 0, false
	}
	end := entry.BlockOffset + uint64(n) // #nosec G115 -- n is non-negative
	if end > uint64(len(chunkBytes)) ||
		crc32.ChecksumIEEE(
			chunkBytes[entry.BlockOffset:end],
		) != entry.Checksum {
		return 0, false
	}
	return end, true
}

func truncateFile(path string, size uint64) error {
	if err := unshare(path); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("immutable store: open %s: %w", path, err)
	}
	terr := f.Truncate(int64(size)) // #nosec G115 -- file sizes fit int64
	serr := f.Sync()
	cerr := f.Close()
	return errors.Join(terr, serr, cerr)
}

// ensureFileHeader makes sure a primary file has its version byte.
func ensureFileHeader(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() == 0 {
		if _, err := f.WriteAt([]byte{primaryIndexVersion}, 0); err != nil {
			return err
		}
	}
	return nil
}

// openTail opens the recovered chunk num for appending and loads the tip.
func (s *Store) openTail(num uint64) error {
	if err := s.openChunkFiles(num, false); err != nil {
		return err
	}
	secBytes, err := os.ReadFile(s.path(num, ".secondary"))
	if err != nil {
		return err
	}
	st, err := s.chunkFile.Stat()
	if err != nil {
		return err
	}
	s.chunkSize = uint64(st.Size()) // #nosec G115 -- non-negative
	s.secSize = uint64(len(secBytes))
	offs, err := readPrimaryOffsets(s.path(num, ".primary"))
	if err != nil {
		return err
	}
	if len(offs) == 0 {
		return errors.New("immutable store: tail primary index is empty")
	}
	s.nextRel = uint64(len(offs) - 1) // #nosec G115 -- offs is non-empty
	if s.secSize > 0 {
		last := decodeSecEntry(secBytes[s.secSize-secondaryIndexEntrySize:])
		blockOrEbb := last.BlockOrEbb
		slot := blockOrEbb
		// The last entry's slot: an EBB stores its epoch, a regular block its
		// absolute slot.
		rels := occupiedRels(offs)
		if s.geom.CanContainEBB && rels[len(rels)-1] == 0 {
			slot = blockOrEbb * s.geom.ChunkSlots
		}
		s.tip = StoreTip{
			Slot: slot,
			Hash: append([]byte(nil), last.HeaderHash[:]...),
		}
		s.hasTip = true
	}
	return nil
}

// openChunkFiles opens (and, when create is set, creates) the three files of
// chunk num as the append target.
func (s *Store) openChunkFiles(num uint64, create bool) error {
	s.closeFiles()
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE | os.O_EXCL
	} else if err := unshareChunk(s, num); err != nil {
		return err
	}
	var err error
	if s.chunkFile, err = os.OpenFile(s.path(num, ".chunk"), flags, 0o644); err != nil {
		return err
	}
	if s.secFile, err = os.OpenFile(s.path(num, ".secondary"), flags, 0o644); err != nil {
		return err
	}
	if s.priFile, err = os.OpenFile(s.path(num, ".primary"), flags, 0o644); err != nil {
		return err
	}
	s.chunkNo = num
	return nil
}

func (s *Store) closeFiles() {
	for _, f := range []**os.File{&s.chunkFile, &s.secFile, &s.priFile} {
		if *f != nil {
			_ = (*f).Close()
			*f = nil
		}
	}
}

// syncOpenFiles fsyncs the open chunk's three files.
func (s *Store) syncOpenFiles() error {
	for _, f := range []*os.File{s.chunkFile, s.secFile, s.priFile} {
		if f == nil {
			continue
		}
		var err error
		if s.syncFile != nil {
			err = s.syncFile(f)
		} else {
			err = f.Sync()
		}
		if err != nil {
			return fmt.Errorf("immutable store: sync: %w", err)
		}
	}
	return nil
}

// completeChunk pads the open chunk and makes it durable. Store.Sync only
// reaches the open handles, so a chunk left behind by advancing must be
// fsynced here or a marker could commit for bytes the disk never held.
func (s *Store) completeChunk() error {
	if err := s.finishChunk(); err != nil {
		return err
	}
	return s.syncOpenFiles()
}

// finishChunk pads the open chunk's primary index to the full slot count, the
// way cardano-node completes a chunk before starting the next.
func (s *Store) finishChunk() error {
	return s.padPrimary(s.geom.relSlots())
}

// padPrimary extends the primary index with empty-slot offsets until the next
// relative slot to describe is upTo.
func (s *Store) padPrimary(upTo uint64) error {
	if upTo <= s.nextRel {
		return nil
	}
	buf := make([]byte, 0, 4*(upTo-s.nextRel))
	var word [4]byte
	binary.BigEndian.PutUint32(
		word[:],
		uint32(s.secSize),
	) // #nosec G115 -- bounded by geometry
	for k := s.nextRel; k < upTo; k++ {
		buf = append(buf, word[:]...)
	}
	if _, err := s.priFile.WriteAt(buf, int64(1+4*(s.nextRel+1))); err != nil { // #nosec G115 -- bounded by geometry
		return err
	}
	s.nextRel = upTo
	return nil
}

// newChunk creates chunk num's trio as the append target.
func (s *Store) newChunk(num uint64) error {
	if err := s.openChunkFiles(num, true); err != nil {
		return err
	}
	// Version byte and the single final offset of an empty chunk.
	if _, err := s.priFile.WriteAt([]byte{primaryIndexVersion, 0, 0, 0, 0}, 0); err != nil {
		return err
	}
	s.chunkSize, s.secSize, s.nextRel = 0, 0, 0
	return nil
}

// Append adds a block after the store's tip. It writes the chunk, then the
// secondary index, then the primary index, so a crash can only leave the
// indexes behind the chunk bytes. Nothing is durable until Sync.
func (s *Store) Append(b WriteBlock) (Location, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Location{}, errors.New("immutable store: closed")
	}
	if len(b.Hash) != 32 {
		return Location{}, fmt.Errorf(
			"immutable store: block hash is %d bytes, want 32", len(b.Hash),
		)
	}
	ebb := b.Type == byron.BlockTypeByronEbb
	chunkNo, rel, err := s.geom.position(b.Slot, ebb)
	if err != nil {
		return Location{}, err
	}
	wrapped, headerOffset, headerSize, err := wrapBlock(b.Type, b.Cbor)
	if err != nil {
		return Location{}, err
	}
	if s.chunkFile == nil {
		// First block ever: chunks below it exist as empty trios so the
		// directory stays a gapless range from chunk zero.
		for n := range chunkNo {
			if err := s.newChunk(n); err != nil {
				return Location{}, err
			}
			if err := s.completeChunk(); err != nil {
				return Location{}, err
			}
		}
		if err := s.newChunk(chunkNo); err != nil {
			return Location{}, err
		}
		if err := fsyncdir.Sync(s.dir); err != nil {
			return Location{}, err
		}
	} else {
		if chunkNo < s.chunkNo || (chunkNo == s.chunkNo && rel < s.nextRel &&
			s.secSize > 0) {
			return Location{}, ErrStoreOutOfOrder
		}
		advanced := false
		for s.chunkNo < chunkNo {
			if err := s.completeChunk(); err != nil {
				return Location{}, err
			}
			if err := s.newChunk(s.chunkNo + 1); err != nil {
				return Location{}, err
			}
			advanced = true
		}
		if advanced {
			if err := fsyncdir.Sync(s.dir); err != nil {
				return Location{}, err
			}
		}
	}
	if rel < s.nextRel {
		return Location{}, ErrStoreOutOfOrder
	}
	if s.secSize/secondaryIndexEntrySize >= uint64(
		^uint32(0),
	)/secondaryIndexEntrySize {
		return Location{}, errors.New("immutable store: chunk is full")
	}
	loc := Location{
		Chunk:  s.chunkNo,
		Offset: s.chunkSize,
		Size:   uint32(len(wrapped)), // #nosec G115 -- bounded by maxBlockSize
		CRC:    crc32.ChecksumIEEE(wrapped),
	}
	if _, err := s.chunkFile.WriteAt(wrapped, int64(s.chunkSize)); err != nil { // #nosec G115
		return Location{}, fmt.Errorf("immutable store: write chunk: %w", err)
	}
	blockOrEbb := b.Slot
	if ebb {
		blockOrEbb = b.Slot / s.geom.ChunkSlots
	}
	entry := secEntry{secondaryIndexEntryInner{
		BlockOffset:  s.chunkSize,
		HeaderOffset: headerOffset,
		HeaderSize:   headerSize,
		Checksum:     loc.CRC,
		HeaderHash:   [32]byte(b.Hash),
		BlockOrEbb:   blockOrEbb,
	}}
	if _, err := s.secFile.WriteAt(entry.encode(), int64(s.secSize)); err != nil { // #nosec G115
		return Location{}, fmt.Errorf(
			"immutable store: write secondary: %w",
			err,
		)
	}
	// Primary: empty slots up to rel repeat the current offset (the previous
	// final offset already serves slot nextRel), then the new final offset.
	if err := s.padPrimary(rel); err != nil {
		return Location{}, fmt.Errorf("immutable store: write primary: %w", err)
	}
	var word [4]byte
	binary.BigEndian.PutUint32(
		word[:],
		uint32(s.secSize+secondaryIndexEntrySize),
	) // #nosec G115
	if _, err := s.priFile.WriteAt(word[:], int64(1+4*(rel+1))); err != nil { // #nosec G115
		return Location{}, fmt.Errorf("immutable store: write primary: %w", err)
	}
	s.nextRel = rel + 1
	s.chunkSize += uint64(len(wrapped))
	s.secSize += secondaryIndexEntrySize
	s.tip = StoreTip{Slot: b.Slot, Hash: append([]byte(nil), b.Hash...)}
	s.hasTip = true
	return loc, nil
}

// wrapBlock returns the on-disk bytes of a block, [type, cbor], and the offset
// and size of its header within them.
func wrapBlock(typ uint, inner []byte) ([]byte, uint16, uint16, error) {
	prefix, err := cbor.Encode([]any{typ, 0})
	if err != nil || len(prefix) < 3 {
		return nil, 0, 0, fmt.Errorf("immutable store: encode wrapper: %w", err)
	}
	// [type, 0]: array header, type, then the single placeholder byte.
	prefix = prefix[:len(prefix)-1]
	wrapped := make([]byte, 0, len(prefix)+len(inner))
	wrapped = append(wrapped, prefix...)
	wrapped = append(wrapped, inner...)
	var parts []cbor.RawMessage
	if _, err := cbor.Decode(inner, &parts); err != nil || len(parts) == 0 {
		return nil, 0, 0, fmt.Errorf(
			"immutable store: block is not a CBOR array: %w", err,
		)
	}
	// The inner array header is one byte for the era block shapes (fewer than
	// 24 elements).
	hOff := len(prefix) + 1
	hSize := len(parts[0])
	if hOff > 0xFFFF || hSize > 0xFFFF {
		return nil, 0, 0, errors.New(
			"immutable store: header exceeds index range",
		)
	}
	return wrapped, uint16(hOff), uint16(hSize), nil // #nosec G115 -- checked
}

// Sync makes everything appended so far durable: the three files, then the
// directory entry of any newly created one.
func (s *Store) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.chunkFile == nil {
		return nil
	}
	if err := s.syncOpenFiles(); err != nil {
		return err
	}
	return fsyncdir.Sync(s.dir)
}

// Tip returns the last block appended, if any.
func (s *Store) Tip() (StoreTip, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tip, s.hasTip
}

// Geometry returns the store's slot geometry.
func (s *Store) Geometry() Geometry { return s.geom }

// Dir returns the directory the store writes to.
func (s *Store) Dir() string { return s.dir }

// Close releases the append handles. It does not Sync.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.closeFiles()
	return nil
}

// ReadLocation reads the block at loc and returns its inner CBOR and era type.
func (s *Store) ReadLocation(loc Location) (uint, []byte, error) {
	f, err := os.Open(s.path(loc.Chunk, ".chunk"))
	if err != nil {
		return 0, nil, fmt.Errorf("immutable store: open chunk: %w", err)
	}
	defer f.Close()
	return readWrapped(f, loc)
}

func readWrapped(f io.ReaderAt, loc Location) (uint, []byte, error) {
	if int64(loc.Size) > maxBlockSize {
		return 0, nil, fmt.Errorf(
			"%w: block size %d exceeds maximum %d",
			ErrInvalidChunkOffset, loc.Size, maxBlockSize,
		)
	}
	buf := make([]byte, loc.Size)
	if _, err := f.ReadAt(buf, int64(loc.Offset)); err != nil { // #nosec G115
		return 0, nil, fmt.Errorf("immutable store: read block: %w", err)
	}
	if crc32.ChecksumIEEE(buf) != loc.CRC {
		return 0, nil, fmt.Errorf(
			"immutable store: checksum mismatch in chunk %d at offset %d",
			loc.Chunk, loc.Offset,
		)
	}
	var tmp struct {
		cbor.StructAsArray
		BlockType uint
		BlockCbor cbor.RawMessage
	}
	if _, err := cbor.Decode(buf, &tmp); err != nil {
		return 0, nil, err
	}
	return tmp.BlockType, []byte(tmp.BlockCbor), nil
}

// ReadBlockByPoint reads an existing chunk directory without opening an append
// writer or recovering its tail. The caller keeps the files stable during the read.
func ReadBlockByPoint(
	dir string,
	geom Geometry,
	slot uint64,
	hash []byte,
) (*Block, *Location, error) {
	if !geom.Valid() {
		return nil, nil, fmt.Errorf("invalid ImmutableDB geometry %+v", geom)
	}
	return (&Store{dir: dir, geom: geom}).GetBlock(slot, hash)
}

// GetBlock reads the block at the point in O(1): two index reads and one
// chunk read, with no scan of the chunk. A Byron epoch boundary block is found
// at its chunk's leading relative slot. It returns nil, nil when the point is
// not in the store.
func (s *Store) GetBlock(slot uint64, hash []byte) (*Block, *Location, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	candidates := []bool{false}
	if s.geom.CanContainEBB && slot%s.geom.ChunkSlots == 0 {
		candidates = []bool{true, false}
	}
	for _, ebb := range candidates {
		chunkNo, rel, err := s.geom.position(slot, ebb)
		if err != nil {
			return nil, nil, err
		}
		entry, loc, err := s.lookup(chunkNo, rel)
		if err != nil {
			return nil, nil, err
		}
		if entry == nil || loc == nil ||
			!bytes.Equal(entry.HeaderHash[:], hash) {
			continue
		}
		typ, inner, err := s.ReadLocation(*loc)
		if err != nil {
			return nil, nil, err
		}
		return &Block{
			Hash:  append([]byte(nil), hash...),
			Cbor:  inner,
			Type:  typ,
			Slot:  entry.BlockOrEbb,
			IsEbb: typ == byron.BlockTypeByronEbb,
		}, loc, nil
	}
	return nil, nil, nil
}

// lookup finds the secondary entry for a relative slot, or nil when the slot
// or chunk is empty or absent.
func (s *Store) lookup(chunkNo, rel uint64) (*secEntry, *Location, error) {
	pri, err := os.Open(s.path(chunkNo, ".primary"))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	defer pri.Close()
	var offs [8]byte
	n, err := pri.ReadAt(offs[:], int64(1+4*rel)) // #nosec G115
	if n < 8 {
		if err == nil || errors.Is(err, io.EOF) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	start := binary.BigEndian.Uint32(offs[:4])
	end := binary.BigEndian.Uint32(offs[4:])
	if end <= start {
		return nil, nil, nil
	}
	sec, err := os.Open(s.path(chunkNo, ".secondary"))
	if err != nil {
		return nil, nil, err
	}
	defer sec.Close()
	// This entry and, when present, the next one, whose offset bounds it.
	var buf [2 * secondaryIndexEntrySize]byte
	n, err = sec.ReadAt(buf[:], int64(start))
	if n < secondaryIndexEntrySize {
		return nil, nil, fmt.Errorf(
			"immutable store: secondary index of chunk %d is short: %w",
			chunkNo, err,
		)
	}
	entry := decodeSecEntry(buf[:])
	var endOffset uint64
	if n >= 2*secondaryIndexEntrySize {
		endOffset = decodeSecEntry(buf[secondaryIndexEntrySize:]).BlockOffset
	} else {
		st, err := os.Stat(s.path(chunkNo, ".chunk"))
		if err != nil {
			return nil, nil, err
		}
		endOffset = uint64(st.Size()) // #nosec G115
	}
	if endOffset <= entry.BlockOffset ||
		endOffset-entry.BlockOffset > uint64(maxBlockSize) {
		return nil, nil, fmt.Errorf(
			"%w: chunk %d entry spans %d..%d",
			ErrInvalidChunkOffset, chunkNo, entry.BlockOffset, endOffset,
		)
	}
	return &entry, &Location{
		Chunk:  chunkNo,
		Offset: entry.BlockOffset,
		Size:   uint32(endOffset - entry.BlockOffset), // #nosec G115 -- bounded
		CRC:    entry.Checksum,
	}, nil
}

// TruncateAfter removes every block after the point, which must be in the
// store. The point itself is kept.
func (s *Store) TruncateAfter(slot uint64, hash []byte) error {
	blk, loc, err := s.GetBlock(slot, hash)
	if err != nil {
		return err
	}
	if blk == nil || loc == nil {
		return fmt.Errorf(
			"immutable store: point at slot %d with hash %x not found; "+
				"refusing truncation", slot, hash,
		)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	nums, err := s.listChunks()
	if err != nil {
		return err
	}
	s.closeFiles()
	for i := len(nums) - 1; i >= 0 && nums[i] > loc.Chunk; i-- {
		if err := s.removeChunk(nums[i]); err != nil {
			return err
		}
	}
	// Cut the point's chunk right after the point: chunk and secondary first
	// so the primary, rewritten last, never claims more than they hold.
	end := loc.Offset + uint64(loc.Size)
	secOff := uint64(0)
	{
		sec, err := os.ReadFile(s.path(loc.Chunk, ".secondary"))
		if err != nil {
			return err
		}
		for o := uint64(0); o+secondaryIndexEntrySize <= uint64(len(sec)); o += secondaryIndexEntrySize {
			if decodeSecEntry(sec[o:]).BlockOffset == loc.Offset {
				secOff = o + secondaryIndexEntrySize
				break
			}
		}
	}
	if secOff == 0 {
		return errors.New(
			"immutable store: truncation point missing from secondary index",
		)
	}
	offs, err := readPrimaryOffsets(s.path(loc.Chunk, ".primary"))
	if err != nil {
		return err
	}
	rels := occupiedRels(offs)
	idx := secOff/secondaryIndexEntrySize - 1
	if idx >= uint64(len(rels)) {
		return errors.New(
			"immutable store: truncation point missing from primary index",
		)
	}
	keep := rels[idx] + 1
	if err := truncateFile(s.path(loc.Chunk, ".chunk"), end); err != nil {
		return err
	}
	if err := truncateFile(s.path(loc.Chunk, ".secondary"), secOff); err != nil {
		return err
	}
	pri := s.path(loc.Chunk, ".primary")
	if err := truncateFile(pri, 1+4*keep); err != nil {
		return err
	}
	f, err := os.OpenFile(pri, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	var word [4]byte
	binary.BigEndian.PutUint32(word[:], uint32(secOff)) // #nosec G115
	_, werr := f.WriteAt(word[:], int64(1+4*keep))      // #nosec G115
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		return err
	}
	if err := fsyncdir.Sync(s.dir); err != nil {
		return err
	}
	s.hasTip = false
	return s.recoverLocked()
}

// recoverLocked re-derives the append state from disk.
func (s *Store) recoverLocked() error {
	s.closeFiles()
	s.hasTip = false
	s.chunkSize, s.secSize, s.nextRel = 0, 0, 0
	return s.recover()
}

// TruncateAll removes every chunk file.
func (s *Store) TruncateAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeFiles()
	nums, err := s.listChunks()
	if err != nil {
		return err
	}
	for _, num := range slices.Backward(nums) {
		if err := s.removeChunk(num); err != nil {
			return err
		}
	}
	s.hasTip = false
	s.chunkSize, s.secSize, s.nextRel = 0, 0, 0
	return nil
}

// ReadBlockAt reads the block at loc from the chunk files in dir without a
// Store, which is all a reader holding a marker needs.
func ReadBlockAt(dir string, loc Location) (uint, []byte, error) {
	f, err := os.Open(
		filepath.Join(dir, ChunkName(loc.Chunk)+chunkFileExtension),
	)
	if err != nil {
		return 0, nil, fmt.Errorf("immutable store: open chunk: %w", err)
	}
	defer f.Close()
	return readWrapped(f, loc)
}
