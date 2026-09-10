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

package immutable

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type ImmutableDb struct {
	dataDir string
	// root, when set, is an open handle on the data directory that every
	// read resolves through instead of re-walking dataDir.
	//
	// This is what lets a caller that vetted the directory hand the vetted
	// directory itself across the API rather than a name for it. A name is
	// only ever a description of whatever occupies it at the moment it is
	// resolved, so a caller opening by name gets no guarantee that the tree
	// it checked is the tree that gets read. Mithril bootstrap needs that
	// guarantee, because the directory sits in a download area a concurrent
	// writer may reach and the decision to trust it was made earlier.
	root *os.Root
	// digests, when set, is the certified SHA-256 of every file this database
	// may read, keyed by the name directly beneath the data directory.
	//
	// A handle on the directory says which directory is read; it says nothing
	// about which bytes the files in it hold. Those are two separate
	// substitutions, and only the first is closed by holding the directory
	// open: a writer who cannot escape the directory can still rename a file
	// of their own over `00000.chunk` inside it, or write through the one
	// already there. So each file is read once and parsed from the bytes that
	// were hashed — see openEntry.
	digests map[string]string
	// maxChunk, when non-empty, is the highest chunk name this database will
	// expose. See NewFromRootVerified.
	maxChunk string
}

var ErrPointBeyondLastChunk = errors.New(
	"immutable DB: point is beyond the last chunk",
)

// ChunkName returns the name an immutable file number is stored under, without
// an extension: five digits, zero padded.
//
// Exported because callers that know a chunk by its number have to ask for it
// by name — the position of a name in a sorted listing is not its number
// whenever the range on disk does not start at zero, which is what a catch-up
// produces.
func ChunkName(num uint64) string {
	return fmt.Sprintf("%05d", num)
}

// isCanonicalChunkName reports whether name is exactly what ChunkName produces
// for some number: decimal digits, and padded the way ChunkName pads.
//
// Round-tripping rather than pattern matching, so the two can never drift: if
// ChunkName's format changes, this follows it.
func isCanonicalChunkName(name string) bool {
	num, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		return false
	}
	return ChunkName(num) == name
}

// ChunkNameAbove reports whether chunk name sorts after bound numerically.
//
// Not a plain string comparison. ChunkName pads to five digits, so names are a
// fixed width only below 100000 — past that they grow, and "99999" > "100000"
// as strings while 99999 < 100000 as numbers. A bound compared lexically would
// then hide the chunk just below it and admit the one just above, which is
// both of the things a bound exists to prevent. Longer means larger first, and
// only equal widths compare as text.
func ChunkNameAbove(name, bound string) bool {
	if len(name) != len(bound) {
		return len(name) > len(bound)
	}
	return name > bound
}

// ErrDigestMismatch reports a file whose contents are not the contents that
// were certified for it: either the digest of what was opened differs from the
// certified one, or nothing certified that name at all.
//
// The second is not a lesser case. A file the digest map does not cover is one
// nobody vouched for, and reading it because the map is silent would leave the
// map's coverage up to whoever added the file.
var ErrDigestMismatch = errors.New("immutable DB: file digest mismatch")

type Block struct {
	Hash  []byte
	Cbor  []byte
	Type  uint
	Slot  uint64
	IsEbb bool
}

// New returns a new ImmutableDb using the specified data directory or an error
func New(dataDir string) (*ImmutableDb, error) {
	if _, err := os.Stat(dataDir); err != nil {
		return nil, err
	}
	i := &ImmutableDb{
		dataDir: dataDir,
	}
	return i, nil
}

// NewFromRoot returns a new ImmutableDb that reads every file through root
// rather than by resolving the data directory's pathname again.
//
// Use this when the directory was vetted and the caller needs the reads to be
// about that directory rather than about whatever its name refers to later. The
// handle refers to the directory itself, so a tree substituted behind the name
// afterwards is not what gets read; New cannot offer that, because it only ever
// holds a name.
//
// The caller keeps ownership of root and must hold it open for as long as the
// returned ImmutableDb is used. Closing it early makes subsequent reads fail
// rather than silently fall back to the pathname.
func NewFromRoot(root *os.Root) (*ImmutableDb, error) {
	if root == nil {
		return nil, errors.New("immutable DB: nil data directory handle")
	}
	if _, err := root.Stat("."); err != nil {
		return nil, err
	}
	i := &ImmutableDb{
		// Kept for messages only. Every read below goes through root, so
		// this name is never resolved.
		dataDir: root.Name(),
		root:    root,
	}
	return i, nil
}

// NewFromRootVerified is NewFromRoot for a tree whose files are individually
// certified: every file is read once, checked against digests, and parsed from
// the bytes that were checked.
//
// NewFromRoot binds the reads to a directory. This binds them to the bytes.
// The two are not the same guarantee, and the difference is the whole point
// here: a Mithril bootstrap hashes each downloaded file when it lands, and
// whatever consumes it opens it again later. Between those, a writer who
// shares the download directory can rename a file of their own over the
// verified one without ever leaving the directory the handle refers to, and
// the second open reads what they wrote.
//
// Holding the descriptor open across both would answer the rename and nothing
// else. A descriptor names an inode, so writes made through the file it still
// refers to are visible to a reader that has merely rewound it — the check and
// the parse would be two readings of one mutable thing, and only the first
// would be the one that was compared against the digest. So there is only one
// reading: the entry is read into memory, the digest is computed over that
// buffer, and the parser walks the same buffer. Nothing that happens to the
// file afterwards is reachable from it.
//
// This holds one entry in memory at a time, bounded by the certified size of
// the largest file, which is what the guarantee costs. The file is read twice —
// once streaming, to learn that size without trusting it, and once into the
// buffer — so it is the parser's read that is removed, not one of the two.
//
// digests maps a name directly beneath the data directory ("00000.chunk") to
// its lowercase hex SHA-256. An empty or nil map is refused: it would verify
// nothing while looking as though it did.
//
// maxChunk, when non-empty, hides every chunk named above it (use ChunkName).
// The pipelined bootstrap copy reads a tree its download pool is still filling,
// and chunks arrive out of order — so a chunk above the contiguous prefix may
// be present and half written. The bound keeps the reader inside the prefix
// whose archives have been verified rather than failing on one that is merely
// unfinished. A name rather than a count, because a count would mean the same
// thing as a position, and a position is not a chunk number.
//
// The caller keeps ownership of root on the same terms as NewFromRoot.
func NewFromRootVerified(
	root *os.Root,
	digests map[string]string,
	maxChunk string,
) (*ImmutableDb, error) {
	if len(digests) == 0 {
		return nil, errors.New(
			"immutable DB: verified open requires a digest map",
		)
	}
	i, err := NewFromRoot(root)
	if err != nil {
		return nil, err
	}
	i.digests = digests
	i.maxChunk = maxChunk
	return i, nil
}

// entryPath names an entry in the data directory for use in messages. It is
// never opened when a root handle is held; see openEntry.
func (i *ImmutableDb) entryPath(name string) string {
	return filepath.Join(i.dataDir, name)
}

// openEntry opens a file directly beneath the data directory.
//
// Without digests the reads go to the descriptor, as an ordinary local
// ImmutableDB's always have.
//
// With digests the entry is checked, read, and then served from the bytes that
// were checked — the descriptor is closed before the parser runs and the parser
// never touches the file. Hashing a descriptor and then parsing through it does
// not establish that the parser saw certified bytes: rewinding does not detach
// the descriptor from its inode, and a writer who can modify that inode in
// place changes what the parser reads without changing anything the hash could
// have noticed. What closes that is not checking harder but giving the parser
// something nobody else can reach, which is a buffer whose own digest was
// compared. See readVerifiedEntry for why it takes two passes over the file to
// produce one safely.
func (i *ImmutableDb) openEntry(name string) (entryReader, error) {
	var f *os.File
	var err error
	if i.root != nil {
		f, err = i.root.Open(name)
	} else {
		f, err = os.Open(i.entryPath(name))
	}
	if err != nil {
		return nil, err
	}
	if i.digests == nil {
		return fileEntry{file: f}, nil
	}
	// Closed here rather than carried: once the bytes are in hand the
	// descriptor can only be a way to read them again, which is the thing
	// being removed.
	defer func() { _ = f.Close() }()
	return i.readVerifiedEntry(f, name)
}

// readVerifiedEntry returns an entry's bytes, but only if they are the
// certified ones, and only after establishing how many of them there may be.
//
// Two passes, and the first one is what makes the second safe to allocate for.
// How large a file is, is whoever wrote it's choice; how large the *certified*
// file is, is not. So the streaming pass measures and checks the entry without
// holding it, and the buffered pass then reads that many bytes and confirms the
// digest over the buffer it is about to hand the parser. Reading straight into
// memory instead would size an allocation from an untrusted file — a planted
// entry of any size at all would be materialised in full before its digest
// could be found wrong, which turns a refusal into an out-of-memory kill.
//
// The point of the second pass is not to check again but to check *the buffer*.
// A digest taken during the first pass describes what the descriptor held then;
// the parser reads the buffer, so the buffer is what has to be compared. That
// is also what makes an in-place write between the passes harmless rather than
// undetected: it changes the bytes, so it changes the digest, so it is refused.
//
// A file that grew between the passes is read only to its certified length, and
// what follows is never looked at. One that shrank fails to fill the buffer,
// which is a mismatch like any other.
func (i *ImmutableDb) readVerifiedEntry(
	f *os.File,
	name string,
) (entryReader, error) {
	expected, ok := i.digests[name]
	if !ok {
		return nil, fmt.Errorf(
			"%w: %s is not certified", ErrDigestMismatch, name,
		)
	}
	certifiedSize, err := i.measureVerifiedEntry(f, name, expected)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewinding %s: %w", i.entryPath(name), err)
	}
	data := make([]byte, certifiedSize)
	if _, err := io.ReadFull(f, data); err != nil {
		// Including ErrUnexpectedEOF: an entry that no longer holds as much as
		// the certified one is not the certified one.
		return nil, fmt.Errorf(
			"%w: re-reading %s: %w", ErrDigestMismatch, name, err,
		)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != expected {
		return nil, fmt.Errorf(
			"%w: %s computed %s, certified %s",
			ErrDigestMismatch, name, got, expected,
		)
	}
	return newBytesEntry(data), nil
}

// measureVerifiedEntry streams an entry past a hash, in constant memory,
// returning its length if it is the certified entry and an error if it is not.
//
// The length is worth having only because the digest matched: SHA-256 preimage
// resistance is what stops a planted file from reporting a size the caller then
// allocates. So the two answers come from one pass and neither is usable
// without the other.
func (i *ImmutableDb) measureVerifiedEntry(
	f *os.File,
	name string,
	expected string,
) (int64, error) {
	hasher := sha256.New()
	size, err := io.Copy(hasher, f)
	if err != nil {
		return 0, fmt.Errorf("hashing %s: %w", i.entryPath(name), err)
	}
	if sum := hex.EncodeToString(hasher.Sum(nil)); sum != expected {
		return 0, fmt.Errorf(
			"%w: %s computed %s, certified %s",
			ErrDigestMismatch, name, sum, expected,
		)
	}
	// Sizes are counted in int64 and indexed in int, which are the same width
	// on the platforms this runs on and are not everywhere. Stated rather than
	// assumed, so the buffer below cannot be asked for a length that does not
	// fit — and widening rather than round-tripping through int, so the
	// comparison says what it means on the platform where it can be false.
	if size > int64(math.MaxInt) {
		return 0, fmt.Errorf(
			"%w: %s is %d bytes, too large to read on this platform",
			ErrDigestMismatch, name, size,
		)
	}
	return size, nil
}

// removeEntry removes a file directly beneath the data directory.
func (i *ImmutableDb) removeEntry(name string) error {
	if i.root != nil {
		return i.root.Remove(name)
	}
	return os.Remove(i.entryPath(name))
}

// readDir lists the data directory.
func (i *ImmutableDb) readDir() ([]os.DirEntry, error) {
	if i.root != nil {
		return fs.ReadDir(i.root.FS(), ".")
	}
	return os.ReadDir(i.dataDir)
}

func (i *ImmutableDb) getChunkNames() ([]string, error) {
	ret := []string{}
	files, err := i.readDir()
	if err != nil {
		return nil, err
	}
	for _, entry := range files {
		entryName := entry.Name()
		entryExt := filepath.Ext(entryName)
		if entryExt != chunkFileExtension {
			continue
		}
		chunkName := strings.TrimSuffix(entryName, entryExt)
		// Only ChunkName's own output. The ordering below is numeric because
		// these names are canonical, and one that is not — differently
		// padded, or not a number at all — would sort by neither rule and
		// could take the last position, which is the tip.
		//
		// Dropped rather than refused, unlike the slot entries in a ledger
		// tree: there, ignoring a candidate selects another one, while a name
		// that is not a chunk name names no chunk at all. A verified database
		// refuses anything absent from its digest map in any case, so the
		// only reading that lets a planted file decide the tip is the one
		// that keeps it.
		if !isCanonicalChunkName(chunkName) {
			continue
		}
		ret = append(ret, chunkName)
	}
	// Numerically, not lexically. Names are a fixed width only below 100000,
	// and every lookup here works off this order — the tip is the last entry
	// and the point search bisects it — so a lexical sort past that width
	// would report the wrong tip and bisect a list that is not ordered.
	slices.SortFunc(ret, func(a, b string) int {
		if len(a) != len(b) {
			return cmp.Compare(len(a), len(b))
		}
		return strings.Compare(a, b)
	})
	// Bounded reads see a shorter database rather than a failing one; see
	// NewFromRootVerified.
	if i.maxChunk != "" {
		ret = slices.DeleteFunc(ret, func(name string) bool {
			return ChunkNameAbove(name, i.maxChunk)
		})
	}
	return ret, nil
}

func (i *ImmutableDb) getChunkNamesFromPoint(
	point ocommon.Point,
) ([]string, error) {
	chunkNames, err := i.getChunkNames()
	if err != nil {
		return nil, err
	}
	if len(chunkNames) == 0 {
		return nil, errors.New(
			"immutable DB: no chunk files found in data directory",
		)
	}
	// Return all chunks for the origin
	if point.Slot == 0 {
		return chunkNames, nil
	}
	lowerBound := 0
	upperBound := len(chunkNames) - 1
	for lowerBound <= upperBound {
		// Get chunk in the middle of the current bounds
		middlePoint := (lowerBound + upperBound) / 2
		middleChunkName := chunkNames[middlePoint]
		middleSecondary, err := i.getChunkSecondaryIndex(middleChunkName)
		if err != nil {
			return nil, err
		}
		defer func() { _ = middleSecondary.Close() }()
		next, err := middleSecondary.Next()
		if err != nil {
			return nil, err
		}
		if next == nil {
			break
		}
		startSlot := next.BlockOrEbb
		var endSlot uint64
		for {
			next, err := middleSecondary.Next()
			if err != nil {
				return nil, err
			}
			if next == nil {
				break
			}
			endSlot = next.BlockOrEbb
		}
		if point.Slot < startSlot {
			// The slot we're looking for is less than the first slot in the chunk, so
			// we can eliminate all later chunks
			upperBound = middlePoint - 1
		} else if point.Slot > endSlot {
			// The slot we're looking for is greater than the last slot in the chunk, so
			// we can eliminate all earlier chunks
			lowerBound = middlePoint + 1
		} else {
			// We found the chunk that (probably) has the requested point
			break
		}
	}
	if lowerBound >= len(chunkNames) {
		return nil, fmt.Errorf(
			"immutable DB: slot %d is beyond the last chunk: %w",
			point.Slot,
			ErrPointBeyondLastChunk,
		)
	}
	return chunkNames[lowerBound:], nil
}

func (i *ImmutableDb) getChunkPrimaryIndex(
	chunkName string,
) (*primaryIndex, error) {
	primaryFileName := chunkName + primaryFileExtension
	f, err := i.openEntry(primaryFileName)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to read primary index: %s: %w",
			i.entryPath(primaryFileName),
			err,
		)
	}
	primary := newPrimaryIndex()
	if err := primary.Open(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf(
			"failed to read primary index: %s: %w",
			i.entryPath(primaryFileName),
			err,
		)
	}
	return primary, nil
}

func (i *ImmutableDb) getChunkSecondaryIndex(
	chunkName string,
) (*secondaryIndex, error) {
	primary, err := i.getChunkPrimaryIndex(chunkName)
	if err != nil {
		return nil, err
	}
	secondaryFileName := chunkName + secondaryFileExtension
	f, err := i.openEntry(secondaryFileName)
	if err != nil {
		_ = primary.Close()
		return nil, fmt.Errorf(
			"failed to read secondary index: %s: %w",
			i.entryPath(secondaryFileName),
			err,
		)
	}
	secondary := newSecondaryIndex()
	if err := secondary.Open(f, primary); err != nil {
		_ = f.Close()
		_ = primary.Close()
		return nil, fmt.Errorf(
			"failed to read secondary index: %s: %w",
			i.entryPath(secondaryFileName),
			err,
		)
	}
	return secondary, nil
}

func (i *ImmutableDb) getChunk(chunkName string) (*chunk, error) {
	// Open secondary index
	secondary, err := i.getChunkSecondaryIndex(chunkName)
	if err != nil {
		return nil, err
	}
	// Open chunk
	chunkFileName := chunkName + chunkFileExtension
	f, err := i.openEntry(chunkFileName)
	if err != nil {
		_ = secondary.Close()
		return nil, fmt.Errorf(
			"failed to read chunk: %s: %w",
			i.entryPath(chunkFileName),
			err,
		)
	}
	chunk := newChunk()
	if err := chunk.Open(f, secondary); err != nil {
		_ = f.Close()
		_ = secondary.Close()
		return nil, fmt.Errorf(
			"failed to read chunk: %s: %w",
			i.entryPath(chunkFileName),
			err,
		)
	}
	return chunk, nil
}

func (i *ImmutableDb) GetTip() (*ocommon.Point, error) {
	var ret *ocommon.Point
	chunkNames, err := i.getChunkNames()
	if err != nil {
		return nil, err
	}
	if len(chunkNames) == 0 {
		return nil, nil
	}
	secondary, err := i.getChunkSecondaryIndex(chunkNames[len(chunkNames)-1])
	if err != nil {
		return nil, err
	}
	defer func() { _ = secondary.Close() }()
	var tmpPoint ocommon.Point
	for {
		next, err := secondary.Next()
		if err != nil {
			return nil, err
		}
		if next == nil {
			break
		}
		tmpPoint = ocommon.NewPoint(
			next.BlockOrEbb,
			next.HeaderHash[:],
		)
		ret = &tmpPoint
	}
	return ret, nil
}

// LastSlotInChunk returns the slot of the last block in the immutable file
// numbered num. The second return value is false when no chunk of that number
// is present, with no error. This bounds an incremental copy to a contiguous
// chunk prefix while later chunks may still be downloading out of order.
//
// By number, not by position in the sorted listing. The two coincide only when
// the range on disk starts at chunk 0, and a catch-up downloads only the
// archives above the import marker — so a position lookup would answer about a
// different chunk than the caller named, and bound the copy by the wrong slot.
func (i *ImmutableDb) LastSlotInChunk(
	num uint64,
) (uint64, bool, error) {
	chunkNames, err := i.getChunkNames()
	if err != nil {
		return 0, false, err
	}
	name := ChunkName(num)
	if !slices.Contains(chunkNames, name) {
		return 0, false, nil
	}
	secondary, err := i.getChunkSecondaryIndex(name)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = secondary.Close() }()
	var last uint64
	found := false
	for {
		next, err := secondary.Next()
		if err != nil {
			return 0, false, err
		}
		if next == nil {
			break
		}
		last = next.BlockOrEbb
		found = true
	}
	return last, found, nil
}

func (i *ImmutableDb) GetBlock(point ocommon.Point) (*Block, error) {
	var err error
	chunkNames, err := i.getChunkNamesFromPoint(point)
	if err != nil {
		return nil, err
	}
	chunk, err := i.getChunk(chunkNames[0])
	if err != nil {
		return nil, err
	}
	defer func() { _ = chunk.Close() }()
	var tmpBlock *Block
	for {
		tmpBlock, err = chunk.Next()
		if err != nil {
			return nil, err
		}
		if tmpBlock == nil {
			break
		}
		if tmpBlock.Slot != point.Slot {
			continue
		}
		if string(tmpBlock.Hash) != string(point.Hash) {
			continue
		}
		return tmpBlock, nil
	}
	return nil, nil
}

func (i *ImmutableDb) TruncateChunksFromPoint(point ocommon.Point) error {
	chunkNames, err := i.getChunkNamesFromPoint(point)
	if err != nil {
		return err
	}
	for _, chunkName := range chunkNames {
		if err := i.removeEntry(chunkName + chunkFileExtension); err != nil {
			return err
		}
		if err := i.removeEntry(chunkName + secondaryFileExtension); err != nil {
			return err
		}
		if err := i.removeEntry(chunkName + primaryFileExtension); err != nil {
			return err
		}
	}
	return nil
}

func (i *ImmutableDb) BlocksFromPoint(
	point ocommon.Point,
) (*BlockIterator, error) {
	chunkNames, err := i.getChunkNamesFromPoint(point)
	if err != nil {
		return nil, err
	}
	ret := &BlockIterator{
		db:         i,
		chunkNames: chunkNames,
		startPoint: point,
	}
	return ret, nil
}

type BlockIterator struct {
	db              *ImmutableDb
	chunk           *chunk
	startPoint      ocommon.Point
	chunkNames      []string
	chunkIdx        int
	foundStartPoint bool
}

func (b *BlockIterator) Next() (*Block, error) {
	var err error
	var tmpChunk *chunk
	if b.chunk == nil {
		if b.chunkIdx == 0 && len(b.chunkNames) > 0 {
			// Open initial chunk
			tmpChunk, err = b.db.getChunk(b.chunkNames[b.chunkIdx])
			if err != nil {
				return nil, err
			}
			b.chunk = tmpChunk
		} else {
			return nil, nil
		}
	}
	var tmpBlock *Block
	for {
		tmpBlock, err = b.chunk.Next()
		if err != nil {
			closeErr := b.chunk.Close()
			b.chunk = nil
			return nil, errors.Join(err, closeErr)
		}
		if tmpBlock == nil {
			// We've reached the end of the current chunk
			if err := b.chunk.Close(); err != nil {
				return nil, err
			}
			b.chunk = nil
			b.chunkIdx++
			if b.chunkIdx >= len(b.chunkNames) {
				return nil, nil
			}
			tmpChunk, err = b.db.getChunk(b.chunkNames[b.chunkIdx])
			if err != nil {
				return nil, err
			}
			b.chunk = tmpChunk
			continue
		}
		if !b.foundStartPoint {
			if tmpBlock.Slot < b.startPoint.Slot {
				continue
			}
			b.foundStartPoint = true
		}
		return tmpBlock, nil
	}
}

func (b *BlockIterator) Close() error {
	if b.chunk != nil {
		if err := b.chunk.Close(); err != nil {
			return err
		}
	}
	return nil
}
