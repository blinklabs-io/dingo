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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
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
	// of their own over `00000.chunk` inside it. So each file is verified from
	// the descriptor the read then goes through — see openEntry.
	digests map[string]string
	// chunkLimit, when non-zero, bounds the database to the first chunkLimit
	// chunk names. See NewFromRootVerified.
	chunkLimit uint64
}

var ErrPointBeyondLastChunk = errors.New(
	"immutable DB: point is beyond the last chunk",
)

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
// certified: every file is checked against digests as it is opened, and the
// read then goes through that same open descriptor.
//
// NewFromRoot binds the reads to a directory. This binds them to the bytes.
// The two are not the same guarantee, and the difference is the whole point
// here: a Mithril bootstrap hashes each downloaded file when it lands, and
// whatever consumes it opens it again later. Between those, a writer who
// shares the download directory can rename a file of their own over the
// verified one without ever leaving the directory the handle refers to, and
// the second open reads what they wrote. Verifying at the open the reader
// keeps closes that, because a rename afterwards does not reach a descriptor
// that is already open.
//
// digests maps a name directly beneath the data directory ("00000.chunk") to
// its lowercase hex SHA-256. An empty or nil map is refused: it would verify
// nothing while looking as though it did.
//
// chunkLimit, when non-zero, bounds the database to the first chunkLimit chunk
// names. The pipelined bootstrap copy reads a tree its download pool is still
// filling, and chunks arrive out of order — so a chunk above the contiguous
// prefix may be present and half written. The bound keeps the reader inside
// the prefix whose archives have been verified rather than failing on one that
// is merely unfinished.
//
// The caller keeps ownership of root on the same terms as NewFromRoot.
func NewFromRootVerified(
	root *os.Root,
	digests map[string]string,
	chunkLimit uint64,
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
	i.chunkLimit = chunkLimit
	return i, nil
}

// entryPath names an entry in the data directory for use in messages. It is
// never opened when a root handle is held; see openEntry.
func (i *ImmutableDb) entryPath(name string) string {
	return filepath.Join(i.dataDir, name)
}

// openEntry opens a file directly beneath the data directory.
//
// When the database was opened with digests, the returned descriptor is one
// whose contents have been confirmed to be the certified contents. Everything
// downstream reads through it, so no name is resolved a second time between
// the check and the read.
func (i *ImmutableDb) openEntry(name string) (*os.File, error) {
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
		return f, nil
	}
	if err := i.verifyEntry(f, name); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// verifyEntry hashes an open entry and compares it with the certified digest
// for its name, leaving the descriptor rewound for the caller to read.
func (i *ImmutableDb) verifyEntry(f *os.File, name string) error {
	expected, ok := i.digests[name]
	if !ok {
		return fmt.Errorf("%w: %s is not certified", ErrDigestMismatch, name)
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return fmt.Errorf("hashing %s: %w", i.entryPath(name), err)
	}
	if sum := hex.EncodeToString(hasher.Sum(nil)); sum != expected {
		return fmt.Errorf(
			"%w: %s computed %s, certified %s",
			ErrDigestMismatch, name, sum, expected,
		)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewinding %s: %w", i.entryPath(name), err)
	}
	return nil
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
		ret = append(ret, chunkName)
	}
	slices.Sort(ret)
	// Bounded reads see a shorter database rather than a failing one; see
	// NewFromRootVerified.
	if i.chunkLimit > 0 && uint64(len(ret)) > i.chunkLimit {
		ret = ret[:i.chunkLimit]
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

// LastSlotInChunk returns the slot of the last block in the chunk at the
// given 0-based index. The second return value is false when num is beyond
// the chunks currently present, with no error. This bounds an incremental
// copy to a contiguous chunk prefix while later chunks may still be
// downloading out of order: chunks 0..num are known complete, so num maps
// to the num-th sorted chunk name.
func (i *ImmutableDb) LastSlotInChunk(
	num uint64,
) (uint64, bool, error) {
	chunkNames, err := i.getChunkNames()
	if err != nil {
		return 0, false, err
	}
	if num >= uint64(len(chunkNames)) {
		return 0, false, nil
	}
	secondary, err := i.getChunkSecondaryIndex(chunkNames[num])
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
