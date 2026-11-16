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

package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	datatypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/fsyncdir"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ErrAdoptUnavailable reports a database whose existing blocks cannot be
// joined to adopted chunk files: it already holds blocks that are not in the
// ImmutableDB, so the caller falls back to copying block bodies.
var ErrAdoptUnavailable = errors.New(
	"database holds blocks outside the ImmutableDB",
)

// ErrAdoptGeometry reports a block that does not sit in the chunk the
// configured geometry assigns it, which would leave the chunk files unreadable
// to the writer that appends after them.
var ErrAdoptGeometry = errors.New("chunk file does not match the geometry")

var chunkFileExts = [...]string{".chunk", ".primary", ".secondary"}

// AdoptChunksConfig configures AdoptImmutableChunks.
type AdoptChunksConfig struct {
	// SourceDir holds the certified chunk trios.
	SourceDir string
	// Digests is the certified SHA-256 of every file by name ("00000.chunk").
	// Only chunks whose three files are all certified are adopted; anything
	// else in SourceDir, such as a partial chunk that was never certified, is
	// left alone.
	Digests map[string]string
	// OnChunkCommitted, when set, is called after every block of a chunk has
	// committed and the blob store is synced, with the number of the next
	// chunk to adopt. Every chunk below it is in the ImmutableDB.
	OnChunkCommitted func(firstUnadopted uint64) error
	// OnProgress receives progress after each flushed batch.
	OnProgress func(LoadBlobsProgress)
	// BeforeCommit, when set, is called with a chunk's number once its files
	// are verified and durable in the ImmutableDB directory and before any
	// block of it commits. A non-nil result stops the import there. It exists
	// so a test can observe or interrupt that point.
	BeforeCommit func(chunk uint64) error
	// MoveFile moves src to dst. Nil uses a rename that falls back to a copy
	// across file systems. It exists so a test can interrupt or tamper with a
	// move.
	MoveFile func(src, dst string) error
}

// AdoptImmutableChunks takes verified Mithril chunk files into the ImmutableDB
// instead of copying their block bodies into the blob store. For each chunk, in
// order, it moves the chunk, primary and secondary files into the database's
// ImmutableDB directory, re-reads them there against the certified digests,
// syncs them, and only then commits that chunk's index entries, markers and
// UTxO offsets and advances the recorded tip. The chunk files are in place and
// durable before any marker that names them exists.
//
// It resumes: a chunk that is already in the ImmutableDB directory is verified
// rather than moved again, a chain that stops inside a chunk continues from
// there, and a chunk whose files were left in both places is taken from the
// ImmutableDB directory. The caller holds Database.LockImmutable.
//
// ErrAdoptUnavailable means nothing was changed and the caller should copy
// bodies instead.
func AdoptImmutableChunks(
	ctx context.Context,
	logger *slog.Logger,
	db *database.Database,
	cfg AdoptChunksConfig,
) (*LoadBlobsResult, error) {
	st, ok := db.ImmutableState()
	if !ok {
		return nil, database.ErrImmutableNotEnabled
	}
	geom := st.Geometry
	cm, err := chain.NewManager(ctx, db, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load chain manager: %w", err)
	}
	c := cm.PrimaryChain()
	if c == nil {
		return nil, errors.New("primary chain not available")
	}
	chainTip := c.Tip()
	hasChain := chainTip.Point.Slot != 0 || len(chainTip.Point.Hash) != 0
	chunks := certifiedChunks(cfg.Digests)
	if len(chunks) == 0 {
		return nil, errors.New("no certified chunk files to adopt")
	}
	tipChunk := st.TipSlot / geom.ChunkSlots
	switch {
	case hasChain && !st.HasTip:
		return nil, ErrAdoptUnavailable
	case (!hasChain && st.HasTip) ||
		(hasChain && chainTip.Point.Slot < st.TipSlot):
		// The recorded tip is recorded before a chunk's blocks commit, so an
		// interrupted run leaves the chain behind it. That is only a pending
		// adoption when the chunk the tip ends is one being adopted.
		if !slices.Contains(chunks, tipChunk) {
			return nil, fmt.Errorf(
				"immutable tip is slot %d but the chain tip is slot %d and "+
					"chunk %d is not being adopted",
				st.TipSlot, chainTip.Point.Slot, tipChunk,
			)
		}
	}

	destDir := db.ImmutableDir()
	if err := os.MkdirAll(destDir, 0o755); err != nil { // #nosec G301
		return nil, fmt.Errorf("creating immutable directory: %w", err)
	}
	destRoot, err := os.OpenRoot(destDir)
	if err != nil {
		return nil, fmt.Errorf("opening immutable directory: %w", err)
	}
	defer destRoot.Close()
	dest, err := immutable.NewFromRootVerified(destRoot, cfg.Digests, "")
	if err != nil {
		return nil, fmt.Errorf("opening immutable directory: %w", err)
	}
	move := cfg.MoveFile
	if move == nil {
		move = moveChunkFile
	}

	var callback func(chain.RawBlock, *database.Txn) error
	if db.StorageMode() != datatypes.StorageModeCore {
		callback = func(rb chain.RawBlock, txn *database.Txn) error {
			_, err := storeRawBlockUtxoOffsets(txn, rb)
			return err
		}
	}

	var (
		blocksCopied int
		startTime    = time.Now()
		lastSlot     = st.TipSlot
		lastChunk    = chunks[len(chunks)-1]
		progressTip  = (lastChunk + 1) * geom.ChunkSlots
		adopted      = false
	)
	for _, num := range chunks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if st.HasTip && (num < tipChunk || (num == tipChunk &&
			chunkCommitted(db, c, st))) {
			// Committed by an earlier run; only a duplicate left behind by a
			// copy that was interrupted before its source was removed remains.
			removeChunkFiles(cfg.SourceDir, num)
			continue
		}
		if err := moveChunkIn(cfg.SourceDir, destDir, num, move); err != nil {
			return nil, err
		}
		n, last, err := adoptChunk(
			ctx, db, c, dest, destDir, num, geom, callback,
			cfg.BeforeCommit,
			func(copied int, slot uint64) {
				reportLoadBlobsProgress(
					cfg.OnProgress, blocksCopied+copied, slot,
					progressTip, startTime,
				)
			},
		)
		if err != nil {
			if errors.Is(err, immutable.ErrDigestMismatch) {
				// Nothing committed for this chunk, so its files are not
				// covered by any marker and are not worth keeping.
				removeChunkFiles(destDir, num)
			}
			return nil, fmt.Errorf("adopting chunk %d: %w", num, err)
		}
		blocksCopied += n
		adopted = true
		if last != nil {
			lastSlot = last.Slot
		}
		blobStore := db.Blob()
		if blobStore == nil {
			return nil, datatypes.ErrBlobStoreUnavailable
		}
		if err := blobStore.Sync(); err != nil {
			return nil, fmt.Errorf(
				"syncing blob store after chunk %d: %w", num, err,
			)
		}
		removeChunkFiles(cfg.SourceDir, num)
		if cfg.OnChunkCommitted != nil {
			if err := cfg.OnChunkCommitted(num + 1); err != nil {
				return nil, err
			}
		}
		st, _ = db.ImmutableState()
	}
	if adopted && callback != nil && blocksCopied > 0 {
		if err := markImmutableUtxoOffsetsComplete(db, lastSlot); err != nil {
			return nil, err
		}
	}
	logger.Info(
		"adopted ImmutableDB chunks",
		"blocks", blocksCopied,
		"chunks", len(chunks),
		"tip_slot", lastSlot,
	)
	return &LoadBlobsResult{
		BlocksCopied:     blocksCopied,
		ImmutableTipSlot: lastSlot,
	}, nil
}

// chunkCommitted reports whether every block through the recorded tip is in
// the chain. Blocks added after an adoption, such as gap blocks, put the chain
// tip past the recorded tip, so only an index below it means the chunk the tip
// ends is unfinished. A tip that cannot be resolved counts as not committed,
// leaving the adoption to report the problem.
func chunkCommitted(
	db *database.Database,
	c *chain.Chain,
	st database.ImmutableState,
) bool {
	idx, err := chainTipIndex(db, c)
	return err == nil && idx >= st.TipIndex
}

// primaryIndexSize is the byte length of a completed chunk's primary index
// under geom: a version byte and one offset for every relative slot plus the
// final one, where a geometry that can hold an epoch boundary block has a
// relative slot for it.
func primaryIndexSize(geom immutable.Geometry) int64 {
	slots := geom.ChunkSlots
	if geom.CanContainEBB {
		slots++
	}
	return 1 + 4*int64(slots+1) // #nosec G115 -- bounded by Geometry.Valid
}

// adoptChunk reads chunk num from the ImmutableDB directory, which verifies
// its three files against the certified digests, commits its blocks as
// markers and records the tip. It returns the number of blocks added and the
// last block of the chunk.
func adoptChunk(
	ctx context.Context,
	db *database.Database,
	c *chain.Chain,
	dest *immutable.ImmutableDb,
	destDir string,
	num uint64,
	geom immutable.Geometry,
	callback func(chain.RawBlock, *database.Txn) error,
	beforeCommit func(chunk uint64) error,
	progress func(copied int, slot uint64),
) (int, *immutable.Block, error) {
	// Slot ranges alone cannot tell a geometry with an epoch boundary slot
	// from one without, and a primary index of the wrong shape would leave
	// every later lookup in the chunk addressing the wrong entry.
	pri, err := os.Stat(filepath.Join(destDir, chunkFileName(num, ".primary")))
	if err != nil {
		return 0, nil, err
	}
	if want := primaryIndexSize(geom); pri.Size() != want {
		return 0, nil, fmt.Errorf(
			"%w: primary index of chunk %d is %d bytes, geometry gives %d",
			ErrAdoptGeometry, num, pri.Size(), want,
		)
	}
	iter, err := dest.BlocksInChunk(num)
	if err != nil {
		return 0, nil, err
	}
	defer iter.Close()
	chainTip := c.Tip()
	skipTo := ocommon.Point{}
	if chainTip.Point.Slot >= num*geom.ChunkSlots &&
		(chainTip.Point.Slot != 0 || len(chainTip.Point.Hash) != 0) {
		if chainTip.Point.Slot >= (num+1)*geom.ChunkSlots {
			return 0, nil, fmt.Errorf(
				"chain tip slot %d is past chunk %d",
				chainTip.Point.Slot, num,
			)
		}
		skipTo = chainTip.Point
	}
	skipping := len(skipTo.Hash) != 0
	// Read and check the whole chunk before committing any of it, so a chunk
	// the geometry does not describe leaves nothing behind.
	var (
		raws      []chain.RawBlock
		last      *immutable.Block
		synced    bool
		wantIndex uint64
	)
	for {
		b, err := iter.Next()
		if err != nil {
			return 0, nil, fmt.Errorf("reading next block: %w", err)
		}
		if b == nil {
			break
		}
		if !synced {
			// The first block read means all three files matched their
			// digests; make them durable before any marker names them.
			if err := syncChunkFiles(destDir, num); err != nil {
				return 0, nil, err
			}
			synced = true
		}
		last = b
		rb, err := rawBlockFromImmutableBlock(b)
		if err != nil {
			return 0, nil, fmt.Errorf("building raw block: %w", err)
		}
		if rb.Slot/geom.ChunkSlots != num {
			return 0, nil, fmt.Errorf(
				"%w: block at slot %d is in chunk %d, geometry places it in %d",
				ErrAdoptGeometry, rb.Slot, num, rb.Slot/geom.ChunkSlots,
			)
		}
		if skipping {
			if bytes.Equal(rb.Hash, skipTo.Hash) {
				skipping = false
			}
			continue
		}
		rb.Immutable = &datatypes.ImmutableMarker{
			Chunk: num, Offset: b.Offset, Size: b.Size, CRC: b.CRC,
		}
		raws = append(raws, rb)
	}
	if skipping {
		return 0, nil, fmt.Errorf(
			"chain tip %x is not in chunk %d", skipTo.Hash, num,
		)
	}
	if !synced {
		// A chunk with no blocks still has to be durable; the iterator has
		// already checked its digests.
		if err := syncChunkFiles(destDir, num); err != nil {
			return 0, nil, err
		}
	}
	if beforeCommit != nil {
		if err := beforeCommit(num); err != nil {
			return 0, nil, err
		}
	}
	var lastRaw chain.RawBlock
	if last != nil {
		lastRaw, err = rawBlockFromImmutableBlock(last)
		if err != nil {
			return 0, nil, fmt.Errorf("building raw block: %w", err)
		}
		// Record the chunk's last block as the tip before any of its markers
		// commit. Reconciliation at startup cuts chunk bytes beyond the
		// recorded tip, and the blocks of an interrupted chunk are still
		// waiting for their markers, so the tip has to cover the whole chunk
		// first. Block indexes are consecutive, so the last one is the chain
		// tip's index plus the blocks still to add.
		tipIndex, err := chainTipIndex(db, c)
		if err != nil {
			return 0, nil, err
		}
		wantIndex = tipIndex + uint64(len(raws))
		if err := db.AdoptImmutableTip(
			lastRaw.Slot, lastRaw.Hash, wantIndex,
		); err != nil {
			return 0, nil, err
		}
	}
	var added int
	for len(raws) > 0 {
		n := min(len(raws), loadBlockBatchSize)
		if err := c.AddRawBlocksWithCallback(ctx, raws[:n], callback); err != nil {
			return added, nil, fmt.Errorf("failed to import block: %w", err)
		}
		added += n
		progress(added, raws[n-1].Slot)
		raws = raws[n:]
	}
	if last == nil {
		return added, nil, nil
	}
	txn := db.BlobTxn(false)
	id, err := database.BlockIDByPointMetadataTxn(
		txn, ocommon.NewPoint(lastRaw.Slot, lastRaw.Hash),
	)
	txn.Rollback() //nolint:errcheck
	if err != nil {
		return added, nil, fmt.Errorf("resolving last block of chunk: %w", err)
	}
	if id != wantIndex {
		return added, nil, fmt.Errorf(
			"last block of chunk %d has index %d, recorded tip index %d",
			num, id, wantIndex,
		)
	}
	last.Slot = lastRaw.Slot
	return added, last, nil
}

// chainTipIndex returns the block index of the chain's tip, zero when empty.
func chainTipIndex(db *database.Database, c *chain.Chain) (uint64, error) {
	tip := c.Tip().Point
	if tip.Slot == 0 && len(tip.Hash) == 0 {
		return 0, nil
	}
	txn := db.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	id, err := database.BlockIDByPointMetadataTxn(txn, tip)
	if err != nil {
		return 0, fmt.Errorf("resolving chain tip index: %w", err)
	}
	return id, nil
}

// certifiedChunks lists, in order, the chunk numbers whose chunk, primary and
// secondary files are all certified.
func certifiedChunks(digests map[string]string) []uint64 {
	have := map[uint64]int{}
	for name := range digests {
		num, ok := immutable.ParseFileName(name)
		if !ok {
			continue
		}
		have[num]++
	}
	var nums []uint64
	for num, n := range have {
		if n == len(chunkFileExts) {
			nums = append(nums, num)
		}
	}
	slices.Sort(nums)
	return nums
}

func chunkFileName(num uint64, ext string) string {
	return immutable.ChunkName(num) + ext
}

// moveChunkIn moves chunk num's files that are not already in destDir.
func moveChunkIn(
	srcDir, destDir string,
	num uint64,
	move func(src, dst string) error,
) error {
	for _, ext := range chunkFileExts {
		name := chunkFileName(num, ext)
		dst := filepath.Join(destDir, name)
		if _, err := os.Lstat(dst); err == nil {
			continue
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		src := filepath.Join(srcDir, name)
		if err := move(src, dst); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf(
					"certified chunk file %s is missing from %s and %s: %w",
					name, srcDir, destDir, err,
				)
			}
			return fmt.Errorf("moving %s: %w", name, err)
		}
	}
	return fsyncdir.Sync(destDir)
}

func removeChunkFiles(dir string, num uint64) {
	for _, ext := range chunkFileExts {
		_ = os.Remove(filepath.Join(dir, chunkFileName(num, ext)))
	}
}

func syncChunkFiles(dir string, num uint64) error {
	for _, ext := range chunkFileExts {
		f, err := os.OpenFile(
			filepath.Join(dir, chunkFileName(num, ext)), os.O_RDWR, 0,
		)
		if err != nil {
			return err
		}
		err = f.Sync()
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return fmt.Errorf("syncing %s: %w", chunkFileName(num, ext), err)
		}
	}
	return fsyncdir.Sync(dir)
}

// moveChunkFile renames src to dst, copying through a temporary name when the
// two are on different file systems so dst never appears partly written.
func moveChunkFile(src, dst string) error {
	err := os.Rename(src, dst)
	if err == nil || !errors.Is(err, syscall.EXDEV) {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(
		tmp,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0o644,
	) // #nosec G302
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Remove(src)
}
