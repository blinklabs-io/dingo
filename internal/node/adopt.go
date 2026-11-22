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
	// OnChunkCommitted, when set, is called once every block of a group of
	// chunks has committed and the blob store is synced, with the number of
	// the next chunk to adopt. Every chunk below it is in the ImmutableDB.
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
// instead of copying their block bodies into the blob store. It takes chunks
// in order, in groups of up to adoptGroupChunks: it moves each chunk's chunk,
// primary and secondary files into the database's ImmutableDB directory and
// syncs the group's files and the directory, then for each chunk re-reads the
// files against the certified digests, advances the recorded tip and commits
// the chunk's index entries, markers and UTxO offsets. The chunk files are in
// place and durable before any marker that names them exists.
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
	case hasChain && chainTip.Point.Slot > st.TipSlot:
		// Blocks past the recorded tip were stored with their bodies.
		// Adopting a later chunk would move the tip over them, below where
		// promotion resumes, so they would keep their bodies for good.
		for _, num := range chunks {
			if num > tipChunk {
				return nil, ErrAdoptUnavailable
			}
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
	var pending []uint64
	for _, num := range chunks {
		if st.HasTip && (num < tipChunk || (num == tipChunk &&
			chunkCommitted(db, c, st))) {
			// Committed by an earlier run; only a duplicate left behind by a
			// copy that was interrupted before its source was removed remains.
			removeChunkFiles(cfg.SourceDir, num)
			continue
		}
		pending = append(pending, num)
	}
	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		group := pending[:min(len(pending), adoptGroupChunks)]
		pending = pending[len(group):]
		// Record the group before any file moves: from the first rename until
		// the tip covers a chunk, reconciliation would read its files as
		// bytes past the recorded tip and cut them. The record stays until
		// the tip covers the whole group.
		if err := db.BeginImmutableAdopt(group[0]); err != nil {
			return nil, err
		}
		for _, num := range group {
			if err := moveChunkFilesIn(
				cfg.SourceDir, destDir, num, move,
			); err != nil {
				return nil, err
			}
		}
		// Every file must be durable, under its new name, before a tip that
		// covers it is recorded. One directory sync covers the group's
		// renames; per-chunk directory syncs were most of the import's time.
		for _, num := range group {
			if err := syncChunkFileData(destDir, num); err != nil {
				return nil, err
			}
		}
		if err := fsyncdir.Sync(destDir); err != nil {
			return nil, err
		}
		for i, num := range group {
			last := i == len(group)-1
			n, lastBlock, err := adoptGroupChunk(
				ctx, db, c, dest, destDir, num, geom, callback, cfg, last,
				func(copied int, slot uint64) {
					reportLoadBlobsProgress(
						cfg.OnProgress, blocksCopied+copied, slot,
						progressTip, startTime,
					)
				},
			)
			if err != nil {
				// The chunks after this one were moved in under the group's
				// record but no tip covers them. Return them to the source
				// so the failed chunk is undone exactly as it would be alone.
				if rerr := moveChunksBack(
					cfg.SourceDir, destDir, group[i+1:], move,
				); rerr != nil {
					return nil, errors.Join(err, rerr)
				}
				// The chunks before it committed; finish them as a group
				// of their own so they are synced and released.
				if ferr := finishAdoptedGroup(
					db, cfg, group[:i],
				); ferr != nil {
					return nil, errors.Join(err, ferr)
				}
				return nil, discardAdoptedChunk(db, destDir, num, geom, err)
			}
			blocksCopied += n
			adopted = true
			if lastBlock != nil {
				lastSlot = lastBlock.Slot
			}
		}
		if err := finishAdoptedGroup(db, cfg, group); err != nil {
			return nil, err
		}
		st, _ = db.ImmutableState()
	}
	if err := db.ClearImmutableAdopt(); err != nil {
		return nil, err
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
	clearRecord bool,
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
	// The caller made the chunk's files and their names durable before
	// calling, so no marker below can name a file a crash would lose.
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
	// The recorded tip now covers the chunk, so reconciliation keeps its
	// files. A chunk with no blocks records no tip and its files hold nothing
	// for reconciliation to cut. In a group the record also covers the later
	// chunks' files, so only the last chunk clears it.
	if clearRecord {
		if err := db.ClearImmutableAdopt(); err != nil {
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

func removeChunkFiles(dir string, num uint64) {
	for _, ext := range chunkFileExts {
		_ = os.Remove(filepath.Join(dir, chunkFileName(num, ext)))
	}
}

// moveChunkFile renames src to dst, copying through a temporary name when the
// two are on different file systems so dst never appears partly written.
func moveChunkFile(src, dst string) error {
	err := os.Rename(src, dst)
	if err == nil || !errors.Is(err, syscall.EXDEV) {
		return err
	}
	if err := copyChunkFile(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}

// copyChunkFile copies src to dst through a temporary name. It closes src
// before returning, since Windows refuses to remove a file a handle still
// holds open.
func copyChunkFile(src, dst string) error {
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
	return nil
}

// adoptGroupChunks bounds how many chunks share one adopt record, one
// directory sync and one blob-store sync. A crash redoes at most this many.
const adoptGroupChunks = 64

// adoptGroupChunk verifies and publishes one chunk of a group, then adopts
// it. Only the group's last chunk clears the adopt record: until then later
// chunks' files sit past the recorded tip.
func adoptGroupChunk(
	ctx context.Context,
	db *database.Database,
	c *chain.Chain,
	dest *immutable.ImmutableDb,
	destDir string,
	num uint64,
	geom immutable.Geometry,
	callback func(chain.RawBlock, *database.Txn) error,
	cfg AdoptChunksConfig,
	clearRecord bool,
	progress func(copied int, slot uint64),
) (int, *immutable.Block, error) {
	if db.ImmutableChunksPublished() {
		// Publication uploads whatever the staged files hold, and the
		// object store keeps it whether or not the adoption goes on. Check
		// the files first so bytes that are not certified are not
		// published at all; adoptChunk still verifies the bytes it parses.
		if err := dest.VerifyChunkFiles(num); err != nil {
			return 0, nil, fmt.Errorf("verifying chunk %d: %w", num, err)
		}
		if err := db.PublishImmutableChunk(num); err != nil {
			return 0, nil, fmt.Errorf("publishing chunk %d: %w", num, err)
		}
	}
	n, last, err := adoptChunk(
		ctx, db, c, dest, destDir, num, geom, callback,
		cfg.BeforeCommit, progress, clearRecord,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("adopting chunk %d: %w", num, err)
	}
	return n, last, nil
}

// discardAdoptedChunk undoes a chunk that failed before its markers could
// commit, once the later chunks of its group are back in the source. The
// recorded tip is read afresh: adoptChunk moves it over the chunk before the
// first marker commits, and from then on, as for a chunk an earlier run
// stopped inside, committed markers can name the chunk's local and published
// files, so both stay. No file then sits past the tip, so the adopt record
// is cleared, as a chunk adopted alone clears it once its tip is recorded.
func discardAdoptedChunk(
	db *database.Database,
	destDir string,
	num uint64,
	geom immutable.Geometry,
	err error,
) error {
	cur, _ := db.ImmutableState()
	if cur.HasTip && num <= cur.TipSlot/geom.ChunkSlots {
		if cerr := db.ClearImmutableAdopt(); cerr != nil {
			return errors.Join(err, cerr)
		}
		return err
	}
	if uerr := db.UnpublishImmutableChunk(num); uerr != nil {
		return errors.Join(err, fmt.Errorf(
			"removing published chunk %d: %w", num, uerr,
		))
	}
	if errors.Is(err, immutable.ErrDigestMismatch) {
		// Files that are not the certified ones are not worth keeping for
		// a later run to resume with.
		removeChunkFiles(destDir, num)
		if cerr := db.ClearImmutableAdopt(); cerr != nil {
			return cerr
		}
	}
	return err
}

// moveChunkFilesIn moves a chunk's certified files into destDir without
// syncing the directory; the caller syncs it once for the group.
func moveChunkFilesIn(
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
	return nil
}

// moveChunksBack returns chunks' files from destDir to srcDir and syncs both
// directories, so no file of them remains past the recorded tip.
func moveChunksBack(
	srcDir, destDir string,
	nums []uint64,
	move func(src, dst string) error,
) error {
	if len(nums) == 0 {
		return nil
	}
	for _, num := range nums {
		for _, ext := range chunkFileExts {
			name := chunkFileName(num, ext)
			err := move(
				filepath.Join(destDir, name),
				filepath.Join(srcDir, name),
			)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("returning %s: %w", name, err)
			}
		}
	}
	if err := fsyncdir.Sync(destDir); err != nil {
		return err
	}
	return fsyncdir.Sync(srcDir)
}

// syncChunkFileData syncs a chunk's three files but not their directory.
func syncChunkFileData(dir string, num uint64) error {
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
	return nil
}

// finishAdoptedGroup makes committed chunks durable in the blob store, then
// releases their local copies, removes their sources and reports them.
func finishAdoptedGroup(
	db *database.Database,
	cfg AdoptChunksConfig,
	group []uint64,
) error {
	if len(group) == 0 {
		return nil
	}
	blobStore := db.Blob()
	if blobStore == nil {
		return datatypes.ErrBlobStoreUnavailable
	}
	lastNum := group[len(group)-1]
	if err := blobStore.Sync(); err != nil {
		return fmt.Errorf("syncing blob store after chunk %d: %w", lastNum, err)
	}
	for _, num := range group {
		if err := db.ReleaseImmutableChunk(num); err != nil {
			return fmt.Errorf("releasing local chunk %d: %w", num, err)
		}
		removeChunkFiles(cfg.SourceDir, num)
	}
	if cfg.OnChunkCommitted != nil {
		return cfg.OnChunkCommitted(lastNum + 1)
	}
	return nil
}
