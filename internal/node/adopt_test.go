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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	datatypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// adoptGeometry is the geometry production derives for the preview network,
// which the shared fixture chunks come from: 4320 slots per chunk and a slot
// for an epoch boundary block in every primary index.
func adoptGeometry(t *testing.T) database.ImmutableConfig {
	t.Helper()
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	slots, ebb, err := nodeCfg.ImmutableChunkGeometry()
	require.NoError(t, err)
	return database.ImmutableConfig{
		Enabled: true, ChunkSlots: slots, CanContainEBB: ebb,
	}
}

// adoptSource copies the first n fixture chunks into a directory and returns it
// with the SHA-256 of every file, the way a certified tree is described.
func adoptSource(t *testing.T, n int) (string, map[string]string) {
	t.Helper()
	dir := copyImmutableChunkPrefix(t, n)
	return dir, adoptDigests(t, dir)
}

func adoptDigests(t *testing.T, dir string) map[string]string {
	t.Helper()
	files, err := immutable.ListFiles(dir)
	require.NoError(t, err)
	digests := make(map[string]string, len(files))
	for _, f := range files {
		data, err := os.ReadFile(filepath.Join(dir, f.Name))
		require.NoError(t, err)
		sum := sha256.Sum256(data)
		digests[f.Name] = hex.EncodeToString(sum[:])
	}
	return digests
}

func newAdoptDB(t *testing.T, mode string) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		Logger:      slog.New(slog.DiscardHandler),
		StorageMode: mode,
		Immutable:   adoptGeometry(t),
	})
	require.NoError(t, err)
	return db
}

func adopt(
	t *testing.T,
	ctx context.Context,
	db *database.Database,
	dir string,
	digests map[string]string,
	mutate func(*AdoptChunksConfig),
) (*LoadBlobsResult, error) {
	t.Helper()
	unlock, err := db.LockImmutable(ctx)
	require.NoError(t, err)
	defer unlock()
	cfg := AdoptChunksConfig{SourceDir: dir, Digests: digests}
	if mutate != nil {
		mutate(&cfg)
	}
	return AdoptImmutableChunks(ctx, slog.New(slog.DiscardHandler), db, cfg)
}

func rawBody(
	t *testing.T,
	db *database.Database,
	slot uint64,
	hash []byte,
) []byte {
	t.Helper()
	store := db.Blob()
	if u, ok := store.(interface{ Unwrap() blob.BlobStore }); ok {
		store = u.Unwrap()
	}
	txn := store.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck
	v, err := store.Get(txn, datatypes.BlockBlobKey(slot, hash))
	require.NoError(t, err)
	return v
}

func fixtureBlocks(t *testing.T, dir string) []immutable.Block {
	t.Helper()
	imm, err := immutable.New(dir)
	require.NoError(t, err)
	it, err := imm.BlocksFromPoint(ocommon.NewPointOrigin())
	require.NoError(t, err)
	defer it.Close()
	var out []immutable.Block
	for {
		b, err := it.Next()
		require.NoError(t, err)
		if b == nil {
			return out
		}
		out = append(out, *b)
	}
}

func chainTipOf(t *testing.T, db *database.Database) (uint64, []byte) {
	t.Helper()
	recent, err := database.BlocksRecent(context.Background(), db, 1)
	require.NoError(t, err)
	require.Len(t, recent, 1)
	return recent[0].Slot, recent[0].Hash
}

// requireBlocksMatch compares every fixture block as the two databases return
// it: the stored body, the index fields and the by-hash lookup.
func requireBlocksMatch(
	t *testing.T,
	want, got *database.Database,
	blocks []immutable.Block,
) {
	t.Helper()
	ctx := context.Background()
	for i, b := range blocks {
		point := ocommon.NewPoint(b.Slot, b.Hash)
		w, err := database.BlockByPoint(ctx, want, point)
		require.NoError(t, err, "block %d", i)
		g, err := database.BlockByPoint(ctx, got, point)
		require.NoError(t, err, "block %d", i)
		require.Equal(t, w.Cbor, g.Cbor, "cbor of block %d", i)
		require.Equal(t, b.Cbor, g.Cbor, "cbor of block %d vs chunk", i)
		require.Equal(t, w.ID, g.ID, "index of block %d", i)
		require.Equal(t, w.Number, g.Number, "number of block %d", i)
		require.Equal(t, w.PrevHash, g.PrevHash, "prev hash of block %d", i)
		require.Equal(t, w.Type, g.Type, "type of block %d", i)
		byHash, err := database.BlockByHash(ctx, got, b.Hash)
		require.NoError(t, err)
		require.Equal(t, b.Cbor, byHash.Cbor)
	}
}

// Importing the same chunks with and without keeping them must give the same
// blocks and the same CBOR offset resolution; only where the bodies live
// differs.
func TestAdoptImmutableChunksMatchesBodyCopy(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{datatypes.StorageModeAPI, datatypes.StorageModeCore} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			const chunks = 3
			copyDir := copyImmutableChunkPrefix(t, chunks)
			imm, err := immutable.New(copyDir)
			require.NoError(t, err)
			baseline, err := dbtest.NewDatabase(t, &database.Config{
				DataDir:     t.TempDir(),
				Logger:      slog.New(slog.DiscardHandler),
				StorageMode: mode,
			})
			require.NoError(t, err)
			_, err = LoadBlobsWithDB(
				ctx, &config.Config{}, slog.New(slog.DiscardHandler),
				copyDir, baseline, WithImmutableDB(imm),
			)
			require.NoError(t, err)

			dir, digests := adoptSource(t, chunks)
			blocks := fixtureBlocks(t, dir)
			db := newAdoptDB(t, mode)
			res, err := adopt(t, ctx, db, dir, digests, nil)
			require.NoError(t, err)
			require.Equal(t, len(blocks), res.BlocksCopied)
			last := blocks[len(blocks)-1]
			require.Equal(t, last.Slot, res.ImmutableTipSlot)

			requireBlocksMatch(t, baseline, db, blocks)
			for i, b := range blocks {
				require.True(t, datatypes.IsImmutableMarker(
					rawBody(t, db, b.Slot, b.Hash)), "block %d", i)
				require.False(t, datatypes.IsImmutableMarker(
					rawBody(t, baseline, b.Slot, b.Hash)), "block %d", i)
			}
			st, ok := db.ImmutableState()
			require.True(t, ok)
			require.True(t, st.HasTip)
			require.Equal(t, last.Slot, st.TipSlot)
			require.Equal(t, last.Hash, st.TipHash)
			require.Equal(t, uint64(len(blocks)), st.TipIndex)

			if mode == datatypes.StorageModeAPI {
				requireOffsetsMatch(t, baseline, db, blocks)
			}
			// The chunk files moved: the source directory no longer holds
			// them, the database's ImmutableDB directory does.
			for n := range chunks {
				for _, ext := range []string{".chunk", ".primary", ".secondary"} {
					name := immutable.ChunkName(uint64(n)) + ext
					_, err := os.Stat(filepath.Join(dir, name))
					require.ErrorIs(t, err, os.ErrNotExist, name)
					_, err = os.Stat(filepath.Join(db.ImmutableDir(), name))
					require.NoError(t, err, name)
				}
			}
			// The store accepts the adopted chunks as its own.
			require.NoError(t, db.ReconcileImmutable())
		})
	}
}

// requireOffsetsMatch resolves every produced UTxO through its DOFF offset,
// which indexes into the stored block CBOR, and requires the two databases to
// agree on each, including on which outputs have no offset.
func requireOffsetsMatch(
	t *testing.T,
	want, got *database.Database,
	blocks []immutable.Block,
) {
	t.Helper()
	var resolved int
	for _, b := range blocks {
		if b.IsEbb {
			continue
		}
		decoded, err := gledger.NewBlockFromCbor(
			b.Type, b.Cbor, lcommon.VerifyConfig{SkipBodyHashValidation: true},
		)
		require.NoError(t, err)
		for _, tx := range decoded.Transactions() {
			hash := tx.Hash().Bytes()
			for _, out := range tx.Produced() {
				idx := out.Id.Index()
				w, werr := want.CborCache().ResolveUtxoCbor(hash, idx)
				g, gerr := got.CborCache().ResolveUtxoCbor(hash, idx)
				require.Equal(t, werr == nil, gerr == nil,
					"utxo %x#%d: baseline err %v, adopted err %v",
					hash, idx, werr, gerr)
				if werr != nil {
					continue
				}
				require.NotEmpty(t, g)
				require.Equal(t, w, g, "utxo %x#%d", hash, idx)
				resolved++
			}
		}
	}
	require.Positive(t, resolved, "fixture resolved no produced UTxOs")
}

// blocksBelow returns the fixture blocks whose slot is below limit.
func blocksBelow(blocks []immutable.Block, limit uint64) []immutable.Block {
	var out []immutable.Block
	for _, b := range blocks {
		if b.Slot < limit {
			out = append(out, b)
		}
	}
	return out
}

func requireTipAt(
	t *testing.T,
	db *database.Database,
	want immutable.Block,
	wantIndex int,
) {
	t.Helper()
	st, ok := db.ImmutableState()
	require.True(t, ok)
	require.True(t, st.HasTip)
	require.Equal(t, want.Slot, st.TipSlot)
	require.Equal(t, want.Hash, st.TipHash)
	require.Equal(t, uint64(wantIndex), st.TipIndex)
	slot, hash := chainTipOf(t, db)
	require.Equal(t, want.Slot, slot)
	require.Equal(t, want.Hash, hash)
}

func baselineCopy(t *testing.T, chunks int) *database.Database {
	t.Helper()
	dir := copyImmutableChunkPrefix(t, chunks)
	imm, err := immutable.New(dir)
	require.NoError(t, err)
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		Logger:      slog.New(slog.DiscardHandler),
		StorageMode: datatypes.StorageModeAPI,
	})
	require.NoError(t, err)
	_, err = LoadBlobsWithDB(
		context.Background(), &config.Config{},
		slog.New(slog.DiscardHandler), dir, db, WithImmutableDB(imm),
	)
	require.NoError(t, err)
	return db
}

// A kill between the renames of one chunk's three files leaves a torn trio in
// the ImmutableDB directory. The next run finishes that chunk, and a run after
// that finds nothing left to do.
func TestAdoptImmutableChunksResumesAfterKilledMove(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const chunks = 3
	baseline := baselineCopy(t, chunks)
	dir, digests := adoptSource(t, chunks)
	blocks := fixtureBlocks(t, dir)
	db := newAdoptDB(t, datatypes.StorageModeAPI)

	killed := errors.New("killed mid-move")
	var bound uint64
	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.OnChunkCommitted = func(n uint64) error { bound = n; return nil }
		cfg.MoveFile = func(src, dst string) error {
			if filepath.Base(dst) == "00001.secondary" {
				return killed
			}
			return moveChunkFile(src, dst)
		}
	})
	require.ErrorIs(t, err, killed)
	// A group moves every chunk in before committing any of them, so the
	// kill lands before chunk 0 commits.
	require.Zero(t, bound, "no chunk was committed")
	st, ok := db.ImmutableState()
	require.True(t, ok)
	require.False(t, st.HasTip, "no tip covers the moved chunks")
	for _, name := range []string{
		"00000.chunk", "00001.chunk", "00001.primary",
	} {
		_, err := os.Stat(filepath.Join(db.ImmutableDir(), name))
		require.NoError(t, err, name)
	}
	_, err = os.Stat(filepath.Join(db.ImmutableDir(), "00001.secondary"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(dir, "00001.secondary"))
	require.NoError(t, err)

	res, err := adopt(t, ctx, db, dir, digests, nil)
	require.NoError(t, err)
	require.Equal(t, len(blocks), res.BlocksCopied)
	requireTipAt(t, db, blocks[len(blocks)-1], len(blocks))
	requireBlocksMatch(t, baseline, db, blocks)
	requireOffsetsMatch(t, baseline, db, blocks)

	res, err = adopt(t, ctx, db, dir, digests, nil)
	require.NoError(t, err)
	require.Zero(t, res.BlocksCopied)
	require.Equal(t, blocks[len(blocks)-1].Slot, res.ImmutableTipSlot)
	requireTipAt(t, db, blocks[len(blocks)-1], len(blocks))
}

// A kill after some of a chunk's blocks committed leaves the chain inside that
// chunk with the recorded tip already at the chunk's last block, so startup
// reconciliation never cuts the chunk's uncommitted blocks. The next run
// continues after the chain tip without adding any block twice.
func TestAdoptImmutableChunksResumesInsideChunk(t *testing.T) {
	t.Parallel()
	const chunks = 3
	baseline := baselineCopy(t, chunks)
	dir, digests := adoptSource(t, chunks)
	blocks := fixtureBlocks(t, dir)
	chunk0 := blocksBelow(blocks, 4320)
	db := newAdoptDB(t, datatypes.StorageModeAPI)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.OnProgress = func(p LoadBlobsProgress) {
			if p.BlocksCopied >= len(chunk0)+loadBlockBatchSize {
				cancel()
			}
		}
	})
	require.ErrorIs(t, err, context.Canceled)
	st, ok := db.ImmutableState()
	require.True(t, ok)
	chunk1 := blocksBelow(blocks, 2*4320)
	require.Equal(t, uint64(len(chunk1)), st.TipIndex,
		"the tip covers the whole chunk being adopted")
	require.Equal(t, chunk1[len(chunk1)-1].Slot, st.TipSlot)
	slot, _ := chainTipOf(t, db)
	require.Greater(t, slot, chunk0[len(chunk0)-1].Slot,
		"blocks of chunk 1 already committed")

	res, err := adopt(t, context.Background(), db, dir, digests, nil)
	require.NoError(t, err)
	require.Less(t, res.BlocksCopied, len(blocks)-len(chunk0))
	requireTipAt(t, db, blocks[len(blocks)-1], len(blocks))
	requireBlocksMatch(t, baseline, db, blocks)
	requireOffsetsMatch(t, baseline, db, blocks)
}

// Interrupt inside the first chunk, then resume: the run must continue
// adopting rather than report the database as holding bodies.
func TestAdoptImmutableChunksResumesInsideFirstChunk(t *testing.T) {
	t.Parallel()
	const chunks = 2
	baseline := baselineCopy(t, chunks)
	dir, digests := adoptSource(t, chunks)
	blocks := fixtureBlocks(t, dir)
	db := newAdoptDB(t, datatypes.StorageModeAPI)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.OnProgress = func(p LoadBlobsProgress) {
			if p.BlocksCopied >= loadBlockBatchSize {
				cancel()
			}
		}
	})
	require.ErrorIs(t, err, context.Canceled)
	slot, _ := chainTipOf(t, db)
	require.Less(
		t,
		slot,
		blocksBelow(blocks, 4320)[len(blocksBelow(blocks, 4320))-1].Slot,
		"the chain stopped inside chunk 0",
	)

	res, err := adopt(t, context.Background(), db, dir, digests, nil)
	require.NoError(t, err, "resume inside the first chunk")
	require.Less(t, res.BlocksCopied, len(blocks))
	requireTipAt(t, db, blocks[len(blocks)-1], len(blocks))
	requireBlocksMatch(t, baseline, db, blocks)
}

// Startup reconciliation of an interrupted adoption must leave every committed
// marker readable and the pending blocks adoptable.
func TestAdoptImmutableChunksSurvivesStartupReconcile(t *testing.T) {
	t.Parallel()
	const chunks = 2
	baseline := baselineCopy(t, chunks)
	dir, digests := adoptSource(t, chunks)
	blocks := fixtureBlocks(t, dir)
	db := newAdoptDB(t, datatypes.StorageModeAPI)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.OnProgress = func(p LoadBlobsProgress) {
			if p.BlocksCopied >= loadBlockBatchSize {
				cancel()
			}
		}
	})
	require.ErrorIs(t, err, context.Canceled)

	require.NoError(t, db.ReconcileImmutable())
	slot, _ := chainTipOf(t, db)
	for _, b := range blocks {
		if b.Slot > slot {
			break
		}
		got, err := database.BlockByPoint(
			context.Background(), db, ocommon.NewPoint(b.Slot, b.Hash),
		)
		require.NoError(t, err, "committed block at slot %d", b.Slot)
		require.Equal(t, b.Cbor, got.Cbor)
	}
	_, err = adopt(t, context.Background(), db, dir, digests, nil)
	require.NoError(t, err)
	requireBlocksMatch(t, baseline, db, blocks)
}

// A file swapped in while the chunk moves is refused after the move, before
// any marker names it, and does not stay in the ImmutableDB directory.
func TestAdoptImmutableChunksRefusesSubstitutedFileAfterMove(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir, digests := adoptSource(t, 3)
	blocks := fixtureBlocks(t, dir)
	chunk0 := blocksBelow(blocks, 4320)
	db := newAdoptDB(t, datatypes.StorageModeAPI)

	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.MoveFile = func(src, dst string) error {
			if err := moveChunkFile(src, dst); err != nil {
				return err
			}
			if filepath.Base(dst) != "00001.chunk" {
				return nil
			}
			// A byte inside the last block's body, past the header the
			// import decodes, so only the digest notices.
			data, err := os.ReadFile(dst)
			if err != nil {
				return err
			}
			data[len(data)-40] ^= 0xFF
			return os.WriteFile(dst, data, 0o600)
		}
	})
	require.ErrorIs(t, err, immutable.ErrDigestMismatch)
	requireTipAt(t, db, chunk0[len(chunk0)-1], len(chunk0))
	for _, ext := range []string{".chunk", ".primary", ".secondary"} {
		_, err := os.Stat(filepath.Join(db.ImmutableDir(), "00001"+ext))
		require.ErrorIs(t, err, os.ErrNotExist, ext)
	}
	next := blocksBelow(blocks, 8640)[len(chunk0)]
	_, err = database.BlockByPoint(
		ctx, db, ocommon.NewPoint(next.Slot, next.Hash),
	)
	require.Error(t, err, "no block of the refused chunk may be stored")
}

// A chunk that is in the extracted tree but not certified, as the ancillary
// archive's partial chunk is, is neither moved nor read; its blocks arrive
// through the ordinary body copy on top of the adopted tip.
func TestAdoptImmutableChunksLeavesUncertifiedChunkAlone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir, digests := adoptSource(t, 3)
	for _, ext := range []string{".chunk", ".primary", ".secondary"} {
		delete(digests, "00002"+ext)
	}
	blocks := fixtureBlocks(t, dir)
	certified := blocksBelow(blocks, 8640)
	db := newAdoptDB(t, datatypes.StorageModeAPI)

	res, err := adopt(t, ctx, db, dir, digests, nil)
	require.NoError(t, err)
	require.Equal(t, len(certified), res.BlocksCopied)
	requireTipAt(t, db, certified[len(certified)-1], len(certified))
	for _, ext := range []string{".chunk", ".primary", ".secondary"} {
		_, err := os.Stat(filepath.Join(db.ImmutableDir(), "00002"+ext))
		require.ErrorIs(t, err, os.ErrNotExist, ext)
		_, err = os.Stat(filepath.Join(dir, "00002"+ext))
		require.NoError(t, err, ext)
	}

	cm, err := chain.NewManager(ctx, db, nil)
	require.NoError(t, err)
	rest := blocks[len(certified):]
	n, err := CopyImmutableBlocks(ctx, cm.PrimaryChain(), rest)
	require.NoError(t, err)
	require.Equal(t, len(rest), n)
	for _, b := range rest {
		require.False(t, datatypes.IsImmutableMarker(
			rawBody(t, db, b.Slot, b.Hash)))
		got, err := database.BlockByPoint(
			ctx, db, ocommon.NewPoint(b.Slot, b.Hash),
		)
		require.NoError(t, err)
		require.Equal(t, b.Cbor, got.Cbor)
	}
}

// A database that already holds block bodies cannot have chunk files joined
// onto it; the caller is told to copy, and nothing is touched.
func TestAdoptImmutableChunksRefusesDatabaseWithBodies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir, digests := adoptSource(t, 2)
	db := newAdoptDB(t, datatypes.StorageModeCore)
	cm, err := chain.NewManager(ctx, db, nil)
	require.NoError(t, err)
	blocks := fixtureBlocks(t, dir)
	_, err = CopyImmutableBlocks(ctx, cm.PrimaryChain(), blocks[:10])
	require.NoError(t, err)

	_, err = adopt(t, ctx, db, dir, digests, nil)
	require.ErrorIs(t, err, ErrAdoptUnavailable)
	files, err := immutable.ListFiles(db.ImmutableDir())
	require.NoError(t, err)
	require.Empty(t, files)
	_, err = os.Stat(filepath.Join(dir, "00000.chunk"))
	require.NoError(t, err)
}

func TestAdoptImmutableChunksRefusesWrongGeometry(t *testing.T) {
	t.Parallel()
	dir, digests := adoptSource(t, 1)
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		Logger:      slog.New(slog.DiscardHandler),
		StorageMode: datatypes.StorageModeCore,
		Immutable: database.ImmutableConfig{
			Enabled: true, ChunkSlots: 2000,
		},
	})
	require.NoError(t, err)
	_, err = adopt(t, context.Background(), db, dir, digests, nil)
	require.ErrorIs(t, err, ErrAdoptGeometry)
	st, _ := db.ImmutableState()
	require.False(t, st.HasTip)
	recent, err := database.BlocksRecent(context.Background(), db, 1)
	require.NoError(t, err)
	require.Empty(t, recent, "a chunk the geometry rejects commits nothing")
}

// Every chunk is in the ImmutableDB directory, whole and matching its
// certified digests, before the first block that names it commits. A kill at
// that point leaves the chunk in place with nothing committed, and the next
// run, which has no source copy of it any more, completes it.
func TestAdoptImmutableChunksPlacesChunkBeforeCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const chunks = 3
	baseline := baselineCopy(t, chunks)
	dir, digests := adoptSource(t, chunks)
	blocks := fixtureBlocks(t, dir)
	chunk0 := blocksBelow(blocks, 4320)
	db := newAdoptDB(t, datatypes.StorageModeAPI)

	killed := errors.New("killed before commit")
	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.BeforeCommit = func(chunk uint64) error {
			for _, ext := range []string{".chunk", ".primary", ".secondary"} {
				name := immutable.ChunkName(chunk) + ext
				data, err := os.ReadFile(filepath.Join(db.ImmutableDir(), name))
				require.NoError(t, err, name)
				sum := sha256.Sum256(data)
				require.Equal(
					t,
					digests[name],
					hex.EncodeToString(sum[:]),
					name,
				)
			}
			recent, err := database.BlocksRecent(ctx, db, 1)
			require.NoError(t, err)
			if len(recent) > 0 {
				require.Less(t, recent[0].Slot, chunk*4320,
					"no block of chunk %d may be committed yet", chunk)
			}
			if chunk == 1 {
				return killed
			}
			return nil
		}
	})
	require.ErrorIs(t, err, killed)
	requireTipAt(t, db, chunk0[len(chunk0)-1], len(chunk0))
	_, err = os.Stat(filepath.Join(dir, "00001.chunk"))
	require.ErrorIs(t, err, os.ErrNotExist, "the source copy moved")
	pending, ok, err := db.ImmutableAdoptPending()
	require.NoError(t, err)
	require.True(t, ok, "the adoption is recorded as pending")
	require.Equal(t, uint64(0), pending,
		"the record names its group's first chunk")

	res, err := adopt(t, ctx, db, dir, digests, nil)
	require.NoError(t, err)
	require.Equal(t, len(blocks)-len(chunk0), res.BlocksCopied)
	requireTipAt(t, db, blocks[len(blocks)-1], len(blocks))
	requireBlocksMatch(t, baseline, db, blocks)
	_, ok, err = db.ImmutableAdoptPending()
	require.NoError(t, err)
	require.False(t, ok, "a finished adoption leaves no pending record")
}

// A stop after a chunk's files moved into the ImmutableDB directory and before
// the recorded tip covers them must survive startup reconciliation: the
// source copy is gone, so cutting the files as bytes past the tip would lose
// the certified chunk. Chunk 0 has no recorded tip at all, chunk 1 has a tip
// at chunk 0's end.
func TestAdoptImmutableChunksSurvivesReconcileBeforeTipRecorded(t *testing.T) {
	t.Parallel()
	for _, stopChunk := range []uint64{0, 1} {
		t.Run(fmt.Sprintf("chunk%d", stopChunk), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			const chunks = 3
			baseline := baselineCopy(t, chunks)
			dir, digests := adoptSource(t, chunks)
			blocks := fixtureBlocks(t, dir)
			db := newAdoptDB(t, datatypes.StorageModeAPI)

			killed := errors.New("killed before commit")
			_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
				cfg.BeforeCommit = func(chunk uint64) error {
					if chunk == stopChunk {
						return killed
					}
					return nil
				}
			})
			require.ErrorIs(t, err, killed)

			sums := map[string][sha256.Size]byte{}
			for _, ext := range []string{".chunk", ".primary", ".secondary"} {
				name := immutable.ChunkName(stopChunk) + ext
				data, err := os.ReadFile(filepath.Join(db.ImmutableDir(), name))
				require.NoError(t, err, name)
				sums[name] = sha256.Sum256(data)
			}

			require.NoError(t, db.ReconcileImmutable())

			for name, want := range sums {
				data, err := os.ReadFile(filepath.Join(db.ImmutableDir(), name))
				require.NoError(t, err, "reconcile kept %s", name)
				require.Equal(t, want, sha256.Sum256(data), name)
			}

			res, err := adopt(t, ctx, db, dir, digests, nil)
			require.NoError(t, err)
			require.Positive(t, res.BlocksCopied)
			requireTipAt(t, db, blocks[len(blocks)-1], len(blocks))
			requireBlocksMatch(t, baseline, db, blocks)
			requireOffsetsMatch(t, baseline, db, blocks)
		})
	}
}

// certifiedPrefix keeps the digests of the first n chunks.
func certifiedPrefix(digests map[string]string, n uint64) map[string]string {
	out := map[string]string{}
	for name, sum := range digests {
		if num, ok := immutable.ParseFileName(name); ok && num < n {
			out[name] = sum
		}
	}
	return out
}

// Blocks committed after a completed adoption, such as gap blocks before the
// sync is marked complete, put the chain tip past the recorded immutable tip.
// A resumed adoption has nothing left to do for the chunks it already holds.
func TestAdoptImmutableChunksResumesWithChainPastRecordedTip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir, digests := adoptSource(t, 4)
	blocks := fixtureBlocks(t, dir)
	adopted := blocksBelow(blocks, 3*4320)
	extra := blocksBelow(blocks, 4*4320)[len(adopted):]
	require.NotEmpty(t, extra)
	certified := certifiedPrefix(digests, 3)
	db := newAdoptDB(t, datatypes.StorageModeAPI)

	res, err := adopt(t, ctx, db, dir, certified, nil)
	require.NoError(t, err)
	require.Equal(t, len(adopted), res.BlocksCopied)

	cm, err := chain.NewManager(ctx, db, nil)
	require.NoError(t, err)
	raws := make([]chain.RawBlock, 0, len(extra))
	for i := range extra {
		rb, err := rawBlockFromImmutableBlock(&extra[i])
		require.NoError(t, err)
		raws = append(raws, rb)
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(ctx, raws))

	res, err = adopt(t, ctx, db, dir, certified, nil)
	require.NoError(t, err, "resume with the chain past the recorded tip")
	require.Zero(t, res.BlocksCopied)
	st, _ := db.ImmutableState()
	require.Equal(t, uint64(len(adopted)), st.TipIndex)
	slot, _ := chainTipOf(t, db)
	require.Equal(t, extra[len(extra)-1].Slot, slot)
}

// A chunk whose primary index is not the shape the geometry gives, here an
// epoch boundary slot the geometry does not have, is refused before any of
// it commits.
func TestAdoptImmutableChunksRefusesPrimaryIndexOfOtherGeometry(t *testing.T) {
	t.Parallel()
	dir, digests := adoptSource(t, 1)
	geom := adoptGeometry(t)
	geom.CanContainEBB = !geom.CanContainEBB
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		Logger:      slog.New(slog.DiscardHandler),
		StorageMode: datatypes.StorageModeAPI,
		Immutable:   geom,
	})
	require.NoError(t, err)
	_, err = adopt(t, context.Background(), db, dir, digests, nil)
	require.ErrorIs(t, err, ErrAdoptGeometry)
	st, _ := db.ImmutableState()
	require.False(t, st.HasTip)
	recent, err := database.BlocksRecent(context.Background(), db, 1)
	require.NoError(t, err)
	require.Empty(t, recent)
}

// Lowering the tip into an adopted range cuts the chunk files at a block the
// adopted chunks hold and leaves them reconcilable.
func TestLowerImmutableTipIntoAdoptedRange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir, digests := adoptSource(t, 3)
	blocks := fixtureBlocks(t, dir)
	db := newAdoptDB(t, datatypes.StorageModeAPI)
	_, err := adopt(t, ctx, db, dir, digests, nil)
	require.NoError(t, err)

	keep := len(blocksBelow(blocks, 4320)) + 5
	require.NoError(t, db.LowerImmutableTip(uint64(keep)))
	st, _ := db.ImmutableState()
	require.True(t, st.HasTip)
	require.Equal(t, blocks[keep-1].Slot, st.TipSlot)
	require.Equal(t, blocks[keep-1].Hash, st.TipHash)
	require.Equal(t, uint64(keep), st.TipIndex)
	require.NoError(t, db.ReconcileImmutable())
	_, err = os.Stat(filepath.Join(db.ImmutableDir(), "00002.chunk"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

// An interrupted adoption leaves the chain behind the recorded immutable tip.
// A body copy onto that database would store full bodies at indexes at or
// below the tip that nothing ever promotes, so it is refused.
func TestLoadBlobsRefusesDatabaseBehindImmutableTip(t *testing.T) {
	t.Parallel()
	const chunks = 2
	dir, digests := adoptSource(t, chunks)
	db := newAdoptDB(t, datatypes.StorageModeAPI)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.OnProgress = func(p LoadBlobsProgress) {
			if p.BlocksCopied >= loadBlockBatchSize {
				cancel()
			}
		}
	})
	require.ErrorIs(t, err, context.Canceled)
	before, _ := chainTipOf(t, db)

	copyDir := copyImmutableChunkPrefix(t, chunks)
	imm, err := immutable.New(copyDir)
	require.NoError(t, err)
	_, err = LoadBlobsWithDB(
		context.Background(), &config.Config{},
		slog.New(slog.DiscardHandler), copyDir, db, WithImmutableDB(imm),
	)
	require.ErrorIs(t, err, ErrLoadBehindImmutableTip)
	after, _ := chainTipOf(t, db)
	require.Equal(t, before, after, "the refused load added no block")
}

// Blocks the chain already holds past the recorded tip keep their bodies in
// the blob store, below a tip the adoption would move past them, so promotion
// would never reach them. Adoption refuses rather than skip over them.
func TestAdoptImmutableChunksRefusesBodiesPastRecordedTip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir, digests := adoptSource(t, 2)
	blocks := fixtureBlocks(t, dir)
	chunk0 := blocksBelow(blocks, 4320)
	chunk1 := blocksBelow(blocks, 2*4320)[len(chunk0):]
	require.Greater(t, len(chunk1), 1)
	db := newAdoptDB(t, datatypes.StorageModeAPI)

	_, err := adopt(t, ctx, db, dir, certifiedPrefix(digests, 1), nil)
	require.NoError(t, err)
	cm, err := chain.NewManager(ctx, db, nil)
	require.NoError(t, err)
	rb, err := rawBlockFromImmutableBlock(&chunk1[0])
	require.NoError(t, err)
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(ctx, []chain.RawBlock{rb}))

	_, err = adopt(t, ctx, db, dir, digests, nil)
	require.ErrorIs(t, err, ErrAdoptUnavailable)
	_, err = os.Stat(filepath.Join(dir, "00001.chunk"))
	require.NoError(t, err, "the refused chunk stays in the source")
	_, err = os.Stat(filepath.Join(db.ImmutableDir(), "00001.chunk"))
	require.ErrorIs(t, err, os.ErrNotExist)
	st, ok := db.ImmutableState()
	require.True(t, ok)
	require.Equal(t, uint64(len(chunk0)), st.TipIndex)
	require.Equal(t, chunk0[len(chunk0)-1].Slot, st.TipSlot)
}

// The chunk the recorded tip ends in already has committed markers when a run
// stopped inside it. A digest mismatch on the resumed run must not delete the
// files those markers name.
func TestAdoptImmutableChunksKeepsTipChunkFilesOnDigestMismatch(t *testing.T) {
	t.Parallel()
	const chunks = 3
	dir, digests := adoptSource(t, chunks)
	blocks := fixtureBlocks(t, dir)
	chunk0 := blocksBelow(blocks, 4320)
	db := newAdoptDB(t, datatypes.StorageModeAPI)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.OnProgress = func(p LoadBlobsProgress) {
			if p.BlocksCopied >= len(chunk0)+loadBlockBatchSize {
				cancel()
			}
		}
	})
	require.ErrorIs(t, err, context.Canceled)
	slot, _ := chainTipOf(t, db)
	require.Greater(t, slot, chunk0[len(chunk0)-1].Slot,
		"blocks of chunk 1 already committed")

	chunkPath := filepath.Join(db.ImmutableDir(), "00001.chunk")
	data, err := os.ReadFile(chunkPath)
	require.NoError(t, err)
	data[len(data)-1] ^= 0xff
	require.NoError(t, os.WriteFile(chunkPath, data, 0o644))

	_, err = adopt(t, context.Background(), db, dir, digests, nil)
	require.ErrorIs(t, err, immutable.ErrDigestMismatch)
	for _, ext := range []string{".chunk", ".primary", ".secondary"} {
		_, err := os.Stat(filepath.Join(db.ImmutableDir(), "00001"+ext))
		require.NoError(t, err, "committed markers name 00001%s", ext)
	}
}

// A chunk that stops after its tip was recorded, with later chunks of its
// group returned to the source, leaves no file past the recorded tip, so the
// adopt record is cleared and the ImmutableDB writer can open.
func TestAdoptImmutableChunksMidGroupStopAfterTipClearsRecord(t *testing.T) {
	t.Parallel()
	const chunks = 3
	dir, digests := adoptSource(t, chunks)
	blocks := fixtureBlocks(t, dir)
	chunk0 := blocksBelow(blocks, 4320)
	db := newAdoptDB(t, datatypes.StorageModeAPI)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.OnProgress = func(p LoadBlobsProgress) {
			if p.BlocksCopied >= len(chunk0)+loadBlockBatchSize {
				cancel()
			}
		}
	})
	require.ErrorIs(t, err, context.Canceled)
	st, ok := db.ImmutableState()
	require.True(t, ok)
	chunk1 := blocksBelow(blocks, 2*4320)
	require.Equal(t, uint64(len(chunk1)), st.TipIndex,
		"the tip covers chunk 1")
	_, pending, err := db.ImmutableAdoptPending()
	require.NoError(t, err)
	require.False(t, pending, "the recorded tip covers every adopted file")
	for _, ext := range chunkFileExts {
		_, err := os.Stat(filepath.Join(db.ImmutableDir(), "00002"+ext))
		require.ErrorIs(t, err, os.ErrNotExist,
			"chunk 2 left the ImmutableDB")
	}
	require.NoError(t, db.ReconcileImmutable())

	res, err := adopt(t, context.Background(), db, dir, digests, nil)
	require.NoError(t, err)
	require.Less(t, res.BlocksCopied, len(blocks)-len(chunk0))
	requireTipAt(t, db, blocks[len(blocks)-1], len(blocks))
}

// A chunk that fails inside a group returns the chunks after it to the source
// directory and finishes the chunks before it: their blobs are synced, their
// source files removed and the committed callback reports them.
func TestAdoptImmutableChunksMidGroupFailureReturnsLaterChunks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const chunks = 3
	dir, digests := adoptSource(t, chunks)
	db := newAdoptDB(t, datatypes.StorageModeAPI)

	killed := errors.New("killed before commit")
	var committed []uint64
	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.BeforeCommit = func(chunk uint64) error {
			if chunk == 1 {
				return killed
			}
			return nil
		}
		cfg.OnChunkCommitted = func(firstUnadopted uint64) error {
			committed = append(committed, firstUnadopted)
			return nil
		}
	})
	require.ErrorIs(t, err, killed)
	require.Equal(t, []uint64{1}, committed, "chunk 0 was finished")
	for _, ext := range chunkFileExts {
		_, err := os.Stat(filepath.Join(dir, "00000"+ext))
		require.ErrorIs(t, err, os.ErrNotExist, "chunk 0 source removed")
		_, err = os.Stat(filepath.Join(dir, "00002"+ext))
		require.NoError(t, err, "chunk 2 returned to the source")
		_, err = os.Stat(filepath.Join(db.ImmutableDir(), "00002"+ext))
		require.ErrorIs(t, err, os.ErrNotExist, "chunk 2 left the ImmutableDB")
	}
}

// Not t.Parallel: swaps the package-level adoptGroupChunks seam.
//
// Each group of chunks carries its own adopt record, naming its first chunk,
// and reports once, after its last chunk. A run stopped in a later group
// keeps the earlier group's chunks and resumes with the rest.
func TestAdoptImmutableChunksSpansGroups(t *testing.T) {
	prior := adoptGroupChunks
	t.Cleanup(func() { adoptGroupChunks = prior })
	adoptGroupChunks = 2

	ctx := context.Background()
	const chunks = 4
	baseline := baselineCopy(t, chunks)
	dir, digests := adoptSource(t, chunks)
	blocks := fixtureBlocks(t, dir)
	db := newAdoptDB(t, datatypes.StorageModeAPI)

	killed := errors.New("killed in the second group")
	records := map[uint64]uint64{}
	var committed []uint64
	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.BeforeCommit = func(chunk uint64) error {
			rec, pending, err := db.ImmutableAdoptPending()
			require.NoError(t, err)
			require.True(t, pending, "chunk %d is under a record", chunk)
			records[chunk] = rec
			if chunk == 2 {
				for _, ext := range chunkFileExts {
					_, err := os.Stat(filepath.Join(dir, "00001"+ext))
					require.ErrorIs(t, err, os.ErrNotExist,
						"the first group finished before the second")
					_, err = os.Stat(
						filepath.Join(db.ImmutableDir(), "00003"+ext),
					)
					require.NoError(t, err,
						"chunk 3 moved in with its group")
				}
				return killed
			}
			return nil
		}
		cfg.OnChunkCommitted = func(firstUnadopted uint64) error {
			committed = append(committed, firstUnadopted)
			return nil
		}
	})
	require.ErrorIs(t, err, killed)
	require.Equal(t, map[uint64]uint64{0: 0, 1: 0, 2: 2}, records)
	require.Equal(t, []uint64{2}, committed,
		"the first group reports once, past its last chunk")
	first := blocksBelow(blocks, 2*4320)
	requireTipAt(t, db, first[len(first)-1], len(first))

	committed = nil
	res, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.OnChunkCommitted = func(firstUnadopted uint64) error {
			committed = append(committed, firstUnadopted)
			return nil
		}
	})
	require.NoError(t, err)
	require.Equal(t, len(blocks)-len(first), res.BlocksCopied)
	require.Equal(t, []uint64{4}, committed)
	_, pending, err := db.ImmutableAdoptPending()
	require.NoError(t, err)
	require.False(t, pending)
	requireTipAt(t, db, blocks[len(blocks)-1], len(blocks))
	requireBlocksMatch(t, baseline, db, blocks)
	requireOffsetsMatch(t, baseline, db, blocks)
}

// recordBlobSyncs swaps the blob-sync seam for one that notes, at each sync,
// whether the adopt record is still on disk.
func recordBlobSyncs(t *testing.T, db *database.Database) *[]string {
	t.Helper()
	prior := syncAdoptedBlobs
	t.Cleanup(func() { syncAdoptedBlobs = prior })
	var events []string
	syncAdoptedBlobs = func(d *database.Database) error {
		_, pending, err := db.ImmutableAdoptPending()
		require.NoError(t, err)
		events = append(events, fmt.Sprintf("sync pending=%t", pending))
		return prior(d)
	}
	return &events
}

// Not t.Parallel: swaps the package-level syncAdoptedBlobs seam.
//
// The recorded tip that covers a group's files is a blob-store write, durable
// only once the blob store syncs. The adopt record must outlive that sync, or a
// crash between them leaves moved files past a lost tip with nothing to stop
// reconciliation cutting them.
func TestAdoptImmutableChunksKeepsRecordUntilBlobSync(t *testing.T) {
	ctx := context.Background()
	dir, digests := adoptSource(t, 2)
	db := newAdoptDB(t, datatypes.StorageModeAPI)
	events := recordBlobSyncs(t, db)

	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.OnChunkCommitted = func(uint64) error {
			_, pending, err := db.ImmutableAdoptPending()
			require.NoError(t, err)
			*events = append(
				*events,
				fmt.Sprintf("committed pending=%t", pending),
			)
			return nil
		}
	})
	require.NoError(t, err)
	require.Equal(t,
		[]string{"sync pending=true", "committed pending=false"}, *events,
		"the record is cleared after the group's blob sync")
}

// Not t.Parallel: swaps the package-level syncAdoptedBlobs seam.
//
// A chunk that stops after its tip was recorded, with no earlier chunk of its
// group to finish, clears the adopt record only after a blob sync makes that
// tip durable.
func TestAdoptImmutableChunksStopAfterTipSyncsBeforeClear(t *testing.T) {
	dir, digests := adoptSource(t, 2)
	db := newAdoptDB(t, datatypes.StorageModeAPI)
	events := recordBlobSyncs(t, db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.OnProgress = func(p LoadBlobsProgress) {
			if p.BlocksCopied >= loadBlockBatchSize {
				cancel()
			}
		}
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{"sync pending=true"}, *events,
		"the tip over chunk 0 is synced while the record still stands")
	_, pending, err := db.ImmutableAdoptPending()
	require.NoError(t, err)
	require.False(t, pending, "the durable tip covers every adopted file")
}

// Not t.Parallel: swaps the package-level renameFile and syncDir seams.
//
// A move across file systems copies, then removes the source. The copy and its
// directory entry must be durable first, or a crash can persist the removal
// and lose both.
func TestMoveChunkFileAcrossFileSystemsSyncsBeforeRemove(t *testing.T) {
	priorRename, priorSync := renameFile, syncDir
	t.Cleanup(func() { renameFile, syncDir = priorRename, priorSync })
	srcDir, dstDir := t.TempDir(), t.TempDir()
	src := filepath.Join(srcDir, "00000.chunk")
	dst := filepath.Join(dstDir, "00000.chunk")
	data := []byte("chunk bytes")
	require.NoError(t, os.WriteFile(src, data, 0o600))

	renameFile = func(oldpath, newpath string) error {
		if oldpath == src {
			return &os.LinkError{
				Op: "rename", Old: oldpath, New: newpath, Err: syscall.EXDEV,
			}
		}
		return priorRename(oldpath, newpath)
	}
	var synced []string
	syncDir = func(d string) error {
		if d == dstDir {
			_, err := os.Stat(src)
			require.NoError(t, err, "the source outlives the destination sync")
			got, err := os.ReadFile(dst)
			require.NoError(t, err)
			require.Equal(t, data, got)
		}
		synced = append(synced, d)
		return priorSync(d)
	}

	require.NoError(t, moveChunkFile(src, dst))
	require.Equal(t, []string{dstDir}, synced)
	_, err := os.Stat(src)
	require.ErrorIs(t, err, os.ErrNotExist)
	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, data, got)
}
