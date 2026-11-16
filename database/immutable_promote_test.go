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

package database_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	badgerblob "github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// fixtureModels loads the first n preview fixture blocks as database blocks
// numbered from 1.
func fixtureModels(t *testing.T, n int) []models.Block {
	t.Helper()
	idb, err := immutable.New(filepath.Join("immutable", "testdata"))
	require.NoError(t, err)
	it, err := idb.BlocksFromPoint(ocommon.NewPointOrigin())
	require.NoError(t, err)
	defer it.Close()
	var out []models.Block
	for len(out) < n {
		b, err := it.Next()
		require.NoError(t, err)
		require.NotNil(t, b)
		out = append(out, models.Block{
			ID: uint64(len(out) + 1), Slot: b.Slot, Hash: b.Hash,
			Cbor: b.Cbor, Type: b.Type, Number: uint64(len(out) + 1),
			PrevHash: make([]byte, 32),
		})
	}
	return out
}

func newImmutableDB(
	t *testing.T,
	batch int,
) (*database.Database, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: dir,
		Immutable: database.ImmutableConfig{
			Enabled: true, ChunkSlots: 4320, CanContainEBB: true,
			PromoteBatch: batch,
		},
	})
	require.NoError(t, err)
	return db, dir
}

func seedBlocks(t *testing.T, db *database.Database, blocks []models.Block) {
	t.Helper()
	for _, b := range blocks {
		require.NoError(t, db.BlockCreate(b, nil))
	}
}

func rawBodyValue(t *testing.T, db *database.Database, b models.Block) []byte {
	t.Helper()
	store := db.Blob()
	if u, ok := store.(interface{ Unwrap() blob.BlobStore }); ok {
		store = u.Unwrap()
	}
	txn := store.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck
	v, err := store.Get(txn, types.BlockBlobKey(b.Slot, b.Hash))
	require.NoError(t, err)
	return v
}

func TestPromoteThroughReplacesBodiesAndKeepsReads(t *testing.T) {
	t.Parallel()
	// Spans two chunk files (216 blocks each).
	blocks := fixtureModels(t, 300)
	db, dir := newImmutableDB(t, 50)
	seedBlocks(t, db, blocks)

	res, err := db.PromoteThrough(context.Background(), 250)
	require.NoError(t, err)
	require.Equal(t, 250, res.Promoted)
	st, ok := db.ImmutableState()
	require.True(t, ok)
	require.Equal(t, uint64(250), st.TipIndex)
	require.Equal(t, blocks[249].Slot, st.TipSlot)

	for i, b := range blocks {
		raw := rawBodyValue(t, db, b)
		if i < 250 {
			require.True(t, types.IsImmutableMarker(raw), "block %d", b.ID)
		} else {
			require.False(t, types.IsImmutableMarker(raw), "block %d", b.ID)
		}
		got, err := database.BlockByPoint(
			context.Background(), db, ocommon.NewPoint(b.Slot, b.Hash),
		)
		require.NoError(t, err)
		require.Equal(t, b.Cbor, got.Cbor, "by point %d", b.ID)
		byHash, err := database.BlockByHash(context.Background(), db, b.Hash)
		require.NoError(t, err)
		require.Equal(t, b.Cbor, byHash.Cbor, "by hash %d", b.ID)
		byIdx, err := db.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
		require.Equal(t, b.Cbor, byIdx.Cbor, "by index %d", b.ID)
	}
	for _, name := range []string{"00000.chunk", "00001.chunk"} {
		_, err := os.Stat(filepath.Join(dir, database.ImmutableDirName, name))
		require.NoError(t, err)
	}
	// Resuming is a no-op and a higher ceiling continues from the tip.
	res, err = db.PromoteThrough(context.Background(), 250)
	require.NoError(t, err)
	require.Zero(t, res.Promoted)
	res, err = db.PromoteThrough(context.Background(), 300)
	require.NoError(t, err)
	require.Equal(t, 50, res.Promoted)
}

func TestImmutableFloorRefusesDeletionAtOrBelowTip(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 20)
	db, _ := newImmutableDB(t, 0)
	seedBlocks(t, db, blocks)
	_, err := db.PromoteThrough(context.Background(), 10)
	require.NoError(t, err)

	txn := db.BlockBlobTxn()
	err = database.BlockDeleteTxn(txn, blocks[9])
	_ = txn.Rollback()
	require.ErrorIs(t, err, database.ErrRollbackBelowImmutableTip)

	require.ErrorIs(
		t, db.CheckImmutableRollback(9), database.ErrRollbackBelowImmutableTip,
	)
	require.NoError(t, db.CheckImmutableRollback(10))

	txn = db.BlockBlobTxn()
	require.NoError(t, database.BlockDeleteTxn(txn, blocks[10]))
	require.NoError(t, txn.Commit())
}

func TestReopenInstallsMarkerReadPathWithoutConfig(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 30)
	dir := t.TempDir()
	cfg := &database.Config{
		DataDir: dir,
		Immutable: database.ImmutableConfig{
			Enabled: true, ChunkSlots: 4320, CanContainEBB: true,
		},
	}
	db, err := dbtest.NewDatabase(t, cfg)
	require.NoError(t, err)
	seedBlocks(t, db, blocks)
	_, err = db.PromoteThrough(context.Background(), 30)
	require.NoError(t, err)
	require.NoError(t, dbtest.CloseDatabase(db))

	// A partial opener knows nothing of the feature; the state record alone
	// installs the read path.
	db2, err := dbtest.NewDatabase(t, &database.Config{DataDir: dir})
	require.NoError(t, err)
	got, err := database.BlockByPoint(
		context.Background(), db2,
		ocommon.NewPoint(blocks[5].Slot, blocks[5].Hash),
	)
	require.NoError(t, err)
	require.Equal(t, blocks[5].Cbor, got.Cbor)
	require.False(t, errors.Is(err, types.ErrBadImmutableMarker))
}

func TestInventoryRebuildReadsNoPromotedBodies(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 20)
	db, dir := newImmutableDB(t, 0)
	seedBlocks(t, db, blocks)
	_, err := db.PromoteThrough(context.Background(), 20)
	require.NoError(t, err)

	// Drop the inventory record so the next ensure rescans every bp value,
	// and remove the chunk files so any body read would fail.
	store := db.Blob()
	if u, ok := store.(interface{ Unwrap() blob.BlobStore }); ok {
		store = u.Unwrap()
	}
	txn := store.NewTransaction(true)
	require.NoError(t, store.Delete(txn, []byte("dingo:block-inventory:v1")))
	require.NoError(t, txn.Commit())
	require.NoError(
		t,
		os.RemoveAll(filepath.Join(dir, database.ImmutableDirName)),
	)

	require.NoError(t, db.EnsureBlockInventory())
}

var testImmutableConfig = database.ImmutableConfig{
	Enabled: true, ChunkSlots: 4320, CanContainEBB: true,
}

func TestReconcileTruncatesChunksAheadOfRecordedTip(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 30)
	db, dir := newImmutableDB(t, 0)
	seedBlocks(t, db, blocks)
	_, err := db.PromoteThrough(context.Background(), 10)
	require.NoError(t, err)
	require.NoError(t, dbtest.CloseDatabase(db))

	// A promotion that synced chunk bytes and died before the marker commit.
	s, err := immutable.OpenStore(
		filepath.Join(dir, database.ImmutableDirName),
		immutable.Geometry{ChunkSlots: 4320, CanContainEBB: true},
	)
	require.NoError(t, err)
	for _, b := range blocks[10:15] {
		_, err := s.Append(immutable.WriteBlock{
			Hash: b.Hash, Cbor: b.Cbor, Type: b.Type, Slot: b.Slot,
		})
		require.NoError(t, err)
	}
	require.NoError(t, s.Sync())
	require.NoError(t, s.Close())

	db2, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: dir, Immutable: testImmutableConfig,
	})
	require.NoError(t, err)
	require.NoError(t, db2.ReconcileImmutable())
	// Promotion then rewrites the truncated range and reads stay correct.
	res, err := db2.PromoteThrough(context.Background(), 30)
	require.NoError(t, err)
	require.Equal(t, 20, res.Promoted)
	for _, b := range blocks {
		got, err := db2.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
		require.Equal(t, b.Cbor, got.Cbor, "index %d", b.ID)
	}
	s2, err := immutable.OpenStore(
		filepath.Join(dir, database.ImmutableDirName),
		immutable.Geometry{ChunkSlots: 4320, CanContainEBB: true},
	)
	require.NoError(t, err)
	defer s2.Close()
	tip, ok := s2.Tip()
	require.True(t, ok)
	require.Equal(t, blocks[29].Slot, tip.Slot)
}

func TestReconcileRefusesChunksBehindRecordedTip(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 30)
	db, dir := newImmutableDB(t, 0)
	seedBlocks(t, db, blocks)
	_, err := db.PromoteThrough(context.Background(), 20)
	require.NoError(t, err)
	require.NoError(t, dbtest.CloseDatabase(db))

	s, err := immutable.OpenStore(
		filepath.Join(dir, database.ImmutableDirName),
		immutable.Geometry{ChunkSlots: 4320, CanContainEBB: true},
	)
	require.NoError(t, err)
	require.NoError(t, s.TruncateAfter(blocks[9].Slot, blocks[9].Hash))
	require.NoError(t, s.Close())

	db2, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: dir, Immutable: testImmutableConfig,
	})
	require.NoError(t, err)
	err = db2.ReconcileImmutable()
	require.ErrorIs(t, err, database.ErrImmutableChunksBehind)
	_, err = db2.PromoteThrough(context.Background(), 30)
	require.ErrorIs(t, err, database.ErrImmutableChunksBehind)
}

func TestExistingDatabaseMigratesResumably(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 30)
	dir := t.TempDir()
	// A database from before the feature: no state record, bodies in badger.
	plain, err := dbtest.NewDatabase(t, &database.Config{DataDir: dir})
	require.NoError(t, err)
	seedBlocks(t, plain, blocks)
	require.NoError(t, dbtest.CloseDatabase(plain))
	_, err = os.Stat(filepath.Join(dir, database.ImmutableDirName))
	require.ErrorIs(t, err, os.ErrNotExist)

	open := func() *database.Database {
		db, err := dbtest.NewDatabase(t, &database.Config{
			DataDir:   dir,
			Immutable: testImmutableConfig,
		})
		require.NoError(t, err)
		return db
	}
	db := open()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = db.PromoteThrough(cancelled, 30)
	require.ErrorIs(t, err, context.Canceled)
	res, err := db.PromoteThrough(context.Background(), 12)
	require.NoError(t, err)
	require.Equal(t, 12, res.Promoted)
	require.NoError(t, dbtest.CloseDatabase(db))

	// Restart resumes from the recorded tip rather than from origin.
	db = open()
	res, err = db.PromoteThrough(context.Background(), 30)
	require.NoError(t, err)
	require.Equal(t, 18, res.Promoted)
	for _, b := range blocks {
		got, err := db.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
		require.Equal(t, b.Cbor, got.Cbor, "index %d", b.ID)
		require.True(t, types.IsImmutableMarker(rawBodyValue(t, db, b)))
	}
}

// An interruption at any step of a promotion batch leaves a database that
// reopens, resumes, and ends with the same chunk files as an uninterrupted run.
func TestPromotionResumesAfterInterruptionAtEveryStep(t *testing.T) {
	t.Parallel()
	const total = 30
	blocks := fixtureModels(t, total)

	control, controlDir := newImmutableDB(t, 10)
	seedBlocks(t, control, blocks)
	_, err := control.PromoteThrough(context.Background(), total)
	require.NoError(t, err)
	chunkFiles := []string{"00000.chunk", "00000.secondary", "00000.primary"}
	want := map[string][]byte{}
	for _, name := range chunkFiles {
		b, err := os.ReadFile(
			filepath.Join(controlDir, database.ImmutableDirName, name),
		)
		require.NoError(t, err)
		want[name] = b
	}

	errInterrupted := errors.New("interrupted")
	for _, tc := range []struct {
		step string
		// occurrence is the call of that step to interrupt: late enough that
		// an earlier batch has already committed.
		occurrence int
		// committed is the highest block index the interrupted run leaves
		// recorded, as read back after reopening.
		committed uint64
	}{
		{"append", 15, 10},
		{"sync", 2, 10},
		{"commit", 2, 10},
		{"state", 2, 20},
	} {
		t.Run(tc.step, func(t *testing.T) {
			t.Parallel()
			db, dir := newImmutableDB(t, 10)
			seedBlocks(t, db, blocks)
			calls := 0
			database.SetImmutablePromoteFault(db, func(step string) error {
				if step != tc.step {
					return nil
				}
				calls++
				if calls == tc.occurrence {
					return errInterrupted
				}
				return nil
			})
			_, err := db.PromoteThrough(context.Background(), total)
			require.ErrorIs(t, err, errInterrupted)
			require.NoError(t, dbtest.CloseDatabase(db))

			db2, err := dbtest.NewDatabase(t, &database.Config{
				DataDir: dir, Immutable: testImmutableConfig,
			})
			require.NoError(t, err)
			require.NoError(t, db2.ReconcileImmutable())
			st, ok := db2.ImmutableState()
			require.True(t, ok)
			require.Equal(t, tc.committed, st.TipIndex)
			for _, b := range blocks {
				got, err := db2.BlockByIndex(b.ID, nil)
				require.NoError(t, err)
				require.Equal(
					t,
					b.Cbor,
					got.Cbor,
					"after reopen, index %d",
					b.ID,
				)
			}

			_, err = db2.PromoteThrough(context.Background(), total)
			require.NoError(t, err)
			st, _ = db2.ImmutableState()
			require.Equal(t, uint64(total), st.TipIndex)
			for _, name := range chunkFiles {
				got, err := os.ReadFile(
					filepath.Join(dir, database.ImmutableDirName, name),
				)
				require.NoError(t, err)
				require.Equal(t, want[name], got, name)
			}
		})
	}
}

// UTxO and transaction CBOR is resolved from a block-relative offset, so it
// must come out of a block that now lives in a chunk file, singly and in
// batches, with the block cache cold.
func TestCborResolutionOverPromotedBlocks(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 30)
	dir := t.TempDir()
	cfg := &database.Config{
		DataDir: dir,
		Immutable: database.ImmutableConfig{
			Enabled: true, ChunkSlots: 4320, CanContainEBB: true,
		},
	}
	db, err := dbtest.NewDatabase(t, cfg)
	require.NoError(t, err)
	seedBlocks(t, db, blocks)

	type entry struct {
		id   [32]byte
		want []byte
	}
	var entries []entry
	txn := db.BlobTxn(true)
	for i, b := range blocks[:20] {
		off := uint32(8 + i)
		length := uint32(16)
		offset := &database.CborOffset{
			BlockSlot: b.Slot, ByteOffset: off, ByteLength: length,
		}
		copy(offset.BlockHash[:], b.Hash)
		var id [32]byte
		id[0], id[1] = 0xAB, byte(i)
		require.NoError(t, db.Blob().SetUtxo(
			txn.Blob(), id[:], uint32(i), database.EncodeUtxoOffset(offset),
		))
		var txid [32]byte
		txid[0], txid[1] = 0xCD, byte(i)
		require.NoError(t, db.Blob().SetTx(
			txn.Blob(), txid[:], database.EncodeTxOffset(offset),
		))
		entries = append(entries, entry{
			id: id, want: append([]byte(nil), b.Cbor[off:off+length]...),
		})
	}
	require.NoError(t, txn.Commit())
	_, err = db.PromoteThrough(context.Background(), 20)
	require.NoError(t, err)
	require.NoError(t, dbtest.CloseDatabase(db))

	db, err = dbtest.NewDatabase(t, &database.Config{DataDir: dir})
	require.NoError(t, err)
	for i, b := range blocks[:20] {
		require.True(t, types.IsImmutableMarker(rawBodyValue(t, db, b)))
		e := entries[i]
		got, err := db.CborCache().ResolveUtxoCbor(e.id[:], uint32(i))
		require.NoError(t, err)
		require.Equal(t, e.want, got, "utxo %d", i)
		var txid [32]byte
		txid[0], txid[1] = 0xCD, byte(i)
		gotTx, err := db.CborCache().ResolveTxCbor(nil, txid[:])
		require.NoError(t, err)
		require.Equal(t, e.want, gotTx, "tx %d", i)
	}

	require.NoError(t, dbtest.CloseDatabase(db))
	db, err = dbtest.NewDatabase(t, &database.Config{DataDir: dir})
	require.NoError(t, err)
	var refs []database.UtxoRef
	var hashes [][32]byte
	for i := range entries {
		refs = append(
			refs,
			database.UtxoRef{TxId: entries[i].id, OutputIdx: uint32(i)},
		)
		var txid [32]byte
		txid[0], txid[1] = 0xCD, byte(i)
		hashes = append(hashes, txid)
	}
	byRef, err := db.CborCache().ResolveUtxoCborBatch(refs)
	require.NoError(t, err)
	byTx, err := db.CborCache().ResolveTxCborBatch(hashes)
	require.NoError(t, err)
	for i, e := range entries {
		require.Equal(t, e.want, byRef[refs[i]], "batch utxo %d", i)
		require.Equal(t, e.want, byTx[hashes[i]], "batch tx %d", i)
	}
}

// The block iterators read bodies through the marker wrapper.
func TestIteratorsOverPromotedBlocks(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 30)
	db, dir := newImmutableDB(t, 0)
	seedBlocks(t, db, blocks)
	_, err := db.PromoteThrough(context.Background(), 20)
	require.NoError(t, err)
	require.NoError(t, dbtest.CloseDatabase(db))
	db, err = dbtest.NewDatabase(t, &database.Config{DataDir: dir})
	require.NoError(t, err)

	it := db.BlocksFromSlot(0)
	defer it.Close()
	var seen int
	for {
		res, err := it.NextRaw()
		require.NoError(t, err)
		if res == nil {
			break
		}
		require.Equal(
			t,
			blocks[seen].Cbor,
			res.Cbor,
			"from slot, block %d",
			seen+1,
		)
		seen++
	}
	require.Equal(t, len(blocks), seen)

	ranged := db.BlocksInRange(blocks[0].Slot, blocks[29].Slot)
	defer ranged.Close()
	seen = 0
	for {
		res, err := ranged.NextRaw()
		require.NoError(t, err)
		if res == nil {
			break
		}
		require.Equal(
			t,
			blocks[seen].Cbor,
			res.Cbor,
			"in range, block %d",
			seen+1,
		)
		seen++
	}
	require.Equal(t, len(blocks), seen)
}

// barkLikeStore stands in for a wrapper stacked above the marker wrapper.
type barkLikeStore struct{ blob.BlobStore }

func (s *barkLikeStore) Unwrap() blob.BlobStore { return s.BlobStore }

func TestPromoteThroughRetriesInProcessAfterFailedStep(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 30)
	for _, step := range []string{"append", "sync", "commit", "state"} {
		t.Run(step, func(t *testing.T) {
			t.Parallel()
			db, _ := newImmutableDB(t, 10)
			seedBlocks(t, db, blocks)
			errInjected := errors.New("injected")
			fired := false
			database.SetImmutablePromoteFault(db, func(s string) error {
				// Fail the second batch, after the first has committed.
				if s == step && !fired && func() bool {
					st, _ := db.ImmutableState()
					return st.TipIndex >= 10 || step == "state"
				}() {
					fired = true
					return errInjected
				}
				return nil
			})
			_, err := db.PromoteThrough(context.Background(), 30)
			require.ErrorIs(t, err, errInjected)
			res, err := db.PromoteThrough(context.Background(), 30)
			require.NoError(t, err, "retry without a restart")
			st, _ := db.ImmutableState()
			require.Equal(t, uint64(30), st.TipIndex)
			require.Positive(t, res.Promoted)
			for _, b := range blocks {
				got, err := db.BlockByIndex(b.ID, nil)
				require.NoError(t, err)
				require.Equal(t, b.Cbor, got.Cbor, "index %d", b.ID)
			}
		})
	}
}

func TestPromoteThroughCountsEveryPromotedBlock(t *testing.T) {
	t.Parallel()
	db, _ := newImmutableDB(t, 10)
	seedBlocks(t, db, fixtureModels(t, 1))
	res, err := db.PromoteThrough(context.Background(), 100)
	require.NoError(t, err)
	require.Equal(t, 1, res.Promoted)
}

func TestRawBlobStoreWalksWrappersAboveTheMarkerStore(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 20)
	db, _ := newImmutableDB(t, 0)
	seedBlocks(t, db, blocks)
	_, err := db.PromoteThrough(context.Background(), 10)
	require.NoError(t, err)
	inner := database.RawBlobStoreForTest(db.Blob())
	require.NotNil(t, inner)

	wrapped := &barkLikeStore{BlobStore: db.Blob()}
	require.Same(t, inner, database.RawBlobStoreForTest(wrapped))
	db.SetBlobStore(wrapped)

	// Lowering the tip classifies stored values; through the wrapper it must
	// still see markers rather than resolved bodies.
	require.NoError(t, db.LowerImmutableTip(5))
	st, _ := db.ImmutableState()
	require.True(t, st.HasTip)
	require.Equal(t, blocks[4].Slot, st.TipSlot)
	for _, b := range blocks[:5] {
		got, err := db.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
		require.Equal(t, b.Cbor, got.Cbor)
	}
}

func TestPromotionRefusesBlockDeletedAfterItWasRead(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 10)
	db, _ := newImmutableDB(t, 0)
	seedBlocks(t, db, blocks)
	victim := blocks[3]
	fired := false
	database.SetImmutablePromoteFault(db, func(step string) error {
		if step != "sync" || fired {
			return nil
		}
		fired = true
		raw := database.RawBlobStoreForTest(db.Blob())
		txn := raw.NewTransaction(true)
		if err := raw.Delete(txn, types.BlockBlobKey(victim.Slot, victim.Hash)); err != nil {
			return err
		}
		return txn.Commit()
	})
	_, err := db.PromoteThrough(context.Background(), 10)
	require.ErrorIs(t, err, database.ErrPromotionRaced)

	raw := database.RawBlobStoreForTest(db.Blob())
	txn := raw.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck
	_, err = raw.Get(txn, types.BlockBlobKey(victim.Slot, victim.Hash))
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound,
		"the deleted block must not come back as a marker")
}

func TestCloseReleasesTheChunkWriter(t *testing.T) {
	t.Parallel()
	db, _ := newImmutableDB(t, 0)
	seedBlocks(t, db, fixtureModels(t, 5))
	_, err := db.PromoteThrough(context.Background(), 5)
	require.NoError(t, err)
	require.True(t, database.ImmutableStoreOpen(db))
	require.NoError(t, db.Close())
	require.False(t, database.ImmutableStoreOpen(db))
}

func TestPromotionRefusesBlockRewrittenAfterItWasRead(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 10)
	db, _ := newImmutableDB(t, 0)
	seedBlocks(t, db, blocks)
	victim := blocks[3]
	other := append(append([]byte(nil), victim.Cbor...), 0x00)
	fired := false
	database.SetImmutablePromoteFault(db, func(step string) error {
		if step != "sync" || fired {
			return nil
		}
		fired = true
		raw := database.RawBlobStoreForTest(db.Blob())
		txn := raw.NewTransaction(true)
		if err := raw.SetBlock(
			txn, victim.Slot, victim.Hash, other, victim.ID, victim.Type,
			victim.Number, victim.PrevHash,
		); err != nil {
			return err
		}
		return txn.Commit()
	})
	_, err := db.PromoteThrough(context.Background(), 10)
	require.ErrorIs(t, err, database.ErrPromotionRaced)
	raw := database.RawBlobStoreForTest(db.Blob())
	txn := raw.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck
	v, err := raw.Get(txn, types.BlockBlobKey(victim.Slot, victim.Hash))
	require.NoError(t, err)
	require.False(t, types.IsImmutableMarker(v),
		"the rewritten body must not be replaced by a marker")
}

func TestTombstoneRefusesPromotedBlock(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 20)
	db, _ := newImmutableDB(t, 0)
	seedBlocks(t, db, blocks)
	_, err := db.PromoteThrough(context.Background(), 10)
	require.NoError(t, err)

	err = database.TombstoneBlockForTest(db, blocks[2].Slot, blocks[2].Hash)
	require.ErrorIs(t, err, database.ErrRollbackBelowImmutableTip)
	got, err := db.BlockByIndex(blocks[2].ID, nil)
	require.NoError(t, err)
	require.Equal(t, blocks[2].Cbor, got.Cbor, "the promoted body survives")
	require.NoError(t,
		database.TombstoneBlockForTest(db, blocks[15].Slot, blocks[15].Hash),
		"an unpromoted block still expires")
}

// A lowering whose truncation point is not in the chunk files fails before it
// records the lowered state, so a retry, and every later open of the store,
// meets the same recorded tip rather than a state the files cannot satisfy.
func TestLowerImmutableTipLeavesStateWhenPointMissing(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 20)
	db, dir := newImmutableDB(t, 0)
	seedBlocks(t, db, blocks)
	_, err := db.PromoteThrough(context.Background(), 10)
	require.NoError(t, err)
	before, _ := db.ImmutableState()
	require.True(t, before.HasTip)

	// Damage the header hash of the block the lowering would keep.
	secPath := filepath.Join(
		dir, database.ImmutableDirName, immutable.ChunkName(0)+".secondary",
	)
	sec, err := os.ReadFile(secPath)
	require.NoError(t, err)
	at := bytes.Index(sec, blocks[4].Hash)
	require.Positive(t, at, "block 5's hash is in the secondary index")
	sec[at] ^= 0xff
	require.NoError(t, os.WriteFile(secPath, sec, 0o644)) // #nosec G306

	err = db.LowerImmutableTip(5)
	require.ErrorContains(t, err, "not found")
	after, _ := db.ImmutableState()
	require.Equal(t, before, after, "a failed lowering records nothing")
}

// replacedBodyVersions counts the stored versions of the given blocks' bp
// keys that still hold a body rather than a marker.
func replacedBodyVersions(
	t *testing.T,
	db *database.Database,
	blocks []models.Block,
) int {
	t.Helper()
	store := db.Blob()
	if u, ok := store.(interface{ Unwrap() blob.BlobStore }); ok {
		store = u.Unwrap()
	}
	badgerStore, ok := store.(*badgerblob.BlobStoreBadger)
	require.True(t, ok, "test needs the badger blob store")
	count := 0
	require.NoError(t, badgerStore.DB().View(func(txn *badgerdb.Txn) error {
		for _, b := range blocks {
			key := types.BlockBlobKey(b.Slot, b.Hash)
			opts := badgerdb.DefaultIteratorOptions
			opts.AllVersions = true
			opts.PrefetchValues = false
			opts.Prefix = key
			it := txn.NewIterator(opts)
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				if !bytes.Equal(item.Key(), key) {
					continue
				}
				if item.ValueSize() == int64(len(b.Cbor)) {
					count++
				}
			}
			it.Close()
		}
		return nil
	}))
	return count
}

func TestReclaimBlobSpaceDropsPromotedBodies(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 120)
	db, _ := newImmutableDB(t, 50)
	seedBlocks(t, db, blocks)
	require.Equal(t, len(blocks), replacedBodyVersions(t, db, blocks))

	res, err := db.PromoteThrough(context.Background(), 100)
	require.NoError(t, err)
	require.Equal(t, 100, res.Promoted)
	// Promotion alone leaves the replaced bodies in the blob store.
	require.Equal(t, len(blocks), replacedBodyVersions(t, db, blocks))

	require.NoError(t, db.ReclaimBlobSpace())

	// Only the blocks above the ceiling still hold a body.
	require.Equal(
		t, len(blocks)-100, replacedBodyVersions(t, db, blocks),
		"promoted bodies still occupy the blob store",
	)
	for _, b := range blocks {
		got, err := database.BlockByPoint(
			context.Background(), db, ocommon.NewPoint(b.Slot, b.Hash),
		)
		require.NoError(t, err)
		require.Equal(t, b.Cbor, got.Cbor)
	}
}
