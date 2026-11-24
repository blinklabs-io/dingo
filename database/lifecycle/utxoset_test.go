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

package lifecycle_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// blockPoint is the point of the chain fixture's block id, which testBlock
// derives from the id.
func blockPoint(id uint64) ocommon.Point {
	return ocommon.NewPoint(id*10, bytes.Repeat([]byte{byte(id)}, 32))
}

// applyBlocks applies one commit per block id in [from, to], each adding one
// output named for the id.
func applyBlocks(t *testing.T, store *badgerutxo.Store, from, to uint64) {
	t.Helper()
	for id := from; id <= to; id++ {
		_, err := store.Apply(
			blockPoint(id),
			[]badgerutxo.Output{{
				Ref:  badgerutxo.NewRef(bytes.Repeat([]byte{byte(id)}, 32), 0),
				Cbor: []byte{0x80, byte(id)},
			}},
			nil,
		)
		require.NoError(t, err)
	}
}

func openUtxoSetStore(t *testing.T, dir string) *badgerutxo.Store {
	t.Helper()
	store, err := badgerutxo.Open(dir)
	require.NoError(t, err)
	closing := store
	t.Cleanup(func() {
		target := closing
		closing = nil
		_ = target.Close()
	})
	return store
}

func requireSetAt(
	t *testing.T,
	store *badgerutxo.Store,
	id uint64,
) {
	t.Helper()
	tip, ok, err := store.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, blockPoint(id), tip)
	n, err := store.Count()
	require.NoError(t, err)
	require.Equal(t, id, n)
}

// A truncate rolls a set that is ahead of the target back with the database,
// and leaves a set already at or behind the target alone.
func TestTruncateRollsTheUtxoSetBack(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		applied uint64
		wantID  uint64
	}{
		{"set ahead of the target", 5, 3},
		{"set at the target", 3, 3},
		{"set behind the target", 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := buildTestChain(t, 5)
			store := openUtxoSetStore(t, t.TempDir())
			applyBlocks(t, store, 1, tc.applied)
			_, err := lifecycle.Truncate(
				context.Background(), f.db, f.blocks[2], 0,
				lifecycle.WithTruncateUtxoSet(
					dblifecycle.TruncateUtxoSet{Store: store},
				))

			require.NoError(t, err)
			requireSetAt(t, store, tc.wantID)
		})
	}
}

// A target below the set's journal floor is refused before anything is
// removed, from the database or the set, and the floor itself is accepted.
func TestTruncateRefusesATargetBelowTheUtxoSetJournalFloor(t *testing.T) {
	t.Parallel()
	f := buildTestChain(t, 5)
	store := openUtxoSetStore(t, t.TempDir())
	applyBlocks(t, store, 1, 5)
	// The records of blocks 1 to 3 go; block 4's record keeps block 3's
	// point as its previous tip, which is the floor.
	require.NoError(t, store.PruneJournal(blockPoint(4).Slot))
	set := lifecycle.WithTruncateUtxoSet(
		dblifecycle.TruncateUtxoSet{Store: store},
	)

	_, err := lifecycle.Truncate(
		context.Background(), f.db, f.blocks[1], 0, set)

	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
	require.ErrorContains(t, err, "journal floor")
	for _, b := range f.blocks {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.NoError(
			t,
			err,
			"block %d was removed by a refused truncate",
			b.ID,
		)
	}
	requireSetAt(t, store, 5)

	_, err = lifecycle.Truncate(
		context.Background(), f.db, f.blocks[2], 0, set)

	require.NoError(t, err)
	requireSetAt(t, store, 3)
}

// A snapshot taken with the set carries a consistent copy of it, under the
// same commit pause as the other stores, and a restore loads it beside the
// restored database with its journal, so it can roll back.
func TestSnapshotAndRestoreRoundTripTheUtxoSet(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	store := openUtxoSetStore(t, t.TempDir())
	applyBlocks(t, store, 1, 4)

	dir := snapshotTestDir(t, "snap-utxoset")
	key := []byte("operator-trust-root-0123456789ab")
	m, err := lifecycle.Snapshot(
		context.Background(), db, dir, lifecycle.TriggerManual,
		"test-version", "badger", "sqlite",
		lifecycle.WithManifestKey(key), lifecycle.WithUtxoSetBackup(store),
	)
	require.NoError(t, err)
	require.NotZero(t, m.UtxoSetBytes)
	payload, err := os.ReadFile(
		filepath.Join(dir, lifecycle.UtxoSetBackupFileName),
	)
	require.NoError(t, err)
	require.Equal(t, sha256Hex(payload), m.UtxoSetSHA256)
	require.NoError(t, m.Authenticate(lifecycle.WithManifestKey(key)))

	// The source moves on; the snapshot does not.
	applyBlocks(t, store, 5, 6)

	restored := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(), newTestStorageHost(t), nil, dir, restored,
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
		lifecycle.WithManifestKey(key),
	)
	require.NoError(t, err)

	got := openUtxoSetStore(t, badgerutxo.DirFor(restored))
	requireSetAt(t, got, 4)
	for id := uint64(1); id <= 4; id++ {
		cbor, ok, err := got.Get(
			badgerutxo.NewRef(bytes.Repeat([]byte{byte(id)}, 32), 0),
		)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []byte{0x80, byte(id)}, cbor)
	}
	_, err = got.Rollback(blockPoint(2))
	require.NoError(t, err)
	requireSetAt(t, got, 2)
}

// The set copy is as consistent as the commits around it: while commits land
// during the snapshot, the restored set holds exactly the outputs of the
// commits up to its tip.
func TestSnapshotOfTheUtxoSetIsConsistentUnderConcurrentCommits(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	store := openUtxoSetStore(t, t.TempDir())
	applyBlocks(t, store, 1, 10)

	var wg sync.WaitGroup
	wg.Go(func() {
		for id := uint64(11); id <= 200; id++ {
			if _, err := store.Apply(
				blockPoint(id),
				[]badgerutxo.Output{{
					Ref: badgerutxo.NewRef(
						bytes.Repeat([]byte{byte(id)}, 32), uint32(id),
					),
					Cbor: []byte{0x80},
				}},
				nil,
			); err != nil {
				return
			}
		}
	})
	dir := snapshotTestDir(t, "snap-utxoset-concurrent")
	_, err := lifecycle.Snapshot(
		context.Background(), db, dir, lifecycle.TriggerManual,
		"test-version", "badger", "sqlite", lifecycle.WithUtxoSetBackup(store),
	)
	wg.Wait()
	require.NoError(t, err)

	restored := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(), newTestStorageHost(t), nil, dir, restored,
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
	)
	require.NoError(t, err)
	got := openUtxoSetStore(t, badgerutxo.DirFor(restored))
	tip, ok, err := got.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	n, err := got.Count()
	require.NoError(t, err)
	require.Equal(t, tip.Slot/10, n, "the copy holds outputs past its tip")
}

// A snapshot without the set leaves the restored data directory without one,
// and a set payload that no longer matches its digest is refused before the
// target is touched.
func TestRestoreOfTheUtxoSetPayload(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	host := newTestStorageHost(t)
	cfg := lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()}

	t.Run("no set in the snapshot", func(t *testing.T) {
		t.Parallel()
		dir := snapshotTestDir(t, "snap-no-set")
		m, err := lifecycle.Snapshot(
			context.Background(), db, dir, lifecycle.TriggerManual,
			"test-version", "badger", "sqlite",
		)
		require.NoError(t, err)
		require.Zero(t, m.UtxoSetBytes)
		require.Empty(t, m.UtxoSetSHA256)
		require.NoFileExists(
			t,
			filepath.Join(dir, lifecycle.UtxoSetBackupFileName),
		)
		restored := filepath.Join(t.TempDir(), "restored")
		_, err = lifecycle.Restore(
			context.Background(), host, nil, dir, restored, cfg,
		)
		require.NoError(t, err)
		require.NoDirExists(t, badgerutxo.DirFor(restored))
	})

	t.Run("corrupted set payload", func(t *testing.T) {
		t.Parallel()
		store := openUtxoSetStore(t, t.TempDir())
		applyBlocks(t, store, 1, 3)
		dir := snapshotTestDir(t, "snap-corrupt-set")
		_, err := lifecycle.Snapshot(
			context.Background(), db, dir, lifecycle.TriggerManual,
			"test-version", "badger", "sqlite",
			lifecycle.WithUtxoSetBackup(store),
		)
		require.NoError(t, err)
		path := filepath.Join(dir, lifecycle.UtxoSetBackupFileName)
		payload, err := os.ReadFile(path)
		require.NoError(t, err)
		payload[len(payload)/2] ^= 0xff
		require.NoError(t, os.WriteFile(path, payload, 0o600))
		restored := filepath.Join(t.TempDir(), "restored")
		_, err = lifecycle.Restore(
			context.Background(), host, nil, dir, restored, cfg,
		)
		require.ErrorIs(t, err, lifecycle.ErrSnapshotPayloadMismatch)
		require.NoDirExists(t, restored)
	})
}

// openCheckpointedSet applies blocks 1 to checkpointAt to the set under dir,
// writes a checkpoint of it, applies blocks up to tip and prunes the journal
// past block pruneTo.
func openCheckpointedSet(
	t *testing.T,
	dir, ckptDir string,
	checkpointAt, tip, pruneTo uint64,
) {
	t.Helper()
	store, err := badgerutxo.Open(dir)
	require.NoError(t, err)
	applyBlocks(t, store, 1, checkpointAt)
	require.NoError(t, store.Close())
	set, err := utxoset.Open(utxoset.Config{
		Dir: dir, Only: true, Authoritative: true, CheckpointDir: ckptDir,
	})
	require.NoError(t, err)
	_, err = set.WriteCheckpoint(t.Context())
	require.NoError(t, err)
	require.NoError(t, set.Close())
	store, err = badgerutxo.Open(dir)
	require.NoError(t, err)
	applyBlocks(t, store, checkpointAt+1, tip)
	require.NoError(t, store.PruneJournal(blockPoint(pruneTo).Slot))
	require.NoError(t, store.Close())
}

// A target below the set's journal is accepted when a checkpoint of the chain
// is at or below it: the set is left for the next start to restore, so the
// database truncates and the set stays as it was.
func TestTruncateBelowTheUtxoSetJournalUsesACheckpoint(t *testing.T) {
	t.Parallel()
	f := buildTestChain(t, 5)
	dir := filepath.Join(t.TempDir(), "set")
	ckptDir := filepath.Join(t.TempDir(), "ckpt")
	openCheckpointedSet(t, dir, ckptDir, 2, 5, 4)
	store := openUtxoSetStore(t, dir)
	floor, _, err := store.JournalFloor()
	require.NoError(t, err)
	require.Greater(t, floor.Slot, f.blocks[1].Slot,
		"the target is below the journal")
	opts := lifecycle.WithTruncateUtxoSet(dblifecycle.TruncateUtxoSet{
		Store:       store,
		Checkpoints: utxoset.NewOfflineCheckpoints(store, ckptDir),
	})

	_, err = lifecycle.Truncate(
		context.Background(), f.db, f.blocks[1], 0, opts)

	require.NoError(t, err)
	tip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.blocks[1].Slot, tip.Point.Slot)
	requireSetAt(t, store, 5)
}

// With no checkpoint at or below the target the truncate is refused before
// anything is removed, and the error names the earliest slot that can be
// reached: the older of the journal floor and the oldest checkpoint.
func TestTruncateBelowEveryUtxoSetCheckpointNamesTheEarliestSlot(
	t *testing.T,
) {
	t.Parallel()
	f := buildTestChain(t, 5)
	dir := filepath.Join(t.TempDir(), "set")
	ckptDir := filepath.Join(t.TempDir(), "ckpt")
	openCheckpointedSet(t, dir, ckptDir, 2, 5, 4)
	store := openUtxoSetStore(t, dir)
	opts := lifecycle.WithTruncateUtxoSet(dblifecycle.TruncateUtxoSet{
		Store:       store,
		Checkpoints: utxoset.NewOfflineCheckpoints(store, ckptDir),
	})

	_, err := lifecycle.Truncate(
		context.Background(), f.db, f.blocks[0], 0, opts)

	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
	require.ErrorContains(t, err, "journal floor")
	require.ErrorContains(
		t, err,
		fmt.Sprintf("earliest reachable slot is %d", blockPoint(2).Slot),
	)
	tip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.blocks[4].Slot, tip.Point.Slot)
}
