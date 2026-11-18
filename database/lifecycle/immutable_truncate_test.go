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
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func promotedTestBlock(id uint64) models.Block {
	b := testBlock(id, byte(id))
	// [bytes, bytes]: a CBOR array the chunk writer can wrap.
	b.Cbor = []byte{0x82, 0x41, byte(id), 0x41, 0x02}
	return b
}

func TestDeleteBlocksAfterRefusesBelowImmutableTip(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:   t.TempDir(),
		Immutable: database.ImmutableConfig{Enabled: true, ChunkSlots: 1000},
	})
	require.NoError(t, err)
	for id := uint64(1); id <= 8; id++ {
		require.NoError(t, db.BlockCreate(promotedTestBlock(id), nil))
	}
	_, err = db.PromoteThrough(context.Background(), 5)
	require.NoError(t, err)

	_, err = lifecycle.DeleteBlocksAfter(context.Background(), db, 3, 8, 0)
	require.ErrorIs(t, err, database.ErrRollbackBelowImmutableTip)
	for id := uint64(1); id <= 8; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.NoErrorf(t, err, "block %d must survive the refusal", id)
	}

	// An operator truncate lowers the tip and the chunk files first.
	require.NoError(t, db.LowerImmutableTip(3))
	st, ok := db.ImmutableState()
	require.True(t, ok)
	require.Equal(t, uint64(3), st.TipIndex)
	require.Equal(t, uint64(30), st.TipSlot)
	n, err := lifecycle.DeleteBlocksAfter(context.Background(), db, 3, 8, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(5), n)

	s, err := immutable.OpenStore(
		db.ImmutableDir(), immutable.Geometry{ChunkSlots: 1000},
	)
	require.NoError(t, err)
	defer s.Close()
	tip, ok := s.Tip()
	require.True(t, ok)
	require.Equal(t, uint64(30), tip.Slot)
	for id := uint64(1); id <= 3; id++ {
		got, err := db.BlockByIndex(id, nil)
		require.NoError(t, err)
		require.True(t, bytes.Equal(promotedTestBlock(id).Cbor, got.Cbor))
	}
	// Repeating the lowering, as a resumed truncate does, changes nothing.
	require.NoError(t, db.LowerImmutableTip(3))
}

// promotedSnapshotDB returns a database whose first promoted blocks span
// several chunk files (chunks hold slots 0-24, 25-49, ...) and whose blob store
// still holds the unpromoted tail.
func promotedSnapshotDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:   t.TempDir(),
		Immutable: database.ImmutableConfig{Enabled: true, ChunkSlots: 25},
	})
	require.NoError(t, err)
	for id := uint64(1); id <= 10; id++ {
		require.NoError(t, db.BlockCreate(promotedTestBlock(id), nil))
	}
	_, err = db.PromoteThrough(context.Background(), 6)
	require.NoError(t, err)
	return db
}

func chunkDirDigests(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	files, err := immutable.ListFiles(dir)
	require.NoError(t, err)
	out := make(map[string][]byte, len(files))
	for _, f := range files {
		b, err := os.ReadFile(filepath.Join(dir, f.Name))
		require.NoError(t, err)
		out[f.Name] = b
	}
	return out
}

func TestSnapshotRestoresPromotedDatabase(t *testing.T) {
	t.Parallel()
	db := promotedSnapshotDB(t)
	atSnapshot := chunkDirDigests(t, db.ImmutableDir())
	require.Greater(t, len(atSnapshot), 6, "blocks must span several chunks")

	dir := snapshotTestDir(t, "snap-promoted")
	m, err := lifecycle.Snapshot(
		context.Background(), db, dir, lifecycle.TriggerManual,
		"test-version", "badger", "sqlite",
	)
	require.NoError(t, err)
	require.NotEmpty(t, m.ImmutableIndexSHA256)
	require.Equal(t, lifecycle.ManifestFormatVersionImmutable, m.FormatVersion)

	// Promotion after the snapshot must not change what it captured.
	_, err = db.PromoteThrough(context.Background(), 9)
	require.NoError(t, err)

	restoredDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(), newTestStorageHost(t), nil, dir, restoredDir,
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
	)
	require.NoError(t, err)
	require.Equal(
		t,
		atSnapshot,
		chunkDirDigests(
			t,
			filepath.Join(restoredDir, database.ImmutableDirName),
		),
	)

	restored, err := dbtest.NewDatabase(
		t,
		&database.Config{DataDir: restoredDir},
	)
	require.NoError(t, err)
	st, ok := restored.ImmutableState()
	require.True(t, ok)
	require.Equal(t, uint64(6), st.TipIndex)
	require.NoError(t, restored.ReconcileImmutable())
	for id := uint64(1); id <= 10; id++ {
		got, err := restored.BlockByIndex(id, nil)
		require.NoError(t, err)
		require.Truef(
			t,
			bytes.Equal(promotedTestBlock(id).Cbor, got.Cbor),
			"block %d",
			id,
		)
	}
}

func TestSnapshotOfUnpromotedImmutableDatabaseHasNoIndex(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:   t.TempDir(),
		Immutable: database.ImmutableConfig{Enabled: true, ChunkSlots: 25},
	})
	require.NoError(t, err)
	require.NoError(t, db.BlockCreate(promotedTestBlock(1), nil))
	m, err := lifecycle.Snapshot(
		context.Background(), db, snapshotTestDir(t, "snap-unpromoted"),
		lifecycle.TriggerManual, "test-version", "badger", "sqlite",
	)
	require.NoError(t, err)
	require.Empty(t, m.ImmutableIndexSHA256)
	require.Equal(t, lifecycle.ManifestFormatVersion, m.FormatVersion)
}

// A chunk file replaced or cut short after the snapshot is refused before the
// target is created. The file is replaced rather than edited because the
// snapshot may share its bytes with the live chunk through a hard link.
func TestRestoreRejectsTamperedChunkFile(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"flipped byte", func(b []byte) []byte {
			out := append([]byte(nil), b...)
			out[len(out)/2] ^= 0xFF
			return out
		}},
		{"truncated", func(b []byte) []byte { return b[:len(b)-1] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := promotedSnapshotDB(t)
			dir := snapshotTestDir(t, "snap-tamper")
			_, err := lifecycle.Snapshot(
				context.Background(), db, dir, lifecycle.TriggerManual,
				"test-version", "badger", "sqlite",
			)
			require.NoError(t, err)
			path := filepath.Join(dir, "immutable-00000.chunk")
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.NoError(t, os.Remove(path))
			require.NoError(t, os.WriteFile(path, tc.mutate(data), 0o600))

			target := filepath.Join(t.TempDir(), "restored")
			_, err = lifecycle.Restore(
				context.Background(),
				newTestStorageHost(t),
				nil,
				dir,
				target,
				lifecycle.RestoreStorageConfig{
					Blob: testutil.BadgerBlobConfig(),
				},
			)
			require.ErrorIs(t, err, lifecycle.ErrSnapshotPayloadMismatch)
			require.NoDirExists(t, target)
		})
	}
}

// A snapshot waits for promotion to finish rather than capturing chunk files
// mid-step, and gives up when its context ends.
func TestSnapshotWaitsForImmutableLock(t *testing.T) {
	t.Parallel()
	db := promotedSnapshotDB(t)
	unlock, err := db.LockImmutable(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		200*time.Millisecond,
	)
	defer cancel()
	dir := snapshotTestDir(t, "snap-locked")
	_, err = lifecycle.Snapshot(
		ctx,
		db,
		dir,
		lifecycle.TriggerManual,
		"test-version",
		"badger",
		"sqlite",
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoDirExists(t, dir)

	unlock()
	_, err = lifecycle.Snapshot(
		context.Background(), db, dir, lifecycle.TriggerManual,
		"test-version", "badger", "sqlite",
	)
	require.NoError(t, err)
}

// lockProbeStore checks, as the blob backup starts, whether promotion could
// take the ImmutableDB lock.
type lockProbeStore struct {
	blob.BlobStore
	db       *database.Database
	unlocked atomic.Bool
}

func (s *lockProbeStore) Unwrap() blob.BlobStore { return s.BlobStore }

func (s *lockProbeStore) Backup(ctx context.Context, w io.Writer) error {
	probe, cancel := context.WithTimeout(
		context.Background(), 100*time.Millisecond,
	)
	defer cancel()
	if unlock, err := s.db.LockImmutable(probe); err == nil {
		s.unlocked.Store(true)
		unlock()
	}
	return s.BlobStore.(blob.Backuper).Backup(ctx, w)
}

// Promotion commits its markers outside the commit barrier, so the ImmutableDB
// lock must cover the blob backup too. A promotion between the chunk capture
// and the backup would put markers in the snapshot for chunk bytes it lacks.
func TestSnapshotHoldsImmutableLockThroughBlobBackup(t *testing.T) {
	t.Parallel()
	db := promotedSnapshotDB(t)
	probe := &lockProbeStore{BlobStore: db.Blob(), db: db}
	db.SetBlobStore(probe)

	dir := snapshotTestDir(t, "snap-lock-probe")
	_, err := lifecycle.Snapshot(
		context.Background(), db, dir, lifecycle.TriggerManual,
		"test-version", "badger", "sqlite",
	)
	require.NoError(t, err)
	require.False(
		t, probe.unlocked.Load(),
		"promotion could run between the chunk capture and the blob backup",
	)
}

// A local snapshot links the completed chunks instead of copying them; a
// mainnet ImmutableDB is far too large to copy on every snapshot.
func TestSnapshotLinksCompletedLocalChunks(t *testing.T) {
	t.Parallel()
	db := promotedSnapshotDB(t)
	dir := snapshotTestDir(t, "snap-linked")
	_, err := lifecycle.Snapshot(
		context.Background(), db, dir, lifecycle.TriggerManual,
		"test-version", "badger", "sqlite",
	)
	require.NoError(t, err)
	src, err := os.Stat(filepath.Join(db.ImmutableDir(), "00000.chunk"))
	require.NoError(t, err)
	dst, err := os.Stat(filepath.Join(dir, "immutable-00000.chunk"))
	require.NoError(t, err)
	require.True(t, os.SameFile(src, dst), "completed chunk must be linked")
}
