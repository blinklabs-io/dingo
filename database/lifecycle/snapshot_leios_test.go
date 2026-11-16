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
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/leiosdb"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// The LeiosDB holds certified closures that no other store keeps, so a
// snapshot carries it and a restore brings it back.
func TestSnapshotRestoreCarriesLeiosDB(t *testing.T) {
	t.Parallel()

	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))
	store, err := src.LeiosDB()
	require.NoError(t, err)
	ref := leiosdb.Ref{Slot: 5, Hash: bytes.Repeat([]byte{0xEB}, 32)}
	txs := []cbor.RawMessage{{0x41, 0x01}}
	require.NoError(t, store.PutVolatile(
		ref, leiosdb.Content{Manifest: []byte("manifest"), Txs: txs},
	))
	require.NoError(t, store.PutClosure(ref, []byte("closure")))
	_, err = store.Pin(
		ref,
		leiosdb.Ref{Slot: 6, Hash: bytes.Repeat([]byte{0xCC}, 32)},
	)
	require.NoError(t, err)

	snapshotDir := snapshotTestDir(t, "snap-leios")
	key := []byte("operator-trust-root-0123456789ab")
	snapMan, err := lifecycle.Snapshot(
		context.Background(), src, snapshotDir, lifecycle.TriggerManual,
		"test", "badger", "sqlite", lifecycle.WithManifestKey(key),
	)
	require.NoError(t, err)
	require.NotZero(t, snapMan.LeiosBytes)
	backup, err := os.ReadFile(
		filepath.Join(snapshotDir, lifecycle.LeiosBackupFileName),
	)
	require.NoError(t, err)
	require.Equal(t, sha256Hex(backup), snapMan.LeiosSHA256)

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(),
		newTestStorageHost(t),
		testDestinationRegistry,
		snapshotDir,
		targetDir,
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
		lifecycle.WithManifestKey(key),
	)
	require.NoError(t, err)

	restored, err := dbtest.NewDatabase(t, &database.Config{DataDir: targetDir})
	require.NoError(t, err)
	got, err := restored.LeiosDB()
	require.NoError(t, err)
	closure, err := got.GetClosure(ref)
	require.NoError(t, err)
	require.Equal(t, []byte("closure"), closure)
	content, err := got.Get(ref)
	require.NoError(t, err)
	require.Equal(t, txs, content.Txs)
	pinned, err := got.IsPinned(ref)
	require.NoError(t, err)
	require.True(t, pinned)
}

// A restore refuses a snapshot whose Leios backup differs from what the
// manifest declares.
func TestRestoreRejectsTamperedLeiosBackup(t *testing.T) {
	t.Parallel()

	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))
	store, err := src.LeiosDB()
	require.NoError(t, err)
	require.NoError(t, store.PutClosure(
		leiosdb.Ref{Slot: 5, Hash: bytes.Repeat([]byte{0xEB}, 32)},
		[]byte("closure"),
	))
	snapshotDir := snapshotTestDir(t, "snap-leios-tamper")
	_, err = lifecycle.Snapshot(
		context.Background(), src, snapshotDir, lifecycle.TriggerManual,
		"test", "badger", "sqlite",
	)
	require.NoError(t, err)
	path := filepath.Join(snapshotDir, lifecycle.LeiosBackupFileName)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[len(data)/2] ^= 0xFF
	require.NoError(t, os.WriteFile(path, data, 0o600))

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(),
		newTestStorageHost(t),
		testDestinationRegistry,
		snapshotDir,
		targetDir,
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
	)
	require.ErrorIs(t, err, lifecycle.ErrSnapshotPayloadMismatch)
}

// An offline snapshot opens a fresh Database whose LeiosDB is not yet open;
// the endorser-block store already on disk must still be backed up.
func TestSnapshotOfflineCarriesOnDiskLeiosDB(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	ref := leiosdb.Ref{Slot: 5, Hash: bytes.Repeat([]byte{0xEB}, 32)}
	seed, err := leiosdb.Open(leiosdb.Options{DataDir: dataDir})
	require.NoError(t, err)
	require.NoError(t, seed.PutVolatile(
		ref, leiosdb.Content{Manifest: []byte("manifest")},
	))
	require.NoError(t, seed.PutClosure(ref, []byte("closure")))
	require.NoError(t, seed.Close())

	src, err := dbtest.NewDatabase(t, &database.Config{DataDir: dataDir})
	require.NoError(t, err)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := snapshotTestDir(t, "snap-leios-offline")
	key := []byte("operator-trust-root-0123456789ab")
	snapMan, err := lifecycle.Snapshot(
		context.Background(), src, snapshotDir, lifecycle.TriggerManual,
		"test", "badger", "sqlite", lifecycle.WithManifestKey(key),
	)
	require.NoError(t, err)
	require.NotZero(t, snapMan.LeiosBytes)

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(),
		newTestStorageHost(t),
		testDestinationRegistry,
		snapshotDir,
		targetDir,
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
		lifecycle.WithManifestKey(key),
	)
	require.NoError(t, err)

	restored, err := dbtest.NewDatabase(t, &database.Config{DataDir: targetDir})
	require.NoError(t, err)
	got, err := restored.LeiosDB()
	require.NoError(t, err)
	closure, err := got.GetClosure(ref)
	require.NoError(t, err)
	require.Equal(t, []byte("closure"), closure)
}
