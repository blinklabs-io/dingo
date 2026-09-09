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

package conformance

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// sqliteFileIdentity returns the metadata database's file identity, which
// distinguishes "emptied in place" from "deleted and recreated". The
// close-and-reopen path this change replaces removed the whole data directory
// on every Reset, so it produced a different file each time.
//
// os.SameFile rather than a syscall.Stat_t inode: Stat_t does not exist on
// Windows, and this package is built untagged by the Windows CI job.
func sqliteFileIdentity(t *testing.T, dataDir string) os.FileInfo {
	t.Helper()
	info, err := os.Stat(filepath.Join(dataDir, sqliteMetadataFileName))
	require.NoError(t, err, "metadata database must exist after reset")
	return info
}

// TestSqliteResetKeepsTheSameDatabaseFile is the regression test for the whole
// change. Reset must empty the backend in place rather than delete the data
// directory and rebuild it, because rebuilding re-runs every migration -- 260
// DDL statements, once per vector, ~315 vectors per replay.
//
// A different file after Reset means the recreate path ran.
func TestSqliteResetKeepsTheSameDatabaseFile(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := newDingoStateManagerAt(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	before := sqliteFileIdentity(t, dataDir)
	require.NoError(t, sm.Reset())
	after := sqliteFileIdentity(t, dataDir)

	require.True(
		t,
		os.SameFile(before, after),
		"Reset must empty the metadata database in place, not recreate it",
	)
}

// TestSqliteResetPreservesMigrationLedger pins the consequence that makes the
// in-place reset worth doing: schema_migrations survives, so the next
// construction has nothing to re-apply.
func TestSqliteResetPreservesMigrationLedger(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := newDingoStateManagerAt(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	resetter, err := newSqliteResetter(sqliteMetadataPath(dataDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = resetter.Close() })

	var before int
	require.NoError(
		t,
		resetter.db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).
			Scan(&before),
	)
	require.Positive(t, before, "precondition: migrations were recorded")

	require.NoError(t, sm.Reset())

	var after int
	require.NoError(
		t,
		resetter.db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).
			Scan(&after),
	)
	require.Equal(t, before, after, "migration ledger must survive Reset")
}

// TestSqliteResetClearsBlobStore pins the half of Reset that the metadata
// truncate does not cover. The previous path cleared blobs incidentally, by
// removing the whole data directory; emptying only the metadata tables would
// silently leave a prior vector's UTxO CBOR readable.
func TestSqliteResetClearsBlobStore(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := newDingoStateManagerAt(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	key := []byte("conformance-reset-probe")
	value := []byte("stale vector bytes")

	blobStore := sm.db.Blob()
	require.NotNil(t, blobStore)
	// Registered before the assertions below: a failing Set or Commit would
	// otherwise abort the test with badger's single write transaction still
	// open, and every later blob write in this process would block on it.
	// Rollback after a successful Commit is a no-op.
	txn := blobStore.NewTransaction(true)
	t.Cleanup(func() { _ = txn.Rollback() })
	require.NoError(t, blobStore.Set(txn, key, value))
	require.NoError(t, txn.Commit())

	// Confirm it is actually readable before Reset, so a broken write cannot
	// make the post-Reset absence look like success.
	readTxn := sm.db.Blob().NewTransaction(false)
	t.Cleanup(func() { _ = readTxn.Rollback() })
	got, err := sm.db.Blob().Get(readTxn, key)
	require.NoError(t, err)
	require.Equal(t, value, got, "precondition: blob is readable")

	require.NoError(t, sm.Reset())

	afterTxn := sm.db.Blob().NewTransaction(false)
	t.Cleanup(func() { _ = afterTxn.Rollback() })
	_, err = sm.db.Blob().Get(afterTxn, key)
	require.Error(t, err, "Reset must clear the blob store")
}

// TestResetRunsEachWipeHookIndependently pins that Reset checks wipeMetadata
// and wipeBlob separately. They were nested, so a backend that set only
// wipeBlob would have silently skipped emptying its blob store; no backend
// does that today, which is exactly why the contract needs a test rather than
// a convention.
func TestResetRunsEachWipeHookIndependently(t *testing.T) {
	for _, tc := range []struct {
		name           string
		metadata, blob bool
	}{
		{name: "both", metadata: true, blob: true},
		{name: "metadata only", metadata: true},
		{name: "blob only", blob: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var metadataCalled, blobCalled bool
			m := &DingoStateManager{}
			if tc.metadata {
				m.wipeMetadata = func() error {
					metadataCalled = true
					return nil
				}
			}
			if tc.blob {
				m.wipeBlob = func() error {
					blobCalled = true
					return nil
				}
			}

			require.NoError(t, m.Reset())

			require.Equal(t, tc.metadata, metadataCalled, "wipeMetadata")
			require.Equal(t, tc.blob, blobCalled, "wipeBlob")
		})
	}
}
