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

package sqlite

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

// Under synchronous=NORMAL a commit sits in the WAL until SQLite checkpoints.
// SyncCommits must move every commit into the database file, which is what a
// crash that loses the unsynced WAL leaves behind: the main file alone, read
// here from a copy that excludes the WAL.
func TestSyncCommitsPutsEveryCommitInTheDatabaseFile(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, writeDB, _, err := openSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() { _ = store.Close() })
	var syncer metadata.CommitSyncer = store

	_, err = writeDB.ExecContext(t.Context(), "CREATE TABLE sync_probe (id INTEGER)")
	require.NoError(t, err)
	_, err = writeDB.ExecContext(t.Context(), "INSERT INTO sync_probe VALUES (1), (2), (3)")
	require.NoError(t, err)

	mainOnly := func() int {
		raw, err := os.ReadFile(filepath.Join(dataDir, "metadata.sqlite"))
		require.NoError(t, err)
		dir := t.TempDir()
		path := filepath.Join(dir, "copy.sqlite")
		require.NoError(t, os.WriteFile(path, raw, 0o600))
		db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
		require.NoError(t, err)
		defer db.Close()
		var n int
		if err := db.QueryRowContext(
			t.Context(), "SELECT count(*) FROM sync_probe",
		).Scan(&n); err != nil {
			return -1
		}
		return n
	}
	require.Equal(t, -1, mainOnly(),
		"the commits are still only in the WAL before the sync")
	require.NoError(t, syncer.SyncCommits(t.Context()))
	require.Equal(t, 3, mainOnly(),
		"after the sync the database file holds every commit")
}
