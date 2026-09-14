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
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

// TestCheckpointWALTruncatesFile proves checkpointWAL's central claim: a
// PASSIVE checkpoint (what wal_autocheckpoint invokes automatically after
// every commit) never shrinks the -wal file's on-disk size even when it
// fully succeeds, but PRAGMA wal_checkpoint(TRUNCATE) does. Without this,
// dingo_database_sql_wal_bytes -- a plain os.Stat of that file, see
// metrics.go -- can never show a single decrease no matter how well
// checkpointing is otherwise working underneath.
func TestCheckpointWALTruncatesFile(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, writeDB, _, err := openSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	_, err = writeDB.ExecContext(
		t.Context(),
		"CREATE TABLE checkpoint_probe (n INTEGER)",
	)
	require.NoError(t, err)
	// wal_autocheckpoint's own threshold is 10000 pages (~40MB); stay well
	// under it so this test observes checkpointWAL's own effect rather than
	// the automatic pragma firing mid-loop.
	for i := range 200 {
		_, err := writeDB.ExecContext(
			t.Context(),
			"INSERT INTO checkpoint_probe (n) VALUES (?)",
			i,
		)
		require.NoError(t, err)
	}

	walPath := filepath.Join(dataDir, "metadata.sqlite-wal")
	before, err := os.Stat(walPath)
	require.NoError(t, err)
	require.Positive(
		t, before.Size(),
		"WAL file should hold uncheckpointed frames before the forced checkpoint",
	)

	require.NoError(t, checkpointWAL(writeDB, slog.Default())(t.Context()))

	after, err := os.Stat(walPath)
	require.NoError(t, err)
	require.Zero(
		t, after.Size(),
		"a TRUNCATE checkpoint should shrink the WAL file to zero bytes",
	)
}

// TestCheckpointWALSerializesAgainstWriteTransaction proves the correctness
// argument behind issuing the periodic checkpoint against writeDB rather
// than readDB: writeDB is capped at SetMaxOpenConns(1) (see
// openSQLStore/sqliteCommonPragmas), so a checkpoint query issued against it
// cannot be scheduled onto a second connection -- it must wait for the sole
// connection like any other writeDB caller, which makes it impossible for
// checkpointWAL to run concurrently with (or interrupt) an in-progress write
// transaction on the same pool.
func TestCheckpointWALSerializesAgainstWriteTransaction(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, writeDB, _, err := openSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	require.Equal(t, 1, writeDB.Stats().MaxOpenConnections)

	_, err = writeDB.ExecContext(
		t.Context(),
		"CREATE TABLE checkpoint_probe (n INTEGER)",
	)
	require.NoError(t, err)

	txStarted := make(chan struct{})
	var mu sync.Mutex
	var commitAt time.Time

	var wg sync.WaitGroup
	wg.Go(func() {
		tx, err := writeDB.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(
			t.Context(),
			"INSERT INTO checkpoint_probe (n) VALUES (1)",
		)
		require.NoError(t, err)
		close(txStarted)
		// Hold the sole writeDB connection open long enough that a
		// concurrent checkpoint attempt has to wait for it if checkpointWAL
		// is correctly serialized against an in-flight write transaction.
		time.Sleep(200 * time.Millisecond)
		require.NoError(t, tx.Commit())
		mu.Lock()
		commitAt = time.Now()
		mu.Unlock()
	})

	<-txStarted
	require.NoError(t, checkpointWAL(writeDB, slog.Default())(t.Context()))
	checkpointReturnedAt := time.Now()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.False(
		t,
		checkpointReturnedAt.Before(commitAt),
		"checkpointWAL must not return before the in-flight write "+
			"transaction it overlapped with commits",
	)
}
