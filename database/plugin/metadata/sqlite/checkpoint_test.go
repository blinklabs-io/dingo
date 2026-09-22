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
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
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
		t,
		before.Size(),
		"WAL file should hold uncheckpointed frames before the forced checkpoint",
	)

	databaseURI := sqliteFileURI(filepath.Join(dataDir, "metadata.sqlite"))
	require.NoError(
		t,
		checkpointWAL(databaseURI, slog.Default())(t.Context()),
	)

	after, err := os.Stat(walPath)
	require.NoError(t, err)
	require.Zero(
		t, after.Size(),
		"a TRUNCATE checkpoint should shrink the WAL file to zero bytes",
	)
}

// TestCheckpointWALDoesNotBlockWriteBehindReaderSnapshot is the regression
// test for the writeDB-based design checkpointWAL used before: issuing
// PRAGMA wal_checkpoint(TRUNCATE) against writeDB (SetMaxOpenConns(1))
// blocked that sole connection -- and therefore every other write -- for up
// to the full busy_timeout(30000) whenever a readDB snapshot was open, since
// the checkpoint's busy handler has to wait out that snapshot before it can
// truncate. Measured against that design: a checkpoint attempt with one open
// readDB snapshot took 30.04s, blocked a concurrent writeDB insert for
// 29.99s of that, and still finished with busy=1 (no truncation).
// checkpointWAL now issues the pragma from a dedicated connection with a
// short busy_timeout instead, so it fails fast on the same busy=1 outcome
// and a concurrent writeDB write is never blocked by it.
//
// Not t.Parallel: every assertion below is a wall-clock duration, which is a
// process-wide measurement in the same sense as testing.AllocsPerRun or
// runtime.NumGoroutine -- what it reports depends on whatever else is running
// in the process, not only on the code under test. Run in parallel with the
// rest of this package it measured the runner's load as much as the
// checkpoint, and failed on a loaded CI runner while the behaviour it guards
// was intact.
func TestCheckpointWALDoesNotBlockWriteBehindReaderSnapshot(t *testing.T) {
	dataDir := t.TempDir()
	store, writeDB, readDB, err := openSQLStore(
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
	_, err = writeDB.ExecContext(
		t.Context(),
		"INSERT INTO checkpoint_probe (n) VALUES (0)",
	)
	require.NoError(t, err)

	// Hold a readDB snapshot open so a TRUNCATE checkpoint can never
	// complete (busy=1): this is the condition under which the old
	// writeDB-based design blocked concurrent writes for up to
	// busy_timeout(30000).
	readTx, err := readDB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = readTx.Rollback()
	})
	// The design this guards blocked for the full busy_timeout(30000); a
	// healthy dedicated-connection attempt gives up after
	// checkpointBusyTimeout (250ms). Bound the gap well below 30s rather
	// than just above 250ms: these are wall-clock measurements on shared
	// runners, and a bound close to the healthy time fails on load rather
	// than on the defect it exists to catch.
	const maxUnblocked = 10 * time.Second

	var probe int
	require.NoError(
		t,
		readTx.QueryRowContext(
			t.Context(),
			"SELECT n FROM checkpoint_probe LIMIT 1",
		).Scan(&probe),
	)

	type writeResult struct {
		duration time.Duration
		err      error
	}
	writeResultCh := make(chan writeResult, 1)
	go func() {
		started := time.Now()
		_, execErr := writeDB.ExecContext(
			t.Context(),
			"INSERT INTO checkpoint_probe (n) VALUES (1)",
		)
		writeResultCh <- writeResult{
			duration: time.Since(started),
			err:      execErr,
		}
	}()

	var logBuf bytes.Buffer
	checkpointLogger := slog.New(slog.NewTextHandler(&logBuf, nil))

	databaseURI := sqliteFileURI(filepath.Join(dataDir, "metadata.sqlite"))
	checkpointStarted := time.Now()
	require.NoError(
		t,
		checkpointWAL(databaseURI, checkpointLogger)(t.Context()),
	)
	checkpointDuration := time.Since(checkpointStarted)
	require.Less(
		t, checkpointDuration, maxUnblocked,
		"a dedicated-connection checkpoint attempt should fail fast on a "+
			"blocked truncate, not wait out busy_timeout(30000)",
	)
	require.Contains(
		t, logBuf.String(), "could not fully complete",
		"the open reader snapshot should make the truncate impossible, "+
			"reproducing the busy=1 condition the old design blocked on",
	)

	result := testutil.RequireReceive(
		t, writeResultCh, maxUnblocked,
		"concurrent writeDB insert must not be blocked behind the "+
			"checkpoint attempt",
	)
	require.NoError(t, result.err)
	require.Less(
		t, result.duration, maxUnblocked,
		"a concurrent write must not be blocked behind the checkpoint attempt",
	)
}
