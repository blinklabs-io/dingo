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
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestCheckpointWALDoesNotTruncateBusyPassiveCheckpoint(t *testing.T) {
	var modes []string
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	err := checkpointWALWith(
		context.Background(),
		logger,
		func(_ context.Context, mode string) (int, int, int, error) {
			modes = append(modes, mode)
			if mode == "PASSIVE" {
				return 1, 10, 5, nil
			}
			t.Fatal("TRUNCATE must not run after an incomplete PASSIVE checkpoint")
			return 0, 0, 0, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"PASSIVE"}, modes)
}

func TestCheckpointWALTruncatesOnlyAfterPassiveDrainsWAL(t *testing.T) {
	var modes []string
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	err := checkpointWALWith(
		context.Background(),
		logger,
		func(_ context.Context, mode string) (int, int, int, error) {
			modes = append(modes, mode)
			return 0, 10, 10, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"PASSIVE", "TRUNCATE"}, modes)
}

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
func TestCheckpointWALDoesNotBlockWriteBehindReaderSnapshot(t *testing.T) {
	t.Parallel()
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
		t, checkpointDuration, 2*time.Second,
		"a dedicated-connection checkpoint attempt should fail fast on a "+
			"blocked truncate, not wait out busy_timeout(30000)",
	)
	require.Contains(
		t, logBuf.String(), "could not fully complete",
		"the open reader snapshot should make the truncate impossible, "+
			"reproducing the busy=1 condition the old design blocked on",
	)

	result := testutil.RequireReceive(
		t, writeResultCh, 2*time.Second,
		"concurrent writeDB insert must not be blocked behind the "+
			"checkpoint attempt",
	)
	require.NoError(t, result.err)
	require.Less(
		t, result.duration, 2*time.Second,
		"a concurrent write must not be blocked behind the checkpoint attempt",
	)
}

// TestCheckpointWALDoesNotHoldWriterLockBehindReaderSnapshot is the
// regression test for PRAGMA wal_checkpoint(TRUNCATE) being issued directly.
// TRUNCATE (unlike PASSIVE) acquires SQLite's WAL writer lock before it
// copies anything and holds it while its busy handler waits out the reader
// snapshot that prevents backfill, so every write arriving inside that window
// is stalled for the checkpoint connection's whole busy_timeout. Measured
// against the direct-TRUNCATE code with the WAL holding frames a reader
// snapshot pins: the checkpoint call ran 298-519ms and a write issued inside
// that window blocked 229-429ms; with the PASSIVE gate the same write
// completes in well under a millisecond because TRUNCATE is never reached.
//
// A writer started alongside the checkpoint does not reproduce this: it wins
// the race against opening the checkpoint connection and completes before the
// writer lock is taken, which is why TestCheckpointWALDoesNotBlockWriteBehind
// ReaderSnapshot passes either way. The probe below instead samples the writer
// lock continuously for the whole checkpoint call.
func TestCheckpointWALDoesNotHoldWriterLockBehindReaderSnapshot(t *testing.T) {
	t.Parallel()
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
		"CREATE TABLE checkpoint_probe (n INTEGER, payload BLOB)",
	)
	require.NoError(t, err)
	payload := make([]byte, 4096)
	writeRows := func(from, count int) {
		for i := range count {
			_, execErr := writeDB.ExecContext(
				t.Context(),
				"INSERT INTO checkpoint_probe (n, payload) VALUES (?, ?)",
				from+i,
				payload,
			)
			require.NoError(t, execErr)
		}
	}
	// Enough frames that a checkpoint has real work to do while it holds the
	// writer lock; below wal_autocheckpoint's own 10000-page threshold.
	writeRows(0, 4000)

	// A reader snapshot opened here pins the frames written after it, so no
	// checkpoint mode can drain the WAL completely and TRUNCATE must wait.
	readTx, err := readDB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = readTx.Rollback()
	})
	var pinned int
	require.NoError(
		t,
		readTx.QueryRowContext(
			t.Context(),
			"SELECT n FROM checkpoint_probe LIMIT 1",
		).Scan(&pinned),
	)
	writeRows(100_000, 2000)

	// Probe the writer lock from a dedicated connection that never waits, so
	// a single observation of SQLITE_BUSY means the checkpoint was holding
	// it at that moment rather than that the probe was impatient.
	probeDB, err := sqlstore.OpenDB(
		"sqlite",
		sqliteFileURI(filepath.Join(dataDir, "metadata.sqlite"))+
			"?_pragma=busy_timeout(0)",
		"sqlite",
		false,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, probeDB.Close())
	})
	probeDB.SetMaxOpenConns(1)

	done := make(chan struct{})
	blockedCh := make(chan int, 1)
	go func() {
		blocked := 0
		for {
			select {
			case <-done:
				blockedCh <- blocked
				return
			default:
			}
			tx, beginErr := probeDB.BeginTx(context.Background(), nil)
			if beginErr != nil {
				blocked++
				continue
			}
			if _, execErr := tx.ExecContext(
				context.Background(),
				"INSERT INTO checkpoint_probe (n, payload) VALUES (?, ?)",
				-1,
				nil,
			); execErr != nil {
				blocked++
			}
			_ = tx.Rollback()
		}
	}()

	databaseURI := sqliteFileURI(filepath.Join(dataDir, "metadata.sqlite"))
	require.NoError(
		t,
		checkpointWAL(databaseURI, slog.New(
			slog.NewTextHandler(&bytes.Buffer{}, nil),
		))(t.Context()),
	)
	close(done)

	blocked := testutil.RequireReceive(
		t, blockedCh, 10*time.Second,
		"writer-lock probe must finish once the checkpoint returns",
	)
	require.Zero(
		t, blocked,
		"checkpointWAL must not hold SQLite's writer lock while a reader "+
			"snapshot prevents the WAL from draining",
	)
}
