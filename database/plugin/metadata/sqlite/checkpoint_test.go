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

func TestCheckpointWALDoesNotWaitForReaderAtWALTip(t *testing.T) {
	dataDir := t.TempDir()
	store, writeDB, readDB, err := openSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })

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

	// This snapshot starts at the current WAL end, so PASSIVE can report all
	// frames checkpointed even though TRUNCATE must still wait for the reader.
	readTx, err := readDB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = readTx.Rollback() })
	var probe int
	require.NoError(t, readTx.QueryRowContext(
		t.Context(), "SELECT n FROM checkpoint_probe LIMIT 1",
	).Scan(&probe))

	var logBuf bytes.Buffer
	checkpointLogger := slog.New(slog.NewTextHandler(&logBuf, nil))
	databaseURI := sqliteFileURI(filepath.Join(dataDir, "metadata.sqlite"))
	const maxUnblocked = 10 * time.Second
	started := time.Now()
	require.NoError(t, checkpointWAL(databaseURI, checkpointLogger)(t.Context()))
	require.Less(t, time.Since(started), maxUnblocked,
		"TRUNCATE must not wait for a reader holding the WAL tip")
	require.Contains(t, logBuf.String(), "could not fully complete")

	_, err = writeDB.ExecContext(
		t.Context(),
		"INSERT INTO checkpoint_probe (n) VALUES (1)",
	)
	require.NoError(t, err, "the live reader must not stall the import writer")
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
	assertCheckpointWALDoesNotHoldWriterLock(t, true)
}

func TestCheckpointWALDoesNotHoldWriterLockBehindReaderAtTip(t *testing.T) {
	t.Parallel()
	assertCheckpointWALDoesNotHoldWriterLock(t, false)
}

func assertCheckpointWALDoesNotHoldWriterLock(
	t *testing.T,
	pinFrames bool,
) {
	t.Helper()
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

	// A reader opened after the initial writes is either at the WAL tip or,
	// below, made stale by additional frames. Both shapes must avoid holding
	// SQLite's writer lock while TRUNCATE waits for the reader.
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
	if pinFrames {
		writeRows(100_000, 2000)
	}

	// Probe the writer lock from a dedicated connection that never waits, so
	// a single observation of SQLITE_BUSY means the checkpoint was holding
	// it at that moment rather than that the probe was impatient.
	probeDB, err := sqlstore.OpenDB(
		"sqlite",
		sqliteFileURI(filepath.Join(dataDir, "metadata.sqlite"))+
			"?_pragma=busy_timeout(0)&_pragma=synchronous(OFF)",
		"sqlite",
		false,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, probeDB.Close())
	})
	probeDB.SetMaxOpenConns(1)

	done := make(chan struct{})
	probeDone := make(chan struct{})
	stopProbe := func() {
		select {
		case <-done:
		default:
			close(done)
		}
	}
	defer func() {
		stopProbe()
		<-probeDone
	}()
	blockedCh := make(chan time.Duration, 1)
	probeReadyCh := make(chan error, 1)
	go func() {
		var blockedSince time.Time
		var longestBlocked time.Duration
		ready := false
		defer func() {
			if !blockedSince.IsZero() {
				longestBlocked = max(longestBlocked, time.Since(blockedSince))
			}
			blockedCh <- longestBlocked
			close(probeDone)
		}()
		for {
			select {
			case <-done:
				return
			default:
			}
			tx, beginErr := probeDB.BeginTx(context.Background(), nil)
			if beginErr != nil {
				if !ready {
					probeReadyCh <- beginErr
					return
				}
				if blockedSince.IsZero() {
					blockedSince = time.Now()
				}
				continue
			}
			_, execErr := tx.ExecContext(
				context.Background(),
				"INSERT INTO checkpoint_probe (n, payload) VALUES (?, ?)",
				-1,
				nil,
			)
			_ = tx.Rollback()
			if execErr != nil {
				if !ready {
					probeReadyCh <- execErr
					return
				}
				if blockedSince.IsZero() {
					blockedSince = time.Now()
				}
				continue
			}
			if !blockedSince.IsZero() {
				longestBlocked = max(longestBlocked, time.Since(blockedSince))
				blockedSince = time.Time{}
			}
			if !ready {
				ready = true
				probeReadyCh <- nil
			}
		}
	}()
	require.NoError(
		t,
		testutil.RequireReceive(
			t,
			probeReadyCh,
			10*time.Second,
			"writer-lock probe must complete a write before checkpoint starts",
		),
	)

	databaseURI := sqliteFileURI(filepath.Join(dataDir, "metadata.sqlite"))
	checkpointErr := checkpointWAL(databaseURI, slog.New(
		slog.NewTextHandler(&bytes.Buffer{}, nil),
	))(t.Context())
	stopProbe()

	blocked := testutil.RequireReceive(
		t, blockedCh, 10*time.Second,
		"writer-lock probe must finish once the checkpoint returns",
	)
	require.NoError(t, checkpointErr)
	require.Less(
		t, blocked, 50*time.Millisecond,
		"checkpointWAL must not hold SQLite's writer lock for a sustained "+
			"period while a reader snapshot prevents the WAL from draining",
	)
}
