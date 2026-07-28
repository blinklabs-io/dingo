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

//go:build !windows

package lifecycle_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/stretchr/testify/require"
)

// restoreInterruptHelperEnv, when set in the environment, makes
// TestRestoreInterruptedByProcessKillLeavesTargetUntouched act as a helper
// child process instead of a normal test: it calls lifecycle.Restore for
// real and blocks until killed, rather than running the assertions below.
// This is the standard Go re-exec pattern for testing a real OS signal
// (used throughout the stdlib's own os/exec tests) — it re-executes the
// already-compiled test binary (os.Args[0]) filtered to just this one test
// via -test.run, rather than needing to build a separate dingo binary.
const restoreInterruptHelperEnv = "DINGO_LIFECYCLE_RESTORE_INTERRUPT_HELPER"

// TestRestoreInterruptedByProcessKillLeavesTargetUntouched guards against
// a real correctness gap: the offline restore path used to
// restore directly into the final target data directory, with cleanup
// depending on RestoreValidated's own deferred os.RemoveAll running --
// which a killed process (SIGKILL, or any termination path the Go runtime
// does not run deferred cleanup for, including the offline CLI's default
// signal handling before this fix) skips entirely. A large database
// interrupted mid-restore could therefore leave the configured data
// directory half-restored and unusable.
//
// This test exercises a real OS-level SIGKILL against a real child
// process running lifecycle.Restore, not merely an in-process context
// cancellation (which would still run every Go defer normally and so
// would not actually exercise the failure mode this guards against): a
// killed process must never observe a defer, by definition, so only an
// actual process kill proves targetDataDir survives untouched because of
// the staging-directory-plus-atomic-rename design itself, not because
// some cleanup happened to still run.
//
// The child is proven to be genuinely mid-restore (not just "started, at
// some indeterminate point") without any sleep/size-based timing guess:
// the snapshot's metadata backup file is replaced with a FIFO before the
// child starts. RestoreFrom's raw file copy opens that path for reading,
// which — per POSIX FIFO semantics — cannot proceed until this test opens
// the write end; that open() call here blocks until the child's read-side
// open() happens, giving a real, unconditional synchronization point:
// once it returns, the child is provably inside RestoreFrom, blocked
// reading the (still empty) pipe, and killing it there is deterministic
// rather than a race against however fast a real file copy happens to be
// on whatever machine runs this test. Skipped on Windows, which has no
// POSIX FIFO equivalent this test can use the same way.
func TestRestoreInterruptedByProcessKillLeavesTargetUntouched(t *testing.T) {
	if os.Getenv(restoreInterruptHelperEnv) != "" {
		runRestoreInterruptHelper()
		return
	}
	if runtime.GOOS == "windows" {
		t.Skip("FIFO-based synchronization is POSIX-only")
	}

	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap")
	_, err := lifecycle.Snapshot(
		context.Background(), db, snapshotDir,
		lifecycle.TriggerManual, "test", "badger", "sqlite",
	)
	require.NoError(t, err)

	// Replace the real metadata backup file with a FIFO: RestoreFrom's
	// os.Open(backupPath) for reading is then the only thing that can
	// unblock this test's own write-side open below.
	backupPath := filepath.Join(snapshotDir, lifecycle.MetadataBackupFileName)
	require.NoError(t, os.Remove(backupPath))
	require.NoError(t, syscall.Mkfifo(backupPath, 0o600))

	targetDir := filepath.Join(t.TempDir(), "restored")

	cmd := exec.Command( //nolint:gosec
		os.Args[0],
		"-test.run=^TestRestoreInterruptedByProcessKillLeavesTargetUntouched$",
	)
	cmd.Env = append(
		os.Environ(),
		restoreInterruptHelperEnv+"=1",
		"DINGO_TEST_RESTORE_SNAPSHOT_DIR="+snapshotDir,
		"DINGO_TEST_RESTORE_TARGET_DIR="+targetDir,
	)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	// Blocks until the child's RestoreFrom reaches os.Open(backupPath)
	// for reading -- see the doc comment above for why this is a real
	// synchronization point, not a timing guess.
	writer, err := os.OpenFile(backupPath, os.O_WRONLY, 0)
	require.NoError(t, err, "child never reached the metadata backup FIFO")

	require.NoError(t, cmd.Process.Kill())
	_ = cmd.Wait() // expected to report a kill signal; not asserted on
	_ = writer.Close()

	_, statErr := os.Stat(targetDir)
	require.True(
		t, os.IsNotExist(statErr),
		"target data directory must not exist after the restore process "+
			"was killed mid-restore, got stat error: %v",
		statErr,
	)
}

// runRestoreInterruptHelper is the child-process body described above. It
// is not itself a real test: it calls lifecycle.Restore for real against
// the parent-provided directories and blocks (inside RestoreFrom's read
// of the FIFO the parent set up) until the parent kills this process.
func runRestoreInterruptHelper() {
	snapshotDir := os.Getenv("DINGO_TEST_RESTORE_SNAPSHOT_DIR")
	targetDir := os.Getenv("DINGO_TEST_RESTORE_TARGET_DIR")
	_, _ = lifecycle.Restore(context.Background(), snapshotDir, targetDir)
	// Only reached if the parent's kill lands after Restore already
	// finished (not expected: the FIFO blocks it first); exit quietly
	// either way, since the parent's own assertion is what actually
	// matters.
}
