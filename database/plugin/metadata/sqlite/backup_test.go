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
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestBackupToRestoreFromRoundTrip verifies that a backed-up store
// restores into a fresh directory with matching account data.
func TestBackupToRestoreFromRoundTrip(t *testing.T) {
	srcDir := t.TempDir()
	src, err := New(srcDir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, src.Start())
	defer src.Close() //nolint:errcheck

	require.NoError(t, src.SetAccount(
		0,
		make([]byte, 28),
		nil,
		nil,
		100,
		true,
		nil,
	))

	backupPath := filepath.Join(t.TempDir(), "backup.sqlite")
	require.NoError(t, src.BackupTo(context.Background(), backupPath))

	dstDir := t.TempDir()
	dst, err := New(dstDir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, dst.RestoreFrom(context.Background(), backupPath))
	require.NoError(t, dst.Start())
	defer dst.Close() //nolint:errcheck

	account, err := dst.GetAccountByCredential(0, make([]byte, 28), false, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), account.AddedSlot)
}

// TestBackupToInMemoryErrors verifies that BackupTo errors when the
// store has no on-disk data directory to back up.
func TestBackupToInMemoryErrors(t *testing.T) {
	db, err := New("", nil, nil)
	require.NoError(t, err)
	require.NoError(t, db.Start())
	defer db.Close() //nolint:errcheck

	err = db.BackupTo(context.Background(), filepath.Join(t.TempDir(), "out.sqlite"))
	require.Error(t, err)
}

// TestBackupToExistingDestinationErrors verifies that a second BackupTo
// call to the same path fails rather than overwriting it.
func TestBackupToExistingDestinationErrors(t *testing.T) {
	srcDir := t.TempDir()
	src, err := New(srcDir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, src.Start())
	defer src.Close() //nolint:errcheck

	backupPath := filepath.Join(t.TempDir(), "backup.sqlite")
	require.NoError(t, src.BackupTo(context.Background(), backupPath))
	// Second backup to the same path must fail rather than clobber it.
	err = src.BackupTo(context.Background(), backupPath)
	require.Error(t, err)
}

// TestContextReaderStopsOnCancellation verifies contextReader's actual
// contract directly and deterministically: a Read succeeds normally
// before cancellation, and every Read after ctx is cancelled returns
// ctx.Err() instead of delegating to the wrapped reader, regardless of
// how much data is left unread. copyFile wraps its source reader in
// contextReader specifically so io.Copy -- which calls Read on it
// repeatedly as it streams the file through in chunks -- notices
// cancellation within a chunk or two of a real copy call, rather than
// only once the whole transfer already finished (checking ctx just once,
// before opening the files, would leave a cancellation landing mid-copy
// unnoticed until the whole file finished copying and syncing -- a real
// delay for a large metadata restore an operator just asked to cancel).
func TestContextReaderStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cr := &contextReader{ctx: ctx, r: bytes.NewReader(bytes.Repeat([]byte("y"), 1024))}

	buf := make([]byte, 16)
	n, err := cr.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 16, n)

	cancel()
	_, err = cr.Read(buf)
	require.ErrorIs(t, err, context.Canceled)
}

// TestCopyFileSyncsDestinationDirectory verifies copyFile's
// directory-sync step is actually reached and completes without error on
// every successful copy, rather than being unreachable or silently
// skipped: a file's own fsync does not guarantee its directory entry is
// durable on POSIX filesystems, so without also syncing the destination's
// parent directory, a crash right after a "successful" restore could
// leave the synced metadata.sqlite file unreachable (or the directory
// entry simply absent) even though the file's own bytes were flushed.
// Actually observing that durability gap would require simulating a
// crash, which isn't practical in a unit test.
func TestCopyFileSyncsDestinationDirectory(t *testing.T) {
	srcPath := filepath.Join(t.TempDir(), "src.bin")
	require.NoError(t, os.WriteFile(srcPath, []byte("hello"), 0o644))

	dstDir := t.TempDir()
	dstPath := filepath.Join(dstDir, "dst.bin")

	require.NoError(t, copyFile(context.Background(), srcPath, dstPath))

	data, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), data)
}

// TestCopyReaderToFileRemovesPartialDestinationOnCancellation guards a
// real bug: copyFile used to leave a partially-written destination file in
// place when the copy failed partway through (a cancelled context, or a
// disk-full mid-write), which then made a retried RestoreFrom fail at its
// pre-existing-destination check with a misleading "already exists"
// instead of the real cause.
//
// Exercises copyReaderToFile (copyFile's inner, reader-based half)
// directly with an already-cancelled context: the destination file is
// still created unconditionally by os.OpenFile before io.Copy ever runs,
// so io.Copy's very first Read call on the wrapping contextReader sees
// ctx already done and fails immediately -- deterministically reaching
// the cleanup path every time, on every platform and at any speed. An
// earlier version of this test raced a concurrent cancel() against a real
// filesystem copy's completion, which a small/cached copy could simply
// outrun before the cancellation landed (observed as a flaky failure on
// CI runners without -race's extra scheduling overhead).
func TestCopyReaderToFileRemovesPartialDestinationOnCancellation(t *testing.T) {
	dstPath := filepath.Join(t.TempDir(), "dst.bin")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := copyReaderToFile(ctx, bytes.NewReader([]byte("hello")), dstPath)
	require.Error(t, err)

	_, statErr := os.Stat(dstPath)
	require.True(
		t, os.IsNotExist(statErr),
		"partial destination file must be removed on copy failure",
	)
}

// TestBackupToRemovesPartialDestinationOnFailure guards the same
// leftover-file bug on BackupTo's own write path: VACUUM INTO can create
// its output file before failing partway through (a cancelled context, a
// disk-full mid-write), and without cleanup a retried BackupTo would fail
// at the pre-existing-destination check with a misleading "already
// exists" instead of the real cause. BackupTo targets a private,
// operation-owned temp directory (removed via defer regardless of
// success/failure) rather than dstPath itself, so this partial file never
// even reaches dstPath in the first place -- see
// TestBackupToDoesNotRemoveConcurrentDestinationOnFailure for the
// TOCTOU-safety property that design gives on top of this.
//
// Fails runVacuumInto deterministically via its test-injectable seam,
// simulating VACUUM INTO having already created its output file before
// failing, rather than racing a real VACUUM's completion against a timed
// context cancellation -- an earlier version of this test did that and,
// even though it passed repeatedly in practice, remained
// scheduler/storage-speed dependent (and used time.Sleep for
// synchronization, which this repository's testing rules prohibit).
func TestBackupToRemovesPartialDestinationOnFailure(t *testing.T) {
	srcDir := t.TempDir()
	src, err := New(srcDir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, src.Start())
	defer src.Close() //nolint:errcheck

	dstPath := filepath.Join(t.TempDir(), "out.sqlite")

	orig := runVacuumInto
	t.Cleanup(func() { runVacuumInto = orig })
	runVacuumInto = func(_ context.Context, _ *gorm.DB, path string) error {
		require.NoError(t, os.WriteFile(path, []byte("partial"), 0o644))
		return errors.New("simulated vacuum failure")
	}

	err = src.BackupTo(context.Background(), dstPath)
	require.Error(t, err)

	_, statErr := os.Stat(dstPath)
	require.True(
		t, os.IsNotExist(statErr),
		"partial destination file must be removed on backup failure",
	)
}

// TestBackupToDoesNotRemoveConcurrentDestinationOnFailure guards a real
// TOCTOU bug: BackupTo used to run VACUUM INTO directly against dstPath
// and, on failure, unconditionally os.Remove(dstPath) to clean up any
// partial file VACUUM INTO left behind. If some other writer created a
// real file at that exact dstPath in the window between BackupTo's own
// initial existence check and this failure, that unrelated file was
// silently deleted too -- even though it had nothing to do with this
// failed operation. BackupTo now stages VACUUM INTO's output in a private,
// uniquely-named temp directory and only publishes it to dstPath via
// os.Rename once the vacuum has fully succeeded, so a failure's cleanup
// only ever removes that temp directory, never dstPath itself.
//
// Simulates the "concurrent creator" by having runVacuumInto's
// test-injectable seam itself write the file at dstPath (standing in for
// an unrelated writer) immediately before returning an error, rather than
// racing a real second goroutine against BackupTo's own internal timing --
// deterministic regardless of scheduler behavior.
func TestBackupToDoesNotRemoveConcurrentDestinationOnFailure(t *testing.T) {
	srcDir := t.TempDir()
	src, err := New(srcDir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, src.Start())
	defer src.Close() //nolint:errcheck

	dstPath := filepath.Join(t.TempDir(), "out.sqlite")

	orig := runVacuumInto
	t.Cleanup(func() { runVacuumInto = orig })
	runVacuumInto = func(_ context.Context, _ *gorm.DB, _ string) error {
		require.NoError(
			t,
			os.WriteFile(dstPath, []byte("concurrent"), 0o644),
		)
		return errors.New("simulated vacuum failure")
	}

	err = src.BackupTo(context.Background(), dstPath)
	require.Error(t, err)

	data, readErr := os.ReadFile(dstPath)
	require.NoError(t, readErr)
	require.Equal(
		t, []byte("concurrent"), data,
		"a failed backup must never remove or alter a destination file it did not create",
	)
}

// TestBackupToBeforeStartReturnsErrorNotPanic guards a real bug: BackupTo
// called against a store that was constructed (via New) but never
// Start()-ed used to dereference the nil *gorm.DB handle d.DB() returns
// in that state, panicking instead of returning a clean operation error --
// the same open-state validation the Badger blob store's Backup/Restore
// already do (see database/plugin/blob/badger/backup.go) was missing
// here.
func TestBackupToBeforeStartReturnsErrorNotPanic(t *testing.T) {
	dir := t.TempDir()
	db, err := New(dir, nil, nil)
	require.NoError(t, err)
	// Intentionally never call Start().

	dstPath := filepath.Join(t.TempDir(), "out.sqlite")
	require.NotPanics(t, func() {
		err = db.BackupTo(context.Background(), dstPath)
	})
	require.Error(t, err)
}

// TestBackupToAfterCloseReturnsErrorNotPanic verifies the same open-state
// guard also covers a store that was started and then Close()'d: d.db is
// left non-nil by Close (only the underlying *sql.DB connection pool is
// closed), so the nil-handle check alone would miss this case without
// also consulting d.closed.
func TestBackupToAfterCloseReturnsErrorNotPanic(t *testing.T) {
	dir := t.TempDir()
	db, err := New(dir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, db.Start())
	require.NoError(t, db.Close())

	dstPath := filepath.Join(t.TempDir(), "out.sqlite")
	require.NotPanics(t, func() {
		err = db.BackupTo(context.Background(), dstPath)
	})
	require.Error(t, err)
}

// TestCreateDirDurableCreatesNestedDirectories verifies createDirDurable
// actually creates every missing nested component, matching plain
// os.MkdirAll's own behavior, so its parent-sync walk runs over the
// right set of directories rather than silently no-op'ing. A newly
// created directory's own entry in its parent is not durable until that
// parent is fsynced -- a crash right after a "successful" restore into a
// brand-new nested directory could otherwise leave that directory (and
// everything restored into it) unreachable or entirely absent, even
// though copyFile's own directory sync already made metadata.sqlite
// durable within it. Actually observing that durability gap needs a
// simulated crash, impractical in a unit test (see
// TestCopyFileSyncsDestinationDirectory's identical caveat).
func TestCreateDirDurableCreatesNestedDirectories(t *testing.T) {
	base := t.TempDir()
	nested := filepath.Join(base, "a", "b", "c")

	require.NoError(t, createDirDurable(nested))
	require.DirExists(t, nested)
	require.DirExists(t, filepath.Join(base, "a"))
	require.DirExists(t, filepath.Join(base, "a", "b"))
}

// TestCreateDirDurableIsIdempotentOnExistingDirectory verifies that
// createDirDurable, like os.MkdirAll, succeeds as a no-op when the
// directory (and all its ancestors) already exist -- no newly-created
// directory means no parent-sync walk needs to run at all.
func TestCreateDirDurableIsIdempotentOnExistingDirectory(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, createDirDurable(dir))
	require.DirExists(t, dir)
}

// TestRestoreFromCreatesNestedTargetDirectory verifies the actual
// RestoreFrom path (not just createDirDurable in isolation) succeeds when
// its data directory requires creating more than one new nested
// component, rather than a plain os.MkdirAll that leaves those newly
// created directory entries' own durability unaddressed.
func TestRestoreFromCreatesNestedTargetDirectory(t *testing.T) {
	srcDir := t.TempDir()
	src, err := New(srcDir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, src.Start())
	defer src.Close() //nolint:errcheck

	backupPath := filepath.Join(t.TempDir(), "backup.sqlite")
	require.NoError(t, src.BackupTo(context.Background(), backupPath))

	dstDir := filepath.Join(t.TempDir(), "nested", "restore-target")
	dst, err := New(dstDir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, dst.RestoreFrom(context.Background(), backupPath))
	require.NoError(t, dst.Start())
	defer dst.Close() //nolint:errcheck

	require.FileExists(t, filepath.Join(dstDir, "metadata.sqlite"))
}

var _ io.Reader = &contextReader{}

// TestRestoreFromExistingDestinationErrors verifies that RestoreFrom
// refuses to clobber a destination that already has a metadata database.
func TestRestoreFromExistingDestinationErrors(t *testing.T) {
	srcDir := t.TempDir()
	src, err := New(srcDir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, src.Start())
	defer src.Close() //nolint:errcheck

	backupPath := filepath.Join(t.TempDir(), "backup.sqlite")
	require.NoError(t, src.BackupTo(context.Background(), backupPath))

	dstDir := t.TempDir()
	dst, err := New(dstDir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, dst.Start())
	defer dst.Close() //nolint:errcheck

	// dst already has a metadata.sqlite from Start(); restoring must refuse
	// to clobber it.
	err = dst.RestoreFrom(context.Background(), backupPath)
	require.Error(t, err)
}
