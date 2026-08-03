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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"github.com/blinklabs-io/dingo/internal/fsyncdir"
)

// runVacuumInto executes "VACUUM INTO" against dstPath, indirected through
// a variable so a test can inject a failure at this exact point --
// deterministically, including one that leaves a partial destination file
// behind first -- instead of racing a real VACUUM's completion against a
// timed context cancellation (scheduler/storage-speed dependent even when
// it happens to pass repeatedly).
var runVacuumInto = func(ctx context.Context, db *sql.DB, dstPath string) error {
	_, err := db.ExecContext(ctx, "VACUUM INTO ?", dstPath)
	return err
}

// BackupTo writes a standalone, defragmented copy of the store's current
// contents to dstPath (which must not already exist) using SQLite's
// `VACUUM INTO` statement. This takes only a brief read lock under WAL
// mode and does not require stopping concurrent writers.
func backupSQLite(
	ctx context.Context,
	databasePath string,
	dataDir string,
	dstPath string,
) error {
	if dataDir == "" {
		return errors.New(
			"sqlite backup: in-memory database has nothing to back up",
		)
	}
	if _, err := os.Stat(dstPath); err == nil {
		return fmt.Errorf(
			"sqlite backup: destination %q already exists",
			dstPath,
		)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("sqlite backup: stat %q: %w", dstPath, err)
	}
	dstDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return fmt.Errorf(
			"sqlite backup: create destination directory: %w",
			err,
		)
	}

	// VACUUM INTO targets a private, operation-owned temporary directory,
	// not dstPath directly, and is only published to dstPath via the
	// os.Link below once it has fully succeeded -- matching
	// database/lifecycle/manifest.go's WriteManifest write-to-temp-then-
	// rename pattern. VACUUM INTO targeting dstPath directly, cleaned up
	// with an unconditional os.Remove(dstPath) on failure, is a TOCTOU
	// race: a concurrent writer that creates a real file at dstPath in the
	// window between the existence check above and this failure would
	// have that file silently deleted too, even though it has nothing to
	// do with this failed operation. os.MkdirTemp's uniquely-named
	// directory can't collide with anything another goroutine/process is
	// doing. Publishing with a hard link is no-clobber: unlike Rename,
	// Link fails if another creator populated dstPath after the initial
	// existence check.
	tmpDir, err := os.MkdirTemp(dstDir, ".backup-tmp-*")
	if err != nil {
		return fmt.Errorf(
			"sqlite backup: create temporary directory: %w",
			err,
		)
	}
	// Harmless once the success path below has already renamed the backup
	// file out of tmpDir: RemoveAll on an empty (or, on failure,
	// still-populated) directory either way only ever touches this
	// attempt's own private directory, never dstPath.
	defer func() { _ = os.RemoveAll(tmpDir) }()
	tmpPath := filepath.Join(tmpDir, filepath.Base(dstPath))

	backupDB, err := openSQLiteBackupDB(databasePath)
	if err != nil {
		return fmt.Errorf("sqlite backup: open source: %w", err)
	}
	defer backupDB.Close() //nolint:errcheck
	if err := runVacuumInto(ctx, backupDB, tmpPath); err != nil {
		return fmt.Errorf("sqlite backup: VACUUM INTO %q: %w", dstPath, err)
	}
	if err := os.Link(tmpPath, dstPath); err != nil {
		return fmt.Errorf(
			"sqlite backup: publish %q: %w",
			dstPath,
			err,
		)
	}
	// A file's own fsync (already done implicitly by VACUUM INTO closing
	// the destination file) does not guarantee its directory entry is
	// persisted -- sync dstDir so the link above is durable too, not
	// just atomic.
	return fsyncdir.Sync(dstDir)
}

// openSQLiteBackupDB opens a short-lived connection for VACUUM INTO. The
// provider's write pool is deliberately single-connection; borrowing it can
// deadlock a live snapshot when another metadata operation still owns that
// connection. A dedicated connection preserves SQLite's WAL snapshot
// semantics without contending with the pool's connection accounting.
func openSQLiteBackupDB(databasePath string) (*sql.DB, error) {
	dsn := sqliteFileURI(databasePath) +
		"?_txlock=deferred" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_pragma=busy_timeout(30000)" +
		"&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// RestoreFrom replaces this store's on-disk database file with the backup
// at srcPath (produced by BackupTo). It must be called before the store
// has been started (Start), against a data directory that does not
// already contain a metadata database file.
func restoreSQLite(ctx context.Context, dataDir, srcPath string) error {
	if dataDir == "" {
		return errors.New(
			"sqlite restore: cannot restore into an in-memory database",
		)
	}
	dstPath := filepath.Join(dataDir, "metadata.sqlite")
	if _, err := os.Stat(dstPath); err == nil {
		return fmt.Errorf(
			"sqlite restore: destination %q already exists",
			dstPath,
		)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("sqlite restore: stat %q: %w", dstPath, err)
	}
	if err := createDirDurable(dataDir); err != nil {
		return fmt.Errorf(
			"sqlite restore: create data directory: %w",
			err,
		)
	}
	if err := copyFile(ctx, srcPath, dstPath); err != nil {
		return fmt.Errorf("sqlite restore: %w", err)
	}
	return nil
}

// createDirDurable is os.MkdirAll(dir, 0o755), but additionally fsyncs the
// parent of every directory component it actually had to create, so each
// new directory's own entry is durable -- not just, per copyFile's own
// directory-sync, the eventual contents placed inside it. A directory's
// fsync only guarantees ITS children's directory entries are persisted; a
// power loss right after mkdir could otherwise leave the newly created
// directory itself unreachable (or entirely absent) from its parent after
// a crash, even though metadata.sqlite was safely and durably written
// inside it a moment later.
func createDirDurable(dir string) error {
	var created []string
	for cur := dir; ; {
		if _, err := os.Stat(cur); err == nil {
			break
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("stat %q: %w", cur, err)
		}
		created = append(created, cur)
		parent := filepath.Dir(cur)
		if parent == cur {
			// Reached the filesystem root without finding an existing
			// ancestor -- MkdirAll below will fail on this same path.
			break
		}
		cur = parent
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create directory %q: %w", dir, err)
	}
	// Shallowest first: each level's own directory entry should be
	// durable before its child's existence under it is relied upon.
	for _, dir := range slices.Backward(created) {
		if err := fsyncdir.Sync(filepath.Dir(dir)); err != nil {
			return err
		}
	}
	return nil
}

// copyFile copies srcPath to dstPath, fsyncing the destination file (and
// then its parent directory) before returning, so both the restored
// file's content and its directory entry are durable on disk before the
// caller proceeds to open it. A file's own fsync does not guarantee its
// directory entry is persisted -- a power loss right after could leave
// the synced file unreachable (or absent) after a crash without that
// second, directory-level fsync.
func copyFile(ctx context.Context, srcPath, dstPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open source %q: %w", srcPath, err)
	}
	defer src.Close()
	if err := copyReaderToFile(ctx, src, dstPath); err != nil {
		return fmt.Errorf("copy %q to %q: %w", srcPath, dstPath, err)
	}
	return nil
}

// copyReaderToFile does copyFile's actual write/durability work against an
// already-open source reader, factored out so a cancellation's
// destination-cleanup path (see the retErr!=nil branch below) can be
// exercised with a deterministic, pre-cancelled context and a plain
// bytes.Reader in tests -- rather than a real filesystem copy racing a
// concurrent cancellation's wall-clock timing, which a fast/cached copy
// can simply outrun before the cancellation ever lands.
func copyReaderToFile(ctx context.Context, src io.Reader, dstPath string) (retErr error) {
	dst, err := os.OpenFile(
		dstPath,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o644,
	)
	if err != nil {
		return fmt.Errorf("create destination %q: %w", dstPath, err)
	}
	dstClosed := false
	defer func() {
		if !dstClosed {
			_ = dst.Close()
		}
		if retErr != nil {
			// dstPath is a partial/corrupt copy at this point (a
			// cancelled context, or a mid-copy/sync failure), not a
			// usable restore target -- remove it so a retry hits the
			// real cause instead of the pre-existing-destination check
			// in RestoreFrom failing with a misleading "already exists",
			// or a caller mistaking the partial file for a complete
			// restore.
			_ = os.Remove(dstPath)
		}
	}()

	// Wrapping src (not dst) is enough: io.Copy drives the loop by
	// repeatedly calling Read on this reader, so wrapping it checks ctx on
	// the same cadence as if io.Copy itself were ctx-aware -- cancellation
	// during a large metadata restore takes effect within a chunk or two
	// rather than only once the whole file has already been copied.
	if _, err := io.Copy(dst, &contextReader{ctx: ctx, r: src}); err != nil {
		return fmt.Errorf("copy to %q: %w", dstPath, err)
	}
	if err := dst.Sync(); err != nil {
		return fmt.Errorf("sync %q: %w", dstPath, err)
	}
	dstClosed = true
	if err := dst.Close(); err != nil {
		return fmt.Errorf("close %q: %w", dstPath, err)
	}

	// A file's own fsync does not guarantee its directory entry is
	// persisted -- a power loss right after could leave the synced file
	// unreachable (or absent) after a crash without also syncing the
	// parent directory.
	return fsyncdir.Sync(filepath.Dir(dstPath))
}

// contextReader wraps an io.Reader, checking ctx before each Read so a
// long-running copy can be cancelled mid-transfer instead of only before
// or after the whole thing runs.
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (cr *contextReader) Read(p []byte) (int, error) {
	if err := cr.ctx.Err(); err != nil {
		return 0, err
	}
	return cr.r.Read(p)
}
