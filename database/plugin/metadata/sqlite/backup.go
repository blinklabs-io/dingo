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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
)

// BackupTo writes a standalone, defragmented copy of the store's current
// contents to dstPath (which must not already exist) using SQLite's
// `VACUUM INTO` statement. This takes only a brief read lock under WAL
// mode and does not require stopping concurrent writers.
func (d *MetadataStoreSqlite) BackupTo(ctx context.Context, dstPath string) error {
	if d.dataDir == "" {
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
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return fmt.Errorf(
			"sqlite backup: create destination directory: %w",
			err,
		)
	}
	if err := d.DB().WithContext(ctx).
		Exec("VACUUM INTO ?", dstPath).Error; err != nil {
		return fmt.Errorf("sqlite backup: VACUUM INTO %q: %w", dstPath, err)
	}
	return nil
}

// RestoreFrom replaces this store's on-disk database file with the backup
// at srcPath (produced by BackupTo). It must be called before the store
// has been started (Start), against a data directory that does not
// already contain a metadata database file.
func (d *MetadataStoreSqlite) RestoreFrom(ctx context.Context, srcPath string) error {
	if d.dataDir == "" {
		return errors.New(
			"sqlite restore: cannot restore into an in-memory database",
		)
	}
	dstPath := filepath.Join(d.dataDir, "metadata.sqlite")
	if _, err := os.Stat(dstPath); err == nil {
		return fmt.Errorf(
			"sqlite restore: destination %q already exists",
			dstPath,
		)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("sqlite restore: stat %q: %w", dstPath, err)
	}
	if err := createDirDurable(d.dataDir); err != nil {
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
		if err := syncDir(filepath.Dir(dir)); err != nil {
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
	}()

	// Wrapping src (not dst) is enough: io.Copy drives the loop by
	// repeatedly calling Read on this reader, so wrapping it checks ctx on
	// the same cadence as if io.Copy itself were ctx-aware -- cancellation
	// during a large metadata restore takes effect within a chunk or two
	// rather than only once the whole file has already been copied.
	if _, err := io.Copy(dst, &contextReader{ctx: ctx, r: src}); err != nil {
		return fmt.Errorf("copy %q to %q: %w", srcPath, dstPath, err)
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
	return syncDir(filepath.Dir(dstPath))
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
