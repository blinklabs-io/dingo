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
	if err := os.MkdirAll(d.dataDir, 0o755); err != nil {
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

// copyFile copies srcPath to dstPath, fsyncing the destination before
// close so the restored file is durable on disk before the caller
// proceeds to open it.
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
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("copy %q to %q: %w", srcPath, dstPath, err)
	}
	if err := dst.Sync(); err != nil {
		return fmt.Errorf("sync %q: %w", dstPath, err)
	}
	return nil
}
