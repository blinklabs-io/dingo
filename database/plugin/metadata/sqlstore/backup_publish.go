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

package sqlstore

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"github.com/blinklabs-io/dingo/internal/fsyncdir"
)

// PublishBackupFile runs write against a path inside a private, uniquely
// named staging directory next to dstPath, then publishes the result to
// dstPath (which must not already exist) with an os.Link and fsyncs dstDir
// so the link is durable, not just atomic.
//
// Every dump-producing metadata backend (sqlite's VACUUM INTO, postgres's
// pg_dump, mysql's mysqldump) needs the exact same crash-safety shape: the
// dump tool must never write dstPath directly, because a failed or
// cancelled dump would then require deleting dstPath to clean up, and an
// unconditional os.Remove(dstPath) on failure is a TOCTOU race against a
// concurrent creator that populated dstPath in the window between the
// existence check and the failure. Staging into a private
// os.MkdirTemp directory sidesteps that: nothing else can be using that
// path, and publishing via os.Link (not os.Rename) is itself no-clobber --
// it fails if a concurrent creator populated dstPath after the initial
// check, rather than silently overwriting it.
func PublishBackupFile(
	dstPath string,
	write func(stagedPath string) error,
) error {
	if _, err := os.Stat(dstPath); err == nil {
		return fmt.Errorf("destination %q already exists", dstPath)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat %q: %w", dstPath, err)
	}
	dstDir := filepath.Dir(dstPath)
	if err := CreateDirDurable(dstDir); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}

	tmpDir, err := os.MkdirTemp(dstDir, ".backup-tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	// Harmless once the success path below has already removed tmpDir
	// itself: RemoveAll on an already-removed (or, on failure,
	// still-populated) directory only ever touches this attempt's own
	// private directory, never dstPath.
	defer func() { _ = os.RemoveAll(tmpDir) }()
	stagedPath := filepath.Join(tmpDir, filepath.Base(dstPath))

	if err := write(stagedPath); err != nil {
		return err
	}
	// write's own durability varies by caller: sqlite's VACUUM INTO
	// fsyncs its destination file as part of closing it, but pg_dump and
	// mysqldump do not fsync the file they write at all. Rather than
	// relying on every current and future write callback to handle this
	// itself, fsync the staged file here unconditionally -- otherwise a
	// crash after publish (the os.Link below, or its directory fsync)
	// could leave a durable directory entry pointing at a file whose
	// contents were never actually flushed to disk, i.e. a published
	// backup that reads back truncated or empty.
	if err := syncFile(stagedPath); err != nil {
		return fmt.Errorf("sync staged backup %q: %w", stagedPath, err)
	}
	if err := os.Link(stagedPath, dstPath); err != nil {
		return fmt.Errorf("publish %q: %w", dstPath, err)
	}
	// A file's own fsync does not guarantee its directory entry is
	// persisted -- sync dstDir so the link above is durable too, not
	// just atomic.
	if err := fsyncdir.Sync(dstDir); err != nil {
		return fmt.Errorf("sync %q: %w", dstDir, err)
	}
	// tmpDir's removal below is itself a mutation to dstDir's own entries
	// (removing tmpDir's name) that happens after the sync above -- sync
	// dstDir a second time once it's done, or a crash in between could
	// leave an orphaned ".backup-tmp-*" directory reappearing in dstDir on
	// restart even though the published backup itself is already durable.
	if err := os.RemoveAll(tmpDir); err != nil {
		return fmt.Errorf("remove staging directory %q: %w", tmpDir, err)
	}
	return fsyncdir.Sync(dstDir)
}

// CreateDirDurable is os.MkdirAll(dir, 0o755), but additionally fsyncs the
// parent of every directory component it actually had to create, so each
// new directory's own entry is durable -- not just, per a subsequent file
// write's own directory-sync, the eventual contents placed inside it. A
// directory's fsync only guarantees ITS children's directory entries are
// persisted; a power loss right after mkdir could otherwise leave the newly
// created directory itself unreachable (or entirely absent) from its parent
// after a crash, even though a file was safely and durably written inside
// it a moment later.
func CreateDirDurable(dir string) error {
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

// syncFile opens path and fsyncs it. Used to durably persist a staged
// backup file's contents before PublishBackupFile links it into place,
// for callers (pg_dump, mysqldump) that don't fsync their own output.
func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %q: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync %q: %w", path, err)
	}
	return f.Close()
}
