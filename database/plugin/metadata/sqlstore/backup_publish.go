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
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}

	tmpDir, err := os.MkdirTemp(dstDir, ".backup-tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	// Harmless once the success path below has already linked the staged
	// file out of tmpDir: RemoveAll on an empty (or, on failure,
	// still-populated) directory only ever touches this attempt's own
	// private directory, never dstPath.
	defer func() { _ = os.RemoveAll(tmpDir) }()
	stagedPath := filepath.Join(tmpDir, filepath.Base(dstPath))

	if err := write(stagedPath); err != nil {
		return err
	}
	if err := os.Link(stagedPath, dstPath); err != nil {
		return fmt.Errorf("publish %q: %w", dstPath, err)
	}
	// A file's own fsync (expected to already have been done by write
	// above) does not guarantee its directory entry is persisted -- sync
	// dstDir so the link above is durable too, not just atomic.
	return fsyncdir.Sync(dstDir)
}
