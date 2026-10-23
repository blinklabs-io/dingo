//go:build !windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lifecycle

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

func requirePrivateSnapshotParent(parent string) error {
	_, err := inspectSnapshotPath(parent, false)
	return err
}

func requirePrivateLocalSnapshot(snapshotDir string) error {
	info, err := inspectSnapshotPath(snapshotDir, true)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrSnapshotTrustRequired, err)
	}
	if err := requireTrustedSnapshotOwner(snapshotDir, info); err != nil {
		return fmt.Errorf("%w: %w", ErrSnapshotTrustRequired, err)
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf(
			"%w: snapshot directory %q is writable by group or other",
			ErrSnapshotTrustRequired,
			snapshotDir,
		)
	}
	for _, name := range []string{
		ManifestFileName,
		BlobBackupFileName,
		MetadataBackupFileName,
	} {
		path := filepath.Join(snapshotDir, name)
		entry, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("inspect local snapshot file %q: %w", path, err)
		}
		if !entry.Mode().IsRegular() {
			return fmt.Errorf(
				"%w: local snapshot file %q is not regular",
				ErrSnapshotTrustRequired,
				path,
			)
		}
		if err := requireTrustedSnapshotOwner(path, entry); err != nil {
			return fmt.Errorf("%w: %w", ErrSnapshotTrustRequired, err)
		}
		if entry.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf(
				"%w: local snapshot file %q is writable by group or other",
				ErrSnapshotTrustRequired,
				path,
			)
		}
	}
	return nil
}

func inspectSnapshotPath(path string, requireLeaf bool) (fs.FileInfo, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve snapshot path %q: %w", path, err)
	}
	absPath = filepath.Clean(absPath)
	current := absPath
	var leaf fs.FileInfo
	for {
		info, err := os.Lstat(current)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) || requireLeaf {
				return nil, fmt.Errorf("inspect snapshot path %q: %w", current, err)
			}
		} else {
			if leaf == nil {
				leaf = info
			}
			if current == absPath {
				if requireLeaf && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
					return nil, fmt.Errorf("snapshot path %q is not a real directory", current)
				}
			}
			if err := validateSnapshotAncestorChain(current); err != nil {
				return nil, err
			}
			resolved, err := filepath.EvalSymlinks(current)
			if err != nil {
				return nil, fmt.Errorf("resolve snapshot path %q: %w", current, err)
			}
			if resolved != current {
				if err := validateSnapshotAncestorChain(resolved); err != nil {
					return nil, err
				}
			}
			break
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}
	if leaf == nil {
		return nil, fmt.Errorf("snapshot path %q has no existing ancestor", path)
	}
	return leaf, nil
}

func validateSnapshotAncestorChain(path string) error {
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("inspect snapshot path %q: %w", current, err)
		}
		// The owner of a symlink can replace it inside a sticky directory, so
		// a link owned by another user could be retargeted after this check.
		// Its mode bits carry no meaning, so only the owner is checked.
		if info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			if err := requireTrustedSnapshotOwner(current, info); err != nil {
				return fmt.Errorf("snapshot path ancestor: %w", err)
			}
		}
		if info.IsDir() {
			if info.Mode().Perm()&0o022 != 0 && info.Mode()&os.ModeSticky == 0 {
				return fmt.Errorf(
					"snapshot path ancestor %q is writable by group or other without the sticky bit",
					current,
				)
			}
		}
		parent := filepath.Dir(current)
		if parent == current {
			return nil
		}
	}
}

func requireTrustedSnapshotOwner(path string, info fs.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("inspect owner of %q", path)
	}
	if uid := int64(stat.Uid); uid != int64(os.Geteuid()) && uid != 0 {
		return fmt.Errorf("%q is owned by UID %d", path, stat.Uid)
	}
	return nil
}
