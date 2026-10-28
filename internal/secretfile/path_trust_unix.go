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

//go:build unix

package secretfile

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/blinklabs-io/dingo/keystore"
)

func checkTrustedPath(path string, opened fs.FileInfo) error {
	if err := requireTrustedOwner(path, opened); err != nil {
		return err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve value file path %q: %w", path, err)
	}
	absPath = filepath.Clean(absPath)
	if err := checkTrustedPathChain(absPath); err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		return fmt.Errorf("resolve value file path %q: %w", path, err)
	}
	if resolved != absPath {
		if err := checkTrustedPathChain(resolved); err != nil {
			return err
		}
	}
	named, err := os.Stat(absPath)
	if err != nil {
		return fmt.Errorf("stat value file path %q: %w", path, err)
	}
	if !os.SameFile(opened, named) {
		return fmt.Errorf(
			"value file path %q changed while it was checked: %w",
			path,
			keystore.ErrInsecureFileOwner,
		)
	}
	return nil
}

func checkTrustedPathChain(path string) error {
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("inspect value file path %q: %w", current, err)
		}
		if err := checkTrustedPathEntry(current, info); err != nil {
			return err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return nil
		}
	}
}

// checkTrustedPathEntry applies the per-entry rules: every entry must be owned
// by the current user or root, and a directory writable by group or other must
// carry the sticky bit. A sticky directory is still subject to the ownership
// check and the walk continues to its ancestors.
func checkTrustedPathEntry(path string, info fs.FileInfo) error {
	if err := requireTrustedOwner(path, info); err != nil {
		return err
	}
	if info.IsDir() &&
		info.Mode().Perm()&0o022 != 0 &&
		info.Mode()&os.ModeSticky == 0 {
		return fmt.Errorf(
			"value file directory %q has mode %04o; group or other "+
				"write access requires the sticky bit: %w",
			path,
			info.Mode().Perm(),
			keystore.ErrInsecureFileMode,
		)
	}
	return nil
}

func requireTrustedOwner(path string, info fs.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf(
			"value file path %q owner is unavailable: %w",
			path,
			keystore.ErrInsecureFileOwner,
		)
	}
	uid := uint64(stat.Uid)
	// #nosec G115 -- Effective UIDs are nonnegative.
	euid := uint64(os.Geteuid())
	if uid != euid && uid != 0 {
		return fmt.Errorf(
			"value file path %q is owned by UID %d, current UID is %d: %w",
			path,
			uid,
			euid,
			keystore.ErrInsecureFileOwner,
		)
	}
	return nil
}
