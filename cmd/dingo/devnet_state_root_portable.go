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

//go:build windows || wasip1

package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

func openOrCreateDevnetStateRootPortable(runDir string) (*os.Root, error) {
	_, volumeRoot, names, err := devnetStatePathComponents(runDir)
	if err != nil {
		return nil, err
	}

	root, err := os.OpenRoot(volumeRoot)
	if err != nil {
		return nil, fmt.Errorf("opening devnet data volume: %w", err)
	}
	// Keep each parent handle open until its child is pinned. Pathname
	// replacement can then make the final path unreachable, but cannot redirect
	// creation or traversal into a different directory tree.
	current := volumeRoot
	for _, name := range names {
		info, statErr := root.Lstat(name)
		if errors.Is(statErr, fs.ErrNotExist) {
			if mkdirErr := root.Mkdir(name, 0o700); mkdirErr != nil &&
				!errors.Is(mkdirErr, fs.ErrExist) {
				_ = root.Close()
				return nil, fmt.Errorf(
					"creating devnet data directory %q: %w",
					filepath.Join(current, name),
					mkdirErr,
				)
			}
			info, statErr = root.Lstat(name)
		}
		if statErr != nil {
			_ = root.Close()
			return nil, fmt.Errorf(
				"checking devnet data directory %q: %w",
				filepath.Join(current, name),
				statErr,
			)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			_ = root.Close()
			return nil, fmt.Errorf(
				"devnet data path %q is not a directory",
				filepath.Join(current, name),
			)
		}
		child, openErr := root.OpenRoot(name)
		if openErr == nil {
			var opened os.FileInfo
			opened, openErr = child.Stat(".")
			if openErr == nil && !os.SameFile(info, opened) {
				openErr = errors.New("directory changed while it was opened")
			}
		}
		if openErr != nil {
			if child != nil {
				_ = child.Close()
			}
			_ = root.Close()
			return nil, fmt.Errorf(
				"opening devnet data directory %q: %w",
				filepath.Join(current, name),
				openErr,
			)
		}
		_ = root.Close()
		root = child
		current = filepath.Join(current, name)
	}
	return root, nil
}
