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

//go:build unix

package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func openOrCreateDevnetStateRoot(runDir string) (*os.Root, error) {
	absRunDir, volumeRoot, names, err := devnetStatePathComponents(runDir)
	if err != nil {
		return nil, err
	}

	parentFd, err := openDevnetSearchDir(unix.AT_FDCWD, volumeRoot)
	if err != nil {
		return nil, fmt.Errorf("opening devnet data volume: %w", err)
	}
	defer func() {
		if parentFd >= 0 {
			_ = unix.Close(parentFd)
		}
	}()

	// Keep each parent descriptor open until its child is pinned. Pathname
	// replacement can then make the final path unreachable, but cannot redirect
	// creation or traversal into a different directory tree.
	current := volumeRoot
	for _, name := range names {
		childFd, openErr := openDevnetSearchDir(parentFd, name)
		if errors.Is(openErr, fs.ErrNotExist) {
			if mkdirErr := unix.Mkdirat(parentFd, name, 0o700); mkdirErr != nil &&
				!errors.Is(mkdirErr, fs.ErrExist) {
				return nil, fmt.Errorf(
					"creating devnet data directory %q: %w",
					filepath.Join(current, name),
					mkdirErr,
				)
			}
			childFd, openErr = openDevnetSearchDir(parentFd, name)
		}
		if openErr != nil {
			return nil, fmt.Errorf(
				"opening devnet data directory %q: %w",
				filepath.Join(current, name),
				openErr,
			)
		}
		_ = unix.Close(parentFd)
		parentFd = childFd
		current = filepath.Join(current, name)
	}

	anchoredFile := os.NewFile(uintptr(parentFd), absRunDir)
	if anchoredFile == nil {
		return nil, fmt.Errorf("opening devnet data directory %q", absRunDir)
	}
	parentFd = -1
	defer anchoredFile.Close()
	anchored, err := anchoredFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("checking devnet data directory: %w", err)
	}
	return openDevnetStateRootMatching(absRunDir, anchored)
}
