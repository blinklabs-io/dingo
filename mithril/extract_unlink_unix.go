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

package mithril

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// removeExtractedFile removes name from root only if it is not a directory,
// and reports an error for anything else.
//
// It is removeEmptyExtractDir's counterpart and exists for the same reason.
// os.Root.Remove acts on whatever it finds — it unlinks a regular file and
// removes an empty directory — and extraction has to clear a file it is about
// to write while leaving a directory alone, because a directory at that name
// is something it was never asked to touch and used to refuse.
//
// Deciding that with a check first would put the decision and the act in
// different instants: a writer who swaps a file into the name in between turns
// "refuse the directory" into "unlink their file". unlinkat without
// AT_REMOVEDIR cannot remove a directory at all, so the refusal is the
// operation rather than a guard in front of it.
//
// The traversal stays with os.Root: only the final component reaches the
// syscall, so a name cannot walk out of the root on the way to being removed.
//
// fullPath is unused here; the Windows implementation needs it because it has
// no handle-relative removal to address the entry through.
func removeExtractedFile(root *os.Root, name, _ string) error {
	parent := root
	base := name
	if dir := filepath.Dir(name); dir != "." && dir != "" {
		nested, err := root.OpenRoot(dir)
		if err != nil {
			return fmt.Errorf("opening %s: %w", dir, err)
		}
		defer nested.Close()
		parent, base = nested, filepath.Base(name)
	}
	dir, err := parent.Open(".")
	if err != nil {
		return fmt.Errorf("opening extraction directory: %w", err)
	}
	defer dir.Close()
	conn, err := dir.SyscallConn()
	if err != nil {
		return fmt.Errorf("accessing extraction directory: %w", err)
	}
	var unlinkErr error
	if err := conn.Control(func(fd uintptr) {
		unlinkErr = unix.Unlinkat(int(fd), base, 0)
	}); err != nil {
		return fmt.Errorf("accessing extraction directory: %w", err)
	}
	if unlinkErr != nil {
		return &os.PathError{Op: "unlink", Path: name, Err: unlinkErr}
	}
	return nil
}
