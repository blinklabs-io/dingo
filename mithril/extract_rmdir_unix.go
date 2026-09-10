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

	"golang.org/x/sys/unix"
)

// removeEmptyExtractDir removes name from root only if it is an empty
// directory, and reports an error for anything else.
//
// os.Root offers no directory-only removal: Root.Remove unlinks a regular
// file as readily as it removes a directory. That difference is the whole
// point here. Publication identifies the destination as an empty directory and
// then removes it, and a writer can swap a file into that name in between. A
// removal that unlinks whatever it finds destroys their file; one that can
// only ever act on a directory fails instead, so the swap costs nothing.
//
// unlinkat with AT_REMOVEDIR is rmdir addressed relative to a directory
// handle, which is exactly that. name is a single path component, so it cannot
// traverse out of root even before the handle constrains it.
//
// fullPath is unused here; the Windows implementation keeps it only to name
// the operation in its returned error, not to address the entry.
func removeEmptyExtractDir(root *os.Root, name, _ string) error {
	dir, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("opening extraction parent: %w", err)
	}
	defer dir.Close()
	conn, err := dir.SyscallConn()
	if err != nil {
		return fmt.Errorf("accessing extraction parent: %w", err)
	}
	var unlinkErr error
	if err := conn.Control(func(fd uintptr) {
		unlinkErr = unix.Unlinkat(int(fd), name, unix.AT_REMOVEDIR)
	}); err != nil {
		return fmt.Errorf("accessing extraction parent: %w", err)
	}
	if unlinkErr != nil {
		return &os.PathError{Op: "rmdir", Path: name, Err: unlinkErr}
	}
	return nil
}
