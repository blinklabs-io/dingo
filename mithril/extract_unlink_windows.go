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

//go:build windows

package mithril

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// removeExtractedFile removes fullPath only if it is not a directory, and
// reports an error for anything else.
//
// DeleteFile is the file-only removal here: it fails on a directory, so no
// separate type check is needed and none is done — establishing the type and
// then removing would let a writer swap a file in between and have it
// unlinked, which is the whole reason this is not os.Root.Remove.
//
// Unlike the Unix path the deletion itself is addressed by name, because
// Windows has no handle-relative removal — the same limit removeEmptyExtractDir
// carries, for the same reason.
//
// What is narrowed is everything above it. Every directory component is walked
// through its parent's handle and confirmed to be the entry the name denotes
// (openVerifiedParent), and those handles are held across the deletion, so a
// reparse point substituted mid-extraction is refused during the walk rather
// than followed by DeleteFile. Only the immediate parent's own name is
// resolved a second time, and Windows makes substituting it hard by refusing
// to move a directory while handles are open on it.
func removeExtractedFile(root *os.Root, name, fullPath string) error {
	parent, base, release, err := openVerifiedParent(root, name)
	if err != nil {
		return err
	}
	defer release()
	// Built from the verified walk rather than taken from the caller, so the
	// name deleted is the one the handles above were checked for. fullPath is
	// kept for the error, which is what an operator sees.
	target := filepath.Join(parent.Name(), base)
	path, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return fmt.Errorf("resolving extraction destination: %w", err)
	}
	if err := windows.DeleteFile(path); err != nil {
		return &os.PathError{Op: "unlink", Path: fullPath, Err: err}
	}
	return nil
}
