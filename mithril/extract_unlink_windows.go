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

	"golang.org/x/sys/windows"
)

// removeExtractedFile removes fullPath only if it is not a directory, and
// reports an error for anything else.
//
// Refusing a directory is the deletion itself here, not a check in front of
// it: establishing the type and then removing would let a writer swap a file
// in between and have it unlinked, which is the whole reason this is not
// os.Root.Remove.
//
// Every directory component down to the parent is walked through its own
// parent's handle and confirmed to be the entry the name denotes
// (openVerifiedParent), and those handles are held across the deletion. The
// final component is then opened relative to that verified parent's handle —
// not resolved from a path — and the deletion is applied to that open handle
// (openRelativeForDeletion, setDeleteDisposition; see
// extract_handlerelative_windows.go), which is what closes the residual
// window issue #3228 tracked: nothing here resolves any component's name a
// second time, so a reparse point substituted at the leaf after the walk
// finishes is refused at the open rather than followed.
func removeExtractedFile(root *os.Root, name, fullPath string) error {
	parent, base, release, err := openVerifiedParent(root, name)
	if err != nil {
		return err
	}
	defer release()
	dirFile, dir, err := rootDirHandle(parent)
	if err != nil {
		return fmt.Errorf("opening extraction directory: %w", err)
	}
	defer dirFile.Close()

	handle, err := openRelativeForDeletion(dir, base, windows.FILE_NON_DIRECTORY_FILE)
	if err != nil {
		return &os.PathError{Op: "unlink", Path: fullPath, Err: err}
	}
	defer func() { _ = windows.CloseHandle(handle) }()

	if err := setDeleteDisposition(handle); err != nil {
		return &os.PathError{Op: "unlink", Path: fullPath, Err: err}
	}
	return nil
}
