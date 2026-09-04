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

// removeEmptyExtractDir removes name from root only if it is an empty
// directory, and reports an error for anything else.
//
// Refusing a populated directory or a file is the removal itself, not a check
// in front of it: establishing the type and then removing would let a writer
// swap a file into the name in between and have it unlinked, which is the
// whole reason this is not os.Root.Remove. NTFS enforces emptiness on the
// underlying disposition set below the same way it enforces it for
// RemoveDirectory, so nothing here checks it separately either.
//
// root is the already-verified parent — callers pass the same handle
// openVerifiedParent would resolve name's parent to, since name is always a
// direct child of it here — so name is opened as a single component relative
// to root's own handle rather than resolved from fullPath
// (openRelativeForDeletion, setDeleteDisposition; see
// extract_handlerelative_windows.go). That is what closes the gap issue #3228
// tracked: earlier, only DeleteFile/RemoveDirectory's path was avoided for
// removeExtractedFile's and renameExtractedDirectory's own final component,
// while this operation still resolved fullPath directly and a substituted
// parent could redirect it. Nothing here resolves any component's name a
// second time now, and a reparse point substituted at name after root was
// verified is refused at the open rather than followed.
func removeEmptyExtractDir(root *os.Root, name, fullPath string) error {
	dirFile, dir, err := rootDirHandle(root)
	if err != nil {
		return fmt.Errorf("opening extraction parent: %w", err)
	}
	defer dirFile.Close()

	handle, err := openRelativeForDeletion(
		dir,
		name,
		windows.FILE_DIRECTORY_FILE,
	)
	if err != nil {
		return &os.PathError{Op: "rmdir", Path: fullPath, Err: err}
	}
	defer func() { _ = windows.CloseHandle(handle) }()

	if err := setDeleteDisposition(handle); err != nil {
		return &os.PathError{Op: "rmdir", Path: fullPath, Err: err}
	}
	return nil
}
