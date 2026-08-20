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

// removeEmptyExtractDir removes fullPath only if it is an empty directory, and
// reports an error for anything else.
//
// RemoveDirectory is the directory-only removal here. It fails with
// ERROR_DIRECTORY on a file and ERROR_DIR_NOT_EMPTY on a populated directory,
// so no separate type check is needed and none is done: establishing the type
// and then removing would let a writer swap a file in between and have it
// unlinked, which is the whole reason this is not os.Root.Remove.
//
// Unlike the Unix path this addresses the entry by name rather than through
// the parent handle, because Windows has no handle-relative removal. A
// substituted parent could therefore redirect it. The parent is held open for
// the whole extraction, which does not prevent that: os.Root opens its handles
// with FILE_SHARE_DELETE (see internal/syscall/windows/at_windows.go), so an
// open handle does not stop another process renaming or deleting the
// directory. The verified walk rejects a component already substituted when it
// runs; the race against a concurrent substitution is open until the removal
// is handle-relative. See issue #3228.
func removeEmptyExtractDir(_ *os.Root, _, fullPath string) error {
	path, err := windows.UTF16PtrFromString(fullPath)
	if err != nil {
		return fmt.Errorf("resolving extraction destination: %w", err)
	}
	if err := windows.RemoveDirectory(path); err != nil {
		return &os.PathError{Op: "rmdir", Path: fullPath, Err: err}
	}
	return nil
}
