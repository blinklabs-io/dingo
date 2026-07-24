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

package fsyncdir

import (
	"errors"
	"fmt"

	"golang.org/x/sys/windows"
)

// Sync attempts to flush the directory at path's own entries to disk, so
// a prior change to one of them (a new or renamed file appearing in it)
// is durable across a crash, not just the changed file's own content.
//
// Unlike POSIX, a directory handle opened the way os.Open opens it
// (GENERIC_READ only -- even though FILE_FLAG_BACKUP_SEMANTICS is
// already set, since Go's runtime ORs that flag into every Windows
// open, directory or not) cannot be flushed: FlushFileBuffers documents
// that its handle argument "must have been created with the
// GENERIC_WRITE access right", which is exactly the ERROR_ACCESS_DENIED
// a plain os.Open-based sync hits. This opens the directory directly via
// CreateFile with GENERIC_WRITE added, which FlushFileBuffers then
// accepts.
//
// This is best-effort, not a guarantee: if the directory can't be
// reopened with write access, or the underlying filesystem still
// rejects the flush (some non-NTFS or network filesystems have no
// directory-flush support at all), this returns nil rather than failing
// the restore/snapshot that depends on it -- a narrower durability
// guarantee than POSIX is preferable to refusing to serve Windows
// entirely. NTFS itself journals directory-entry changes through its
// own transaction log independently of this call succeeding, which is
// what keeps the practical gap here far narrower than the POSIX case
// this function exists to close.
func Sync(path string) error {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return fmt.Errorf("encode %q for directory sync: %w", path, err)
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		// A genuinely missing path is the caller's own mistake, not a
		// platform durability gap -- propagate it like the POSIX
		// implementation's os.Open would. Anything else here (most
		// commonly access-denied on a filesystem/OS combination that
		// won't grant write access to a directory handle at all) falls
		// back to best-effort instead (see doc comment).
		if errors.Is(err, windows.ERROR_FILE_NOT_FOUND) ||
			errors.Is(err, windows.ERROR_PATH_NOT_FOUND) {
			return fmt.Errorf("open %q for directory sync: %w", path, err)
		}
		return nil
	}
	defer windows.CloseHandle(handle) //nolint:errcheck
	// Best-effort: some filesystems reject this outright even with a
	// write-capable handle (see doc comment), which is not a failure of
	// this call's own correctness.
	_ = windows.FlushFileBuffers(handle)
	return nil
}
