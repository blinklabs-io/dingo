//go:build windows

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

package lifecycle

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// syncSnapshotRoot flushes the entries of the directory root holds, through
// that handle rather than its path, so a directory renamed into place after
// the root was opened is never the one synced.
//
// FlushFileBuffers requires GENERIC_WRITE, and os.Root cannot open a
// directory for writing, so the read-only handle is reopened with write
// access. As in fsyncdir.Sync, a filesystem that refuses a writable
// directory handle leaves the flush best-effort; a failed flush on a handle
// that was granted is still reported.
func syncSnapshotRoot(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close() //nolint:errcheck // read-only handle
	handle, err := reopenSnapshotDirectoryHandle(
		dir,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
	)
	if err != nil {
		if errors.Is(err, windows.ERROR_ACCESS_DENIED) {
			return nil
		}
		return fmt.Errorf("reopen directory for sync: %w", err)
	}
	defer windows.CloseHandle(handle) //nolint:errcheck
	return windows.FlushFileBuffers(handle)
}
