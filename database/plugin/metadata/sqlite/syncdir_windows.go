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

package sqlite

// syncDir is a no-op on Windows: unlike POSIX, Windows has no supported
// way to fsync a directory's entries through a regular file handle --
// (*os.File).Sync on a directory opened via os.Open returns
// "Access is denied" (FlushFileBuffers rejects directory handles opened
// without FILE_FLAG_BACKUP_SEMANTICS, which os.Open does not set, and
// even a backup-semantics handle is not guaranteed to support it). NTFS
// itself journals directory-entry changes through its own transaction
// log independently of an application-level fsync, so this durability
// gap does not apply to Windows the way it does to POSIX filesystems.
func syncDir(path string) error {
	return nil
}
