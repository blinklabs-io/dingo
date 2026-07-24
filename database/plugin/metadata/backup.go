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

package metadata

import "context"

// Backuper is implemented by metadata store plugins that can write a
// self-contained, restorable copy of their contents to a local filesystem
// path. Path-based (rather than io.Writer-based) because the underlying
// mechanism for most relational backends (e.g. SQLite's `VACUUM INTO`) is
// itself path-oriented; streaming to a remote destination is layered on
// top of the local copy by the caller, not by the plugin.
type Backuper interface {
	// BackupTo writes a self-contained copy of the store's current
	// contents to dstPath, which must not already exist.
	BackupTo(ctx context.Context, dstPath string) error
}

// Restorer is implemented by metadata store plugins that can replace their
// contents from a backup produced by Backuper.BackupTo.
type Restorer interface {
	// RestoreFrom replaces this store's on-disk state with the copy at
	// srcPath. Must be called before the store has been started against
	// its real data directory, or after Close().
	RestoreFrom(ctx context.Context, srcPath string) error
}
