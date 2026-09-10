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

package blob

import (
	"context"
	"io"
)

// Backuper is implemented by blob store plugins that can stream a
// consistent point-in-time copy of their contents. Implementations should
// use their native MVCC/versioned backup mechanism so this does not
// require blocking concurrent writers for the duration of the backup.
type Backuper interface {
	// Backup streams a backup of the current contents of the store to w.
	Backup(ctx context.Context, w io.Writer) error
}

// Restorer is implemented by blob store plugins that can replace their
// contents from a backup produced by Backuper.Backup. Restore must only be
// called against a freshly created, empty store — it is not a merge.
type Restorer interface {
	// Restore replaces the store's contents with the backup read from r.
	Restore(ctx context.Context, r io.Reader) error
}

// BackupValidator is implemented by blob store plugins that can validate a
// backup stream completely without writing to the target store. Live restore
// uses this before either store is mutated, so a truncated record, a checksum
// mismatch, or trailing data cannot be discovered only after metadata has
// already been replaced.
type BackupValidator interface {
	ValidateBackup(ctx context.Context, r io.Reader) error
}

// Resettable is implemented by remote blob stores whose configured target is
// independent of the local data directory passed to the provider. A live
// restore cannot obtain an empty target for these providers by restoring into
// a sibling directory, so it takes a rollback backup and then calls Reset
// before loading the replacement. Callers must not use Reset without first
// retaining a restorable copy of the current contents.
type Resettable interface {
	Reset(ctx context.Context) error
}
