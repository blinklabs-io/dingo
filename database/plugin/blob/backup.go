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
