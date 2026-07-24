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

//go:build !windows

// Package fsyncdir syncs a directory's own entries (a new or renamed
// file appearing in it) to disk, so that change is durable across a
// crash, not just the changed file's own content -- a file's own fsync
// does not guarantee its directory entry is persisted. This is shared by
// database/plugin/metadata/sqlite and database/lifecycle, both of which
// need this after writing a restored/renamed file into a directory,
// rather than each keeping its own platform-specific copy to drift out
// of sync.
package fsyncdir

import (
	"fmt"
	"os"
)

// Sync opens and fsyncs the directory at path.
func Sync(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %q for directory sync: %w", path, err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync directory %q: %w", path, err)
	}
	return nil
}
