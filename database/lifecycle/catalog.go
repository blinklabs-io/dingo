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
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// SnapshotEntry is one catalog entry produced by ListSnapshots: a
// snapshot's directory name (its ID, in callers like bark's
// DatabaseService that key snapshots by directory name under a fixed base
// directory) paired with its manifest.
type SnapshotEntry struct {
	ID       string
	Manifest Manifest
}

// ListSnapshots scans baseDir's immediate subdirectories for a valid
// manifest.json, returning one SnapshotEntry per readable snapshot, newest
// first (by Manifest.CreatedAt). This covers both manually and
// automatically (epoch-boundary) triggered snapshots, since both are
// written as ordinary subdirectories of the same configured snapshot
// directory — there is no separate catalog store.
//
// A subdirectory that exists but has no valid manifest.json (a snapshot
// still being written, or one that failed partway through) is silently
// skipped rather than treated as a hard error, since that is an expected
// transient state, not corruption of the catalog itself. baseDir not
// existing yet (no snapshot has ever been taken) returns an empty result,
// not an error.
func ListSnapshots(baseDir string) ([]SnapshotEntry, error) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []SnapshotEntry{}, nil
		}
		return nil, fmt.Errorf("read snapshot directory %q: %w", baseDir, err)
	}

	result := []SnapshotEntry{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(baseDir, entry.Name())
		m, err := ReadManifest(dir)
		if err != nil {
			continue
		}
		result = append(result, SnapshotEntry{ID: entry.Name(), Manifest: m})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Manifest.CreatedAt.After(result[j].Manifest.CreatedAt)
	})
	return result, nil
}
