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

package deferred

import (
	"fmt"
	"testing"
)

// TestNoDuplicateManifestEntries guards against accidental
// duplication when a contributor adds a new entry next to an
// existing one for the same field.
func TestNoDuplicateManifestEntries(t *testing.T) {
	seen := map[string]int{}
	for i, idx := range Manifest {
		key := fmt.Sprintf("%T:%s:%s", idx.Model, idx.Field, idx.Name)
		if prev, ok := seen[key]; ok {
			t.Errorf(
				"duplicate manifest entry at indices %d and %d (model=%T field=%q name=%q)",
				prev, i, idx.Model, idx.Field, idx.Name,
			)
		}
		seen[key] = i
	}
}

// TestManifestEntriesHaveResolvableName confirms each entry has
// either a Field (used by GORM to resolve to the auto-named index)
// or a literal Name. An entry with neither is unusable.
func TestManifestEntriesHaveResolvableName(t *testing.T) {
	for i, idx := range Manifest {
		if idx.ResolvedName() == "" {
			t.Errorf(
				"manifest entry %d (table=%q) has neither Field nor Name set",
				i, idx.Table,
			)
		}
		if idx.Table == "" {
			t.Errorf(
				"manifest entry %d (field=%q name=%q) has empty Table",
				i, idx.Field, idx.Name,
			)
		}
		if idx.Model == nil {
			t.Errorf(
				"manifest entry %d (table=%q) has nil Model",
				i, idx.Table,
			)
		}
	}
}

// TestCriticalManifestNotEmpty ensures CriticalManifest returns a
// non-empty slice and that every entry in it also appears in Manifest.
func TestCriticalManifestNotEmpty(t *testing.T) {
	critical := CriticalManifest()
	if len(critical) == 0 {
		t.Fatal("CriticalManifest returned empty slice")
	}
	// Pin the expected count so accidental de-classification is caught.
	const wantCritical = 12
	if len(critical) != wantCritical {
		t.Errorf("CriticalManifest: got %d entries, want %d — update this constant if the classification changed intentionally", len(critical), wantCritical)
	}
	// Every critical entry must exist in the full manifest.
	full := map[string]bool{}
	for _, idx := range Manifest {
		full[fmt.Sprintf("%T:%s:%s", idx.Model, idx.Field, idx.Name)] = true
	}
	for _, idx := range critical {
		key := fmt.Sprintf("%T:%s:%s", idx.Model, idx.Field, idx.Name)
		if !full[key] {
			t.Errorf("critical entry %q not found in full Manifest", key)
		}
	}
}

// TestSyncStateConstants pins the marker key/value strings. Any
// change must be a deliberate migration: changing the key would
// orphan markers written by older binaries during a partial
// upgrade.
func TestSyncStateConstants(t *testing.T) {
	if SyncStateKey != "metadata_indexes_pending" {
		t.Errorf("SyncStateKey changed: got %q", SyncStateKey)
	}
	if SyncStateValue != "true" {
		t.Errorf("SyncStateValue changed: got %q", SyncStateValue)
	}
}
