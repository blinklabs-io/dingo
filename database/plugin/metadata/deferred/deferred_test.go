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
		key := fmt.Sprintf("%s:%s", idx.Table, idx.Name)
		if prev, ok := seen[key]; ok {
			t.Errorf(
				"duplicate manifest entry at indices %d and %d (table=%q name=%q)",
				prev,
				i,
				idx.Table,
				idx.Name,
			)
		}
		seen[key] = i
	}
}

// TestManifestEntriesHaveResolvableName confirms each entry carries everything the
// shared SQL store needs to rebuild it.
func TestManifestEntriesHaveResolvableName(t *testing.T) {
	for i, idx := range Manifest {
		if idx.Name == "" {
			t.Errorf(
				"manifest entry %d (table=%q) has an empty Name",
				i, idx.Table,
			)
		}
		if idx.Table == "" {
			t.Errorf(
				"manifest entry %d (name=%q) has empty Table",
				i, idx.Name,
			)
		}
		if len(idx.Columns) == 0 {
			t.Errorf(
				"manifest entry %d (table=%q) has no columns",
				i, idx.Table,
			)
		}
	}
}

func TestManifestKeepsImportIdempotencyIndexes(t *testing.T) {
	want := map[string]string{
		"idx_key_witness_transaction_id":         "key_witness",
		"idx_witness_scripts_transaction_id":     "witness_scripts",
		"idx_redeemer_transaction_id":            "redeemer",
		"idx_plutus_data_transaction_id":         "plutus_data",
		"idx_address_transaction_transaction_id": "address_transaction",
		"idx_certs_transaction_id":               "certs",
	}
	manifest := make(map[string]bool, len(Manifest))
	for _, idx := range Manifest {
		manifest[idx.Name] = true
	}
	retained := make(map[string]string, len(Retained))
	for _, idx := range Retained {
		retained[idx.Name] = idx.Table
	}
	for name, table := range want {
		if manifest[name] {
			t.Errorf("%s is deferred despite being used by import cleanup", name)
		}
		if retained[name] != table {
			t.Errorf("%s retained table = %q, want %q", name, retained[name], table)
		}
	}
}

// TestRetainedIsDisjointFromManifest guards the contract the repair rests on:
// an index cannot be both dropped for bulk load and restored before the drop.
func TestRetainedIsDisjointFromManifest(t *testing.T) {
	deferredNames := map[string]bool{}
	for _, idx := range Manifest {
		deferredNames[idx.Name] = true
	}
	for _, idx := range Retained {
		if deferredNames[idx.Name] {
			t.Errorf(
				"%q is in both Manifest and Retained; an index is either "+
					"deferrable or kept resident, not both",
				idx.Name,
			)
		}
	}
}

// TestRetainedEntriesAreResolvable confirms each entry carries what the shared
// SQL store needs to recreate it, since a Retained entry is only ever used to
// issue CREATE INDEX for an index that is already missing.
func TestRetainedEntriesAreResolvable(t *testing.T) {
	if len(Retained) == 0 {
		t.Fatal("Retained is empty; the exclusion rule names entries")
	}
	seen := map[string]int{}
	for i, idx := range Retained {
		if idx.Name == "" || idx.Table == "" || len(idx.Columns) == 0 {
			t.Errorf(
				"retained entry %d is incomplete (name=%q table=%q columns=%v)",
				i, idx.Name, idx.Table, idx.Columns,
			)
		}
		if prev, ok := seen[idx.Name]; ok {
			t.Errorf(
				"duplicate retained entry %q at indices %d and %d",
				idx.Name, prev, i,
			)
		}
		seen[idx.Name] = i
	}
}
