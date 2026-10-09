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

// TestCriticalManifestNotEmpty ensures CriticalManifest returns a
// non-empty slice and that every entry in it also appears in Manifest.
func TestCriticalManifestNotEmpty(t *testing.T) {
	critical := CriticalManifest()
	if len(critical) == 0 {
		t.Fatal("CriticalManifest returned empty slice")
	}
	// Pin the expected count so accidental de-classification is caught.
	// idx_utxo_staking_deleted_amount left the manifest entirely: the
	// API-backfill per-batch live-stake SUM needs it during bulk load,
	// so it is never dropped and no longer needs a critical rebuild slot.
	// idx_utxo_transaction_id joined the critical subset: the rollback's
	// DELETE FROM "transaction" cascades through it, and a rollback can
	// run as soon as the database is marked ready.
	const wantCritical = 19
	if len(critical) != wantCritical {
		t.Errorf(
			"CriticalManifest: got %d entries, want %d — update this constant if the classification changed intentionally",
			len(critical),
			wantCritical,
		)
	}
	// Every critical entry must exist in the full manifest.
	full := map[string]bool{}
	for _, idx := range Manifest {
		full[fmt.Sprintf("%s:%s", idx.Table, idx.Name)] = true
	}
	for _, idx := range critical {
		key := fmt.Sprintf("%s:%s", idx.Table, idx.Name)
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

// TestManifestDefersNewTransactionDetailIndexes pins the indexes whose
// idempotency deletes are skipped for a fresh transaction ID during batched
// replay. Existing transaction replays still run those deletes.
func TestManifestDefersNewTransactionDetailIndexes(t *testing.T) {
	want := map[string]string{
		"idx_key_witness_transaction_id":         "key_witness",
		"idx_witness_scripts_transaction_id":     "witness_scripts",
		"idx_redeemer_transaction_id":            "redeemer",
		"idx_plutus_data_transaction_id":         "plutus_data",
		"idx_address_transaction_transaction_id": "address_transaction",
		"idx_certs_transaction_id":               "certs",
	}
	found := make(map[string]bool, len(want))
	for _, idx := range Manifest {
		if table, ok := want[idx.Name]; ok {
			found[idx.Name] = true
			if idx.Table != table {
				t.Errorf("%s table = %q, want %q", idx.Name, idx.Table, table)
			}
			if !idx.Critical {
				t.Errorf("%s must be rebuilt before API and rollback traffic", idx.Name)
			}
		}
	}
	for name := range want {
		if !found[name] {
			t.Errorf("%s missing from deferred-index manifest", name)
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

// TestRetainedCoversLiveStakeImportIndex keeps the live-stake aggregation
// index out of the deferred manifest because backfill queries it per batch.
func TestRetainedCoversLiveStakeImportIndex(t *testing.T) {
	want := []string{"idx_utxo_staking_deleted_amount"}
	present := map[string]bool{}
	for _, idx := range Retained {
		present[idx.Name] = true
	}
	for _, name := range want {
		if !present[name] {
			t.Errorf("%q is missing from Retained", name)
		}
	}
}
