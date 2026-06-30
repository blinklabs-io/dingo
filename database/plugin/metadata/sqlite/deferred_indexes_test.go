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

package sqlite

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
)

// TestDeferredManifestNotEmpty guards against the manifest
// accidentally being emptied — without entries we'd silently fall
// back to no-op behavior and silently lose the bulk-load speed-up.
func TestDeferredManifestNotEmpty(t *testing.T) {
	t.Parallel()
	if len(deferred.Manifest) == 0 {
		t.Fatal("deferred.Manifest is empty; bulk-load optimization disabled")
	}
}

// TestDeferredManifestResolvesToRealIndexes walks every manifest
// entry and asserts the GORM migrator sees the index after
// AutoMigrate. A failure here means a manifest entry no longer
// matches a struct-tag-declared index — usually because someone
// renamed a field or dropped the `gorm:"index"` tag without
// updating the manifest.
func TestDeferredManifestResolvesToRealIndexes(t *testing.T) {
	t.Parallel()
	d := setupTestDB(t)
	migrator := d.DB().Migrator()
	for _, idx := range deferred.Manifest {
		name := idx.ResolvedName()
		if !migrator.HasIndex(idx.Model, name) {
			t.Errorf(
				"manifest entry %q on %q not present in schema "+
					"after AutoMigrate — manifest is stale or "+
					"the struct tag was removed (notes: %s)",
				name, idx.Table, idx.Notes,
			)
		}
	}
}

// TestDropAndBuildDeferredIndexesRoundTrip exercises the full
// drop-then-rebuild flow against an in-memory database and asserts
// every manifest entry round-trips. The HasDeferredIndexesPending
// state transitions are checked at each step so callers (mithril
// sync, serve repair path) can rely on them.
func TestDropAndBuildDeferredIndexesRoundTrip(t *testing.T) {
	t.Parallel()
	d := setupTestDB(t)

	pending, err := d.HasDeferredIndexesPending()
	if err != nil {
		t.Fatalf("HasDeferredIndexesPending: %v", err)
	}
	if pending {
		t.Fatal("indexes_pending should be false at startup")
	}

	if err := d.DropDeferredIndexes(); err != nil {
		t.Fatalf("DropDeferredIndexes: %v", err)
	}

	pending, err = d.HasDeferredIndexesPending()
	if err != nil {
		t.Fatalf("HasDeferredIndexesPending after drop: %v", err)
	}
	if !pending {
		t.Fatal("indexes_pending should be true after Drop")
	}

	migrator := d.DB().Migrator()
	for _, idx := range deferred.Manifest {
		if migrator.HasIndex(idx.Model, idx.ResolvedName()) {
			t.Errorf(
				"index %q on %q still present after drop",
				idx.ResolvedName(), idx.Table,
			)
		}
	}

	if err := d.BuildDeferredIndexes(); err != nil {
		t.Fatalf("BuildDeferredIndexes: %v", err)
	}

	pending, err = d.HasDeferredIndexesPending()
	if err != nil {
		t.Fatalf("HasDeferredIndexesPending after build: %v", err)
	}
	if pending {
		t.Fatal("indexes_pending should be false after Build")
	}

	for _, idx := range deferred.Manifest {
		if !migrator.HasIndex(idx.Model, idx.ResolvedName()) {
			t.Errorf(
				"index %q on %q missing after rebuild",
				idx.ResolvedName(), idx.Table,
			)
		}
	}
}

// TestBuildCriticalDeferredIndexesOnlyRebuildsCriticalSubset verifies the
// split rebuild contract: critical indexes are restored first, lazy indexes
// remain missing, and the pending marker is not cleared until the full rebuild.
func TestBuildCriticalDeferredIndexesOnlyRebuildsCriticalSubset(t *testing.T) {
	t.Parallel()
	d := setupTestDB(t)
	if err := d.DropDeferredIndexes(); err != nil {
		t.Fatalf("DropDeferredIndexes: %v", err)
	}

	if err := d.BuildCriticalDeferredIndexes(); err != nil {
		t.Fatalf("BuildCriticalDeferredIndexes: %v", err)
	}

	pending, err := d.HasDeferredIndexesPending()
	if err != nil {
		t.Fatalf("HasDeferredIndexesPending after critical build: %v", err)
	}
	if !pending {
		t.Fatal("indexes_pending should remain true after critical-only build")
	}

	critical := map[string]bool{}
	for _, idx := range deferred.CriticalManifest() {
		critical[idx.Table+"\x00"+idx.ResolvedName()] = true
	}

	migrator := d.DB().Migrator()
	for _, idx := range deferred.Manifest {
		name := idx.ResolvedName()
		hasIndex := migrator.HasIndex(idx.Model, name)
		isCritical := critical[idx.Table+"\x00"+name]
		switch {
		case isCritical && !hasIndex:
			t.Errorf("critical index %q on %q missing after critical rebuild", name, idx.Table)
		case !isCritical && hasIndex:
			t.Errorf("lazy index %q on %q rebuilt during critical rebuild", name, idx.Table)
		}
	}

	if err := d.BuildDeferredIndexes(); err != nil {
		t.Fatalf("BuildDeferredIndexes after critical build: %v", err)
	}
	pending, err = d.HasDeferredIndexesPending()
	if err != nil {
		t.Fatalf("HasDeferredIndexesPending after full build: %v", err)
	}
	if pending {
		t.Fatal("indexes_pending should be false after full rebuild")
	}
}

// TestDropDeferredIndexesPreservesAssetImportConflictTarget covers the
// #2457 failure mode: dropping the asset policy-id query index must not drop
// idx_asset_unique, because ledger-state UTxO import uses it as the
// ON CONFLICT target for asset inserts.
func TestDropDeferredIndexesPreservesAssetImportConflictTarget(t *testing.T) {
	t.Parallel()
	d := setupTestDB(t)
	migrator := d.DB().Migrator()

	if !migrator.HasIndex(&models.Asset{}, "idx_asset_policy_id") {
		t.Fatal("asset policy-id query index missing before drop")
	}
	if !migrator.HasIndex(&models.Asset{}, "idx_asset_unique") {
		t.Fatal("asset unique import index missing before drop")
	}

	if err := d.DropDeferredIndexes(); err != nil {
		t.Fatalf("DropDeferredIndexes: %v", err)
	}

	if migrator.HasIndex(&models.Asset{}, "idx_asset_policy_id") {
		t.Fatal("asset policy-id query index still present after drop")
	}
	if !migrator.HasIndex(&models.Asset{}, "idx_asset_unique") {
		t.Fatal("asset unique import index was dropped")
	}

	txID := bytes.Repeat([]byte{0x11}, 32)
	policyID := bytes.Repeat([]byte{0x22}, 28)
	utxo := models.Utxo{
		TxId:      txID,
		OutputIdx: 0,
		AddedSlot: 100,
		Amount:    10,
		Assets: []models.Asset{{
			PolicyId:    policyID,
			Name:        []byte("asset-a"),
			NameHex:     []byte("61737365742d61"),
			Fingerprint: []byte("asset-fingerprint"),
			Amount:      1,
		}},
	}

	if err := d.ImportUtxos([]models.Utxo{utxo}, nil); err != nil {
		t.Fatalf("ImportUtxos after deferred index drop: %v", err)
	}
	if err := d.ImportUtxos([]models.Utxo{utxo}, nil); err != nil {
		t.Fatalf("duplicate ImportUtxos after deferred index drop: %v", err)
	}

	var assetCount int64
	if err := d.DB().Model(&models.Asset{}).Where(
		"policy_id = ? AND name = ?",
		policyID,
		[]byte("asset-a"),
	).Count(&assetCount).Error; err != nil {
		t.Fatalf("count assets: %v", err)
	}
	if assetCount != 1 {
		t.Fatalf("duplicate asset rows inserted: got %d, want 1", assetCount)
	}

	if err := d.BuildDeferredIndexes(); err != nil {
		t.Fatalf("BuildDeferredIndexes: %v", err)
	}
}

// TestBuildDeferredIndexesIsIdempotent confirms that calling Build
// without a prior Drop is a safe no-op (the auto-migrated schema
// already has every manifest entry). This is the path the serve
// repair flow takes when no rebuild is actually outstanding.
func TestBuildDeferredIndexesIsIdempotent(t *testing.T) {
	t.Parallel()
	d := setupTestDB(t)
	if err := d.BuildDeferredIndexes(); err != nil {
		t.Fatalf("first BuildDeferredIndexes: %v", err)
	}
	if err := d.BuildDeferredIndexes(); err != nil {
		t.Fatalf("second BuildDeferredIndexes: %v", err)
	}
	pending, err := d.HasDeferredIndexesPending()
	if err != nil {
		t.Fatalf("HasDeferredIndexesPending: %v", err)
	}
	if pending {
		t.Fatal("indexes_pending should be false after idempotent rebuild")
	}
}

// TestDropDeferredIndexesIsIdempotent guards the crash-then-resume
// path: a re-run of mithril sync after a partial drop should
// complete without errors and leave the pending marker set.
func TestDropDeferredIndexesIsIdempotent(t *testing.T) {
	t.Parallel()
	d := setupTestDB(t)
	if err := d.DropDeferredIndexes(); err != nil {
		t.Fatalf("first DropDeferredIndexes: %v", err)
	}
	if err := d.DropDeferredIndexes(); err != nil {
		t.Fatalf("second DropDeferredIndexes: %v", err)
	}
	pending, err := d.HasDeferredIndexesPending()
	if err != nil {
		t.Fatalf("HasDeferredIndexesPending: %v", err)
	}
	if !pending {
		t.Fatal("indexes_pending should remain set after repeated Drop")
	}
}

// TestCrashRecoveryDetectedAcrossReopen simulates a crash by
// running DropDeferredIndexes, closing the store, opening a fresh
// MetadataStoreSqlite against the same on-disk file, and asking the
// new instance whether indexes are pending. This is the crash
// recovery contract that `dingo serve` and `dingo mithril sync`
// rely on: the pending marker must survive a process exit.
func TestCrashRecoveryDetectedAcrossReopen(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()

	first, err := NewWithOptions(WithDataDir(dataDir))
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	if err := first.Start(); err != nil {
		t.Fatalf("start first store: %v", err)
	}
	if err := first.DropDeferredIndexes(); err != nil {
		t.Fatalf("DropDeferredIndexes: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	// Re-open the same on-disk database and confirm the pending
	// marker persisted across the close/reopen.
	second, err := NewWithOptions(WithDataDir(dataDir))
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	if err := second.Start(); err != nil {
		t.Fatalf("start reopened store: %v", err)
	}
	t.Cleanup(func() { second.Close() }) //nolint:errcheck

	pending, err := second.HasDeferredIndexesPending()
	if err != nil {
		t.Fatalf("HasDeferredIndexesPending after reopen: %v", err)
	}
	if !pending {
		t.Fatal("indexes_pending marker did not survive reopen")
	}

	// And the rebuild path on the new instance clears the marker
	// — i.e. the recovery flow actually completes.
	if err := second.BuildDeferredIndexes(); err != nil {
		t.Fatalf("BuildDeferredIndexes after reopen: %v", err)
	}
	pending, err = second.HasDeferredIndexesPending()
	if err != nil {
		t.Fatalf("HasDeferredIndexesPending after rebuild: %v", err)
	}
	if pending {
		t.Fatal("indexes_pending should be false after rebuild on reopened store")
	}
}
