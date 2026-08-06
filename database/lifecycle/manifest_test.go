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

package lifecycle_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

func testManifest() lifecycle.Manifest {
	return lifecycle.Manifest{
		CreatedAt:      time.Unix(1700000000, 0).UTC(),
		Trigger:        lifecycle.TriggerManual,
		StorageMode:    "core",
		Network:        "mainnet",
		TipSlot:        12345,
		TipHash:        []byte{0xde, 0xad, 0xbe, 0xef},
		TipBlockNumber: 100,
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DingoVersion:   "test",
	}
}

// TestManifestRoundTrip verifies that a written manifest reads back with
// matching fields, a valid checksum, and the current format version.
func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, lifecycle.WriteManifest(dir, testManifest()))

	got, err := lifecycle.ReadManifest(dir)
	require.NoError(t, err)
	require.Equal(t, "mainnet", got.Network)
	require.Equal(t, uint64(12345), got.TipSlot)
	require.Equal(t, lifecycle.ManifestFormatVersion, got.FormatVersion)
	require.NotEmpty(t, got.Checksum)
}

// TestManifestDetectsTamperedContent verifies that a hand-edited manifest
// fails checksum validation with the ErrManifestCorrupted sentinel.
func TestManifestDetectsTamperedContent(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, lifecycle.WriteManifest(dir, testManifest()))

	path := filepath.Join(dir, lifecycle.ManifestFileName)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	tampered := strings.Replace(
		string(data),
		`"network": "mainnet"`,
		`"network": "testnet"`,
		1,
	)
	require.NotEqual(t, string(data), tampered)
	require.NoError(t, os.WriteFile(path, []byte(tampered), 0o644))

	_, err = lifecycle.ReadManifest(dir)
	require.Error(t, err)
	require.True(
		t,
		errors.Is(err, lifecycle.ErrManifestCorrupted),
		"tampered manifest must be distinguishable (via errors.Is) from a "+
			"manifest that simply isn't there, so callers like bark's "+
			"resolveSnapshotSource can report corruption instead of "+
			"'not found'",
	)
}

// TestManifestRejectsNewerFormatVersion verifies that a manifest whose
// formatVersion exceeds what this build understands is rejected.
func TestManifestRejectsNewerFormatVersion(t *testing.T) {
	dir := t.TempDir()
	m := testManifest()
	require.NoError(t, lifecycle.WriteManifest(dir, m))

	// Directly bump the on-disk format version past what this build
	// understands, without recomputing the checksum, mirroring an
	// actually-newer manifest whose extra fields we can't see.
	path := filepath.Join(dir, lifecycle.ManifestFileName)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	patched := strings.Replace(
		string(data),
		`"formatVersion": 1`,
		`"formatVersion": 999`,
		1,
	)
	require.NotEqual(t, string(data), patched)
	require.NoError(t, os.WriteFile(path, []byte(patched), 0o644))

	_, err = lifecycle.ReadManifest(dir)
	require.Error(t, err)
}

// TestCheckPluginMatch verifies that CheckPluginMatch passes for matching
// blob/metadata plugins and errors on either mismatching.
// TestWriteManifestLeavesNoLeftoverTempFile verifies that a successful
// WriteManifest cleans up after itself: only the final manifest.json
// remains, no stray same-directory temp file used for the atomic rename.
func TestWriteManifestLeavesNoLeftoverTempFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, lifecycle.WriteManifest(dir, testManifest()))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, lifecycle.ManifestFileName, entries[0].Name())
}

// TestWriteManifestFailureLeavesExistingManifestUntouched guards against
// a real gap: WriteManifest used to write directly to
// manifest.json (truncating any existing content first), so an
// interruption partway through could leave a corrupt, partially-written
// file in its place -- most dangerous for LabelSnapshot, which rewrites
// the manifest of an already-complete snapshot. This forces the failure
// mode a truncating write can't ever produce cleanly: something already
// occupies manifest.json's path in a way the final atomic rename cannot
// replace (a non-empty directory, which os.Rename refuses to replace
// with a regular file), after the new manifest's temp file has already
// been fully written and synced. WriteManifest must fail without
// touching whatever was already there, and without leaving its own temp
// file behind.
func TestWriteManifestFailureLeavesExistingManifestUntouched(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, lifecycle.ManifestFileName)
	require.NoError(t, os.Mkdir(manifestPath, 0o755))
	sentinelPath := filepath.Join(manifestPath, "sentinel")
	require.NoError(t, os.WriteFile(sentinelPath, []byte("original"), 0o644))

	err := lifecycle.WriteManifest(dir, testManifest())
	require.Error(t, err)

	require.DirExists(t, manifestPath)
	data, readErr := os.ReadFile(sentinelPath)
	require.NoError(t, readErr)
	require.Equal(t, "original", string(data))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(
		t, entries, 1,
		"a failed WriteManifest must not leave its temp file behind",
	)
}

func TestCheckPluginMatch(t *testing.T) {
	m := testManifest()
	require.NoError(t, m.CheckPluginMatch("badger", "sqlite"))
	require.Error(t, m.CheckPluginMatch("gcs", "sqlite"))
	require.Error(t, m.CheckPluginMatch("badger", "postgres"))
}

// TestLabelSnapshotSetsNameAndDescription verifies that LabelSnapshot
// sets Name/Description and rewrites the manifest with all else unchanged.
func TestLabelSnapshotSetsNameAndDescription(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, lifecycle.WriteManifest(dir, testManifest()))

	require.NoError(
		t,
		lifecycle.LabelSnapshot(dir, "nightly", "pre-hardfork backup"),
	)

	got, err := lifecycle.ReadManifest(dir)
	require.NoError(t, err)
	require.Equal(t, "nightly", got.Name)
	require.Equal(t, "pre-hardfork backup", got.Description)
	// Everything else must survive unchanged, including checksum validity.
	require.Equal(t, testManifest().TipSlot, got.TipSlot)
}

// TestLabelSnapshotMissingManifestErrors verifies that labeling a
// directory with no manifest.json returns an error.
func TestLabelSnapshotMissingManifestErrors(t *testing.T) {
	dir := t.TempDir()
	err := lifecycle.LabelSnapshot(dir, "name", "description")
	require.Error(t, err)
}

// TestCheckGateMatchAcceptsIdenticalGates verifies that CheckGateMatch
// passes when a gate the manifest recorded has an identical value in the
// caller's configured map.
func TestCheckGateMatchAcceptsIdenticalGates(t *testing.T) {
	m := lifecycle.Manifest{Gates: nodesettings.Values{"network_magic": "1"}}
	require.NoError(
		t,
		m.CheckGateMatch(nodesettings.Values{"network_magic": "1"}),
	)
}

// TestCheckGateMatchRejectsDifferingGate verifies that CheckGateMatch
// errors, naming the gate, when a gate present in both maps disagrees.
func TestCheckGateMatchRejectsDifferingGate(t *testing.T) {
	m := lifecycle.Manifest{Gates: nodesettings.Values{"network_magic": "1"}}
	err := m.CheckGateMatch(nodesettings.Values{"network_magic": "2"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "network_magic")
}

// TestCheckGateMatchIgnoresGatesTheTargetDoesNotKnow verifies that a gate
// recorded in the manifest but absent from the caller's configured map
// (e.g. a genesis hash gate when the caller has no cardano config loaded)
// is not an error -- CheckGateMatch only compares gates present in both.
func TestCheckGateMatchIgnoresGatesTheTargetDoesNotKnow(t *testing.T) {
	m := lifecycle.Manifest{Gates: nodesettings.Values{
		"network_magic":         "1",
		"dijkstra_genesis_hash": "dddd",
	}}
	require.NoError(
		t,
		m.CheckGateMatch(nodesettings.Values{"network_magic": "1"}),
	)
}

// TestChecksumCoversGates verifies that Gates is covered by the manifest
// checksum. Exercised through the Write/Read round trip the existing tests
// use, rather than reaching for the unexported checksum method: two
// manifests differing only in Gates must produce different checksums.
func TestChecksumCoversGates(t *testing.T) {
	dirA, dirB := t.TempDir(), t.TempDir()
	a := lifecycle.Manifest{Gates: nodesettings.Values{"network_magic": "1"}}
	b := lifecycle.Manifest{Gates: nodesettings.Values{"network_magic": "2"}}
	require.NoError(t, lifecycle.WriteManifest(dirA, a))
	require.NoError(t, lifecycle.WriteManifest(dirB, b))
	gotA, err := lifecycle.ReadManifest(dirA)
	require.NoError(t, err)
	gotB, err := lifecycle.ReadManifest(dirB)
	require.NoError(t, err)
	require.NotEqual(t, gotA.Checksum, gotB.Checksum)
}

// TestCheckGateMatchIgnoresBlobStoreID verifies that blob_store_id is
// never compared, even when present in both maps: the restored blob store
// IS the snapshot's, so its identity always differs from whatever the
// caller had, and comparing it would fail every restore.
func TestCheckGateMatchIgnoresBlobStoreID(t *testing.T) {
	m := lifecycle.Manifest{Gates: nodesettings.Values{
		"blob_store_id": "aaaa-from-snapshot",
	}}
	require.NoError(t, m.CheckGateMatch(nodesettings.Values{
		"blob_store_id": "bbbb-different",
	}))
}

// TestCheckGateMatchToleratesGateMissingFromOlderSnapshot verifies the
// opposite direction from TestCheckGateMatchIgnoresGatesTheTargetDoesNotKnow:
// an older snapshot predates a gate this dingo now records. The caller
// supplies it, the manifest does not carry it, and that must NOT be a
// mismatch -- otherwise every older snapshot becomes unrestorable the
// moment a new gate is added to the registry.
func TestCheckGateMatchToleratesGateMissingFromOlderSnapshot(t *testing.T) {
	m := lifecycle.Manifest{Gates: nodesettings.Values{
		"network_magic": "1",
	}}
	require.NoError(t, m.CheckGateMatch(nodesettings.Values{
		"network_magic": "1",
		"start_era":     "dijkstra", // absent from the manifest
	}))
}
