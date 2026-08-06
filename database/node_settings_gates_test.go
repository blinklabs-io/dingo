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

package database

import (
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

func TestPhase1PersistsNetworkMagicOnFirstStart(t *testing.T) {
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:      dataDir,
		StorageMode:  "core",
		Network:      "preprod",
		NetworkMagic: 1,
	})
	require.NoError(t, err)
	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, "1", gates["network_magic"])
	require.NoError(t, closeTestDatabase(db))
}

func TestPhase1RejectsNetworkMagicChange(t *testing.T) {
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:      dataDir,
		StorageMode:  "core",
		Network:      "preprod",
		NetworkMagic: 1,
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	_, err = newTestDatabase(t, &Config{
		DataDir:      dataDir,
		StorageMode:  "core",
		Network:      "preprod",
		NetworkMagic: 2,
	})
	var settingsErr NodeSettingsError
	require.True(t, errors.As(err, &settingsErr))
	require.Contains(t, settingsErr.Error(), "network magic")
}

// TestPhase1RecordsNoStartEraAndRejectsLaterDijkstra pins the fix for the
// common case a database that ran with no start era override previously
// recorded nothing at all for the gate (phase1GateValues emitted "" and
// FrozenFillOnce's first-start rule skips writing an empty configured
// value), so a later --start-era dijkstra against that same database was
// silently accepted as a first-time fill instead of rejected as the
// consensus-affecting flip the gate exists to freeze. A full caller (one
// that sets MetadataPlugin, distinguishing it from mithril/sync.go's and
// restore.go's partial reopen) must now persist nodesettings.NoStartEra
// instead, so the comparison actually happens on the next open.
func TestPhase1RecordsNoStartEraAndRejectsLaterDijkstra(t *testing.T) {
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, nodesettings.NoStartEra, gates["start_era"])
	require.NoError(t, closeTestDatabase(db))

	_, err = newTestDatabase(t, &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		StartEra:       "dijkstra",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	var settingsErr NodeSettingsError
	require.True(t, errors.As(err, &settingsErr))
	require.Contains(t, settingsErr.Error(), "dijkstra")
}

func TestPhase1RejectsCoreToAPI(t *testing.T) {
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	_, err = newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "api",
		Network:     "preprod",
	})
	var settingsErr NodeSettingsError
	require.True(t, errors.As(err, &settingsErr))
}

// TestPhase1AllowsAPIToCore pins the round-3 fix for a latch write that
// was silently discarded: reopened.StorageMode() alone (the original
// assertion here) reads back d.config, not the persisted row, so it
// passes vacuously even when the write never reached the store -- which is
// exactly what happened before node_settings_gate became authoritative
// for storage_mode (see persistedGateValues's doc comment). This asserts
// the actual persisted gate value instead, and additionally that the
// latch really did latch: a further reopen as "api" must now be rejected,
// which is the behavior the whole test is meant to pin and which nothing
// here previously asserted.
func TestPhase1AllowsAPIToCore(t *testing.T) {
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "api",
		Network:     "preprod",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	gates, err := reopened.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, "core", gates["storage_mode"])
	require.NoError(t, closeTestDatabase(reopened))

	_, err = newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "api",
		Network:     "preprod",
	})
	var settingsErr NodeSettingsError
	require.True(t, errors.As(err, &settingsErr))
}

// TestPhase1LatchAndNetworkFillTogether pins Critical 2 from round 3: when
// storage_mode latches (api -> core) and network is filled for the first
// time on the very same open, both gates must land in node_settings_gate,
// and the legacy node_settings row's network backfill (best-effort, for
// older tooling -- see writeGateValues's doc comment) must also succeed.
// The bug this guards was using the new, not-yet-persisted effective
// storage_mode ("core") as the backfill's WHERE match key while the row's
// actual physical storage_mode column was still "api" from first insert:
// the match always missed, and network was silently never recorded either
// in the legacy row or (in an earlier version of this fix) in
// node_settings_gate.
func TestPhase1LatchAndNetworkFillTogether(t *testing.T) {
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "api",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	gates, err := reopened.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, "core", gates["storage_mode"])
	require.Equal(t, "preprod", gates["network"])
	legacy, err := reopened.Metadata().GetNodeSettings()
	require.NoError(t, err)
	require.NotNil(t, legacy)
	require.Equal(t, "preprod", legacy.Network)
	require.NoError(t, closeTestDatabase(reopened))
}

// TestPhase1SkipsPartialConfigWithoutTripping guards the regression found
// while auditing mithril/sync.go and database/lifecycle/restore.go: both
// reopen an existing database with only DataDir/Logger/StorageMode/Network
// set, since their config types (mithril.SyncConfig, the restore Manifest)
// carry nothing else. A database first opened with a fuller config must
// still be reopenable that way -- every gate the partial reopen cannot
// supply (NetworkMagic, BlobPlugin, MetadataPlugin, all opt-in-absent) or
// cannot express as "unknown" via its own zero value (StartEra, whose
// FrozenFillOnce class treats an empty configured value as "not known on
// this path") must be skipped rather than compared, not silently rejected.
// This is also why no bool-derived gate -- the two validation taints, and
// history_expiry_active -- lives in phase1GateValues: a bool has no such
// "unknown" state, so they all belong to phase 2 instead (see
// TestPhase1SkipsHistoryExpiryGateOnPartialReopen for the regression this
// specifically guards for history_expiry_active).
func TestPhase1SkipsPartialConfigWithoutTripping(t *testing.T) {
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:              dataDir,
		StorageMode:          "core",
		Network:              "preprod",
		NetworkMagic:         1,
		StartEra:             "dijkstra",
		StrictUtxoValidation: true,
		BlobPlugin:           "badger",
		MetadataPlugin:       "sqlite",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(reopened))
}

// TestPhase1SkipsHistoryExpiryGateOnPartialReopen pins the regression found
// while auditing mithril/sync.go and database/lifecycle/restore.go for
// history_expiry_active specifically: database.Config has no
// HistoryExpiryActive field (its LatchBool gate is phase 2's
// responsibility, written by EnforceNodeSettings once a full node has
// started with expiry on), so a database that persisted
// history_expiry_active = "on" from an earlier full-config open must still
// be reopenable through a partial Config carrying only
// DataDir/Logger/StorageMode/Network -- the shape mithril/sync.go:1200 and
// database/lifecycle/restore.go:609 use. Before the fix, computing "off"
// from the field's zero value on that reopen would trip LatchBool's
// "cannot be turned off once enabled" mismatch, which is exactly the
// regression this guards.
func TestPhase1SkipsHistoryExpiryGateOnPartialReopen(t *testing.T) {
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	// Simulate a database that previously ran with history expiry enabled:
	// there is no phase-1 Config field to drive this through, so persist the
	// gate directly the way phase 2's write path eventually will.
	require.NoError(t, db.Metadata().SetNodeSettingsGates(
		nodesettings.Values{"history_expiry_active": nodesettings.LatchOn},
		0,
		0,
	))
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(reopened))
}

// openForRecoveryTest opens a database directly through New, the same way
// node.go does, rather than through newTestDatabase: newTestDatabase
// discards the returned *Database on any error, but node.go's
// dbNeedsRecovery path -- and this test -- specifically needs the
// *Database New still returns alongside a CommitTimestampError, since a
// database on that path is available for recovery rather than closed. See
// newTestDatabaseWithHost's doc comment (test_database_test.go) for the
// keepOnError contract this delegates to.
func openForRecoveryTest(
	tb testing.TB,
	config *Config,
) (*Database, error) {
	tb.Helper()
	return newTestDatabaseWithHost(tb, config, true)
}

// TestPhase1SkippedOnRecoveryPathButCatchesMismatchOnceReCheckable pins the
// P1 fix: database.New returns a CommitTimestampError before it ever calls
// CheckNodeSettings (checkCommitTimestamp runs first in init and returns
// immediately on failure), so phase 1 -- and the gates only it validates,
// like blob_plugin -- goes completely unchecked for that entire open. This
// reproduces that gap directly (a commit-timestamp mismatch combined with a
// blob_plugin change reports only CommitTimestampError, never the gate
// mismatch), then proves the fix: calling the now-exported
// CheckNodeSettings on the *Database New still returned -- exactly what
// node.go's dbNeedsRecovery path does once RecoverCommitTimestampConflict
// succeeds -- does catch it.
func TestPhase1SkippedOnRecoveryPathButCatchesMismatchOnceReCheckable(
	t *testing.T,
) {
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, "badger", gates["blob_plugin"])

	// Induce a commit-timestamp mismatch the same way
	// TestCheckCommitTimestamp_MetadataOnly does: give metadata a commit
	// timestamp with none on the blob side.
	metaTxn := db.Metadata().Transaction()
	require.NoError(t, db.Metadata().SetCommitTimestamp(123456789, metaTxn))
	require.NoError(t, metaTxn.Commit())
	require.NoError(t, closeTestDatabase(db))

	// Reopen with a changed blob_plugin. On a healthy reopen this alone
	// would be a NodeSettingsError from phase 1. Here, the commit-timestamp
	// mismatch above makes checkCommitTimestamp fail first, and init
	// returns immediately without ever reaching CheckNodeSettings.
	reopened, reopenErr := openForRecoveryTest(t, &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "gcs",
		MetadataPlugin: "sqlite",
	})
	require.Error(t, reopenErr)
	var cte CommitTimestampError
	require.ErrorAs(
		t,
		reopenErr,
		&cte,
		"phase 1 must not run before the commit-timestamp conflict is "+
			"resolved, so the error on this open must be exactly "+
			"CommitTimestampError, not a NodeSettingsError from the "+
			"blob_plugin change",
	)
	var settingsErr NodeSettingsError
	require.False(
		t,
		errors.As(reopenErr, &settingsErr),
		"the blob_plugin mismatch must not have been reported yet -- "+
			"phase 1 has not run on this open at all",
	)
	require.NotNil(
		t,
		reopened,
		"New must still return the *Database on a CommitTimestampError so "+
			"the caller can recover it, per node.go's dbNeedsRecovery path",
	)

	// This is the fix: node.go calls CheckNodeSettings explicitly once
	// RecoverCommitTimestampConflict succeeds. Simulate that here directly
	// against the *Database New returned above, without needing a real
	// ledgerState-driven recovery run (recovery repairs the commit
	// timestamp, an orthogonal concern from the blob_plugin gate this
	// checks).
	checkErr := reopened.CheckNodeSettings()
	require.True(
		t,
		errors.As(checkErr, &settingsErr),
		"re-invoking CheckNodeSettings after recovery must catch the "+
			"blob_plugin change phase 1 never got to see: got %v",
		checkErr,
	)
	require.Contains(t, settingsErr.Error(), "blob plugin")
}
