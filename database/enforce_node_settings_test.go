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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

func TestEnforceNodeSettingsPersistsGenesisHashesOnFirstStart(t *testing.T) {
	dir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir: dir, StorageMode: "core", Network: "preprod",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.NoError(t, db.EnforceNodeSettings(nodesettings.Values{
		"shelley_genesis_hash": "aaaa",
		"conway_genesis_hash":  "bbbb",
	}))
	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, "aaaa", gates["shelley_genesis_hash"])
	require.NoError(t, db.Close())
}

func TestEnforceNodeSettingsRejectsGenesisHashChange(t *testing.T) {
	dir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir: dir, StorageMode: "core", Network: "preprod",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.NoError(t, db.EnforceNodeSettings(nodesettings.Values{
		"shelley_genesis_hash": "aaaa",
	}))
	enforceErr := db.EnforceNodeSettings(nodesettings.Values{
		"shelley_genesis_hash": "cccc",
	})
	var settingsErr NodeSettingsError
	require.ErrorAs(t, enforceErr, &settingsErr)
	require.Contains(t, settingsErr.Error(), "Shelley genesis hash")
	require.NoError(t, db.Close())
}

func TestEnforceNodeSettingsFillsHashLearnedLater(t *testing.T) {
	// An earlier dingo did not know the dijkstra hash; a later one does.
	dir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir: dir, StorageMode: "core", Network: "preprod",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.NoError(t, db.EnforceNodeSettings(nodesettings.Values{
		"dijkstra_genesis_hash": "",
	}))
	require.NoError(t, db.EnforceNodeSettings(nodesettings.Values{
		"dijkstra_genesis_hash": "dddd",
	}))
	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, "dddd", gates["dijkstra_genesis_hash"])
	require.NoError(t, db.Close())
}

func TestEnforceNodeSettingsLedgerGateActivationRecordsEpoch(t *testing.T) {
	dir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir: dir, StorageMode: "core", Network: "preprod",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	off := nodesettings.EncodeLatchBool(false, "")
	require.NoError(t, db.EnforceNodeSettings(nodesettings.Values{
		"pledge_leverage": off,
	}))
	on := nodesettings.EncodeLatchBool(true, "3")
	require.NoError(t, db.EnforceNodeSettings(nodesettings.Values{
		"pledge_leverage": on,
	}))
	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, on, gates["pledge_leverage"])
	require.NoError(t, db.Close())
}

func TestEnforceNodeSettingsRecordsValidationTaintOnFirstStart(t *testing.T) {
	dir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir: dir, StorageMode: "core", Network: "preprod",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.NoError(t, db.EnforceNodeSettings(nodesettings.Values{
		"historical_validation_relaxed": nodesettings.LatchOn,
	}))
	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(
		t,
		nodesettings.LatchOn,
		gates["historical_validation_relaxed"],
	)
	require.NoError(t, db.Close())
}

func TestEnforceNodeSettingsRejectsRelaxingValidationOnStrictDatabase(
	t *testing.T,
) {
	dir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir: dir, StorageMode: "core", Network: "preprod",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.NoError(t, db.EnforceNodeSettings(nodesettings.Values{
		"historical_validation_relaxed": nodesettings.LatchOff,
	}))
	enforceErr := db.EnforceNodeSettings(nodesettings.Values{
		"historical_validation_relaxed": nodesettings.LatchOn,
	})
	var settingsErr NodeSettingsError
	require.ErrorAs(t, enforceErr, &settingsErr)
	require.NoError(t, db.Close())
}

func TestEnforceNodeSettingsKeepsTaintWhenValidationTightens(t *testing.T) {
	dir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir: dir, StorageMode: "core", Network: "preprod",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.NoError(t, db.EnforceNodeSettings(nodesettings.Values{
		"historical_validation_relaxed": nodesettings.LatchOn,
	}))
	// Tightening is allowed and must not clear the record.
	require.NoError(t, db.EnforceNodeSettings(nodesettings.Values{
		"historical_validation_relaxed": nodesettings.LatchOff,
	}))
	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(
		t,
		nodesettings.LatchOn,
		gates["historical_validation_relaxed"],
	)
	require.NoError(t, db.Close())
}

func TestEnforceNodeSettingsRejectsDisablingLedgerGate(t *testing.T) {
	dir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir: dir, StorageMode: "core", Network: "preprod",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.NoError(t, db.EnforceNodeSettings(nodesettings.Values{
		"full_pot_rewards": nodesettings.EncodeLatchBool(true, ""),
	}))
	enforceErr := db.EnforceNodeSettings(nodesettings.Values{
		"full_pot_rewards": nodesettings.EncodeLatchBool(false, ""),
	})
	var settingsErr NodeSettingsError
	require.ErrorAs(t, enforceErr, &settingsErr)
	require.NoError(t, db.Close())
}

// TestEnforceNodeSettingsPhase2GatesDoNotLeakIntoPhase1 pins the reason
// history_expiry_active and the two validation taints were moved out of
// database.New's phase 1 and into EnforceNodeSettings's phase 2: a bool has
// no "unknown" sentinel, so computing them from a zero-value partial Config
// -- the shape mithril/sync.go and database/lifecycle/restore.go actually
// construct -- would either fabricate a relaxed taint of "on" against every
// normal database (historical_validation_relaxed computed from a zero
// validateHistorical) or fabricate a forbidden "off" latch against a
// database that legitimately ran with expiry on (history_expiry_active
// computed from a zero Enabled). Opening with a full config, closing, then
// reopening with only the fields those two partial callers set must
// succeed.
func TestEnforceNodeSettingsPhase2GatesDoNotLeakIntoPhase1(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	db, err := newTestDatabase(t, &Config{
		DataDir:              dir,
		StorageMode:          "core",
		Network:              "preprod",
		Logger:               logger,
		StrictUtxoValidation: true,
	})
	require.NoError(t, err)
	require.NoError(t, db.EnforceNodeSettings(nodesettings.Values{
		"history_expiry_active": nodesettings.EncodeLatchBool(true, ""),
		"historical_validation_relaxed": nodesettings.EncodeLatchBool(
			false, "",
		),
		"strict_utxo_validation_relaxed": nodesettings.EncodeLatchBool(
			false, "",
		),
	}))
	// closeTestDatabase, not db.Close: db.Close alone leaves this test's
	// badger host running (tb.Cleanup stops it at the very end of the
	// test), which would hold the directory lock the reopen below needs.
	require.NoError(t, closeTestDatabase(db))

	// Reopen the way mithril/sync.go:1200 and
	// database/lifecycle/restore.go:609 do: only DataDir, Logger,
	// StorageMode, and Network. If either gate leaked into phase 1's
	// CheckNodeSettings this reopen would fail: history_expiry_active would
	// compute "off" from the zero value (a forbidden latch reversal against
	// the "on" just recorded), and the validation taints would compute "on"
	// from zero-value bools (rejected against the "off" -- untainted --
	// state just recorded).
	reopened, err := newTestDatabase(t, &Config{
		DataDir:     dir,
		StorageMode: "core",
		Network:     "preprod",
		Logger:      logger,
	})
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}
