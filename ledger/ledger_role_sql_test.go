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

package ledger

import (
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// The ledger applies blocks and rolls back against its block store and the
// live UTxO set alone: with every statement on its metadata store refused,
// both succeed and none was attempted.
func TestLedgerApplyAndRollbackIssueNoSQL(t *testing.T) {
	t.Parallel()
	guard := &dbtest.SQLRefusal{}
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config:         &database.Config{DataDir: t.TempDir()},
		StatementGuard: guard,
	})
	require.NoError(t, err)
	blocks, _ := externalIngestBlocks(t, 0)
	run := runRowBatchingScenarioOnDB(
		t, db, blocks, true, 2, false,
		func(*LedgerState) { guard.Arm() },
	)
	require.Empty(t, guard.Statements(), "block apply")

	target := blockPoint(blocks[0])
	require.NoError(t, run.ls.rollback(t.Context(), target))
	require.Empty(t, guard.Statements(), "rollback")
	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, target.Hash, tip.Point.Hash)
}

// The ledger's node settings live in its data directory: opening a fresh
// ledger database, reopening it, and running both settings phases record and
// enforce them without a statement on the metadata store's settings tables.
func TestLedgerNodeSettingsIssueNoSettingsTableSQL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := &database.Config{
		DataDir:        dir,
		StorageMode:    "core",
		Network:        "preprod",
		NetworkMagic:   1,
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	}
	// The metadata store's own start reads the per-family state mask, which
	// stays in its settings table: that one statement is not a settings
	// read.
	const maskRead = "SELECT value FROM node_settings_gate WHERE name = ?"
	settingsSQL := func(guard *dbtest.SQLRefusal) []string {
		var found []string
		for _, statement := range guard.Statements() {
			if strings.Contains(statement, "node_settings") &&
				statement != maskRead {
				found = append(found, statement)
			}
		}
		return found
	}
	for _, open := range []string{"fresh", "reopened"} {
		guard := &dbtest.SQLRefusal{}
		guard.Record()
		db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
			Config:         cfg,
			StatementGuard: guard,
		})
		require.NoError(t, err, open)
		require.NoError(t, db.CheckNodeSettings(t.Context()), open)
		require.NoError(t, db.EnforceNodeSettings(t.Context(),
			nodesettings.Values{
				"history_expiry_active": nodesettings.LatchOn,
			},
		), open)
		require.Empty(t, settingsSQL(guard), open)
		gates, err := db.NodeSettingsGates(t.Context())
		require.NoError(t, err, open)
		require.Equal(t, "preprod", gates["network"], open)
		require.Equal(t, nodesettings.LatchOn,
			gates["history_expiry_active"], open)
		require.NoError(t, dbtest.CloseDatabase(db), open)
	}
}
