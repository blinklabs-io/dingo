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

package main

import (
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

func gateDB(t *testing.T, withTable bool, mask string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	if !withTable {
		return db
	}
	_, err = db.Exec(`CREATE TABLE node_settings_gate (
name TEXT PRIMARY KEY, value TEXT NOT NULL,
recorded_epoch INTEGER NOT NULL, recorded_slot INTEGER NOT NULL)`)
	require.NoError(t, err)
	if mask != "" {
		_, err = db.Exec(
			`INSERT INTO node_settings_gate VALUES (?, ?, 0, 0)`,
			nodesettings.LedgerStateDrainedGateName, mask,
		)
		require.NoError(t, err)
	}
	return db
}

// The repair reads and deletes the reward tables of a metadata database, so a
// database that drained any ledger-state family is refused, and one that
// drained nothing, or predates the latch, is not.
func TestRepairRefusesADrainedDatabase(t *testing.T) {
	t.Parallel()
	drained := nodesettings.EncodeFamilyMask(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyRewardOutputs),
	)
	for _, tc := range []struct {
		name    string
		table   bool
		mask    string
		refused bool
	}{
		{"a drained family", true, drained, true},
		{"no latch row", true, "", false},
		{"no gate table", false, "", false},
		{
			"an empty latch", true,
			nodesettings.EncodeFamilyMask(0), false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := refuseDrainedDatabase(gateDB(t, tc.table, tc.mask))
			if tc.refused {
				require.ErrorIs(t, err, errDrainedDatabase)
				return
			}
			require.NoError(t, err)
		})
	}
}
