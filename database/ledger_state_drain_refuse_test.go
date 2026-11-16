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

package database_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

func TestRefuseLedgerStateRewrite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, db *database.Database)
		refused bool
	}{
		{"clean database", func(*testing.T, *database.Database) {}, false},
		{
			"latch bit",
			func(t *testing.T, db *database.Database) {
				require.NoError(t, db.RecordDrainedFamily(
					t.Context(), nodesettings.FamilyNonces,
				))
			},
			true,
		},
		{
			"drain begun",
			func(t *testing.T, db *database.Database) {
				require.NoError(t, db.SetSyncState(
					statedrain.MarkerPrefix+"nonces", "1", nil,
				))
			},
			true,
		},
		{
			"unrelated sync state",
			func(t *testing.T, db *database.Database) {
				require.NoError(t, db.SetSyncState("sync_status", "x", nil))
			},
			false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, err := dbtest.NewDatabase(t, &database.Config{})
			require.NoError(t, err)
			t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
			tc.prepare(t, db)
			err = db.RefuseLedgerStateRewrite(t.Context())
			if tc.refused {
				require.ErrorIs(t, err, database.ErrLedgerStateDrained)
				return
			}
			require.NoError(t, err)
		})
	}
}
