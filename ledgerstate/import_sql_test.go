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

package ledgerstate

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// A ledger-state import writes the LedgerDB set and the block store: it runs
// no statement on the ledger's metadata store, and the checkpoint lands where
// the sync reads it.
func TestImportLedgerStateRunsNoSQL(t *testing.T) {
	t.Parallel()
	guard := &dbtest.SQLRefusal{}
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config:         &database.Config{DataDir: t.TempDir()},
		StatementGuard: guard,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	set := openFixtureSet(t)
	cfg := richConfig(t, db, fixtureAnchorSlot, true, false, set)
	cfg.ImportKey = "abcd:10"
	guard.Record()

	require.NoError(t, importLedgerStateForTest(t.Context(), cfg))

	require.Empty(t, guard.Statements())
	phase, err := db.ImportCheckpoint("abcd:10")
	require.NoError(t, err)
	require.Equal(t, models.ImportPhaseTip, phase)
}
