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
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// TestImportLedgerStateRefusesADrainedDatabase requires the import, and so a
// catch-up, to refuse before it reads the state it was given.
func TestImportLedgerStateRefusesADrainedDatabase(t *testing.T) {
	t.Parallel()
	for _, reconcile := range []bool{false, true} {
		db, err := dbtest.NewDatabase(t, &database.Config{})
		require.NoError(t, err)
		t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
		require.NoError(t, db.RecordDrainedFamily(
			t.Context(), nodesettings.FamilyNonces,
		))
		err = ImportLedgerState(t.Context(), ImportConfig{
			Database:  db,
			Reconcile: reconcile,
		})
		require.ErrorIs(t, err, database.ErrLedgerStateDrained)
	}
}
