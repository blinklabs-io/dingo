// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package koiosparity

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// DingoDB queries the SQL tables itself, so it refuses a database that has
// deleted the rows of a family it reads, and opens one whose drained
// families it does not read.
func TestOpenDingoDBRefusesADatabaseDrainedOfAFamilyItReads(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		family  nodesettings.Family
		refused bool
	}{
		{"epochs", nodesettings.FamilyEpochs, true},
		{"reward inputs", nodesettings.FamilyRewardInputs, true},
		{"reward outputs", nodesettings.FamilyRewardOutputs, true},
		{"nonces", nodesettings.FamilyNonces, false},
		{"network donations", nodesettings.FamilyNetworkDonations, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			db, err := dbtest.NewDatabase(t, &database.Config{DataDir: dir})
			require.NoError(t, err)
			require.NoError(t, db.RecordDrainedFamily(t.Context(), tc.family))
			require.NoError(t, dbtest.CloseDatabase(db))

			dingo, err := OpenDingoDB(
				DingoDBConfig{Plugin: "sqlite", DataDir: dir},
			)

			if tc.refused {
				require.ErrorIs(t, err, ErrDrainedDatabase)
				return
			}
			require.NoError(t, err)
			require.NoError(t, dingo.Close())
		})
	}
}
