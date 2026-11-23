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

package storageadopt

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/stretchr/testify/require"
)

// The copy takes the snapshots whose stake inputs are still being written
// from the source's sync state, which holds that record only until the
// reward inputs move into the LedgerDB. A source that has moved them holds no
// record to read, so the copy must refuse it rather than publish every
// snapshot's rows as whole.
func TestMigrationRefusesASourceWithoutItsRewardInputs(t *testing.T) {
	t.Parallel()
	cfg, _ := seedAPIStore(t, "api", 3)
	m := &splitMigration{cfg: cfg, logger: migrateTestLogger()}
	deps, err := m.dependencies("api")
	require.NoError(t, err)
	rt, err := internalplugins.OpenDatabase(
		t.Context(), m.databaseConfig("api", ""), m.selections(), deps,
	)
	require.NoError(t, err)
	// What moving the reward inputs into the LedgerDB leaves: no record, and
	// the mark that they moved.
	require.NoError(t, rt.Database.Metadata().DeleteSyncState(
		models.RewardStakeInputsPendingKey, nil,
	))
	require.NoError(t, rt.Database.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyRewardInputs,
	))
	require.NoError(t, rt.Close(t.Context()))

	err = MigrateToSplit(t.Context(), cfg, migrateTestLogger())
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	require.NoDirExists(t, indexerDirOf(cfg),
		"nothing is published from a source that cannot name its "+
			"incomplete snapshots")
	requireNoMigrationWork(t, cfg)
}
