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

package dingo

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// With the live-stake family drained the startup consistency check neither
// reads reward_live_stake nor rebuilds it, although an active account has no
// row, which is what an undrained database rebuilds.
func TestBackfillRewardLiveStakeLeavesADrainedFamilyAlone(t *testing.T) {
	t.Parallel()
	guard := statedrain.NewTripGuard()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config:         &database.Config{DataDir: t.TempDir()},
		StatementGuard: guard,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })
	seedIncompleteRewardLiveStake(t, db)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	rows := func() int {
		var n int
		require.NoError(t, raw.QueryRow(
			`SELECT COUNT(*) FROM reward_live_stake`,
		).Scan(&n))
		return n
	}
	require.Equal(t, 1, rows())
	require.NoError(t, db.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyLiveStake,
	))
	guard.Arm(
		nodesettings.FamilyLiveStake,
		statedrain.TableNames(nodesettings.FamilyLiveStake),
	)

	n := &Node{
		db: db,
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	n.utxoSetReconciled = true
	require.NoError(t, n.backfillRewardLiveStake(context.Background()))

	require.Empty(t, guard.Trips())
	require.Equal(t, 1, rows(), "a drained family was rebuilt")
}
