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

package migrations

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestRewardCreditRoundBackfillMovesLegacyState(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := SQLiteRegistry()
	require.NoError(t, err)
	locker := NewFileLocker(databasePath + ".migrate.lock")
	legacyRunner := Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry[:30],
		Locker:   locker,
	}
	require.NoError(t, legacyRunner.Run(context.Background()))

	want := make([]models.RewardCreditRound, 1_005)
	for i := range want {
		want[i] = models.RewardCreditRound{
			SnapshotEpoch: uint64(i),
			BoundarySlot:  uint64(i*10 + 5),
		}
	}
	raw, err := json.Marshal(want)
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO sync_state (sync_key, value) VALUES (?, ?)`,
		models.PendingRewardCreditRoundsKey,
		string(raw),
	)
	require.NoError(t, err)

	runner := Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   locker,
	}
	require.NoError(t, runner.Run(context.Background()))

	rows, err := db.Query(
		`SELECT snapshot_epoch, boundary_slot FROM reward_credit_round ORDER BY snapshot_epoch`,
	)
	require.NoError(t, err)
	defer rows.Close()
	var got []models.RewardCreditRound
	for rows.Next() {
		var epoch, slot int64
		require.NoError(t, rows.Scan(&epoch, &slot))
		got = append(got, models.RewardCreditRound{
			SnapshotEpoch: uint64(epoch),
			BoundarySlot:  uint64(slot),
		})
	}
	require.NoError(t, rows.Err())
	require.Equal(t, want, got)
	var legacyCount int
	err = db.QueryRow(
		`SELECT COUNT(*) FROM sync_state WHERE sync_key = ?`,
		models.PendingRewardCreditRoundsKey,
	).Scan(&legacyCount)
	require.NoError(t, err)
	require.Zero(t, legacyCount, "the legacy state key should be consumed")
}
