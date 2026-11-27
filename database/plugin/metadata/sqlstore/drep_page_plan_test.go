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

package sqlstore

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// The DRep list resolves a registration epoch for every DRep before it pages,
// so each lookup must be one seek on idx_epoch_start_slot rather than a walk
// of the epoch table, with and without planner statistics.
func TestDrepPageResolvesRegistrationEpochBySeek(t *testing.T) {
	skipPlanTestUnderRace(t)
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	tx, err := store.writeDB.Begin()
	require.NoError(t, err)
	for epoch := range 500 {
		_, err := tx.Exec(`INSERT INTO epoch (epoch_id, start_slot, era_id,
 slot_length, length_in_slots) VALUES (?, ?, 6, 1000, 100)`,
			epoch, epoch*100)
		require.NoError(t, err)
	}
	for id := 1; id <= 300; id++ {
		credential := make([]byte, 28)
		binary.BigEndian.PutUint64(credential[20:], uint64(id))
		_, err := tx.Exec(`INSERT INTO drep (credential, credential_tag,
 added_slot, active) VALUES (?, 0, ?, TRUE)`, credential, id*150)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	query := drepPageCTEs + `SELECT registration_epoch FROM classified`
	args := []any{int64(500), true, int64(20), int64(500)}
	for _, stats := range []bool{false, true} {
		if stats {
			_, err := store.writeDB.Exec("ANALYZE")
			require.NoError(t, err)
		}
		plan := queryPlan(t, store.writeDB, query, args...)
		require.Contains(t, plan,
			"SEARCH epoch USING INDEX idx_epoch_start_slot (start_slot<?)",
			"stats %t:\n%s", stats, plan)
		require.NotContains(t, plan, "idx_epoch_epoch_id",
			"stats %t:\n%s", stats, plan)
	}

	var epoch int64
	require.NoError(t, store.writeDB.QueryRow(
		query+` WHERE id = 7`, args...,
	).Scan(&epoch))
	// Added at slot 1050, inside the epoch starting at slot 1000.
	require.Equal(t, int64(10), epoch)
}
