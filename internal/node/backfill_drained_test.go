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
package node

import (
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/stretchr/testify/require"
)

func TestBackfillRunRefusesADrainedDatabase(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, db *database.Database)
	}{
		{
			"latch bit",
			func(t *testing.T, db *database.Database) {
				require.NoError(t, db.RecordDrainedFamily(
					t.Context(), nodesettings.FamilyNonces,
				))
			},
		},
		{
			"drain begun",
			func(t *testing.T, db *database.Database) {
				require.NoError(t, db.SetSyncState(
					statedrain.MarkerPrefix+"network-donations", "1", nil,
				))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			tc.prepare(t, db)
			bf := NewBackfill(db, nil, slog.Default())

			err := bf.Run(t.Context())

			require.ErrorIs(t, err, database.ErrLedgerStateDrained)
		})
	}
}

// A completed backfill writes nothing, so a drained database that finished
// one before the drain still starts.
func TestBackfillRunOnACompletedCheckpointIgnoresTheLatch(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	now := time.Now()
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:     BackfillPhase,
			StartedAt: now,
			UpdatedAt: now,
			Completed: true,
		}, nil,
	))
	require.NoError(t, db.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyNonces,
	))

	require.NoError(t, NewBackfill(db, nil, slog.Default()).Run(t.Context()))
}
