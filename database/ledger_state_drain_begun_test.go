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

// A drain marker without its latch bit means some of the family's rows are
// already deleted, so the database must treat the family as drained while the
// latch itself stays unset until the deletes finish.
func TestBegunDrainCountsAsDrainedUntilTheLatchIsSet(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, err := dbtest.NewDatabase(t, &database.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })

	require.NoError(t, db.SetSyncState(
		statedrain.MarkerPrefix+nodesettings.FamilyPoolsHistory.String(),
		"1", nil,
	))
	require.False(t, db.FamilyDrained(nodesettings.FamilyPoolsHistory))

	effective, err := db.EffectiveDrainedFamilies(ctx)
	require.NoError(t, err)
	require.Equal(
		t,
		nodesettings.FamilyMask(0).With(nodesettings.FamilyPoolsHistory),
		effective,
	)
	require.True(t, db.FamilyDrained(nodesettings.FamilyPoolsHistory))
	require.False(t, db.FamilyDrained(nodesettings.FamilyAccounts))

	latch, err := db.DrainedFamilies(ctx)
	require.NoError(t, err)
	require.Zero(t, latch, "the latch waits for the last delete")
	require.True(
		t,
		db.FamilyDrained(nodesettings.FamilyPoolsHistory),
		"reading the latch keeps the begun family drained",
	)
}

func TestBegunDrainRefusesAnUnknownMarker(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	require.NoError(t, db.SetSyncState(
		statedrain.MarkerPrefix+"from-a-newer-release", "1", nil,
	))
	_, err = db.EffectiveDrainedFamilies(t.Context())
	require.ErrorContains(t, err, "from-a-newer-release")
}
