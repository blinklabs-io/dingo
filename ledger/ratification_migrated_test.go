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

package ledger

import (
	"encoding/json"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/stretchr/testify/require"
)

// A v0.94.2 node can stop between a boundary and the write of its
// ratification decision, which its sync_state pending record says. The
// migration reverts the store below its tip, and so below that boundary,
// before the LedgerDB takes the state over. The ledger then applies the
// boundary again: its decision must be written at that boundary and enacted
// at the next, as on a node that never stopped.
func TestMigratedStoreDeliversRatificationAtItsBoundary(t *testing.T) {
	t.Parallel()

	stale, err := json.Marshal(pendingRatificationRecord{
		Epoch: 742, BoundarySlot: 74_200, ID: 9,
	})
	require.NoError(t, err)
	s := newGovDiffScenarioWithOptions(t, govDiffScenarioOptions{
		beforeMigration: func(db *database.Database) {
			require.NoError(t, db.Metadata().SetSyncState(
				pendingRatificationSyncKey, string(stale), nil,
			))
		},
	})
	left, err := s.db.Metadata().GetSyncState(pendingRatificationSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, string(stale), left,
		"the migration leaves its source as it is")
	rec, err := loadPendingRatification(s.ls.ledgerDB(), nil)
	require.NoError(t, err)
	require.Nil(t, rec,
		"a ledger below the boundary holds the record of its ratification")

	s.run(t, 1, func(ls *LedgerState) {
		require.NoError(t, ls.WaitEpochBoundaryJob(t.Context()))
	})
	update := s.proposalByMarker(t, 0x71)
	require.NotNil(t, update.RatifiedEpoch,
		"the boundary applied again did not ratify")
	require.Equal(t, uint64(742), *update.RatifiedEpoch)
	require.Equal(t, uint64(74_200), *update.RatifiedSlot)
	rec, err = loadPendingRatification(s.ls.ledgerDB(), nil)
	require.NoError(t, err)
	require.Nil(t, rec)

	s.run(t, 1, func(ls *LedgerState) {
		require.NoError(t, ls.WaitEpochBoundaryJob(t.Context()))
	})
	update = s.proposalByMarker(t, 0x71)
	require.NotNil(t, update.EnactedEpoch)
	require.Equal(t, uint64(743), *update.EnactedEpoch)
}
