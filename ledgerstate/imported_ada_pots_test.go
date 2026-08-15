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
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// Applying a reward round at the boundary into N reads this node's
// RewardAdaPots row for N-1. A node bootstrapped from a snapshot never saw
// that boundary, so without seeding, the first round after import finds no
// pots and is skipped — and a skipped round is never made up. Reward
// balances, and the leadership stake derived from them, stay short by an
// epoch's rewards for the life of the database, which is what makes such a
// node reject canonical blocks near the eligibility threshold (#3165).
func TestImportSeedsAdaPotsForTheImportedEpoch(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	require.NotNil(t, state.Tip)

	cfg := ImportConfig{
		Database: db,
		State:    state,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}
	noProgress := func(ImportProgress) {}
	ctx := context.Background()
	_, err = importCertState(ctx, cfg, state.Tip.Slot, noProgress)
	require.NoError(t, err)
	require.NoError(t, importSnapShots(
		ctx, cfg, state.Tip.Slot, noProgress, false,
	))

	pots, err := db.Metadata().GetRewardAdaPots(state.Epoch, nil)
	require.NoError(t, err)
	require.NotNil(t, pots,
		"the imported epoch needs an ADA pots row, or the first reward "+
			"round after bootstrap is skipped and never made up")
	require.Equal(t, state.Epoch, pots.Epoch)
	require.Equal(t, state.Treasury, uint64(pots.Treasury))
	require.Equal(t, state.Reserves, uint64(pots.Reserves))

	// The fee pot must be the one SnapShots captured at the boundary, not
	// UTxOState's running total for the current epoch: the reward pot is
	// incentives plus fees, and the row is read as the pots for a boundary.
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err)
	require.Equal(t, snapshots.Fee, uint64(pots.Fees),
		"the seeded fee pot must come from SnapShots' ssFee")
}
