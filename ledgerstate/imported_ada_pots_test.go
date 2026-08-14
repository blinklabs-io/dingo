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

	const (
		epoch    = uint64(1385)
		treasury = uint64(87_920_693_660_807)
		reserves = uint64(14_914_270_613_432_674)
		fees     = uint64(2_589_957_376)
		slot     = uint64(119_750_400)
	)
	require.NoError(t, importTip(context.Background(), ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		State: &RawLedgerState{
			Epoch:    epoch,
			Treasury: treasury,
			Reserves: reserves,
			Fees:     fees,
			EraIndex: 6,
			Tip: &SnapshotTip{
				Slot:      slot,
				BlockHash: make([]byte, 32),
			},
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}))

	pots, err := db.Metadata().GetRewardAdaPots(epoch, nil)
	require.NoError(t, err)
	require.NotNil(t, pots,
		"the imported epoch needs an ADA pots row, or the first reward "+
			"round after bootstrap is skipped and never made up")

	require.Equal(t, epoch, pots.Epoch)
	require.Equal(t, treasury, uint64(pots.Treasury))
	require.Equal(t, reserves, uint64(pots.Reserves))
	// Fees matter as much as the other two: the reward pot is incentives
	// plus fees, so seeding a zero fee pot would compute and credit the
	// round at the wrong amount rather than visibly not running it — a
	// silently wrong reward is worse than an absent one.
	require.Equal(t, fees, uint64(pots.Fees),
		"the fee pot is an addend of the reward pot and must be carried "+
			"through from the snapshot")
	require.Equal(t, slot, pots.CapturedSlot)
}
