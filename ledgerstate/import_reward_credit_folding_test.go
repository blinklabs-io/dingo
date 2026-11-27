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
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// TestImportFoldsCoveredRewardCredits requires a catch-up to fold the
// unclaimed credits of every reward round at or below the snapshot's tip:
// the snapshot's balances already include them. A round past the tip keeps
// its credits, and no balance changes.
func TestImportFoldsCoveredRewardCredits(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	cfg := importTargetForTest(t, ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ldb := cfg.LedgerDB
	key := hash28(0x31)
	other := hash28(0x33)
	for _, k := range [][]byte{key, other} {
		require.NoError(t, ldb.ImportAccount(
			&models.Account{StakingKey: k, Reward: 20, Active: true}, nil,
		))
	}
	for _, r := range []models.RewardCreditRound{
		{SnapshotEpoch: 1, BoundarySlot: 100},
		{SnapshotEpoch: 2, BoundarySlot: 200},
		{SnapshotEpoch: 3, BoundarySlot: 300},
	} {
		require.NoError(t, ldb.AddAppliedRewardCreditRound(r, nil))
	}
	output := func(epoch uint64, k []byte, slot uint64) *models.RewardAccountOutput {
		return &models.RewardAccountOutput{
			Epoch:        epoch,
			StakingKey:   k,
			PoolKeyHash:  hash28(0x51),
			RewardType:   "member",
			Amount:       10,
			Spendable:    true,
			CapturedSlot: slot,
			BoundarySlot: slot,
		}
	}
	require.NoError(t, ldb.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		output(1, key, 100), output(2, key, 200), output(3, key, 300),
		output(1, other, 100),
	}, nil))

	require.NoError(t, importAccounts(context.Background(), cfg, []ParsedAccount{{
		StakingKey: Credential{Type: CredentialTypeKey, Hash: key},
		Reward:     75,
		Active:     true,
	}}, 250))
	require.NoError(t, foldImportedRewardCredits(context.Background(), cfg, 250))

	account, err := ldb.GetAccountByCredential(t.Context(), 0, key, true, nil)
	require.NoError(t, err)
	require.Equal(t, types.Uint64(75), account.Reward)
	pending, err := ldb.GetPendingRewardAccountOutputsForCredential(0, key, nil)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, uint64(3), pending[0].Epoch)

	account, err = ldb.GetAccountByCredential(t.Context(), 0, other, true, nil)
	require.NoError(t, err)
	require.Equal(t, types.Uint64(20), account.Reward)
	pending, err = ldb.GetPendingRewardAccountOutputsForCredential(0, other, nil)
	require.NoError(t, err)
	require.Empty(t, pending)
}
