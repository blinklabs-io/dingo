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

package dbadapter_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// The adapter's reads and writes of the reward outputs, credited rounds and
// their sync keys, and its stake reads that join them, refuse once the family
// is drained, and the credit and stake reads refuse for a drained accounts
// family too. They answer while nothing is drained.
func TestAdapterRewardOutputReadsRefuseOnceTheFamilyIsDrained(t *testing.T) {
	t.Parallel()
	key := make([]byte, 28)
	pool := [][]byte{make([]byte, 28)}
	cursor := "dingo:stake-reward:precompute-cursor:3"
	run := func(t *testing.T, family nodesettings.Family, calls map[string]func(dbadapter.Adapter) error) {
		db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
		require.NoError(t, err)
		adapter := dbadapter.New(db)
		for name, call := range calls {
			require.NotErrorIs(
				t,
				call(adapter),
				database.ErrFamilyDrained,
				name,
			)
		}
		require.NoError(t, db.RecordDrainedFamily(t.Context(), family))
		for name, call := range calls {
			require.ErrorIs(t, call(adapter), database.ErrFamilyDrained, name)
		}
	}
	t.Run("reward-outputs", func(t *testing.T) {
		t.Parallel()
		run(
			t,
			nodesettings.FamilyRewardOutputs,
			map[string]func(dbadapter.Adapter) error{
				"pool outputs": func(a dbadapter.Adapter) error {
					_, err := a.GetRewardPoolOutputs(3, nil)
					return err
				},
				"account outputs": func(a dbadapter.Adapter) error {
					_, err := a.GetRewardAccountOutputs(3, nil)
					return err
				},
				"eligibility outputs": func(a dbadapter.Adapter) error {
					_, err := a.GetRewardAccountOutputsForEligibility(
						[]uint64{3}, 0, key, nil,
					)
					return err
				},
				"save pool outputs": func(a dbadapter.Adapter) error {
					return a.SaveRewardPoolOutputs(nil, nil)
				},
				"save account outputs": func(a dbadapter.Adapter) error {
					return a.SaveRewardAccountOutputs(nil, nil)
				},
				"delete outputs": func(a dbadapter.Adapter) error {
					return a.DeleteRewardOutputsForEpoch(3, nil)
				},
				"rounds": func(a dbadapter.Adapter) error {
					_, err := a.GetPendingRewardCreditRounds(nil)
					return err
				},
				"add round": func(a dbadapter.Adapter) error {
					return a.AddAppliedRewardCreditRound(
						models.RewardCreditRound{},
						nil,
					)
				},
				"claim credential": func(a dbadapter.Adapter) error {
					_, err := a.ClaimPendingRewardCreditsForCredential(
						0,
						key,
						nil,
					)
					return err
				},
				"claim round": func(a dbadapter.Adapter) error {
					_, err := a.ClaimUnfoldedRewardCredits(3, 10, nil)
					return err
				},
				"recheck list": func(a dbadapter.Adapter) error {
					_, err := a.TakeRewardEligibilityRecheck(nil)
					return err
				},
				"cursor read": func(a dbadapter.Adapter) error {
					_, err := a.GetSyncState(cursor, nil)
					return err
				},
				"cursor write": func(a dbadapter.Adapter) error {
					return a.SetSyncState(cursor, "{}", nil)
				},
				"live stake inputs": func(a dbadapter.Adapter) error {
					_, err := a.GetLiveStakeInputsForPools(pool, 0, nil)
					return err
				},
				"post-snapshot credits": func(a dbadapter.Adapter) error {
					_, err := a.GetPostSnapshotRewardCredits(10, nil)
					return err
				},
			},
		)
	})
	t.Run("accounts", func(t *testing.T) {
		t.Parallel()
		run(
			t,
			nodesettings.FamilyAccounts,
			map[string]func(dbadapter.Adapter) error{
				"live stake inputs": func(a dbadapter.Adapter) error {
					_, err := a.GetLiveStakeInputsForPools(pool, 0, nil)
					return err
				},
				"post-snapshot credits": func(a dbadapter.Adapter) error {
					_, err := a.GetPostSnapshotRewardCredits(10, nil)
					return err
				},
				"post-snapshot credit events": func(a dbadapter.Adapter) error {
					_, err := a.GetPostSnapshotRewardCreditEvents(10, nil)
					return err
				},
				"boundary stake by pools": func(a dbadapter.Adapter) error {
					_, _, err := a.GetEpochBoundaryStakeByPools(
						pool,
						10,
						12,
						nil,
						0,
						0,
						nil,
					)
					return err
				},
			},
		)
	})
}
