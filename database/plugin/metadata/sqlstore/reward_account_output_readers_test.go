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
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// TestRewardAccountOutputReadersUseIndexes drives every reader and writer of
// reward_account_output that selects rows by predicate and checks the plan of
// each statement it issues: every one must search an index, none may scan the
// table, with and without planner statistics. The folds, claims and pending
// lookups reach the table through the credential index; the epoch reads
// through the unique index.
func TestRewardAccountOutputReadersUseIndexes(t *testing.T) {
	t.Parallel()
	for _, stats := range []struct {
		name    string
		analyze bool
	}{
		{name: "without_planner_stats"},
		{name: "with_planner_stats", analyze: true},
	} {
		t.Run(stats.name, func(t *testing.T) {
			t.Parallel()
			store, recorder := newRecordingSQLiteStore(t)
			require.NoError(t, store.SetPendingRewardCreditRounds(
				[]models.RewardCreditRound{
					{SnapshotEpoch: 1, BoundarySlot: 100},
					{SnapshotEpoch: 2, BoundarySlot: 200},
				}, nil,
			))
			key := bytesRepeat(0x11, 28)
			pool := bytesRepeat(0x31, 28)
			var outputs []*models.RewardAccountOutput
			for epoch := uint64(1); epoch <= 4; epoch++ {
				for i := range 40 {
					staking := bytesRepeat(byte(0x10+i), 28)
					outputs = append(outputs, &models.RewardAccountOutput{
						Epoch: epoch, StakingKey: staking, PoolKeyHash: pool,
						RewardType: "member", Amount: types.Uint64(uint64(i + 1)),
						Spendable: i%5 != 0, Guarded: i%7 == 1,
						CapturedSlot: epoch*100 - 50, BoundarySlot: epoch * 100,
					})
				}
			}
			require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))
			if stats.analyze {
				analyzeStore(t, store)
			}
			selected := map[historicalRewardKey]struct{}{
				{tag: 0, key: string(key)}:                   {},
				{tag: 0, key: string(bytesRepeat(0x12, 28))}: {},
			}
			steps := []struct {
				name string
				run  func() error
			}{
				{"GetRewardAccountOutputs", func() error {
					_, err := store.GetRewardAccountOutputs(2, nil)
					return err
				}},
				{"GetRewardAccountOutputsByCredential", func() error {
					_, err := store.GetRewardAccountOutputsByCredential(
						0, key, 10, 0, "desc", nil)
					return err
				}},
				{"CountRewardAccountOutputsByCredential", func() error {
					_, err := store.CountRewardAccountOutputsByCredential(0, key, nil)
					return err
				}},
				{"GetRewardAccountOutputsInPoolKeyHashRange", func() error {
					_, err := store.GetRewardAccountOutputsInPoolKeyHashRange(
						2, pool, pool, nil)
					return err
				}},
				{"GetRewardAccountOutputsForCredential", func() error {
					_, err := store.GetRewardAccountOutputsForCredential(
						[]uint64{1, 2}, 0, key, nil)
					return err
				}},
				{"GetRewardAccountOutputsForEligibility", func() error {
					_, err := store.GetRewardAccountOutputsForEligibility(
						[]uint64{1, 2}, 0, key, nil)
					return err
				}},
				{"GetPendingRewardAccountOutputsForCredential", func() error {
					_, err := store.GetPendingRewardAccountOutputsForCredential(
						0, key, nil)
					return err
				}},
				{"HasPendingRewardCreditRounds", func() error {
					_, err := store.HasPendingRewardCreditRounds(nil)
					return err
				}},
				{"HasUnfoldedRewardCreditsThroughEpoch", func() error {
					_, err := store.HasUnfoldedRewardCreditsThroughEpoch(2, nil)
					return err
				}},
				{"pendingCreditsForCredentials", func() error {
					_, err := store.pendingCreditsForCredentials(
						context.Background(), store.writeDB, 200, selected)
					return err
				}},
				{"pendingRewardCreditTotals", func() error {
					_, err := store.pendingRewardCreditTotals(
						context.Background(), store.writeDB)
					return err
				}},
				{"pendingRewardCreditSubquery", func() error {
					rows, err := store.writeDB.Query(
						`SELECT ` + store.pendingRewardCreditSubquery(
							"a.credential_tag", "a.staking_key") + `
FROM (SELECT 0 AS credential_tag, x'11' AS staking_key) a`)
					if err != nil {
						return err
					}
					return rows.Close()
				}},
				{"FoldRewardAccountOutputs", func() error {
					return store.FoldRewardAccountOutputs(
						[]uint64{1, 2}, 0, bytesRepeat(0x13, 28), nil)
				}},
				{"FoldPendingRewardAccountOutputs", func() error {
					return store.FoldPendingRewardAccountOutputs(
						0, bytesRepeat(0x14, 28), nil)
				}},
				{"ClaimPendingRewardCreditsForCredential", func() error {
					_, err := store.ClaimPendingRewardCreditsForCredential(
						0, bytesRepeat(0x15, 28), nil)
					return err
				}},
				{"ClaimUnfoldedRewardCredits", func() error {
					_, err := store.ClaimUnfoldedRewardCredits(2, 0, 5, nil)
					return err
				}},
				{"DeleteRewardStateAfterSlot", func() error {
					return store.DeleteRewardStateAfterSlot(350, nil)
				}},
			}
			// The readers that name a credential must reach the table through
			// the credential index, not the (spendable, guarded, folded,
			// epoch) index, whose flag prefix walks every unfolded row.
			credentialSteps := map[string]bool{
				"GetRewardAccountOutputsByCredential":         true,
				"CountRewardAccountOutputsByCredential":       true,
				"GetRewardAccountOutputsForCredential":        true,
				"GetRewardAccountOutputsForEligibility":       true,
				"GetPendingRewardAccountOutputsForCredential": true,
				"pendingCreditsForCredentials":                true,
				"pendingRewardCreditSubquery":                 true,
				"FoldRewardAccountOutputs":                    true,
				"FoldPendingRewardAccountOutputs":             true,
				"ClaimPendingRewardCreditsForCredential":      true,
			}
			tableScan := regexp.MustCompile(
				`(?m)^SCAN (reward_account_output|rao|prc|o)\b`,
			)
			for _, step := range steps {
				mark := len(recorder.snapshot())
				require.NoError(t, step.run(), step.name)
				var checked int
				for _, statement := range recorder.snapshot()[mark:] {
					trimmed := strings.TrimSpace(statement)
					if !strings.Contains(statement, "reward_account_output") ||
						strings.HasPrefix(trimmed, "INSERT") ||
						strings.HasPrefix(trimmed, "EXPLAIN") ||
						strings.HasPrefix(trimmed, "PRAGMA") {
						continue
					}
					plan := queryPlan(t, store.writeDB, statement,
						make([]any, strings.Count(statement, "?"))...)
					checked++
					t.Logf("PLAN %s | %s", step.name, strings.ReplaceAll(plan, "\n", " ; "))
					require.NotRegexp(t, tableScan, plan,
						"%s: statement %s\nplan %s", step.name, statement, plan)
					if credentialSteps[step.name] {
						require.Contains(t, plan,
							"idx_reward_account_output_credential_epoch",
							"%s: statement %s\nplan %s", step.name, statement, plan)
						require.NotContains(t, plan, "pending_round",
							"%s: statement %s\nplan %s", step.name, statement, plan)
					}
					require.Regexp(t, `INDEX|INTEGER PRIMARY KEY`, plan,
						"%s: statement %s\nplan %s", step.name, statement, plan)
				}
				require.Positive(t, checked, step.name)
			}
		})
	}
}
