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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRewardOutputClaimStatementPlans pins the SQLite plans of the statements
// that select a round's or a credential's unfolded reward outputs, against the
// fully migrated schema. A statement naming an index a later migration drops
// fails to prepare, and a flag test written as an equality lets the planner
// walk the pending-round index, which has no credential column, once per
// account or credential.
func TestRewardOutputClaimStatementPlans(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	const pendingRound = `idx_reward_account_output_pending_round`
	credentialProbe := func(alias string) string {
		return `SEARCH ` + alias + ` USING INDEX ` +
			`idx_reward_account_output_credential_epoch ` +
			`(credential_tag=? AND staking_key=?`
	}
	tests := []struct {
		name     string
		query    string
		contains []string
		excludes []string
	}{
		{
			name:  "account-ordered fold",
			query: unfoldedRewardOutputsSQLite,
			contains: []string{
				`SEARCH a USING INTEGER PRIMARY KEY (rowid>?)`,
				credentialProbe("o") + ` AND epoch=?)`,
			},
			excludes: []string{pendingRound, `B-TREE FOR ORDER BY`},
		},
		{
			name:     "credential claim",
			query:    pendingRewardOutputsForCredentialSQL,
			contains: []string{credentialProbe("rao")},
			excludes: []string{pendingRound},
		},
		{
			name:  "unaccounted outputs",
			query: unaccountedRewardOutputsSQL,
			contains: []string{
				`SEARCH o USING INDEX ` + pendingRound +
					` (spendable=? AND guarded=? AND folded=? AND epoch=?)`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := store.writeDB.Prepare(test.query)
			require.NoError(t, err, "statement must prepare on the migrated schema")
			require.NoError(t, stmt.Close())
			args := make([]any, strings.Count(test.query, "?"))
			for i := range args {
				args[i] = 1
			}
			plan := queryPlan(t, store.writeDB, test.query, args...)
			for _, want := range test.contains {
				require.Contains(t, plan, want)
			}
			for _, unwanted := range test.excludes {
				require.NotContains(t, plan, unwanted)
			}
		})
	}
}
