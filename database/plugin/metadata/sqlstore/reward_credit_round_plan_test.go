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

// TestUnfoldedCreditLookupsUseTheCredentialIndex pins the SQLite plan of the
// per-stake-credential unfolded-credit lookups. On the (spendable, guarded,
// folded, epoch) index they walk every unfolded row once per stake credential,
// which turns the SNAP-point stake read quadratic in delegators.
func TestUnfoldedCreditLookupsUseTheCredentialIndex(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	queries := map[string]string{
		"live stake": `SELECT rls.staking_key, ` +
			store.pendingRewardCreditSubquery(
				"rls.credential_tag", "rls.staking_key",
			) + ` FROM reward_live_stake rls WHERE rls.registered = TRUE`,
		"one stake credential": `SELECT id FROM reward_account_output
WHERE credential_tag = 0 AND staking_key = x'00'
  AND ` + credentialUnfoldedRewardCreditPredicate,
	}
	for name, query := range queries {
		rows, err := store.readDB.Query("EXPLAIN QUERY PLAN " + query)
		require.NoError(t, err, name)
		var plan []string
		for rows.Next() {
			var id, parent, notused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &notused, &detail))
			if strings.Contains(detail, "reward_account_output") ||
				strings.Contains(detail, " prc ") {
				plan = append(plan, detail)
			}
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		joined := strings.Join(plan, "; ")
		require.Contains(t, joined, "idx_reward_account_output_credential",
			"%s: plan %s", name, joined)
		require.NotContains(t, joined, "pending_round",
			"%s: plan %s", name, joined)
	}
}
