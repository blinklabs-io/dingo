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
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestRewardAccountGuardedQueryUsesIndex(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	rows, err := store.writeDB.Query(`
EXPLAIN QUERY PLAN
SELECT staking_key, pool_key_hash, reward_type, id, epoch, credential_tag,
       amount, spendable, guarded, captured_slot, boundary_slot
FROM reward_account_output
WHERE credential_tag = ? AND staking_key = ?
  AND spendable = TRUE AND guarded = FALSE
ORDER BY epoch ASC, pool_key_hash ASC, reward_type ASC
LIMIT ? OFFSET ?`,
		0,
		bytes.Repeat([]byte{0x11}, 28),
		100,
		0,
	)
	require.NoError(t, err)
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		details = append(details, detail)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, details)
	plan := strings.Join(details, "\n")
	require.Contains(
		t,
		plan,
		"idx_reward_account_output_credential_spendable_guarded",
	)
	require.Contains(t, plan, "guarded=?")
	require.NotContains(t, strings.ToUpper(plan), "SCAN REWARD_ACCOUNT_OUTPUT")
}

