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

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestRewardLiveStakeRefreshesTransactionPlannerStats(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	key := make([]byte, 28)
	key[0] = 1
	pool := make([]byte, 28)
	pool[0] = 2
	_, err := store.writeDB.Exec(
		`INSERT INTO account (staking_key, credential_tag, pool, reward, active, added_slot, created_slot)
VALUES (?, 0, ?, '0', TRUE, 1, 1)`,
		key,
		pool,
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		`INSERT INTO "transaction" (id, slot, block_index) VALUES (1, 1, 0)`,
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		`INSERT INTO certs (id, transaction_id, slot, cert_index) VALUES (1, 1, 1, 0)`,
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		`INSERT INTO stake_delegation (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
VALUES (?, 0, ?, 1, 1)`,
		key,
		pool,
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec("ANALYZE")
	require.NoError(t, err)

	_, err = store.writeDB.Exec(`
WITH digits(d) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9))
INSERT INTO "transaction" (id, slot, block_index)
SELECT n + 2, n + 2, 0
FROM (
    SELECT d0.d + 10*d1.d + 100*d2.d + 1000*d3.d + 10000*d4.d AS n
    FROM digits d0
    CROSS JOIN digits d1
    CROSS JOIN digits d2
    CROSS JOIN digits d3
    CROSS JOIN digits d4
)
`)
	require.NoError(t, err)

	stat := func() string {
		var value string
		require.NoError(t, store.writeDB.QueryRow(
			`SELECT stat FROM sqlite_stat1 WHERE tbl = 'transaction' AND idx = 'idx_transaction_hash'`,
		).Scan(&value))
		return value
	}
	staleStat := stat()
	require.Equal(t, "1 1", staleStat)
	plan := func() string {
		query, args := rewardLiveStakeCredentialQuery(true, stakeKeyRange{
			hi: &stakeKeyBound{tag: 0, key: key},
		})
		query, err = sqliteRewardLiveStakeAccountFirstQuery(query)
		require.NoError(t, err)
		rows, err := store.writeDB.Query("EXPLAIN QUERY PLAN "+query, args...)
		require.NoError(t, err)
		defer rows.Close()
		var details []string
		for rows.Next() {
			var id, parent, notUsed int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
			details = append(details, detail)
		}
		require.NoError(t, rows.Err())
		return strings.Join(details, "\n")
	}
	require.Equal(t, 4, strings.Count(plan(), "SCAN tx"))

	runTxn := func(runBatch func(types.Txn) error) error {
		return runBatch(nil)
	}
	require.NoError(t, store.RebuildRewardLiveStakeFromRunningTotalsInBatches(1, runTxn))
	require.NotEqual(t, staleStat, stat())
	require.Equal(t, 4, strings.Count(plan(), "SEARCH tx USING INTEGER PRIMARY KEY"))
}
