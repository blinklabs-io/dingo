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
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func BenchmarkRewardLiveStakePlannerStatsFinalizer(b *testing.B) {
	runTxn := func(runBatch func(types.Txn) error) error {
		return runBatch(nil)
	}
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		store := newMigratedSQLiteStore(b)
		_, err := store.writeDB.Exec(
			`INSERT INTO "transaction" (id, slot, block_index) VALUES (1, 1, 0)`,
		)
		require.NoError(b, err)
		_, err = store.writeDB.Exec("ANALYZE \"transaction\"")
		require.NoError(b, err)
		_, err = store.writeDB.Exec(`DELETE FROM "transaction"`)
		require.NoError(b, err)
		seedRewardLiveStakeScaleFixture(b, store, 500, 4, 80, 0)
		for _, table := range []string{
			"account",
			"certs",
			"stake_delegation",
			"stake_registration_delegation",
			"stake_vote_delegation",
			"stake_vote_registration_delegation",
			"utxo",
			"reward_live_stake",
		} {
			_, err := store.writeDB.Exec("ANALYZE " + table)
			require.NoError(b, err)
		}
		_, err = store.writeDB.Exec(`
WITH digits(d) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9))
INSERT INTO "transaction" (id, slot, block_index)
SELECT n + 2001, n + 2001, 0
FROM (
    SELECT d0.d + 10*d1.d + 100*d2.d + 1000*d3.d + 10000*d4.d AS n
    FROM digits d0
    CROSS JOIN digits d1
    CROSS JOIN digits d2
    CROSS JOIN digits d3
    CROSS JOIN digits d4
)
WHERE n < 48000
`)
		require.NoError(b, err)
		var staleStat string
		require.NoError(b, store.writeDB.QueryRow(
			`SELECT stat FROM sqlite_stat1 WHERE tbl = 'transaction' AND idx = 'idx_transaction_hash'`,
		).Scan(&staleStat))
		b.ResetTimer()
		b.StartTimer()
		err = store.RebuildRewardLiveStakeFromRunningTotalsInBatches(1, runTxn)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		var refreshedStat string
		require.NoError(b, store.writeDB.QueryRow(
			`SELECT stat FROM sqlite_stat1 WHERE tbl = 'transaction' AND idx = 'idx_transaction_hash'`,
		).Scan(&refreshedStat))
		b.Logf("transaction_stats_before=%s after=%s", staleStat, refreshedStat)
	}
}
