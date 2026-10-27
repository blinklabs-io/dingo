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
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// importedUtxoRows is the size of the never-spent UTxO set analyzed before
// any spend exists, the shape a ledger-state import leaves behind.
const importedUtxoRows = 20000

// newImportStatsStore returns a store whose utxo statistics were taken over
// a never-spent UTxO set and are now stale: every spent_at_tx_id was NULL at
// ANALYZE, so idx_utxo_spent_at_tx_id is recorded as a single value covering
// the whole table, and the spends added afterwards do not grow the table
// enough for the incremental refresh to revisit it. The fixture writes that
// statistic itself rather than relying on ANALYZE to produce it, so the test
// keeps exercising the worst case if the index stops analyzing that way.
func newImportStatsStore(t *testing.T, spenders int) (*Store, [][]byte) {
	t.Helper()
	store := newFilePlannerStatsStore(t, prometheus.NewRegistry())
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := store.writeDB.Exec(query, args...)
		require.NoError(t, err, query)
	}
	exec(fmt.Sprintf(`
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < %d
)
INSERT INTO utxo
    (tx_id, output_idx, credential_tag, staking_key,
     added_slot, deleted_slot, amount)
SELECT randomblob(32), 0, i %% 2, randomblob(28), 1, 0, '1' FROM n`,
		importedUtxoRows))
	exec("ANALYZE")
	exec(
		`UPDATE sqlite_stat1 SET stat = ? WHERE idx = 'idx_utxo_spent_at_tx_id'`,
		fmt.Sprintf("%d %d", importedUtxoRows, importedUtxoRows),
	)

	hashes := make([][]byte, 0, spenders)
	for i := range spenders {
		hash := []byte(fmt.Sprintf("%032d", i))
		hashes = append(hashes, hash)
		exec(
			`INSERT INTO "transaction" (hash, slot, block_index) VALUES (?, 10, 0)`,
			hash,
		)
		exec(
			`UPDATE utxo SET spent_at_tx_id = ?, deleted_slot = 10 WHERE id = ?`,
			hash,
			i+1,
		)
	}
	require.NoError(t, store.refreshReadPool(context.Background()))

	var stat string
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT stat FROM sqlite_stat1 WHERE idx = 'idx_utxo_spent_at_tx_id'`,
	).Scan(&stat))
	want := fmt.Sprintf("%d %d", importedUtxoRows, importedUtxoRows)
	require.Equal(
		t,
		want,
		stat,
		"idx_utxo_spent_at_tx_id statistics must describe the never-spent import",
	)
	return store, hashes
}

func explainQueryPlan(
	t *testing.T,
	store *Store,
	query string,
	args ...any,
) string {
	t.Helper()
	rows, err := store.readDB.QueryContext(
		context.Background(), "EXPLAIN QUERY PLAN "+query, args...,
	)
	require.NoError(t, err)
	var details []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
		details = append(details, detail)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	return strings.Join(details, "\n")
}

// A rollback whose chunk holds a single transaction hash planned this lookup
// as a pass over every utxo row: with spent_at_tx_id recorded as one value,
// SQLite walked idx_utxo_staking_deleted_amount in credential order to satisfy
// the DISTINCT, testing each row's spent_at_tx_id. On a mainnet-sized table
// that held the ledger write lock for tens of seconds per rollback. The lookup
// must search the spender index whatever the statistics say.
func TestRollbackSpentStakeRefsQuerySearchesSpenderIndex(t *testing.T) {
	t.Parallel()
	store, hashes := newImportStatsStore(t, 3)
	ctx := context.Background()
	for _, n := range []int{1, 3} {
		query, err := store.rollbackSpentStakeRefsQuery(ctx, store.readDB, n)
		require.NoError(t, err)
		args := make([]any, n)
		for i := range n {
			args[i] = hashes[i]
		}
		plan := explainQueryPlan(t, store, query, args...)
		require.NotContains(t, plan, "SCAN utxo",
			"%d hash(es) must not scan utxo, plan:\n%s", n, plan)
		require.Contains(t, plan, "idx_utxo_spent_at_tx_id",
			"%d hash(es) must search the spender index, plan:\n%s", n, plan)
	}
}

// legacyDistinctSpentStakeRefsQuery is the statement the rollback stake
// lookup used before it named the spender index and moved DISTINCT into Go.
func legacyDistinctSpentStakeRefsQuery(n int) string {
	return `
SELECT DISTINCT credential_tag, staking_key FROM utxo
WHERE spent_at_tx_id IN (` + bindPlaceholders(n) + `)`
}

// The rewritten lookup, deduplicated in Go, must return exactly the
// credential set the DISTINCT statement returned, including repeated
// credentials and a staking key shared by both credential tags.
func TestRollbackSpentStakeRefsMatchLegacyDistinct(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	keyA := make([]byte, 28)
	keyA[0] = 0xAA
	keyB := make([]byte, 28)
	keyB[0] = 0xBB
	spender1 := []byte(fmt.Sprintf("%032d", 1))
	spender2 := []byte(fmt.Sprintf("%032d", 2))
	other := []byte(fmt.Sprintf("%032d", 3))
	for _, hash := range [][]byte{spender1, spender2, other} {
		_, err := store.writeDB.Exec(
			`INSERT INTO "transaction" (hash, slot, block_index) VALUES (?, 10, 0)`,
			hash,
		)
		require.NoError(t, err)
	}
	rows := []struct {
		spender []byte
		tag     int64
		key     []byte
	}{
		{spender1, 0, keyA},
		{spender1, 0, keyA}, // repeat within one spender
		{spender2, 0, keyA}, // repeat across spenders
		{spender2, 1, keyA}, // same key, other credential tag
		{spender2, 0, keyB},
		{other, 1, keyB}, // spent by a transaction not rolled back
		{nil, 0, keyB},   // unspent
	}
	for i, row := range rows {
		_, err := store.writeDB.Exec(
			`INSERT INTO utxo
    (tx_id, output_idx, credential_tag, staking_key, spent_at_tx_id,
     added_slot, deleted_slot, amount)
VALUES (?, 0, ?, ?, ?, 1, 0, '1')`,
			[]byte{byte(i)}, row.tag, row.key, row.spender,
		)
		require.NoError(t, err)
	}

	ctx := context.Background()
	args := []any{spender1, spender2}
	query, err := store.rollbackSpentStakeRefsQuery(
		ctx,
		store.writeDB,
		len(args),
	)
	require.NoError(t, err)
	got, err := queryStakeRefs(ctx, store.writeDB, query, args...)
	require.NoError(t, err)
	want, err := queryStakeRefs(
		ctx,
		store.writeDB,
		legacyDistinctSpentStakeRefsQuery(len(args)),
		args...,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, want, mergeStakeCredentialRefs(got))
	require.Len(t, want, 3)
}

// The spender index is deferred, so it is absent while a bulk-load rebuild
// is pending. The lookup must still run then rather than fail the rollback
// on an INDEXED BY naming a missing index.
func TestRollbackSpentStakeRefsQueryWithoutSpenderIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	_, err := store.writeDB.Exec(
		"DROP INDEX " + rollbackSpentStakeRefsIndex.Name,
	)
	require.NoError(t, err)

	ctx := context.Background()
	query, err := store.rollbackSpentStakeRefsQuery(ctx, store.writeDB, 1)
	require.NoError(t, err)
	require.NotContains(t, query, "INDEXED BY")
	_, err = queryStakeRefs(ctx, store.writeDB, query, []byte("missing"))
	require.NoError(t, err)
}

// The rollback sweep refreshes each affected credential's live stake with a
// full scan of its UTxOs while the ledger write lock is held. The stake lookup
// returns one row per spent UTxO, so the sweep must deduplicate before the
// refresh: a credential with several UTxOs spent by rolled-back transactions
// is scanned once.
func TestDeleteTransactionsAfterSlotRefreshesEachCredentialOnce(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	keyA := make([]byte, 28)
	keyA[0] = 0xAA
	keyB := make([]byte, 28)
	keyB[0] = 0xBB
	spender := []byte(fmt.Sprintf("%032d", 1))
	_, err := store.writeDB.Exec(
		`INSERT INTO "transaction" (hash, slot, block_index) VALUES (?, 10, 0)`,
		spender,
	)
	require.NoError(t, err)
	for i, key := range [][]byte{keyA, keyA, keyB} {
		_, err := store.writeDB.Exec(
			`INSERT INTO utxo
    (tx_id, output_idx, credential_tag, staking_key, spent_at_tx_id,
     added_slot, deleted_slot, amount)
VALUES (?, 0, 0, ?, ?, 1, 10, '1')`,
			[]byte{byte(i)}, key, spender,
		)
		require.NoError(t, err)
	}

	before := store.sumCredentialUtxoStakeCalls.Load()
	require.NoError(t, store.DeleteTransactionsAfterSlot(5, nil))
	require.Equal(
		t,
		before+2,
		store.sumCredentialUtxoStakeCalls.Load(),
		"two distinct credentials must be refreshed once each",
	)
}
