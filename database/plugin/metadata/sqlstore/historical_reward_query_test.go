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
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

const legacyRewardEffectiveSlot = `COALESCE((SELECT lc.slot FROM leios_transaction_context lc JOIN "transaction" tx ON tx.id = lc.transaction_id WHERE tx.hash = account_reward_delta.tx_hash), added_slot)`

// legacyRewardWithdrawalQuery and legacyRewardCreditQuery are the queries
// historicalRewardsBatch ran before the rewrite; they define the row set and
// order the new queries must reproduce.
func legacyRewardWithdrawalQuery(op, predicate string) string {
	return `
SELECT credential_tag, staking_key, id,
       ` + legacyRewardEffectiveSlot + `, previous_reward
FROM account_reward_delta
WHERE withdrawal = TRUE AND ` + legacyRewardEffectiveSlot + ` ` + op + ` ? AND (` + predicate + `)
	ORDER BY credential_tag, staking_key, added_slot, id`
}

func legacyRewardCreditQuery(includePostSnapshot bool, predicate string) string {
	slotPredicate := legacyRewardEffectiveSlot + ` > ?`
	if includePostSnapshot {
		slotPredicate = `(` + legacyRewardEffectiveSlot + ` > ? OR (` + legacyRewardEffectiveSlot + ` = ? AND post_snapshot = TRUE))`
	}
	return `
SELECT credential_tag, staking_key, id, ` + legacyRewardEffectiveSlot + `, amount
FROM account_reward_delta
WHERE withdrawal = FALSE AND ` + slotPredicate + ` AND (` + predicate + `)
	ORDER BY credential_tag, staking_key, added_slot, id`
}

func legacyRewardCredentialPredicate(
	keys []historicalRewardKey,
) (string, []any) {
	parts := make([]string, 0, len(keys))
	args := make([]any, 0, len(keys)*2)
	for _, key := range keys {
		parts = append(parts, "(credential_tag = ? AND staking_key = ?)")
		args = append(args, key.tag, []byte(key.key))
	}
	return strings.Join(parts, " OR "), args
}

func newHistoricalRewardQueryStore(t *testing.T) *Store {
	t.Helper()
	db, err := OpenDB(
		"sqlite",
		filepath.Join(t.TempDir(), "metadata.sqlite"),
		"sqlite",
		false,
	)
	require.NoError(t, err)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

const historicalRewardQueryBoundary = 1000

// seedHistoricalRewardDeltas writes deltas for five stake keys across both
// credential tags. For every key it covers withdrawal and credit rows,
// post_snapshot set and clear, added slots below, at and above the boundary,
// and every Leios shape: no transaction row, a transaction without a context,
// and a context slot below, at and above the boundary. It returns the seeded
// keys, sorted by tag then key, plus one key with no rows.
func seedHistoricalRewardDeltas(
	t *testing.T,
	store *Store,
) []historicalRewardKey {
	t.Helper()
	keys := []historicalRewardKey{
		{tag: 0, key: string([]byte{0x01})},
		{tag: 0, key: string([]byte{0x02})},
		{tag: 0, key: string([]byte{0x03})},
		{tag: 1, key: string([]byte{0x01})},
		{tag: 1, key: string([]byte{0x04})},
	}
	contextSlots := []any{nil, nil, 900, 1000, 1100}
	var deltaID, txID int
	for _, key := range keys {
		for _, added := range []int{900, 1000, 1100} {
			for _, postSnapshot := range []bool{false, true} {
				for _, withdrawal := range []bool{false, true} {
					for variant, contextSlot := range contextSlots {
						deltaID++
						hash := []byte(fmt.Sprintf("tx-%04d", deltaID))
						if variant > 0 {
							txID++
							_, err := store.writeDB.Exec(
								`INSERT INTO "transaction" (id, hash, slot, block_index) VALUES (?, ?, ?, 0)`,
								txID, hash, added,
							)
							require.NoError(t, err)
							if contextSlot != nil {
								_, err = store.writeDB.Exec(
									`INSERT INTO leios_transaction_context (transaction_id, slot) VALUES (?, ?)`,
									txID, contextSlot,
								)
								require.NoError(t, err)
							}
						}
						_, err := store.writeDB.Exec(
							`INSERT INTO account_reward_delta (id, staking_key, credential_tag, tx_hash, amount, previous_reward, added_slot, withdrawal, post_snapshot) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
							deltaID, []byte(key.key), key.tag, hash,
							fmt.Sprint(deltaID), fmt.Sprint(deltaID*10),
							added, withdrawal, postSnapshot,
						)
						require.NoError(t, err)
					}
				}
			}
		}
	}
	return keys
}

type rewardQueryRow struct {
	tag    uint8
	key    string
	id     int64
	slot   int64
	amount sql.NullString
}

func runRewardQuery(
	t *testing.T,
	store *Store,
	query string,
	args []any,
) []rewardQueryRow {
	t.Helper()
	rows, err := store.writeDB.QueryContext(context.Background(), query, args...)
	require.NoError(t, err)
	defer rows.Close()
	var out []rewardQueryRow
	for rows.Next() {
		var row rewardQueryRow
		var key []byte
		require.NoError(t, rows.Scan(
			&row.tag, &key, &row.id, &row.slot, &row.amount,
		))
		row.key = string(key)
		out = append(out, row)
	}
	require.NoError(t, rows.Err())
	return out
}

func selectedHistoricalRewardKeys(
	keys []historicalRewardKey,
) map[historicalRewardKey]struct{} {
	selected := make(map[historicalRewardKey]struct{}, len(keys))
	for _, key := range keys {
		selected[key] = struct{}{}
	}
	return selected
}

func TestHistoricalRewardQueriesMatchLegacyRows(t *testing.T) {
	t.Parallel()
	store := newHistoricalRewardQueryStore(t)
	keys := seedHistoricalRewardDeltas(t, store)
	unknown := historicalRewardKey{tag: 1, key: string([]byte{0x99})}

	keySets := map[string][]historicalRewardKey{
		"all":         keys,
		"tag0 only":   keys[:3],
		"tag1 only":   keys[3:],
		"single":      keys[1:2],
		"with absent": append(append([]historicalRewardKey{}, keys...), unknown),
	}
	for name, set := range keySets {
		sort.Slice(set, func(i, j int) bool {
			if set[i].tag != set[j].tag {
				return set[i].tag < set[j].tag
			}
			return set[i].key < set[j].key
		})
		t.Run(name, func(t *testing.T) {
			selected := selectedHistoricalRewardKeys(set)
			legacyPredicate, legacyArgs := legacyRewardCredentialPredicate(set)
			newPredicate, newArgs := historicalRewardCredentialPredicateFor(
				selected, "d",
			)
			slot := int64(historicalRewardQueryBoundary)

			for _, op := range []string{">", ">="} {
				want := runRewardQuery(t, store,
					legacyRewardWithdrawalQuery(op, legacyPredicate),
					append([]any{slot}, legacyArgs...))
				got := runRewardQuery(t, store,
					historicalRewardWithdrawalQuery(op, newPredicate),
					append([]any{slot}, newArgs...))
				require.NotEmpty(t, want)
				require.Equal(t, want, got, "withdrawal op %s", op)
			}
			for _, postSnapshot := range []bool{false, true} {
				creditSlots := []any{slot}
				if postSnapshot {
					creditSlots = []any{slot, slot}
				}
				want := runRewardQuery(t, store,
					legacyRewardCreditQuery(postSnapshot, legacyPredicate),
					append(append([]any{}, creditSlots...), legacyArgs...))
				got := runRewardQuery(t, store,
					historicalRewardCreditQuery(postSnapshot, newPredicate),
					append(append([]any{}, creditSlots...), newArgs...))
				require.NotEmpty(t, want)
				require.Equal(t, want, got, "credit postSnapshot %v", postSnapshot)
			}
		})
	}
}

func TestHistoricalRewardCreditQueryUsesCredentialIndexWithoutStats(t *testing.T) {
	t.Parallel()
	store := newHistoricalRewardQueryStore(t)
	keys := seedHistoricalRewardDeltas(t, store)

	var statTables int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE name = 'sqlite_stat1'`,
	).Scan(&statTables))
	require.Zero(t, statTables, "plan must not depend on sqlite_stat1")

	predicate, predicateArgs := historicalRewardCredentialPredicateFor(
		selectedHistoricalRewardKeys(keys), "d",
	)
	for _, postSnapshot := range []bool{false, true} {
		args := []any{int64(historicalRewardQueryBoundary)}
		if postSnapshot {
			args = append(args, int64(historicalRewardQueryBoundary))
		}
		args = append(args, predicateArgs...)
		plan := explainRewardQuery(
			t, store, historicalRewardCreditQuery(postSnapshot, predicate), args,
		)
		require.Contains(
			t, plan, "idx_account_reward_delta_credential",
			"postSnapshot %v plan:\n%s", postSnapshot, plan,
		)
		require.NotContains(
			t, plan, "idx_account_reward_delta_withdrawal",
			"postSnapshot %v plan:\n%s", postSnapshot, plan,
		)
	}
}

func explainRewardQuery(
	t *testing.T,
	store *Store,
	query string,
	args []any,
) string {
	t.Helper()
	rows, err := store.writeDB.QueryContext(
		context.Background(), "EXPLAIN QUERY PLAN "+query, args...,
	)
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
