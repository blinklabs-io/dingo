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
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func overlayTxID(kind byte, i int) []byte {
	id := make([]byte, 32)
	id[0] = kind
	id[1] = byte(i >> 8)
	id[2] = byte(i)
	return id
}

func overlayTxInput(
	t testing.TB,
	txID []byte,
	idx uint32,
) lcommon.TransactionInput {
	t.Helper()
	input, err := mockledger.NewTransactionInputBuilder().
		WithTxId(txID).WithIndex(idx).Build()
	require.NoError(t, err)
	return input
}

// overlayStoreState dumps the tables the overlay writes, ids included, and
// the utxo id sequence. Holdings are dumped by asset identity bytes because
// identity ids are node-local and the per-row path may create identities the
// overlay does not.
func overlayStoreState(t testing.TB, store *Store) string {
	t.Helper()
	var out strings.Builder
	for _, query := range []string{
		`SELECT * FROM utxo`,
		`SELECT u.tx_id, u.output_idx, i.policy_id, i.name, h.amount
		 FROM asset_holding h
		 JOIN asset_identity i ON i.id = h.asset_id
		 JOIN utxo u ON u.id = h.utxo_id`,
		`SELECT * FROM utxo_address`,
		`SELECT * FROM utxo_pointer`,
		`SELECT credential_tag, staking_key, utxo_stake, reward_stake,
		        total_stake, updated_slot FROM reward_live_stake`,
		`SELECT * FROM sqlite_sequence WHERE name = 'utxo'`,
	} {
		rows, err := store.writeDB.QueryContext(context.Background(), query)
		require.NoError(t, err)
		cols, err := rows.Columns()
		require.NoError(t, err)
		var lines []string
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			parts := make([]string, len(cols))
			for i, col := range cols {
				if b, ok := values[i].([]byte); ok {
					parts[i] = col + "=" + hex.EncodeToString(b)
				} else {
					parts[i] = fmt.Sprintf("%s=%v", col, values[i])
				}
			}
			lines = append(lines, strings.Join(parts, "|"))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		sort.Strings(lines)
		fmt.Fprintf(&out, "## %s\n%s\n", query, strings.Join(lines, "\n"))
	}
	return out.String()
}

// newForeignKeySQLiteStore is newMigratedSQLiteStore with foreign keys
// enforced, as the sqlite provider opens production databases, so deleting a
// utxo row cascades to its asset, address and pointer rows.
func newForeignKeySQLiteStore(tb testing.TB) *Store {
	tb.Helper()
	db, err := OpenDB(
		"sqlite",
		fmt.Sprintf(
			"file:overlay_fk_%d?mode=memory&cache=shared"+
				"&_pragma=foreign_keys(1)",
			testStoreSequence.Add(1),
		),
		"sqlite",
		false,
	)
	require.NoError(tb, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	seedMigratedSQLite(tb, db)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(tb, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(tb, err)
	require.NoError(tb, store.Start(context.Background()))
	tb.Cleanup(func() { require.NoError(tb, store.Close()) })
	return store
}

func TestReadsOverlayTables(t *testing.T) {
	t.Parallel()
	for query, want := range map[string]bool{
		"SELECT amount FROM utxo WHERE id = ?":                     true,
		`UPDATE "utxo" SET deleted_slot = 1`:                       true,
		"SELECT * FROM utxo_pointer":                               true,
		"SELECT utxo_stake FROM reward_live_stake":                 true,
		"SELECT * FROM utxo_address WHERE utxo_id = ?":             false,
		"SELECT value FROM sync_state WHERE sync_key = ?":          false,
		"SELECT 'consumed_utxo_prune_floor'":                       false,
		`INSERT INTO "transaction" (hash) VALUES (?) RETURNING id`: false,
		"SELECT * FROM utxo_reference_edge":                        false,
	} {
		require.Equal(t, want, readsOverlayTables(query), query)
	}
}

// TestBatchUtxoOverlayStatementPlans pins the plans of the overlay's bulk
// statements against a skewed utxo table: the prefetch probes
// the output indexes once per key, the spend mark finds rows by primary key,
// and the live-stake prefetches probe the unique credential indexes of
// reward_live_stake and account. No plan may use a deleted_slot
// index or scan utxo, with no statistics or with statistics.
func TestBatchUtxoOverlayStatementPlans(t *testing.T) {
	skipPlanTestUnderRace(t)
	// Not t.Parallel: the stages rewrite this store's statistics and
	// indexes in order.
	store := newMigratedSQLiteStore(t)
	seedSkewedUtxos(t, store, 3_000)
	ctx := context.Background()
	conn, err := store.writeDB.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	const keys = 5
	prefetchArgs := make([]any, 0, 2*keys)
	markArgs := make([]any, 0, 3*keys)
	stakeArgs := make([]any, 0, 2*keys)
	for i := range keys {
		prefetchArgs = append(prefetchArgs, skewedUtxoTxID(i*7), i%4)
		markArgs = append(markArgs, int64(i+1), int64(9), make([]byte, 32))
		stakeArgs = append(stakeArgs, 0, skewedUtxoTxID(i)[:28])
	}

	stages := []struct {
		name  string
		setup func()
	}{
		{"no_stats", func() {}},
		{"bounded_optimize", func() {
			supported, err := store.dialect.OptimizePlannerStats(ctx, conn)
			require.NoError(t, err)
			require.True(t, supported)
		}},
		{"analyzed", func() {
			_, err := conn.ExecContext(ctx, "ANALYZE")
			require.NoError(t, err)
		}},
	}
	for _, stage := range stages {
		stage.setup()
		for _, returnStake := range []bool{false, true} {
			args := []any{int64(1), int64(1)}
			for i := range keys {
				args = sqliteUtxoSQL.bindRef(args, skewedUtxoTxID(i*7), i%4)
			}
			spend := connQueryPlan(
				t, conn, consumeUtxosBatchQuery(keys, returnStake), args...,
			)
			require.NotContains(
				t,
				spend,
				"idx_utxo_deleted",
				stage.name+": "+spend,
			)
			require.NotContains(t, spend, "SCAN utxo", stage.name+": "+spend)
			require.Equal(t, keys, strings.Count(
				spend,
				"USING INDEX idx_utxo_tx_id_output_idx (tx_id=? AND output_idx=?)",
			), stage.name+": "+spend)
		}
		prefetch := connQueryPlan(
			t, conn, overlayPrefetchQuery(keys),
			slices.Clone(prefetchArgs)...,
		)
		require.Contains(t, prefetch,
			"SEARCH t USING COVERING INDEX idx_transaction_hash (hash=?)",
			stage.name+": "+prefetch)
		require.Contains(t, prefetch,
			"SEARCH u USING INDEX idx_utxo_tx_id_output_idx (tx_id=? AND output_idx=?)",
			stage.name+": "+prefetch)
		require.NotContains(t, prefetch, "SCAN u", stage.name+": "+prefetch)
		mark := connQueryPlan(t, conn, overlayMarkQuery(keys), markArgs...)
		require.Contains(t, mark, "USING INTEGER PRIMARY KEY (rowid=?)",
			stage.name+": "+mark)
		stakeRows := connQueryPlan(
			t, conn, overlayStakeRowsQuery(keys), stakeArgs...,
		)
		require.Contains(t, stakeRows,
			"SEARCH r USING INDEX idx_reward_live_stake_cred "+
				"(credential_tag=? AND staking_key=?)",
			stage.name+": "+stakeRows)
		stakeAccounts := connQueryPlan(
			t, conn, overlayStakeAccountsQuery(keys), stakeArgs...,
		)
		require.Contains(t, stakeAccounts,
			"SEARCH a USING INDEX idx_account_credential "+
				"(credential_tag=? AND staking_key=?)",
			stage.name+": "+stakeAccounts)
		for name, plan := range map[string]string{
			"prefetch": prefetch, "mark": mark,
			"stake_rows": stakeRows, "stake_accounts": stakeAccounts,
		} {
			t.Logf("%s/%s:\n%s", stage.name, name, plan)
			require.NotContains(t, plan, "deleted",
				stage.name+"/"+name+": "+plan)
			require.NotContains(t, plan, "SCAN utxo",
				stage.name+"/"+name+": "+plan)
			require.NotContains(t, plan, "SCAN u ",
				stage.name+"/"+name+": "+plan)
		}
	}
}

// Only a UNIQUE failure on a table the overlay writes under its own ids is
// a fallback conflict; the per-row path would fail on any other the same
// way.
func TestOverlayWriteErrorClassifiesOwnTables(t *testing.T) {
	t.Parallel()
	for msg, want := range map[string]bool{
		"UNIQUE constraint failed: utxo.tx_id, utxo.output_idx (2067)": true,
		"UNIQUE constraint failed: utxo.id (1555)":                     true,
		"UNIQUE constraint failed: asset_holding.utxo_id (1555)":       false,
		"UNIQUE constraint failed: reward_live_stake.id (1555)":        true,
		"FOREIGN KEY constraint failed (787)":                          true,
		"UNIQUE constraint failed: redeemer_script.transaction_id":     false,
		"UNIQUE constraint failed: utxo_address.utxo_id":               false,
		"database is locked": false,
	} {
		err := overlayWriteError("write", errors.New(msg))
		require.Equal(t, want,
			errors.Is(err, types.ErrBatchUtxoOverlayConflict), msg)
	}
}

// Exact live-stake ids rest on no account row being deleted inside the
// batch, so the barrier must recognize every such delete.
func TestDeletesAccountRows(t *testing.T) {
	t.Parallel()
	for query, want := range map[string]bool{
		"DELETE FROM account WHERE credential_tag = ?":           true,
		"-- name: X\nDELETE FROM `account` WHERE id = ?":         true,
		"DELETE FROM account;":                                   true,
		"DELETE FROM `account`;":                                 true,
		"DELETE FROM account_reward_delta WHERE added_slot > ?":  false,
		"DELETE FROM x WHERE id IN (SELECT id FROM account)":     false,
		"SELECT created_slot FROM account WHERE staking_key = ?": false,
	} {
		require.Equal(t, want, deletesAccountRows(query), query)
	}
}
