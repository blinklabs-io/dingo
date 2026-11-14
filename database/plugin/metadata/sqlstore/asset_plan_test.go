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
	"encoding/binary"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// recordedStatement is one statement a planRecorder saw.
type recordedStatement struct {
	query string
	args  []any
}

// planRecorder forwards to a real queryer and keeps what it ran, so a
// plan test can explain the statements the production code builds rather
// than copies of them.
type planRecorder struct {
	queryer
	seen *[]recordedStatement
}

func (q planRecorder) record(query string, args []any) {
	*q.seen = append(*q.seen, recordedStatement{query: query, args: args})
}

func (q planRecorder) ExecContext(
	ctx context.Context, query string, args ...any,
) (sql.Result, error) {
	q.record(query, args)
	return q.queryer.ExecContext(ctx, query, args...)
}

func (q planRecorder) QueryContext(
	ctx context.Context, query string, args ...any,
) (*sql.Rows, error) {
	q.record(query, args)
	return q.queryer.QueryContext(ctx, query, args...)
}

func (q planRecorder) QueryRowContext(
	ctx context.Context, query string, args ...any,
) *sql.Row {
	q.record(query, args)
	return q.queryer.QueryRowContext(ctx, query, args...)
}

func planLines(plan string) []string {
	return strings.Split(strings.TrimSpace(plan), "\n")
}

var assetPlanSubjects = []string{
	"asset_holding", "asset_identity", "h", "i", "a",
}

// requireNoAssetScan fails when any plan step scans an asset table, with or
// without an index: a hot asset read must search by key.
func requireNoAssetScan(t *testing.T, name, plan string) {
	t.Helper()
	for _, line := range planLines(plan) {
		step := strings.TrimLeft(line, "|-` ")
		for _, subject := range assetPlanSubjects {
			require.False(t,
				strings.HasPrefix(step, "SCAN "+subject+" ") ||
					step == "SCAN "+subject,
				"%s scans %s:\n%s", name, subject, plan)
		}
	}
}

// seedAssetPlanStore fills a store with live UTxOs holding assets spread over
// many identities, then gathers planner statistics, so the plans below are the
// ones a populated database chooses.
func seedAssetPlanStore(t *testing.T) *Store {
	t.Helper()
	store := newMigratedSQLiteStore(t)
	const utxos, assets = 3000, 300
	tx, err := store.writeDB.Begin()
	require.NoError(t, err)
	for id := 1; id <= utxos; id++ {
		txID := make([]byte, 32)
		binary.BigEndian.PutUint64(txID[24:], uint64(id))
		_, err := tx.Exec(`
INSERT INTO utxo (id, tx_id, output_idx, payment_key, staking_key,
                  credential_tag, added_slot, deleted_slot, amount)
VALUES (?, ?, 0, ?, ?, 0, ?, 0, '1')`,
			id, txID, []byte{byte(id % 7)}, []byte{byte(id % 11)}, id)
		require.NoError(t, err)
		_, err = tx.Exec(
			`INSERT INTO utxo_address (utxo_id, address) VALUES (?, ?)`,
			id, []byte(fmt.Sprintf("addr%d", id%97)))
		require.NoError(t, err)
	}
	for asset := 1; asset <= assets; asset++ {
		_, err := tx.Exec(
			`INSERT INTO asset_identity (policy_id, name) VALUES (?, ?)`,
			[]byte{byte(asset % 5)}, []byte(fmt.Sprintf("n%d", asset)))
		require.NoError(t, err)
	}
	for id := 1; id <= utxos; id++ {
		for k := range 3 {
			_, err := tx.Exec(
				`INSERT INTO asset_holding (utxo_id, asset_id, amount)
VALUES (?, ?, '1')`, id, (id*7+k*13)%assets+1)
			require.NoError(t, err)
		}
	}
	require.NoError(t, tx.Commit())
	_, err = store.writeDB.Exec("ANALYZE")
	require.NoError(t, err)
	return store
}

// TestAssetReadsNeverScanAssetTables explains every hot asset read on a
// populated database and requires each to search the identity and holding
// tables by key.
func TestAssetReadsNeverScanAssetTables(t *testing.T) {
	skipPlanTestUnderRace(t)
	t.Parallel()
	store := seedAssetPlanStore(t)
	policy := []byte{3}
	name := []byte("n3")
	hash := lcommon.Blake2b224{}
	copy(hash[:], policy)

	var seen []recordedStatement
	recorder := planRecorder{queryer: store.writeDB, seen: &seen}
	ctx := t.Context()
	q := store.operationalQueries(recorder)
	_, err := q.AssetHeld(ctx, sqlitequery.AssetHeldParams{
		PolicyID: policy, Name: name,
	})
	require.NoError(t, err)
	var plans []string
	plan := func(label, query string, args ...any) {
		plans = append(plans, label)
		requireNoAssetScan(
			t,
			label,
			queryPlan(t, store.writeDB, query, args...),
		)
	}
	for i, statement := range seen {
		plan(
			fmt.Sprintf("sqlc query %d", i),
			statement.query,
			statement.args...)
	}

	plan("asset quantity",
		assetQuantityQuery("dingo_uint64_sum(h.amount)"),
		policy, name)
	plan("live utxos by asset", liveUtxosByAssetQuery, policy, name)
	plan("live utxos by asset policy", liveUtxosByAssetPolicyQuery, policy)
	plan("asset holders count",
		`SELECT COUNT(*) FROM (SELECT 1`+assetHolderGroups+`) AS holders`,
		policy, name)
	plan("asset holders page",
		`SELECT ua.address, dingo_uint64_sum(a.amount) AS quantity`+
			assetHolderGroups+` ORDER BY quantity DESC, ua.address DESC LIMIT ? OFFSET ?`,
		policy, name, 10, 0)
	source, args, err := assetTransactionsQuery(hash, name, nil, nil)
	require.NoError(t, err)
	plan("asset transactions", `SELECT * FROM `+source, args...)

	for _, query := range []models.UtxoWithOrderingQuery{
		{MatchAllAddresses: true, FilterByAsset: true, AssetPolicyID: policy},
		{MatchAllAddresses: true, FilterByAsset: true, AssetPolicyID: policy, AssetName: name},
	} {
		statement, args, err := countUtxosOrderingStatement(&query,
			utxoOrderingDisqualifiesLivenessIndex("sqlite", &query))
		require.NoError(t, err)
		plan(
			fmt.Sprintf(
				"utxo ordering count (name %t)",
				query.AssetName != nil,
			),
			statement,
			args...)
	}

	seen = nil
	require.NoError(t, accountAssetRows(
		ctx, recorder, 0, []byte{3}, true, "asc", 1000,
		func(_, _ []byte, _ *big.Int, _ bool) error { return nil },
	))
	for i, statement := range seen {
		plan(
			fmt.Sprintf("account asset rows %d", i),
			statement.query,
			statement.args...)
	}

	seen = nil
	utxos := []*models.Utxo{{ID: 1}, {ID: 2}}
	require.NoError(t, store.loadUtxoAssets(ctx, recorder, utxos))
	for i, statement := range seen {
		plan(
			fmt.Sprintf("utxo asset load %d", i),
			statement.query,
			statement.args...)
	}
	require.Greater(t, len(plans), 8)
}

// TestAssetWritePathsNeverUseTheAssetIdIndex drops idx_asset_holding_asset_id,
// the state a Mithril stream catch-up leaves while it holds ingest, and runs
// trusted-batch apply, spending, the consumed-UTxO prune and a rollback. They
// must all succeed and leave the same holdings, and none of the statements
// that write or resolve holdings may name the index.
func TestAssetWritePathsNeverUseTheAssetIdIndex(t *testing.T) {
	skipPlanTestUnderRace(t)
	t.Parallel()
	const index = "idx_asset_holding_asset_id"
	without := newForeignKeySQLiteStore(t)
	_, err := without.writeDB.Exec("DROP INDEX " + index)
	require.NoError(t, err)

	for id := 1; id <= 9; id++ {
		_, err := without.writeDB.Exec(
			`INSERT INTO "transaction" (id, hash) VALUES (?, ?)`,
			id, []byte{0xee, byte(id)},
		)
		require.NoError(t, err)
		_, err = without.writeDB.Exec(`
INSERT INTO utxo (id, tx_id, output_idx, added_slot, deleted_slot, amount)
VALUES (?, ?, 0, 1, 0, '1')`, id, id)
		require.NoError(t, err)
	}
	var seen []recordedStatement
	recorder := planRecorder{queryer: without.writeDB, seen: &seen}
	rows := make([][]any, 0, 4)
	for i := range 4 {
		rows = append(rows, []any{
			int64(i + 1), []byte{byte(i)}, []byte("n"), validString("1"),
		})
	}
	require.NoError(t, flushAssetHoldings(
		t.Context(), recorder, rows, without.dialect.ParameterLimit(),
	))
	require.NoError(t, without.insertAssetHolding(
		t.Context(), recorder, 9,
		&models.Asset{PolicyId: []byte{9}, Name: []byte("x"), Amount: 1},
	))
	require.NotEmpty(t, seen)
	for _, statement := range seen {
		require.NotContains(t, queryPlan(
			t, without.writeDB, statement.query, statement.args...,
		), index, statement.query)
	}

	for _, path := range parityPaths() {
		if path.name != "per-row" && path.name != "overlay" {
			continue
		}
		var holdings [2][]string
		for i, withoutIndex := range []bool{false, true} {
			store := newForeignKeySQLiteStore(t)
			if withoutIndex {
				_, err := store.writeDB.Exec("DROP INDEX " + index)
				require.NoError(t, err)
			}
			path.apply(t, store, false)
			holdings[i] = assetHoldingsText(t, store.writeDB)
			require.NotEmpty(t, holdings[i])
			require.NoError(t, store.DeleteUtxosAfterSlot(0, nil))
			require.Zero(t, countRows(t, store.writeDB,
				`SELECT COUNT(*) FROM asset_holding`), path.name)
		}
		require.Equal(t, holdings[0], holdings[1], path.name)
	}
}

func assetHoldingsText(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`
SELECT lower(hex(i.policy_id)), lower(hex(i.name)), h.utxo_id, h.amount
FROM asset_holding h JOIN asset_identity i ON i.id = h.asset_id
ORDER BY h.utxo_id, i.policy_id, i.name`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var out []string
	for rows.Next() {
		var policy, name, amount string
		var utxo int64
		require.NoError(t, rows.Scan(&policy, &name, &utxo, &amount))
		out = append(
			out,
			fmt.Sprintf("%s/%s/%d/%s", policy, name, utxo, amount),
		)
	}
	require.NoError(t, rows.Err())
	return out
}
