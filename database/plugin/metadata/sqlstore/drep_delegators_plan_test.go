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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/drepquery"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/stretchr/testify/require"
)

// namedStatement returns the statement in seen that carries the name comment
// of name.
func namedStatement(
	t *testing.T,
	seen []recordedStatement,
	name string,
) recordedStatement {
	t.Helper()
	for _, stmt := range seen {
		if strings.Contains(stmt.query, "-- name: "+name+" ") {
			return stmt
		}
	}
	require.Failf(t, "statement not executed", "%s", name)
	return recordedStatement{}
}

// TestDrepCredentialLookupsPlanOnDrepIndex gathers planner statistics while
// no account is delegated, so the sampled statistics hold no script-hash DRep
// type and SQLite rates the drep_type index as selective as the drep index.
// It then delegates every account to a script-hash DRep and requires each
// lookup by one DRep credential to plan on the drep index rather than the
// drep_type index, which matches every delegated account.
func TestDrepCredentialLookupsPlanOnDrepIndex(t *testing.T) {
	t.Parallel()
	const (
		drepIndex     = "idx_account_drep_active_staking_key"
		drepTypeIndex = "idx_account_drep_type_active_staking_key"
		accounts      = 20000
		drepType      = uint8(models.DrepTypeScriptHash)
	)
	store, _ := newWALSQLiteStore(t)
	ctx := context.Background()
	_, err := store.writeDB.ExecContext(ctx, `
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
INSERT INTO account (staking_key, credential_tag, added_slot, created_slot,
  reward, drep_type, active, expiration_epoch)
SELECT randomblob(28), 0, 10, 10, '0', 0, TRUE, 0 FROM n`, accounts)
	require.NoError(t, err)
	_, err = store.writeDB.ExecContext(ctx, "ANALYZE")
	require.NoError(t, err)
	_, err = store.writeDB.ExecContext(ctx, `
UPDATE account SET drep_type = ?, drep = randomblob(28)`, drepType)
	require.NoError(t, err)
	target := []byte("target-drep")
	_, err = store.writeDB.ExecContext(ctx, `
UPDATE account SET drep = ? WHERE rowid = (SELECT min(rowid) FROM account)`,
		target)
	require.NoError(t, err)

	requireDrepIndex := func(t *testing.T, name, plan string) {
		t.Helper()
		require.Contains(t, plan, drepIndex, "%s plan:\n%s", name, plan)
		require.NotContains(t, plan, drepTypeIndex,
			"%s plan:\n%s", name, plan)
	}

	t.Run("GetDRepDelegators", func(t *testing.T) {
		var seen []recordedStatement
		rec := planRecorder{queryer: store.writeDB, seen: &seen}
		rows, err := store.operationalQueries(rec).GetDRepDelegators(ctx,
			sqlitequery.GetDRepDelegatorsParams{
				Drep:     target,
				DrepType: validInt64(int64(drepType)),
			})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		stmt := namedStatement(t, seen, "GetDRepDelegators")
		requireDrepIndex(t, "GetDRepDelegators",
			queryPlan(t, store.writeDB, stmt.query, stmt.args...))
	})

	t.Run("GetDRepDelegatorsAtSlot", func(t *testing.T) {
		plan := queryPlan(t, store.writeDB, drepDelegatorsLiveSQL(true),
			int64(100), drepType, target)
		requireDrepIndex(t, "GetDRepDelegatorsAtSlot", plan)
	})

	t.Run("GetDRepVotingPower", func(t *testing.T) {
		query, args := drepquery.VotingPowerSQL("sqlite", drepType, target, 0)
		plan := queryPlan(t, store.writeDB, query, args...)
		require.NotContains(t, plan, drepTypeIndex,
			"GetDRepVotingPower plan:\n%s", plan)
		require.Contains(t, plan, "SEARCH a USING INDEX "+drepIndex,
			"GetDRepVotingPower plan:\n%s", plan)
	})

	t.Run("removeAllDrepDelegatorsPV10", func(t *testing.T) {
		var seen []recordedStatement
		rec := planRecorder{queryer: store.writeDB, seen: &seen}
		require.NoError(t, removeAllDrepDelegators(
			ctx, rec, drepType, target, 20, 10,
		))
		// The plans depend on the statistics, which the clear leaves alone.
		for _, name := range []string{
			"InsertAccountDrepClearForDrep",
			"ClearAccountDrepDelegationsForDrep",
		} {
			stmt := namedStatement(t, seen, name)
			requireDrepIndex(t, name,
				queryPlan(t, store.writeDB, stmt.query, stmt.args...))
		}
		var cleared int
		require.NoError(t, store.writeDB.QueryRowContext(ctx,
			"SELECT count(*) FROM account WHERE drep IS NULL").Scan(&cleared))
		require.Equal(t, 1, cleared)
	})
}
