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
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	assetPlanUtxos  = 20000
	assetPlanAssets = 2000
)

// newAssetPlanStore returns a store holding a live UTxO set much larger than
// any one asset's holdings: every fifth output is spent at its own slot, each
// output holds two of assetPlanAssets assets and has an address row. With
// analyze set it gathers the bounded statistics a running node keeps. It
// returns the policy ID and name of an asset held by a handful of outputs.
func newAssetPlanStore(t *testing.T, analyze bool) (*Store, []byte, []byte) {
	t.Helper()
	store := newFilePlannerStatsStore(t, prometheus.NewRegistry())
	exec := func(query string) {
		t.Helper()
		_, err := store.writeDB.Exec(query)
		require.NoError(t, err, query)
	}
	exec(fmt.Sprintf(`
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < %d
)
INSERT INTO "transaction" (id, hash) SELECT i, randomblob(32) FROM n`,
		assetPlanUtxos))
	exec(fmt.Sprintf(`
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < %d
)
INSERT INTO utxo
    (id, tx_id, output_idx, credential_tag, staking_key,
     added_slot, deleted_slot, amount)
SELECT i, i, 0, 0, randomblob(28), 1,
       CASE WHEN i %% 5 = 0 THEN i ELSE 0 END, '1'
FROM n`, assetPlanUtxos))
	exec(fmt.Sprintf(`
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < %d
)
INSERT INTO utxo_address (utxo_id, address) SELECT i, randomblob(57) FROM n`,
		assetPlanUtxos))
	exec(fmt.Sprintf(`
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < %d
)
INSERT INTO asset_identity (id, policy_id, name)
SELECT i, CAST(printf('p%%027d', i %% 40) AS BLOB), CAST(printf('a%%05d', i) AS BLOB)
FROM n`, assetPlanAssets))
	exec(fmt.Sprintf(`
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < %d
)
INSERT INTO asset_holding (utxo_id, asset_id, amount)
SELECT i, (i * 7) %% %d + 1, '5' FROM n
UNION ALL
SELECT i, (i * 13) %% %d + 1, '3' FROM n WHERE (i * 13) %% %d <> (i * 7) %% %d`,
		assetPlanUtxos, assetPlanAssets, assetPlanAssets, assetPlanAssets,
		assetPlanAssets))
	if analyze {
		_, err := store.OptimizePlannerStatsContext(
			t.Context(), PlannerStatsTriggerPeriodic,
		)
		require.NoError(t, err)
	}
	require.NoError(t, store.refreshReadPool(t.Context()))
	var policy, name []byte
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT policy_id, name FROM asset_identity WHERE id = 1`,
	).Scan(&policy, &name))
	return store, policy, name
}

// An asset read reached the live UTxO set through
// idx_utxo_deleted_payment_script (deleted_slot = 0) and probed the holdings
// once per live output, whatever few outputs held the asset. With the
// statistics a running node keeps, and with none, every asset read must
// resolve the asset's holdings first and reach utxo by primary key.
func TestAssetReadsDoNotDriveFromLiveUtxoIndex(t *testing.T) {
	skipPlanTestUnderRace(t)
	t.Parallel()
	for _, analyze := range []bool{false, true} {
		t.Run(fmt.Sprintf("analyze=%t", analyze), func(t *testing.T) {
			t.Parallel()
			store, policy, name := newAssetPlanStore(t, analyze)
			holders := assetHolderGroups("sqlite")
			reads := []struct {
				label string
				query string
				args  []any
			}{
				{"asset quantity", assetQuantityQuery(
					"sqlite", "dingo_uint64_sum(h.amount)",
				), []any{policy, name}},
				{"asset holder address check",
					assetHolderAddressCheckQuery("sqlite"),
					[]any{policy, name}},
				{"asset holders count",
					`SELECT COUNT(*) FROM (SELECT 1` + holders +
						`) AS holders`, []any{policy, name}},
				{"asset holders page",
					`SELECT ua.address, dingo_uint64_sum(a.amount)` +
						` AS quantity` + holders +
						` ORDER BY quantity DESC, ua.address DESC` +
						` LIMIT ? OFFSET ?`,
					[]any{policy, name, 10, 0}},
				{"live utxos by asset", liveUtxosByAssetQuery("sqlite"),
					[]any{policy, name}},
				{"live utxos by asset policy",
					liveUtxosByAssetPolicyQuery("sqlite"),
					[]any{policy}},
			}
			for _, read := range reads {
				plan := queryPlan(t, store.writeDB, read.query, read.args...)
				assert.NotContains(t, plan, "idx_utxo_deleted_payment_script",
					"%s drives from the live UTxO index:\n%s", read.label, plan)
				for _, line := range planLines(plan) {
					step := strings.TrimLeft(line, "|-` ")
					assert.False(t,
						strings.HasPrefix(step, "SCAN utxo") ||
							strings.HasPrefix(step, "SCAN u "),
						"%s scans utxo:\n%s", read.label, plan)
				}
			}
		})
	}
}
