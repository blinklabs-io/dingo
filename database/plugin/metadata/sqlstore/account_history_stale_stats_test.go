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
	"regexp"
	"strings"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// staleStatsGrowth is how many rows each table gains after ANALYZE; far past
// the roughly 25-fold growth PRAGMA optimize needs to re-analyze a table
// recorded at one row.
const staleStatsGrowth = 2000

// Block application fills transaction, certs and the certificate tables long
// after the startup statistics run, and every query that reaches a
// transaction through certs then plans against a cardinality of one. This
// pins the recovery: with those statistics stale each query below scans a
// table, and after the incremental run the periodic trigger issues none does.
func TestAccountHistoryPlansAfterPeriodicStatsRefresh(t *testing.T) {
	t.Parallel()
	store := newFilePlannerStatsStore(t, prometheus.NewRegistry())
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := store.writeDB.Exec(query, args...)
		require.NoError(t, err, query)
	}
	exec(`INSERT INTO "transaction" (id, slot, block_index) VALUES (1, 1, 0)`)
	exec(
		`INSERT INTO certs (id, transaction_id, slot, cert_index, cert_type)
VALUES (1, 1, 1, 0, ?)`,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	// Every table the probes read needs a statistics row before the growth,
	// or it plans from defaults and the run measures an unanalyzed table
	// rather than a stale one.
	sourceTables := map[string]struct{}{}
	for _, source := range accountRegistrationHistorySources {
		sourceTables[source.table] = struct{}{}
	}
	for _, source := range accountDelegationHistorySources {
		sourceTables[source.table] = struct{}{}
	}
	for _, table := range accountRegistrationStateTables {
		sourceTables[table] = struct{}{}
	}
	for _, table := range accountDeregistrationStateTables {
		sourceTables[table] = struct{}{}
	}
	for table := range sourceTables {
		exec(fmt.Sprintf(
			`INSERT INTO %s (staking_key, credential_tag, certificate_id, added_slot)
VALUES (randomblob(28), 0, 1, 1)`, table))
	}
	exec(`INSERT INTO pool (id, pool_key_hash) VALUES (1, randomblob(28))`)
	exec(
		`INSERT INTO pool_registration
(pool_key_hash, certificate_id, pool_id, added_slot, margin, pledge, cost)
VALUES (randomblob(28), 1, 1, 1, '1/100', '1', '1')`,
	)
	exec("ANALYZE")
	grow := func(table, columns, values string) {
		exec(fmt.Sprintf(`
WITH RECURSIVE n(i) AS (
    SELECT 2 UNION ALL SELECT i + 1 FROM n WHERE i <= %d
)
INSERT INTO %s (%s) SELECT %s FROM n`,
			staleStatsGrowth+1, table, columns, values))
	}
	grow("\"transaction\"", "id, slot, block_index", "i, i, 0")
	grow(
		"certs",
		"id, transaction_id, slot, cert_index, cert_type",
		fmt.Sprintf("i, i, i, 0, %d",
			uint(lcommon.CertificateTypeStakeRegistration)),
	)
	for table := range sourceTables {
		grow(
			table,
			"staking_key, credential_tag, certificate_id, added_slot",
			"randomblob(28), 0, i, i",
		)
	}
	grow(
		"pool_registration",
		"pool_key_hash, certificate_id, pool_id, added_slot, margin, pledge, cost",
		"randomblob(28), i, 1, i, '1/100', '1', '1'",
	)
	ctx := context.Background()
	require.NoError(t, store.refreshReadPool(ctx))
	var stat string
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT stat FROM sqlite_stat1
WHERE tbl = 'certs' AND idx IS NOT NULL ORDER BY idx LIMIT 1`,
	).Scan(&stat))
	require.True(t, strings.HasPrefix(stat, "1 "),
		"certs statistics must still be stale, got %q", stat)

	key := make([]byte, 28)
	type probe struct {
		name  string
		query string
		args  []any
	}
	var probes []probe
	regQuery, regArgs := accountRegistrationHistoryQuery(0, key)
	probes = append(probes, probe{"registration history", regQuery, regArgs})
	delegQuery, delegArgs := accountDelegationHistoryQuery(0, key)
	probes = append(probes, probe{"delegation history", delegQuery, delegArgs})
	probes = append(probes, probe{
		"stake registrations by credential",
		stakeRegistrationsBySourceSQL,
		[]any{uint8(0), key},
	})
	probes = append(probes, probe{
		"pool registrations",
		poolRegistrationsSQL,
		[]any{key},
	})
	for _, table := range append(
		append([]string{}, accountRegistrationStateTables...),
		accountDeregistrationStateTables...,
	) {
		probes = append(probes, probe{
			"certificate positions " + table,
			accountCertificatePositionsSQL(table, 1),
			[]any{key, uint64(10)},
		})
	}
	// A full scan of any table the queries read: the deposit, delegation
	// and validation paths each reach every one of these through a
	// per-matching-row lookup, so a scan is O(rows) work per call.
	scanned := []string{"certs", "c", "tx", "t", "sr", "p", "source",
		"pool_registration"}
	for table := range sourceTables {
		scanned = append(scanned, table)
	}
	scan := regexp.MustCompile(`SCAN (` + strings.Join(scanned, "|") + `)\b`)
	plans := func() map[string]string {
		out := make(map[string]string, len(probes))
		for _, p := range probes {
			rows, err := store.readDB.QueryContext(
				ctx, "EXPLAIN QUERY PLAN "+p.query, p.args...,
			)
			require.NoError(t, err, p.name)
			var details []string
			for rows.Next() {
				var id, parent, notUsed int
				var detail string
				require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
				details = append(details, detail)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			out[p.name] = strings.Join(details, "\n")
		}
		return out
	}

	// Negative control: with stale statistics every query scans.
	for name, plan := range plans() {
		require.Regexp(t, scan, plan,
			"%s must scan under stale statistics, plan:\n%s", name, plan)
	}

	result, err := store.OptimizePlannerStatsContext(
		ctx, PlannerStatsTriggerPeriodic,
	)
	require.NoError(t, err)
	require.True(t, result.Changed, "the run must re-analyze the grown tables")

	for name, plan := range plans() {
		require.NotRegexp(t, scan, plan,
			"%s must not scan after refresh, plan:\n%s", name, plan)
	}
}
