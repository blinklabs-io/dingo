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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// poolMetricValue returns the value reported for metric name under
// pool="write"|"read", or -1 if no such series has been registered/gathered
// yet. It reads whichever of GetGauge/GetCounter the family actually
// carries, since dingo_database_sql_pool_* mixes GaugeFuncs
// (OpenConnections/InUse/Idle/MaxOpenConnections) and CounterFuncs
// (WaitCount/WaitDuration) under this one helper.
func poolMetricValue(
	t *testing.T,
	reg *prometheus.Registry,
	name, pool string,
) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			var gotPool string
			for _, label := range metric.GetLabel() {
				if label.GetName() == "pool" {
					gotPool = label.GetValue()
				}
			}
			if gotPool != pool {
				continue
			}
			if g := metric.GetGauge(); g != nil {
				return g.GetValue()
			}
			if c := metric.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	return -1
}

// TestSQLPoolMetricsNilWhenNoRegistry proves the pool metrics are a true
// no-op with no PromRegistry configured, the same guarantee
// TestSQLOperationsCounterNilWhenNoRegistry establishes for the
// operations counter/duration histogram: newSQLPoolMetrics must not panic
// or otherwise misbehave when reg is nil, since every provider constructs
// its Store this way whenever metrics are disabled.
func TestSQLPoolMetricsNilWhenNoRegistry(t *testing.T) {
	t.Parallel()
	db, err := OpenDB(
		"sqlite",
		fmt.Sprintf(
			"file:pool_metrics_nil_registry_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
		"sqlite",
		false,
	)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
		// PromRegistry deliberately left nil.
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	// Reaching here without a panic from newSQLPoolMetrics/
	// safeRegisterGaugeFunc/safeRegisterCounterFunc is most of the
	// assertion. WritePoolStats/ReadPoolStats must also still work
	// directly, independent of whether anything is registered against them.
	stats := store.WritePoolStats()
	require.Equal(t, 1, stats.MaxOpenConnections)
}

// TestWritePoolWaitMetricsReflectContention is the regression test this
// change exists for: it proves dingo_database_sql_pool_wait_count_total and
// dingo_database_sql_pool_wait_duration_seconds_total (pool="write") report
// real contention on the single write connection, not just a registered
// metric sitting at its zero value.
//
// It holds writeDB's one and only connection open in an uncommitted
// transaction (mirroring production's SetMaxOpenConns(1) constraint, which
// newMigratedSQLiteStoreWithRegistry already applies), starts a second
// write from another goroutine that can only proceed once that connection
// is freed, confirms via WritePoolStats that database/sql actually recorded
// a wait before releasing the first transaction, and confirms the second
// write was actually blocked (not merely slow) by checking it had not yet
// completed. It then asserts the *gathered Prometheus metrics* -- not just
// the underlying sql.DBStats -- increased, since a bug that registered the
// gauges/counters but wired the wrong statsFn, wrong pool, or wrong label
// would still leave sql.DBStats itself correct.
func TestWritePoolWaitMetricsReflectContention(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := newMigratedSQLiteStoreWithRegistry(t, reg)
	ctx := context.Background()

	require.Equal(
		t, 1, store.WritePoolStats().MaxOpenConnections,
		"the write pool must be capped at one connection for this test to "+
			"actually exercise contention rather than two connections "+
			"running concurrently",
	)
	require.Equal(
		t,
		float64(1),
		poolMetricValue(t, reg, "dingo_database_sql_pool_max_open_connections", "write"),
	)

	baselineWaitCount := poolMetricValue(
		t, reg, "dingo_database_sql_pool_wait_count_total", "write",
	)
	baselineWaitDuration := poolMetricValue(
		t, reg, "dingo_database_sql_pool_wait_duration_seconds_total", "write",
	)

	// Hold the pool's only connection open in an uncommitted transaction.
	holder, err := store.writeDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = holder.Rollback()
	})

	// A second write has no free connection to acquire until holder
	// releases one, so it must block in database/sql's pool wait path.
	done := make(chan error, 1)
	go func() {
		_, execErr := store.writeDB.ExecContext(
			ctx,
			"INSERT INTO sync_state (sync_key, value) VALUES (?, ?)",
			"pool_metrics_test_key", "1",
		)
		done <- execErr
	}()

	// Confirm database/sql actually recorded the second call waiting,
	// rather than racing it to completion.
	testutil.WaitForCondition(
		t,
		func() bool {
			return poolMetricValue(
				t, reg, "dingo_database_sql_pool_wait_count_total", "write",
			) > baselineWaitCount
		},
		testutil.AsyncWait,
		"expected dingo_database_sql_pool_wait_count_total{pool=\"write\"} "+
			"to increase once the second write starts waiting for the held "+
			"connection",
	)

	// The blocked write must still be pending: it cannot have completed
	// while the only connection is held by holder.
	select {
	case execErr := <-done:
		t.Fatalf(
			"the second write completed (err=%v) before the holding "+
				"transaction released the pool's only connection -- it "+
				"was never actually blocked",
			execErr,
		)
	default:
	}

	// Hold the connection a little longer once the wait is confirmed
	// queued, so the blocked write accumulates a duration large enough to
	// survive the host's clock resolution. Releasing immediately made
	// dingo_database_sql_pool_wait_duration_seconds_total read back as
	// exactly 0 on Windows CI: WaitCount had already incremented (proving
	// database/sql queued the request), but the queued-to-released window
	// was short enough that time.Since(waitStart) rounded to zero on that
	// platform's timer.
	time.Sleep(50 * time.Millisecond)

	// Release the held connection so the waiting write can proceed.
	require.NoError(t, holder.Rollback())

	execErr := testutil.RequireReceive(
		t, done, testutil.AsyncWait,
		"expected the previously-blocked write to complete once the "+
			"holding transaction released the connection",
	)
	require.NoError(t, execErr)

	require.Greater(
		t,
		poolMetricValue(t, reg, "dingo_database_sql_pool_wait_count_total", "write"),
		baselineWaitCount,
		"dingo_database_sql_pool_wait_count_total{pool=\"write\"} must "+
			"reflect the wait the blocked write just experienced",
	)
	require.Greater(
		t,
		poolMetricValue(t, reg, "dingo_database_sql_pool_wait_duration_seconds_total", "write"),
		baselineWaitDuration,
		"dingo_database_sql_pool_wait_duration_seconds_total{pool=\"write\"} "+
			"must reflect non-zero time spent waiting",
	)
}
