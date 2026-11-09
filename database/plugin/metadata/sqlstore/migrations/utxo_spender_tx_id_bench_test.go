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

package migrations_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

func peakRSSKiB() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if rest, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			fields := strings.Fields(rest)
			if len(fields) > 0 {
				n, _ := strconv.ParseInt(fields[0], 10, 64)
				return n
			}
		}
	}
	return 0
}

// TestUtxoSpenderTxIDMigrationScale times the version-57 upgrade over a
// previous-schema database with DINGO_MIGRATION_BENCH_ROWS utxo rows, four in
// five of them spent, and reports the time, the database and WAL sizes and
// the process's peak resident memory. It only runs when the variable is set.
func TestUtxoSpenderTxIDMigrationScale(t *testing.T) {
	rowsEnv := os.Getenv("DINGO_MIGRATION_BENCH_ROWS")
	if rowsEnv == "" {
		t.Skip("set DINGO_MIGRATION_BENCH_ROWS to run the scale measurement")
	}
	rows, err := strconv.Atoi(rowsEnv)
	require.NoError(t, err)
	txs := rows / 2
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(OFF)" +
		"&_pragma=wal_autocheckpoint(10000)&_pragma=cache_size(-50000)" +
		"&_pragma=busy_timeout(30000)"
	db, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	run := func(upTo int) {
		runner := migrations.Runner{
			DB: db, Dialect: "sqlite", Registry: registry[:upTo],
			Locker: migrations.NewFileLocker(path + ".migrate.lock"),
		}
		require.NoError(t, runner.Run(context.Background()))
	}
	run(spenderMigrationVersion - 1)

	_, err = db.Exec(fmt.Sprintf(`
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < %d)
INSERT INTO "transaction" (hash, slot, block_index)
SELECT randomblob(32), i, 0 FROM n`, txs))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < %d)
INSERT INTO utxo (transaction_id, tx_id, output_idx, credential_tag,
    payment_key, added_slot, deleted_slot, spent_at_tx_id, amount)
SELECT 1 + (i %% %d), randomblob(32), i %% 3, 0, randomblob(28), i,
    CASE WHEN i %% 5 = 0 THEN 0 ELSE i + 1 END,
    CASE WHEN i %% 5 = 0 THEN NULL
         ELSE (SELECT hash FROM "transaction" WHERE id = 1 + (i * 7919) %% %d) END,
    '1000000'
FROM n`, rows, txs, txs))
	require.NoError(t, err)
	_, err = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, err)
	sizeBefore := fileSize(path)

	var stop atomic.Bool
	var peakWAL atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for !stop.Load() {
			<-ticker.C
			if size := fileSize(path + "-wal"); size > peakWAL.Load() {
				peakWAL.Store(size)
			}
		}
	}()
	rssBefore := peakRSSKiB()
	start := time.Now()
	run(spenderMigrationVersion)
	elapsed := time.Since(start)
	stop.Store(true)
	<-done
	_, err = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, err)

	var spent, legacy int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM utxo WHERE spent_by_tx_id IS NOT NULL",
	).Scan(&spent))
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM utxo WHERE spent_at_tx_id IS NOT NULL",
	).Scan(&legacy))
	require.Equal(t, rows-rows/5, spent)
	require.Zero(t, legacy)
	t.Logf(
		"rows=%d spent=%d elapsed=%s rows/s=%.0f db_before=%dMiB "+
			"db_after=%dMiB peak_wal=%dMiB peak_rss_before=%dMiB "+
			"peak_rss_after=%dMiB",
		rows, spent, elapsed.Round(time.Millisecond),
		float64(spent)/elapsed.Seconds(),
		sizeBefore>>20, fileSize(path)>>20, peakWAL.Load()>>20,
		rssBefore>>10, peakRSSKiB()>>10,
	)
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
