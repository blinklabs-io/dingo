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
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// setMissingSystemTempDir points the operating system's temporary directory
// at a path that does not exist, so any file created there fails.
func setMissingSystemTempDir(t *testing.T) {
	t.Helper()
	missing := filepath.Join(t.TempDir(), "missing")
	t.Setenv("TMPDIR", missing)
	if runtime.GOOS == "windows" {
		t.Setenv("TMP", missing)
		t.Setenv("TEMP", missing)
	}
	_, err := os.CreateTemp("", "probe-*")
	require.Error(t, err, "the system temporary directory must be unusable")
}

// TestLiveUtxoDigestSpoolsInTheScratchDirectory checks that the spool is
// created in the store's scratch directory and not the system temporary
// directory, which on a host with a small or memory-backed /tmp cannot hold
// a mainnet-sized live set.
func TestLiveUtxoDigestSpoolsInTheScratchDirectory(t *testing.T) {
	setMissingSystemTempDir(t)
	store := newMigratedSQLiteStore(t)
	store.scratchDir = t.TempDir()
	all := seedDigestFixture(t, store, 6)
	want, err := referenceDigest(all, 50, nil)
	require.NoError(t, err)
	got, err := store.liveUtxoDigest(context.Background(), 50, nil, nil, 4)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// A scratch directory that cannot hold files fails the digest, so the
	// success above is not the system directory being used after all.
	blocked := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(blocked, nil, 0o600))
	store.scratchDir = blocked
	_, err = store.liveUtxoDigest(context.Background(), 50, nil, nil, 4)
	require.Error(t, err)
}

func TestDigestScratchDirDefaultsToTheSQLiteDirectory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := &Store{sqlitePath: filepath.Join(dir, "metadata.sqlite")}
	require.Equal(t, dir, s.digestScratchDir())
	s.scratchDir = filepath.Join(dir, "scratch")
	require.Equal(t, s.scratchDir, s.digestScratchDir())
	require.Empty(t, (&Store{}).digestScratchDir())
}

// TestLiveUtxoDigestMergesSortedRuns bounds each sorted run so the spool
// holds several, which the merge must interleave back into digest order.
func TestLiveUtxoDigestMergesSortedRuns(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	all := seedDigestFixture(t, store, 12)
	resolve := func(h []byte, idx uint32) ([]byte, error) {
		return resolvedBytes(h, idx), nil
	}
	want, err := referenceDigest(all, 50, resolve)
	require.NoError(t, err)
	for _, runBytes := range []int{1, 100, 300, 1 << 20} {
		store.utxoDigestRunBytes = runBytes
		got, err := store.liveUtxoDigest(
			context.Background(), 50, resolve, nil, 5,
		)
		require.NoError(t, err)
		require.Equal(t, want, got, "run bytes %d", runBytes)
	}
}

// TestLiveUtxoSetQueryNeedsNoDatabaseSort checks that SQLite plans the scan
// without a sort. SQLite spills a large sort into the process-wide temporary
// directory, which no per-connection setting moves, so the spool sorts the
// rows instead.
func TestLiveUtxoSetQueryNeedsNoDatabaseSort(t *testing.T) {
	t.Parallel()
	s := newFilePlannerStatsStore(t, prometheus.NewRegistry())
	_, err := s.writeDB.Exec(`WITH RECURSIVE n(i) AS (
 SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<20000
) INSERT INTO "transaction"(id,hash) SELECT i,randomblob(32) FROM n`)
	require.NoError(t, err)
	_, err = s.writeDB.Exec(`WITH RECURSIVE n(i) AS (
 SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<20000
) INSERT INTO utxo(tx_id,output_idx,added_slot,deleted_slot,amount)
SELECT i,0,1,CASE WHEN i%2=0 THEN 0 ELSE 2 END,'1' FROM n`)
	require.NoError(t, err)
	_, err = s.writeDB.Exec("ANALYZE")
	require.NoError(t, err)
	require.NoError(t, s.refreshReadPool(context.Background()))

	plan := explainQueryPlan(t, s, liveUtxoSetSQL(`"transaction"`), 50, 50)
	require.NotContains(t, plan, "TEMP B-TREE")
	require.NotContains(t, plan, "idx_transaction_hash")
	require.Contains(t, plan, "SCAN u")
}
