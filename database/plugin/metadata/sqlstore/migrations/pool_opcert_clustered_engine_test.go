//go:build dingo_extra_plugins

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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// exercisePoolOpCertClustered upgrades a database holding by-value
// pool_opcert_sequence rows on a real server, replays the finished migration,
// and checks the rows, the key and the rebuilt indexes.
func exercisePoolOpCertClustered(t *testing.T, e *assetSplitEngine) {
	t.Helper()
	e.run(t, e.registry[:poolOpCertClusteredVersion-1])
	for _, row := range poolOpCertRows {
		_, err := e.db.Exec(e.query(`
INSERT INTO pool_opcert_sequence (pool_key_hash, slot, sequence)
VALUES (?, ?, ?)`), row.Pool, *row.Slot, row.Sequence)
		require.NoError(t, err)
	}
	e.run(t, e.registry)

	contents := func() []string {
		rows, err := e.db.Query(`
SELECT c.hash, o.slot, o.sequence FROM pool_opcert_sequence o
JOIN credential c ON c.id = o.pool_key_id ORDER BY o.slot`)
		require.NoError(t, err)
		defer func() { require.NoError(t, rows.Close()) }()
		var out []string
		for rows.Next() {
			var pool []byte
			var slot, sequence int64
			require.NoError(t, rows.Scan(&pool, &slot, &sequence))
			out = append(out, fmt.Sprintf("%x/%d/%d", pool, slot, sequence))
		}
		require.NoError(t, rows.Err())
		return out
	}
	var want []string
	for _, row := range poolOpCertRows {
		want = append(want,
			fmt.Sprintf("%x/%d/%d", row.Pool, *row.Slot, row.Sequence))
	}
	require.Equal(t, want, contents())
	require.False(t, e.tableExists(t, "pool_opcert_sequence_clustered"))
	for _, index := range []string{
		"idx_pool_opcert_sequence_slot",
		"idx_pool_opcert_sequence_pool_sequence",
	} {
		require.True(t, e.indexExists(t, "pool_opcert_sequence", index), index)
	}
	// The key refuses a second counter for one (pool, slot).
	_, err := e.db.Exec(e.query(`
INSERT INTO pool_opcert_sequence (pool_key_id, slot, sequence)
SELECT pool_key_id, slot, 9 FROM pool_opcert_sequence WHERE slot = ?`), 10)
	require.Error(t, err)

	// A crash between the contract phase and its completion replays every
	// earlier phase against the rebuilt table.
	_, err = e.db.Exec(e.query(`
UPDATE schema_migrations SET phase = 'expand', dirty = ?, completed_at = NULL
WHERE version = ?`), true, poolOpCertClusteredVersion)
	require.NoError(t, err)
	e.run(t, e.registry)
	require.Equal(t, want, contents())
}

func TestPoolOpCertClusteredAppliesOnPostgres(t *testing.T) {
	t.Parallel()
	exercisePoolOpCertClustered(
		t, newPostgresAssetSplitEngine(t, "dingo_v79_opcerts"),
	)
}

func TestPoolOpCertClusteredAppliesOnMySQL(t *testing.T) {
	t.Parallel()
	exercisePoolOpCertClustered(
		t, newMySQLAssetSplitEngine(t, "dingo_v79_opcerts"),
	)
}
