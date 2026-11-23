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
	"bytes"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

func (e *assetSplitEngine) columnExists(
	t *testing.T,
	table, column string,
) bool {
	t.Helper()
	schema := "current_schema()"
	if e.dialect == "mysql" {
		schema = "DATABASE()"
	}
	var count int
	require.NoError(t, e.db.QueryRow(e.query(`
SELECT count(*) FROM information_schema.columns
WHERE table_schema = `+schema+` AND table_name = ? AND column_name = ?`),
		table, column).Scan(&count))
	return count > 0
}

// engineBlockIdentityRows renders each applied transaction as tx_id/block
// hash/block index in tx_id order, reading the hash through the dictionary.
func engineBlockIdentityRows(t *testing.T, e *assetSplitEngine) []string {
	t.Helper()
	rows, err := e.db.Query(`
SELECT a.tx_id, b.hash, a.block_index
FROM applied_transaction a LEFT JOIN block_identity b ON b.id = a.block_id
ORDER BY a.tx_id`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var out []string
	for rows.Next() {
		var (
			id    int64
			hash  []byte
			index int64
		)
		require.NoError(t, rows.Scan(&id, &hash, &index))
		out = append(out, fmt.Sprintf("%d/%X/%d", id, hash, index))
	}
	require.NoError(t, rows.Err())
	return out
}

// exerciseBlockIdentity upgrades a database whose transactions repeat their
// block hash, in batches smaller than the data, then replays every phase of
// the finished migration, and checks the dictionary, the dropped column and
// both indexes on a real server.
func exerciseBlockIdentity(t *testing.T, e *assetSplitEngine) {
	t.Helper()
	e.run(t, e.registry[:blockIdentityVersion-1])
	blocks := blockIdentityTestBlocks()
	var want []string
	for i, block := range blocks {
		id := i + 1
		_, err := e.db.Exec(e.query(`
INSERT INTO transaction (id, hash) VALUES (?, ?)`),
			id, bytes.Repeat([]byte{byte(id)}, 32))
		require.NoError(t, err)
		_, err = e.db.Exec(e.query(`
INSERT INTO applied_transaction (tx_id, block_hash, slot, block_index, valid)
VALUES (?, ?, ?, ?, TRUE)`), id, block, 10+i, i)
		require.NoError(t, err)
		want = append(want, fmt.Sprintf("%d/%X/%d", id, block, i))
	}

	registry := append([]migrations.Migration(nil), e.registry...)
	registry[blockIdentityVersion-1].BatchSize = 5
	e.run(t, registry)
	require.Equal(t, want, engineBlockIdentityRows(t, e))

	rows, err := e.db.Query(`SELECT hash FROM block_identity ORDER BY id`)
	require.NoError(t, err)
	var order []string
	for rows.Next() {
		var hash []byte
		require.NoError(t, rows.Scan(&hash))
		order = append(order, fmt.Sprintf("%X", hash))
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, []string{
		fmt.Sprintf("%X", blockIdentityHash(0xa1)),
		fmt.Sprintf("%X", blockIdentityHash(0xb2)),
		fmt.Sprintf("%X", blockIdentityHash(0xc3)),
	}, order, "one row per block, in order of first appearance")
	require.False(t, e.columnExists(t, "applied_transaction", "block_hash"))
	require.True(t, e.columnExists(t, "applied_transaction", "block_id"))
	require.True(t, e.indexExists(
		t, "applied_transaction", "idx_applied_transaction_block_id"))
	require.False(t, e.indexExists(
		t, "applied_transaction", "idx_applied_transaction_block_hash"))
	_, err = e.db.Exec(e.query(`
INSERT INTO block_identity (hash) VALUES (?)`), blockIdentityHash(0xa1))
	require.Error(t, err, "a block hash is held once")

	// A crash before the migration completes replays every phase against a
	// table whose block_hash column and index are already gone.
	_, err = e.db.Exec(e.query(`
UPDATE schema_migrations SET phase = 'expand', dirty = ?, completed_at = NULL
WHERE version = ?`), true, blockIdentityVersion)
	require.NoError(t, err)
	e.run(t, registry)
	require.Equal(t, want, engineBlockIdentityRows(t, e))
}

func TestAppliedTransactionBlockIdentityAppliesOnPostgres(t *testing.T) {
	t.Parallel()
	exerciseBlockIdentity(
		t, newPostgresAssetSplitEngine(t, "dingo_v74_block_identity"))
}

func TestAppliedTransactionBlockIdentityAppliesOnMySQL(t *testing.T) {
	t.Parallel()
	exerciseBlockIdentity(
		t, newMySQLAssetSplitEngine(t, "dingo_v74_block_identity"))
}
