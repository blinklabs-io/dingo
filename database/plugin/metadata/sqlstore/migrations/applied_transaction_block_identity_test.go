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
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

const blockIdentityVersion = 75

func blockIdentityHash(b byte) []byte {
	return bytes.Repeat([]byte{b}, 32)
}

// blockIdentityDB returns a database migrated through the version before the
// block dictionary, holding one applied transaction per entry of blocks: tx_id
// i+1 in the block named by blocks[i], and no block for a nil entry.
func blockIdentityDB(
	t *testing.T,
	blocks [][]byte,
) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, blockIdentityVersion-1)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	for i, block := range blocks {
		txHash := bytes.Repeat([]byte{byte(i + 1)}, 32)
		_, err := db.Exec(
			`INSERT INTO "transaction" (id, hash) VALUES (?, ?)`, i+1, txHash)
		require.NoError(t, err)
		_, err = db.Exec(`
INSERT INTO applied_transaction (tx_id, block_hash, slot, block_index, valid)
VALUES (?, ?, ?, ?, TRUE)`, i+1, block, 10+i, i)
		require.NoError(t, err)
	}
	return db, path
}

func blockIdentityRunner(
	t *testing.T,
	db *sql.DB,
	path string,
	batchSize int,
	wrap func(migrations.Backfill) migrations.Backfill,
) migrations.Runner {
	t.Helper()
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	migration := &registry[blockIdentityVersion-1]
	require.Equal(t, "applied-transaction-block-identity", migration.Name)
	require.NotNil(t, migration.Backfill)
	if batchSize > 0 {
		migration.BatchSize = batchSize
	}
	if wrap != nil {
		migration.Backfill = wrap(migration.Backfill)
	}
	return migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
	}
}

// blockIdentityRows renders each applied transaction as tx_id/block hash/block
// index in tx_id order, reading the hash through the dictionary.
func blockIdentityRows(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`
SELECT a.tx_id, hex(b.hash), a.block_index
FROM applied_transaction a LEFT JOIN block_identity b ON b.id = a.block_id
ORDER BY a.tx_id`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var out []string
	for rows.Next() {
		var (
			id    int64
			hash  sql.NullString
			index int64
		)
		require.NoError(t, rows.Scan(&id, &hash, &index))
		out = append(out, fmt.Sprintf("%d/%s/%d", id, hash.String, index))
	}
	require.NoError(t, rows.Err())
	return out
}

func blockIdentityTestBlocks() [][]byte {
	a, b, c := blockIdentityHash(0xa1), blockIdentityHash(0xb2), blockIdentityHash(0xc3)
	return [][]byte{a, a, b, nil, b, b, c, a, c, c, b, a}
}

// TestAppliedTransactionBlockIdentityBackfill upgrades a database whose
// transactions repeat their block hash and requires one dictionary row per
// distinct block, every transaction pointing at its own block, a NULL block
// left NULL, ids in order of first appearance, the repeated column and its
// index gone, and the block index present in block order.
func TestAppliedTransactionBlockIdentityBackfill(t *testing.T) {
	t.Parallel()
	blocks := blockIdentityTestBlocks()
	db, path := blockIdentityDB(t, blocks)
	var want []string
	for i, block := range blocks {
		want = append(want, fmt.Sprintf("%d/%X/%d", i+1, block, i))
	}
	runner := blockIdentityRunner(t, db, path, 5, nil)
	require.NoError(t, runner.Run(context.Background()))

	require.Equal(t, want, blockIdentityRows(t, db))

	rows, err := db.Query(`SELECT hex(hash) FROM block_identity ORDER BY id`)
	require.NoError(t, err)
	var order []string
	for rows.Next() {
		var hash string
		require.NoError(t, rows.Scan(&hash))
		order = append(order, hash)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, []string{
		fmt.Sprintf("%X", blockIdentityHash(0xa1)),
		fmt.Sprintf("%X", blockIdentityHash(0xb2)),
		fmt.Sprintf("%X", blockIdentityHash(0xc3)),
	}, order, "one row per block, in order of first appearance")

	var hasHash, hasID int
	require.NoError(t, db.QueryRow(`
SELECT COALESCE(SUM(name = 'block_hash'), 0), COALESCE(SUM(name = 'block_id'), 0)
FROM pragma_table_info('applied_transaction')`).Scan(&hasHash, &hasID))
	require.Zero(t, hasHash, "block_hash is dropped")
	require.Equal(t, 1, hasID)

	var oldIndex, newIndex int
	require.NoError(t, db.QueryRow(`
SELECT COALESCE(SUM(name = 'idx_applied_transaction_block_hash'), 0),
       COALESCE(SUM(name = 'idx_applied_transaction_block_id'), 0)
FROM sqlite_master WHERE type = 'index'`).Scan(&oldIndex, &newIndex))
	require.Zero(t, oldIndex)
	require.Equal(t, 1, newIndex)
	cols, err := db.Query(
		`SELECT name FROM pragma_index_info('idx_applied_transaction_block_id') ORDER BY seqno`)
	require.NoError(t, err)
	var indexed []string
	for cols.Next() {
		var name string
		require.NoError(t, cols.Scan(&name))
		indexed = append(indexed, name)
	}
	require.NoError(t, cols.Err())
	require.NoError(t, cols.Close())
	require.Equal(t, []string{"block_id", "block_index"}, indexed)
}

// TestAppliedTransactionBlockIdentityResumesAfterInterruption cancels the
// backfill partway, resumes, and requires the result of an uninterrupted run.
func TestAppliedTransactionBlockIdentityResumesAfterInterruption(t *testing.T) {
	t.Parallel()
	blocks := blockIdentityTestBlocks()
	wantDB, wantPath := blockIdentityDB(t, blocks)
	wantRunner := blockIdentityRunner(t, wantDB, wantPath, 0, nil)
	require.NoError(t, wantRunner.Run(context.Background()))
	want := blockIdentityRows(t, wantDB)

	db, path := blockIdentityDB(t, blocks)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	interrupted := blockIdentityRunner(t, db, path, 3,
		func(inner migrations.Backfill) migrations.Backfill {
			return func(
				ctx context.Context,
				batch migrations.Batch,
			) (migrations.BatchResult, error) {
				calls++
				if calls == 3 {
					cancel()
				}
				return inner(ctx, batch)
			}
		})
	require.Error(t, interrupted.Run(ctx))
	var phase, cursor string
	require.NoError(t, db.QueryRow(
		`SELECT phase, cursor FROM schema_migrations WHERE version = ?`,
		blockIdentityVersion,
	).Scan(&phase, &cursor))
	require.Equal(t, "backfill", phase)
	require.Equal(t, "6", cursor, "two committed batches of three rows")

	resume := blockIdentityRunner(t, db, path, 3, nil)
	require.NoError(t, resume.Run(context.Background()))
	require.Equal(t, want, blockIdentityRows(t, db))
}

// TestAppliedTransactionBlockIdentityRefusesUnresolvedRows removes a
// dictionary row the cursor already passed and requires the final batch to
// refuse rather than drop block_hash with the row's block unrecoverable.
func TestAppliedTransactionBlockIdentityRefusesUnresolvedRows(t *testing.T) {
	t.Parallel()
	blocks := blockIdentityTestBlocks()
	db, path := blockIdentityDB(t, blocks)
	runner := blockIdentityRunner(t, db, path, 0,
		func(inner migrations.Backfill) migrations.Backfill {
			return func(
				ctx context.Context,
				batch migrations.Batch,
			) (migrations.BatchResult, error) {
				_, err := batch.Tx.ExecContext(ctx, `
CREATE TEMP TRIGGER forget_block AFTER UPDATE OF block_id ON applied_transaction
BEGIN UPDATE applied_transaction SET block_id = NULL WHERE tx_id = new.tx_id AND tx_id = 1; END`)
				require.NoError(t, err)
				return inner(ctx, batch)
			}
		})
	err := runner.Run(context.Background())
	require.ErrorContains(t, err, "without a block id")
	var hasHash int
	require.NoError(t, db.QueryRow(`
SELECT COUNT(*) FROM pragma_table_info('applied_transaction')
WHERE name = 'block_hash'`).Scan(&hasHash))
	require.Equal(t, 1, hasHash, "the hash column must survive a failed backfill")
}
