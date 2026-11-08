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
	"log/slog"
	"path/filepath"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// loggedUpgradeDB is spenderUpgradeDB with the migration log captured and the
// upgrade's error returned.
func loggedUpgradeDB(
	t *testing.T,
) (*sql.DB, func(upTo int) error, *lockedBuffer) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return db, func(upTo int) error {
		runner := migrations.Runner{
			DB: db, Dialect: "sqlite",
			Registry: append([]migrations.Migration(nil), registry[:upTo]...),
			Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
			Logger:   logger,
		}
		return runner.Run(context.Background())
	}, logs
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	_, err := db.Exec(query, args...)
	require.NoError(t, err, query)
}

// Two outputs of one producer at one output index cannot satisfy the unique
// producer index. The upgrade names the rows instead of failing in CREATE
// UNIQUE INDEX, and leaves the schema at the previous version.
func TestUtxoProducerIndexUpgradeRefusesDuplicateProducers(t *testing.T) {
	t.Parallel()
	db, upgrade, _ := loggedUpgradeDB(t)
	require.NoError(t, upgrade(inputEdgeMigrationVersion))
	mustExec(t, db, "INSERT INTO `transaction` (hash, slot, block_index) VALUES (?, 1, 0)", spenderHash(1))
	for _, hash := range [][]byte{spenderHash(0xC1), spenderHash(0xC2)} {
		mustExec(t, db, `INSERT INTO utxo (transaction_id, tx_id, output_idx, deleted_slot, amount)
VALUES (1, ?, 200, 0, '1')`, hash)
	}

	err := upgrade(producerIndexMigrationVersion)
	require.Error(t, err)
	require.ErrorContains(t, err, "duplicate outputs")
	require.ErrorContains(t, err, "producer_tx_id=1 output_idx=200")
	_, ok := indexSQL(t, db, "tx_id_output_idx")
	require.True(t, ok, "the hash index stays until the contract can succeed")
}

// A row whose tx_id is not its producer's hash upgrades, since the producer
// index does not read tx_id, but is unreachable by hash afterwards. The count
// and a sample are logged.
func TestUtxoProducerIndexUpgradeLogsHashMismatch(t *testing.T) {
	t.Parallel()
	db, upgrade, logs := loggedUpgradeDB(t)
	require.NoError(t, upgrade(inputEdgeMigrationVersion))
	mustExec(t, db, "INSERT INTO `transaction` (hash, slot, block_index) VALUES (?, 1, 0)", spenderHash(1))
	mustExec(t, db, `INSERT INTO utxo (transaction_id, tx_id, output_idx, deleted_slot, amount)
VALUES (1, ?, 0, 0, '1'), (1, ?, 1, 0, '1')`, spenderHash(1), spenderHash(0xC3))

	require.NoError(t, upgrade(producerIndexMigrationVersion))
	require.Contains(t, logs.String(), "utxo tx_id differs from its producer transaction hash")
	require.Contains(t, logs.String(), "count=1")
}

// A spender hash with no transaction row cannot move to spent_by_tx_id. The
// row keeps its legacy hash and deleted_slot, and the count is logged.
func TestUtxoSpenderUpgradeLogsUnresolvedSpenders(t *testing.T) {
	t.Parallel()
	db, upgrade, logs := loggedUpgradeDB(t)
	require.NoError(t, upgrade(spenderMigrationVersion-1))
	mustExec(t, db, "INSERT INTO `transaction` (hash, slot, block_index) VALUES (?, 1, 0)", spenderHash(1))
	mustExec(t, db, `INSERT INTO utxo (transaction_id, tx_id, output_idx, deleted_slot, spent_at_tx_id, amount)
VALUES (1, ?, 0, 500, ?, '1')`, spenderHash(1), spenderHash(0xD1))

	require.NoError(t, upgrade(spenderMigrationVersion))
	require.Contains(t, logs.String(), "utxo spender hash has no transaction row")
	require.Contains(t, logs.String(), "count=1")
	var spentBy sql.NullInt64
	var deleted int64
	var legacy []byte
	require.NoError(t, db.QueryRow(
		"SELECT spent_by_tx_id, deleted_slot, spent_at_tx_id FROM utxo",
	).Scan(&spentBy, &deleted, &legacy))
	require.False(t, spentBy.Valid)
	require.Equal(t, int64(500), deleted)
	require.Equal(t, spenderHash(0xD1), legacy)
}

// An edge naming a hash with no transaction row is not copied to the edge
// table. The number dropped per table is logged.
func TestUtxoInputEdgeUpgradeLogsDroppedEdges(t *testing.T) {
	t.Parallel()
	db, upgrade, logs := loggedUpgradeDB(t)
	require.NoError(t, upgrade(inputEdgeMigrationVersion-1))
	mustExec(t, db, "INSERT INTO `transaction` (hash, slot, block_index) VALUES (?, 1, 0)", spenderHash(1))
	mustExec(t, db, `INSERT INTO utxo (transaction_id, tx_id, output_idx, deleted_slot, amount)
VALUES (1, ?, 0, 0, '1')`, spenderHash(1))
	mustExec(t, db, "INSERT INTO utxo_reference_input (utxo_id, transaction_hash) VALUES (1, ?), (1, ?)",
		spenderHash(1), spenderHash(0xE1))
	mustExec(t, db, "INSERT INTO utxo_collateral_input (utxo_id, transaction_hash) VALUES (1, ?)",
		spenderHash(0xE2))

	require.NoError(t, upgrade(inputEdgeMigrationVersion))
	require.Contains(t, logs.String(), "utxo edge names a hash with no transaction row")
	require.Contains(t, logs.String(), "table=utxo_reference_edge count=1")
	require.Contains(t, logs.String(), "table=utxo_collateral_edge count=1")
	var edges int
	require.NoError(t, db.QueryRow(
		"SELECT (SELECT COUNT(*) FROM utxo_reference_edge) + (SELECT COUNT(*) FROM utxo_collateral_edge)",
	).Scan(&edges))
	require.Equal(t, 1, edges)
}
