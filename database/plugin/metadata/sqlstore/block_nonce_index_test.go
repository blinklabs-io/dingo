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
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

// blockNonceHash derives a uniformly distributed 32-byte hash, as a real block
// hash is, so insert order says nothing about index key order.
func blockNonceHash(slot uint64) []byte {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], slot)
	sum := sha256.Sum256(raw[:])
	return sum[:]
}

func newWALFileStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := OpenDB(
		"sqlite",
		"file:"+path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(OFF)"+
			"&_pragma=wal_autocheckpoint(0)",
		"sqlite",
		false,
	)
	require.NoError(t, err)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store, path
}

func blockNonceRows(from, count uint64) []models.BlockNonce {
	rows := make([]models.BlockNonce, 0, count)
	for i := range count {
		slot := (from + i) * 20
		rows = append(rows, models.BlockNonce{
			Hash:  blockNonceHash(slot),
			Slot:  slot,
			Nonce: blockNonceHash(slot + 1),
		})
	}
	return rows
}

// TestBlockNonceLookupsUseAnIndex pins the access path of every hot
// block_nonce read and delete. Each supplies a slot or a slot range, so none
// may scan the table and none needs a hash-leading index.
func TestBlockNonceLookupsUseAnIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	require.NoError(t, store.SetBlockNonces(blockNonceRows(0, 2000), nil))
	_, err := store.writeDB.Exec("ANALYZE")
	require.NoError(t, err)

	queries := map[string]string{
		"exact point": "SELECT nonce FROM block_nonce WHERE hash = x'00' AND slot = 40",
		"slot range": "SELECT hash, nonce, id, slot, is_checkpoint FROM block_nonce " +
			"WHERE slot >= 100 AND slot < 4000 ORDER BY slot ASC",
		"latest":          latestBlockNonceQuery,
		"range stats":     "SELECT COUNT(*), MIN(slot), MAX(slot) FROM block_nonce WHERE slot >= 100 AND slot <= 4000",
		"delete before":   "DELETE FROM block_nonce WHERE slot < 4000",
		"delete after":    "DELETE FROM block_nonce WHERE slot > 4000 OR (slot = 4000 AND hash <> x'00')",
		"delete from":     "DELETE FROM block_nonce WHERE slot >= 4000",
		"first unpruned":  "SELECT MIN(slot) FROM block_nonce WHERE slot >= 100 AND is_checkpoint = FALSE",
		"delete no ckpts": "DELETE FROM block_nonce WHERE slot < 4000 AND is_checkpoint = FALSE",
	}
	for name, query := range queries {
		rows, err := store.writeDB.Query("EXPLAIN QUERY PLAN " + query)
		require.NoError(t, err, name)
		var plan []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &unused, &detail), name)
			plan = append(plan, detail)
		}
		require.NoError(t, rows.Err(), name)
		require.NoError(t, rows.Close(), name)
		joined := strings.Join(plan, "; ")
		require.Contains(t, joined, "INDEX", "%s must use an index: %s", name, joined)
		for _, step := range plan {
			if strings.HasPrefix(step, "SCAN block_nonce") {
				require.Contains(t, step, "INDEX",
					"%s scans block_nonce without an index: %s", name, joined)
			}
		}
	}
}

// TestBlockNonceInsertsDirtyFewIndexPages measures WAL frames per commit of a
// 50-block apply chunk on a populated table. An index led by the random block
// hash dirties a different leaf for nearly every row; an index led by the
// ascending slot appends to the same few right-edge pages.
func TestBlockNonceInsertsDirtyFewIndexPages(t *testing.T) {
	t.Parallel()
	const (
		seedRows  = 60000
		batchSize = 50
		batches   = 20
		// A hash-led index dirties about one leaf per row; a slot-led index
		// dirties the table's tail page, the index's tail page and the
		// occasional split. Half a page per row leaves ample margin on both
		// sides of that gap.
		maxFramesPerBatch = batchSize / 2
	)
	store, path := newWALFileStore(t)
	require.NoError(t, store.SetBlockNonces(blockNonceRows(0, seedRows), nil))
	_, err := store.writeDB.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, err)

	var pageSize int
	require.NoError(t, store.writeDB.QueryRow("PRAGMA page_size").Scan(&pageSize))
	frameSize := int64(pageSize + 24)
	walFrames := func() int64 {
		info, err := os.Stat(path + "-wal")
		require.NoError(t, err)
		if info.Size() < 32 {
			return 0
		}
		return (info.Size() - 32) / frameSize
	}

	before := walFrames()
	for i := range uint64(batches) {
		require.NoError(t, store.SetBlockNonces(
			blockNonceRows(seedRows+i*batchSize, batchSize), nil,
		))
	}
	perBatch := float64(walFrames()-before) / batches
	t.Logf("WAL frames per %d-row batch: %.1f", batchSize, perBatch)
	require.LessOrEqual(t, perBatch, float64(maxFramesPerBatch),
		"each %d-row batch dirtied %.1f pages; block_nonce indexes must "+
			"not lead with the block hash", batchSize, perBatch)
}
