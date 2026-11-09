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
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

func TestBlockNonceIndexUpgradeKeepsRowsAndUniqueness(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	run := func(registry []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: registry,
			Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
		}
		require.NoError(t, runner.Run(context.Background()))
	}

	run(registry[:55])
	_, ok := indexSQL(t, db, "hash_slot")
	require.True(t, ok)
	_, ok = indexSQL(t, db, "idx_block_nonce_slot_hash")
	require.False(t, ok)
	for slot := 1; slot <= 3; slot++ {
		_, err = db.Exec(
			"INSERT INTO block_nonce (hash, slot, nonce, is_checkpoint) VALUES (?, ?, ?, ?)",
			[]byte{byte(slot)}, slot, []byte{0xaa, byte(slot)}, slot == 2,
		)
		require.NoError(t, err)
	}

	run(registry)
	_, ok = indexSQL(t, db, "hash_slot")
	require.False(t, ok)
	definition, ok := indexSQL(t, db, "idx_block_nonce_slot_hash")
	require.True(t, ok)
	require.Contains(t, definition, "UNIQUE")
	require.Contains(t, definition, "(`slot`,`hash`)")

	var count int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM block_nonce").Scan(&count))
	require.Equal(t, 3, count)

	// A repeated (hash, slot) is still one row, and the existing conflict
	// target keeps resolving against the replacement index.
	_, err = db.Exec(`INSERT INTO block_nonce (hash, slot, nonce, is_checkpoint)
VALUES (?, 2, ?, FALSE)
ON CONFLICT (hash, slot) DO UPDATE SET
    nonce = excluded.nonce,
    is_checkpoint = block_nonce.is_checkpoint OR excluded.is_checkpoint`,
		[]byte{2}, []byte{0xbb})
	require.NoError(t, err)
	var nonce []byte
	var checkpoint bool
	require.NoError(t, db.QueryRow(
		"SELECT nonce, is_checkpoint FROM block_nonce WHERE hash = ? AND slot = 2",
		[]byte{2},
	).Scan(&nonce, &checkpoint))
	require.Equal(t, []byte{0xbb}, nonce)
	require.True(t, checkpoint)
	_, err = db.Exec(
		"INSERT INTO block_nonce (hash, slot, nonce) VALUES (?, 2, ?)",
		[]byte{2}, []byte{0xcc},
	)
	require.Error(t, err)

	run(registry)
}
