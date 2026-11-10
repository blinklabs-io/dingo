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
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// assetPagesHash derives a uniformly distributed 28-byte policy id, as a real
// one is, so row order says nothing about identity key order.
func assetPagesHash(n uint64) []byte {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], n)
	sum := sha256.Sum256(raw[:])
	return sum[:28]
}

// TestAssetHoldingInsertsDirtyFewPages measures WAL frames per commit of a
// 50-output apply chunk against a populated table, for the holdings schema and
// for a control table shaped like the legacy asset table (a unique index led
// by the hash-like name and policy, and an index on the policy) over the same
// rows. Holdings key on the ascending UTxO id, so the primary key appends and
// only the asset-id index takes scattered writes; the legacy unique index
// dirties a leaf per row on top of its policy index. The control sits in the
// same database and process, so both sides share page size and commit
// behaviour, and the assertion is a ratio rather than a frame count.
func TestAssetHoldingInsertsDirtyFewPages(t *testing.T) {
	t.Parallel()
	const (
		assets     = 5000
		seedUtxos  = 20000
		perUtxo    = 3
		batchUtxos = 50
		batches    = 20
		// The control scatters writes over its unique index and its policy
		// index and appends to the table; holdings scatter over the asset-id
		// index alone and append to the primary key. That is about a third of
		// the control's pages, where a primary key led by the asset would
		// scatter a second index and reach about two thirds.
		maxRatio = 0.5
	)
	store, path := newWALFileStore(t)
	_, err := store.writeDB.Exec(`
CREATE TABLE asset_control (
    name BLOB, policy_id BLOB, fingerprint BLOB,
    id INTEGER PRIMARY KEY AUTOINCREMENT, utxo_id INTEGER, amount TEXT,
    UNIQUE (name, policy_id, utxo_id));
CREATE INDEX idx_asset_control_policy_id ON asset_control (policy_id)`)
	require.NoError(t, err)

	assetName := func(asset uint64) []byte {
		return assetPagesHash(asset + assets)[:8]
	}
	// pick spreads a UTxO's holdings over identities as a hash would.
	pick := func(utxo, k uint64) uint64 {
		return binary.BigEndian.Uint64(assetPagesHash(utxo*perUtxo+k)) % assets
	}
	seed, err := store.writeDB.Begin()
	require.NoError(t, err)
	for asset := range uint64(assets) {
		_, err := seed.Exec(
			`INSERT INTO asset_identity (policy_id, name) VALUES (?, ?)`,
			assetPagesHash(asset), assetName(asset))
		require.NoError(t, err)
	}
	for utxo := uint64(1); utxo <= seedUtxos; utxo++ {
		for k := range uint64(perUtxo) {
			asset := pick(utxo, k)
			_, err := seed.Exec(`
INSERT OR IGNORE INTO asset_holding (utxo_id, asset_id, amount)
SELECT ?, id, '1' FROM asset_identity WHERE policy_id = ? AND name = ?`,
				utxo, assetPagesHash(asset), assetName(asset))
			require.NoError(t, err)
			_, err = seed.Exec(`
INSERT OR IGNORE INTO asset_control (name, policy_id, utxo_id, amount)
VALUES (?, ?, ?, '1')`,
				assetName(asset), assetPagesHash(asset), utxo)
			require.NoError(t, err)
		}
	}
	require.NoError(t, seed.Commit())

	var pageSize int
	require.NoError(
		t,
		store.writeDB.QueryRow("PRAGMA page_size").Scan(&pageSize),
	)
	frameSize := int64(pageSize + 24)
	walFrames := func() int64 {
		info, err := os.Stat(path + "-wal")
		require.NoError(t, err)
		if info.Size() < 32 {
			return 0
		}
		return (info.Size() - 32) / frameSize
	}
	// measure runs batches commits of batchUtxos outputs and returns the WAL
	// frames each commit wrote.
	measure := func(write func(tx *sql.Tx, utxo uint64)) float64 {
		_, err := store.writeDB.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		require.NoError(t, err)
		before := walFrames()
		next := uint64(seedUtxos) + 1
		for range batches {
			tx, err := store.writeDB.Begin()
			require.NoError(t, err)
			for range batchUtxos {
				write(tx, next)
				next++
			}
			require.NoError(t, tx.Commit())
		}
		return float64(walFrames()-before) / batches
	}
	holdings := measure(func(tx *sql.Tx, utxo uint64) {
		rows := make([][]any, 0, perUtxo)
		for k := range uint64(perUtxo) {
			asset := pick(utxo, k)
			rows = append(rows, []any{
				int64(utxo), assetPagesHash(asset), assetName(asset),
				validString("1"),
			})
		}
		require.NoError(t, flushAssetHoldings(
			t.Context(), tx, rows, store.dialect.ParameterLimit(),
		))
	})
	control := measure(func(tx *sql.Tx, utxo uint64) {
		for k := range uint64(perUtxo) {
			asset := pick(utxo, k)
			_, err := tx.Exec(`
INSERT OR IGNORE INTO asset_control (name, policy_id, utxo_id, amount)
VALUES (?, ?, ?, '1')`,
				assetName(asset), assetPagesHash(asset), utxo)
			require.NoError(t, err)
		}
	})
	t.Logf("WAL frames per %d-output commit: holdings %.1f, legacy shape %.1f",
		batchUtxos, holdings, control)
	require.Positive(t, control)
	require.LessOrEqual(t, holdings/control, maxRatio,
		"holdings dirtied %.1f pages per commit against %.1f for the legacy "+
			"shape; the unique key must append in utxo_id order",
		holdings, control)
}
