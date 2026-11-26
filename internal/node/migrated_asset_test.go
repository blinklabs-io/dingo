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

package node

import (
	"bytes"
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/stretchr/testify/require"
)

// The released version stored one asset row per output in the asset table.
// The preview chunks the released database was loaded from carry no native
// assets, so rows are added to it here, in the released schema, before the
// database moves to the storage-role layout. The IndexerDB a migrated node
// serves from must hold the same holdings in asset_identity and asset_holding,
// with a NULL name stored as the empty name, and no asset table.
func TestMigratedReleasedDatabaseKeepsItsAssetHoldings(t *testing.T) {
	t.Parallel()
	dir := unpackLegacyDatabase(t, legacyParityArchive)
	policy := bytes.Repeat([]byte{0xab}, 28)
	token := []byte("token")

	legacy, err := sql.Open("sqlite", filepath.Join(dir, "metadata.sqlite"))
	require.NoError(t, err)
	var utxoIDs []int64
	rows, err := legacy.Query(
		"SELECT id FROM utxo WHERE deleted_slot = 0 ORDER BY id LIMIT 2",
	)
	require.NoError(t, err)
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		utxoIDs = append(utxoIDs, id)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Len(t, utxoIDs, 2, "the released database holds live outputs")
	require.Zero(t, rowCount(t, legacy, "asset"),
		"the released database holds no asset rows of its own")
	for _, row := range []struct {
		utxo   int64
		name   []byte
		amount string
	}{
		{utxoIDs[0], token, "5"},
		{utxoIDs[1], token, "7"},
		{utxoIDs[1], nil, "1"},
	} {
		_, err := legacy.Exec(
			`INSERT INTO asset (name, policy_id, fingerprint, utxo_id, amount)
VALUES (?, ?, ?, ?, ?)`,
			row.name, policy, []byte("asset1unused"), row.utxo, row.amount,
		)
		require.NoError(t, err)
	}
	require.NoError(t, legacy.Close())

	require.NoError(t, MigrateToIndexerDB(
		t.Context(), parityConfig(dir),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))
	n := startParityNode(t, "migrated", dir, previewNodeConfig(t),
		warnLogger(t))
	require.Eventually(t, func() bool {
		return n.ledgerSlot() == legacyParityTipSlot
	}, 2*time.Minute, 20*time.Millisecond,
		"the migrated node reaches the released tip")
	n.stop()

	indexer := openRaw(t, filepath.Join(
		dir, internalplugins.IndexerDirName, "metadata.sqlite",
	))
	var legacyTables int
	require.NoError(t, indexer.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' "+
			"AND name = 'asset'",
	).Scan(&legacyTables))
	require.Zero(t, legacyTables, "the asset table is dropped")
	got, err := indexer.Query(`
SELECT h.utxo_id, i.policy_id, i.name, typeof(i.name), h.amount
FROM asset_holding h JOIN asset_identity i ON i.id = h.asset_id
ORDER BY h.utxo_id, i.name`)
	require.NoError(t, err)
	type holding struct {
		utxo   int64
		policy []byte
		name   []byte
		amount string
	}
	var holdings []holding
	for got.Next() {
		var h holding
		var nameType string
		require.NoError(t, got.Scan(
			&h.utxo, &h.policy, &h.name, &nameType, &h.amount,
		))
		// The driver reads a zero-length blob as nil; the column type tells
		// the empty name from a NULL one.
		require.Equal(t, "blob", nameType)
		if h.name == nil {
			h.name = []byte{}
		}
		holdings = append(holdings, h)
	}
	require.NoError(t, got.Err())
	require.NoError(t, got.Close())
	require.Equal(t, []holding{
		{utxoIDs[0], policy, token, "5"},
		{utxoIDs[1], policy, []byte{}, "1"},
		{utxoIDs[1], policy, token, "7"},
	}, holdings)
	require.Equal(t, 2, rowCount(t, indexer, "asset_identity"))
}
