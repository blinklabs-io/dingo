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

package mcp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const assetInfoLimitPolicy = "b0000000000000000000000000000000000000000000000000000003"

func newAssetInfoLimitSession(
	t *testing.T,
	limits *readLimits,
	assetRows int,
) *mcp.ClientSession {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`
		CREATE TABLE token_registry_entry (
			subject TEXT PRIMARY KEY, name TEXT, ticker TEXT,
			description TEXT, url TEXT, decimals INTEGER);
		CREATE TABLE utxo (
			id INTEGER PRIMARY KEY AUTOINCREMENT, tx_id BLOB NOT NULL,
			deleted_slot INTEGER NOT NULL DEFAULT 0);
		CREATE TABLE asset (
			id INTEGER PRIMARY KEY AUTOINCREMENT, utxo_id INTEGER NOT NULL,
			policy_id BLOB NOT NULL, name BLOB NOT NULL, amount TEXT NOT NULL);
		CREATE TABLE asset_mint_burn (
			tx_hash BLOB NOT NULL, policy_id BLOB NOT NULL, name BLOB NOT NULL,
			fingerprint BLOB, slot INTEGER NOT NULL, quantity TEXT NOT NULL,
			tx_index INTEGER NOT NULL DEFAULT 0);
		CREATE TABLE "transaction" (
			id INTEGER PRIMARY KEY AUTOINCREMENT, hash BLOB NOT NULL);
		CREATE TABLE transaction_metadata_label (
			transaction_id INTEGER NOT NULL, label TEXT NOT NULL, json_value TEXT);
	`)
	require.NoError(t, err)
	policy, err := hex.DecodeString(assetInfoLimitPolicy)
	require.NoError(t, err)
	for i := 1; i <= assetRows; i++ {
		_, err = db.Exec(
			`INSERT INTO utxo (id, tx_id, deleted_slot) VALUES (?, ?, 0)`,
			i, []byte{byte(i)},
		)
		require.NoError(t, err)
		_, err = db.Exec(
			`INSERT INTO asset (utxo_id, policy_id, name, amount) VALUES (?, ?, ?, '10')`,
			i,
			policy,
			[]byte("T"),
		)
		require.NoError(t, err)
	}

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"}, nil,
	)
	registerCardanoTools(
		server, db, nil, nil, "preview", 5*time.Second, limits,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	ct, st := mcp.NewInMemoryTransports()
	_, err = server.Connect(ctx, st, nil)
	require.NoError(t, err)
	cs, err := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"}, nil,
	).Connect(ctx, ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func callAssetInfo(
	t *testing.T,
	cs *mcp.ClientSession,
) (*mcp.CallToolResult, error) {
	t.Helper()
	return cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "get_asset_info",
		Arguments: map[string]any{"policy_id": assetInfoLimitPolicy},
	})
}

func TestGetAssetInfoRowCap(t *testing.T) {
	t.Parallel()

	limits := newReadLimits()
	limits.maxAssetRows = 5

	t.Run("over cap is an explicit incomplete result", func(t *testing.T) {
		t.Parallel()
		cs := newAssetInfoLimitSession(t, limits, 6)
		res, err := callAssetInfo(t, cs)
		require.NoError(t, err)
		require.True(t, res.IsError, "over-cap policy must not return a result")
		text := res.Content[0].(*mcp.TextContent).Text
		assert.Contains(t, text, "incomplete")
		assert.NotContains(t, text, "Circulating Supply")
	})

	t.Run("at cap is complete", func(t *testing.T) {
		t.Parallel()
		cs := newAssetInfoLimitSession(t, limits, 5)
		res, err := callAssetInfo(t, cs)
		require.NoError(t, err)
		require.False(t, res.IsError)
		text := res.Content[0].(*mcp.TextContent).Text
		assert.True(t, strings.Contains(text, "50 units"), text)
	})
}

func TestGetAssetInfoSharesConcurrencyLimit(t *testing.T) {
	t.Parallel()

	limits := newReadLimits()
	cs := newAssetInfoLimitSession(t, limits, 1)

	held := 0
	for limits.acquire() {
		held++
	}
	require.Equal(t, maxConcurrentSQLiteQueries, held)

	refused, err := callAssetInfo(t, cs)
	require.NoError(t, err)
	require.True(t, refused.IsError)
	assert.Contains(
		t,
		refused.Content[0].(*mcp.TextContent).Text,
		"too many concurrent",
	)

	limits.release()
	res, err := callAssetInfo(t, cs)
	require.NoError(t, err)
	assert.False(t, res.IsError)
	// The call must give its slot back.
	require.True(t, limits.acquire())
}

func TestGetAssetInfoRowCapWithAssetName(t *testing.T) {
	t.Parallel()

	limits := newReadLimits()
	limits.maxAssetRows = 5
	cs := newAssetInfoLimitSession(t, limits, 6)
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "get_asset_info",
		Arguments: map[string]any{
			"policy_id":  assetInfoLimitPolicy,
			"asset_name": "T",
		},
	})
	require.NoError(t, err)
	require.True(t, res.IsError, "over-cap asset must not return a result")
	text := res.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, text, "incomplete")
	// The caller already narrowed by name, so the error must not tell
	// them to do it again.
	assert.NotContains(t, text, "Specify asset_name")
}
