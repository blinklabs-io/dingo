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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mcp

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func newFixtureDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)

	_, err = db.Exec(`
		CREATE TABLE "transaction" (id INTEGER PRIMARY KEY, slot INTEGER, hash BLOB, block_hash BLOB, fee INTEGER, block_index INTEGER);
        CREATE TABLE epoch (epoch_id INTEGER, start_slot INTEGER, length_in_slots INTEGER, nonce BLOB);
		INSERT INTO epoch VALUES (10, 1000, 100, NULL);
		CREATE TABLE epoch_summary (epoch INTEGER, epoch_nonce BLOB);
		CREATE TABLE block_nonce (slot INTEGER);
		CREATE TABLE blocks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			hash TEXT NOT NULL UNIQUE,
			slot INTEGER NOT NULL,
			epoch INTEGER NOT NULL,
			height INTEGER NOT NULL,
			time INTEGER NOT NULL
		);
		CREATE TABLE tx (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			hash TEXT NOT NULL UNIQUE,
			block_id INTEGER NOT NULL,
			slot INTEGER NOT NULL,
			fee INTEGER NOT NULL,
			size INTEGER NOT NULL,
			valid_contract INTEGER NOT NULL DEFAULT 1
		);
		CREATE TABLE utxo (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tx_hash TEXT NOT NULL,
			tx_index INTEGER NOT NULL,
			address TEXT NOT NULL,
		value INTEGER NOT NULL,
		amount TEXT,
		transaction_id INTEGER,
		tx_id BLOB
		);
		CREATE TABLE account (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			stake_address TEXT NOT NULL UNIQUE,
			controlled_amount INTEGER NOT NULL DEFAULT 0,
			rewards_sum INTEGER NOT NULL DEFAULT 0,
			withdrawals_sum INTEGER NOT NULL DEFAULT 0,
			pool_id TEXT
		);

		INSERT INTO blocks (hash, slot, epoch, height, time)
		VALUES ('0000000000000000000000000000000000000000000000000000000000000001', 1000, 10, 100, 1700000000);

		INSERT INTO tx (hash, block_id, slot, fee, size)
		VALUES ('1111111111111111111111111111111111111111111111111111111111111111', 1, 1000, 175000, 450);
 INSERT INTO "transaction"(id,slot,hash,fee,block_index) SELECT id,slot,hash,fee,0 FROM tx;

		INSERT INTO utxo (tx_hash, tx_index, address, value)
		VALUES ('1111111111111111111111111111111111111111111111111111111111111111', 0, 'addr_test1vrm9x2zs...sample', 5000000);
		UPDATE utxo SET amount='5000000',transaction_id=1;

		INSERT INTO account (stake_address, controlled_amount, rewards_sum, withdrawals_sum, pool_id)
		VALUES ('stake_test1uq...sample', 10000000, 250000, 0, 'pool1sample');
	`)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func newSchemaDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	schema, err := os.ReadFile(
		"../../database/plugin/metadata/sqlstore/queries/sqlite/schema.sql",
	)
	require.NoError(t, err)
	_, err = db.Exec(string(schema))
	require.NoError(t, err)
	return db
}

func newToolSession(
	t *testing.T,
	db *sql.DB,
	maxRows int,
	nodeDB ...*database.Database,
) *mcp.ClientSession {
	t.Helper()
	return newToolSessionWithTimeout(t, db, maxRows, time.Second, nodeDB...)
}

func newToolSessionWithTimeout(
	t *testing.T,
	db *sql.DB,
	maxRows int,
	timeout time.Duration,
	nodeDB ...*database.Database,
) *mcp.ClientSession {
	t.Helper()
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1"},
		nil,
	)
	var lookup utxoAddressLookup
	if len(nodeDB) > 0 && nodeDB[0] != nil {
		lookup = nodeDB[0].UtxosByAddressPage
	}
	RegisterCardanoTools(
		server,
		db,
		nil,
		nil,
		"preview",
		timeout,
		lookup)
	RegisterSQLiteTools(server, db, time.Second, maxRows)
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ss, err := server.Connect(t.Context(), serverTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func callTool(
	t *testing.T,
	cs *mcp.ClientSession,
	name string,
	args map[string]any,
	wantError bool,
) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := cs.CallTool(
		ctx,
		&mcp.CallToolParams{Name: name, Arguments: args},
	)
	require.NoError(t, err)
	require.Equal(t, wantError, result.IsError, "%v", result.Content)
	require.NotEmpty(t, result.Content)
	return result.Content[0].(*mcp.TextContent).Text
}
