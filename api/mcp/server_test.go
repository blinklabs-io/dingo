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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenReadOnlySQLite(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "metadata.sqlite")

	// Missing file returns error
	_, err := OpenReadOnlySQLite(dbPath)
	require.Error(t, err)

	// Create a real SQLite database file
	initDB, err := sql.Open(
		"sqlite",
		"file:"+dbPath+"?_pragma=synchronous(OFF)",
	)
	require.NoError(t, err)
	_, err = initDB.Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY);")
	require.NoError(t, err)
	initDB.Close()

	// Open read-only SQLite connection
	roDB, err := OpenReadOnlySQLite(dbPath)
	require.NoError(t, err)
	defer roDB.Close()

	err = roDB.Ping()
	require.NoError(t, err)

	// Verify sqliteFileURI
	uri := sqliteFileURI("test/path.sqlite")
	assert.True(t, strings.HasPrefix(uri, "file://"))
}

func TestServerStopClosesDB(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)

	server, err := NewServer(
		DefaultProviderConfig(),
		ProviderDependencies{},
		apiconfig.EffectiveTLS{Enabled: false},
		"127.0.0.1:0",
	)
	require.NoError(t, err)
	server.openedDB = db

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = server.Stop(ctx)
	require.NoError(t, err)
	assert.Error(t, db.Ping(), "db should be closed after Server.Stop")
}

func TestNewMCPServerUsesActiveSQLiteProvider(t *testing.T) {
	t.Parallel()
	root, active := t.TempDir(), t.TempDir()
	stale, err := sql.Open(
		"sqlite",
		sqliteFileURI(
			filepath.Join(root, "metadata.sqlite"),
		)+"?_pragma=synchronous(OFF)&_pragma=journal_mode(MEMORY)",
	)
	require.NoError(t, err)
	_, err = stale.Exec("CREATE TABLE stale_marker(n)")
	require.NoError(t, err)
	require.NoError(t, stale.Close())
	nodeDB, err := dbtest.NewDatabaseWithOptions(
		t,
		dbtest.Options{
			Config: &database.Config{DataDir: root},
			Metadata: dbtest.StorageProvider{
				Config: map[string]any{"dataDir": active},
			},
		},
	)
	require.NoError(t, err)
	_, ro, err := NewMCPServer(
		DefaultProviderConfig(),
		ProviderDependencies{Database: nodeDB},
	)
	require.NoError(t, err)
	require.NotNil(t, ro)
	defer ro.Close()
	var n int
	require.NoError(t, ro.QueryRow("SELECT count(*) FROM epoch").Scan(&n))
	require.Error(t, ro.QueryRow("SELECT count(*) FROM stale_marker").Scan(&n))
	_, err = ro.Exec("CREATE TABLE forbidden(n)")
	require.Error(t, err)
}

func TestSQLiteConnectionBlocksWritesWithoutValidator(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	writer, err := sql.Open(
		"sqlite",
		sqliteFileURI(
			path,
		)+"?_pragma=synchronous(OFF)&_pragma=journal_mode(MEMORY)",
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	_, err = writer.Exec(
		"CREATE TABLE item (id INTEGER); INSERT INTO item VALUES (1)",
	)
	require.NoError(t, err)
	reader, err := OpenReadOnlySQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	for _, query := range []string{
		"WITH x AS (SELECT 1) DELETE FROM item",
		"WITH x AS (SELECT 1) UPDATE item SET id=2",
		"WITH x AS (SELECT 1) INSERT INTO item VALUES(2)",
	} {
		_, err := reader.Exec(query)
		require.Error(t, err)
	}
	var id int
	require.NoError(
		t,
		reader.QueryRow("WITH x AS (SELECT id FROM item) SELECT id FROM x").
			Scan(&id),
	)
	require.Equal(t, 1, id)
}

func TestMCPSessionEndToEnd(t *testing.T) {
	t.Parallel()

	db := newFixtureDB(t)

	cfg := DefaultProviderConfig()
	server, _, err := NewMCPServer(cfg, ProviderDependencies{
		SQLDB:   db,
		Network: "preview",
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientTransport, serverTransport := mcp.NewInMemoryTransports()

	// Connect server transport
	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	// Connect client transport
	client := mcp.NewClient(&mcp.Implementation{
		Name:    "test-client",
		Version: "1.0.0",
	}, nil)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Check Advertised Capabilities
	initRes := cs.InitializeResult()
	require.NotNil(t, initRes)
	require.NotNil(t, initRes.Capabilities)
	assert.NotNil(t, initRes.Capabilities.Tools)
	assert.True(t, initRes.Capabilities.Tools.ListChanged)
	assert.NotNil(t, initRes.Capabilities.Resources)
	assert.True(t, initRes.Capabilities.Resources.ListChanged)

	// 2. List Tools
	toolsRes, err := cs.ListTools(ctx, nil)
	require.NoError(t, err)
	toolNames := make(map[string]bool)
	for _, tool := range toolsRes.Tools {
		toolNames[tool.Name] = true
	}
	assert.True(t, toolNames["sqlite_query"])
	assert.True(t, toolNames["sqlite_explain"])
	assert.True(t, toolNames["sqlite_table_schema"])
	assert.True(t, toolNames["get_cardano_tip"])
	assert.True(t, toolNames["get_block"])
	assert.True(t, toolNames["get_transaction"])
	assert.True(t, toolNames["get_utxos"])
	assert.True(t, toolNames["get_account"])
	assert.True(t, toolNames["get_epoch_summary"])
	assert.True(t, toolNames["get_node_info"])
	assert.True(t, toolNames["get_protocol_parameters"])
	assert.True(t, toolNames["get_mempool_info"])
	assert.True(t, toolNames["decode_address"])
	assert.True(t, toolNames["get_governance_proposal"])
	assert.True(t, toolNames["calculate_min_utxo"])
	assert.Equal(t, 21, len(toolsRes.Tools))

	// 3. Call sqlite_query
	queryRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "sqlite_query",
		Arguments: map[string]any{
			"query": "SELECT id, hash, slot, epoch FROM blocks",
			"limit": 5,
		},
	})
	require.NoError(t, err)
	assert.False(t, queryRes.IsError)
	require.NotEmpty(t, queryRes.Content)
	textVal := queryRes.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, textVal, "blocks")
	assert.Contains(
		t,
		textVal,
		"0000000000000000000000000000000000000000000000000000000000000001",
	)

	// 4. Call sqlite_query with mutating statement (must return clean error inside text, IsError=true)
	mutRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "sqlite_query",
		Arguments: map[string]any{
			"query": "DROP TABLE blocks",
		},
	})
	require.NoError(t, err) // JSON-RPC does not fail
	assert.True(t, mutRes.IsError)
	assert.Contains(t, mutRes.Content[0].(*mcp.TextContent).Text, "forbidden")

	// 5. Call sqlite_explain
	expRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "sqlite_explain",
		Arguments: map[string]any{
			"query": "SELECT * FROM blocks WHERE id = 1",
		},
	})
	require.NoError(t, err)
	assert.False(t, expRes.IsError)
	assert.Contains(t, expRes.Content[0].(*mcp.TextContent).Text, "SEARCH")

	// 6. Call sqlite_table_schema
	schemaRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "sqlite_table_schema",
		Arguments: map[string]any{
			"table_name": "blocks",
		},
	})
	require.NoError(t, err)
	assert.False(t, schemaRes.IsError)
	assert.Contains(t, schemaRes.Content[0].(*mcp.TextContent).Text, "hash")

	// 7. Call Cardano domain tools
	// 7a. get_block
	blockRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_block",
		Arguments: map[string]any{
			"hash_or_slot": "1000",
		},
	})
	require.NoError(t, err)
	assert.False(t, blockRes.IsError)
	assert.Contains(t, blockRes.Content[0].(*mcp.TextContent).Text, "1000")

	// 7a2. get_block by hash
	blockByHashRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_block",
		Arguments: map[string]any{
			"hash_or_slot": "0000000000000000000000000000000000000000000000000000000000000001",
		},
	})
	require.NoError(t, err)
	assert.False(t, blockByHashRes.IsError)
	assert.Contains(
		t,
		blockByHashRes.Content[0].(*mcp.TextContent).Text,
		"1000",
	)

	// 7b. get_transaction
	txRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_transaction",
		Arguments: map[string]any{
			"tx_hash": "1111111111111111111111111111111111111111111111111111111111111111",
		},
	})
	require.NoError(t, err)
	assert.False(t, txRes.IsError)
	assert.Contains(t, txRes.Content[0].(*mcp.TextContent).Text, "175000")

	// 7c. get_utxos
	utxoRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_utxos",
		Arguments: map[string]any{
			"address_or_credential": "addr_test1vrm9x2zs...sample",
		},
	})
	require.NoError(t, err)
	assert.False(t, utxoRes.IsError)
	assert.Contains(t, utxoRes.Content[0].(*mcp.TextContent).Text, "5000000")

	// 7d. get_account
	accRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_account",
		Arguments: map[string]any{
			"stake_address_or_credential": "stake_test1uq...sample",
		},
	})
	require.NoError(t, err)
	assert.False(t, accRes.IsError)
	assert.Contains(t, accRes.Content[0].(*mcp.TextContent).Text, "pool1sample")

	// 7e. get_epoch_summary
	epochRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_epoch_summary",
		Arguments: map[string]any{
			"epoch": 10,
		},
	})
	require.NoError(t, err)
	assert.False(t, epochRes.IsError)
	assert.Contains(t, epochRes.Content[0].(*mcp.TextContent).Text, "preview")

	// 7e2. get_epoch_summary default (nil epoch)
	epochDefRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_epoch_summary",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.False(t, epochDefRes.IsError)

	// 8. List Resources
	resList, err := cs.ListResources(ctx, nil)
	require.NoError(t, err)
	resourceURIs := make(map[string]bool)
	for _, r := range resList.Resources {
		resourceURIs[r.URI] = true
	}
	assert.True(t, resourceURIs["dingo://dbsync/cheatsheet"])
	assert.True(t, resourceURIs["dingo://schema/tables"])
	assert.True(t, resourceURIs["dingo://node/status"])

	// 9. Read Resource dingo://dbsync/cheatsheet
	cheatSheetRes, err := cs.ReadResource(ctx, &mcp.ReadResourceParams{
		URI: "dingo://dbsync/cheatsheet",
	})
	require.NoError(t, err)
	require.NotEmpty(t, cheatSheetRes.Contents)
	assert.Contains(t, cheatSheetRes.Contents[0].Text, "cardano-db-sync")
	assert.Contains(t, cheatSheetRes.Contents[0].Text, "SQLite WAL")

	// 10. Read Resource dingo://schema/tables
	tablesRes, err := cs.ReadResource(ctx, &mcp.ReadResourceParams{
		URI: "dingo://schema/tables",
	})
	require.NoError(t, err)
	require.NotEmpty(t, tablesRes.Contents)
	assert.Contains(t, tablesRes.Contents[0].Text, "transaction")
	assert.Contains(t, tablesRes.Contents[0].Text, "utxo")

	// 11. Read Resource dingo://schema/table/blocks
	tableSchemaRes, err := cs.ReadResource(ctx, &mcp.ReadResourceParams{
		URI: "dingo://schema/table/blocks",
	})
	require.NoError(t, err)
	require.NotEmpty(t, tableSchemaRes.Contents)
	assert.Contains(t, tableSchemaRes.Contents[0].Text, "CREATE TABLE blocks")

	// 12. Read Resource dingo://schema/table/nonexistent
	_, err = cs.ReadResource(ctx, &mcp.ReadResourceParams{
		URI: "dingo://schema/table/nonexistent",
	})
	require.Error(t, err)

	// 13. Read Resource dingo://node/status
	statusRes, err := cs.ReadResource(ctx, &mcp.ReadResourceParams{
		URI: "dingo://node/status",
	})
	require.NoError(t, err)
	require.NotEmpty(t, statusRes.Contents)
	assert.Contains(t, statusRes.Contents[0].Text, "Dingo Node Status")

	// 14. Error/Not-Found Cases for Cardano Tools
	// 14a. get_block not found
	nfBlock, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_block",
		Arguments: map[string]any{
			"hash_or_slot": "99999999",
		},
	})
	require.NoError(t, err)
	assert.True(t, nfBlock.IsError)

	// 14b. get_transaction not found
	nfTx, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_transaction",
		Arguments: map[string]any{
			"tx_hash": "0000000000000000000000000000000000000000000000000000000000000000",
		},
	})
	require.NoError(t, err)
	assert.True(t, nfTx.IsError)

	// 14c. get_utxos not found
	nfUtxos, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_utxos",
		Arguments: map[string]any{
			"address_or_credential": "addr_test1nonexistent",
		},
	})
	require.NoError(t, err)
	assert.False(t, nfUtxos.IsError)
	assert.Contains(
		t,
		nfUtxos.Content[0].(*mcp.TextContent).Text,
		"Showing 0 results",
	)

	// 14d. get_account not found
	nfAcc, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_account",
		Arguments: map[string]any{
			"stake_address_or_credential": "stake_test1nonexistent",
		},
	})
	require.NoError(t, err)
	assert.True(t, nfAcc.IsError)

	// 14e. get_epoch_summary with no recorded blocks for epoch
	nfEpoch, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_epoch_summary",
		Arguments: map[string]any{
			"epoch": 999999,
		},
	})
	require.NoError(t, err)
	assert.True(t, nfEpoch.IsError)
	assert.Contains(
		t,
		nfEpoch.Content[0].(*mcp.TextContent).Text,
		"read epoch boundaries",
	)

	// 14f. sqlite_table_schema not found
	nfSchema, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "sqlite_table_schema",
		Arguments: map[string]any{
			"table_name": "nonexistent_table",
		},
	})
	require.NoError(t, err)
	assert.True(t, nfSchema.IsError)

	// 14g. sqlite_query with multi-statement injection
	injRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "sqlite_query",
		Arguments: map[string]any{
			"query": "SELECT 1; DROP TABLE blocks;",
		},
	})
	require.NoError(t, err)
	assert.True(t, injRes.IsError)
}

func TestRegisterProvider(t *testing.T) {
	t.Parallel()

	host := plugin.NewHost()
	err := RegisterProvider(host)
	require.NoError(t, err)

	// Verify plugin description was registered
	descs := host.Providers()
	found := false
	for _, d := range descs {
		if d.Capability == plugin.CapabilityAPIMcp && d.Name == "builtin" {
			found = true
			break
		}
	}
	assert.True(t, found, "builtin MCP provider should be registered")

	// Instantiate through plugin.Resolve
	db := newFixtureDB(t)
	srv, err := plugin.Resolve[*Server](
		context.Background(),
		host,
		plugin.CapabilityAPIMcp,
		"builtin",
		map[string]any{"port": 0},
		ProviderDependencies{SQLDB: db, Host: "127.0.0.1", Network: "preview"},
	)
	require.NoError(t, err)
	require.NotNil(t, srv)

	// RegisterProvider with nil host should fail
	require.Error(t, RegisterProvider(nil))
}

func TestMCPDefaultRequiresOptIn(t *testing.T) {
	t.Parallel()
	require.Zero(t, DefaultProviderConfig().Port)
	require.Equal(t, "127.0.0.1", DefaultProviderConfig().Host)
}

func TestValidateListenSecurity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		host      string
		token     string
		tls       bool
		wantError bool
	}{
		{name: "ipv4 loopback", host: "127.0.0.1"},
		{name: "ipv6 loopback", host: "::1"},
		{name: "localhost", host: "localhost"},
		{name: "public without protection", host: "0.0.0.0", wantError: true},
		{name: "public without TLS", host: "192.0.2.1", token: "secret", wantError: true},
		{name: "public without token", host: "192.0.2.1", tls: true, wantError: true},
		{name: "public protected", host: "192.0.2.1", token: "secret", tls: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateListenSecurity(tc.host, tc.token, tc.tls)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestServerHealthEndpoint(t *testing.T) {
	t.Parallel()

	cfg := DefaultProviderConfig()
	server, err := NewServer(
		cfg,
		ProviderDependencies{},
		apiconfig.EffectiveTLS{},
		"127.0.0.1:0",
	)
	require.NoError(t, err)

	req := httptest.NewRequest("GET", "/healthz", nil)
	rec := httptest.NewRecorder()
	server.handler().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	var body map[string]string
	err = json.Unmarshal(rec.Body.Bytes(), &body)
	require.NoError(t, err)
	assert.Equal(t, "ok", body["status"])
	assert.Equal(t, "dingo-mcp", body["service"])
}

func TestServerStartStopLifecycle(t *testing.T) {
	t.Parallel()

	db := newFixtureDB(t)

	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := occupied.Addr().String()
	require.NoError(t, occupied.Close())

	cfg := DefaultProviderConfig()
	server, err := NewServer(cfg, ProviderDependencies{
		SQLDB:   db,
		Network: "preview",
	}, apiconfig.EffectiveTLS{}, addr)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Start(ctx)
	require.NoError(t, err)

	// Second Start call should error
	err = server.Start(ctx)
	require.Error(t, err)

	// Query HTTP healthz endpoint
	resp, err := http.Get(fmt.Sprintf("http://%s/healthz", addr))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// Query root endpoint
	respRoot, err := http.Get(fmt.Sprintf("http://%s/", addr))
	require.NoError(t, err)
	respRoot.Body.Close()

	// Query not found path
	resp404, err := http.Get(fmt.Sprintf("http://%s/random_path", addr))
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp404.StatusCode)
	resp404.Body.Close()

	// Graceful stop
	err = server.Stop(ctx)
	require.NoError(t, err)
}

func TestSSESessionSurvivesIdleInterval(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("exercises the former 60-second SSE deadline")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	server, err := NewServer(
		DefaultProviderConfig(),
		ProviderDependencies{},
		apiconfig.EffectiveTLS{},
		addr,
	)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 75*time.Second)
	defer cancel()
	require.NoError(t, server.Start(ctx))
	defer func() {
		stopCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		require.NoError(t, server.Stop(stopCtx))
	}()
	client := mcp.NewClient(
		&mcp.Implementation{Name: "test", Version: "1"},
		nil,
	)
	session, err := client.Connect(
		ctx,
		&mcp.SSEClientTransport{Endpoint: "http://" + addr + "/sse"},
		nil,
	)
	require.NoError(t, err)
	defer session.Close()
	_, err = session.ListTools(ctx, nil)
	require.NoError(t, err)
	// This timer advances the scenario beyond the former deadline; it is
	// not used to synchronize goroutines or wait for readiness.
	timer := time.NewTimer(61 * time.Second)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_, err = session.ListTools(ctx, nil)
	require.NoError(t, err)
}
