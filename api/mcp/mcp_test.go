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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/btcsuite/btcd/btcutil/bech32"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)

	_, err = db.Exec(`
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
			value INTEGER NOT NULL
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

		INSERT INTO utxo (tx_hash, tx_index, address, value)
		VALUES ('1111111111111111111111111111111111111111111111111111111111111111', 0, 'addr_test1vrm9x2zs...sample', 5000000);

		INSERT INTO account (stake_address, controlled_amount, rewards_sum, withdrawals_sum, pool_id)
		VALUES ('stake_test1uq...sample', 10000000, 250000, 0, 'pool1sample');
	`)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func TestSanitizeText(t *testing.T) {
	t.Parallel()

	// Zero-width and RTL override stripping
	malicious := "Hello\u202EWorld\u200BTest\uFEFF!"
	sanitized := SanitizeExternalString(malicious)
	assert.NotContains(t, sanitized, "\u202E")
	assert.NotContains(t, sanitized, "\u200B")
	assert.NotContains(t, sanitized, "\uFEFF")
	assert.Contains(t, sanitized, "HelloWorldTest!")

	// Markdown pipe and newline escaping in cell
	cell := "First\nSecond|Third"
	formatted := FormatCell(cell)
	assert.NotContains(t, formatted, "\n")
	assert.Contains(t, formatted, `\|`)

	// External string wrapper
	tagged := WrapUntrustedChainData("evil prompt injection")
	assert.True(t, strings.HasPrefix(tagged, "<untrusted_chain_data>"))
	assert.True(t, strings.HasSuffix(tagged, "</untrusted_chain_data>"))
}

func TestValidateReadOnlyQuery(t *testing.T) {
	t.Parallel()

	validQueries := []string{
		"SELECT * FROM blocks",
		"select id, hash from tx where slot > 100",
		"EXPLAIN QUERY PLAN SELECT * FROM blocks",
		"explain select 1",
		"PRAGMA table_info('blocks')",
		"pragma index_list('tx')",
		"pragma table_info ( 'utxo' )",
	}
	for _, q := range validQueries {
		assert.NoError(
			t,
			ValidateReadOnlyQuery(q),
			"query should be valid: %s",
			q,
		)
	}

	invalidQueries := []string{
		"",
		"INSERT INTO blocks (hash) VALUES ('abc')",
		"UPDATE blocks SET height = 0",
		"DELETE FROM tx",
		"DROP TABLE blocks",
		"ALTER TABLE blocks ADD COLUMN foo TEXT",
		"CREATE TABLE evil (id INT)",
		"SELECT 1; DROP TABLE blocks;",
		"PRAGMA writable_schema=ON",
		"ATTACH DATABASE 'other.db' AS other",
	}
	for _, q := range invalidQueries {
		assert.Error(
			t,
			ValidateReadOnlyQuery(q),
			"query should be rejected: %s",
			q,
		)
	}
}

func TestSecurityMiddleware_Auth(t *testing.T) {
	t.Parallel()

	nextHandler := http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		},
	)

	// Case 1: No auth required
	unprotected := SecurityMiddleware("", 0, 0, nil, nextHandler)
	req := httptest.NewRequest("GET", "/mcp", nil)
	rec := httptest.NewRecorder()
	unprotected.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Case 2: Auth required
	token := "my-secret-test-token"
	protected := SecurityMiddleware(token, 0, 0, nil, nextHandler)

	// Request with missing token
	req = httptest.NewRequest("GET", "/mcp", nil)
	rec = httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// Request with wrong token
	req = httptest.NewRequest("GET", "/mcp", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec = httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// Request with correct Bearer token
	req = httptest.NewRequest("GET", "/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec = httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Request with correct X-API-Key token
	req = httptest.NewRequest("GET", "/mcp", nil)
	req.Header.Set("X-API-Key", token)
	rec = httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestSecurityMiddleware_RateLimiting(t *testing.T) {
	t.Parallel()

	nextHandler := http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
	)

	// Rate limit: 1 rps, burst: 2
	limited := SecurityMiddleware("", 1.0, 2, nil, nextHandler)

	// First 2 requests (within burst) should succeed
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/mcp", nil)
		req.RemoteAddr = "192.0.2.1:1234"
		rec := httptest.NewRecorder()
		limited.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	}

	// Third immediate request should be rate-limited (429)
	req := httptest.NewRequest("GET", "/mcp", nil)
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	limited.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusTooManyRequests, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("Retry-After"))
}

func TestSecurityMiddleware_CORS(t *testing.T) {
	t.Parallel()

	nextHandler := http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
	)

	handler := SecurityMiddleware(
		"",
		0,
		0,
		[]string{"http://localhost:5173"},
		nextHandler,
	)

	// Preflight OPTIONS
	req := httptest.NewRequest("OPTIONS", "/mcp", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(
		t,
		"http://localhost:5173",
		rec.Header().Get("Access-Control-Allow-Origin"),
	)
	assert.Contains(t, rec.Header().Get("Access-Control-Allow-Methods"), "POST")
}

func TestServerHealthEndpoint(t *testing.T) {
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

func TestMCPSession_EndToEnd(t *testing.T) {
	db := setupTestDB(t)

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
	assert.False(t, nfEpoch.IsError)
	assert.Contains(
		t,
		nfEpoch.Content[0].(*mcp.TextContent).Text,
		"Recorded Blocks**: 0",
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

func TestOpenReadOnlySQLite(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "metadata.sqlite")

	// Missing file returns error
	_, err := OpenReadOnlySQLite(dbPath)
	require.Error(t, err)

	// Create a real SQLite database file
	initDB, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=synchronous(OFF)")
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

	// Test NewMCPServer with DataDir pointing to metadata.sqlite
	serverWithDir, openedDB, err := NewMCPServer(
		DefaultProviderConfig(),
		ProviderDependencies{
			DataDir: tmpDir,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, serverWithDir)
	require.NotNil(t, openedDB)
	_ = openedDB.Close()
}

func TestFormatCell(t *testing.T) {
	assert.Equal(t, "NULL", FormatCell(nil))
	assert.Equal(t, "123", FormatCell(int64(123)))
	assert.Equal(t, "12.34", FormatCell(float64(12.34)))
	assert.Equal(t, "true", FormatCell(true))
	assert.Equal(t, "false", FormatCell(false))
	assert.Equal(t, "0x", FormatCell([]byte{}))
	assert.Equal(t, "0x68656c6c6f", FormatCell([]byte("hello")))
	assert.Equal(t, "0xdeadbeef", FormatCell([]byte{0xde, 0xad, 0xbe, 0xef}))

	// Large blob preview
	largeBlob := make([]byte, 100)
	for i := range largeBlob {
		largeBlob[i] = byte(i)
	}
	blobFormatted := FormatCell(largeBlob)
	assert.Contains(t, blobFormatted, "bytes: 100")
	assert.Contains(t, blobFormatted, "...")

	// String truncation
	longString := strings.Repeat("x", 200)
	assert.Equal(t, strings.Repeat("x", 128)+"...", FormatCell(longString))

	// Markdown pipe and newline escaping
	assert.Equal(t, "a \\| b c", FormatCell("a | b\nc"))

	// Large default type truncation
	largeDefault := strings.Repeat("z", 200)
	formattedDefault := FormatCell([]rune(largeDefault))
	assert.True(t, strings.HasSuffix(formattedDefault, "..."))
	assert.Equal(t, 128+3, len(formattedDefault))
}

func TestFormatMarkdownTable(t *testing.T) {
	assert.Equal(t, "*(empty result)*\n", FormatMarkdownTable(nil, nil))
	assert.Equal(t, "*(empty result)*\n", FormatMarkdownTable([]string{}, nil))

	tbl := FormatMarkdownTable([]string{"ColA", "ColB"}, [][]string{{"1", "2"}})
	assert.Contains(t, tbl, "| ColA | ColB |")
	assert.Contains(t, tbl, "| 1 | 2 |")
}

func TestFormatHash(t *testing.T) {
	assert.Equal(t, "<nil>", formatHash(nil))
	assert.Equal(t, "abc", formatHash("abc"))
	assert.Equal(t, "0102", formatHash([]byte{1, 2}))
	assert.Equal(t, "123", formatHash(123))
}

func TestRegisterProvider(t *testing.T) {
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
	db := setupTestDB(t)
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

func TestServer_StartStopLifecycle(t *testing.T) {
	db := setupTestDB(t)

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

func TestRegisterResources_NilDB(t *testing.T) {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1.0"},
		nil,
	)
	RegisterResources(server, nil, nil, "preview")

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// Reading status with nil DB and nil LedgerState returns zero status
	res, err := cs.ReadResource(
		ctx,
		&mcp.ReadResourceParams{URI: "dingo://node/status"},
	)
	require.NoError(t, err)
	assert.Contains(t, res.Contents[0].Text, "Dingo Node Status")
	assert.Contains(t, res.Contents[0].Text, "Tip Slot**: `0`")

	// Reading tables with nil DB returns catalog
	tablesRes, err := cs.ReadResource(
		ctx,
		&mcp.ReadResourceParams{URI: "dingo://schema/tables"},
	)
	require.NoError(t, err)
	assert.Contains(
		t,
		tablesRes.Contents[0].Text,
		"Dingo SQLite Database Tables Catalog",
	)

	// Reading dingo://docs/identity returns identity content
	idRes, err := cs.ReadResource(
		ctx,
		&mcp.ReadResourceParams{URI: "dingo://docs/identity"},
	)
	require.NoError(t, err)
	assert.Contains(t, idRes.Contents[0].Text, "Protocol Minor Version 69")

	// Reading dingo://docs/faq returns FAQ content
	faqRes, err := cs.ReadResource(
		ctx,
		&mcp.ReadResourceParams{URI: "dingo://docs/faq"},
	)
	require.NoError(t, err)
	assert.Contains(t, faqRes.Contents[0].Text, "Frequently Asked Questions")

	// Reading dingo://docs/architecture returns architecture content
	archRes, err := cs.ReadResource(
		ctx,
		&mcp.ReadResourceParams{URI: "dingo://docs/architecture"},
	)
	require.NoError(t, err)
	assert.Contains(t, archRes.Contents[0].Text, "Dingo Node Architecture")

	// Reading table schema with nil DB returns error
	_, err = cs.ReadResource(
		ctx,
		&mcp.ReadResourceParams{URI: "dingo://schema/table/blocks"},
	)
	require.Error(t, err)
}

func TestRegisterCardanoTools_NilDB(t *testing.T) {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, nil, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// get_node_info works without DB
	infoRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_node_info",
		Arguments: map[string]any{"topic": "identity"},
	})
	require.NoError(t, err)
	assert.False(t, infoRes.IsError)
	assert.Contains(t, infoRes.Content[0].(*mcp.TextContent).Text, "Protocol Minor Version 69")

	faqToolRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_node_info",
		Arguments: map[string]any{"topic": "faq"},
	})
	require.NoError(t, err)
	assert.False(t, faqToolRes.IsError)
	assert.Contains(t, faqToolRes.Content[0].(*mcp.TextContent).Text, "Frequently Asked Questions")

	archToolRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_node_info",
		Arguments: map[string]any{"topic": "architecture"},
	})
	require.NoError(t, err)
	assert.False(t, archToolRes.IsError)
	assert.Contains(t, archToolRes.Content[0].(*mcp.TextContent).Text, "Dingo Node Architecture")

	defaultToolRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_node_info",
	})
	require.NoError(t, err)
	assert.False(t, defaultToolRes.IsError)
	assert.Contains(t, defaultToolRes.Content[0].(*mcp.TextContent).Text, "Dingo Node Information")

	// get_cardano_tip with nil db returns error
	tipRes, err := cs.CallTool(
		ctx,
		&mcp.CallToolParams{Name: "get_cardano_tip"},
	)
	require.NoError(t, err)
	assert.True(t, tipRes.IsError)

	// get_block with nil db returns error
	blockRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_block",
		Arguments: map[string]any{"hash_or_slot": "1000"},
	})
	require.NoError(t, err)
	assert.True(t, blockRes.IsError)

	// get_transaction with nil db returns error
	txRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_transaction",
		Arguments: map[string]any{
			"tx_hash": "0000000000000000000000000000000000000000000000000000000000000001",
		},
	})
	require.NoError(t, err)
	assert.True(t, txRes.IsError)

	// get_utxos with nil db returns error
	utxoRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_utxos",
		Arguments: map[string]any{"address_or_credential": "addr_test123"},
	})
	require.NoError(t, err)
	assert.True(t, utxoRes.IsError)

	// get_account with nil db returns error
	accRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_account",
		Arguments: map[string]any{
			"stake_address_or_credential": "stake_test123",
		},
	})
	require.NoError(t, err)
	assert.True(t, accRes.IsError)

	// get_epoch_summary with nil db returns error
	epochRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_epoch_summary",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.True(t, epochRes.IsError)
}

func TestCardanoTools_Fallbacks(t *testing.T) {
	// Database with tip table and account with credential column
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE tip (id INTEGER PRIMARY KEY, hash TEXT, slot INTEGER, block_number INTEGER);
		INSERT INTO tip VALUES (1, '0000000000000000000000000000000000000000000000000000000000000055', 5555, 55);

		CREATE TABLE account (credential TEXT, balance INTEGER, pool_id TEXT);
		INSERT INTO account VALUES ('cred_abc123', 9999999, 'pool_preview');

		CREATE TABLE epoch_summary (epoch INTEGER, blocks INTEGER);
		INSERT INTO epoch_summary VALUES (42, 2160);

		CREATE TABLE block_nonce (epoch INTEGER, nonce BLOB);
		INSERT INTO block_nonce VALUES (43, X'abcdef123456');
	`)
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, db, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. get_cardano_tip from tip table (source: metadata_tip)
	tipRes, err := cs.CallTool(
		ctx,
		&mcp.CallToolParams{Name: "get_cardano_tip"},
	)
	require.NoError(t, err)
	assert.False(t, tipRes.IsError)
	assert.Contains(
		t,
		tipRes.Content[0].(*mcp.TextContent).Text,
		"metadata_tip",
	)
	assert.Contains(t, tipRes.Content[0].(*mcp.TextContent).Text, "5555")

	// 2. get_block by slot from tip table
	blockSlotRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_block",
		Arguments: map[string]any{"hash_or_slot": "5555"},
	})
	require.NoError(t, err)
	assert.False(t, blockSlotRes.IsError)
	assert.Contains(t, blockSlotRes.Content[0].(*mcp.TextContent).Text, "5555")

	// 3. get_block by hash from tip table
	blockHashRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_block",
		Arguments: map[string]any{
			"hash_or_slot": "0000000000000000000000000000000000000000000000000000000000000055",
		},
	})
	require.NoError(t, err)
	assert.False(t, blockHashRes.IsError)
	assert.Contains(t, blockHashRes.Content[0].(*mcp.TextContent).Text, "5555")

	// 4. get_account by credential
	accRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_account",
		Arguments: map[string]any{"stake_address_or_credential": "cred_abc123"},
	})
	require.NoError(t, err)
	assert.False(t, accRes.IsError)
	assert.Contains(t, accRes.Content[0].(*mcp.TextContent).Text, "cred_abc123")
	assert.Contains(
		t,
		accRes.Content[0].(*mcp.TextContent).Text,
		"pool_preview",
	)

	// 5. get_epoch_summary from epoch_summary fallback
	epochRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_epoch_summary",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.False(t, epochRes.IsError)
	assert.Contains(t, epochRes.Content[0].(*mcp.TextContent).Text, "42")

	// 6. get_epoch_summary with nonce from block_nonce
	epochNonceRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_epoch_summary",
		Arguments: map[string]any{"epoch": 43},
	})
	require.NoError(t, err)
	assert.False(t, epochNonceRes.IsError)
	assert.Contains(
		t,
		epochNonceRes.Content[0].(*mcp.TextContent).Text,
		"abcdef123456",
	)
}

func TestCardanoTools_TransactionFallback(t *testing.T) {
	// Database with transaction table (no tip, no blocks)
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE "transaction" (id INTEGER PRIMARY KEY, hash BLOB, block_hash BLOB, slot INTEGER, fee INTEGER, block_index INTEGER);
		INSERT INTO "transaction" VALUES (1, X'1111111111111111111111111111111111111111111111111111111111111111', X'1111111111111111111111111111111111111111111111111111111111111111', 777, 190000, 3);

		CREATE TABLE account (staking_key TEXT, balance INTEGER);
		INSERT INTO account VALUES ('stake_key_999', 12345);

		CREATE TABLE utxo (tx_hash TEXT, tx_id INTEGER, address TEXT, value INTEGER);
		INSERT INTO utxo VALUES ('1111111111111111111111111111111111111111111111111111111111111111', 1, 'addr_test_paging', 5000000);
	`)
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, db, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. get_cardano_tip from transaction table (source: metadata_transaction)
	tipRes, err := cs.CallTool(
		ctx,
		&mcp.CallToolParams{Name: "get_cardano_tip"},
	)
	require.NoError(t, err)
	assert.False(t, tipRes.IsError)
	assert.Contains(
		t,
		tipRes.Content[0].(*mcp.TextContent).Text,
		"metadata_transaction",
	)
	assert.Contains(t, tipRes.Content[0].(*mcp.TextContent).Text, "777")

	// 2. get_block by slot from transaction table
	blockSlotRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_block",
		Arguments: map[string]any{"hash_or_slot": "777"},
	})
	require.NoError(t, err)
	assert.False(t, blockSlotRes.IsError)
	assert.Contains(t, blockSlotRes.Content[0].(*mcp.TextContent).Text, "777")

	// 3. get_block by hash from transaction table
	blockHashRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_block",
		Arguments: map[string]any{
			"hash_or_slot": "1111111111111111111111111111111111111111111111111111111111111111",
		},
	})
	require.NoError(t, err)
	assert.False(t, blockHashRes.IsError)
	assert.Contains(t, blockHashRes.Content[0].(*mcp.TextContent).Text, "777")

	// 4. get_transaction with block index and UTxO outputs created
	txRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_transaction",
		Arguments: map[string]any{
			"tx_hash": "1111111111111111111111111111111111111111111111111111111111111111",
		},
	})
	require.NoError(t, err)
	assert.False(t, txRes.IsError)
	assert.Contains(
		t,
		txRes.Content[0].(*mcp.TextContent).Text,
		"Block Index**: `3`",
	)
	assert.Contains(
		t,
		txRes.Content[0].(*mcp.TextContent).Text,
		"Outputs Created**: 1",
	)

	// 5. get_account by staking_key
	accRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_account",
		Arguments: map[string]any{
			"stake_address_or_credential": "stake_key_999",
		},
	})
	require.NoError(t, err)
	assert.False(t, accRes.IsError)
	assert.Contains(
		t,
		accRes.Content[0].(*mcp.TextContent).Text,
		"stake_key_999",
	)

	// 6. get_utxos with limit and offset boundaries
	utxoRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_utxos",
		Arguments: map[string]any{
			"address_or_credential": "addr_test_paging",
			"limit":                 150, // exceeds 100, defaults to 20
			"offset":                -5,  // negative, clamped to 0
		},
	})
	require.NoError(t, err)
	assert.False(t, utxoRes.IsError)
	assert.Contains(
		t,
		utxoRes.Content[0].(*mcp.TextContent).Text,
		"addr_test_paging",
	)
}

func TestSQLiteTools_ErrorBranches(t *testing.T) {
	db := setupTestDB(t)

	// Create an index on blocks table to test sqlite_table_schema index parsing
	_, err := db.Exec("CREATE INDEX idx_blocks_slot ON blocks(slot);")
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1.0"},
		nil,
	)
	RegisterSQLiteTools(server, db, 5*time.Second, 300)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. sqlite_query with execution error (nonexistent table)
	queryErrRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "sqlite_query",
		Arguments: map[string]any{
			"query": "SELECT * FROM non_existent_table_abc",
		},
	})
	require.NoError(t, err)
	assert.True(t, queryErrRes.IsError)
	assert.Contains(
		t,
		queryErrRes.Content[0].(*mcp.TextContent).Text,
		"SQLite execution error",
	)

	// 2. sqlite_query with limit > 200 and offset > 0
	queryLimitRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "sqlite_query",
		Arguments: map[string]any{
			"query":  "SELECT * FROM blocks",
			"limit":  250,
			"offset": 1,
		},
	})
	require.NoError(t, err)
	assert.False(t, queryLimitRes.IsError)
	assert.Contains(
		t,
		queryLimitRes.Content[0].(*mcp.TextContent).Text,
		"LIMIT 200 OFFSET 1",
	)

	// 3. sqlite_explain with invalid mutation query
	explainMutRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "sqlite_explain",
		Arguments: map[string]any{"query": "DELETE FROM blocks"},
	})
	require.NoError(t, err)
	assert.True(t, explainMutRes.IsError)

	// 4. sqlite_explain with execution error
	explainErrRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "sqlite_explain",
		Arguments: map[string]any{
			"query": "SELECT * FROM non_existent_table_xyz",
		},
	})
	require.NoError(t, err)
	assert.True(t, explainErrRes.IsError)
	assert.Contains(
		t,
		explainErrRes.Content[0].(*mcp.TextContent).Text,
		"EXPLAIN error",
	)

	// 5. sqlite_table_schema with invalid characters in table name
	schemaBadNameRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "sqlite_table_schema",
		Arguments: map[string]any{"table_name": "blocks; DROP TABLE blocks;"},
	})
	require.NoError(t, err)
	assert.True(t, schemaBadNameRes.IsError)
	assert.Contains(
		t,
		schemaBadNameRes.Content[0].(*mcp.TextContent).Text,
		"Invalid table name",
	)

	// 6. sqlite_table_schema with index inspection
	schemaIdxRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "sqlite_table_schema",
		Arguments: map[string]any{"table_name": "blocks"},
	})
	require.NoError(t, err)
	assert.False(t, schemaIdxRes.IsError)
	assert.Contains(
		t,
		schemaIdxRes.Content[0].(*mcp.TextContent).Text,
		"Indexes",
	)
	assert.Contains(
		t,
		schemaIdxRes.Content[0].(*mcp.TextContent).Text,
		"idx_blocks_slot",
	)
}

func TestSQLiteTools_NilDB(t *testing.T) {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1.0"},
		nil,
	)
	RegisterSQLiteTools(server, nil, 5*time.Second, 100)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. sqlite_query with nil db
	queryRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "sqlite_query",
		Arguments: map[string]any{"query": "SELECT 1"},
	})
	require.NoError(t, err)
	assert.True(t, queryRes.IsError)

	// 2. sqlite_explain with nil db
	explainRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "sqlite_explain",
		Arguments: map[string]any{"query": "SELECT 1"},
	})
	require.NoError(t, err)
	assert.True(t, explainRes.IsError)

	// 3. sqlite_table_schema with nil db
	schemaRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "sqlite_table_schema",
		Arguments: map[string]any{"table_name": "blocks"},
	})
	require.NoError(t, err)
	assert.True(t, schemaRes.IsError)
}

func TestServer_StopClosesDB(t *testing.T) {
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

func TestResolveDatumOrScript_Success(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE datum (
			hash BLOB NOT NULL UNIQUE,
			raw_datum BLOB NOT NULL,
			id INTEGER PRIMARY KEY,
			added_slot INTEGER NOT NULL
		);
		CREATE TABLE script (
			hash BLOB UNIQUE,
			content BLOB,
			id INTEGER PRIMARY KEY,
			created_slot INTEGER,
			type INTEGER
		);
	`)
	require.NoError(t, err)

	// Encode a Plutus Constr datum: Tag 121 (Constr 0) with fields ["bidder_address", 5000000]
	datumData := cbor.Tag{
		Number: 121,
		Content: []any{
			[]byte("bidder_address"),
			int64(5000000),
		},
	}
	datumCbor, err := cbor.Encode(datumData)
	require.NoError(t, err)

	datumHashHex := "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	datumHashBytes, err := hex.DecodeString(datumHashHex)
	require.NoError(t, err)

	_, err = db.Exec(
		"INSERT INTO datum (hash, raw_datum, id, added_slot) VALUES (?, ?, ?, ?)",
		datumHashBytes,
		datumCbor,
		1,
		123456,
	)
	require.NoError(t, err)

	// Script: 28-byte hash, PlutusV2 (type 2)
	scriptHashHex := "aa112233445566778899aabbccddeeff00112233445566778899aabb"
	scriptHashBytes, err := hex.DecodeString(scriptHashHex)
	require.NoError(t, err)

	scriptContent := []byte{0x4e, 0x4d, 0x01, 0x00, 0x00, 0x22, 0x26}
	_, err = db.Exec(
		"INSERT INTO script (hash, content, id, created_slot, type) VALUES (?, ?, ?, ?, ?)",
		scriptHashBytes,
		scriptContent,
		1,
		654321,
		2,
	)
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, db, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Resolve Datum
	datumRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": datumHashHex},
	})
	require.NoError(t, err)
	assert.False(t, datumRes.IsError)
	datumText := datumRes.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, datumText, "### Resolved Datum")
	assert.Contains(t, datumText, "Plutus Datum (CIP-32)")
	assert.Contains(t, datumText, "123456")
	assert.Contains(t, datumText, "constructor")
	assert.Contains(t, datumText, "5000000")

	// 2. Resolve Script
	scriptRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": scriptHashHex},
	})
	require.NoError(t, err)
	assert.False(t, scriptRes.IsError)
	scriptText := scriptRes.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, scriptText, "### Resolved Script")
	assert.Contains(t, scriptText, "PlutusV2")
	assert.Contains(t, scriptText, "654321")
	assert.Contains(t, scriptText, "4e4d0100002226")

	// 3. Resolve Datum with corrupted CBOR (exercises fallback comment)
	corruptDatumHashHex := "0202030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	corruptHashBytes, err := hex.DecodeString(corruptDatumHashHex)
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO datum (hash, raw_datum, id, added_slot) VALUES (?, ?, ?, ?)",
		corruptHashBytes,
		[]byte{0xff},
		2,
		999,
	)
	require.NoError(t, err)

	corruptRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": corruptDatumHashHex},
	})
	require.NoError(t, err)
	assert.False(t, corruptRes.IsError)
	assert.Contains(
		t,
		corruptRes.Content[0].(*mcp.TextContent).Text,
		"raw cbor parse error",
	)

	// 4. Resolve Script with content > 512 bytes and null slot/type
	longScriptHashHex := "bb112233445566778899aabbccddeeff00112233445566778899aabb"
	longScriptBytes, err := hex.DecodeString(longScriptHashHex)
	require.NoError(t, err)
	longContent := make([]byte, 600)
	for i := range longContent {
		longContent[i] = 0xab
	}
	_, err = db.Exec(
		"INSERT INTO script (hash, content, id, created_slot, type) VALUES (?, ?, ?, NULL, NULL)",
		longScriptBytes,
		longContent,
		2,
	)
	require.NoError(t, err)

	longRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": longScriptHashHex},
	})
	require.NoError(t, err)
	assert.False(t, longRes.IsError)
	longText := longRes.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, longText, "### Resolved Script")
	assert.Contains(t, longText, "Unknown")
	assert.Contains(t, longText, "...")
}

func TestResolveDatumOrScript_FailingAndSelfCorrecting(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE datum (hash BLOB NOT NULL UNIQUE, raw_datum BLOB NOT NULL, id INTEGER PRIMARY KEY, added_slot INTEGER NOT NULL);
		CREATE TABLE script (hash BLOB UNIQUE, content BLOB, id INTEGER PRIMARY KEY, created_slot INTEGER, type INTEGER);
	`)
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, db, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Empty hash
	resEmpty, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": "   "},
	})
	require.NoError(t, err)
	assert.True(t, resEmpty.IsError)
	assert.Contains(
		t,
		resEmpty.Content[0].(*mcp.TextContent).Text,
		"Hash parameter is empty",
	)

	// 2. Non-hex characters
	resNonHex, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": "not-valid-hex-zzz!"},
	})
	require.NoError(t, err)
	assert.True(t, resNonHex.IsError)
	assert.Contains(
		t,
		resNonHex.Content[0].(*mcp.TextContent).Text,
		"Invalid hex-encoded hash",
	)

	// 3. Invalid length (e.g. 10 hex chars = 5 bytes)
	resShort, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": "0102030405"},
	})
	require.NoError(t, err)
	assert.True(t, resShort.IsError)
	assert.Contains(
		t,
		resShort.Content[0].(*mcp.TextContent).Text,
		"Invalid hash length",
	)

	// 4. Hash not found in DB (valid 32-byte hash)
	resNotFound, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "resolve_datum_or_script",
		Arguments: map[string]any{
			"hash": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		},
	})
	require.NoError(t, err)
	assert.True(t, resNotFound.IsError)
	assert.Contains(
		t,
		resNotFound.Content[0].(*mcp.TextContent).Text,
		"was not found in the local database",
	)
	assert.Contains(
		t,
		resNotFound.Content[0].(*mcp.TextContent).Text,
		"inline datum",
	)

	// 5. Nil DB
	serverNil := mcp.NewServer(
		&mcp.Implementation{Name: "test-nil", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(serverNil, nil, nil, nil, "preview", 5*time.Second)
	clTrNil, srvTrNil := mcp.NewInMemoryTransports()
	_, err = serverNil.Connect(ctx, srvTrNil, nil)
	require.NoError(t, err)
	clientNil := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	csNil, err := clientNil.Connect(ctx, clTrNil, nil)
	require.NoError(t, err)
	defer csNil.Close()

	resNilDB, err := csNil.CallTool(ctx, &mcp.CallToolParams{
		Name: "resolve_datum_or_script",
		Arguments: map[string]any{
			"hash": "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		},
	})
	require.NoError(t, err)
	assert.True(t, resNilDB.IsError)
	assert.Contains(
		t,
		resNilDB.Content[0].(*mcp.TextContent).Text,
		"SQLite database is not available",
	)
}

func TestEvaluateTx_FailingAndSelfCorrecting(t *testing.T) {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, nil, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Empty payload
	resEmpty, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "evaluate_tx",
		Arguments: map[string]any{"cbor": "   "},
	})
	require.NoError(t, err)
	assert.True(t, resEmpty.IsError)
	assert.Contains(
		t,
		resEmpty.Content[0].(*mcp.TextContent).Text,
		"Transaction payload is empty",
	)

	// 2. Non-hex non-base64
	resInvalidEnc, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "evaluate_tx",
		Arguments: map[string]any{"cbor": "zzz-not-cbor-!@#$"},
	})
	require.NoError(t, err)
	assert.True(t, resInvalidEnc.IsError)
	assert.Contains(
		t,
		resInvalidEnc.Content[0].(*mcp.TextContent).Text,
		"Failed to decode transaction CBOR",
	)

	// 3. Valid hex of non-transaction CBOR
	resBadType, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "evaluate_tx",
		Arguments: map[string]any{"cbor": "80"},
	})
	require.NoError(t, err)
	assert.True(t, resBadType.IsError)
	assert.Contains(
		t,
		resBadType.Content[0].(*mcp.TextContent).Text,
		"failed to determine transaction type",
	)

	// 4. Valid transaction CBOR, but nil LedgerState
	validTxHex := "84a700818258200c07395aed88bdddc6de0518d1462dd0ec7e52e1" +
		"e3a53599f7cdb24dc80237f8010181a20058390073a817bb425cbe179af824529d96ce" +
		"b93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e391" +
		"4f5bcca3a793011a02dc6c00021a001e84800b5820192d0c0c2c2320e843e080b5f91a" +
		"9ca35155bc50f3ef3bfdbc72c1711b86367e0d818258203af629a5cd75f76d0cc21172" +
		"e1193b85f199ca78e837c3965d77d7d6bc90206b0010a20058390073a817bb425cbe17" +
		"9af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebee" +
		"f022245944e3914f5bcca3a793011a006acfc0111a002dc6c0a40081825820" +
		"25fcacade3fffc096b53bdaf4c7d012bded303c9edbee686d24b372dae60aa1b58409d" +
		"a928a064ff9f795110bdcb8ab05d2a7a023dd15ebc42044f102ce366c0c9077024c795" +
		"1c2d63584b7d2eea7bf1da4a7453bde4c99dd083889c1e2e2e3db804048119077a0581" +
		"840000187b820a0a06814746010000222601f4f6"
	resNilLS, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "evaluate_tx",
		Arguments: map[string]any{"cbor": validTxHex},
	})
	require.NoError(t, err)
	assert.True(t, resNilLS.IsError)
	assert.Contains(
		t,
		resNilLS.Content[0].(*mcp.TextContent).Text,
		"Ledger state is not initialized on this node",
	)
}

func TestToolsCardano_PlutusHelpers(t *testing.T) {
	// 1. scriptTypeName
	assert.Equal(t, "Native / MultiSig (Phase-1)", scriptTypeName(0))
	assert.Equal(t, "PlutusV1", scriptTypeName(1))
	assert.Equal(t, "PlutusV2", scriptTypeName(2))
	assert.Equal(t, "PlutusV3", scriptTypeName(3))
	assert.Equal(t, "Unknown (99)", scriptTypeName(99))

	// 2. redeemerPurposeString
	assert.Equal(t, "spend", redeemerPurposeString(lcommon.RedeemerTagSpend))
	assert.Equal(t, "mint", redeemerPurposeString(lcommon.RedeemerTagMint))
	assert.Equal(t, "cert", redeemerPurposeString(lcommon.RedeemerTagCert))
	assert.Equal(t, "reward", redeemerPurposeString(lcommon.RedeemerTagReward))
	assert.Equal(t, "voting", redeemerPurposeString(lcommon.RedeemerTagVoting))
	assert.Equal(
		t,
		"proposing",
		redeemerPurposeString(lcommon.RedeemerTagProposing),
	)
	assert.Equal(
		t,
		"guarding",
		redeemerPurposeString(lcommon.RedeemerTagGuarding),
	)
	assert.Equal(t, "tag(99)", redeemerPurposeString(lcommon.RedeemerTag(99)))

	// 3. cborToPrettyJSON
	validMap := map[any]any{
		"key": "val",
		123:   []any{int64(1), []byte{0xde, 0xad}},
	}
	mapCbor, err := cbor.Encode(validMap)
	require.NoError(t, err)
	pretty, err := cborToPrettyJSON(mapCbor)
	require.NoError(t, err)
	assert.Contains(t, pretty, "dead")

	// Corrupted CBOR
	_, err = cborToPrettyJSON([]byte{0xff})
	assert.Error(t, err)
}

func TestGetUtxosByAsset_SuccessAndFailing(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE utxo (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tx_id BLOB NOT NULL,
			output_idx INTEGER NOT NULL,
			payment_key BLOB NOT NULL,
			amount TEXT NOT NULL,
			deleted_slot INTEGER NOT NULL DEFAULT 0,
			datum_hash BLOB
		);
		CREATE TABLE asset (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			utxo_id INTEGER NOT NULL,
			policy_id BLOB NOT NULL,
			name BLOB NOT NULL,
			fingerprint BLOB NOT NULL,
			amount TEXT NOT NULL
		);
	`)
	require.NoError(t, err)

	policyHex := "a0000000000000000000000000000000000000000000000000000001"
	policyBytes, _ := hex.DecodeString(policyHex)
	txIDBytes, _ := hex.DecodeString(
		"1111111111111111111111111111111111111111111111111111111111111111",
	)
	payKeyBytes, _ := hex.DecodeString(
		"22222222222222222222222222222222222222222222222222222222",
	)

	// Insert live UTxO 1
	_, err = db.Exec(`
		INSERT INTO utxo (id, tx_id, output_idx, payment_key, amount, deleted_slot)
		VALUES (1, ?, 0, ?, '5000000', 0)
	`, txIDBytes, payKeyBytes)
	require.NoError(t, err)

	// Insert asset for UTxO 1
	_, err = db.Exec(`
		INSERT INTO asset (id, utxo_id, policy_id, name, fingerprint, amount)
		VALUES (1, 1, ?, ?, ?, '1000')
	`, policyBytes, []byte("HOSKY"), []byte("asset1hosky"))
	require.NoError(t, err)

	// Insert spent UTxO 2
	_, err = db.Exec(`
		INSERT INTO utxo (id, tx_id, output_idx, payment_key, amount, deleted_slot)
		VALUES (2, ?, 1, ?, '3000000', 100)
	`, txIDBytes, payKeyBytes)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO asset (id, utxo_id, policy_id, name, fingerprint, amount)
		VALUES (2, 2, ?, ?, ?, '500')
	`, policyBytes, []byte("HOSKY"), []byte("asset1hosky"))
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, db, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Success query by policy ID only
	resPolicy, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_utxos_by_asset",
		Arguments: map[string]any{"policy_id": policyHex},
	})
	require.NoError(t, err)
	assert.False(t, resPolicy.IsError)
	assert.Contains(
		t,
		resPolicy.Content[0].(*mcp.TextContent).Text,
		"Found 1 UTxOs",
	)
	assert.Contains(
		t,
		resPolicy.Content[0].(*mcp.TextContent).Text,
		"1111111111111111111111111111111111111111111111111111111111111111#0",
	)

	// 2. Success query by policy ID + ASCII asset name
	resName, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_utxos_by_asset",
		Arguments: map[string]any{
			"policy_id":  policyHex,
			"asset_name": "HOSKY",
		},
	})
	require.NoError(t, err)
	assert.False(t, resName.IsError)
	assert.Contains(t, resName.Content[0].(*mcp.TextContent).Text, "HOSKY")

	// 3. Success query by policy ID + Hex asset name
	resHexName, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_utxos_by_asset",
		Arguments: map[string]any{
			"policy_id":  policyHex,
			"asset_name": "484f534b59",
		},
	})
	require.NoError(t, err)
	assert.False(t, resHexName.IsError)
	assert.Contains(t, resHexName.Content[0].(*mcp.TextContent).Text, "HOSKY")

	// 4. Asset not found
	resNotFound, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_utxos_by_asset",
		Arguments: map[string]any{
			"policy_id":  policyHex,
			"asset_name": "OTHER",
		},
	})
	require.NoError(t, err)
	assert.False(t, resNotFound.IsError)
	assert.Contains(
		t,
		resNotFound.Content[0].(*mcp.TextContent).Text,
		"No live unspent UTxOs found",
	)

	// 5. Invalid policy ID length
	resShort, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_utxos_by_asset",
		Arguments: map[string]any{"policy_id": "short"},
	})
	require.NoError(t, err)
	assert.True(t, resShort.IsError)
	assert.Contains(
		t,
		resShort.Content[0].(*mcp.TextContent).Text,
		"must be a 56-character hex string",
	)

	// 6. Invalid hex chars in policy ID
	resInvalidHex, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_utxos_by_asset",
		Arguments: map[string]any{
			"policy_id": "z0000000000000000000000000000000000000000000000000000001",
		},
	})
	require.NoError(t, err)
	assert.True(t, resInvalidHex.IsError)
	assert.Contains(
		t,
		resInvalidHex.Content[0].(*mcp.TextContent).Text,
		"invalid hex in policy_id",
	)

	// 7. Nil DB
	serverNil := mcp.NewServer(
		&mcp.Implementation{Name: "nil-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(serverNil, nil, nil, nil, "preview", 5*time.Second)
	ctNil, stNil := mcp.NewInMemoryTransports()
	_, _ = serverNil.Connect(ctx, stNil, nil)
	csNil, _ := client.Connect(ctx, ctNil, nil)
	defer csNil.Close()

	resNilDB, err := csNil.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_utxos_by_asset",
		Arguments: map[string]any{"policy_id": policyHex},
	})
	require.NoError(t, err)
	assert.True(t, resNilDB.IsError)
	assert.Contains(
		t,
		resNilDB.Content[0].(*mcp.TextContent).Text,
		"SQLite database is not available",
	)
}

func TestGetAssetInfo_SuccessAndFailing(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE token_registry_entry (
			subject TEXT PRIMARY KEY,
			name TEXT,
			ticker TEXT,
			description TEXT,
			url TEXT,
			decimals INTEGER
		);
		CREATE TABLE utxo (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tx_id BLOB NOT NULL,
			deleted_slot INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE asset (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			utxo_id INTEGER NOT NULL,
			policy_id BLOB NOT NULL,
			name BLOB NOT NULL,
			amount TEXT NOT NULL
		);
		CREATE TABLE asset_mint_burn (
			tx_hash BLOB NOT NULL,
			policy_id BLOB NOT NULL,
			name BLOB NOT NULL,
			fingerprint BLOB,
			slot INTEGER NOT NULL,
			quantity TEXT NOT NULL,
			tx_index INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE "transaction" (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			hash BLOB NOT NULL
		);
		CREATE TABLE transaction_metadata_label (
			transaction_id INTEGER NOT NULL,
			label TEXT NOT NULL,
			json_value TEXT
		);
	`)
	require.NoError(t, err)

	policyHex := "b0000000000000000000000000000000000000000000000000000002"
	policyBytes, _ := hex.DecodeString(policyHex)
	mintTxHash, _ := hex.DecodeString(
		"3333333333333333333333333333333333333333333333333333333333333333",
	)

	// 1. Insert token registry entry
	subject := policyHex + "544f4b454e"
	_, err = db.Exec(`
		INSERT INTO token_registry_entry (subject, name, ticker, description, url, decimals)
		VALUES (?, 'My Token', 'TKN', 'A utility token', 'https://example.com', 6)
	`, subject)
	require.NoError(t, err)

	// 2. Insert UTxO & live asset
	_, err = db.Exec(
		`INSERT INTO utxo (id, tx_id, deleted_slot) VALUES (1, ?, 0)`,
		mintTxHash,
	)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO asset (id, utxo_id, policy_id, name, amount)
		VALUES (1, 1, ?, ?, '500000')
	`, policyBytes, []byte("TOKEN"))
	require.NoError(t, err)

	// 3. Insert mint record
	_, err = db.Exec(`
		INSERT INTO asset_mint_burn (tx_hash, policy_id, name, slot, quantity)
		VALUES (?, ?, ?, 12345, '1000000')
	`, mintTxHash, policyBytes, []byte("TOKEN"))
	require.NoError(t, err)

	// 4. Insert transaction and CIP-25 metadata
	_, err = db.Exec(
		`INSERT INTO "transaction" (id, hash) VALUES (1, ?)`,
		mintTxHash,
	)
	require.NoError(t, err)
	cip25JSON := `{"` + policyHex + `": {"TOKEN": {"name": "Token NFT", "image": "ipfs://Qm123"}}}`
	_, err = db.Exec(`
		INSERT INTO transaction_metadata_label (transaction_id, label, json_value)
		VALUES (1, '721', ?)
	`, cip25JSON)
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, db, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Success query with registry, supply, mint, and CIP-25
	resFull, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_asset_info",
		Arguments: map[string]any{
			"policy_id":  policyHex,
			"asset_name": "TOKEN",
		},
	})
	require.NoError(t, err)
	assert.False(t, resFull.IsError)
	text := resFull.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, text, "My Token")
	assert.Contains(t, text, "TKN")
	assert.Contains(t, text, "500000 units")
	assert.Contains(t, text, "CIP-25 NFT Metadata (Label 721)")
	assert.Contains(t, text, "ipfs://Qm123")

	// 2. Query unknown asset
	resNotFound, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_asset_info",
		Arguments: map[string]any{
			"policy_id":  policyHex,
			"asset_name": "UNKNOWN",
		},
	})
	require.NoError(t, err)
	assert.False(t, resNotFound.IsError)
	assert.Contains(
		t,
		resNotFound.Content[0].(*mcp.TextContent).Text,
		"was not found in the local token registry",
	)

	// 3. Invalid policy length
	resShort, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_asset_info",
		Arguments: map[string]any{"policy_id": "tooshort"},
	})
	require.NoError(t, err)
	assert.True(t, resShort.IsError)

	// 4. Invalid policy hex
	resBadHex, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_asset_info",
		Arguments: map[string]any{
			"policy_id": "z0000000000000000000000000000000000000000000000000000002",
		},
	})
	require.NoError(t, err)
	assert.True(t, resBadHex.IsError)

	// 5. Nil DB
	serverNil := mcp.NewServer(
		&mcp.Implementation{Name: "nil-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(serverNil, nil, nil, nil, "preview", 5*time.Second)
	ctNil, stNil := mcp.NewInMemoryTransports()
	_, _ = serverNil.Connect(ctx, stNil, nil)
	csNil, _ := client.Connect(ctx, ctNil, nil)
	defer csNil.Close()

	resNilDB, err := csNil.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_asset_info",
		Arguments: map[string]any{"policy_id": policyHex},
	})
	require.NoError(t, err)
	assert.True(t, resNilDB.IsError)
}

func TestGetGovernanceState_SuccessAndFailing(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE constitution (
			anchor_url TEXT,
			anchor_hash BLOB,
			policy_hash BLOB,
			added_slot INTEGER,
			deleted_slot INTEGER
		);
		CREATE TABLE committee_quorum (
			quorum TEXT,
			added_slot INTEGER
		);
		CREATE TABLE committee_member (
			cold_cred_hash BLOB,
			expires_epoch INTEGER,
			term_start_slot INTEGER,
			added_slot INTEGER,
			deleted_slot INTEGER
		);
		CREATE TABLE drep (
			credential BLOB,
			anchor_url TEXT,
			anchor_hash BLOB,
			added_slot INTEGER,
			last_activity_epoch INTEGER,
			expiry_epoch INTEGER,
			active BOOLEAN
		);
	`)
	require.NoError(t, err)

	constHash, _ := hex.DecodeString(
		"4444444444444444444444444444444444444444444444444444444444444444",
	)
	commColdHash, _ := hex.DecodeString(
		"55555555555555555555555555555555555555555555555555555555",
	)
	drepCred, _ := hex.DecodeString(
		"66666666666666666666666666666666666666666666666666666666",
	)

	// 1. Insert governance state
	_, err = db.Exec(`
		INSERT INTO constitution (anchor_url, anchor_hash, added_slot)
		VALUES ('https://cardanofoundation.org/constitution.pdf', ?, 5000)
	`, constHash)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO committee_quorum (quorum, added_slot) VALUES ('2/3', 5000)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO committee_member (cold_cred_hash, expires_epoch, term_start_slot)
		VALUES (?, 600, 5000)
	`, commColdHash)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO drep (credential, anchor_url, anchor_hash, added_slot, last_activity_epoch, expiry_epoch, active)
		VALUES (?, 'https://drep.me', ?, 5100, 480, 550, 1)
	`, drepCred, constHash)
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, db, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Success query overall governance state
	resGov, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_governance_state",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.False(t, resGov.IsError)
	govText := resGov.Content[0].(*mcp.TextContent).Text
	assert.Contains(
		t,
		govText,
		"https://cardanofoundation.org/constitution.pdf",
	)
	assert.Contains(t, govText, "2/3")
	assert.Contains(t, govText, "**Active Registered DReps**: 1")

	// 2. Query specific DRep by hex
	resDrep, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_governance_state",
		Arguments: map[string]any{
			"drep_credential": "66666666666666666666666666666666666666666666666666666666",
		},
	})
	require.NoError(t, err)
	assert.False(t, resDrep.IsError)
	assert.Contains(
		t,
		resDrep.Content[0].(*mcp.TextContent).Text,
		"https://drep.me",
	)

	// 3. Query specific DRep with invalid format
	resBadDrep, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_governance_state",
		Arguments: map[string]any{"drep_credential": "invalid-drep-id!"},
	})
	require.NoError(t, err)
	assert.False(t, resBadDrep.IsError)
	assert.Contains(
		t,
		resBadDrep.Content[0].(*mcp.TextContent).Text,
		"Could not parse DRep identifier",
	)

	// 4. Nil DB
	serverNil := mcp.NewServer(
		&mcp.Implementation{Name: "nil-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(serverNil, nil, nil, nil, "preview", 5*time.Second)
	ctNil, stNil := mcp.NewInMemoryTransports()
	_, _ = serverNil.Connect(ctx, stNil, nil)
	csNil, _ := client.Connect(ctx, ctNil, nil)
	defer csNil.Close()

	resNilDB, err := csNil.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_governance_state",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.True(t, resNilDB.IsError)
}

func TestGetPoolPerformance_SuccessAndFailing(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE reward_pool_output (
			epoch INTEGER NOT NULL,
			pool_key_hash BLOB NOT NULL,
			apparent_performance TEXT,
			optimal_reward TEXT,
			total_reward TEXT,
			leader_reward TEXT,
			member_reward_total TEXT
		);
		CREATE TABLE reward_pool_input (
			epoch INTEGER NOT NULL,
			pool_key_hash BLOB NOT NULL,
			pledge TEXT,
			cost TEXT,
			margin TEXT,
			delegated_stake TEXT,
			blocks_produced INTEGER
		);
	`)
	require.NoError(t, err)

	poolHashHex := "c0000000000000000000000000000000000000000000000000000003"
	poolHashBytes, _ := hex.DecodeString(poolHashHex)

	conv, err := bech32.ConvertBits(poolHashBytes, 8, 5, true)
	require.NoError(t, err)
	poolBech32, err := bech32.Encode("pool", conv)
	require.NoError(t, err)

	// Insert input and output
	_, err = db.Exec(`
		INSERT INTO reward_pool_output (epoch, pool_key_hash, apparent_performance, optimal_reward, total_reward, leader_reward, member_reward_total)
		VALUES (450, ?, '0.985', '13000000', '12500000', '2500000', '10000000')
	`, poolHashBytes)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO reward_pool_input (epoch, pool_key_hash, pledge, cost, margin, delegated_stake, blocks_produced)
		VALUES (450, ?, '50000000000', '340000000', '0.02', '15000000000000', 18)
	`, poolHashBytes)
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, db, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Success with Bech32 pool ID
	resBech32, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_pool_performance",
		Arguments: map[string]any{"pool_id": poolBech32},
	})
	require.NoError(t, err)
	assert.False(t, resBech32.IsError)
	bText := resBech32.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, bText, "450")
	assert.Contains(t, bText, "18")
	assert.Contains(t, bText, "0.985")
	assert.Contains(t, bText, "12500000")

	// 2. Success with 56-hex pool ID and specific epoch
	epochNum := 450
	resHex, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_pool_performance",
		Arguments: map[string]any{"pool_id": poolHashHex, "epoch": epochNum},
	})
	require.NoError(t, err)
	assert.False(t, resHex.IsError)
	hText := resHex.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, hText, "450")

	// 3. Pool not found
	unknownPoolHex := "d0000000000000000000000000000000000000000000000000000004"
	resNotFound, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_pool_performance",
		Arguments: map[string]any{"pool_id": unknownPoolHex},
	})
	require.NoError(t, err)
	assert.False(t, resNotFound.IsError)
	assert.Contains(
		t,
		resNotFound.Content[0].(*mcp.TextContent).Text,
		"No historical reward performance records found",
	)

	// 4. Invalid pool ID format
	resInvalid, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_pool_performance",
		Arguments: map[string]any{"pool_id": "invalid-pool"},
	})
	require.NoError(t, err)
	assert.True(t, resInvalid.IsError)
	assert.Contains(
		t,
		resInvalid.Content[0].(*mcp.TextContent).Text,
		"Error parsing pool ID",
	)

	// 5. Nil DB
	serverNil := mcp.NewServer(
		&mcp.Implementation{Name: "nil-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(serverNil, nil, nil, nil, "preview", 5*time.Second)
	ctNil, stNil := mcp.NewInMemoryTransports()
	_, _ = serverNil.Connect(ctx, stNil, nil)
	csNil, _ := client.Connect(ctx, ctNil, nil)
	defer csNil.Close()

	resNilDB, err := csNil.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_pool_performance",
		Arguments: map[string]any{"pool_id": poolHashHex},
	})
	require.NoError(t, err)
	assert.True(t, resNilDB.IsError)
}

func TestToolsCardano_NewHelpers(t *testing.T) {
	// 1. parseAssetName
	raw, hexStr := parseAssetName("HOSKY")
	assert.Equal(t, []byte("HOSKY"), raw)
	assert.Equal(t, "484f534b59", hexStr)

	rawHex, hexStr2 := parseAssetName("484f534b59")
	assert.Equal(t, []byte("HOSKY"), rawHex)
	assert.Equal(t, "484f534b59", hexStr2)

	rawEmpty, hexEmpty := parseAssetName("")
	assert.Nil(t, rawEmpty)
	assert.Equal(t, "", hexEmpty)

	// 2. parsePoolID
	_, err := parsePoolID("")
	assert.Error(t, err)

	_, err = parsePoolID("short")
	assert.Error(t, err)

	validHex := "c0000000000000000000000000000000000000000000000000000003"
	parsed, err := parsePoolID(validHex)
	require.NoError(t, err)
	assert.Equal(t, 28, len(parsed))

	// 3. parseDrepID
	drepEmpty, err := parseDrepID("")
	require.NoError(t, err)
	assert.Nil(t, drepEmpty)

	parsedDrep, err := parseDrepID(validHex)
	require.NoError(t, err)
	assert.Equal(t, 28, len(parsedDrep))

	_, err = parseDrepID("invalid-drep!")
	assert.Error(t, err)
}

func TestParseAddressOrCredential(t *testing.T) {
	// Empty string
	_, _, err := parseAddressOrCredential("")
	assert.Error(t, err)

	// 56-hex payment credential
	hexCred := "5e68f6d9c3203ea691221f59a3988477cfaeda716e2b7bf60b216ae0"
	pk, sk, err := parseAddressOrCredential(hexCred)
	require.NoError(t, err)
	assert.Equal(t, 28, len(pk))
	assert.Nil(t, sk)

	// Valid real testnet address
	realAddr := "addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp"
	pkReal, skReal, err := parseAddressOrCredential(realAddr)
	require.NoError(t, err)
	assert.Equal(t, 28, len(pkReal))
	assert.Equal(t, 28, len(skReal))

	// Invalid format
	_, _, err = parseAddressOrCredential("invalid_addr_format!")
	assert.Error(t, err)
}

func TestGetUtxos_RealDingoSchema(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Real Dingo UTxO schema
	_, err = db.Exec(`
		CREATE TABLE utxo (
			id INTEGER PRIMARY KEY,
			tx_id BLOB,
			output_idx INTEGER,
			payment_key BLOB,
			staking_key BLOB,
			amount TEXT,
			added_slot INTEGER,
			deleted_slot INTEGER
		);
	`)
	require.NoError(t, err)

	txHash, _ := hex.DecodeString(
		"918d475d9a22799647dc55e51dd8b65939a79ec0daf4406e4c393557f62398ea",
	)
	payKey, _ := hex.DecodeString(
		"5e68f6d9c3203ea691221f59a3988477cfaeda716e2b7bf60b216ae0",
	)

	// Insert unspent UTxO
	_, err = db.Exec(`
		INSERT INTO utxo (id, tx_id, output_idx, payment_key, amount, added_slot, deleted_slot)
		VALUES (1, ?, 0, ?, '25000000', 50000, 0);
	`, txHash, payKey)
	require.NoError(t, err)

	// Insert spent UTxO (deleted_slot > 0)
	_, err = db.Exec(`
		INSERT INTO utxo (id, tx_id, output_idx, payment_key, amount, added_slot, deleted_slot)
		VALUES (2, ?, 1, ?, '10000000', 40000, 50001);
	`, txHash, payKey)
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, db, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Success: Query by 56-hex payment credential
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_utxos",
		Arguments: map[string]any{
			"address_or_credential": "5e68f6d9c3203ea691221f59a3988477cfaeda716e2b7bf60b216ae0",
		},
	})
	require.NoError(t, err)
	assert.False(t, res.IsError)
	text := res.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, text, "25000000")
	// Must NOT contain spent UTxO
	assert.NotContains(t, text, "10000000")

	// 2. Not found: Query by different 56-hex credential
	resNotFound, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_utxos",
		Arguments: map[string]any{
			"address_or_credential": "00000000000000000000000000000000000000000000000000000000",
		},
	})
	require.NoError(t, err)
	assert.False(t, resNotFound.IsError)
	assert.Contains(
		t,
		resNotFound.Content[0].(*mcp.TextContent).Text,
		"0 results",
	)

	// 3. Error: Query by invalid credential
	resInvalid, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_utxos",
		Arguments: map[string]any{
			"address_or_credential": "not_an_address_or_hash",
		},
	})
	require.NoError(t, err)
	assert.True(t, resInvalid.IsError)
	assert.Contains(
		t,
		resInvalid.Content[0].(*mcp.TextContent).Text,
		"Invalid address or payment credential",
	)
}

func TestMCP_PromptsList(t *testing.T) {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		&mcp.ServerOptions{
			Capabilities: &mcp.ServerCapabilities{
				Prompts: &mcp.PromptCapabilities{ListChanged: true},
			},
		},
	)
	RegisterPrompts(server, nil, nil, "preview")

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "test-client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	res, err := cs.ListPrompts(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, res)

	promptNames := make(map[string]bool)
	for _, p := range res.Prompts {
		promptNames[p.Name] = true
	}

	expectedPrompts := []string{
		"diagnose_node_health",
		"simulate_and_diagnose_tx",
		"audit_pool_rewards",
		"track_asset_portfolio",
		"conway_governance_brief",
		"investigate_address",
	}

	for _, expected := range expectedPrompts {
		assert.True(t, promptNames[expected], "expected prompt %s to be registered", expected)
	}
	assert.Equal(t, len(expectedPrompts), len(res.Prompts))
}

func TestMCP_PromptsGet(t *testing.T) {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		&mcp.ServerOptions{
			Capabilities: &mcp.ServerCapabilities{
				Prompts: &mcp.PromptCapabilities{ListChanged: true},
			},
		},
	)
	RegisterPrompts(server, nil, nil, "preview")

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "test-client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. diagnose_node_health (standard & detailed)
	resDiag, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "diagnose_node_health",
	})
	require.NoError(t, err)
	require.Len(t, resDiag.Messages, 1)
	assert.Contains(t, resDiag.Messages[0].Content.(*mcp.TextContent).Text, "get_cardano_tip")
	assert.Contains(t, resDiag.Messages[0].Content.(*mcp.TextContent).Text, "BlockHeaderProtocolMinor is 69")

	resDiagDetailed, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name:      "diagnose_node_health",
		Arguments: map[string]string{"detailed": "true"},
	})
	require.NoError(t, err)
	assert.Contains(t, resDiagDetailed.Messages[0].Content.(*mcp.TextContent).Text, "Detailed mode enabled")

	// 2. simulate_and_diagnose_tx (success & missing argument)
	resSim, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "simulate_and_diagnose_tx",
		Arguments: map[string]string{
			"tx_cbor": "84a3008182582001...",
			"purpose": "dex-swap",
		},
	})
	require.NoError(t, err)
	simText := resSim.Messages[0].Content.(*mcp.TextContent).Text
	assert.Contains(t, simText, "84a3008182582001...")
	assert.Contains(t, simText, "dex-swap")
	assert.Contains(t, simText, "evaluate_tx")

	_, errSimMissing := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "simulate_and_diagnose_tx",
	})
	assert.Error(t, errSimMissing)
	assert.Contains(t, errSimMissing.Error(), "missing required argument 'tx_cbor'")

	// 3. audit_pool_rewards (success & missing pool_id)
	resAudit, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "audit_pool_rewards",
		Arguments: map[string]string{
			"pool_id": "pool1z502w0ctavu55...",
			"epoch":   "450",
		},
	})
	require.NoError(t, err)
	auditText := resAudit.Messages[0].Content.(*mcp.TextContent).Text
	assert.Contains(t, auditText, "pool1z502w0ctavu55...")
	assert.Contains(t, auditText, "epoch 450")
	assert.Contains(t, auditText, "get_pool_performance")

	_, errAuditMissing := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "audit_pool_rewards",
	})
	assert.Error(t, errAuditMissing)
	assert.Contains(t, errAuditMissing.Error(), "missing required argument 'pool_id'")

	// 4. track_asset_portfolio (success & missing policy_id)
	resAsset, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "track_asset_portfolio",
		Arguments: map[string]string{
			"policy_id":  "a0000000000000000000000000000000000000000000000000000001",
			"asset_name": "HOSKY",
		},
	})
	require.NoError(t, err)
	assetText := resAsset.Messages[0].Content.(*mcp.TextContent).Text
	assert.Contains(t, assetText, "a0000000000000000000000000000000000000000000000000000001")
	assert.Contains(t, assetText, "HOSKY")
	assert.Contains(t, assetText, "get_utxos_by_asset")

	_, errAssetMissing := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "track_asset_portfolio",
	})
	assert.Error(t, errAssetMissing)
	assert.Contains(t, errAssetMissing.Error(), "missing required argument 'policy_id'")

	// 5. conway_governance_brief (success with and without arguments)
	resGov, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "conway_governance_brief",
		Arguments: map[string]string{
			"drep_id":       "drep1c000000000000000000000000000000000000000000000000003",
			"stake_address": "stake1u0000000000000000000000000000000000000000000000001",
		},
	})
	require.NoError(t, err)
	govText := resGov.Messages[0].Content.(*mcp.TextContent).Text
	assert.Contains(t, govText, "drep1c000000000000000000000000000000000000000000000000003")
	assert.Contains(t, govText, "stake1u0000000000000000000000000000000000000000000000001")
	assert.Contains(t, govText, "get_governance_state")

	resGovEmpty, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "conway_governance_brief",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resGovEmpty.Messages[0].Content.(*mcp.TextContent).Text)

	// 6. investigate_address (success & missing address)
	resAddr, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "investigate_address",
		Arguments: map[string]string{
			"address": "addr_test1qrz9...",
		},
	})
	require.NoError(t, err)
	addrText := resAddr.Messages[0].Content.(*mcp.TextContent).Text
	assert.Contains(t, addrText, "addr_test1qrz9...")
	assert.Contains(t, addrText, "get_utxos")

	_, errAddrMissing := cs.GetPrompt(ctx, &mcp.GetPromptParams{
		Name: "investigate_address",
	})
	assert.Error(t, errAddrMissing)
	assert.Contains(t, errAddrMissing.Error(), "missing required argument 'address'")
}

func TestMCP_PromptsCapabilities(t *testing.T) {
	cfg := DefaultProviderConfig()
	deps := ProviderDependencies{
		Network: "preview",
	}
	server, _, err := NewMCPServer(cfg, deps)
	require.NoError(t, err)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "test-client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// Verify server capabilities include Prompts
	initRes := cs.InitializeResult()
	require.NotNil(t, initRes)
	caps := initRes.Capabilities
	require.NotNil(t, caps)
	require.NotNil(t, caps.Prompts)
	assert.True(t, caps.Prompts.ListChanged)
	require.NotNil(t, caps.Tools)
	assert.True(t, caps.Tools.ListChanged)
	require.NotNil(t, caps.Resources)
	assert.True(t, caps.Resources.ListChanged)
}

type testMempoolMock struct {
	mempool.Service
	txs      []mempool.MempoolTransaction
	capacity int64
}

func (m *testMempoolMock) GetTransaction(hash string) (mempool.MempoolTransaction, bool) {
	for _, tx := range m.txs {
		if strings.EqualFold(tx.Hash, hash) {
			return tx, true
		}
	}
	return mempool.MempoolTransaction{}, false
}

func (m *testMempoolMock) Transactions() []mempool.MempoolTransaction {
	return m.txs
}

func (m *testMempoolMock) CapacityBytes() int64 {
	return m.capacity
}

func TestMCP_ProtocolParameters(t *testing.T) {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, nil, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "test-client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Call with nil ledger state
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_protocol_parameters",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.False(t, res.IsError)
	require.NotEmpty(t, res.Content)
	text := res.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, text, "Protocol parameters are currently unavailable")
}

func TestMCP_MempoolInfo(t *testing.T) {
	// 1. Nil Mempool
	serverNil := mcp.NewServer(
		&mcp.Implementation{Name: "test-nil", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(serverNil, nil, nil, nil, "preview", 5*time.Second)

	clTrNil, srvTrNil := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := serverNil.Connect(ctx, srvTrNil, nil)
	require.NoError(t, err)

	clientNil := mcp.NewClient(
		&mcp.Implementation{Name: "client-nil", Version: "1.0"},
		nil,
	)
	csNil, err := clientNil.Connect(ctx, clTrNil, nil)
	require.NoError(t, err)
	defer csNil.Close()

	resNil, err := csNil.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_mempool_info",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.False(t, resNil.IsError)
	assert.Contains(t, resNil.Content[0].(*mcp.TextContent).Text, "mempool service is currently inactive")

	// 2. Active Mempool Mock
	mockMP := &testMempoolMock{
		capacity: 1048576, // 1 MiB
		txs: []mempool.MempoolTransaction{
			{
				Hash:     "aaaabbbbccccdddd0000111122223333444455556666777788889999aaaabbbb",
				Type:     6,
				Cbor:     make([]byte, 512),
				LastSeen: time.Now().Add(-10 * time.Second),
			},
			{
				Hash:     "bbbbccccddddeeee0000111122223333444455556666777788889999bbbbcccc",
				Type:     6,
				Cbor:     make([]byte, 1024),
				LastSeen: time.Now().Add(-5 * time.Second),
			},
		},
	}

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-mp", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, nil, nil, mockMP, "preview", 5*time.Second)

	clTr, srvTr := mcp.NewInMemoryTransports()
	_, err = server.Connect(ctx, srvTr, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clTr, nil)
	require.NoError(t, err)
	defer cs.Close()

	// Summary inspection
	resSummary, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_mempool_info",
		Arguments: map[string]any{"limit": 5},
	})
	require.NoError(t, err)
	assert.False(t, resSummary.IsError)
	summaryText := resSummary.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, summaryText, "Pending Transactions**: 2")
	assert.Contains(t, summaryText, "Buffered Volume**: 1536 bytes")
	assert.Contains(t, summaryText, "Mempool Capacity**: 1048576 bytes")
	assert.Contains(t, summaryText, "aaaabbbbccccdddd0000111122223333444455556666777788889999aaaabbbb")

	// Specific Tx Found
	resTx, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_mempool_info",
		Arguments: map[string]any{
			"tx_hash": "aaaabbbbccccdddd0000111122223333444455556666777788889999aaaabbbb",
		},
	})
	require.NoError(t, err)
	assert.False(t, resTx.IsError)
	txText := resTx.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, txText, "Payload Size**: 512 bytes")
	assert.Contains(t, txText, "Pending Block Inclusion")

	// Specific Tx Not Found
	resNotFound, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_mempool_info",
		Arguments: map[string]any{
			"tx_hash": "0000000000000000000000000000000000000000000000000000000000000000",
		},
	})
	require.NoError(t, err)
	assert.False(t, resNotFound.IsError)
	assert.Contains(t, resNotFound.Content[0].(*mcp.TextContent).Text, "not currently present")
}

func TestMCP_DecodeAddress(t *testing.T) {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, nil, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "test-client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Shelley Base Address (Mainnet)
	resBase, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "decode_address",
		Arguments: map[string]any{
			"address": "addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd",
		},
	})
	require.NoError(t, err)
	assert.False(t, resBase.IsError)
	baseText := resBase.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, baseText, "Shelley Base Address (Payment + Stake)")
	assert.Contains(t, baseText, "Mainnet (ID 1)")
	assert.Contains(t, baseText, "Public Key Hash (Ed25519 VKey)")
	assert.Contains(t, baseText, "Stake Public Key Hash (VKey Delegator)")

	// 2. Stake Reward Address (derived from base address)
	baseAddrObj, err := lcommon.NewAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd")
	require.NoError(t, err)
	stakeAddrStr := baseAddrObj.StakeAddress().String()

	resStake, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "decode_address",
		Arguments: map[string]any{
			"address": stakeAddrStr,
		},
	})
	require.NoError(t, err)
	assert.False(t, resStake.IsError)
	stakeText := resStake.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, stakeText, "Shelley Reward Address")

	// 3. Invalid Address
	resInvalid, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "decode_address",
		Arguments: map[string]any{
			"address": "not_an_address",
		},
	})
	require.NoError(t, err)
	assert.True(t, resInvalid.IsError)
}

func TestMCP_GovernanceProposal(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE governance_proposal (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tx_hash BLOB NOT NULL,
			action_index INTEGER NOT NULL,
			action_type INTEGER NOT NULL,
			proposed_epoch INTEGER NOT NULL,
			expires_epoch INTEGER NOT NULL,
			enacted_epoch INTEGER,
			ratified_epoch INTEGER,
			expired_epoch INTEGER,
			anchor_url TEXT,
			anchor_hash BLOB,
			deposit INTEGER NOT NULL,
			return_address BLOB
		);

		CREATE TABLE governance_vote (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			proposal_id INTEGER NOT NULL,
			voter_type INTEGER NOT NULL,
			vote INTEGER NOT NULL
		);
	`)
	require.NoError(t, err)

	txHashBytes := make([]byte, 32)
	txHashBytes[0] = 0xca
	txHashBytes[1] = 0xfe
	anchorHashBytes := make([]byte, 32)
	anchorHashBytes[0] = 0xde
	anchorHashBytes[1] = 0xad

	_, err = db.Exec(`
		INSERT INTO governance_proposal (
			id, tx_hash, action_index, action_type, proposed_epoch, expires_epoch,
			anchor_url, anchor_hash, deposit
		) VALUES (
			1, ?, 0, 0, 500, 505, 'https://cardanofoundation.org/gov/cip1694.json', ?, 100000000000
		);
	`, txHashBytes, anchorHashBytes)
	require.NoError(t, err)

	// Add votes: 3 CC Yes, 10 DRep Yes, 2 DRep No, 5 SPO Abstain
	_, err = db.Exec(`
		INSERT INTO governance_vote (proposal_id, voter_type, vote) VALUES
		(1, 0, 1), (1, 0, 1), (1, 0, 1),
		(1, 1, 1), (1, 1, 1), (1, 1, 0),
		(1, 2, 2);
	`)
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, db, nil, nil, "preview", 5*time.Second)

	clTr, srvTr := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, srvTr, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clTr, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. List proposals
	resList, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_governance_proposal",
		Arguments: map[string]any{"status": "active"},
	})
	require.NoError(t, err)
	assert.False(t, resList.IsError)
	listText := resList.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, listText, "ParameterChange")
	assert.Contains(t, listText, "100000.00") // 100k ADA deposit

	// 2. Query specific proposal with voting breakdown
	txHex := hex.EncodeToString(txHashBytes)
	resDetail, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_governance_proposal",
		Arguments: map[string]any{
			"tx_hash":      txHex,
			"action_index": 0,
		},
	})
	require.NoError(t, err)
	assert.False(t, resDetail.IsError)
	detailText := resDetail.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, detailText, "ParameterChange (ID 0)")
	assert.Contains(t, detailText, "CIP-1694 Voting Procedure Breakdown")
	assert.Contains(t, detailText, "| **Constitutional Committee (CC)** | 3 | 0 | 0 |")
	assert.Contains(t, detailText, "| **Delegated Representatives (DReps)** | 2 | 1 | 0 |")
	assert.Contains(t, detailText, "| **Stake Pool Operators (SPOs)** | 0 | 0 | 1 |")
	assert.Contains(t, detailText, "https://cardanofoundation.org/gov/cip1694.json")
}

func TestMCP_CalculateMinUtxo(t *testing.T) {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, nil, nil, nil, "preview", 5*time.Second)

	clTr, srvTr := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := server.Connect(ctx, srvTr, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clTr, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Basic ADA-only UTxO
	resBasic, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "calculate_min_utxo",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.False(t, resBasic.IsError)
	basicText := resBasic.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, basicText, "CIP-55 Minimum UTxO Lovelace Calculation")
	assert.Contains(t, basicText, "4310 Lovelace/byte")
	assert.Contains(t, basicText, "Required Minimum Deposit")

	// 2. Multi-asset + Inline Datum
	resComplex, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "calculate_min_utxo",
		Arguments: map[string]any{
			"coins_per_utxo_byte": 4310,
			"assets_count":        3,
			"policies_count":      2,
			"inline_datum_hex":    "d8799f010203ff",
		},
	})
	require.NoError(t, err)
	assert.False(t, resComplex.IsError)
	complexText := resComplex.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, complexText, "3 assets across 2 policies")
	assert.Contains(t, complexText, "Inline Datum (7 bytes serialized)")
}
