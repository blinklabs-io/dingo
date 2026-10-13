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
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestSQLiteCTEMutationsRejected(t *testing.T) {
	t.Parallel()
	queries := []string{
		"WITH x AS (SELECT 1) DELETE FROM token_registry_entry RETURNING subject",
		"WITH x AS (SELECT 1) UPDATE token_registry_entry SET name = 'changed' RETURNING subject",
		"WITH x AS (SELECT 1) INSERT INTO token_registry_entry(subject) VALUES ('new') RETURNING subject",
		"WITH x AS (SELECT 1) REPLACE INTO token_registry_entry(subject) VALUES ('new') RETURNING subject",
		"WITH RECURSIVE x(n) AS (VALUES(1)) DELETE/**/FROM token_registry_entry RETURNING subject",
		"WITH x AS NOT MATERIALIZED (SELECT ') DELETE'), y AS (SELECT 2) UPDATE token_registry_entry SET name='changed' RETURNING subject",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			t.Parallel()
			db := newSchemaDB(t)
			_, err := db.Exec(
				"INSERT INTO token_registry_entry(subject, name) VALUES ('original', 'original')",
			)
			require.NoError(t, err)
			result, err := newToolSession(
				t,
				db,
				100,
			).CallTool(t.Context(), &mcp.CallToolParams{Name: "sqlite_query", Arguments: map[string]any{"query": query}})
			require.NoError(t, err)
			var count int
			require.NoError(
				t,
				db.QueryRow("SELECT COUNT(*) FROM token_registry_entry WHERE subject='original' AND name='original'").
					Scan(&count),
			)
			require.Equal(t, 1, count, "query changed the original row")
			require.NoError(
				t,
				db.QueryRow("SELECT COUNT(*) FROM token_registry_entry").
					Scan(&count),
			)
			require.Equal(t, 1, count, "query inserted a row")
			require.True(
				t,
				result.IsError,
				"write CTE must be rejected before execution",
			)
			require.Error(t, ValidateReadOnlyQuery(query))
		})
	}
}

func TestSQLiteReadOnlyCTEControls(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t)
	cs := newToolSession(t, db, 100)
	for _, query := range []string{
		"WITH x AS (SELECT 1 AS n) SELECT n FROM x",
		"\ufeffWITH x AS (SELECT 1) \ufeffSELECT * FROM x",
		"WITH RECURSIVE x(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM x WHERE n<3) SELECT n FROM x",
		"WITH x(n) AS NOT MATERIALIZED (SELECT 1), y AS MATERIALIZED (SELECT n FROM x) SELECT * FROM y",
		"WITH x AS (SELECT ') DELETE') VALUES(1)",
		"WITH `UPDATE` AS (SELECT 1) SELECT * FROM `UPDATE`",
		"WITH [DELETE] AS (SELECT 1) SELECT * FROM [DELETE]",
		"WITH \"INSERT\" AS (SELECT 'it''s; DELETE') SELECT * FROM \"INSERT\"",
		"WITH/**/x AS (SELECT replace('UPDATE', 'UP', '')) -- DELETE\n SELECT * FROM x",
		"EXPLAIN WITH x AS (SELECT 1) DELETE FROM token_registry_entry",
		"EXPLAIN CREATE TABLE scratch (id INTEGER)",
		"EXPLAIN QUERY PLAN WITH x AS (SELECT 1) SELECT * FROM x",
		"SELECT 1 /* trailing comment",
		"SELECT '; DELETE FROM token_registry_entry' AS text; -- trailing comment",
		"PRAGMA table_info(token_registry_entry)",
	} {
		require.NoError(t, ValidateReadOnlyQuery(query), query)
		callTool(t, cs, "sqlite_query", map[string]any{"query": query}, false)
	}
	var created int
	require.NoError(
		t,
		db.QueryRow("SELECT COUNT(*) FROM sqlite_schema WHERE name='scratch'").
			Scan(&created),
	)
	require.Zero(t, created, "EXPLAIN must not create the table")
	for _, query := range []string{
		"WITH x AS (SELECT 1) SELECT 1; DELETE FROM token_registry_entry",
		"WITH x AS (SELECT 1) SELECT 1; /* comment */ UPDATE token_registry_entry SET name='x'",
		"SELECT 1; PRAGMA query_only=OFF",
		"EXPLAIN PRAGMA query_only=OFF",
		"WITH x AS (SELECT 'unterminated) SELECT 1",
	} {
		require.Error(t, ValidateReadOnlyQuery(query), query)
	}
}

func TestSQLiteExplainPragmaCannotChangeConnection(t *testing.T) {
	t.Parallel()
	for _, query := range []string{
		"EXPLAIN PRAGMA query_only=OFF",
		"EXPLAIN \ufeffPRAGMA query_only=OFF",
		"EXPLAIN\vPRAGMA query_only=OFF",
		"EXPLAIN QUERY PLAN /* comment */ PRAGMA query_only=OFF",
	} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()
			db := newSchemaDB(t)
			_, err := db.Exec("PRAGMA query_only=ON")
			require.NoError(t, err)
			cs := newToolSession(t, db, 100)
			result, err := cs.CallTool(
				t.Context(),
				&mcp.CallToolParams{
					Name:      "sqlite_query",
					Arguments: map[string]any{"query": query},
				},
			)
			require.NoError(t, err)
			var queryOnly int
			require.NoError(
				t,
				db.QueryRow("PRAGMA query_only").Scan(&queryOnly),
			)
			require.Equal(
				t,
				1,
				queryOnly,
				"EXPLAIN must not disable the connection guard",
			)
			require.True(t, result.IsError)
		})
	}
}

func TestSQLiteToolsErrorBranches(t *testing.T) {
	t.Parallel()

	db := newFixtureDB(t)

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
		"Returned 0 rows",
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

func TestSQLiteToolsNilDB(t *testing.T) {
	t.Parallel()

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

func TestSQLiteQueryResultBounds(t *testing.T) {
	t.Parallel()
	db := newSchemaDB(t)
	cs := newToolSession(t, db, 1)
	for _, query := range []string{"SELECT 1 AS n UNION ALL SELECT 2", "SELECT 1 AS n UNION ALL SELECT 2 LIMIT 100", "PRAGMA table_info(utxo)", "EXPLAIN SELECT 1"} {
		for _, limit := range []int{0, 100, 1} {
			text := callTool(
				t,
				cs,
				"sqlite_query",
				map[string]any{"query": query, "limit": limit},
				false,
			)
			require.Contains(t, text, "Returned 1 rows")
		}
	}
	text := callTool(
		t,
		cs,
		"sqlite_query",
		map[string]any{
			"query":  "SELECT 1 AS n UNION ALL SELECT 2 LIMIT 100",
			"offset": 1,
		},
		false,
	)
	require.Contains(t, text, "| 2 |")
}
