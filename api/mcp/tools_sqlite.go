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
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// SQLiteQueryParams defines the input schema for sqlite_query.
type SQLiteQueryParams struct {
	Query  string `json:"query"            jsonschema:"The SQL SELECT query to execute against Dingo's SQLite database"`
	Limit  int    `json:"limit,omitempty"  jsonschema:"Maximum rows to return (default 50, capped by configured maxRows and 200)"`
	Offset int    `json:"offset,omitempty" jsonschema:"Number of rows to skip for pagination (default 0)"`
}

// SQLiteExplainParams defines the input schema for sqlite_explain.
type SQLiteExplainParams struct {
	Query string `json:"query" jsonschema:"The SQL query to explain using EXPLAIN QUERY PLAN"`
}

// SQLiteTableSchemaParams defines the input schema for sqlite_table_schema.
type SQLiteTableSchemaParams struct {
	TableName string `json:"table_name" jsonschema:"Name of the SQLite table to inspect"`
}

// RegisterSQLiteTools registers the SQLite inspection and querying tools with the MCP server.
func RegisterSQLiteTools(
	server *mcp.Server,
	db *sql.DB,
	queryTimeout time.Duration,
	maxRows int,
) {
	if queryTimeout <= 0 {
		queryTimeout = defaultQueryTimeout
	}
	if maxRows <= 0 {
		maxRows = 100
	}

	// Tool: sqlite_query
	mcp.AddTool(server, &mcp.Tool{
		Name:        "sqlite_query",
		Description: "Execute a read-only SQL SELECT query against Dingo's Cardano metadata SQLite database. Output is formatted as a compact Markdown table with pagination support.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input SQLiteQueryParams) (*mcp.CallToolResult, any, error) {
		if db == nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: SQLite database is not available or not configured in read-only mode",
					},
				},
			}, nil, nil
		}

		if err := ValidateReadOnlyQuery(input.Query); err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Query validation failed: " + err.Error(),
					},
				},
			}, nil, nil
		}

		effectiveLimit := input.Limit
		if effectiveLimit <= 0 {
			effectiveLimit = 50
		}
		effectiveLimit = min(effectiveLimit, maxRows, 200)
		offset := max(input.Offset, 0)
		if offset > 10000 {
			return nil, nil, errors.New("offset must not exceed 10000")
		}
		q := strings.TrimRight(strings.TrimSpace(input.Query), ";")

		qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		conn, release, err := boundedSQLiteConn(qCtx, db)
		if err != nil {
			return nil, nil, err
		}
		defer release()
		start := time.Now()
		//nolint:gosec // q is validated by ValidateReadOnlyQuery
		rows, err := conn.QueryContext(qCtx, q)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf(
							"SQLite execution error: %s\nHint: Check table and column names with 'dingo://schema/tables' or 'sqlite_table_schema'.",
							err.Error(),
						),
					},
				},
			}, nil, nil
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Failed to get result columns: " + err.Error(),
					},
				},
			}, nil, nil
		}

		var resultRows [][]string
		rowValues := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range rowValues {
			valuePtrs[i] = &rowValues[i]
		}

		count := 0
		for rows.Next() {
			if offset > 0 {
				offset--
				continue
			}
			if err := rows.Scan(valuePtrs...); err != nil {
				return &mcp.CallToolResult{
					IsError: true,
					Content: []mcp.Content{
						&mcp.TextContent{
							Text: fmt.Sprintf(
								"Failed to scan row %d: %s",
								count+1,
								err.Error(),
							),
						},
					},
				}, nil, nil
			}

			rowFormatted := make([]string, len(cols))
			for i, val := range rowValues {
				rowFormatted[i] = FormatCell(val)
			}
			resultRows = append(resultRows, rowFormatted)
			count++
			if count >= effectiveLimit {
				break
			}
		}

		if err := rows.Err(); err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "SQLite rows iteration error: " + err.Error(),
					},
				},
			}, nil, nil
		}

		duration := time.Since(start)
		tableMarkdown := FormatMarkdownTable(cols, resultRows)
		summary := fmt.Sprintf(
			"\n*(Returned %d rows in %v | Query: `%s`)*\n",
			count,
			duration.Round(time.Millisecond),
			formatUntrustedInline(q),
		)

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: tableMarkdown + summary},
			},
		}, nil, nil
	})

	// Tool: sqlite_explain
	mcp.AddTool(server, &mcp.Tool{
		Name:        "sqlite_explain",
		Description: "Run EXPLAIN QUERY PLAN on a query to inspect SQLite's query execution strategy, index usage, and scan performance.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input SQLiteExplainParams) (*mcp.CallToolResult, any, error) {
		if db == nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: SQLite database is not available",
					},
				},
			}, nil, nil
		}

		cleanQ := strings.TrimRight(strings.TrimSpace(input.Query), ";")
		if err := ValidateReadOnlyQuery(input.Query); err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Query validation failed: " + err.Error(),
					},
				},
			}, nil, nil
		}

		// #nosec G202 //nolint:gosec // cleanQ is validated by ValidateReadOnlyQuery
		explainQuery := "EXPLAIN QUERY PLAN " + cleanQ
		qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		conn, release, err := boundedSQLiteConn(qCtx, db)
		if err != nil {
			return nil, nil, err
		}
		defer release()

		rows, err := conn.QueryContext(qCtx, explainQuery)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{Text: "EXPLAIN error: " + err.Error()},
				},
			}, nil, nil
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		var planRows [][]string
		rowValues := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range rowValues {
			valuePtrs[i] = &rowValues[i]
		}

		for rows.Next() {
			if err := rows.Scan(valuePtrs...); err == nil {
				rowFormatted := make([]string, len(cols))
				for i, val := range rowValues {
					rowFormatted[i] = FormatCell(val)
				}
				planRows = append(planRows, rowFormatted)
			}
		}

		if err := rows.Err(); err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "EXPLAIN rows iteration error: " + err.Error(),
					},
				},
			}, nil, nil
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{
					Text: "### Query Execution Plan\n\n" + FormatMarkdownTable(
						cols,
						planRows,
					),
				},
			},
		}, nil, nil
	})

	// Tool: sqlite_table_schema
	mcp.AddTool(server, &mcp.Tool{
		Name:        "sqlite_table_schema",
		Description: "Inspect the schema, column definitions, types, default values, primary keys, and indexes for a specific SQLite table.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input SQLiteTableSchemaParams) (*mcp.CallToolResult, any, error) {
		if db == nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "Error: SQLite database is not available",
					},
				},
			}, nil, nil
		}

		tableName := strings.TrimSpace(input.TableName)
		// Table name must be alphanumeric or underscore
		for _, r := range tableName {
			if !strings.ContainsRune(
				"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_",
				r,
			) {
				return &mcp.CallToolResult{
					IsError: true,
					Content: []mcp.Content{
						&mcp.TextContent{
							Text: "Invalid table name: must contain only alphanumeric characters and underscores",
						},
					},
				}, nil, nil
			}
		}

		qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		// Get DDL from sqlite_master
		var sqlDDL sql.NullString
		err := db.QueryRowContext(qCtx, "SELECT sql FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?", tableName).
			Scan(&sqlDDL)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: fmt.Sprintf(
							"Table '%s' not found: %s",
							tableName,
							err.Error(),
						),
					},
				},
			}, nil, nil
		}

		rows, err := db.QueryContext(
			qCtx,
			"SELECT * FROM pragma_table_info(?)", tableName,
		)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "PRAGMA table_info failed: " + err.Error(),
					},
				},
			}, nil, nil
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			return nil, nil, err
		}
		var colRows [][]string
		rowValues := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range rowValues {
			valuePtrs[i] = &rowValues[i]
		}

		for rows.Next() {
			if err := rows.Scan(valuePtrs...); err != nil {
				return nil, nil, err
			} else {
				rowFormatted := make([]string, len(cols))
				for i, val := range rowValues {
					rowFormatted[i] = FormatCell(val)
				}
				colRows = append(colRows, rowFormatted)
			}
		}

		if err := rows.Err(); err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{
					&mcp.TextContent{
						Text: "PRAGMA table_info row error: " + err.Error(),
					},
				},
			}, nil, nil
		}

		idxRows, qErr := db.QueryContext(
			qCtx,
			"SELECT * FROM pragma_index_list(?)", tableName,
		)
		var indexesMarkdown string
		if qErr != nil {
			return nil, nil, fmt.Errorf("read indexes: %w", qErr)
		}
		if idxRows != nil {
			defer idxRows.Close()
			idxCols, err := idxRows.Columns()
			if err != nil {
				return nil, nil, err
			}
			var idxData [][]string
			idxVal := make([]any, len(idxCols))
			idxPtr := make([]any, len(idxCols))
			for i := range idxVal {
				idxPtr[i] = &idxVal[i]
			}
			for idxRows.Next() {
				if err := idxRows.Scan(idxPtr...); err != nil {
					return nil, nil, err
				} else {
					rowFormatted := make([]string, len(idxCols))
					for i, val := range idxVal {
						rowFormatted[i] = FormatCell(val)
					}
					idxData = append(idxData, rowFormatted)
				}
			}
			if err := idxRows.Err(); err != nil {
				return nil, nil, err
			}
			if len(idxData) > 0 {
				indexesMarkdown = "\n### Indexes\n\n" + FormatMarkdownTable(
					idxCols,
					idxData,
				)
			}
		}

		out := fmt.Sprintf(
			"## Table: `%s`\n\n```sql\n%s\n```\n\n### Columns\n\n%s%s",
			tableName,
			formatUntrustedInline(sqlDDL.String),
			FormatMarkdownTable(cols, colRows),
			indexesMarkdown,
		)

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: out},
			},
		}, nil, nil
	})
}

// boundedSQLiteConn applies limits before SQLite materializes arbitrary values.
// Settings are restored before an injected pool's connection can be reused.
func boundedSQLiteConn(
	ctx context.Context,
	db *sql.DB,
) (*sql.Conn, func(), error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	var queryOnly int
	if err := conn.QueryRowContext(ctx, "PRAGMA query_only").Scan(&queryOnly); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	limits := []struct{ id, value, previous int }{
		{sqlite3.SQLITE_LIMIT_LENGTH, 1 << 20, 0},
		{sqlite3.SQLITE_LIMIT_SQL_LENGTH, maxSQLiteQueryLength, 0},
		{sqlite3.SQLITE_LIMIT_COLUMN, 128, 0},
	}
	applied := 0
	release := func() { //nolint:contextcheck // Cleanup must restore connection state after qCtx cancellation.
		// Cleanup must survive request cancellation before returning the connection.
		cleanupCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			time.Second,
		)
		defer cancel()
		var restoreErr error
		for _, limit := range limits[:applied] {
			_, err := sqlite.Limit(conn, limit.id, limit.previous)
			restoreErr = errors.Join(restoreErr, err)
		}
		if queryOnly == 0 {
			_, err := conn.ExecContext(cleanupCtx, "PRAGMA query_only=0")
			restoreErr = errors.Join(restoreErr, err)
		}
		if restoreErr != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
	}
	for i := range limits {
		previous, err := sqlite.Limit(conn, limits[i].id, -1)
		if err != nil {
			release()
			return nil, nil, err
		}
		limits[i].previous = previous
		if _, err := sqlite.Limit(conn, limits[i].id, min(previous, limits[i].value)); err != nil {
			release()
			return nil, nil, err
		}
		applied++
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA query_only=1"); err != nil {
		release()
		return nil, nil, err
	}
	return conn, release, nil
}
