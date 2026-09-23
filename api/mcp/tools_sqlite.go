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
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// allowedPragmaRegex matches read-only metadata pragmas
var allowedPragmaRegex = regexp.MustCompile(
	`(?i)^\s*PRAGMA\s+(table_info|index_list|index_info|foreign_key_list)\s*\(`,
)

// ValidateReadOnlyQuery ensures that the given query is strictly a read-only query.
func ValidateReadOnlyQuery(query string) error {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return errors.New("empty query")
	}

	// Reject semicolons to prevent multi-statement injection
	if strings.Contains(trimmed, ";") {
		// If semicolon is at the very end, trim it off
		if strings.TrimRight(
			trimmed,
			"; \t\r\n",
		) == strings.TrimRight(
			trimmed,
			";",
		) {
			trimmed = strings.TrimRight(trimmed, "; \t\r\n")
		}
		if strings.Contains(trimmed, ";") {
			return errors.New(
				"multiple statements or semicolons are not permitted for security",
			)
		}
	}

	upper := strings.ToUpper(trimmed)

	// Check for dangerous keywords that indicate write or schema modifications
	disallowedKeywords := []string{
		"INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ",
		"CREATE ", "REPLACE ", "ATTACH ", "DETACH ", "VACUUM",
		"REINDEX ", "TRUNCATE ",
	}
	for _, kw := range disallowedKeywords {
		if strings.HasPrefix(upper, kw) {
			return fmt.Errorf(
				"mutation statements are strictly forbidden: %s",
				strings.TrimSpace(kw),
			)
		}
	}

	// Check if it's an allowed read-only PRAGMA
	if strings.HasPrefix(upper, "PRAGMA") {
		if !allowedPragmaRegex.MatchString(trimmed) {
			return errors.New(
				"only read-only PRAGMAs (table_info, index_list, index_info, foreign_key_list) are allowed",
			)
		}
		return nil
	}

	// Must begin with SELECT, WITH (CTE), or EXPLAIN
	if !strings.HasPrefix(upper, "SELECT") &&
		!strings.HasPrefix(upper, "WITH") &&
		!strings.HasPrefix(upper, "EXPLAIN") {
		return errors.New("query must be a SELECT, WITH, or EXPLAIN statement")
	}

	return nil
}

// SQLiteQueryParams defines the input schema for sqlite_query.
type SQLiteQueryParams struct {
	Query  string `json:"query"            jsonschema:"The SQL SELECT query to execute against Dingo's SQLite database"`
	Limit  int    `json:"limit,omitempty"  jsonschema:"Maximum number of rows to return (default 50, max 200)"`
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
		queryTimeout = 5 * time.Second
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

		// Enforce pagination limits
		effectiveLimit := input.Limit
		if effectiveLimit <= 0 || effectiveLimit > maxRows {
			effectiveLimit = 50
		}
		if effectiveLimit > 200 {
			effectiveLimit = 200
		}

		q := strings.TrimRight(strings.TrimSpace(input.Query), ";")
		upperQ := strings.ToUpper(q)
		if !strings.Contains(upperQ, " LIMIT ") {
			if input.Offset > 0 {
				q = fmt.Sprintf(
					"%s LIMIT %d OFFSET %d",
					q,
					effectiveLimit,
					input.Offset,
				)
			} else {
				q = fmt.Sprintf("%s LIMIT %d", q, effectiveLimit)
			}
		}

		qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		start := time.Now()
		//nolint:gosec // q is validated by ValidateReadOnlyQuery
		rows, err := db.QueryContext(qCtx, q)
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
			if count >= 200 {
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
			q,
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
		if err := ValidateReadOnlyQuery(cleanQ); err != nil {
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

		rows, err := db.QueryContext(qCtx, explainQuery)
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

		// Get columns via PRAGMA table_info
		//nolint:gosec // tableName is validated to be alphanumeric/underscore only
		rows, err := db.QueryContext(
			qCtx,
			fmt.Sprintf("PRAGMA table_info(%s)", tableName),
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

		cols, _ := rows.Columns()
		var colRows [][]string
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

		// Get indexes via PRAGMA index_list
		//nolint:gosec // tableName is validated to be alphanumeric/underscore only
		idxRows, qErr := db.QueryContext(
			qCtx,
			fmt.Sprintf("PRAGMA index_list(%s)", tableName),
		)
		var indexesMarkdown string
		if qErr == nil && idxRows != nil {
			defer idxRows.Close()
			idxCols, _ := idxRows.Columns()
			var idxData [][]string
			idxVal := make([]any, len(idxCols))
			idxPtr := make([]any, len(idxCols))
			for i := range idxVal {
				idxPtr[i] = &idxVal[i]
			}
			for idxRows.Next() {
				if err := idxRows.Scan(idxPtr...); err == nil {
					rowFormatted := make([]string, len(idxCols))
					for i, val := range idxVal {
						rowFormatted[i] = FormatCell(val)
					}
					idxData = append(idxData, rowFormatted)
				}
			}
			if err := idxRows.Err(); err == nil && len(idxData) > 0 {
				indexesMarkdown = "\n### Indexes\n\n" + FormatMarkdownTable(
					idxCols,
					idxData,
				)
			}
		}

		out := fmt.Sprintf(
			"## Table: `%s`\n\n```sql\n%s\n```\n\n### Columns\n\n%s%s",
			tableName,
			sqlDDL.String,
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
