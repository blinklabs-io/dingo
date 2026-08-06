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
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

func TestSQLiteVersionOneSchemaIsDeterministic(t *testing.T) {
	t.Parallel()
	firstDB, err := migratedSQLiteV1(t)
	require.NoError(t, err)
	secondDB, err := migratedSQLiteV1(t)
	require.NoError(t, err)

	first, err := normalizedSQLiteSchema(firstDB)
	require.NoError(t, err)
	second, err := normalizedSQLiteSchema(secondDB)
	require.NoError(t, err)
	require.NotEmpty(t, first)
	require.Equal(t, first, second)
}

func migratedSQLiteV1(t *testing.T) (*sql.DB, error) {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	registry, err := migrations.SQLiteRegistry()
	if err != nil {
		return nil, err
	}
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker: migrations.NewFileLocker(
			databasePath + ".migrate.lock",
		),
	}
	if err := runner.Run(context.Background()); err != nil {
		return nil, err
	}
	return db, nil
}

func normalizedSQLiteSchema(db *sql.DB) ([]string, error) {
	rows, err := db.Query(
		`SELECT name FROM sqlite_master
		 WHERE type = 'table'
		   AND name NOT LIKE 'sqlite_%'
		   AND name <> 'schema_migrations'
		 ORDER BY name`,
	)
	if err != nil {
		return nil, err
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			_ = rows.Close()
			return nil, err
		}
		tables = append(tables, table)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var schema []string
	for _, table := range tables {
		schema = append(schema, "table:"+table)
		columns, err := db.Query(
			"PRAGMA table_info(" + quoteSQLite(table) + ")",
		)
		if err != nil {
			return nil, err
		}
		for columns.Next() {
			var (
				cid        int
				name       string
				columnType string
				notNull    int
				defaultVal sql.NullString
				primaryKey int
			)
			if err := columns.Scan(
				&cid,
				&name,
				&columnType,
				&notNull,
				&defaultVal,
				&primaryKey,
			); err != nil {
				_ = columns.Close()
				return nil, err
			}
			schema = append(schema, fmt.Sprintf(
				"column:%s:%d:%s:%s:%d:%s:%t:%d",
				table,
				cid,
				name,
				columnType,
				notNull,
				defaultVal.String,
				defaultVal.Valid,
				primaryKey,
			))
		}
		if err := columns.Close(); err != nil {
			return nil, err
		}
		if err := columns.Err(); err != nil {
			return nil, err
		}
		indexes, err := db.Query(
			"PRAGMA index_list(" + quoteSQLite(table) + ")",
		)
		if err != nil {
			return nil, err
		}
		type indexMetadata struct {
			name    string
			unique  int
			origin  string
			partial int
		}
		var tableIndexes []indexMetadata
		for indexes.Next() {
			var (
				sequence int
				name     string
				unique   int
				origin   string
				partial  int
			)
			if err := indexes.Scan(
				&sequence,
				&name,
				&unique,
				&origin,
				&partial,
			); err != nil {
				_ = indexes.Close()
				return nil, err
			}
			schema = append(schema, fmt.Sprintf(
				"index:%s:%s:%d:%s:%d",
				table,
				name,
				unique,
				origin,
				partial,
			))
			tableIndexes = append(tableIndexes, indexMetadata{
				name:    name,
				unique:  unique,
				origin:  origin,
				partial: partial,
			})
		}
		if err := indexes.Close(); err != nil {
			return nil, err
		}
		if err := indexes.Err(); err != nil {
			return nil, err
		}
		for _, index := range tableIndexes {
			indexColumns, err := db.Query(
				"PRAGMA index_info(" + quoteSQLite(index.name) + ")",
			)
			if err != nil {
				return nil, err
			}
			for indexColumns.Next() {
				var indexSequence, cid int
				var column string
				if err := indexColumns.Scan(
					&indexSequence,
					&cid,
					&column,
				); err != nil {
					_ = indexColumns.Close()
					return nil, err
				}
				schema = append(schema, fmt.Sprintf(
					"index-column:%s:%s:%d:%d:%s",
					table,
					index.name,
					indexSequence,
					cid,
					column,
				))
			}
			if err := indexColumns.Close(); err != nil {
				return nil, err
			}
			if err := indexColumns.Err(); err != nil {
				return nil, err
			}
		}
		foreignKeys, err := db.Query(
			"PRAGMA foreign_key_list(" + quoteSQLite(table) + ")",
		)
		if err != nil {
			return nil, err
		}
		for foreignKeys.Next() {
			var id, sequence int
			var parent, from, to, onUpdate, onDelete, match string
			if err := foreignKeys.Scan(
				&id,
				&sequence,
				&parent,
				&from,
				&to,
				&onUpdate,
				&onDelete,
				&match,
			); err != nil {
				_ = foreignKeys.Close()
				return nil, err
			}
			schema = append(schema, fmt.Sprintf(
				"foreign-key:%s:%s:%s:%s:%s:%s:%s",
				table,
				parent,
				from,
				to,
				onUpdate,
				onDelete,
				match,
			))
		}
		if err := foreignKeys.Close(); err != nil {
			return nil, err
		}
		if err := foreignKeys.Err(); err != nil {
			return nil, err
		}
	}
	sort.Strings(schema)
	return schema, nil
}

func quoteSQLite(identifier string) string {
	quoted := `"`
	for _, character := range identifier {
		quoted += string(character)
		if character == '"' {
			quoted += `"`
		}
	}
	return quoted + `"`
}
