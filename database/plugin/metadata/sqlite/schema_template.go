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

package sqlite

import (
	"fmt"
	"strings"
	"sync"
)

// Creating an in-memory store means migrating roughly eighty models from
// nothing, which is the single most expensive part of Start and is paid again
// for every store. A process that builds hundreds of them (a package of
// database-backed tests) spends most of its time in gorm's migrator rather
// than in the code under test.
//
// The result of that work is a constant. An in-memory database is always
// empty at Start, the model list is fixed at compile time, and every
// pre-migration fixup returns early when its table is absent, so the schema
// the chain produces cannot vary between stores in one process. The first
// in-memory store therefore runs the real migration and records the schema it
// ended up with; later ones replay that DDL.
//
// The template is deliberately not shared across processes and not used for
// file-backed stores. A file-backed database may hold an older schema, or
// data that a fixup needs to repair, so it always runs the full chain.
var (
	inMemorySchemaMu       sync.Mutex
	inMemorySchemaTemplate []string
)

// sqliteMasterEntry is the DDL of one row of the schema catalog.
type sqliteMasterEntry struct {
	SQL string
}

// captureInMemorySchemaTemplate records the DDL of a freshly migrated
// in-memory database. It is a no-op once a template exists.
func (d *MetadataStoreSqlite) captureInMemorySchemaTemplate() error {
	inMemorySchemaMu.Lock()
	recorded := inMemorySchemaTemplate != nil
	inMemorySchemaMu.Unlock()
	if recorded {
		return nil
	}
	ddl, err := d.dumpSchemaDDL()
	if err != nil {
		return fmt.Errorf("failed to capture in-memory schema: %w", err)
	}
	if len(ddl) == 0 {
		// Nothing to replay. Leave the template unset so the next store
		// retries the capture rather than caching an empty schema.
		return nil
	}
	inMemorySchemaMu.Lock()
	if inMemorySchemaTemplate == nil {
		inMemorySchemaTemplate = ddl
	}
	inMemorySchemaMu.Unlock()
	return nil
}

// migrateInMemoryFromTemplate applies the recorded schema to this store and
// reports whether it did. It declines, leaving the caller to run the full
// migration chain, when no template has been recorded yet or when the
// database already holds objects of its own.
func (d *MetadataStoreSqlite) migrateInMemoryFromTemplate() (bool, error) {
	inMemorySchemaMu.Lock()
	template := inMemorySchemaTemplate
	inMemorySchemaMu.Unlock()
	if template == nil {
		return false, nil
	}
	empty, err := d.schemaIsEmpty()
	if err != nil {
		return false, err
	}
	if !empty {
		return false, nil
	}
	// Issue the whole schema as one script on the raw connection. Going
	// through gorm would prepare and cache a statement per CREATE, which
	// costs more than the migration this is replacing. The script is the DDL
	// SQLite itself reported for a database this process migrated, so it
	// carries no caller-supplied text.
	sqlDB, err := d.db.DB()
	if err != nil {
		return false, fmt.Errorf(
			"failed to get sql.DB for schema template: %w",
			err,
		)
	}
	var script strings.Builder
	for _, stmt := range template {
		script.WriteString(stmt)
		script.WriteString(";\n")
	}
	if _, err := sqlDB.Exec(script.String()); err != nil {
		return false, fmt.Errorf(
			"failed to apply schema template: %w",
			err,
		)
	}
	return true, nil
}

// schemaIsEmpty reports whether the database contains no user-defined objects.
func (d *MetadataStoreSqlite) schemaIsEmpty() (bool, error) {
	var count int64
	if result := d.db.Raw(
		`SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'`,
	).Scan(&count); result.Error != nil {
		return false, fmt.Errorf(
			"failed to inspect schema catalog: %w",
			result.Error,
		)
	}
	return count == 0, nil
}

// dumpSchemaDDL returns the CREATE statements for every user-defined object,
// in creation order. Rows with no SQL text are SQLite's own implicit indexes
// for PRIMARY KEY and UNIQUE constraints; those are recreated by the CREATE
// TABLE statement that owns them and cannot be issued directly.
func (d *MetadataStoreSqlite) dumpSchemaDDL() ([]string, error) {
	var entries []sqliteMasterEntry
	if result := d.db.Raw(
		`SELECT sql FROM sqlite_master ` +
			`WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ` +
			`ORDER BY rowid`,
	).Scan(&entries); result.Error != nil {
		return nil, fmt.Errorf(
			"failed to read schema catalog: %w",
			result.Error,
		)
	}
	ddl := make([]string, 0, len(entries))
	for _, entry := range entries {
		stmt := strings.TrimSpace(entry.SQL)
		if stmt == "" {
			continue
		}
		ddl = append(ddl, stmt)
	}
	return ddl, nil
}
