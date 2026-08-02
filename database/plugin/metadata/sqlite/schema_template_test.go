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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// schemaCatalog is a comparable description of a database schema: the
// non-table objects verbatim from the schema catalog, and each table's
// columns and foreign keys read back through PRAGMA.
//
// Tables are not compared as raw DDL text because gorm emits a table's
// foreign key clauses in Go map order, so two stores in one process can spell
// the same table differently. The clause set is what matters and is what this
// compares.
type schemaCatalog struct {
	objects []string
	tables  map[string][]string
}

// columnInfo is one row of PRAGMA table_info.
type columnInfo struct {
	Name      string
	Type      string
	DfltValue *string
	NotNull   int
	PK        int
}

// foreignKeyInfo is one row of PRAGMA foreign_key_list.
type foreignKeyInfo struct {
	Table    string
	From     string
	To       *string
	OnUpdate string
	OnDelete string
	Match    string
}

// readSchemaCatalog collects everything the template has to reproduce.
func readSchemaCatalog(
	t *testing.T,
	store *MetadataStoreSqlite,
) schemaCatalog {
	t.Helper()
	catalog := schemaCatalog{tables: map[string][]string{}}
	var rows []struct {
		Type string
		Name string
		SQL  string
	}
	require.NoError(t, store.DB().Raw(
		`SELECT type, name, coalesce(sql, '') AS sql FROM sqlite_master `+
			`ORDER BY type, name`,
	).Scan(&rows).Error)
	require.NotEmpty(t, rows)
	for _, row := range rows {
		if row.Type != "table" {
			catalog.objects = append(
				catalog.objects,
				fmt.Sprintf("%s %s %s", row.Type, row.Name, row.SQL),
			)
			continue
		}
		var columns []columnInfo
		require.NoError(t, store.DB().Raw(
			fmt.Sprintf("PRAGMA table_info(%q)", row.Name),
		).Scan(&columns).Error)
		var foreignKeys []foreignKeyInfo
		require.NoError(t, store.DB().Raw(
			fmt.Sprintf("PRAGMA foreign_key_list(%q)", row.Name),
		).Scan(&foreignKeys).Error)
		details := make([]string, 0, len(columns)+len(foreignKeys))
		for _, column := range columns {
			dflt := "<nil>"
			if column.DfltValue != nil {
				dflt = *column.DfltValue
			}
			details = append(details, fmt.Sprintf(
				"column %s %s notnull=%d default=%s pk=%d",
				column.Name, column.Type, column.NotNull, dflt, column.PK,
			))
		}
		for _, fk := range foreignKeys {
			to := "<nil>"
			if fk.To != nil {
				to = *fk.To
			}
			details = append(details, fmt.Sprintf(
				"foreignkey %s->%s.%s onupdate=%s ondelete=%s match=%s",
				fk.From, fk.Table, to, fk.OnUpdate, fk.OnDelete, fk.Match,
			))
		}
		sort.Strings(details)
		catalog.tables[row.Name] = details
	}
	return catalog
}

// startInMemoryStore starts an in-memory store, optionally forcing it through
// the full migration chain rather than the recorded schema template.
func startInMemoryStore(
	t *testing.T,
	skipTemplate bool,
) *MetadataStoreSqlite {
	t.Helper()
	store, err := New("", nil, nil)
	require.NoError(t, err)
	store.skipSchemaTemplate = skipTemplate
	require.NoError(t, store.Start())
	t.Cleanup(func() {
		store.Close() //nolint:errcheck
	})
	return store
}

// TestInMemorySchemaTemplateMatchesFullMigration pins the assumption the
// template rests on: replaying the recorded DDL leaves an in-memory database
// in exactly the state the full migration chain would have produced. If a
// model, index or fixup ever makes the chain's output depend on something
// other than the compile-time model set, the two catalogs diverge here.
func TestInMemorySchemaTemplateMatchesFullMigration(t *testing.T) {
	migrated := startInMemoryStore(t, true)
	// The first in-memory store to finish the chain records the template, so
	// this store either records it or replays one recorded earlier. Either
	// way a template exists afterwards, and the store after it replays.
	startInMemoryStore(t, false)
	templated := startInMemoryStore(t, false)

	inMemorySchemaMu.Lock()
	template := inMemorySchemaTemplate
	inMemorySchemaMu.Unlock()
	require.NotEmpty(
		t,
		template,
		"an in-memory store should have recorded a schema template",
	)

	want := readSchemaCatalog(t, migrated)
	got := readSchemaCatalog(t, templated)
	require.Equal(
		t,
		want.objects,
		got.objects,
		"non-table schema objects diverge from the full migration chain",
	)
	require.Equal(
		t,
		sortedKeys(want.tables),
		sortedKeys(got.tables),
		"table set diverges from the full migration chain",
	)
	for _, name := range sortedKeys(want.tables) {
		require.Equal(
			t,
			want.tables[name],
			got.tables[name],
			"table %s diverges from the full migration chain",
			name,
		)
	}
}

// TestInMemorySchemaTemplateDeclinesNonEmptyDatabase checks the guard that
// keeps the template from being applied over a database that already has
// objects of its own, which would fail on the first duplicate CREATE.
func TestInMemorySchemaTemplateDeclinesNonEmptyDatabase(t *testing.T) {
	store := startInMemoryStore(t, false)

	inMemorySchemaMu.Lock()
	hasTemplate := len(inMemorySchemaTemplate) > 0
	inMemorySchemaMu.Unlock()
	require.True(t, hasTemplate)

	empty, err := store.schemaIsEmpty()
	require.NoError(t, err)
	require.False(t, empty, "a migrated store is not an empty schema")

	applied, err := store.migrateInMemoryFromTemplate()
	require.NoError(t, err)
	require.False(
		t,
		applied,
		"the template must not be replayed over an existing schema",
	)
}

func sortedKeys(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
