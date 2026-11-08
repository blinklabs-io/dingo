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

package sqlstore

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	oldShapeRefPair = regexp.MustCompile(
		`\b((?:[A-Za-z_]+\.)?)tx_id = \? AND (?:[A-Za-z_]+\.)?output_idx = \?`,
	)
	oldShapeRefIn = regexp.MustCompile(
		`\b((?:[A-Za-z_]+\.)?)tx_id IN \(`,
	)
	oldShapeConflict = regexp.MustCompile(
		`ON CONFLICT \(tx_id, output_idx\)( WHERE)?`,
	)
)

// utxoRefAllowList names the source files that may spell an output reference
// as (tx_id, output_idx), each with the reason. Every other file must reach
// the utxo table on SQLite through the producer and imported indexes, which
// utxoSQL and sqliteRefs spell out.
var utxoRefAllowList = map[string]string{
	"utxo_sql.go": "defines the PostgreSQL/MySQL forms, which keep the " +
		"unique (tx_id, output_idx) index, next to the SQLite forms, whose " +
		"imported-row arm is keyed by tx_id",
}

// TestNoSourceLooksUpUtxoByHashPairOnSQLite fails when a statement in the
// package's sources names an output by (tx_id, output_idx) outside
// utxoRefAllowList. On SQLite the unique index over every output's hash does
// not exist, so such a statement is correct but scans.
func TestNoSourceLooksUpUtxoByHashPairOnSQLite(t *testing.T) {
	t.Parallel()
	var files []string
	for _, pattern := range []string{
		"*.go", "queries/sqlite/*.sql", "internal/query/sqlite/*.go",
		"migrations/*.go",
	} {
		matches, err := filepath.Glob(pattern)
		require.NoError(t, err)
		files = append(files, matches...)
	}
	require.NotEmpty(t, files)
	scanned := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		scanned++
		raw, err := os.ReadFile(file)
		require.NoError(t, err)
		for i, line := range strings.Split(string(raw), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") ||
				strings.HasPrefix(trimmed, "--") {
				continue
			}
			for _, pattern := range []*regexp.Regexp{
				oldShapeRefPair, oldShapeRefIn, oldShapeConflict,
			} {
				if !pattern.MatchString(line) {
					continue
				}
				if _, ok := utxoRefAllowList[filepath.Base(file)]; ok {
					continue
				}
				t.Errorf(
					"%s:%d: output looked up by (tx_id, output_idx) with no "+
						"allow-list entry: %s",
					file, i+1, trimmed,
				)
			}
		}
	}
	require.Greater(t, scanned, 50, "the scan must cover the package")
	for file := range utxoRefAllowList {
		_, err := os.Stat(file)
		require.NoError(t, err, "stale allow-list entry %s", file)
	}
}

// TestSQLiteUtxoStatementsKeyOutputsByProducer checks the texts of every
// SQLite statement that names an output: the hash pair may appear only as the
// imported-row arm (alias ref, with the imported index predicate), and the
// conflict target (tx_id, output_idx) only as the partial imported index.
func TestSQLiteUtxoStatementsKeyOutputsByProducer(t *testing.T) {
	t.Parallel()
	statements := map[string]string{
		"consumeUtxoSQLiteReturningSQL": consumeUtxoSQLiteReturningSQL,
	}
	v := reflect.ValueOf(*sqliteUtxoSQL)
	for i := range v.NumField() {
		if v.Field(i).Kind() == reflect.String && v.Field(i).String() != "" {
			statements["utxoSQL."+v.Type().Field(i).Name] = v.Field(i).String()
		}
	}
	v = reflect.ValueOf(*sqliteRefs)
	for i := range v.NumField() {
		name := v.Type().Field(i).Name
		switch field := v.Field(i); field.Kind() {
		case reflect.Map:
			for _, key := range field.MapKeys() {
				value := field.MapIndex(key)
				if value.Kind() == reflect.String {
					statements[fmt.Sprintf("%s[%v]", name, key)] = value.String()
					continue
				}
				for _, inner := range value.MapKeys() {
					statements[fmt.Sprintf("%s[%v][%v]", name, key, inner)] =
						value.MapIndex(inner).String()
				}
			}
		case reflect.Array:
			for n := range field.Len() {
				for _, key := range field.Index(n).MapKeys() {
					statements[fmt.Sprintf("%s[%d][%v]", name, n, key)] =
						field.Index(n).MapIndex(key).String()
				}
			}
		}
	}
	require.Greater(t, len(statements), 100)
	for name, text := range statements {
		for _, m := range oldShapeRefPair.FindAllStringSubmatch(text, -1) {
			require.Equal(t, "ref.", m[1], "%s: bare hash pair: %s", name, text)
		}
		for _, m := range oldShapeRefIn.FindAllStringSubmatch(text, -1) {
			if strings.Contains(text, "INDEXED BY idx_utxo_imported_ref") {
				continue
			}
			require.Equal(t, "ref.", m[1], "%s: bare hash list: %s", name, text)
		}
		for _, m := range oldShapeConflict.FindAllStringSubmatch(text, -1) {
			require.Equal(
				t, " WHERE", m[1],
				"%s: conflict target without the imported-row predicate: %s",
				name, text,
			)
		}
	}
}

var singleColumnHashLookup = regexp.MustCompile(
	`(?i)\b(?:[a-z_]+\.)?tx_id\s*(?:=|IN\s*\(|IN\s*\?|ANY\b)`,
)

// utxoHashLookupAllowList names the non-test sources outside this package's
// own pair scan that may filter utxo by the bare tx_id column.
var utxoHashLookupAllowList = map[string]string{
	"database/plugin/metadata/sqlstore/utxo_sql.go": "defines the " +
		"PostgreSQL/MySQL forms and the SQLite imported-row arms",
	"database/plugin/metadata/sqlstore/utxo_overlay.go": "the imported-row " +
		"arm of the overlay prefetch uses idx_utxo_imported_ref",
}

// TestNoSourceFiltersUtxoByBareTxIDOnSQLite scans api, database and internal:
// on SQLite no index leads with utxo.tx_id for produced rows, so a statement
// filtering by it alone scans the table. Readers address a transaction's
// outputs through producer_tx_id.
func TestNoSourceFiltersUtxoByBareTxIDOnSQLite(t *testing.T) {
	t.Parallel()
	root := filepath.Join("..", "..", "..", "..")
	scanned := 0
	for _, dir := range []string{"api", "database", "internal"} {
		err := filepath.WalkDir(
			filepath.Join(root, dir),
			func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if d.IsDir() {
					return nil
				}
				name := d.Name()
				if strings.HasSuffix(name, "_test.go") ||
					(!strings.HasSuffix(name, ".go") &&
						!strings.HasSuffix(name, ".sql")) {
					return nil
				}
				rel, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				rel = filepath.ToSlash(rel)
				if strings.Contains(rel, "/migrations/v") {
					return nil
				}
				if _, ok := utxoHashLookupAllowList[rel]; ok {
					return nil
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				scanned++
				for i, line := range strings.Split(string(raw), "\n") {
					trimmed := strings.TrimSpace(line)
					if strings.HasPrefix(trimmed, "//") ||
						strings.HasPrefix(trimmed, "--") {
						continue
					}
					if singleColumnHashLookup.MatchString(line) {
						t.Errorf(
							"%s:%d: filters by bare tx_id, which no SQLite "+
								"index serves for produced rows: %s",
							rel, i+1, trimmed,
						)
					}
				}
				return nil
			},
		)
		require.NoError(t, err)
	}
	require.Greater(t, scanned, 500, "the scan must cover the tree")
	for file := range utxoHashLookupAllowList {
		_, err := os.Stat(filepath.Join(root, file))
		require.NoError(t, err, "stale allow-list entry %s", file)
	}
}
