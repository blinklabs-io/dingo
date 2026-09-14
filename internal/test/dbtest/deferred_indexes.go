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

package dbtest

import (
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
)

// MetadataIndexExists reports whether the named index is present in the
// SQLite catalog behind raw (see RawSQLiteMetadata). Deferred-index tests in
// several packages assert against the catalog rather than a store method,
// because the state under test is "the manifest disagrees with the schema",
// which no store method can express.
func MetadataIndexExists(tb testing.TB, raw *sql.DB, name string) bool {
	tb.Helper()
	var count int
	if err := raw.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
		name,
	).Scan(&count); err != nil {
		tb.Fatalf("query sqlite_master for index %s: %v", name, err)
	}
	return count == 1
}

// LazyManifestIndex returns the name of a non-critical deferred-index manifest
// entry, so a test can distinguish the full rebuild from the critical subset
// without pinning an index the manifest may reclassify later.
func LazyManifestIndex(tb testing.TB) string {
	tb.Helper()
	for _, index := range deferred.Manifest {
		if !index.Critical {
			return index.Name
		}
	}
	tb.Fatal("the manifest must keep at least one lazy entry")
	return ""
}
