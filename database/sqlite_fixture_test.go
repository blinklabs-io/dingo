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

package database

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

// rawSQLiteMetadataFixture is intentionally test-only. Production callers use
// the metadata contract; tests that must seed impossible/interrupted states
// can inspect the database without requiring a DB() escape hatch on Store.
func rawSQLiteMetadataFixture(
	t *testing.T,
	db *Database,
) *sql.DB {
	t.Helper()
	raw, err := sql.Open(
		"sqlite",
		"file:"+filepath.Join(db.DataDir(), "metadata.sqlite")+
			"?_pragma=busy_timeout(30000)&_pragma=foreign_keys(1)",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, raw.Close())
	})
	return raw
}
