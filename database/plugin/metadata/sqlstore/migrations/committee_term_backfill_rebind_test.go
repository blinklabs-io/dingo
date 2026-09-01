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

package migrations

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

// The committee term-start backfill is data driven, so its SQL never passes
// through the migration DDL translator. A dialect that does not take ? has to
// see its own placeholders, which is why Batch carries the rebinder.
func TestCommitteeTermStartBackfillUsesDialectPlaceholders(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	registry, err := SQLiteRegistry()
	require.NoError(t, err)

	var rebound []string
	runner := Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   NewFileLocker(databasePath + ".migrate.lock"),
		Rebind: func(query string) string {
			if strings.Contains(query, "committee_member") {
				rebound = append(rebound, query)
			}
			return query
		},
	}
	require.NoError(t, runner.Run(context.Background()))

	require.NotEmpty(
		t,
		rebound,
		"the committee term-start backfill must route its SQL through Batch.Rebind",
	)
	for _, query := range rebound {
		require.Contains(
			t,
			query,
			"?",
			"the backfill must hand the rebinder ? placeholders",
		)
	}
}

// Batch.Rebind is never nil, so a backfill can call it unconditionally.
func TestBackfillBatchRebindDefaultsToIdentity(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	registry, err := SQLiteRegistry()
	require.NoError(t, err)
	runner := Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   NewFileLocker(databasePath + ".migrate.lock"),
	}
	require.NoError(t, runner.Run(context.Background()))
}
