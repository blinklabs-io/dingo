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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

func TestMaintenanceStatusSeparatesCriticalAndBackgroundIndexes(t *testing.T) {
	t.Parallel()
	nodeDB, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	db, err := dbtest.RawSQLiteMetadata(t, nodeDB)
	require.NoError(t, err)
	text, err := sqliteMaintenanceStatus(t.Context(), db)
	require.NoError(t, err)
	require.Contains(t, text, "**Metadata Index Readiness**: ready")
	var critical, lazy string
	for _, idx := range deferred.Manifest {
		if idx.Critical {
			critical = idx.Name
		} else {
			lazy = idx.Name
		}
	}
	require.NotEmpty(t, lazy)
	require.NotEmpty(t, critical)
	_, err = db.Exec("DROP INDEX " + lazy)
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO sync_state(sync_key,value) VALUES(?,'true')",
		deferred.SyncStateKey,
	)
	require.NoError(t, err)
	text, err = sqliteMaintenanceStatus(t.Context(), db)
	require.NoError(t, err)
	require.Contains(t, text, "**Metadata Index Readiness**: ready")
	require.Contains(t, text, "pending (1 background indexes missing)")
	_, err = db.Exec("DROP INDEX " + critical)
	require.NoError(t, err)
	text, err = sqliteMaintenanceStatus(t.Context(), db)
	require.NoError(t, err)
	require.Contains(t, text, "not ready; missing critical indexes: "+critical)
	_, err = db.Exec(
		"INSERT INTO sync_state(sync_key,value) VALUES('sync_status','backfill')",
	)
	require.NoError(t, err)
	text, err = sqliteMaintenanceStatus(t.Context(), db)
	require.NoError(t, err)
	require.Contains(t, text, "not ready; import state:")
}
