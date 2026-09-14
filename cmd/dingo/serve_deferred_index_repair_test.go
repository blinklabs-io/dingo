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

package main

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// TestServeCoreModeRepairsMissingCascadeIndex covers the composition serveRun
// actually takes: a core-mode configuration reaches repairDeferredIndexes,
// which is the only deferred-index entry point on that path.
//
// The state under test is the one two Mithril-bootstrapped preview nodes were
// found in: idx_utxo_transaction_id absent with no pending marker to record
// it, so every rollback's DELETE FROM "transaction" cascade scanned utxo once
// per deleted transaction row.
func TestServeCoreModeRepairsMissingCascadeIndex(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	cfg := &config.Config{
		DatabasePath:    dir,
		DatabaseWorkers: 1,
		Plugins:         testStoragePlugins(),
	}
	require.False(
		t,
		effectiveStorageMode(cfg).IsAPI(),
		"serveRun sends this configuration down the core-mode branch, "+
			"which is the branch repairDeferredIndexes is wired into",
	)

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: dir,
		Logger:  logger,
	})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec("DROP INDEX IF EXISTS idx_utxo_transaction_id")
	require.NoError(t, err)
	_, err = raw.Exec(
		"DELETE FROM sync_state WHERE sync_key = ?",
		deferred.SyncStateKey,
	)
	require.NoError(t, err)
	require.NoError(t, dbtest.CloseDatabase(db))

	require.NoError(t, repairDeferredIndexes(cfg, logger))

	var count int
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
		"idx_utxo_transaction_id",
	).Scan(&count))
	require.Equal(
		t,
		1,
		count,
		"core-mode serve must restore the rollback cascade index before "+
			"the node starts",
	)
}
