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

package plannerstats_test

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/plannerstats"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// A node synced from genesis never ran ANALYZE. Opening the real file-backed
// store and publishing the ledger's epoch transition must produce
// sqlite_stat1, and a second transition must find nothing left to change.
func TestEpochTransitionCreatesStat1OnGenesisStyleDatabase(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, err := sqlite.NewSQLStore(
		sqlite.Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	defer func() { require.NoError(t, store.Close()) }()

	probe, err := sql.Open(
		"sqlite",
		"file:"+filepath.Join(dataDir, "metadata.sqlite")+
			"?_pragma=busy_timeout(30000)&_pragma=synchronous(OFF)",
	)
	require.NoError(t, err)
	defer probe.Close()
	_, err = probe.Exec(
		"WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c " +
			"WHERE i < 300) INSERT INTO utxo (tx_id, output_idx, " +
			"staking_key, credential_tag, added_slot, deleted_slot, amount) " +
			"SELECT randomblob(32), i % 4, randomblob(28), 0, i, 0, '1' FROM c",
	)
	require.NoError(t, err)
	stat1 := func() int {
		var n int
		require.NoError(t, probe.QueryRow(
			"SELECT COUNT(*) FROM sqlite_schema WHERE name = 'sqlite_stat1'",
		).Scan(&n))
		return n
	}
	require.Zero(t, stat1(), "fixture must start without sqlite_stat1")

	bus := event.NewEventBus(nil, nil)
	manager := plannerstats.NewManager(
		store, bus, slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	require.NoError(t, manager.Start(t.Context()))
	defer func() { require.NoError(t, manager.Stop()) }()

	bus.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(
			event.EpochTransitionEventType,
			event.EpochTransitionEvent{NewEpoch: 1, EpochNonce: []byte{1}},
		),
	)
	require.Eventually(t, func() bool { return stat1() == 1 },
		30*time.Second, 20*time.Millisecond,
		"sqlite_stat1 must exist after the first rollover")

	result, err := manager.RunStartup(context.Background())
	require.NoError(t, err)
	require.True(t, result.Supported)
	require.False(t, result.Changed)
}
