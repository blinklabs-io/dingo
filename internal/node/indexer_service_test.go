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

package node

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/indexer"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

type fixedSecurityParam struct{}

func (fixedSecurityParam) SecurityParam() int { return 1_000_000 }

func rowCount(t *testing.T, raw *sql.DB, table string) int {
	t.Helper()
	var n int
	require.NoError(t, raw.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&n))
	return n
}

func openRaw(t *testing.T, path string) *sql.DB {
	t.Helper()
	raw, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(30000)")
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	return raw
}

func TestSplitIndexerFollowsLedgerAndRollsBack(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := t.TempDir()
	selections := internalplugins.StorageSelections{
		Blob: plugin.Selection{
			Provider: "badger", Config: testutil.BadgerBlobConfig(),
		},
		Metadata: plugin.Selection{
			Provider: "sqlite",
			Config:   map[string]any{},
		},
	}
	deps := internalplugins.StorageDependencies{
		DataDir: dataDir, StorageMode: "core", Logger: logger,
	}
	dbConfig := &database.Config{
		DataDir: dataDir, Logger: logger, StorageMode: "core",
		IndexerLayout: "split", Network: "preview",
	}
	runtime, err := internalplugins.OpenDatabase(
		ctx,
		dbConfig,
		selections,
		deps,
	)
	require.NoError(t, err)
	consensus := runtime.Database
	var closeRuntime func()
	closeRuntime = func() {
		require.NoError(t, runtime.Close(context.WithoutCancel(ctx)))
		closeRuntime = func() {}
	}
	t.Cleanup(func() { closeRuntime() })

	// The ledger replays real blocks in core mode: the split layout's
	// consensus store.
	dir := copyImmutableChunkPrefix(t, 30)
	require.NoError(t, LoadWithDB(
		ctx, &config.Config{Network: "preview"}, logger, dir, consensus,
	))
	ledgerTip, err := consensus.GetTip(nil)
	require.NoError(t, err)
	require.NotZero(t, ledgerTip.BlockNumber)

	indexerDB, err := internalplugins.OpenIndexerDatabase(
		ctx, runtime.Host, consensus, dbConfig, selections, deps,
	)
	require.NoError(t, err)
	closeIndexer := func() { require.NoError(t, indexerDB.Close()) }
	indexerClosed := false
	t.Cleanup(func() {
		if !indexerClosed {
			closeIndexer()
		}
	})
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath(
			"preview",
		),
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	factLedger, _ := openFactLedger(t, consensus)
	writer, err := NewIndexerService(dingo.IndexerWriterDeps{
		Consensus: consensus, FactRows: factLedger, Indexer: indexerDB,
		NodeConfig: nodeCfg, Logger: logger, MaxReadyLagBlocks: 10,
	})
	require.NoError(t, err)
	svc := writer.(*IndexerService)
	require.NoError(t, svc.Start(ctx))
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			require.NoError(t, svc.Stop(ctx))
		}
	}
	t.Cleanup(stop)
	svc.Wake()

	waitForTip := func(hash []byte, what string) {
		t.Helper()
		testutil.WaitForCondition(t, func() bool {
			tip, err := svc.Store().Tip(nil)
			return err == nil && string(tip.Hash) == string(hash)
		}, 60*time.Second, what)
	}
	waitForTip(ledgerTip.Point.Hash, "the IndexerDB to reach the ledger tip")

	indexerRaw := openRaw(
		t,
		filepath.Join(
			dataDir,
			internalplugins.IndexerDirName,
			"metadata.sqlite",
		),
	)
	apiTables := []string{"address_transaction", "utxo_address", "key_witness"}
	for _, table := range apiTables {
		require.NotZero(t, rowCount(t, indexerRaw, table),
			"the IndexerDB must hold the API rows (%s)", table)
	}
	txBefore := rowCount(t, indexerRaw, "applied_transaction")

	// A rollback of the canonical chain takes the IndexerDB back with it.
	cm, err := chain.NewManager(ctx, consensus, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(fixedSecurityParam{}))
	primary := cm.PrimaryChain()
	require.Equal(t, ledgerTip.Point.Hash, primary.Tip().Point.Hash)
	var target ocommon.Point
	src, err := consensus.BlockByIndex(2, nil) // an early block, past genesis
	require.NoError(t, err)
	target = ocommon.Point{Slot: src.Slot, Hash: src.Hash}
	require.NoError(t, primary.Rollback(ctx, target))
	svc.Wake()
	waitForTip(target.Hash, "the IndexerDB to roll back to the rollback point")

	require.Less(t, rowCount(t, indexerRaw, "applied_transaction"), txBefore,
		"rows of rolled-back blocks must be removed")
	tip, err := svc.Store().Tip(nil)
	require.NoError(t, err)
	require.Equal(t, target.Slot, tip.Slot)
	var _ indexerdb.Tip = tip

	stop()
	closeIndexer()
	indexerClosed = true
	closeRuntime()
}

// A service built without the ledger's facts exists to roll the IndexerDB
// back. Following the ledger would fail every pass, so it does not start.
func TestIndexerServiceWithoutALedgerDoesNotStart(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	consensus, err := dbtest.NewDatabase(
		t, &database.Config{DataDir: t.TempDir()},
	)
	require.NoError(t, err)
	indexerDB, err := dbtest.NewIndexerDatabase(
		t, consensus, dbtest.IndexerOptions{},
	)
	require.NoError(t, err)

	svc, err := NewIndexerService(dingo.IndexerWriterDeps{
		Consensus: consensus, Indexer: indexerDB, Logger: logger,
	})
	require.NoError(t, err)
	require.ErrorIs(t, svc.Start(t.Context()), indexer.ErrNoLedger)
}
