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

package dingo

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// bootSetNode runs the startup sequence of a node over dir with the embedded
// configuration of network: reconcile, the epoch cache, then the ledger.
// Each mutate edits the loaded configuration first, and stop shuts the node
// down.
func bootSetNode(
	t *testing.T,
	dir string,
	network string,
	mutate ...func(*cardano.CardanoNodeConfig),
) (n *Node, stop func()) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath(network),
		network,
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	for _, m := range mutate {
		m(nodeCfg)
	}
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: dir, StorageMode: types.StorageModeCore,
	})
	require.NoError(t, err)
	set, err := utxoset.Open(utxoset.Config{Dir: utxoset.DirFor(dir)})
	require.NoError(t, err)
	bus := event.NewEventBus(nil, logger)
	cm, err := chain.NewManager(t.Context(), db, bus)
	require.NoError(t, err)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		EventBus:          bus,
		Logger:            logger,
		CardanoNodeConfig: nodeCfg,
		Network:           network,
		PromRegistry:      prometheus.NewRegistry(),
		UtxoSet:           set,
	})
	require.NoError(t, err)
	n = &Node{
		db: db, utxoSet: set, ledgerState: ls, chainManager: cm, eventBus: bus,
	}
	n.config.logger = logger
	n.config.dataDir = dir
	n.config.cardanoNodeConfig = nodeCfg

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, n.reconcileUtxoSet(ctx))
	require.NoError(t, n.prepareEpochCache(ctx))
	require.NoError(t, ls.Start(ctx))
	closingLedger, closingSet, closingDB := ls, set, db
	return n, func() {
		cancel()
		require.NoError(t, closingLedger.Close())
		closingLedger = nil
		bus.Stop()
		require.NoError(t, closingSet.Close())
		closingSet = nil
		require.NoError(t, dbtest.CloseDatabase(closingDB))
		closingDB = nil
	}
}

// The ledger writes its epochs to the live UTxO set, and a restarted node
// serves the same epochs from it.
func TestNodeRestartServesEpochsFromTheSet(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	first, stop := bootSetNode(t, dir, "preview")
	require.Nil(t, first.db.Metadata())
	want, err := first.db.GetEpochs(nil)
	require.NoError(t, err)
	require.NotEmpty(t, want, "the ledger writes its first epoch to the set")
	stop()

	second, stop := bootSetNode(t, dir, "preview")
	defer stop()
	got, err := second.db.GetEpochs(nil)
	require.NoError(t, err)
	require.Equal(t, epochIDs(want), epochIDs(got))
	require.Equal(t, want, got)
}

func epochIDs(epochs []models.Epoch) []uint64 {
	ids := make([]uint64, 0, len(epochs))
	for _, e := range epochs {
		ids = append(ids, e.EpochId)
	}
	return ids
}
