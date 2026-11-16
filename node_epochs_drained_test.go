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
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// bootDrainedNode runs the startup sequence of a node with ledgerStateSetOnly
// over dir: reconcile (which drains every graduated family), the epoch cache,
// then the ledger. The statement guard is armed over every graduated table
// once the drain has run, and stop shuts the node down. Each boot installs its
// own guard, as a process does: the schema checks that run while the store
// opens read tables a drained family has already emptied.
func bootDrainedNode(
	t *testing.T,
	dir string,
) (n *Node, guard *statedrain.TripGuard, stop func()) {
	t.Helper()
	guard = statedrain.NewTripGuard()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"),
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{
			DataDir: dir, StorageMode: types.StorageModeCore,
		},
		StatementGuard: guard,
	})
	require.NoError(t, err)
	set, err := utxoset.Open(utxoset.Config{
		Dir: utxoset.DirFor(dir), Only: true, Authoritative: true,
	})
	require.NoError(t, err)
	bus := event.NewEventBus(nil, logger)
	cm, err := chain.NewManager(t.Context(), db, bus)
	require.NoError(t, err)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:           db,
		ChainManager:       cm,
		EventBus:           bus,
		Logger:             logger,
		CardanoNodeConfig:  nodeCfg,
		Network:            "preview",
		PromRegistry:       prometheus.NewRegistry(),
		UtxoSet:            set,
		UtxoSetReads:       ledger.UtxoSetReadsSet,
		UtxoSetOnly:        true,
		LedgerStateSetOnly: true,
	})
	require.NoError(t, err)
	n = &Node{db: db, utxoSet: set, ledgerState: ls, chainManager: cm}
	n.config.logger = logger
	n.config.ledgerUtxoBadger = true
	n.config.ledgerUtxoBadgerOnly = true
	n.config.ledgerUtxoReads = ledger.UtxoSetReadsSet
	n.config.ledgerStateSetOnly = true

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, n.reconcileUtxoSet(ctx))
	for _, e := range statedrain.Graduated(drainNoSource{}) {
		guard.Arm(e.Family, statedrain.TableNames(e.Family))
	}
	require.NoError(t, n.prepareEpochCache(ctx))
	require.NoError(t, ls.Start(ctx))
	return n, guard, func() {
		cancel()
		require.NoError(t, ls.Close())
		bus.Stop()
		require.NoError(t, set.Close())
		require.NoError(t, dbtest.CloseDatabase(db))
	}
}

// drainNoSource names the graduated entries without a set behind them.
type drainNoSource struct{ statedrain.Source }

// A node started with every graduated family drained keeps its epochs and
// epoch summaries only in the live UTxO set: the first start drains the empty
// SQL tables, the ledger's own epoch writes go to the set, and a restart
// serves the same epochs from it without a statement touching either table.
func TestNodeRestartServesEpochsFromTheSetWhenEveryFamilyIsDrained(
	t *testing.T,
) {
	t.Parallel()
	dir := t.TempDir()

	first, firstGuard, stop := bootDrainedNode(t, dir)
	want, err := first.db.GetEpochs(nil)
	require.NoError(t, err)
	require.NotEmpty(t, want, "the ledger writes its first epoch to the set")
	mask, err := first.db.DrainedFamilies(t.Context())
	require.NoError(t, err)
	for _, e := range statedrain.Graduated(drainNoSource{}) {
		require.True(t, mask.Has(e.Family), "latch bit of %s", e.Family)
	}
	raw, err := dbtest.RawSQLiteMetadata(t, first.db)
	require.NoError(t, err)
	for _, table := range []string{"epoch", "epoch_summary"} {
		var rows int
		require.NoError(t, raw.QueryRow(
			"SELECT COUNT(*) FROM `"+table+"`",
		).Scan(&rows))
		require.Zero(t, rows, "drained %s", table)
	}
	require.Empty(t, firstGuard.Trips(), "a drained table was touched")
	stop()

	second, guard, stop := bootDrainedNode(t, dir)
	defer stop()
	got, err := second.db.GetEpochs(nil)
	require.NoError(t, err)
	require.Equal(t, epochIDs(want), epochIDs(got))
	require.Equal(t, want, got)
	require.Empty(t, guard.Trips(), "a drained table was touched")
}

func epochIDs(epochs []models.Epoch) []uint64 {
	ids := make([]uint64, 0, len(epochs))
	for _, e := range epochs {
		ids = append(ids, e.EpochId)
	}
	return ids
}
