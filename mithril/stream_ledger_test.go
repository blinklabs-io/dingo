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

// This test lives in the external package because it drives the stream
// runner through internal/nodemithril, which imports mithril, so package
// mithril's own tests cannot import it.
package mithril_test

import (
	"context"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/nodemithril"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mithril"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// realChunksDir is the preview ImmutableDB the fixture certifies.
var realChunksDir = filepath.Join("..", "database", "immutable", "testdata")

// ledgerApplier presents a LedgerState as a dingo.TrustedBatchApplier, as the
// node's own adapter does, and counts the chunk files on disk at every batch.
type ledgerApplier struct {
	ls          *ledger.LedgerState
	downloadDir string

	mu       sync.Mutex
	batches  int
	maxBatch int
	maxTrios int
	err      error
}

func (a *ledgerApplier) Tip() ocommon.Point {
	return a.ls.Tip().Point
}

func (a *ledgerApplier) ApplyTrustedBatch(
	ctx context.Context,
	expected ocommon.Point,
	blocks []gledger.Block,
) (ocommon.Point, error) {
	trios := 0
	err := filepath.WalkDir(
		a.downloadDir,
		func(_ string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if strings.HasSuffix(d.Name(), ".chunk") {
				trios++
			}
			return nil
		},
	)
	a.mu.Lock()
	a.batches++
	a.maxBatch = max(a.maxBatch, len(blocks))
	a.maxTrios = max(a.maxTrios, trios)
	if err != nil && a.err == nil {
		a.err = err
	}
	a.mu.Unlock()
	return a.ls.ApplyTrustedBatch(ctx, expected, blocks)
}

// TestStreamCatchUpIntoRealLedger streams real certified preview chunks
// through the node's Mithril source and the real runner into a real
// LedgerState at origin: Byron EBBs, chunk splitting across batches, block
// continuity in the primary chain, the stale-batch mapping, the marker and
// the release of every trio all against real storage.
func TestStreamCatchUpIntoRealLedger(t *testing.T) {
	t.Parallel()
	const (
		last   = 2
		window = 2
	)
	agg := mithril.NewRealChunkTestAggregator(t, realChunksDir, last)

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"),
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	nodeCfg.MithrilGenesisVerificationKey = agg.GenesisVerificationKey
	logger := slog.New(slog.DiscardHandler)
	dataDir := t.TempDir()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		StorageMode: types.StorageModeCore,
	})
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
		Network:           "preview",
		PromRegistry:      prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, ls.Start(ctx))
	t.Cleanup(func() {
		cancel()
		require.NoError(t, ls.Close())
		bus.Stop()
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	src, err := nodemithril.New(dingo.MithrilCatchUpParams{
		Mithril: internalconfig.MithrilConfig{
			Enabled:           true,
			AggregatorURL:     agg.URL,
			AllowInsecureHTTP: true,
			WindowChunks:      window,
			BatchChunks:       window,
			DownloadWorkers:   window,
		},
		StorageMode:       dingo.StorageModeCore,
		CardanoNodeConfig: nodeCfg,
		Network:           agg.Network,
		DataDir:           dataDir,
		Logger:            logger,
	})
	require.NoError(t, err)
	require.NotNil(t, src)

	downloadDir := filepath.Join(dataDir, "mithril-stream")
	applier := &ledgerApplier{ls: ls, downloadDir: downloadDir}
	holds := 0
	hold := func() func() {
		holds++
		release := ls.HoldNetworkIngest()
		resume := ls.PauseChainIngest()
		return func() {
			resume()
			release()
		}
	}
	require.NoError(t, src.CatchUp(t.Context(), db, applier, hold))
	assert.Equal(t, 1, holds)

	// The certified tip is the last block of the last file.
	imm, err := immutable.New(realChunksDir)
	require.NoError(t, err)
	iter, err := imm.BlocksInChunk(last)
	require.NoError(t, err)
	var want ocommon.Point
	for {
		b, err := iter.Next()
		require.NoError(t, err)
		if b == nil {
			break
		}
		want = ocommon.NewPoint(b.Slot, b.Hash)
	}
	require.NoError(t, iter.Close())
	got := ls.Tip().Point
	assert.Equal(t, want.Slot, got.Slot, "ledger tip slot")
	assert.Equal(t, want.Hash, got.Hash, "ledger tip hash")
	assert.Equal(t, want.Hash, cm.PrimaryChain().Tip().Point.Hash,
		"primary chain tip")

	marker, marked, err := mithril.ImmutableImportMarkerForTest(db)
	require.NoError(t, err)
	assert.True(t, marked)
	assert.Equal(t, uint64(last), marker)

	applier.mu.Lock()
	defer applier.mu.Unlock()
	require.NoError(t, applier.err)
	assert.LessOrEqual(t, applier.maxTrios, window,
		"more trios on disk than the window")
	// Each file holds 216 blocks, so two files per batch exceed the cap and
	// the second file is split across batches.
	assert.Equal(t, ledger.MaxTrustedBatchBlocks, applier.maxBatch,
		"no batch reached the cap, so no file was split")
	assert.Equal(t, 2, applier.batches)
	trios := 0
	require.NoError(t, filepath.WalkDir(
		downloadDir,
		func(_ string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if strings.HasSuffix(d.Name(), ".chunk") {
				trios++
			}
			return nil
		},
	))
	assert.Zero(t, trios, "trios left after every file was released")
}
