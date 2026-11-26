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
	"bytes"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// loadNodeChunks is how many leading preview chunks the load-then-node tests
// replay: enough real blocks for several replay batches.
const loadNodeChunks = 4

// requireLoadedStorage opens the stores of the stopped data directory dir and
// requires the layout a node leaves: the ledger and the live UTxO set at tip,
// and the database marked as holding its UTxO set only in the set.
func requireLoadedStorage(t *testing.T, dir string, tip ocommon.Point) {
	t.Helper()
	cfg := parityConfig(dir)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	genesisStakingEmpty, err := internalplugins.ResolveGenesisStakingEmpty(
		nil, cfg.CardanoConfig, cfg.Network, cfg.NetworkMagic,
	)
	require.NoError(t, err)
	rt, err := internalplugins.OpenDatabase(
		t.Context(),
		&database.Config{
			DataDir: dir, Logger: logger,
			StorageMode:    cfg.ConsensusStorageMode(),
			Network:        cfg.Network,
			BlobPlugin:     cfg.Plugins.Storage.Blob.Provider,
			MetadataPlugin: cfg.Plugins.Storage.Metadata.Provider,
			AlonzoLovelacePerUtxoWord: cardano.AlonzoLovelacePerUtxoWord(
				nil, cfg.CardanoConfig, cfg.Network,
			),
		},
		internalplugins.StorageSelections{
			Blob:     cfg.Plugins.Storage.Blob,
			Metadata: cfg.Plugins.Storage.Metadata,
		},
		internalplugins.StorageDependencies{
			DataDir: dir, RunMode: string(cfg.RunMode),
			StorageMode: cfg.ConsensusStorageMode(),
			Logger:      logger, GenesisStakingEmpty: genesisStakingEmpty,
		},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close(t.Context())) }()
	db := rt.Database

	ledgerTip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, tip.Slot, ledgerTip.Point.Slot)
	require.Equal(t, tip.Hash, ledgerTip.Point.Hash)

	set, err := utxoset.Open(utxoset.Config{
		Dir: utxoset.DirFor(dir), Logger: logger, RetainRewardRecords: cfg.HasIndexerDB(),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, set.Close()) }()
	setTip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok, "the live UTxO set has a tip")
	require.Equal(t, tip.Slot, setTip.Slot)
	require.Equal(t, tip.Hash, setTip.Hash)
	count, err := set.Count()
	require.NoError(t, err)
	require.NotZero(t, count, "the live UTxO set holds the UTxO set")
}

// TestNodeServesTheTipDingoLoadWrote loads real blocks into a new data
// directory as `dingo load` does and starts a node over it. The load must
// leave the storage a node writes -- the live UTxO set at the loaded tip --
// so the node opens it without a migration and serves the loaded tip.
func TestNodeServesTheTipDingoLoadWrote(t *testing.T) {
	t.Parallel()
	immutableDir := copyImmutableChunkPrefix(t, loadNodeChunks)
	imm, err := immutable.New(immutableDir)
	require.NoError(t, err)
	immutableTip, err := imm.GetTip()
	require.NoError(t, err)
	require.NotNil(t, immutableTip)

	dir := loadParityChain(t, immutableDir)
	requireLoadedStorage(t, dir, *immutableTip)

	cardanoCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	node := startParityNode(t, "loaded", dir, cardanoCfg)
	var latest struct {
		Hash string `json:"hash"`
		Slot uint64 `json:"slot"`
	}
	require.Eventually(t, func() bool {
		return node.getJSON(t, "/api/v0/blocks/latest", &latest) &&
			latest.Hash == hex.EncodeToString(immutableTip.Hash)
	}, 2*time.Minute, 100*time.Millisecond,
		"the node serves the tip the load wrote")
	require.Equal(t, immutableTip.Slot, latest.Slot)
	node.stop()
	requireLoadedStorage(t, dir, *immutableTip)
}

// TestLoadExtendsADatabaseItLoaded loads a chain prefix into a new data
// directory and then a longer one into the same directory. The first load
// leaves the ledger state in the live UTxO set, so the second load must find
// the ledger state the first one wrote and extend it.
func TestLoadExtendsADatabaseItLoaded(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tipOf := func(dir string) ocommon.Point {
		imm, err := immutable.New(dir)
		require.NoError(t, err)
		tip, err := imm.GetTip()
		require.NoError(t, err)
		require.NotNil(t, tip)
		return *tip
	}
	shortDir := copyImmutableChunkPrefix(t, slowFixtureChunks)
	longDir := copyImmutableChunkPrefix(t, loadNodeChunks)
	shortTip, longTip := tipOf(shortDir), tipOf(longDir)
	require.Greater(t, longTip.Slot, shortTip.Slot)

	dir := loadParityChain(t, shortDir)
	requireLoadedStorage(t, dir, shortTip)
	require.NoError(t, Load(t.Context(), parityConfig(dir), logger, longDir))
	requireLoadedStorage(t, dir, longTip)
}

// TestLoadRefusesALedgerWithoutItsUtxoSet removes the live UTxO set of a
// loaded data directory. The set is the only copy of the ledger state, so
// nothing can rebuild it, and a load that replayed onto an empty set would
// write state that matches no chain.
func TestLoadRefusesALedgerWithoutItsUtxoSet(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	immutableDir := copyImmutableChunkPrefix(t, slowFixtureChunks)
	dir := loadParityChain(t, immutableDir)
	require.NoError(t, os.RemoveAll(utxoset.DirFor(dir)))

	err := Load(t.Context(), parityConfig(dir), logger, immutableDir)
	require.ErrorContains(t, err, "cannot be caught up to the ledger")
	require.ErrorContains(
		t, err, "no metadata store holds an earlier copy of the ledger state",
	)
}

// TestLoadWithDBRefusesAnUnfinishedImport leaves an import into the LedgerDB
// set still writing, as an interrupted Mithril sync does. Only that import can
// finish it, so load must stop before it replays.
func TestLoadWithDBRefusesAnUnfinishedImport(t *testing.T) {
	t.Parallel()
	db := newFileTestDB(t)
	set, err := utxoset.Open(utxoset.Config{Dir: utxoset.DirFor(db.DataDir())})
	require.NoError(t, err)
	require.NoError(t, set.LoadUtxoSet(
		ocommon.Point{Slot: 10, Hash: bytes.Repeat([]byte{1}, 32)},
		func(func([]byte, uint32, []byte) error) error { return nil },
	))
	require.NoError(t, set.Close())
	require.NoError(t, db.SetChainState("sync_status", "in_progress", nil))

	err = LoadWithDB(
		t.Context(),
		&config.Config{Network: "preview"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"unused",
		db,
	)
	require.ErrorIs(t, err, badgerutxo.ErrImportPending)
}
