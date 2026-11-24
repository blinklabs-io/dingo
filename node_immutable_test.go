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
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/blinklabs-io/dingo/internal/test/livesettest"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// fakeImmutablePoint reports a fixed immutable point, as the ledger does k
// blocks behind its applied tip.
type fakeImmutablePoint struct {
	point ocommon.Point
	found bool
	err   error
}

func (f fakeImmutablePoint) ImmutablePoint(
	context.Context,
) (ocommon.Point, bool, error) {
	return f.point, f.found, f.err
}

func TestImmutableCeilingIsTheLedgerImmutablePoint(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	hashes := make([][]byte, 31)
	for id := uint64(1); id <= 30; id++ {
		hashes[id] = bytes.Repeat([]byte{byte(id)}, 32)
		require.NoError(t, db.BlockCreate(models.Block{
			ID: id, Slot: id * 10, Number: id, Type: 1, Hash: hashes[id],
			Cbor: []byte{0x82, 0x41, byte(id), 0x41, 0x02},
		}, nil))
	}
	at := func(id uint64) fakeImmutablePoint {
		return fakeImmutablePoint{
			point: ocommon.NewPoint(id*10, hashes[id]), found: true,
		}
	}
	ceiling, ok, err := immutableCeilingFor(context.Background(), db, at(15))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(15), ceiling)

	// No immutable point yet, an unconfigured ledger, or a point the block
	// index does not hold permits nothing.
	for name, src := range map[string]fakeImmutablePoint{
		"not found": {},
		"unconfigured": {
			err: fmt.Errorf("%w: test", ledger.ErrImmutablePointUnconfigured),
		},
		"unknown point": {
			point: ocommon.NewPoint(999, bytes.Repeat([]byte{0xEE}, 32)),
			found: true,
		},
	} {
		_, ok, err = immutableCeilingFor(context.Background(), db, src)
		require.NoError(t, err, name)
		require.False(t, ok, name)
	}

	boom := errors.New("read chain")
	_, _, err = immutableCeilingFor(
		context.Background(), db, fakeImmutablePoint{err: boom},
	)
	require.ErrorIs(t, err, boom)
}

func TestImmutableDBEnabled(t *testing.T) {
	t.Parallel()
	blob := func(provider string) map[hostplugin.Capability]hostplugin.Selection {
		return map[hostplugin.Capability]hostplugin.Selection{
			hostplugin.CapabilityStorageBlob: {Provider: provider},
		}
	}
	tests := []struct {
		name string
		cfg  Config
		want bool
	}{
		{"in memory", Config{}, true},
		{"on disk", Config{dataDir: "/data"}, true},
		{
			"badger",
			Config{dataDir: "/data", pluginSelections: blob("badger")},
			true,
		},
		{
			"remote blob store",
			Config{dataDir: "/data", pluginSelections: blob("s3")},
			true,
		},
		{"history expiry", Config{
			dataDir:       "/data",
			historyExpiry: HistoryExpiryConfig{Enabled: true},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.cfg.cardanoNodeConfig = newNodeTestCardanoNodeCfg(t)
			n := &Node{config: tt.cfg}
			require.Equal(t, tt.want, n.immutableConfig().Enabled)
		})
	}
}

func TestHistoryExpiryRefusedOverRecordedImmutableState(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
		Immutable: database.ImmutableConfig{
			Enabled: true, ChunkSlots: 4320, CanContainEBB: true,
		},
	})
	require.NoError(t, err)
	expiry := Config{historyExpiry: HistoryExpiryConfig{Enabled: true}}
	err = refuseHistoryExpiryOverImmutable(expiry, db)
	require.ErrorContains(t, err, "ImmutableDB")
	require.ErrorContains(t, err, "turn historyExpiry off")
	require.NoError(t, refuseHistoryExpiryOverImmutable(Config{}, db))

	// A database that never had an ImmutableDB, such as one a v0.94.2 node
	// expired history on, keeps expiring in place.
	plain, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	require.NoError(t, refuseHistoryExpiryOverImmutable(expiry, plain))
}

// A node without a data directory opens its database through the same
// storage composition as an on-disk one, with the ImmutableDB in a temporary
// directory, and serves promoted blocks from it.
func TestInMemoryNodeDatabaseServesPromotedBlocks(t *testing.T) {
	t.Parallel()
	cardanoCfg := newNodeTestCardanoNodeCfg(t)
	n, err := New(NewConfig(
		WithCardanoNodeConfig(cardanoCfg),
		WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
	))
	require.NoError(t, err)
	require.Empty(t, n.config.dataDir)
	t.Cleanup(func() { _ = n.pluginHost.Stop(context.Background()) })
	deps, err := n.storageDependencies(n.config.dataDir)
	require.NoError(t, err)
	stores, err := internalplugins.ResolveStorage(
		t.Context(), n.pluginHost, n.storageSelections(), deps,
	)
	require.NoError(t, err)
	db, err := database.New(t.Context(), n.databaseConfig(), stores)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, ok := db.ImmutableState()
	require.True(t, ok, "the in-memory node runs the ImmutableDB")

	block := models.Block{
		ID: 1, Slot: 100, Number: 1, Type: 1,
		Hash: bytes.Repeat([]byte{0xC1}, 32),
		Cbor: []byte{0x82, 0x41, 0x01, 0x41, 0x02},
	}
	require.NoError(t, db.BlockCreate(block, nil))
	result, err := db.PromoteThrough(t.Context(), block.ID)
	require.NoError(t, err)
	require.Equal(t, 1, result.Promoted)
	got, err := database.BlockByPoint(
		t.Context(), db, ocommon.NewPoint(block.Slot, block.Hash),
	)
	require.NoError(t, err)
	require.Equal(t, block.Cbor, got.Cbor)
}

// An in-memory node gets past storage, ledger and promoter startup.
func TestInMemoryNodeRunsPastStorageStartup(t *testing.T) {
	t.Parallel()
	n := newExpiryRunNode(t, "", StorageModeCore, false)
	require.ErrorIs(t, n.Run(context.Background()), fs.ErrNotExist)
}

// newExpiryRunNode builds a node whose Run gets past storage, ledger and
// promoter startup and then fails block-producer validation on its missing
// key files, so Run returns without an external shutdown.
func newExpiryRunNode(
	t *testing.T,
	dataDir string,
	mode StorageMode,
	expiry bool,
) *Node {
	t.Helper()
	cardanoCfg := newNodeTestCardanoNodeCfg(t)
	opts := []ConfigOptionFunc{
		WithCardanoNodeConfig(cardanoCfg),
		WithNetwork("preview"),
		WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithStorageMode(mode),
		WithHistoryExpiry(HistoryExpiryConfig{
			Enabled: expiry, Frequency: time.Hour,
		}),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		WithUtxorpcPort(0),
		WithBlockfrostPort(0),
		WithMidnightConfig(MidnightConfig{Port: 0}),
		WithShutdownTimeout(5 * time.Second),
	}
	if dataDir != "" {
		opts = append(opts, WithDatabasePath(dataDir))
	}
	if mode == StorageModeAPI {
		opts = append(opts, WithIndexerWriterFactory(
			func(IndexerWriterDeps) (IndexerWriter, error) {
				return &fakeIndexerWriter{wakes: make(chan struct{}, 8)}, nil
			},
		))
	}
	n, err := New(NewConfig(opts...))
	require.NoError(t, err)
	n.config.listeners = nil
	n.config.blockProducer = true
	return n
}

// A node that expires history, in either storage mode, runs no ImmutableDB:
// no promoter starts and no ImmutableDB state is created.
func TestHistoryExpiryNodeRunsWithoutImmutableDB(t *testing.T) {
	t.Parallel()
	for _, mode := range []StorageMode{StorageModeCore, StorageModeAPI} {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			n := newExpiryRunNode(t, t.TempDir(), mode, true)
			require.ErrorIs(t, n.Run(context.Background()), fs.ErrNotExist)
			require.Nil(t, n.blockPromoter)
			_, ok := n.db.ImmutableState()
			require.False(t, ok, "history expiry creates no ImmutableDB state")
		})
	}
}

// A database whose bodies moved into the ImmutableDB refuses history expiry,
// and the refusal leaves it startable with expiry off, as its message says.
func TestHistoryExpiryRefusalLeavesTheDatabaseStartable(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	first := newExpiryRunNode(t, dataDir, StorageModeCore, false)
	require.ErrorIs(t, first.Run(context.Background()), fs.ErrNotExist)
	_, ok := first.db.ImmutableState()
	require.True(t, ok)

	refused := newExpiryRunNode(t, dataDir, StorageModeCore, true)
	err := refused.Run(context.Background())
	require.ErrorContains(t, err, "turn historyExpiry off")

	again := newExpiryRunNode(t, dataDir, StorageModeCore, false)
	require.ErrorIs(t, again.Run(context.Background()), fs.ErrNotExist)
}

// The promotion ceiling is read from the ledger's applied tip at the call
// site: a chain that is ahead of the ledger, as during catch-up, must not let
// blocks the ledger has not applied leave the VolatileDB.
func TestNodeImmutableCeilingUsesLedgerAppliedTip(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	// The chain holds 600 blocks and the ledger has applied the first 500.
	// The manager reads its tip at construction, so seed the blocks first.
	const total, appliedID = 600, 500
	hashes := make([][]byte, total+1)
	for id := uint64(1); id <= total; id++ {
		hashes[id] = bytes.Repeat([]byte{byte(id), byte(id >> 8)}, 16)
		require.NoError(t, db.BlockCreate(models.Block{
			ID: id, Slot: id * 10, Number: id, Type: 1, Hash: hashes[id],
			Cbor: []byte{0x82, 0x41, byte(id), 0x41, 0x02},
		}, nil))
	}
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(total*10), cm.PrimaryChain().Tip().Point.Slot)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:          db,
		UtxoSet:           livesettest.OpenLedgerDB(t, db),
		ChainManager:      cm,
		Logger:            logger,
		CardanoNodeConfig: nodeCfg,
		Network:           "preview",
		PromRegistry:      prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	k := uint64(ls.SecurityParam())
	require.Positive(t, k)
	require.Less(t, k, uint64(appliedID), "the applied tip must be past k")
	ls.SetTipForTesting(ochainsync.Tip{
		Point:       ocommon.NewPoint(appliedID*10, hashes[appliedID]),
		BlockNumber: appliedID,
	})

	n := &Node{db: db, ledgerState: ls, chainManager: cm}
	ceiling, ok, err := n.immutableCeiling(t.Context())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, appliedID-k, ceiling,
		"k behind the ledger's applied tip, not behind the chain's")
}

func newSpaceCheckNode(t *testing.T, blobBytes int, free uint64) *Node {
	t.Helper()
	dataDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "blob"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dataDir, "blob", "000001.vlog"),
		make([]byte, blobBytes), 0o600,
	))
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	n := &Node{
		db:                 db,
		availableDiskBytes: func(string) (uint64, error) { return free, nil },
	}
	n.config.dataDir = dataDir
	n.config.immutableDB.PromoteBatch = 10
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return n
}

// The first promotion of a database copies every block body into chunk files
// before the blob store reclaims them, so it needs about the blob store's size
// free and is refused, with the figure, when the volume holds less.
func TestImmutableSpaceCheckRefusesABulkPromotionWithoutSpace(t *testing.T) {
	t.Parallel()
	n := newSpaceCheckNode(t, 1<<20, immutableSpaceMargin+(1<<20)-1)
	err := n.immutableSpaceCheck(t.Context(), 1000)
	require.ErrorContains(t, err, "not enough free disk space")
	require.ErrorContains(t, err, strconv.Itoa(immutableSpaceMargin+(1<<20)))

	n.availableDiskBytes = func(string) (uint64, error) {
		return immutableSpaceMargin + (1 << 20), nil
	}
	require.NoError(t, n.immutableSpaceCheck(t.Context(), 1000))
}

// Once the ImmutableDB keeps up, a step moves a few blocks and needs only the
// margin, however large the blob store is.
func TestImmutableSpaceCheckNeedsOnlyTheMarginForASmallStep(t *testing.T) {
	t.Parallel()
	n := newSpaceCheckNode(t, 1<<20, immutableSpaceMargin)
	require.NoError(t, n.immutableSpaceCheck(t.Context(), 5))
	n.availableDiskBytes = func(string) (uint64, error) {
		return immutableSpaceMargin - 1, nil
	}
	require.ErrorContains(
		t, n.immutableSpaceCheck(t.Context(), 5), "not enough free disk space",
	)
}

// A Badger value log is preallocated far beyond what it holds, so the estimate
// counts the blocks a file occupies, not its length: a sparse file takes no
// space to copy and must not make a small disk refuse the promotion.
func TestImmutableSpaceCheckCountsAllocatedBytesNotFileLength(t *testing.T) {
	t.Parallel()
	n := newSpaceCheckNode(t, 1<<20, immutableSpaceMargin+(2<<20))
	sparse := filepath.Join(n.config.dataDir, "blob", "000002.vlog")
	f, err := os.Create(sparse) // #nosec G304 -- inside the test's directory
	require.NoError(t, err)
	const apparent = 1 << 30
	require.NoError(t, f.Truncate(apparent))
	require.NoError(t, f.Close())
	info, err := os.Stat(sparse)
	require.NoError(t, err)
	if allocatedBytes(info) >= apparent/2 {
		t.Skip("the filesystem does not keep a truncated file sparse")
	}
	require.NoError(t, n.immutableSpaceCheck(t.Context(), 1000),
		"a 1 GiB sparse file beside a 1 MiB one needs about 1 MiB")
}
