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

package integration

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/internal/blockverify"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/dingo/mithril"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// loadImmutableBlocks loads numBlocks real blocks -- hash, type, slot, and
// CBOR together, so a caller that needs a block whose (hash, type, slot)
// genuinely match its own CBOR (e.g. for a cloud backend's content
// verification, which independently re-derives all three from the bytes)
// can use them as a set instead of only the raw CBOR loadBlockData exposes.
func loadImmutableBlocks(numBlocks int) ([]immutable.Block, error) {
	var blocks []immutable.Block
	// Use absolute path to testdata directory by going up from the current package
	// internal/integration -> internal -> root -> database/immutable/testdata
	testdataDir := filepath.Join(
		"..",
		"..",
		"database",
		"immutable",
		"testdata",
	)

	// Open immutable database to parse chunks
	imm, err := immutable.New(testdataDir)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to open immutable DB at %s: %v",
			testdataDir,
			err,
		)
	}

	// Create iterator from origin (slot 0) to get all blocks
	origin := ocommon.NewPoint(0, make([]byte, 32))
	iter, err := imm.BlocksFromPoint(origin)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create block iterator from %s: %v",
			testdataDir,
			err,
		)
	}
	defer iter.Close()

	// Extract blocks
	for len(blocks) < numBlocks {
		block, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to read block: %v", err)
		}
		if block == nil {
			break
		}

		blocks = append(blocks, *block)
	}

	if len(blocks) == 0 {
		return nil, fmt.Errorf("no blocks found in testdata")
	}

	if len(blocks) < numBlocks {
		// If we don't have enough blocks, duplicate the ones we have
		for len(blocks) < numBlocks {
			for _, block := range blocks {
				if len(blocks) >= numBlocks {
					break
				}
				blocks = append(blocks, block)
			}
		}
	}

	return blocks[:numBlocks], nil
}

// loadBlockData loads real block CBOR from testdata chunks for benchmarking.
func loadBlockData(numBlocks int) ([][]byte, error) {
	blocks, err := loadImmutableBlocks(numBlocks)
	if err != nil {
		return nil, err
	}
	cbors := make([][]byte, len(blocks))
	for i, block := range blocks {
		cbors[i] = block.Cbor
	}
	return cbors, nil
}

// verifyBlockSelfConsistent proves block's (Hash, Type, Slot) are what
// blockverify.Hash -- the same production check S3/GCS's own GetBlock
// runs -- would accept, by calling it directly rather than maintaining a
// second copy of its logic. Checking only that block.Cbor decodes under
// block.Type would not be enough on its own: NewBlockFromCbor treats Type
// as a decode hint, and blockverify.Hash independently re-derives the hash
// and slot rather than trusting the caller's claim.
func verifyBlockSelfConsistent(block immutable.Block) error {
	_, err := blockverify.Hash(block.Type, block.Slot, block.Cbor, block.Hash)
	return err
}

// TestLoadImmutableBlocksAreSelfConsistent proves every block this testdata
// set's loadImmutableBlocks loads has a (hash, type, slot) that
// blockverify.Hash accepts -- see verifyBlockSelfConsistent's own doc
// comment for what that means. blockverify.Hash used to also independently
// re-derive era from the header (gledger.DetermineBlockType) and reject a
// disagreement with the recorded Type; some of this testdata set's earlier
// blocks failed that check ("unknown proto major 7 for Shelley-like") even
// though they are genuine, correctly-encoded blocks -- DetermineBlockType
// classifies era from the header's announced protocol-major version, which
// a block producer bumps ahead of an upcoming hard fork, before that
// fork's own era actually begins. blockverify.Hash's own doc comment has
// the full account of why that check was dropped; this test now covers
// every loaded block rather than scanning for the first one that happens
// to pass, since there is no longer a known-bad subset to scan past.
func TestLoadImmutableBlocksAreSelfConsistent(t *testing.T) {
	t.Parallel()

	const numBlocks = 50
	blocks, err := loadImmutableBlocks(numBlocks)
	require.NoError(t, err)
	for _, block := range blocks {
		require.NoErrorf(t, verifyBlockSelfConsistent(block),
			"block at slot %d, type %d", block.Slot, block.Type)
	}
}

// storageBenchBackend is one storage backend under benchmark: a display name,
// the dbtest options that compose its database, and whether it is a local
// on-disk backend that should receive a fresh directory for each run.
type storageBenchBackend struct {
	name      string
	opts      dbtest.Options
	localDisk bool
}

// getTestBackends returns the storage backends to benchmark. Memory and disk
// (Badger) are always present; cloud backends are appended only in
// dingo_extra_plugins builds when the matching credentials are configured.
func getTestBackends(diskDataDir, benchName string) []storageBenchBackend {
	backends := []storageBenchBackend{
		{
			name: "memory",
			opts: dbtest.Options{Config: &database.Config{DataDir: ""}},
		},
		{
			name: "disk",
			opts: dbtest.Options{
				Config: &database.Config{DataDir: diskDataDir},
			},
			localDisk: true,
		},
	}
	return append(
		backends,
		cloudStorageBenchmarkBackends(diskDataDir, benchName)...,
	)
}

// BenchmarkStorageBackends benchmarks different storage backends
func BenchmarkStorageBackends(b *testing.B) {
	for _, backend := range getTestBackends(b.TempDir(), b.Name()) {
		b.Run(backend.name, func(b *testing.B) {
			benchmarkStorageBackend(b, backend)
		})
	}
}

// BenchmarkTestLoad benchmarks the equivalent of loading the first 200 blocks
func BenchmarkTestLoad(b *testing.B) {
	for _, backend := range getTestBackends(b.TempDir(), b.Name()) {
		b.Run(backend.name, func(b *testing.B) {
			benchmarkTestLoad(b, backend)
		})
	}
}

func benchmarkStorageBackend(
	b *testing.B,
	backend storageBenchBackend,
) {
	opts := backend.opts
	// Give a local on-disk backend a fresh directory for this run.
	if backend.localDisk && opts.Config.DataDir != "" {
		tempDir, err := os.MkdirTemp(
			"",
			fmt.Sprintf("dingo-bench-%s-", backend.name),
		)
		if err != nil {
			b.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tempDir)
		cfg := *opts.Config
		cfg.DataDir = filepath.Join(tempDir, "data")
		opts.Config = &cfg
	}

	// Create database with the specified backend
	db, err := dbtest.NewDatabaseWithOptions(b, opts)
	if err != nil {
		b.Fatalf(
			"failed to create database with %s backend: %v",
			backend.name,
			err,
		)
	}
	defer dbtest.CloseDatabase(db)

	// Pre-populate with 10 real blocks
	blocks, err := loadBlockData(10)
	if err != nil {
		b.Fatalf("failed to load block data: %v", err)
	}

	for i := range 10 {
		txn := db.Transaction(true)
		key := fmt.Appendf(nil, "block-%d", i)
		blob := txn.DB().Blob()
		if blob == nil || txn.Blob() == nil {
			txn.Rollback()
			b.Fatalf("blob store/txn not available")
		}
		if err := blob.Set(txn.Blob(), key, blocks[i]); err != nil {
			txn.Rollback()
			b.Fatalf("failed to set block %d: %v", i, err)
		}
		if err := txn.Commit(); err != nil {
			b.Fatalf("failed to commit block %d: %v", i, err)
		}
	}

	b.ReportAllocs()

	for b.Loop() {
		// Process 10 blocks of data
		txn := db.Transaction(false)
		blob := txn.DB().Blob()
		if blob == nil || txn.Blob() == nil {
			txn.Rollback()
			b.Fatalf("blob store/txn not available")
		}
		for blockNum := range 10 {
			key := fmt.Appendf(nil, "block-%d", blockNum)
			_, err := blob.Get(txn.Blob(), key)
			if err != nil {
				txn.Rollback()
				b.Fatalf("failed to get block %d: %v", blockNum, err)
			}
		}
		txn.Rollback()
	}
}

func benchmarkTestLoad(
	b *testing.B,
	backend storageBenchBackend,
) {
	opts := backend.opts
	// Give a local on-disk backend a fresh directory for this run.
	if backend.localDisk && opts.Config.DataDir != "" {
		tempDir, err := os.MkdirTemp(
			"",
			fmt.Sprintf("dingo-testload-%s-", backend.name),
		)
		if err != nil {
			b.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tempDir)
		cfg := *opts.Config
		cfg.DataDir = filepath.Join(tempDir, "data")
		opts.Config = &cfg
	}

	// Create database with the specified backend
	db, err := dbtest.NewDatabaseWithOptions(b, opts)
	if err != nil {
		b.Fatalf(
			"failed to create database with %s backend: %v",
			backend.name,
			err,
		)
	}
	defer dbtest.CloseDatabase(db)

	// Pre-populate with 200 real blocks
	blocks, err := loadBlockData(200)
	if err != nil {
		b.Fatalf("failed to load block data: %v", err)
	}

	for i := range 200 {
		txn := db.Transaction(true)
		key := fmt.Appendf(nil, "block-%d", i)
		blob := txn.DB().Blob()
		if blob == nil || txn.Blob() == nil {
			txn.Rollback()
			b.Fatalf("blob store/txn not available")
		}
		if err := blob.Set(txn.Blob(), key, blocks[i]); err != nil {
			txn.Rollback()
			b.Fatalf("failed to set block %d: %v", i, err)
		}
		if err := txn.Commit(); err != nil {
			b.Fatalf("failed to commit block %d: %v", i, err)
		}
	}

	b.ReportAllocs()

	for b.Loop() {
		// Load first 200 blocks
		txn := db.Transaction(false)
		blob := txn.DB().Blob()
		if blob == nil || txn.Blob() == nil {
			txn.Rollback()
			b.Fatalf("blob store/txn not available")
		}
		for blockNum := range 200 {
			key := fmt.Appendf(nil, "block-%d", blockNum)
			_, err := blob.Get(txn.Blob(), key)
			if err != nil {
				txn.Rollback()
				b.Fatalf("failed to get block %d: %v", blockNum, err)
			}
		}
		txn.Rollback()
	}
}

// newTestStorageHost builds a plugin host with just the badger/sqlite
// providers registered -- lifecycle.Restore takes a host as an explicit
// parameter rather than building one itself (composition code's job in
// production; a test's job here).
func newTestStorageHost(t *testing.T) *plugin.Host {
	t.Helper()
	host := plugin.NewHost()
	require.NoError(t, badger.RegisterProvider(host))
	require.NoError(t, sqlite.RegisterProvider(host))
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	return host
}

// setupLifecycleTestChain opens a fresh database in tmpDir, loads
// numBlocks real blocks from the immutable testdata into it via a chain
// manager (as a running node's chainsync would), and persists the tip —
// Chain.AddBlock only updates in-memory chain state, so the tip must be
// set explicitly to match what full ledger processing would do, since
// database/lifecycle reads the persisted tip, not the in-memory chain.
func setupLifecycleTestChain(
	t *testing.T,
	tmpDir string,
	numBlocks int,
) (db *database.Database, points []ocommon.Point) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: tmpDir})
	require.NoError(t, err)

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(&mockLedgerState{securityParam: 50}))
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	blocks, pts := loadBlocksFromImmutable(t, c, numBlocks)
	if len(blocks) < numBlocks || len(pts) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(blocks),
			numBlocks,
		)
	}

	// These blocks are added directly to the chain index (c.AddBlock, inside
	// loadBlocksFromImmutable) rather than run through LedgerState's normal
	// block-application path, so no block_nonce row exists for any of them --
	// unlike a really-synced chain, which writes one for every applied block
	// including a per-epoch checkpoint. Without at least one checkpoint here,
	// TestDatabaseLifecycleTruncateRealChain's truncate hits
	// database.TruncateAfterSlot's checkpoint check with nothing to satisfy
	// it -- correctly refused, but for this harness gap rather than a genuine
	// unreconstructable truncate. The nonce value is a fixed placeholder, not
	// folded from real VRF output: these tests assert lifecycle/truncate
	// mechanics, not nonce correctness, and nothing here runs a LedgerState
	// to fold or verify it.
	require.NoError(t, db.SetBlockNonce(
		blocks[0].Hash().Bytes(),
		blocks[0].SlotNumber(),
		bytes.Repeat([]byte{0x5c}, 32),
		true, // isCheckpoint
		nil,
	))

	last := blocks[len(blocks)-1]
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       pts[len(pts)-1],
		BlockNumber: last.BlockNumber(),
	}, nil))

	return db, pts
}

// TestDatabaseLifecycleSnapshotRestoreRoundTrip verifies that a snapshot
// of a real multi-block chain restores byte-for-byte, tip included.
func TestDatabaseLifecycleSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	const numBlocks = 60
	db, points := setupLifecycleTestChain(t, t.TempDir(), numBlocks)
	defer db.Close()

	snapDir := filepath.Join(t.TempDir(), "snap")
	manifest, err := lifecycle.Snapshot(
		context.Background(),
		db,
		snapDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)
	require.Equal(t, points[len(points)-1].Slot, manifest.TipSlot)

	restoredDir := filepath.Join(t.TempDir(), "restored")
	restoredManifest, err := lifecycle.Restore(
		context.Background(), newTestStorageHost(t), nil, snapDir, restoredDir,
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
	)
	require.NoError(t, err)
	require.Equal(t, manifest.CommitTimestamp, restoredManifest.CommitTimestamp)

	restoredDB, err := dbtest.NewDatabase(
		t,
		&database.Config{DataDir: restoredDir},
	)
	require.NoError(t, err)
	defer restoredDB.Close()

	// Every real block that was in the source database must round-trip
	// byte-identically-addressable (same hash resolves) in the restored one.
	for _, p := range points {
		_, err := database.BlockByHash(restoredDB, p.Hash)
		require.NoErrorf(
			t,
			err,
			"block at slot %d missing after restore",
			p.Slot,
		)
	}
	restoredTip, err := restoredDB.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, points[len(points)-1].Slot, restoredTip.Point.Slot)
}

// TestDatabaseLifecycleTruncateRealChain is the direct CIP-0135
// conformance check: truncating a real, multi-block chain to a target
// point removes everything after it and leaves the tip consistent, so
// the database is immediately resync-ready from that point (verified
// separately end-to-end via `dingo load` -> `dingo database truncate` ->
// `dingo load` during manual testing, since re-running full ledger sync
// from this package's lighter chain-manager-only harness is out of scope
// here).
func TestDatabaseLifecycleTruncateRealChain(t *testing.T) {
	t.Parallel()

	const numBlocks = 60
	db, points := setupLifecycleTestChain(t, t.TempDir(), numBlocks)
	defer db.Close()

	targetIndex := numBlocks / 2
	target, err := lifecycle.ResolveTargetBySlot(db, points[targetIndex].Slot)
	require.NoError(t, err)
	require.Equal(t, points[targetIndex].Slot, target.Slot)

	blocksRemoved, err := lifecycle.Truncate(
		context.Background(),
		db,
		target,
		0,
		false,
		0,
	)
	require.NoError(t, err)
	// Blocks strictly after targetIndex, up to and including the last one
	// (numBlocks-1): a difference between contiguous IDs, so it's exact
	// regardless of the ID space's starting offset.
	require.Equal(t, uint64(numBlocks-1-targetIndex), blocksRemoved)

	for i, p := range points {
		_, err := database.BlockByHash(db, p.Hash)
		if i <= targetIndex {
			require.NoErrorf(
				t,
				err,
				"block at index %d (slot %d) should survive truncation",
				i,
				p.Slot,
			)
		} else {
			require.Errorf(t, err, "block at index %d (slot %d) should have been truncated away", i, p.Slot)
			require.ErrorIs(t, err, models.ErrBlockNotFound)
		}
	}

	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, target.Slot, tip.Point.Slot)
	require.Equal(t, target.Hash, tip.Point.Hash)
}

// TestDatabaseLifecycleTruncateRejectsBeyondMithrilBoundary verifies that
// a truncate target before the recorded Mithril trust boundary is rejected.
func TestDatabaseLifecycleTruncateRejectsBeyondMithrilBoundary(t *testing.T) {
	t.Parallel()

	const numBlocks = 60
	db, points := setupLifecycleTestChain(t, t.TempDir(), numBlocks)
	defer db.Close()

	boundaryIndex := numBlocks / 2
	require.NoError(t, db.SetSyncState(
		"mithril_ledger_slot",
		strconv.FormatUint(points[boundaryIndex].Slot, 10),
		nil,
	))

	beforeBoundary, err := lifecycle.ResolveTargetBySlot(
		db, points[boundaryIndex/2].Slot,
	)
	require.NoError(t, err)

	_, err = lifecycle.Truncate(
		context.Background(),
		db,
		beforeBoundary,
		0,
		false,
		0,
	)
	require.Error(t, err)
	// Not just any error: specifically the Mithril-boundary rejection,
	// wrapped in ErrTruncateNotStarted (nothing on disk was touched) --
	// asserting only require.Error above would also pass if Truncate
	// failed for a completely unrelated reason (e.g. a bug elsewhere that
	// broke target resolution or the delete path), silently defeating the
	// point of this test.
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
	require.ErrorContains(t, err, "before the Mithril trust boundary")
}

func TestPluginSystemIntegration(t *testing.T) {
	t.Parallel()

	host, err := internalplugins.NewHost()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, host.Stop(context.Background())) })
	providers := host.Providers()
	require.NotEmpty(t, providers)
	require.Contains(t, providers, plugin.Descriptor{
		Capability: plugin.CapabilityStorageBlob,
		Name:       "badger", Description: "BadgerDB local key-value store",
	})
	require.Contains(t, providers, plugin.Descriptor{
		Capability: plugin.CapabilityStorageMetadata,
		Name:       "sqlite", Description: "SQLite relational database",
	})

	runtime, err := internalplugins.OpenDatabase(
		context.Background(),
		&database.Config{DataDir: t.TempDir()},
		internalplugins.StorageSelections{
			Blob: plugin.Selection{
				Provider: "badger",
				Config:   map[string]any{},
			},
			Metadata: plugin.Selection{
				Provider: "sqlite",
				Config:   map[string]any{},
			},
		},
		internalplugins.StorageDependencies{DataDir: t.TempDir()},
	)
	require.NoError(t, err)
	require.NotNil(t, runtime.Database.Blob())
	require.NotNil(t, runtime.Database.Metadata())
	require.NoError(t, runtime.Close(context.Background()))
	require.NoError(t, runtime.Close(context.Background()))
}

func TestNodeShutdownIntegration(t *testing.T) {
	t.Parallel()

	cfg := dingo.NewConfig(
		dingo.WithDatabasePath(t.TempDir()),
		dingo.WithLogger(slog.New(slog.NewJSONHandler(io.Discard, nil))),
		dingo.WithPrometheusRegistry(prometheus.NewRegistry()),
		dingo.WithNetworkMagic(764824073),
		dingo.WithShutdownTimeout(5*time.Second),
		dingo.WithListeners(dingo.ListenerConfig{
			ListenNetwork: "tcp", ListenAddress: "127.0.0.1:0",
		}),
	)
	node, err := dingo.New(cfg)
	require.NoError(t, err)
	require.NoError(t, node.Stop())
	require.NoError(t, node.Stop())
}

func TestStorageBackends(t *testing.T) {
	t.Parallel()

	for _, dataDir := range []string{"", t.TempDir()} {
		t.Run(fmt.Sprintf("dir-%t", dataDir != ""), func(t *testing.T) {
			runtime, err := internalplugins.OpenDatabase(
				context.Background(),
				&database.Config{DataDir: dataDir},
				internalplugins.StorageSelections{
					Blob: plugin.Selection{
						Provider: "badger",
						Config:   map[string]any{},
					},
					Metadata: plugin.Selection{
						Provider: "sqlite",
						Config:   map[string]any{},
					},
				},
				internalplugins.StorageDependencies{DataDir: dataDir},
			)
			require.NoError(t, err)
			t.Cleanup(
				func() { require.NoError(t, runtime.Close(context.Background())) },
			)

			blocks, err := loadBlockData(10)
			require.NoError(t, err)
			for i := range 10 {
				txn := runtime.Database.Transaction(true)
				key := fmt.Appendf(nil, "block-%d", i)
				require.NoError(
					t,
					txn.DB().Blob().Set(txn.Blob(), key, blocks[i]),
				)
				require.NoError(t, txn.Commit())
			}
		})
	}
}

// TestImportLedgerStateFromMithril downloads a preview network
// Mithril snapshot, extracts the ledger state, and imports it
// into a temporary SQLite database. This is an integration test
// that requires network access and takes several minutes.
func TestImportLedgerStateFromMithril(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if os.Getenv("DINGO_INTEGRATION_TEST") == "" {
		t.Skip(
			"set DINGO_INTEGRATION_TEST=1 to run " +
				"integration tests",
		)
	}

	ctx := context.Background()
	logger := slog.New(
		slog.NewTextHandler(
			os.Stderr,
			&slog.HandlerOptions{Level: slog.LevelInfo},
		),
	)

	// Download and extract a preview snapshot
	aggregatorURL, err := mithril.AggregatorURLForNetwork(
		"preview",
	)
	require.NoError(t, err, "getting aggregator URL")

	downloadDir := t.TempDir()

	result, err := mithril.Bootstrap(
		ctx,
		mithril.BootstrapConfig{
			Network:       "preview",
			AggregatorURL: aggregatorURL,
			DownloadDir:   downloadDir,
			Logger:        logger,
			OnProgress: func(p mithril.DownloadProgress) {
				if p.TotalBytes > 0 &&
					int(p.Percent)%10 == 0 {
					t.Logf(
						"download: %.1f%% (%d/%d)",
						p.Percent,
						p.BytesDownloaded,
						p.TotalBytes,
					)
				}
			},
		},
	)
	if errors.Is(err, mithril.ErrNoSnapshotsAvailable) {
		t.Skipf("Mithril aggregator has no snapshots: %v", err)
	}
	require.NoError(t, err, "bootstrapping from Mithril")
	t.Logf(
		"snapshot extracted: epoch=%d, immutable=%s",
		result.Snapshot.Beacon.Epoch,
		result.ImmutableDir,
	)

	// Search for ledger state in ancillary dir first, then extract dir,
	// through the directory handles the bootstrap vetted — the same discovery
	// the import performs. Searching by pathname here would leave the
	// integration coverage on a code path production no longer takes.
	var snapshot *ledgerstate.SnapshotFiles
	for _, root := range []*os.Root{
		result.AncillaryRoot, result.ExtractRoot,
	} {
		if root == nil {
			continue
		}
		files, findErr := ledgerstate.OpenSnapshotAtOrBefore(
			root, ^uint64(0),
		)
		if findErr == nil {
			snapshot = files
			break
		}
		t.Logf("no ledger state in %s: %v", root.Name(), findErr)
	}
	require.NotNil(t, snapshot, "should find ledger state file")
	defer snapshot.Close()
	t.Logf("ledger state file: %s", snapshot.StatePath)

	// Parse the snapshot
	state, err := ledgerstate.ParseSnapshotFile(snapshot.State)
	require.NoError(t, err, "parsing snapshot")

	// Check for UTxO-HD tvar file
	if snapshot.Table != nil {
		state.UTxOTablePath = snapshot.TablePath
		state.UTxOTableFile = snapshot.Table
		t.Logf("UTxO table file (UTxO-HD): %s", snapshot.TablePath)
	}

	require.NotNil(t, state.Tip, "tip should not be nil")
	t.Logf(
		"parsed: era=%s epoch=%d slot=%d",
		ledgerstate.EraName(state.EraIndex),
		state.Epoch,
		state.Tip.Slot,
	)
	require.Greater(
		t, state.Epoch, uint64(0),
		"epoch should be > 0",
	)
	if state.UTxOTablePath == "" {
		require.NotNil(
			t, state.UTxOData,
			"UTxO data should not be nil (legacy format)",
		)
	}
	require.NotNil(
		t, state.CertStateData,
		"cert state data should not be nil",
	)

	// Open a temporary database
	dbDir := t.TempDir()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: dbDir,
		Logger:  logger,
	})
	require.NoError(t, err, "creating database")
	defer dbtest.CloseDatabase(db)

	// Import the ledger state
	var lastProgress ledgerstate.ImportProgress
	err = ledgerstate.ImportLedgerState(
		ctx,
		ledgerstate.ImportConfig{
			Database: db,
			State:    state,
			Logger:   logger,
			OnProgress: func(p ledgerstate.ImportProgress) {
				lastProgress = p
				t.Logf("import: %s", p.Description)
			},
		},
	)
	require.NoError(t, err, "importing ledger state")

	t.Logf("final progress: %+v", lastProgress)

	// Verify tip was set
	store := db.Metadata()
	txn := db.MetadataTxn(false)
	defer txn.Release()
	tip, err := store.GetTip(txn.Metadata())
	require.NoError(t, err, "getting tip")
	require.Equal(
		t,
		state.Tip.Slot,
		tip.Point.Slot,
		"tip slot should match snapshot",
	)
	t.Logf(
		"verified tip: slot=%d hash=%x",
		tip.Point.Slot,
		tip.Point.Hash,
	)
}

// testDataDir returns the path to the immutable testdata directory
func testDataDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(
		filepath.Dir(thisFile),
		"..",
		"..",
		"database",
		"immutable",
		"testdata",
	)
}

// mockLedgerState implements the interface for ChainManager.SetLedger
type mockLedgerState struct {
	securityParam int
}

func (m *mockLedgerState) SecurityParam() int {
	return m.securityParam
}

// loadBlocksFromImmutable loads blocks from the immutable testdata into the chain
// Returns the loaded blocks and points for use in rollback tests
func loadBlocksFromImmutable(
	t *testing.T,
	c *chain.Chain,
	maxBlocks int,
) ([]ledger.Block, []ocommon.Point) {
	t.Helper()

	imm, err := immutable.New(testDataDir())
	if err != nil {
		t.Fatalf("failed to open immutable db: %v", err)
	}

	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: []byte{}})
	if err != nil {
		t.Fatalf("failed to create block iterator: %v", err)
	}
	defer iter.Close()

	var blocks []ledger.Block
	var points []ocommon.Point

	for range maxBlocks {
		immBlock, err := iter.Next()
		if err != nil {
			t.Fatalf("unexpected error reading block: %v", err)
		}
		if immBlock == nil {
			// No more blocks
			break
		}

		// Decode the block
		block, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		if err != nil {
			t.Fatalf(
				"failed to decode block at slot %d: %v",
				immBlock.Slot,
				err,
			)
		}

		// Add block to chain
		if err := c.AddBlock(block, nil); err != nil {
			t.Fatalf(
				"failed to add block to chain at slot %d: %v",
				immBlock.Slot,
				err,
			)
		}

		blocks = append(blocks, block)
		points = append(points, ocommon.Point{
			Slot: block.SlotNumber(),
			Hash: block.Hash().Bytes(),
		})
	}

	return blocks, points
}

func TestRollbackToSecurityParamDepth(t *testing.T) {
	t.Parallel()

	// Set up temporary directory for test database
	tmpDir := t.TempDir()

	// Create database
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer dbtest.CloseDatabase(db)

	// Create chain manager with database
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Set security parameter (K=432 for preview network)
	// For this test, we use a smaller value to avoid loading too many blocks
	const testSecurityParam = 50
	mockLedger := &mockLedgerState{securityParam: testSecurityParam}
	if err := cm.SetLedger(mockLedger); err != nil {
		t.Fatalf("SetLedger: %v", err)
	}

	// Get primary chain
	c := cm.PrimaryChain()
	if c == nil {
		t.Fatal("primary chain is nil")
	}

	// Load enough blocks to test rollback (K + 10 blocks)
	numBlocks := testSecurityParam + 10
	blocks, points := loadBlocksFromImmutable(t, c, numBlocks)

	if len(blocks) < numBlocks || len(points) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(blocks),
			numBlocks,
		)
	}

	// Get current tip before rollback
	tipBefore := c.Tip()
	t.Logf(
		"chain tip before rollback: slot=%d hash=%s",
		tipBefore.Point.Slot,
		hex.EncodeToString(tipBefore.Point.Hash),
	)

	// Test 1: Rollback to exactly K blocks back (should succeed)
	// The rollback point is at index (len(blocks) - 1 - K) = last block - K
	rollbackIndex := len(blocks) - 1 - testSecurityParam
	if rollbackIndex < 0 || rollbackIndex >= len(points) {
		t.Fatalf(
			"rollback index out of range: %d (blocks=%d, points=%d, K=%d)",
			rollbackIndex,
			len(blocks),
			len(points),
			testSecurityParam,
		)
	}
	rollbackPoint := points[rollbackIndex]

	t.Logf(
		"rolling back to K blocks back: slot=%d hash=%s (index=%d)",
		rollbackPoint.Slot,
		hex.EncodeToString(rollbackPoint.Hash),
		rollbackIndex,
	)

	err = c.Rollback(rollbackPoint)
	if err != nil {
		t.Fatalf(
			"rollback to K blocks should succeed, but got error: %v",
			err,
		)
	}

	// Verify chain tip after rollback
	tipAfterRollback := c.Tip()
	t.Logf(
		"chain tip after rollback: slot=%d hash=%s",
		tipAfterRollback.Point.Slot,
		hex.EncodeToString(tipAfterRollback.Point.Hash),
	)

	if tipAfterRollback.Point.Slot != rollbackPoint.Slot {
		t.Errorf(
			"tip slot mismatch after rollback: got %d, want %d",
			tipAfterRollback.Point.Slot,
			rollbackPoint.Slot,
		)
	}
	if string(tipAfterRollback.Point.Hash) != string(rollbackPoint.Hash) {
		t.Errorf(
			"tip hash mismatch after rollback: got %s, want %s",
			hex.EncodeToString(tipAfterRollback.Point.Hash),
			hex.EncodeToString(rollbackPoint.Hash),
		)
	}

	t.Log("rollback to K blocks back succeeded as expected")
}

func TestRollbackBeyondSecurityParam(t *testing.T) {
	t.Parallel()

	// Set up temporary directory for test database
	tmpDir := t.TempDir()

	// Create database
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer dbtest.CloseDatabase(db)

	// Create chain manager with database
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Set security parameter (K=432 for preview network)
	// For this test, we use a smaller value to avoid loading too many blocks
	const testSecurityParam = 50
	mockLedger := &mockLedgerState{securityParam: testSecurityParam}
	if err := cm.SetLedger(mockLedger); err != nil {
		t.Fatalf("SetLedger: %v", err)
	}

	// Get primary chain
	c := cm.PrimaryChain()
	if c == nil {
		t.Fatal("primary chain is nil")
	}

	// Load enough blocks to test rollback beyond K (K + 20 blocks)
	numBlocks := testSecurityParam + 20
	blocks, points := loadBlocksFromImmutable(t, c, numBlocks)

	if len(blocks) < numBlocks || len(points) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(blocks),
			numBlocks,
		)
	}

	// Get current tip before rollback attempt
	tipBefore := c.Tip()
	t.Logf(
		"chain tip before rollback attempt: slot=%d hash=%s",
		tipBefore.Point.Slot,
		hex.EncodeToString(tipBefore.Point.Hash),
	)

	// Test: Attempt rollback to a point that doesn't exist in the chain
	// We use a valid slot from the chain but with a fake hash
	// This simulates a rollback to a point that was never added
	fakeHash := make([]byte, 32)
	for i := range fakeHash {
		fakeHash[i] = byte(i)
	}
	// Use a slot from the middle of the chain but with wrong hash
	midIndex := len(points) / 2
	midSlot := points[midIndex].Slot
	fakePoint := ocommon.Point{
		Slot: midSlot,
		Hash: fakeHash,
	}

	t.Logf(
		"attempting rollback to non-existent point: slot=%d hash=%s",
		fakePoint.Slot,
		hex.EncodeToString(fakePoint.Hash),
	)

	err = c.Rollback(fakePoint)
	if err == nil {
		t.Fatal(
			"rollback to non-existent point should fail, but succeeded",
		)
	}

	// Assert expected error type - rollback to non-existent point
	// should return ErrBlockNotFound
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Errorf(
			"expected ErrBlockNotFound, got error type: %T, value: %v",
			err,
			err,
		)
	} else {
		t.Log("rollback failed with ErrBlockNotFound as expected")
	}

	// Verify chain tip is unchanged after failed rollback
	tipAfterFailed := c.Tip()
	if tipAfterFailed.Point.Slot != tipBefore.Point.Slot {
		t.Errorf(
			"tip slot changed after failed rollback: got %d, want %d",
			tipAfterFailed.Point.Slot,
			tipBefore.Point.Slot,
		)
	}
	if string(tipAfterFailed.Point.Hash) != string(tipBefore.Point.Hash) {
		t.Errorf(
			"tip hash changed after failed rollback: got %s, want %s",
			hex.EncodeToString(tipAfterFailed.Point.Hash),
			hex.EncodeToString(tipBefore.Point.Hash),
		)
	}

	t.Log("chain state preserved after failed rollback attempt")
}

func TestRollbackStateRestoration(t *testing.T) {
	t.Parallel()

	// Set up temporary directory for test database
	tmpDir := t.TempDir()

	// Create database
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer dbtest.CloseDatabase(db)

	// Create chain manager with database
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Set security parameter large enough that the midpoint rollback
	// (roughly half of numBlocks) stays within the allowed depth.
	const testSecurityParam = 100
	mockLedger := &mockLedgerState{securityParam: testSecurityParam}
	if err := cm.SetLedger(mockLedger); err != nil {
		t.Fatalf("SetLedger: %v", err)
	}

	// Get primary chain
	c := cm.PrimaryChain()
	if c == nil {
		t.Fatal("primary chain is nil")
	}

	// Load blocks
	numBlocks := 100
	blocks, points := loadBlocksFromImmutable(t, c, numBlocks)

	if len(blocks) < numBlocks || len(points) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(blocks),
			numBlocks,
		)
	}

	// Get initial tip
	initialTip := c.Tip()
	t.Logf(
		"initial chain tip: slot=%d hash=%s blockNumber=%d",
		initialTip.Point.Slot,
		hex.EncodeToString(initialTip.Point.Hash),
		initialTip.BlockNumber,
	)

	// Rollback to a midpoint
	midpointIndex := len(points) / 2
	midpointPoint := points[midpointIndex]

	t.Logf(
		"rolling back to midpoint: slot=%d hash=%s (index=%d)",
		midpointPoint.Slot,
		hex.EncodeToString(midpointPoint.Hash),
		midpointIndex,
	)

	err = c.Rollback(midpointPoint)
	if err != nil {
		t.Fatalf("rollback to midpoint failed: %v", err)
	}

	// Verify tip after rollback
	tipAfterRollback := c.Tip()
	if tipAfterRollback.Point.Slot != midpointPoint.Slot {
		t.Errorf(
			"tip slot mismatch after rollback: got %d, want %d",
			tipAfterRollback.Point.Slot,
			midpointPoint.Slot,
		)
	}
	if string(tipAfterRollback.Point.Hash) != string(midpointPoint.Hash) {
		t.Errorf(
			"tip hash mismatch after rollback: got %s, want %s",
			hex.EncodeToString(tipAfterRollback.Point.Hash),
			hex.EncodeToString(midpointPoint.Hash),
		)
	}

	// Verify the chain tip reflects the rollback point
	// Note: BlockByPoint may still find blocks in blob storage after
	// rollback since blob storage retains block data even after chain
	// rollback. The important thing is that the chain tip is correct.
	if tipAfterRollback.BlockNumber != blocks[midpointIndex].BlockNumber() {
		t.Errorf(
			"tip block number mismatch after rollback: got %d, want %d",
			tipAfterRollback.BlockNumber,
			blocks[midpointIndex].BlockNumber(),
		)
	}

	// Verify the rollback point block is still accessible
	block, err := c.BlockByPoint(midpointPoint, nil)
	if err != nil {
		t.Errorf(
			"rollback point block should still be accessible, "+
				"got error: %v",
			err,
		)
	} else if block.Slot != midpointPoint.Slot {
		t.Errorf(
			"rollback point block slot mismatch: got %d, want %d",
			block.Slot,
			midpointPoint.Slot,
		)
	}

	t.Log("state correctly restored after rollback")

	// Test that we can add new blocks after rollback
	// We need to reload blocks from the rollback point forward
	imm, err := immutable.New(testDataDir())
	if err != nil {
		t.Fatalf("failed to reopen immutable db: %v", err)
	}

	iter, err := imm.BlocksFromPoint(midpointPoint)
	if err != nil {
		t.Fatalf(
			"failed to create block iterator from midpoint: %v",
			err,
		)
	}
	defer iter.Close()

	// Skip the midpoint block itself (it's already in the chain)
	_, err = iter.Next()
	if err != nil {
		t.Fatalf("failed to skip midpoint block: %v", err)
	}

	// Add a few more blocks after the rollback point
	blocksAdded := 0
	for range 10 {
		immBlock, err := iter.Next()
		if err != nil {
			t.Fatalf(
				"unexpected error reading block after rollback: %v",
				err,
			)
		}
		if immBlock == nil {
			break
		}

		decodedBlock, err := ledger.NewBlockFromCbor(
			immBlock.Type,
			immBlock.Cbor,
		)
		if err != nil {
			t.Fatalf("failed to decode block: %v", err)
		}

		if err := c.AddBlock(decodedBlock, nil); err != nil {
			t.Fatalf("failed to add block after rollback: %v", err)
		}
		blocksAdded++
	}

	t.Logf("successfully added %d blocks after rollback", blocksAdded)

	// Verify new tip is correct
	newTip := c.Tip()
	if newTip.Point.Slot <= midpointPoint.Slot {
		t.Errorf(
			"new tip slot should be greater than midpoint: "+
				"got %d, midpoint was %d",
			newTip.Point.Slot,
			midpointPoint.Slot,
		)
	}

	t.Logf(
		"final chain tip: slot=%d hash=%s blockNumber=%d",
		newTip.Point.Slot,
		hex.EncodeToString(newTip.Point.Hash),
		newTip.BlockNumber,
	)
}

func TestRollbackToOrigin(t *testing.T) {
	t.Parallel()

	// Set up temporary directory for test database
	tmpDir := t.TempDir()

	// Create database
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer dbtest.CloseDatabase(db)

	// Create chain manager with database
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Set security parameter
	mockLedger := &mockLedgerState{securityParam: 100}
	if err := cm.SetLedger(mockLedger); err != nil {
		t.Fatalf("SetLedger: %v", err)
	}

	// Get primary chain
	c := cm.PrimaryChain()
	if c == nil {
		t.Fatal("primary chain is nil")
	}

	// Load some blocks
	numBlocks := 50
	blocks, _ := loadBlocksFromImmutable(t, c, numBlocks)

	if len(blocks) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(blocks),
			numBlocks,
		)
	}

	// Get tip before rollback
	tipBefore := c.Tip()
	t.Logf(
		"chain tip before rollback to origin: slot=%d hash=%s",
		tipBefore.Point.Slot,
		hex.EncodeToString(tipBefore.Point.Hash),
	)

	// Rollback to origin (slot 0, empty hash)
	originPoint := ocommon.Point{
		Slot: 0,
		Hash: []byte{},
	}

	err = c.Rollback(originPoint)
	if err != nil {
		t.Fatalf("rollback to origin failed: %v", err)
	}

	// Verify tip is at origin
	tipAfter := c.Tip()
	if tipAfter.Point.Slot != 0 {
		t.Errorf(
			"tip slot after origin rollback should be 0, got %d",
			tipAfter.Point.Slot,
		)
	}
	if len(tipAfter.Point.Hash) != 0 {
		t.Errorf(
			"tip hash after origin rollback should be empty, got %s",
			hex.EncodeToString(tipAfter.Point.Hash),
		)
	}

	t.Log("successfully rolled back to origin")
}

func TestChainIteratorAfterRollback(t *testing.T) {
	t.Parallel()

	// Set up temporary directory for test database
	tmpDir := t.TempDir()

	// Create database
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer dbtest.CloseDatabase(db)

	// Create chain manager with database
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Set security parameter
	mockLedger := &mockLedgerState{securityParam: 50}
	if err := cm.SetLedger(mockLedger); err != nil {
		t.Fatalf("SetLedger: %v", err)
	}

	// Get primary chain
	c := cm.PrimaryChain()
	if c == nil {
		t.Fatal("primary chain is nil")
	}

	// Load blocks
	numBlocks := 100
	blocks, points := loadBlocksFromImmutable(t, c, numBlocks)

	if len(blocks) < numBlocks || len(points) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(blocks),
			numBlocks,
		)
	}

	// Create iterator from origin
	iter, err := c.FromPoint(ocommon.NewPointOrigin(), false)
	if err != nil {
		t.Fatalf("failed to create chain iterator: %v", err)
	}
	defer iter.Cancel()

	// Advance iterator to near the tip
	for i := 0; i < len(blocks)-10; i++ {
		next, err := iter.Next(false)
		if err != nil {
			if errors.Is(err, chain.ErrIteratorChainTip) {
				break
			}
			t.Fatalf("unexpected error from iterator: %v", err)
		}
		if next == nil {
			t.Fatal("unexpected nil from iterator")
		}
	}

	// Rollback to midpoint
	midpointIndex := len(points) / 2
	midpointPoint := points[midpointIndex]

	t.Logf(
		"rolling back while iterator is active: slot=%d",
		midpointPoint.Slot,
	)

	err = c.Rollback(midpointPoint)
	if err != nil {
		t.Fatalf("rollback failed: %v", err)
	}

	// Iterator should report rollback on next call
	next, err := iter.Next(false)
	if err != nil {
		t.Fatalf("iterator.Next after rollback failed: %v", err)
	}
	if next == nil {
		t.Fatal("iterator returned nil after rollback")
	}
	if !next.Rollback {
		t.Error(
			"iterator should report rollback, " +
				"but Rollback flag is false",
		)
	}

	// Verify rollback point matches
	if next.Point.Slot != midpointPoint.Slot {
		t.Errorf(
			"rollback point slot mismatch: got %d, want %d",
			next.Point.Slot,
			midpointPoint.Slot,
		)
	}

	t.Log("iterator correctly reported rollback event")
}
