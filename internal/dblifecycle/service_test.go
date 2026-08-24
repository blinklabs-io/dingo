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

package dblifecycle_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/plugin"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func testConfig(dataDir string) *config.Config {
	return &config.Config{
		DatabasePath: dataDir,
		Plugins: config.PluginsConfig{
			Storage: config.StoragePluginsConfig{
				Blob:     plugin.Selection{Provider: "badger"},
				Metadata: plugin.Selection{Provider: "sqlite"},
			},
		},
		StorageMode: "core",
		Network:     "test",
	}
}

//go:fix inline
func uint64Ptr(v uint64) *uint64 { return new(v) }

// seedCommitTimestampMismatch opens dir's database, forces the metadata
// store's commit timestamp out of sync with the blob store's, and closes it
// again -- mirroring internal/plugins.
// TestOpenDatabaseReturnsRecoveryErrorOnRuntime's setup for producing the
// same recoverable database.CommitTimestampError that OpenDatabase
// surfaces through DatabaseRuntime.RecoveryError instead of as an error
// return.
func seedCommitTimestampMismatch(t *testing.T, dir string) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: dir})
	require.NoError(t, err)
	metaTxn := db.Metadata().Transaction(t.Context())
	require.NoError(t, db.Metadata().SetCommitTimestamp(123456789, metaTxn))
	require.NoError(t, metaTxn.Commit())
	require.NoError(t, dbtest.CloseDatabase(db))
}

// TestServiceSnapshotAndRestore verifies the offline path end to end:
// Service.Snapshot writes a manifest, and Service.Restore reads it back.
func TestServiceSnapshotAndRestore(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "src")
	svc := dblifecycle.NewService(testConfig(srcDir), nil, nil)

	// Seed the source database directly since Service has no block-write
	// API of its own.
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: srcDir})
	require.NoError(t, err)
	require.NoError(t, dbtest.CloseDatabase(db))

	snapDir := filepath.Join(t.TempDir(), "snap")
	m, err := svc.Snapshot(context.Background(), snapDir, "", "")
	require.NoError(t, err)
	require.Equal(t, "badger", m.BlobPlugin)

	restoreSvc := dblifecycle.NewService(
		testConfig(filepath.Join(t.TempDir(), "restored")), nil, nil,
	)
	restoredManifest, err := restoreSvc.Restore(context.Background(), snapDir)
	require.NoError(t, err)
	require.Equal(t, m.CommitTimestamp, restoredManifest.CommitTimestamp)
}

// TestServiceRestoreRejectsIncompatibleTarget verifies that Service.
// Restore refuses a snapshot whose recorded network doesn't match the
// restoring Service's own configured network, before ever creating the
// target data directory — guarding against a real gap,
// where the offline restore path had no check against the caller's own
// configuration at all (only Restore's internal self-consistency check
// against the manifest's own recorded plugins).
func TestServiceRestoreRejectsIncompatibleTarget(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "src")
	srcCfg := testConfig(srcDir)
	svc := dblifecycle.NewService(srcCfg, nil, nil)

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: srcDir})
	require.NoError(t, err)
	require.NoError(t, dbtest.CloseDatabase(db))

	snapDir := filepath.Join(t.TempDir(), "snap")
	_, err = svc.Snapshot(context.Background(), snapDir, "", "")
	require.NoError(t, err)

	restoreCfg := testConfig(filepath.Join(t.TempDir(), "restored"))
	restoreCfg.Network = "mainnet"
	restoreSvc := dblifecycle.NewService(restoreCfg, nil, nil)

	_, err = restoreSvc.Restore(context.Background(), snapDir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not compatible")

	require.NoDirExists(t, restoreCfg.DatabasePath)
}

// TestServiceTruncateRequiresAtLeastOneTarget verifies that Truncate
// rejects a target with none of Slot/Hash/BlockNumber set. See
// TestResolveTargetAcceptsConsistentCombinedFields/
// TestResolveTargetRejectsInconsistentCombinedFields below for the
// "more than one field set" cases this test used to also cover, before
// ResolveTarget started accepting a combination of fields as long as they
// agree on the same block (dingo#1651 follow-up).
func TestServiceTruncateRequiresAtLeastOneTarget(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: dir})
	require.NoError(t, err)
	require.NoError(t, dbtest.CloseDatabase(db))

	svc := dblifecycle.NewService(testConfig(dir), nil, nil)

	_, err = svc.Truncate(context.Background(), dblifecycle.TruncateTarget{})
	require.Error(t, err)
}

// buildResolveTargetTestChain creates n blocks (IDs/numbers 1..n, slots
// scaled by 10, one distinct hash byte each) against a real test database
// and sets the tip to the last one, mirroring
// database/lifecycle_test.buildTestChain -- used to exercise ResolveTarget
// against real, resolvable blocks.
func buildResolveTargetTestChain(
	t *testing.T,
	n uint64,
) (*database.Database, []models.Block) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	blocks := make([]models.Block, 0, n)
	for id := uint64(1); id <= n; id++ {
		block := models.Block{
			ID:     id,
			Slot:   id * 10,
			Hash:   bytes.Repeat([]byte{byte(id)}, 32),
			Cbor:   []byte{0x80},
			Number: id,
			Type:   1,
		}
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}
	last := blocks[len(blocks)-1]
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
		BlockNumber: last.Number,
	}, nil))
	return db, blocks
}

// TestResolveTargetAcceptsConsistentCombinedFields verifies that
// ResolveTarget accepts a target with more than one of Slot/Hash/
// BlockNumber set, as long as they all identify the same block -- per the
// bark proto's documented BlockRef contract ("When multiple fields are
// set, all must agree"). Guards dingo#1651's finding that bark's own
// Truncate handler used to reject any such combination outright, even a
// mutually consistent one an operator might pass for extra safety (e.g.
// a slot and a hash it already resolved).
func TestResolveTargetAcceptsConsistentCombinedFields(t *testing.T) {
	db, blocks := buildResolveTargetTestChain(t, 5)
	target := blocks[1] // id=2, slot=20, number=2

	resolved, err := dblifecycle.ResolveTarget(db, dblifecycle.TruncateTarget{
		Slot:        &target.Slot,
		Hash:        target.Hash,
		BlockNumber: &target.Number,
	})
	require.NoError(t, err)
	require.Equal(t, target.ID, resolved.ID)
}

// TestResolveTargetRejectsInconsistentCombinedFields verifies that
// ResolveTarget rejects a target whose supplied fields disagree about
// which block is meant, rather than silently trusting whichever field
// happens to be authoritative for the actual lookup (hash, in both cases
// below).
func TestResolveTargetRejectsInconsistentCombinedFields(t *testing.T) {
	db, blocks := buildResolveTargetTestChain(t, 5)
	hashOfBlock2 := blocks[1].Hash

	wrongSlot := blocks[2].Slot // slot of block 3, not block 2
	_, err := dblifecycle.ResolveTarget(db, dblifecycle.TruncateTarget{
		Hash: hashOfBlock2,
		Slot: &wrongSlot,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match")

	wrongNumber := blocks[3].Number // number of block 4, not block 2
	_, err = dblifecycle.ResolveTarget(db, dblifecycle.TruncateTarget{
		Hash:        hashOfBlock2,
		BlockNumber: &wrongNumber,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match")
}

// TestServiceSnapshotRefusesCommitTimestampMismatch verifies that the
// offline Snapshot path checks DatabaseRuntime.RecoveryError before
// operating on the database, instead of silently backing up a store that
// OpenDatabase already flagged as inconsistent between its blob and
// metadata halves.
func TestServiceSnapshotRefusesCommitTimestampMismatch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	seedCommitTimestampMismatch(t, dir)

	svc := dblifecycle.NewService(testConfig(dir), nil, nil)
	_, err := svc.Snapshot(
		context.Background(),
		filepath.Join(t.TempDir(), "snap"),
		"",
		"",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "inconsistent")
}

// TestServiceTruncateRefusesCommitTimestampMismatch is
// TestServiceSnapshotRefusesCommitTimestampMismatch's counterpart for
// Truncate, which shares the same openDatabase helper.
func TestServiceTruncateRefusesCommitTimestampMismatch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	seedCommitTimestampMismatch(t, dir)

	svc := dblifecycle.NewService(testConfig(dir), nil, nil)
	_, err := svc.Truncate(context.Background(), dblifecycle.TruncateTarget{
		Slot: uint64Ptr(1),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "inconsistent")
}

// fakeLiveNode records whether Snapshot/Restore/Truncate were called on
// it, so tests can confirm Service.SetLiveNode actually delegates instead
// of falling through to the offline path.
type fakeLiveNode struct {
	snapshotCalled bool
	restoreCalled  bool
	truncateCalled bool
	snapshotDir    string
	restoreDir     string
	truncateTarget dblifecycle.TruncateTarget
}

func (f *fakeLiveNode) Snapshot(
	_ context.Context,
	destDir string,
	_ string,
	_ string,
) (lifecycle.Manifest, error) {
	f.snapshotCalled = true
	f.snapshotDir = destDir
	return lifecycle.Manifest{DingoVersion: "fake-live-snapshot"}, nil
}

func (f *fakeLiveNode) Restore(
	_ context.Context,
	snapshotDir string,
) (lifecycle.Manifest, error) {
	f.restoreCalled = true
	f.restoreDir = snapshotDir
	return lifecycle.Manifest{DingoVersion: "fake-live-restore"}, nil
}

func (f *fakeLiveNode) Truncate(
	_ context.Context,
	target dblifecycle.TruncateTarget,
) (uint64, error) {
	f.truncateCalled = true
	f.truncateTarget = target
	return 3, nil
}

// TestServiceDelegatesToLiveNodeWhenSet verifies that once SetLiveNode is
// called, Snapshot/Restore/Truncate delegate to it instead of the offline path.
func TestServiceDelegatesToLiveNodeWhenSet(t *testing.T) {
	svc := dblifecycle.NewService(testConfig(t.TempDir()), nil, nil)
	live := &fakeLiveNode{}
	svc.SetLiveNode(live)

	snapManifest, err := svc.Snapshot(
		context.Background(),
		"/some/dest/dir",
		"",
		"",
	)
	require.NoError(t, err)
	require.True(t, live.snapshotCalled)
	require.Equal(t, "/some/dest/dir", live.snapshotDir)
	require.Equal(t, "fake-live-snapshot", snapManifest.DingoVersion)

	manifest, err := svc.Restore(context.Background(), "/some/snapshot/dir")
	require.NoError(t, err)
	require.True(t, live.restoreCalled)
	require.Equal(t, "/some/snapshot/dir", live.restoreDir)
	require.Equal(t, "fake-live-restore", manifest.DingoVersion)

	slot := uint64(42)
	blocksRemoved, err := svc.Truncate(
		context.Background(),
		dblifecycle.TruncateTarget{
			Slot: &slot,
		},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(3), blocksRemoved)
	require.True(t, live.truncateCalled)
	require.Equal(t, &slot, live.truncateTarget.Slot)
}
