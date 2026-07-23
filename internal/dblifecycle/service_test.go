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
	"context"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/stretchr/testify/require"
)

func testConfig(dataDir string) *config.Config {
	return &config.Config{
		DatabasePath:   dataDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
		StorageMode:    "core",
		Network:        "test",
	}
}

func uint64Ptr(v uint64) *uint64 { return &v }

// TestServiceSnapshotAndRestore verifies the offline path end to end:
// Service.Snapshot writes a manifest, and Service.Restore reads it back.
func TestServiceSnapshotAndRestore(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "src")
	svc := dblifecycle.NewService(testConfig(srcDir), nil)

	// Seed the source database directly since Service has no block-write
	// API of its own.
	db, err := database.New(&database.Config{
		DataDir:        srcDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	snapDir := filepath.Join(t.TempDir(), "snap")
	m, err := svc.Snapshot(context.Background(), snapDir)
	require.NoError(t, err)
	require.Equal(t, config.DefaultBlobPlugin, m.BlobPlugin)

	restoreSvc := dblifecycle.NewService(
		testConfig(filepath.Join(t.TempDir(), "restored")), nil,
	)
	restoredManifest, err := restoreSvc.Restore(context.Background(), snapDir)
	require.NoError(t, err)
	require.Equal(t, m.CommitTimestamp, restoredManifest.CommitTimestamp)
}

// TestServiceTruncateRequiresExactlyOneTarget verifies that Truncate
// rejects both no target set and more than one target field set at once.
func TestServiceTruncateRequiresExactlyOneTarget(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := database.New(&database.Config{
		DataDir:        dir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	svc := dblifecycle.NewService(testConfig(dir), nil)

	_, err = svc.Truncate(context.Background(), dblifecycle.TruncateTarget{})
	require.Error(t, err)

	_, err = svc.Truncate(context.Background(), dblifecycle.TruncateTarget{
		Slot:        uint64Ptr(1),
		BlockNumber: uint64Ptr(1),
	})
	require.Error(t, err)
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
	svc := dblifecycle.NewService(testConfig(t.TempDir()), nil)
	live := &fakeLiveNode{}
	svc.SetLiveNode(live)

	snapManifest, err := svc.Snapshot(context.Background(), "/some/dest/dir")
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
	blocksRemoved, err := svc.Truncate(context.Background(), dblifecycle.TruncateTarget{
		Slot: &slot,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(3), blocksRemoved)
	require.True(t, live.truncateCalled)
	require.Equal(t, &slot, live.truncateTarget.Slot)
}
