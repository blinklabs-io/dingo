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
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/stretchr/testify/require"
)

func newManagerTestDB(t *testing.T) *database.Database {
	t.Helper()
	dir := t.TempDir()
	db, err := database.New(&database.Config{
		DataDir:        dir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func publishEpochTransition(eb *event.EventBus, newEpoch uint64) {
	eb.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(event.EpochTransitionEventType, event.EpochTransitionEvent{
			PreviousEpoch: newEpoch - 1,
			NewEpoch:      newEpoch,
		}),
	)
}

// TestManagerDisabledByDefaultDoesNothing verifies that with
// SnapshotEnabled false, an epoch-transition event never triggers a snapshot.
func TestManagerDisabledByDefaultDoesNothing(t *testing.T) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	snapshotDir := filepath.Join(t.TempDir(), "snapshots")
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled: false,
		SnapshotDir:     snapshotDir,
	}, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 1)
	require.Never(t, func() bool {
		_, err := os.Stat(snapshotDir)
		return err == nil
	}, 200*time.Millisecond, 10*time.Millisecond)
}

// TestManagerCapturesSnapshotOnEpochBoundary verifies that an
// epoch-transition event captures a real snapshot under epoch-<N>.
func TestManagerCapturesSnapshotOnEpochBoundary(t *testing.T) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:      true,
		SnapshotDir:          snapshotDir,
		SnapshotEveryNEpochs: 1,
	}, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 5)

	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(snapshotDir, "epoch-5", "manifest.json"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
}

// TestManagerRespectsEveryNEpochsGating verifies that with
// SnapshotEveryNEpochs=2, only an epoch divisible by 2 is captured.
func TestManagerRespectsEveryNEpochsGating(t *testing.T) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:      true,
		SnapshotDir:          snapshotDir,
		SnapshotEveryNEpochs: 2,
	}, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 3) // 3 % 2 != 0, must be skipped
	publishEpochTransition(eb, 4) // 4 % 2 == 0, must be captured

	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(snapshotDir, "epoch-4", "manifest.json"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)

	require.NoDirExists(t, filepath.Join(snapshotDir, "epoch-3"))
}

// TestManagerRedeliveredEventIsNotFatal verifies that publishing the same
// epoch's transition event twice does not crash or stall the manager.
func TestManagerRedeliveredEventIsNotFatal(t *testing.T) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:      true,
		SnapshotDir:          snapshotDir,
		SnapshotEveryNEpochs: 1,
	}, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 7)
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(snapshotDir, "epoch-7", "manifest.json"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)

	// Redeliver the same epoch's event (simulating a restart replay) —
	// must not crash the loop or leave it stuck; a later, new epoch must
	// still be captured afterward.
	publishEpochTransition(eb, 7)
	publishEpochTransition(eb, 8)
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(snapshotDir, "epoch-8", "manifest.json"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
}

// TestManagerPrunesOldSnapshotsBeyondRetention verifies that with
// SnapshotRetention=2, capturing a 3rd snapshot deletes the oldest one.
func TestManagerPrunesOldSnapshotsBeyondRetention(t *testing.T) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:      true,
		SnapshotDir:          snapshotDir,
		SnapshotEveryNEpochs: 1,
		SnapshotRetention:    2,
	}, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	for epoch := uint64(1); epoch <= 3; epoch++ {
		publishEpochTransition(eb, epoch)
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(
				snapshotDir,
				"epoch-"+strconv.FormatUint(epoch, 10),
				"manifest.json",
			))
			return err == nil
		}, 5*time.Second, 10*time.Millisecond)
	}

	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(snapshotDir, "epoch-1"))
		return os.IsNotExist(err)
	}, 5*time.Second, 10*time.Millisecond)
	require.DirExists(t, filepath.Join(snapshotDir, "epoch-2"))
	require.DirExists(t, filepath.Join(snapshotDir, "epoch-3"))
}
