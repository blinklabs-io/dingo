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
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/stretchr/testify/require"
)

// midnightCheckpointSlot reads the Midnight indexer's backfill checkpoint
// from the node's current storage. A rebuilt indexer that backfilled the
// replaced database leaves it at the ledger tip; one that was never
// recreated, or that was handed the closed database, leaves none.
func midnightCheckpointSlot(t *testing.T, n *Node) (uint64, bool) {
	t.Helper()
	cp, err := n.db.Metadata().GetBackfillCheckpoint("midnight", nil)
	require.NoError(t, err)
	if cp == nil {
		return 0, false
	}
	return cp.LastSlot, true
}

// TestLiveTruncateReinitializesMidnightIndexerByEnableGate drives a real live
// truncate and checks that the Midnight indexer is recreated against the
// rebuilt storage only when the subsystem is enabled.
func TestLiveTruncateReinitializesMidnightIndexerByEnableGate(t *testing.T) {
	t.Parallel()

	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled],
			func(t *testing.T) {
				t.Parallel()

				const numBlocks = 20
				n, points := newLiveLifecycleTestNodeWithStorageMode(
					t, numBlocks, nil,
					disabledLiveLifecycleTestWorkerPoolCfg, StorageModeAPI,
				)
				n.config.midnight = MidnightConfig{Enabled: enabled}

				targetSlot := points[numBlocks/2].Slot
				_, err := n.Truncate(
					context.Background(),
					dblifecycle.TruncateTarget{Slot: &targetSlot},
				)
				require.NoError(t, err)

				slot, ok := midnightCheckpointSlot(t, n)
				if !enabled {
					require.Nil(t, n.midnightIndexer,
						"a disabled Midnight indexer must not be recreated")
					require.False(t, ok,
						"a disabled Midnight indexer must not backfill")
					return
				}
				require.NotNil(t, n.midnightIndexer)
				require.True(t, ok,
					"the recreated indexer must backfill the rebuilt storage")
				require.Equal(t, targetSlot, slot,
					"backfill must stop at the truncated ledger tip")
				n.midnightIndexer.Stop()
			})
	}
}

// TestLiveRestoreReinitializesMidnightIndexerByEnableGate is the restore
// counterpart: the indexer is recreated against the restored storage only
// when the subsystem is enabled.
func TestLiveRestoreReinitializesMidnightIndexerByEnableGate(t *testing.T) {
	t.Parallel()

	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled],
			func(t *testing.T) {
				t.Parallel()

				const numBlocks = 20
				n, points := newLiveLifecycleTestNodeWithStorageMode(
					t, numBlocks, nil,
					disabledLiveLifecycleTestWorkerPoolCfg, StorageModeAPI,
				)
				snapshotDir := filepath.Join(t.TempDir(), "snapshot")
				_, err := lifecycleSnapshot(t, n, snapshotDir)
				require.NoError(t, err)
				n.config.midnight = MidnightConfig{Enabled: enabled}

				_, err = n.Restore(context.Background(), snapshotDir)
				require.NoError(t, err)

				slot, ok := midnightCheckpointSlot(t, n)
				if !enabled {
					require.Nil(t, n.midnightIndexer,
						"a disabled Midnight indexer must not be recreated")
					require.False(t, ok,
						"a disabled Midnight indexer must not backfill")
					return
				}
				require.NotNil(t, n.midnightIndexer)
				require.True(t, ok,
					"the recreated indexer must backfill the restored storage")
				require.Equal(t, points[len(points)-1].Slot, slot,
					"backfill must stop at the restored ledger tip")
				n.midnightIndexer.Stop()
			})
	}
}
