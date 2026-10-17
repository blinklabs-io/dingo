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
	"testing"

	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The selector's stale-tip rule is inactive without a wall-clock source, so
// the composition site must install the hook, and it must report an unknown
// clock rather than slot 0 while no ledger is available.
func TestBuildChainSelectorConfigWallClockSlot(t *testing.T) {
	t.Parallel()
	n := &Node{}
	cfg := n.buildChainSelectorConfig(2160, true, 0)
	require.NotNil(t, cfg.WallClockSlot)
	slot, ok := cfg.WallClockSlot()
	assert.False(t, ok)
	assert.Zero(t, slot)
}

func TestBuildChainSelectorConfigWallClockSlotUnavailableInByron(
	t *testing.T,
) {
	t.Parallel()
	cardanoNodeCfg := newNodeTestCardanoNodeCfg(t)
	shelleyEpoch := uint64(100)
	cardanoNodeCfg.TestShelleyHardForkAtEpoch = &shelleyEpoch
	n, _ := newLiveLifecycleTestNodeWithGenesis(
		t,
		4,
		cardanoNodeCfg,
		disabledLiveLifecycleTestWorkerPoolCfg,
	)
	require.True(t, n.ledgerState.IsByronEra(), "fixture must remain in Byron")

	cfg := n.buildChainSelectorConfig(2160, true, 0)
	slot, ok := cfg.WallClockSlot()
	assert.False(t, ok)
	assert.Zero(t, slot)
}

func TestCleanupFailedStartupWithdrawsWallClockLedgerBeforeClose(
	t *testing.T,
) {
	t.Parallel()
	n := &Node{}
	n.wallClockLedger.Store(&ledger.LedgerState{})
	closed := false
	n.startupLifecycleMu.Lock()
	n.cleanupFailedStartup([]func(){func() {
		closed = true
		assert.Nil(t, n.wallClockLedger.Load())
	}})

	assert.True(t, closed)
}

// The chain selector outlives a live Truncate or Restore and is never rebuilt,
// so its wall-clock hook must follow the node's current ledger state: unknown
// while storage is closed, and the rebuilt ledger afterwards, never the closed
// instance it was built alongside.
func TestChainSelectorWallClockFollowsLiveLedgerRebuild(t *testing.T) {
	t.Parallel()
	n, _ := newLiveLifecycleTestNode(t, 4)
	cfg := n.buildChainSelectorConfig(2160, true, 0)
	_, ok := cfg.WallClockSlot()
	require.True(t, ok, "fixture: wall clock unavailable before the rebuild")

	ctx := context.Background()
	n.liveLifecycleMu.Lock()
	defer n.liveLifecycleMu.Unlock()
	require.NoError(t, n.quiesceForLiveLifecycleOp(ctx))
	require.NoError(t, n.closeStorageForLiveLifecycleOp(ctx))
	_, ok = cfg.WallClockSlot()
	assert.False(t, ok,
		"wall clock read from the ledger state a live lifecycle op closed")

	require.NoError(t, n.reinitializeAndResume(ctx))
	_, ok = cfg.WallClockSlot()
	assert.True(t, ok, "wall clock unavailable after the ledger was rebuilt")
}

// The selector calls the hook from its own goroutine while a live rebuild runs,
// so the rebuilt ledger must not be visible to it before Start has finished
// initializing the slot clock the hook reads.
func TestChainSelectorWallClockConcurrentWithLiveLedgerRebuild(t *testing.T) {
	t.Parallel()
	n, _ := newLiveLifecycleTestNode(t, 4)
	cfg := n.buildChainSelectorConfig(2160, true, 0)

	ctx := context.Background()
	n.liveLifecycleMu.Lock()
	defer n.liveLifecycleMu.Unlock()
	require.NoError(t, n.quiesceForLiveLifecycleOp(ctx))
	require.NoError(t, n.closeStorageForLiveLifecycleOp(ctx))

	done := make(chan struct{})
	polled := make(chan struct{})
	go func() {
		defer close(polled)
		for {
			select {
			case <-done:
				return
			default:
				cfg.WallClockSlot()
			}
		}
	}()
	err := n.reinitializeAndResume(ctx)
	close(done)
	<-polled
	require.NoError(t, err)
	_, ok := cfg.WallClockSlot()
	assert.True(t, ok, "wall clock unavailable after the ledger was rebuilt")
}
