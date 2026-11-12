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

package ledger

import (
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger/eras"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestRollbackWithinSecurityParamIsNotRefusedByUtxoPruneFloor asserts the
// Praos rule: a rollback of at most k blocks must be accepted. The consumed
// UTxO prune floor was measured in slots (tip minus the 3k/f stability window)
// while k is measured in blocks, so on a low-density chain the k-th block back
// from the tip can sit below that slot floor. The sweep now caps its target at
// the slot of the block k deep.
//
// The fixture's tail is sparse in slots but dense in blocks: the consumer block
// is 430 blocks (k=432) below the tip yet 30000 slots below it, under the
// 114080 slot window. Before the fix the sweep raised the floor to 114080 and
// refused this rollback with ErrRollbackBelowUtxoPruneFloor.
func TestRollbackWithinSecurityParamIsNotRefusedByUtxoPruneFloor(t *testing.T) {
	t.Parallel()

	f := newPrunedUtxoFixtureWithFillers(t, 0, 428, true)
	f.ls.cleanupConsumedUtxos(context.Background())
	floor, err := f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(pruneFixtureProducerSlot),
		floor,
		"the floor must be the slot of the block k deep, not tip-3k/f",
	)

	target := ocommon.NewPoint(
		pruneFixtureConsumerSlot,
		testHashBytes("3766-consumer"),
	)
	require.Less(
		t,
		target.Slot,
		uint64(pruneFixtureFloorSlot),
		"fixture must place the fork point below the slot-based window",
	)
	require.NoError(
		t,
		f.ls.chain.ValidateRollback(context.Background(), target),
		"fork point is 430 blocks deep and must be within k",
	)
	err = f.ls.rollback(context.Background(), target)
	require.NotErrorIs(t, err, ErrRollbackBelowUtxoPruneFloor)
	require.NoError(t, err)
	require.Equal(t, target.Slot, f.ls.currentTip.Point.Slot)
}

// TestConsumedUtxoSweepWaitsForKBlocks pins the other half of the fix: a chain
// shorter than k blocks may be replaced whole, so nothing may be pruned yet,
// however far the tip is past the slot window.
func TestConsumedUtxoSweepWaitsForKBlocks(t *testing.T) {
	t.Parallel()

	f := newPrunedUtxoFixtureWithFillers(t, 0, 0, false)
	f.ls.cleanupConsumedUtxos(context.Background())
	floor, err := f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Zero(t, floor)
}

// TestConsumedUtxoSweepSkipsWhenSecurityParamUnavailable pins that an unknown
// k skips the prune instead of substituting the 50000 fallback depth. The
// chain is deeper than the fallback, so a sweep that used it would find a
// k-deep point and raise the prune floor, which could refuse a rollback the
// chain manager still accepts.
func TestConsumedUtxoSweepSkipsWhenSecurityParamUnavailable(t *testing.T) {
	t.Parallel()

	f := newPrunedUtxoFixtureWithFillers(
		t, 0, blockfetchBatchSlotThresholdDefault+10, false,
	)
	f.ls.config.CardanoNodeConfig = nil
	_, ok := f.ls.securityParamForEra(eras.ConwayEraDesc.Id)
	require.False(t, ok, "fixture must have no security parameter")
	f.ls.cleanupConsumedUtxos(context.Background())
	floor, err := f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Zero(t, floor)
}

// TestConsumedUtxoSweepReadsDepthAfterRollbackSerializes holds the prune
// boundary the way a rolling-back caller does, truncates the chain below the
// point k blocks deep, and then lets a sweep that started earlier run. The
// sweep must locate the k-deep point after the truncation: a target computed
// before it would delete rows a rollback from the new, shorter chain can
// still need to restore.
func TestConsumedUtxoSweepReadsDepthAfterRollbackSerializes(t *testing.T) {
	t.Parallel()

	f := newDeepPrunedUtxoFixture(t, 0)

	f.ls.consumedUtxoPruneMutex.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.ls.cleanupConsumedUtxos(context.Background())
	}()
	// Give the sweep time to reach the boundary; the assertions hold
	// whenever it gets there, but only a sweep that already read the depth
	// can fail them.
	time.Sleep(300 * time.Millisecond)
	require.NoError(t, f.ls.chain.Rollback(
		context.Background(),
		ocommon.NewPoint(
			pruneFixtureFloorSlot, testHashBytes("3766-floor"),
		),
	))
	f.ls.consumedUtxoPruneMutex.Unlock()
	<-done

	floor, err := f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Zero(t, floor, "no spent row is prunable on a chain shorter than k")
}
