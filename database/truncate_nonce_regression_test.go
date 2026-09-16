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

package database

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestTruncateAfterSlotRejectsTargetWithPrunedNonce reproduces a
// live-incident finding: dingo prunes non-checkpoint block_nonce rows older
// than 3 epochs behind the current tip (ledger/state.go's
// cleanupBlockNoncesBefore), keeping only each epoch's single checkpoint
// row. A disaster-recovery truncate (database/lifecycle/truncate.go) can
// roll the tip back to an arbitrary historical point that is *older* than
// that 3-epoch retention window -- exactly the case that already ran
// against a live database and produced a wrong epoch nonce for the epoch
// immediately following the truncate point (every VRF verification in that
// epoch then failed against real, canonical chain headers on 4 independent
// nodes).
//
// TruncateAfterSlot fetches the new tip's evolving nonce with a single
// exact-point lookup (GetBlockNonce(point)). GetBlockNonce returns (nil,
// nil) -- no error -- when no row matches the point, so when the target
// block's own block_nonce row has already been pruned (its epoch's
// checkpoint is some other, earlier block), TruncateAfterSlot used to
// silently return an empty nonce instead of failing loudly. A node restart
// after such a truncate then seeds the resumed evolving-nonce fold
// (LedgerState.loadTip -> ledgerProcessBlocks' runningNonce) from empty
// bytes, corrupting every block nonce computed for the remainder of the
// epoch and, through it, the following epoch's nonce.
//
// This test does not fold real VRF outputs (it does not need real block
// content to demonstrate the defect): it shows that a truncate target whose
// own block_nonce row was pruned must now fail loudly instead of silently
// returning an empty nonce, even though an earlier, non-pruned checkpoint
// row exists for the same epoch.
func TestTruncateAfterSlotRejectsTargetWithPrunedNonce(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	// Epoch checkpoint block: the first block persisted in its epoch,
	// whose block_nonce row survives the 3-epoch retention pruning
	// (ledger/state.go's cleanupBlockNoncesBefore keeps checkpoints
	// indefinitely).
	checkpointBlock := testIndexedBlock(1000, 1, 0x10)
	require.NoError(t, db.BlockCreate(checkpointBlock, nil))
	checkpointNonce := bytes.Repeat([]byte{0xc1}, 32)
	require.NoError(t, db.SetBlockNonce(
		checkpointBlock.Hash,
		checkpointBlock.Slot,
		checkpointNonce,
		true, // isCheckpoint
		nil,
	))

	// Truncate target: a later, ordinary (non-checkpoint) block in the
	// same epoch. In production this is the disaster-recovery truncate's
	// target -- often many epochs behind the pre-truncate tip. Given a
	// Praos (post-Byron) block type explicitly: testIndexedBlock defaults
	// to Type 1 (byron.BlockTypeByronMain), which the fix under test
	// deliberately exempts from the empty-nonce check since Byron blocks
	// have no Praos nonce at all. babbage.BlockTypeBabbage matches the
	// live incident, which occurred entirely within the Babbage era.
	targetBlock := testIndexedBlock(1500, 2, 0x15)
	targetBlock.Type = babbage.BlockTypeBabbage
	require.NoError(t, db.BlockCreate(targetBlock, nil))
	targetNonce := bytes.Repeat([]byte{0xc2}, 32)
	require.NoError(t, db.SetBlockNonce(
		targetBlock.Hash,
		targetBlock.Slot,
		targetNonce,
		false, // isCheckpoint
		nil,
	))

	// Sanity check: before pruning, TruncateAfterSlot correctly returns
	// the target block's own nonce.
	point := ocommon.Point{Slot: targetBlock.Slot, Hash: targetBlock.Hash}
	_, nonceBeforePruning, err := db.TruncateAfterSlot(point, 0, nil)
	require.NoError(t, err)
	require.Equal(t, targetNonce, nonceBeforePruning,
		"sanity check: truncate must return the target's own nonce "+
			"while its row still exists")

	// Simulate the routine 3-epoch retention pruning that runs during
	// normal operation: it removes the target's own (non-checkpoint) row
	// while preserving the earlier checkpoint. In production this can
	// happen long before any truncate is requested, simply because the
	// live chain kept advancing for 3+ epochs after slot 1500.
	require.NoError(t, db.DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
		targetBlock.Slot+1,
		nil,
	))
	prunedNonce, err := db.GetBlockNonce(point, nil)
	require.NoError(t, err)
	require.Empty(t, prunedNonce,
		"sanity check: the target's own block_nonce row must actually "+
			"be gone after retention pruning")
	survivingCheckpoint, err := db.GetBlockNonce(
		ocommon.Point{Slot: checkpointBlock.Slot, Hash: checkpointBlock.Hash},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, checkpointNonce, survivingCheckpoint,
		"sanity check: the epoch's checkpoint row must survive retention pruning")

	// Re-run the exact same truncate against the now-pruned target. Before
	// the fix, this silently returned (Tip, nil-nonce, nil-error) -- the
	// exact live-incident mechanism: a deep 'dingo database truncate'
	// landing on a pruned slot silently corrupted the resumed nonce chain,
	// causing every VRF verification in the following epoch to fail
	// against real, canonical chain headers on 4 independent
	// Preview-testnet nodes. TruncateAfterSlot must now refuse to proceed
	// instead of silently substituting an empty nonce.
	_, nonceAfterPruning, err := db.TruncateAfterSlot(point, 0, nil)
	require.Error(t, err,
		"TruncateAfterSlot must reject a target whose block_nonce row has "+
			"been pruned instead of silently continuing with an empty "+
			"nonce -- doing so would seed the resumed evolving-nonce fold "+
			"with wrong state and corrupt every subsequent epoch's "+
			"VRF-verification nonce")
	require.Contains(t, err.Error(), "no stored block")
	require.Nil(t, nonceAfterPruning)
}
