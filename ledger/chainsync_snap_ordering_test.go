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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/require"
)

// TestProcessEpochRollover_SnapStakeReadOrdering pins the SNAP read point.
//
// The reference sequence is NEWEPOCH = applyRUpd, MIR, EPOCH; EPOCH = SNAP,
// POOLREAP, ratification/enactment. So exactly two boundary rules precede SNAP —
// the delayed reward update and MIR — and every remaining rule that credits
// reward accounts at the boundary slot follows it: POOLREAP deposit refunds
// (applyPoolRetirements) and enacted treasury withdrawals plus proposal-deposit
// refunds (ProcessEpoch).
//
// The snapshot row itself is still written at the very end of the rollover,
// where the new epoch record and the post-enactment protocol version exist, so
// the read point and the write point are deliberately different places in the
// sequence. This test locks the read point; TestProcessEpochRollover_RewardOrdering
// and TestProcessEpochRollover_OrderingInvariant lock the rest of the sequence.
func TestProcessEpochRollover_SnapStakeReadOrdering(t *testing.T) {
	const targetFunc = "processEpochRollover"

	wantOrder := []string{
		"applyStakeRewards",                 // pre-SNAP: delayed reward update
		"applyMIRCerts",                     // pre-SNAP: Shelley-era INSTANT rule
		"captureEpochBoundarySnapshotStake", // SNAP read point
		"applyPoolRetirements",              // post-SNAP: POOLREAP refunds
		"ProcessEpoch",                      // post-SNAP: enactment credits
		"captureEpochBoundarySnapshot",      // snapshot write, end of rollover
	}

	seen, observed := observeProcessEpochRolloverCallOrder(t, targetFunc, wantOrder)

	for _, m := range wantOrder {
		require.True(t, seen[m],
			"marker %q not found in %s body — the SNAP read must stay wired "+
				"between the pre-SNAP boundary rules and the boundary rules that "+
				"credit reward accounts after SNAP.",
			m, targetFunc)
	}

	require.Equal(t, wantOrder, observed,
		"SNAP read point in %s drifted. The mark snapshot's stake must be read "+
			"after applyStakeRewards and applyMIRCerts, which precede SNAP in "+
			"cardano-ledger, and before applyPoolRetirements and ProcessEpoch, "+
			"which credit reward accounts at the boundary slot after it. "+
			"Expected %v, observed %v.",
		targetFunc, wantOrder, observed)
}

// TestCaptureEpochBoundarySnapshotStakeHookInvoked verifies the SNAP-point stake
// hook receives the same boundary identity the persist hook later builds from the
// new epoch record, so the two phases of one capture can be matched.
func TestCaptureEpochBoundarySnapshotStakeHookInvoked(t *testing.T) {
	ls, db := newHookTestLedger(t)

	var called bool
	var got event.EpochTransitionEvent
	ls.SetEpochBoundarySnapshotStakeHook(
		func(_ *database.Txn, evt event.EpochTransitionEvent) error {
			called = true
			got = evt
			return nil
		},
	)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		ls.captureEpochBoundarySnapshotStake(
			txn, models.Epoch{EpochId: 0}, 432000,
		)
		return nil
	}))

	require.True(t, called, "stake hook must be invoked during the rollover")
	require.Equal(t, uint64(0), got.PreviousEpoch)
	require.Equal(t, uint64(1), got.NewEpoch)
	require.Equal(t, uint64(432000), got.BoundarySlot)
	require.Equal(t, uint64(431999), got.SnapshotSlot)
}

// TestCaptureEpochBoundarySnapshotStakeHookFailureDeferred verifies a failed
// SNAP-point read neither aborts the rollover nor leaves its writes behind: the
// persist half then reads the stake itself.
func TestCaptureEpochBoundarySnapshotStakeHookFailureDeferred(t *testing.T) {
	ls, db := newHookTestLedger(t)

	ls.SetEpochBoundarySnapshotStakeHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			// Read hooks must not write, but prove the savepoint covers it.
			if err := db.Metadata().SaveRewardSnapshot(&models.RewardSnapshot{
				Epoch:           evt.NewEpoch,
				SnapshotType:    "mark",
				CapturedSlot:    1,
				BoundarySlot:    1,
				ProtocolVersion: 8,
				Authoritative:   true,
			}, txn.Metadata()); err != nil {
				return err
			}
			return errStakeHookBoom
		},
	)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		ls.captureEpochBoundarySnapshotStake(
			txn, models.Epoch{EpochId: 0}, 432000,
		)
		return nil
	}))

	snap, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.Nil(t, snap,
		"a failed snap-point read must be rolled back to the savepoint")
}

// errStakeHookBoom is a sentinel failure for the snap-point stake hook test.
var errStakeHookBoom = errors.New("snap-point read boom")
