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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/require"
)

// newHookTestLedger builds a minimal LedgerState with a discard logger, matching
// the logger NewLedgerState installs when none is configured.
func newHookTestLedger(t *testing.T) (*LedgerState, *database.Database) {
	t.Helper()
	db := newDonationTestDB(t)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	return ls, db
}

// TestCaptureEpochBoundarySnapshotHookNil verifies the rollover capture is a
// no-op (and does not error) when no hook is installed — preserving the
// event-driven fallback-only behavior.
func TestCaptureEpochBoundarySnapshotHookNil(t *testing.T) {
	ls, db := newHookTestLedger(t)

	result := &EpochRolloverResult{
		NewCurrentEpoch: models.Epoch{EpochId: 1, StartSlot: 432000},
	}
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.captureEpochBoundarySnapshot(
			txn, models.Epoch{EpochId: 0}, result,
		)
	}))
}

// TestCaptureEpochBoundarySnapshotHookInvoked verifies the hook is called inside
// the rollover transaction with an event derived from the new/previous epoch.
func TestCaptureEpochBoundarySnapshotHookInvoked(t *testing.T) {
	ls, db := newHookTestLedger(t)

	var called bool
	var got event.EpochTransitionEvent
	ls.SetEpochBoundarySnapshotHook(
		func(_ *database.Txn, evt event.EpochTransitionEvent) error {
			called = true
			got = evt
			return nil
		},
	)

	result := &EpochRolloverResult{
		NewCurrentEpoch: models.Epoch{
			EpochId:   1,
			StartSlot: 432000,
			Nonce:     []byte{0xaa, 0xbb},
		},
	}
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.captureEpochBoundarySnapshot(
			txn, models.Epoch{EpochId: 0}, result,
		)
	}))

	require.True(t, called, "hook must be invoked during the rollover")
	require.Equal(t, uint64(0), got.PreviousEpoch)
	require.Equal(t, uint64(1), got.NewEpoch)
	require.Equal(t, uint64(432000), got.BoundarySlot)
	require.Equal(t, uint64(431999), got.SnapshotSlot)
	require.Equal(t, []byte{0xaa, 0xbb}, got.EpochNonce)
}

// TestCaptureEpochBoundarySnapshotHookFailureDeferred verifies that a hook
// failure is swallowed (the rollover is not aborted) and that the failed
// capture's writes are rolled back to the savepoint rather than committed.
func TestCaptureEpochBoundarySnapshotHookFailureDeferred(t *testing.T) {
	ls, db := newHookTestLedger(t)

	ls.SetEpochBoundarySnapshotHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			// Write a row, then fail: the savepoint rollback must discard it.
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
			return errors.New("capture boom")
		},
	)

	result := &EpochRolloverResult{
		NewCurrentEpoch: models.Epoch{EpochId: 1, StartSlot: 432000},
	}
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		// Must NOT surface the hook error: capture failures defer to the
		// event-driven fallback rather than wedging the rollover.
		return ls.captureEpochBoundarySnapshot(
			txn, models.Epoch{EpochId: 0}, result,
		)
	}))

	snap, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.Nil(t, snap,
		"a failed capture must be rolled back to the savepoint, not committed")
}
