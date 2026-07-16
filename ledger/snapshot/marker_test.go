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

package snapshot

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/require"
)

type markerDelegation = struct {
	stakingKey  []byte
	utxoAmounts []types.Uint64
}

// TestCaptureEpochBoundarySnapshotMarksAuthoritative verifies the authoritative
// (epoch-rollover) capture writes reward_snapshot.authoritative = true.
func TestCaptureEpochBoundarySnapshotMarksAuthoritative(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})
	poolHash := []byte("poolA_12345678901234567890AB")
	stakingKey := bytes.Repeat([]byte{0xa1}, 28)
	seedPoolAndDelegations(t, db, poolHash, []markerDelegation{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{50_000_000}},
	}, 500)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432000,
		EpochNonce:      []byte{0x01, 0x02, 0x03},
		ProtocolVersion: 8,
		SnapshotSlot:    431999,
	}

	txn := db.Transaction(true)
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(context.Background(), txn, evt))
	require.NoError(t, txn.Commit())

	snap, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.True(t, snap.Authoritative,
		"epoch-rollover capture must mark the snapshot authoritative")
}

// TestFallbackCaptureMarksNonAuthoritative verifies the event-driven fallback
// capture writes reward_snapshot.authoritative = false.
func TestFallbackCaptureMarksNonAuthoritative(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})
	poolHash := []byte("poolB_12345678901234567890AB")
	stakingKey := bytes.Repeat([]byte{0xb1}, 28)
	seedPoolAndDelegations(t, db, poolHash, []markerDelegation{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{20_000_000}},
	}, 500)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432000,
		EpochNonce:      nil,
		ProtocolVersion: 8,
		SnapshotSlot:    431999,
	}
	require.NoError(t, mgr.captureMarkSnapshot(context.Background(), evt))

	snap, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.False(t, snap.Authoritative,
		"fallback capture must leave the snapshot non-authoritative")
}

// TestFallbackCaptureReplacesProvisionalFallback verifies that a later fallback
// capture (carrying the real epoch nonce) refreshes an earlier provisional
// fallback row in place, exercising the ClaimFallbackSnapshot replace branch,
// and that it stays non-authoritative.
func TestFallbackCaptureReplacesProvisionalFallback(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})
	poolHash := []byte("poolC_12345678901234567890AB")
	stakingKey := bytes.Repeat([]byte{0xc1}, 28)
	seedPoolAndDelegations(t, db, poolHash, []markerDelegation{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{30_000_000}},
	}, 500)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)

	// First (slot-clock) fallback: no nonce yet.
	provisional := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432000,
		EpochNonce:      nil,
		ProtocolVersion: 8,
		SnapshotSlot:    431999,
	}
	require.NoError(t, mgr.captureMarkSnapshot(context.Background(), provisional))
	first, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.False(t, first.Authoritative)
	require.Empty(t, first.EpochNonce)

	// Second (block-based) fallback: carries the real nonce, replaces the row.
	withNonce := provisional
	withNonce.EpochNonce = []byte{0x0a, 0x0b, 0x0c}
	require.NoError(t, mgr.captureMarkSnapshot(context.Background(), withNonce))
	second, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.False(t, second.Authoritative,
		"replacing a provisional fallback row must stay non-authoritative")
	require.Equal(t, []byte{0x0a, 0x0b, 0x0c}, second.EpochNonce,
		"the refreshed fallback row must carry the real epoch nonce")
}

// TestClaimFallbackRewardSnapshotSkipsAuthoritative verifies the metadata-level
// claim refuses to overwrite an existing authoritative row and forces the
// authoritative flag off for a fresh fallback claim.
func TestClaimFallbackRewardSnapshotSkipsAuthoritative(t *testing.T) {
	db := setupTestDB(t)
	meta := db.Metadata()

	authoritative := &models.RewardSnapshot{
		Epoch:            5,
		SnapshotType:     "mark",
		TotalActiveStake: types.Uint64(100),
		TotalPoolCount:   1,
		TotalDelegators:  1,
		CapturedSlot:     10,
		BoundarySlot:     11,
		EpochNonce:       []byte{0xde, 0xad},
		ProtocolVersion:  8,
		Authoritative:    true,
	}
	require.NoError(t, meta.SaveRewardSnapshot(authoritative, nil))

	// A fallback claim (note Authoritative: true is set by the caller but must be
	// forced false by the claim) must be refused and must not mutate the row.
	fallback := &models.RewardSnapshot{
		Epoch:            5,
		SnapshotType:     "mark",
		TotalActiveStake: types.Uint64(999),
		TotalPoolCount:   9,
		TotalDelegators:  9,
		CapturedSlot:     99,
		BoundarySlot:     99,
		ProtocolVersion:  8,
		Authoritative:    true,
	}
	proceed, err := meta.ClaimFallbackRewardSnapshot(fallback, nil)
	require.NoError(t, err)
	require.False(t, proceed,
		"fallback claim must be refused when an authoritative row exists")

	got, err := meta.GetRewardSnapshot(5, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.Authoritative)
	require.Equal(t, uint64(100), uint64(got.TotalActiveStake),
		"authoritative row must be untouched by the refused claim")
}

// TestClaimFallbackRewardSnapshotFreshAndReplace verifies a fresh claim writes a
// non-authoritative row and a second fallback claim replaces it in place.
func TestClaimFallbackRewardSnapshotFreshAndReplace(t *testing.T) {
	db := setupTestDB(t)
	meta := db.Metadata()

	first := &models.RewardSnapshot{
		Epoch:            6,
		SnapshotType:     "mark",
		TotalActiveStake: types.Uint64(100),
		TotalPoolCount:   1,
		TotalDelegators:  1,
		CapturedSlot:     10,
		BoundarySlot:     11,
		ProtocolVersion:  8,
		Authoritative:    true, // must be forced to false by the claim
	}
	proceed, err := meta.ClaimFallbackRewardSnapshot(first, nil)
	require.NoError(t, err)
	require.True(t, proceed)
	got, err := meta.GetRewardSnapshot(6, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.False(t, got.Authoritative,
		"fresh fallback claim must be non-authoritative")
	require.Equal(t, uint64(100), uint64(got.TotalActiveStake))

	second := &models.RewardSnapshot{
		Epoch:            6,
		SnapshotType:     "mark",
		TotalActiveStake: types.Uint64(200),
		TotalPoolCount:   2,
		TotalDelegators:  2,
		CapturedSlot:     20,
		BoundarySlot:     21,
		ProtocolVersion:  8,
	}
	proceed, err = meta.ClaimFallbackRewardSnapshot(second, nil)
	require.NoError(t, err)
	require.True(t, proceed)
	got, err = meta.GetRewardSnapshot(6, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.False(t, got.Authoritative)
	require.Equal(t, uint64(200), uint64(got.TotalActiveStake),
		"second fallback claim must replace the provisional row in place")
}

func TestFallbackRewardSnapshotGuardTemporaryRow(t *testing.T) {
	db := setupTestDB(t)
	meta := db.Metadata()
	txn := db.Transaction(true)

	proceed, guardID, err := meta.ClaimFallbackRewardSnapshotGuard(
		7,
		"mark",
		txn.Metadata(),
	)
	require.NoError(t, err)
	require.True(t, proceed)
	require.NotZero(t, guardID)

	guard, err := meta.GetRewardSnapshot(7, "mark", txn.Metadata())
	require.NoError(t, err)
	require.NotNil(t, guard)
	require.False(t, guard.Authoritative)

	require.NoError(t, meta.ReleaseFallbackRewardSnapshotGuard(
		guardID,
		txn.Metadata(),
	))
	require.NoError(t, txn.Commit())

	guard, err = meta.GetRewardSnapshot(7, "mark", nil)
	require.NoError(t, err)
	require.Nil(t, guard,
		"the temporary serialization row must not survive commit")
}

func TestFallbackRewardSnapshotGuardRefusesAuthoritative(t *testing.T) {
	db := setupTestDB(t)
	meta := db.Metadata()
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:         8,
		SnapshotType:  "mark",
		CapturedSlot:  80,
		BoundarySlot:  81,
		Authoritative: true,
	}, nil))

	txn := db.Transaction(true)
	proceed, guardID, err := meta.ClaimFallbackRewardSnapshotGuard(
		8,
		"mark",
		txn.Metadata(),
	)
	require.NoError(t, err)
	require.False(t, proceed)
	require.Zero(t, guardID)
	require.NoError(t, txn.Rollback())

	got, err := meta.GetRewardSnapshot(8, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.Authoritative)
	require.Equal(t, uint64(80), got.CapturedSlot)
}

func TestFallbackRewardSnapshotGuardRequiresTransaction(t *testing.T) {
	db := setupTestDB(t)
	meta := db.Metadata()

	_, _, err := meta.ClaimFallbackRewardSnapshotGuard(9, "mark", nil)
	require.ErrorContains(t, err, "transaction is required")
	require.ErrorContains(
		t,
		meta.ReleaseFallbackRewardSnapshotGuard(1, nil),
		"transaction is required",
	)
}
