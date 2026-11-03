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

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/stretchr/testify/require"
)

// captureAcrossEraBoundary stashes a SNAP-point distribution for a boundary
// whose outgoing epoch runs at fromEra and incoming epoch at toEra, overwrites
// the stash with a sentinel stake, persists the boundary and returns the
// persisted Mark stake. A result equal to the sentinel means the stash was
// persisted; the seeded stake means the boundary was reconstructed instead.
func captureAcrossEraBoundary(
	t *testing.T,
	fromEra, toEra uint,
) (sentinel, seeded, persisted uint64) {
	t.Helper()
	const (
		seededStake   = uint64(75_000_000)
		sentinelStake = uint64(999_000_111)
	)
	db := setupTestDB(t)
	for _, epoch := range []struct {
		id, start uint64
		era       uint
	}{
		{id: 0, start: 0, era: fromEra},
		{id: 1, start: 432000, era: toEra},
	} {
		require.NoError(t, db.SetEpoch(
			epoch.start, epoch.id, nil, nil, nil, nil, epoch.era, 1, 432000, nil,
		))
	}
	poolHash := []byte("eraboundary_pool_1234567890A")
	stakingKey := bytes.Repeat([]byte{0xe7}, 28)
	seedPoolAndDelegations(t, db, poolHash, []markerDelegation{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{types.Uint64(seededStake)}},
	}, 500)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432000,
		EpochNonce:      []byte{0x01, 0x02},
		ProtocolVersion: 8,
		SnapshotSlot:    431999,
	}
	txn := db.Transaction(context.Background(), true)
	require.NoError(
		t,
		mgr.ComputeEpochBoundarySnapshot(context.Background(), txn, evt),
	)
	mgr.mu.Lock()
	require.NotNil(t, mgr.pendingBoundary, "stash must exist after Compute")
	for k := range mgr.pendingBoundary.distribution.PoolStakes {
		mgr.pendingBoundary.distribution.PoolStakes[k] = sentinelStake
	}
	mgr.pendingBoundary.distribution.TotalStake = sentinelStake
	mgr.mu.Unlock()
	require.NoError(
		t,
		mgr.CaptureEpochBoundarySnapshot(context.Background(), txn, evt),
	)
	require.NoError(t, txn.Commit())

	rows, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(1, "mark", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	return sentinelStake, seededStake, uint64(rows[0].TotalStake)
}

// TestCaptureEpochBoundaryKeepsSnapPointStakeAcrossEraWithSameStakeRules
// pins that an era change which leaves every era-gated stake rule unchanged
// persists the stashed SNAP-point distribution. Reconstructing the boundary
// historically there is not merely redundant: on mainnet the reconstruction
// runs for the whole rollover transaction, during which header verification
// has no stable ledger context, so the node stops at the last block of the
// outgoing era.
func TestCaptureEpochBoundaryKeepsSnapPointStakeAcrossEraWithSameStakeRules(
	t *testing.T,
) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		from, to uint
	}{
		{"shelley_to_allegra", eras.ShelleyEraDesc.Id, eras.AllegraEraDesc.Id},
		{"allegra_to_mary", eras.AllegraEraDesc.Id, eras.MaryEraDesc.Id},
		{"mary_to_alonzo", eras.MaryEraDesc.Id, eras.AlonzoEraDesc.Id},
		{"alonzo_to_babbage", eras.AlonzoEraDesc.Id, eras.BabbageEraDesc.Id},
		{"conway_to_dijkstra", eras.ConwayEraDesc.Id, eras.DijkstraEraDesc.Id},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sentinel, _, persisted := captureAcrossEraBoundary(t, tc.from, tc.to)
			require.Equal(t, sentinel, persisted,
				"the persisted Mark stake must come from the SNAP-point "+
					"stash: no stake rule changes between these eras, so "+
					"the boundary must not be reconstructed at persist time")
		})
	}
}

// TestCaptureEpochBoundaryAgreesOnPointerStakeAcrossSameRuleEraChanges pins
// that the kept SNAP-point stash and the historical reconstruction attribute
// pointer-address stake identically at every pre-Conway era change. The
// Shelley through Babbage translations carry ShelleyInstantStake across
// unchanged (coerce in each era's Translation.hs), so the mark snapshot at
// those boundaries counts pointer stake under both the outgoing and the
// incoming era; only the outgoing epoch's row exists when the SNAP read runs,
// exactly as in the rollover.
func TestCaptureEpochBoundaryAgreesOnPointerStakeAcrossSameRuleEraChanges(
	t *testing.T,
) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		from, to uint
	}{
		{"shelley_to_allegra", eras.ShelleyEraDesc.Id, eras.AllegraEraDesc.Id},
		{"allegra_to_mary", eras.AllegraEraDesc.Id, eras.MaryEraDesc.Id},
		{"mary_to_alonzo", eras.MaryEraDesc.Id, eras.AlonzoEraDesc.Id},
		{"alonzo_to_babbage", eras.AlonzoEraDesc.Id, eras.BabbageEraDesc.Id},
	} {
		for _, computeSnap := range []bool{true, false} {
			name := tc.name + "/historical_fallback"
			if computeSnap {
				name = tc.name + "/snap_point_stash"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				db := setupTestDB(t)
				seedEpochRow(t, db, nil, 0, 0, 300, tc.from)
				poolHash := bytes.Repeat([]byte{0xb7}, 28)
				seedPointerStakeFixture(t, db, poolHash)

				mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
				evt := event.EpochTransitionEvent{
					PreviousEpoch:   0,
					NewEpoch:        1,
					BoundarySlot:    300,
					EpochNonce:      []byte{0x0a, 0x0b},
					ProtocolVersion: 2,
					SnapshotSlot:    299,
				}
				txn := db.Transaction(context.Background(), true)
				if computeSnap {
					require.NoError(t, mgr.ComputeEpochBoundarySnapshot(
						context.Background(), txn, evt,
					))
				}
				seedEpochRow(t, db, txn, 300, 1, 300, tc.to)
				require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
					context.Background(), txn, evt,
				))
				require.NoError(t, txn.Commit())

				poolSnapshot, err := db.Metadata().GetPoolStakeSnapshot(
					1, "mark", poolHash, nil,
				)
				require.NoError(t, err)
				require.NotNil(t, poolSnapshot)
				require.Equal(t, uint64(1_300), uint64(poolSnapshot.TotalStake),
					"base-address 700 plus pointer-address 600: both eras "+
						"count pointer stake, so both capture routes must "+
						"attribute it")
			})
		}
	}
}

// TestCaptureEpochBoundaryDiscardsSnapPointStakeWhenPointerStakeRuleChanges
// is the negative case: the Babbage->Conway boundary stops counting
// pointer-address stake, and the SNAP-point read was taken under the outgoing
// era's rule, so the persist half must reconstruct the boundary instead.
func TestCaptureEpochBoundaryDiscardsSnapPointStakeWhenPointerStakeRuleChanges(
	t *testing.T,
) {
	t.Parallel()
	_, seeded, persisted := captureAcrossEraBoundary(
		t, eras.BabbageEraDesc.Id, eras.ConwayEraDesc.Id,
	)
	require.Equal(t, seeded, persisted,
		"the persisted Mark stake must be reconstructed at persist time "+
			"when the incoming era changes the pointer-stake rule")
}

func TestCurrentBoundarySPOStakeRowsUsesIncomingEraBeforeEpochRowExists(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := setupTestDB(t)
	seedEpochRow(t, db, nil, 0, 0, 300, eras.BabbageEraDesc.Id)
	poolHash := bytes.Repeat([]byte{0xb7}, 28)
	seedPointerStakeFixture(t, db, poolHash)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    300,
		EpochNonce:      []byte{0x0a, 0x0b},
		ProtocolVersion: 8,
		SnapshotSlot:    299,
	}
	txn := db.Transaction(ctx, true)
	require.NoError(t, mgr.ComputeEpochBoundarySnapshot(ctx, txn, evt))

	rows, err := mgr.CurrentBoundarySPOStakeRows(
		ctx, txn, evt, eras.ConwayEraDesc.Id,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, uint64(700), uint64(rows[0].TotalStake),
		"the Conway tally must exclude pointer stake before its epoch row exists")

	seedEpochRow(t, db, txn, 300, 1, 300, eras.ConwayEraDesc.Id)
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(ctx, txn, evt))
	require.NoError(t, txn.Commit())

	persisted, err := db.Metadata().GetPoolStakeSnapshot(
		1, "mark", poolHash, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	require.Equal(t, uint64(rows[0].TotalStake), uint64(persisted.TotalStake),
		"RATIFY and the persisted boundary must use the same incoming-era stake")
}
