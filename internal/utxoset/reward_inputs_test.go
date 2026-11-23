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

package utxoset_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func stakeRows(epoch uint64, n int, slot uint64) []*models.RewardStakeInput {
	rows := make([]*models.RewardStakeInput, 0, n)
	for i := range n {
		pool := bytes.Repeat([]byte{byte(i % 7)}, 28)
		key := make([]byte, 28)
		binary.BigEndian.PutUint32(
			key,
			uint32(i),
		) //nolint:gosec // a test index
		rows = append(rows, &models.RewardStakeInput{
			PoolKeyHash: pool, StakingKey: key, Epoch: epoch,
			CredentialTag: uint8(i % 2), //nolint:gosec // 0 or 1
			Stake:         types.Uint64(uint64(i) + 1),
			Owner:         i%5 == 0, Registered: true,
			CapturedSlot: slot - 1, BoundarySlot: slot,
		})
	}
	return rows
}

// A snapshot's stake inputs larger than one store transaction holds are
// written in several, under one tombstone, read back in order and range, and
// dropped by one rollback; a later epoch's prune drops the oldest epoch's.
func TestStakeInputsLargerThanAStoreTransactionAreChunkedAndRolledBack(
	t *testing.T,
) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })

	const rows = 150_000
	apply := func(slot uint64, changes ...ledger.RewardInputChange) {
		tip := ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32))
		require.NoError(t, set.ApplyLedgerChanges(
			&tip, ledger.LedgerChanges{RewardInputs: changes},
		))
	}
	apply(10, ledger.RewardInputChange{
		Kind: ledger.RewardInputStakes, Epoch: 5,
		Stakes: stakeRows(5, 10, 10),
	})
	apply(20,
		ledger.RewardInputChange{Kind: ledger.RewardInputReplace, Epoch: 6},
		ledger.RewardInputChange{
			Kind: ledger.RewardInputStakes, Epoch: 6,
			Stakes: stakeRows(6, rows, 20),
		},
	)
	got, err := set.RewardStakeRows(6, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, got, rows)
	for i := 1; i < len(got); i++ {
		a, b := got[i-1], got[i]
		require.LessOrEqual(t, bytes.Compare(a.PoolKeyHash, b.PoolKeyHash), 0)
	}
	pool := bytes.Repeat([]byte{3}, 28)
	ranged, err := set.RewardStakeRows(6, pool, pool, nil)
	require.NoError(t, err)
	require.NotEmpty(t, ranged)
	for _, r := range ranged {
		require.Equal(t, pool, r.PoolKeyHash)
	}

	apply(30, ledger.RewardInputChange{
		Kind: ledger.RewardInputPrune, PruneBelow: 6,
	})
	pruned, err := set.RewardStakeRows(5, nil, nil, nil)
	require.NoError(t, err)
	require.Empty(t, pruned, "the prune drops the epoch below the window")
	kept, err := set.RewardStakeRows(6, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, kept, rows)

	_, err = set.Rollback(ocommon.NewPoint(10, bytes.Repeat([]byte{10}, 32)))
	require.NoError(t, err)
	gone, err := set.RewardStakeRows(6, nil, nil, nil)
	require.NoError(t, err)
	require.Empty(t, gone, "one tombstone drops every chunk's rows")
}

// A drained reward-inputs family leaves the set the only holder of the mark
// snapshots' calculation versions, so the set names the epochs whose mark
// snapshot was produced by another version. A set or go row of that version
// is not a mark snapshot and is not named.
func TestStaleRewardSnapshotEpochsNamesMarkSnapshotsOfAnotherVersion(
	t *testing.T,
) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	const current = models.RewardStakeCalculationVersion

	snap := func(epoch uint64, typ string, version uint) ledger.RewardInputChange {
		return ledger.RewardInputChange{
			Kind: ledger.RewardInputSnapshot,
			Snapshot: &models.RewardSnapshot{
				Epoch: epoch, SnapshotType: typ, CalculationVersion: version,
				CapturedSlot: 10, BoundarySlot: 10,
			},
		}
	}
	tip := ocommon.NewPoint(10, bytes.Repeat([]byte{10}, 32))
	require.NoError(t, set.ApplyLedgerChanges(&tip, ledger.LedgerChanges{
		RewardInputs: []ledger.RewardInputChange{
			snap(4, "mark", current),
			snap(5, "mark", current-1),
			snap(6, "set", current-1),
			snap(7, "mark", current+1),
		},
	}))

	stale, err := set.StaleRewardSnapshotEpochs(current)
	require.NoError(t, err)
	require.Equal(t, []uint64{5, 7}, stale)

	none, err := set.StaleRewardSnapshotEpochs(current - 1)
	require.NoError(t, err)
	require.Equal(t, []uint64{4, 7}, none)
}

// The pending stake-input record is replaced as a whole by each change, read
// back oldest first, and a rollback below a snapshot's boundary drops its
// entry.
func TestPendingStakeInputsAreReplacedAndRolledBack(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	apply := func(slot uint64, entries ...ledger.RewardStakeInputsPendingEntry) {
		tip := ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32))
		require.NoError(t, set.ApplyLedgerChanges(
			&tip, ledger.LedgerChanges{RewardInputs: []ledger.RewardInputChange{{
				Kind: ledger.RewardInputPending, Pending: entries,
			}}},
		))
	}
	entry := func(epoch, slot uint64) ledger.RewardStakeInputsPendingEntry {
		return ledger.RewardStakeInputsPendingEntry{
			Epoch: epoch, BoundarySlot: slot,
		}
	}
	apply(10, entry(5, 10), entry(4, 5))
	got, err := set.RewardStakePending(nil)
	require.NoError(t, err)
	require.Equal(t, []ledger.RewardStakeInputsPendingEntry{
		entry(4, 5), entry(5, 10),
	}, got)

	apply(20, entry(5, 10))
	got, err = set.RewardStakePending(nil)
	require.NoError(t, err)
	require.Equal(t, []ledger.RewardStakeInputsPendingEntry{entry(5, 10)}, got)

	apply(30)
	got, err = set.RewardStakePending(nil)
	require.NoError(t, err)
	require.Empty(t, got)

	apply(40, entry(6, 40))
	_, err = set.Rollback(ocommon.NewPoint(30, bytes.Repeat([]byte{30}, 32)))
	require.NoError(t, err)
	got, err = set.RewardStakePending(nil)
	require.NoError(t, err)
	require.Empty(t, got, "a rollback below the boundary drops the entry")
}

// Listing the epochs the set holds reward inputs of steps once per epoch and
// tag, not once per stored row, so the cost of a listing does not grow with
// the stake rows an epoch holds.
func TestRewardInputEpochsDoesNotStepOverEveryRow(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })

	const perEpoch = 500
	epochs := []uint64{5, 6, 7}
	var changes []ledger.RewardInputChange
	for _, e := range epochs {
		changes = append(changes, ledger.RewardInputChange{
			Kind: ledger.RewardInputStakes, Epoch: e,
			Stakes: stakeRows(e, perEpoch, 10),
		})
	}
	tip := ocommon.NewPoint(10, bytes.Repeat([]byte{10}, 32))
	require.NoError(t, set.ApplyLedgerChanges(
		&tip, ledger.LedgerChanges{RewardInputs: changes},
	))

	before := set.RangeRowsVisited()
	snaps, _, err := set.RewardInputEpochs()
	require.NoError(t, err)
	visited := set.RangeRowsVisited() - before

	require.Equal(t, epochs, snaps)
	require.LessOrEqual(t, visited, uint64(3*len(epochs)+3),
		"the listing stepped over stored rows, not one row per epoch")
}

// RewardStakeSlotBounds names the slots of an epoch's first and last stake
// rows by reading those two rows, whatever the epoch holds, and reports an
// epoch with no rows as absent.
func TestRewardStakeSlotBoundsReadsTheEdgeRowsOnly(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })

	rows := stakeRows(5, 500, 10)
	for _, r := range rows {
		if r.PoolKeyHash[0] == 6 {
			r.CapturedSlot = 99
		}
	}
	var changes []ledger.RewardInputChange
	changes = append(changes,
		ledger.RewardInputChange{
			Kind: ledger.RewardInputStakes, Epoch: 5, Stakes: rows,
		},
		ledger.RewardInputChange{
			Kind: ledger.RewardInputStakes, Epoch: 6,
			Stakes: stakeRows(6, 500, 20),
		},
	)
	tip := ocommon.NewPoint(99, bytes.Repeat([]byte{9}, 32))
	require.NoError(t, set.ApplyLedgerChanges(
		&tip, ledger.LedgerChanges{RewardInputs: changes},
	))

	before := set.RangeRowsVisited()
	first, last, found, err := set.RewardStakeSlotBounds(6)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, [2]uint64{19, 19}, [2]uint64{first, last})
	require.LessOrEqual(t, set.RangeRowsVisited()-before, uint64(2),
		"the bounds read stepped over more than the two edge rows")

	// Keys lead with the pool, so the edges are the lowest and highest pool.
	first, last, found, err = set.RewardStakeSlotBounds(5)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, [2]uint64{9, 99}, [2]uint64{first, last})

	_, _, found, err = set.RewardStakeSlotBounds(7)
	require.NoError(t, err)
	require.False(t, found)
}
