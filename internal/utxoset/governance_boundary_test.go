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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/stretchr/testify/require"
)

var _ ledgerstate.GovernanceBoundaryLoader = (*utxoset.Set)(nil)

// boundarySnapshot is epoch's boundary at slot 10*epoch.
func boundarySnapshot(epoch uint64) models.GovernanceBoundarySnapshot {
	return models.GovernanceBoundarySnapshot{
		Epoch:        epoch,
		BoundarySlot: 10 * epoch,
		DRepDistribution: []models.DRepStakeDistributionEntry{{
			Credential: bytes.Repeat([]byte{byte(epoch)}, 28),
			Stake:      1000 + epoch,
		}},
		Ratified: []models.GovernanceActionRef{{
			TxHash: bytes.Repeat([]byte{0xa0 + byte(epoch)}, 32),
		}},
		RatificationTreasury: 7 * epoch,
	}
}

// applyBoundaryChain commits one block per slot in [1, to], with the
// boundary of epoch e written at slot 10*e when the chain reaches it.
func applyBoundaryChain(t *testing.T, set *utxoset.Set, to uint64) {
	t.Helper()
	for slot := uint64(1); slot <= to; slot++ {
		tip := slotPoint(slot)
		lc := ledger.LedgerChanges{Utxo: []ledger.UtxoEvent{{
			Point: tip,
			Added: []ledger.UtxoOutput{{
				Ref:  models.UtxoId{Hash: bytes.Repeat([]byte{byte(slot)}, 32)},
				Cbor: []byte{0x80},
			}},
		}}}
		if slot%10 == 0 {
			lc.GovernanceBoundaries = []models.GovernanceBoundarySnapshot{
				boundarySnapshot(slot / 10),
			}
		}
		require.NoError(t, set.ApplyLedgerChanges(&tip, lc))
	}
}

func requireBoundary(t *testing.T, set *utxoset.Set, epoch uint64, at *uint64) {
	t.Helper()
	got, ok, err := set.GovernanceBoundarySnapshotAt(epoch, at)
	require.NoError(t, err)
	require.True(t, ok, "epoch %d has no boundary row", epoch)
	require.Equal(t, boundarySnapshot(epoch), *got)
}

func requireNoBoundary(
	t *testing.T,
	set *utxoset.Set,
	epoch uint64,
	at *uint64,
) {
	t.Helper()
	_, ok, err := set.GovernanceBoundarySnapshotAt(epoch, at)
	require.NoError(t, err)
	require.False(t, ok, "epoch %d keeps a boundary row", epoch)
}

func TestGovernanceBoundarySurvivesRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	set, err := utxoset.Open(utxoset.Config{Dir: dir})
	require.NoError(t, err)
	applyBoundaryChain(t, set, 25)
	require.NoError(t, set.Close())

	set, err = utxoset.Open(utxoset.Config{Dir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	requireBoundary(t, set, 1, nil)
	requireBoundary(t, set, 2, nil)
	requireNoBoundary(t, set, 3, nil)
}

// A rollback below a boundary removes its row through the journal entry at
// the boundary slot, and a read as of a slot before the boundary finds none.
func TestGovernanceBoundaryRollbackBelowTheBoundaryRemovesIt(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	applyBoundaryChain(t, set, 25)

	before, at := uint64(19), uint64(20)
	requireNoBoundary(t, set, 2, &before)
	requireBoundary(t, set, 2, &at)

	_, err = set.Rollback(slotPoint(20))
	require.NoError(t, err)
	requireBoundary(t, set, 2, nil)

	_, err = set.Rollback(slotPoint(19))
	require.NoError(t, err)
	requireNoBoundary(t, set, 2, nil)
	requireBoundary(t, set, 1, nil)
}

// A boundary written after the boundary committed, as a deferred RATIFY
// writes it, is still journaled at the boundary slot.
func TestGovernanceBoundaryWrittenLateRollsBackWithItsBoundary(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	applyBoundaryChain(t, set, 9)
	for slot := uint64(10); slot <= 15; slot++ {
		tip := slotPoint(slot)
		lc := ledger.LedgerChanges{Utxo: []ledger.UtxoEvent{{Point: tip}}}
		if slot == 15 {
			lc.GovernanceBoundaries = []models.GovernanceBoundarySnapshot{
				boundarySnapshot(1),
			}
		}
		require.NoError(t, set.ApplyLedgerChanges(&tip, lc))
	}

	_, err = set.Rollback(slotPoint(12))
	require.NoError(t, err)
	requireBoundary(t, set, 1, nil)
	_, err = set.Rollback(slotPoint(9))
	require.NoError(t, err)
	requireNoBoundary(t, set, 1, nil)
}

// Pruning keeps the newest boundary below the journal floor, which the slots
// still inside the rollback window read, and drops the older ones.
func TestGovernanceBoundaryPruneKeepsTheEpochTheFloorIsIn(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	applyBoundaryChain(t, set, 45)

	require.NoError(t, set.PruneHistory(35))
	requireNoBoundary(t, set, 1, nil)
	requireNoBoundary(t, set, 2, nil)
	requireBoundary(t, set, 3, nil)
	requireBoundary(t, set, 4, nil)
}

// A full load replaces the set, its boundaries included; a catch-up keeps
// the boundaries below the snapshot and rolls back those above it.
func TestGovernanceBoundaryImportLoads(t *testing.T) {
	t.Parallel()
	t.Run("full", func(t *testing.T) {
		t.Parallel()
		set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
		require.NoError(t, err)
		t.Cleanup(func() { _ = set.Close() })
		applyBoundaryChain(t, set, 25)

		require.NoError(t, set.LoadUtxoSet(slotPoint(35), catchUpFeed(0xa1)))
		imported := boundarySnapshot(3)
		require.NoError(t, set.LoadGovernanceBoundarySnapshot(&imported))
		requireNoBoundary(t, set, 1, nil)
		requireNoBoundary(t, set, 2, nil)
		requireBoundary(t, set, 3, nil)
	})
	t.Run("catch-up", func(t *testing.T) {
		t.Parallel()
		set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
		require.NoError(t, err)
		t.Cleanup(func() { _ = set.Close() })
		applyBoundaryChain(t, set, 35)

		require.NoError(
			t,
			set.LoadUtxoSetCatchUp(slotPoint(25), catchUpFeed(0xa1)),
		)
		imported := boundarySnapshot(2)
		imported.RatificationTreasury = 99
		require.NoError(t, set.LoadGovernanceBoundarySnapshot(&imported))
		requireBoundary(t, set, 1, nil)
		requireNoBoundary(t, set, 3, nil)
		got, ok, err := set.GovernanceBoundarySnapshotAt(2, nil)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, imported, *got)
	})
}

// The startup rebuild writes a missing boundary with no tip once the chain
// has passed it. The row lands at the boundary slot: a read as of an earlier
// slot finds none, a rollback that keeps the boundary keeps the row, and one
// below the boundary removes it.
func TestGovernanceBoundaryRebuiltAfterTheBoundaryIsJournaledAtIt(
	t *testing.T,
) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	applySlotsWithNonces(t, set, 1, 25)
	requireNoBoundary(t, set, 2, nil)

	require.NoError(t, set.ApplyLedgerChanges(nil, ledger.LedgerChanges{
		GovernanceBoundaries: []models.GovernanceBoundarySnapshot{
			boundarySnapshot(2),
		},
	}))
	requireBoundary(t, set, 2, nil)
	before := uint64(19)
	requireNoBoundary(t, set, 2, &before)

	_, err = set.Rollback(slotPoint(22))
	require.NoError(t, err)
	requireBoundary(t, set, 2, nil)
	_, err = set.Rollback(slotPoint(19))
	require.NoError(t, err)
	requireNoBoundary(t, set, 2, nil)
}

// A boundary replay below the set's journal must see the rollback preview
// fail as ledger.ErrUtxoAsOfUnavailable, which ends the startup rebuild
// instead of failing startup.
func TestPreviewBeyondTheJournalIsUtxoAsOfUnavailable(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	set.UpdateAnchor(slotPoint(12))
	applySlotsWithNonces(t, set, 1, 12)
	require.NoError(t, set.PruneJournalForTest(9))

	_, err = set.PreviewUtxoSetRollback(slotPoint(2))
	require.ErrorIs(t, err, ledger.ErrUtxoAsOfUnavailable)
}
