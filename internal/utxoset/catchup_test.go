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

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// applySlotsWithNonces applies one output and one block nonce per slot in
// [from, to], each its own commit, to a set that holds every family.
func applySlotsWithNonces(t *testing.T, set *utxoset.Set, from, to uint64) {
	t.Helper()
	if _, ok, err := set.Tip(); err == nil && !ok {
		require.NoError(t, set.LoadUtxoSet(
			slotPoint(0), catchUpFeed(),
		))
		require.NoError(t, set.FinishImport())
	}
	for slot := from; slot <= to; slot++ {
		tip := slotPoint(slot)
		require.NoError(t, set.ApplyLedgerChanges(&tip, ledger.LedgerChanges{
			Utxo: []ledger.UtxoEvent{{
				Point: tip,
				Added: []ledger.UtxoOutput{{
					Ref:  models.UtxoId{Hash: bytes.Repeat([]byte{byte(slot)}, 32)},
					Cbor: []byte{0x80},
				}},
			}},
			Nonces: []models.BlockNonce{{
				Slot: slot, Hash: tip.Hash,
				Nonce: bytes.Repeat([]byte{byte(slot)}, 32),
			}},
		}))
	}
}

func catchUpFeed(slots ...byte) func(func([]byte, uint32, []byte) error) error {
	return func(put func([]byte, uint32, []byte) error) error {
		for _, s := range slots {
			if err := put(bytes.Repeat([]byte{s}, 32), 0, []byte{0x80}); err != nil {
				return err
			}
		}
		return nil
	}
}

func nonceSlots(t *testing.T, set *utxoset.Set) []uint64 {
	t.Helper()
	ns, err := set.BlockNoncesInSlotRange(0, 1000)
	require.NoError(t, err)
	out := make([]uint64, 0, len(ns))
	for _, n := range ns {
		out = append(out, n.Slot)
	}
	return out
}

// TestCatchUpOfASetPastTheSnapshotRollsItBackAndKeepsItsHistory requires a
// catch-up whose snapshot is older than the set's tip to roll the set back to
// the snapshot's point through its journal and merge, keeping the history
// below it, in place of replacing the whole set.
func TestCatchUpOfASetPastTheSnapshotRollsItBackAndKeepsItsHistory(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	applySlotsWithNonces(t, set, 1, 10)

	require.NoError(t, set.LoadUtxoSetCatchUp(slotPoint(6), catchUpFeed(0xa1, 0xa2)))

	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(6), tip.Slot)
	n, err := set.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(2), n, "the outputs are the snapshot's")
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, nonceSlots(t, set),
		"the history below the snapshot survives and the tail above it is rolled back")
}

// TestCatchUpOfASetThatCannotReachTheSnapshotIsRefused requires a catch-up that
// would replace a set whose journal does not reach the snapshot's point to
// fail with the set untouched, since replacing it loses the history it keeps.
func TestCatchUpOfASetThatCannotReachTheSnapshotIsRefused(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	set.UpdateAnchor(slotPoint(12))
	applySlotsWithNonces(t, set, 1, 12)
	require.NoError(t, set.PruneJournalForTest(9))

	err = set.LoadUtxoSetCatchUp(slotPoint(2), catchUpFeed(0xa1))

	require.ErrorIs(t, err, badgerutxo.ErrRollbackBeyondJournal)
	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(12), tip.Slot)
	n, err := set.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(12), n, "the set is untouched")
}

// TestCatchUpReachesOnlyWhatTheCatchUpLoadAccepts requires CatchUpReaches to
// agree with LoadUtxoSetCatchUp: a point the journal holds, a point past the
// tip, and no point the load refuses.
func TestCatchUpReachesOnlyWhatTheCatchUpLoadAccepts(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	set.UpdateAnchor(slotPoint(12))
	applySlotsWithNonces(t, set, 1, 12)
	require.NoError(t, set.PruneJournalForTest(9))

	onOtherChain := slotPoint(10)
	onOtherChain.Hash = bytes.Repeat([]byte{0xfe}, 32)
	for _, tc := range []struct {
		name    string
		point   func() ocommon.Point
		reaches bool
	}{
		{"journaled point", func() ocommon.Point { return slotPoint(10) }, true},
		{"past the tip", func() ocommon.Point { return slotPoint(20) }, true},
		{"below the journal", func() ocommon.Point { return slotPoint(2) }, false},
		{"another chain's point", func() ocommon.Point { return onOtherChain }, false},
	} {
		reaches, err := set.CatchUpReaches(tc.point())
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.reaches, reaches, tc.name)
	}
	tip, _, err := set.Tip()
	require.NoError(t, err)
	require.Equal(t, uint64(12), tip.Slot, "the check changes nothing")

	require.Error(t, set.LoadUtxoSetCatchUp(onOtherChain, catchUpFeed(0xa1)),
		"the load refuses the point the check refuses")
}
