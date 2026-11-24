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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// chainUpTo returns a resume that accepts a point only when its hash is the
// one slotPoint gives its slot and the slot is at most last, as a ledger whose
// chain holds the first last blocks does, and records every point asked.
func chainUpTo(
	last uint64,
	asked *[]uint64,
) func(context.Context, ocommon.Point) (bool, error) {
	return func(_ context.Context, p ocommon.Point) (bool, error) {
		*asked = append(*asked, p.Slot)
		return p.Slot <= last && bytes.Equal(p.Hash, slotPoint(p.Slot).Hash), nil
	}
}

// A set the metadata store cannot rebuild is rolled back through its journal
// to the nearest point the ledger can resume from, instead of stopping the
// node.
func TestReconcileRecoversADrainedSetFromItsJournal(t *testing.T) {
	t.Parallel()
	const applied = 8
	forkTip := ocommon.NewPoint(5, bytes.Repeat([]byte{0xee}, 32))
	for _, tc := range []struct {
		name       string
		cfg        utxoset.Config
		ledgerOnly bool
		ledgerTip  ocommon.Point
		chainTo    uint64
		wantAsked  []uint64
		wantSlot   uint64
	}{
		{
			name:       "behind on a fork the chain left, drained",
			ledgerOnly: true,
			ledgerTip:  slotPoint(10),
			chainTo:    5,
			// The set's own tip is asked first by Reconcile, then the
			// journal points newest first.
			wantAsked: []uint64{applied, 7, 6, 5},
			wantSlot:  5,
		},
		{
			name:      "behind on a fork the chain left, only",
			cfg:       utxoset.Config{Only: true},
			ledgerTip: slotPoint(10),
			chainTo:   3,
			wantAsked: []uint64{applied, 7, 6, 5, 4, 3},
			wantSlot:  3,
		},
		{
			name:      "ahead, ledger tip on another fork, only",
			cfg:       utxoset.Config{Only: true},
			ledgerTip: forkTip,
			chainTo:   3,
			wantAsked: []uint64{applied, 4, 3},
			wantSlot:  3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			set := openRecoverySet(t, tc.cfg)
			require.NoError(t, set.StartEmptyForTest())
			applySlots(t, set, applied)
			if tc.ledgerOnly {
				set.ServeLedgerOnly()
			}
			db := ledgerAt(t, tc.ledgerTip)
			var asked []uint64
			err := set.Reconcile(
				t.Context(), dbadapter.New(db), chainUpTo(tc.chainTo, &asked),
			)
			require.NoError(t, err)
			require.Equal(t, tc.wantAsked, asked)
			tip, ok, err := set.Tip()
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.wantSlot, tip.Slot)
			n, err := set.Count()
			require.NoError(t, err)
			require.Equal(t, tc.wantSlot, n)
		})
	}
}

// When no point of the journal is one the ledger can resume from, the error
// names the journal floor and the missing retained state, and does not tell
// the operator to resync.
func TestReconcileNamesWhatIsMissingWhenTheJournalHoldsNoBase(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{Only: true})
	applySlots(t, set, 6)
	set.ServeLedgerOnly()
	db := ledgerAt(t, slotPoint(9))
	var asked []uint64
	err := set.Reconcile(
		t.Context(), dbadapter.New(db), chainUpTo(0, &asked),
	)
	require.ErrorContains(t, err, "rollback journal reaches back only to")
	require.NotContains(t, err.Error(), "resync")
}

// An error from the ledger's rollback stops the walk at once: the set is not
// rolled further back for a failure that may be transient.
func TestReconcileJournalWalkStopsOnAResumeError(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{Only: true})
	applySlots(t, set, 6)
	db := ledgerAt(t, slotPoint(9))
	calls := 0
	resume := func(context.Context, ocommon.Point) (bool, error) {
		calls++
		if calls == 1 {
			return false, nil
		}
		return false, context.DeadlineExceeded
	}
	err := set.Reconcile(t.Context(), dbadapter.New(db), resume)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 2, calls)
	tip, _, err := set.Tip()
	require.NoError(t, err)
	require.Equal(t, uint64(5), tip.Slot)
}

// setFormatMark rewrites the layout version a closed set directory records.
func setFormatMark(t *testing.T, dir string, version byte) {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("mfmt"), []byte{version})
	}))
	require.NoError(t, db.Close())
}

// A set written in another layout that the metadata store cannot rebuild
// fails with a message naming the layout and the migration this release lacks,
// and leaves the set untouched; a set the store can rebuild is rebuilt.
func TestReconcileNamesTheMissingLayoutMigration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		cfg        utxoset.Config
		ledgerOnly bool
		wantErr    bool
	}{
		{"drained", utxoset.Config{}, true, true},
		{"only", utxoset.Config{Only: true}, false, true},
		{"released source", utxoset.Config{}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			cfg := tc.cfg
			cfg.Dir = dir
			set, err := utxoset.Open(cfg)
			require.NoError(t, err)
			require.NoError(t, set.StartEmptyForTest())
			applySlots(t, set, 3)
			require.NoError(t, set.Close())
			setFormatMark(t, dir, 1)

			set, err = utxoset.Open(cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			if tc.ledgerOnly {
				set.ServeLedgerOnly()
			}
			if tc.wantErr {
				db := ledgerAt(t, slotPoint(3))
				err = set.Reconcile(t.Context(), dbadapter.New(db), neverResume)
			} else {
				db := releasedAt(t, slotPoint(3))
				err = set.ReconcileReleased(t.Context(), dbadapter.New(db))
			}
			n, countErr := set.Count()
			require.NoError(t, countErr)
			if !tc.wantErr {
				require.NoError(t, err)
				require.Zero(t, n, "rebuilt from the empty store")
				return
			}
			require.ErrorContains(t, err, "layout version 1")
			require.ErrorContains(t, err, "no migration of the set's rows")
			require.NotContains(t, err.Error(), "resync")
			require.EqualValues(t, 3, n, "the set must be left as it was")
		})
	}
}
